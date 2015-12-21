package raftsql

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
)

type cluster struct {
	peers []string
	dbs   []*raftdb
}

func newCluster(num_peers int) *cluster {
	peers := make([]string, num_peers)
	for i := range peers {
		peers[i] = fmt.Sprintf("http://127.0.0.1:%d", i+10000)
	}

	clus := &cluster{peers, make([]*raftdb, len(peers))}
	clus.Apply(func(i int) {
		os.RemoveAll(fmt.Sprintf("raftsql-%d", i+1))
		clus.newNode(i)
	})
	return clus
}

func (clus *cluster) newNode(i int) {
	clus.newNodeListen(i, nil)
}

func (clus *cluster) newNodeListen(i int, commitListenerC chan<- *string) {
	if clus.dbs[i] != nil {
		return
	}
	dbpath := fmt.Sprintf("testcase-%d.db", i)
	rp := NewRaftPipe(i+1, clus.peers, make(chan string))
	clus.dbs[i] = NewDBListen(dbpath, rp, commitListenerC)
}

func (clus *cluster) resumeCluster() {
	clus.Apply(func(i int) { clus.newNode(i) })
}

func (clus *cluster) stopNode(i int) {
	if clus.dbs[i] != nil {
		clus.dbs[i].Close()
		clus.dbs[i] = nil
	}
}

func (clus *cluster) createEntries(t *testing.T) int {
	err := <-clus.dbs[0].Propose(
		"CREATE TABLE main.t (id int primary key asc, nodeid text)")
	if err != nil {
		t.Fatal(err)
	}

	clus.Apply(func(i int) {
		q := fmt.Sprintf("INSERT INTO main.t (nodeid) VALUES (\"%d\")", i)
		if err := <-clus.dbs[i].Propose(q); err != nil {
			t.Fatal(err)
		}
	})

	return 1 + len(clus.peers)
}

func (clus *cluster) Close(t *testing.T) {
	clus.Apply(func(i int) {
		if err := clus.dbs[i].Close(); err != nil {
			t.Fatal(err)
		}
	})
}

func (clus *cluster) Apply(f func(i int)) {
	var wg sync.WaitGroup
	wg.Add(len(clus.peers))
	for i := range clus.peers {
		go func(i int) {
			// defer in case of Fatal() to make progress
			defer wg.Done()
			f(i)
		}(i)
	}
	wg.Wait()
}

func TestNewDB(t *testing.T) {
	clus := newCluster(3)
	defer clus.Close(t)
	clus.createEntries(t)

	// check all nodes for consistent view
	clus.Apply(func(i int) {
		db := clus.dbs[i]
		if _, err := db.Query("SELECT * from main.x"); err == nil {
			t.Fatalf("Expected no such table, got nil")
		}

		v, err := db.Query("SELECT * from main.t")
		if err != nil {
			t.Fatal(err)
		}

		if !strings.Contains(v, "||0|") ||
			!strings.Contains(v, "||1|") ||
			!strings.Contains(v, "||2|") {
			t.Fatalf("Unexpected result", v)
		}
	})
}

func TestRestartDB(t *testing.T) {
	clus := newCluster(3)
	defer clus.Close(t)
	expected_ents := clus.createEntries(t)

	// take down node, add an entry
	clus.stopNode(1)
	q := "INSERT INTO main.t (nodeid) VALUES (\"foo\")"
	if err := <-clus.dbs[2].Propose(q); err != nil {
		t.Fatal(err)
	}

	// roll back db with log
	db1cc := make(chan *string)
	donec := make(chan struct{})
	go func() {
		clus.newNodeListen(1, db1cc)
		close(donec)
	}()
	// ignore rollback activity
	n := 0
	for s := range db1cc {
		if s == nil {
			break
		}
		n++
	}
	if n != expected_ents {
		t.Fatalf("Expected %d, got %d replay entries", expected_ents, n)
	}
	// wait for db to be queriable
	<-donec

	// make sure 'foo' is not in log; still out of sync with cluster
	v, err := clus.dbs[1].Query("SELECT * from main.t")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(v, "||foo|") {
		t.Fatalf("\"foo\" already in db! %s", v)
	}
	// sync with rest of cluster down to db
	<-db1cc

	// all nodes should now have "foo"
	clus.Apply(func(i int) {
		v, err := clus.dbs[i].Query("SELECT * from main.t")
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(v, "||foo|") {
			t.Fatal("\"foo\" missing from db")
		}
	})
}
