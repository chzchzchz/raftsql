package raftsql

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
)

func TestNewDB(t *testing.T) {
	var wg sync.WaitGroup
	peers := []string{
		"http://127.0.0.1:9021",
		"http://127.0.0.1:9022",
		"http://127.0.0.1:9023",
	}

	dbs := make([]*raftdb, len(peers))
	wg.Add(len(peers))
	for i := range peers {
		go func(i int) {
			defer wg.Done()
			dbpath := fmt.Sprintf("testcase-%d.db", i)
			os.RemoveAll(fmt.Sprintf("raftsql-%d", i+1))
			os.RemoveAll(dbpath)
			dbs[i] = NewDB(dbpath, NewRaftPipe(i+1, peers, make(chan string)))
		}(i)
	}
	wg.Wait()

	err := <-dbs[0].Propose(
		"CREATE TABLE main.t (id int primary key asc, nodeid text);")
	if err != nil {
		t.Fatal(err)
	}

	wg.Add(len(peers))
	for i := range peers {
		go func(i int) {
			defer wg.Done()
			q := fmt.Sprintf("INSERT INTO main.t (nodeid) VALUES (\"%d\");", i)
			if err := <-dbs[i].Propose(q); err != nil {
				t.Fatal(err)
			}
		}(i)
	}
	wg.Wait()

	// check all nodes for consistent view
	wg.Add(len(peers))
	for i := range peers {
		go func(db *raftdb) {
			defer wg.Done()
			if _, err := db.Query("SELECT * from main.x;"); err == nil {
				t.Fatalf("Expected no such table, got nil")
			}

			v, err := db.Query("SELECT * from main.t;")
			if err != nil {
				t.Fatal(err)
			}

			if !strings.Contains(v, "||0|") ||
				!strings.Contains(v, "||1|") ||
				!strings.Contains(v, "||2|") {
				t.Fatalf("Unexpected result", v)
			}
			close(db.rp.ProposeC)
		}(dbs[i])
	}
	wg.Wait()
}
