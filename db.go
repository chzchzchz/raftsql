package raftsql

import (
	"database/sql"
	"errors"
	_ "github.com/mattn/go-sqlite3"
	"log"
	"strings"
	"sync"
)

type raftdb struct {
	rp   *raftPipe
	db   *sql.DB
	dbmu sync.RWMutex
	mu   sync.Mutex
	q2cb map[string][]chan<- error
}

func NewDB(path string, rp *raftPipe) *raftdb {
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		log.Fatal(err)
	}
	rdb := &raftdb{rp: rp, db: db, q2cb: make(map[string][]chan<- error)}
	rdb.readCommits()
	go rdb.readCommits()
	return rdb
}

func (rdb *raftdb) readCommits() {
	for q := range rdb.rp.CommitC {
		if q == nil {
			return
		}
		query := *q

		rdb.dbmu.Lock()
		_, err := rdb.db.Exec(query)
		rdb.dbmu.Unlock()

		rdb.mu.Lock()
		cbcs, ok := rdb.q2cb[query]
		if !ok {
			// either a replay or from another node
			rdb.mu.Unlock()
			continue
		}
		cur_cbc := cbcs[0]
		if len(cbcs) == 1 {
			delete(rdb.q2cb, query)
		} else {
			rdb.q2cb[query] = cbcs[1:]
		}
		rdb.mu.Unlock()
		cur_cbc <- err
		close(cur_cbc)
	}

	if err, ok := <-rdb.rp.ErrorC; ok {
		rdb.mu.Lock()
		for _, v := range rdb.q2cb {
			for i := range v {
				v[i] <- err
				close(v[i])
			}
		}
		rdb.q2cb = make(map[string][]chan<- error)
		rdb.mu.Unlock()
		rdb.db.Close()
		log.Fatal(err)
	}
}

func isSelect(query string) bool {
	tokens := strings.Split(strings.Trim(query, " "), " ")
	if len(tokens) == 0 || tokens[0] != "SELECT" {
		return false
	}
	return true
}

func (rdb *raftdb) Propose(query string) <-chan error {
	errc := make(chan error, 1)
	if isSelect(query) {
		errc <- errors.New("expected non-SELECT")
		return errc
	}
	rdb.mu.Lock()
	if old_cbs, ok := rdb.q2cb[query]; ok {
		rdb.q2cb[query] = append(old_cbs, errc)
	} else {
		rdb.q2cb[query] = []chan<- error{errc}
	}
	rdb.mu.Unlock()
	rdb.rp.ProposeC <- query
	return errc
}

func (rdb *raftdb) Query(query string) (string, error) {
	if !isSelect(query) {
		return "", errors.New("expected SELECT")
	}

	rdb.dbmu.RLock()
	rows, err := rdb.db.Query(query)
	rdb.dbmu.RUnlock()

	if err != nil {
		return "", err
	}
	defer rows.Close()

	ret := ""
	col_names, err := rows.Columns()
	if err != nil {
		return "", err
	}
	for rows.Next() {
		s := make([]interface{}, len(col_names))
		for i := range s {
			s[i] = &[]byte{}
		}
		if err := rows.Scan(s...); err != nil {
			return "", err
		}
		for i := range s {
			ret = ret + "|" + string(*(s[i].(*[]byte)))
		}
		ret = ret + "|\n"
	}

	return ret, nil
}
