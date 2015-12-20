// Copyright 2015 CoreOS, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raftsql

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
)

// Handler for a http based key-value store backed by raft
type httpSQLAPI struct {
	db *raftdb
}

func dumpErr(w http.ResponseWriter, err error) {
	errs := fmt.Sprintf("%v", err)
	log.Print(errs)
	http.Error(w, errs, http.StatusBadRequest)
}

func (h *httpSQLAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.Method == "PUT":
		v, err := ioutil.ReadAll(r.Body)
		if err != nil {
			dumpErr(w, err)
			return
		}

		if err := <-h.db.Propose(string(v)); err != nil {
			dumpErr(w, err)
		} else {
			w.WriteHeader(http.StatusNoContent)
		}

	case r.Method == "GET":
		q, err := ioutil.ReadAll(r.Body)
		if err != nil {
			dumpErr(w, err)
			return
		}
		v, err := h.db.Query(string(q))
		if err != nil {
			dumpErr(w, err)
			return
		}
		w.Write([]byte(v))
	default:
		w.Header().Set("Allow", "PUT")
		w.Header().Add("Allow", "GET")
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// serveHttpSqlAPI starts an sql server with a GET/PUT API and listens.
func ServeHttpSqlAPI(port int, rdb *raftdb) {
	srv := http.Server{
		Addr:    ":" + strconv.Itoa(port),
		Handler: &httpSQLAPI{rdb},
	}
	if err := srv.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}
