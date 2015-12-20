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

package main

import (
	"flag"
	"fmt"
	"github.com/chzchzchz/raftsql"
	"strings"
)

func main() {
	cluster := flag.String("cluster", "http://127.0.0.1:9021", "comma separated cluster peers")
	id := flag.Int("id", 1, "node ID")
	port := flag.Int("port", 9121, "sql server port")
	flag.Parse()

	proposeC := make(chan string)
	defer close(proposeC)

	// raft provides a commit stream for the proposals from the http api
	rp := raftsql.NewRaftPipe(*id, strings.Split(*cluster, ","), proposeC)

	// the key-value http handler will propose updates to raft
	raftsql.ServeHttpSqlAPI(*port, raftsql.NewDB(fmt.Sprintf("raftsql-%d.db", *id), rp))
}
