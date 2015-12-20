package raftsql

type raftPipe struct {
	ProposeC chan<- string
	CommitC  <-chan *string
	ErrorC   <-chan error
}

func NewRaftPipe(id int, peers []string, proposeC chan string) *raftPipe {
	cC, eC := newRaftNode(id, peers, proposeC)
	return &raftPipe{ProposeC: proposeC, CommitC: cC, ErrorC: eC}
}
