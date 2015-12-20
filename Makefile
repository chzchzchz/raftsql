.PHONY: all
all: raftsql

SRCS=$(filter-out %_test.go, $(wildcard *.go */*.go))
TESTSRCS=$(wildcard *_test.go */*_test.go) 

raftsql: $(SRCS)
	cd server && go build -o ../$@

.PHONY: test
test: test.out
	cat test.out

test.out: raftsql $(TESTSRCS)
	go test -v >$@ 2>&1 || cat $@

.PHONY: vet
vet: vet.out
	cat vet.out

vet.out: raftsql
	go tool vet -v ./raftsql >$@ 2>&1 || cat $@