# raftsql

A SQLite database backed by raft.

## Building
```sh
make
```

## Usage

Start a cluster:
```sh
goreman start
```

Manipulate the database:
```sh
curl -L http://localhost:22380 -L -XPUT -d "CREATE TABLE main.t (name text)"
curl -L http://localhost:32380 -L -XPUT -d "INSERT INTO main.t (name) VALUES (\"abc\")"
```

Query the database:
```sh
curl -L http://localhost:32380 -L -XGET -d "SELECT * FROM main.t"
```