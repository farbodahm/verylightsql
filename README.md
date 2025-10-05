# VeryLightSQL (VLsql)

VeryLightSql is a toy database written in Go while reverse-engineering the ideas behind SQLite.
It follows along with the guides at https://cstack.github.io/db_tutorial/ and https://www.databass.dev/ and reimplements the core pieces from scratch for learning purposes.

- A single-table, fixed-schema row store persisted to disk
- Minimal SQL-style REPL that supports `insert` and `select`
- On-disk paging, row serialization, and a tiny byte-addressed pager

## Prerequisites

- Go 1.22 or newer (see `go.mod`)

## Build

```sh
go build -o verylightsql
```

This produces a standalone binary (`verylightsql`) in the current directory.

## Run

Execute the compiled binary:

```sh
./verylightsql vlsql.db
```

### Interactive commands

- SQL-like statements: `insert <id> <username> <email>`, `select`
- Meta commands (start with a dot): `.help`, `.exit`

Example session:

```text
$ go run .
Verylightsql v0.1.0
Opening database: vlsql.db
> insert 1 alice alice@example.com
Executed.
> select
(1, alice, alice@example.com)
Executed.
> .exit
Bye!
```

Rows are serialized to fixed-size pages on disk, so data persists between runs.

## Tests

Run all tests with:

```sh
go test -v -tags=integration ./...
```

The integration test exercises inserting/selecting rows through the REPL in-process to catch regression bugs.

## Motivation

This project exists as a hands-on playground to understand how SQLite keeps tables on disk, encodes rows, and serves queries. The implementation is intentionally lightweight, favoring clarity over performance, and mirrors the progression taught in the tutorials referenced above.
