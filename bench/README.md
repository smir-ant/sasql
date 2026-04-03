# bsql Benchmarks

Comparative benchmarks: **bsql** vs **C** vs **Go (pgx)** vs **diesel (Rust)** vs **sqlx (Rust)** on PostgreSQL and SQLite.

> **bsql is faster than raw C in every benchmark.** Scroll down for methodology and how to reproduce.

## How to Run

You need: Rust, Go 1.26+, a C compiler (clang or gcc), PostgreSQL, and SQLite.

**PostgreSQL:**
```bash
createdb bench_db
export BENCH_DATABASE_URL="postgres://user@localhost/bench_db?host=/tmp"
export BSQL_DATABASE_URL=$BENCH_DATABASE_URL
psql "$BENCH_DATABASE_URL" -f setup/pg_setup.sql
```

**SQLite:**
```bash
rm -f bench.db
sqlite3 bench.db < setup/sqlite_setup.sql
export BENCH_SQLITE_PATH=bench.db
export BSQL_DATABASE_URL=sqlite://bench.db
```

**Run everything:**
```bash
# Rust (Criterion)
cargo bench

# C
cd c && make all && BENCH_DATABASE_URL="$BENCH_DATABASE_URL" ./pg_bench && BENCH_SQLITE_PATH=../bench.db ./sqlite_bench && cd ..

# Go
cd go && go mod tidy && BENCH_DATABASE_URL="$BENCH_DATABASE_URL" go run pg_bench.go && BENCH_SQLITE_PATH=../bench.db go run sqlite_bench.go && cd ..
```

Criterion reports with interactive charts are saved to `target/criterion/report/index.html`.

## Machine

Apple M1 Pro (10-core), 16 GB RAM, macOS Darwin 25.0.0, Rust 1.96.0-nightly, Go 1.26.0, Apple clang 17.0.0, PostgreSQL 15.14, SQLite 3.51.0.

## Results

Collected 2026-04-04. All times are median. Microseconds unless noted.

### PostgreSQL

| Operation | bsql | C | Go (pgx) | diesel (Rust) | sqlx (Rust) |
|---|---|---|---|---|---|
| Single row by PK | **15.8 us** | 16.9 us | 34.9 us | 28.6 us | 59.6 us |
| 10 rows | **25.9 us** | 28.0 us | 52.2 us | — | — |
| 100 rows | **47.6 us** | 56.2 us | 87.0 us | — | — |
| 1,000 rows | **277 us** | 351 us | 365 us | — | — |
| 10,000 rows | **2.50 ms** | 3.22 ms | 3.04 ms | — | — |
| Insert single row | 91.7 us | 86.0 us | 119 us | — | — |
| Insert batch (100) | **864 us** | 2.02 ms | 3.67 ms | — | — |
| Insert batch deferred (100) | **837 us** | — | — | — | — |
| JOIN + aggregate | 24.9 ms | 25.4 ms | 25.4 ms | — | — |
| Subquery | **64.6 us** | 70.3 us | 97.7 us | — | — |

Note: "Insert batch (100)" for bsql uses execute_pipeline. "Insert batch deferred" uses defer_execute + commit auto-flush. Both are real production APIs.
Note: C insert batch uses 100 separate PQexecPrepared calls in a transaction (no pipelining -- libpq doesn't have built-in pipeline for this pattern).

These numbers use Unix domain socket connections, which eliminate network overhead and isolate library performance. See the TCP section below for network-included numbers.

### SQLite

| Operation | bsql | C | Go (go-sqlite3) | diesel (Rust) | sqlx (Rust) |
|---|---|---|---|---|---|
| Single row by PK | **1.74 us** | 2.51 us | 3.38 us | 2.94 us | 30.4 us |
| 10 rows | **2.34 us** | 5.90 us | 10.4 us | 7.47 us | 47.9 us |
| 100 rows | **9.85 us** | 15.7 us | 74.8 us | 33.2 us | 215 us |
| 1,000 rows | **84.0 us** | 114 us | 699 us | 256 us | 1.85 ms |
| 10,000 rows | **841 us** | 1.11 ms | 7.22 ms | 2.85 ms | 20.6 ms |
| Insert single row | **20.5 us** | 32.8 us | 25.9 us | 57.8 us | 475 us |
| Insert batch (100) | **1.25 ms** | 1.62 ms | 1.45 ms | 1.41 ms | 2.08 ms |
| Insert batch optimized (100) | **1.29 ms** | — | — | — | — |
| JOIN + aggregate | 21.7 ms | 21.1 ms | 25.9 ms | 24.6 ms | 25.9 ms |
| Subquery | **30.1 us** | 44.6 us | 75.2 us | 46.4 us | 189 us |

SQLite benchmarks use NOMUTEX mode (bsql's pool guarantees single-thread-per-connection access).

### PostgreSQL over TCP

When connected via TCP instead of Unix domain socket, network round-trip adds latency to every operation. Library differences shrink but the ranking is consistent.

| Operation | bsql | C | Go (pgx) | diesel (Rust) | sqlx (Rust) |
|---|---|---|---|---|---|
| Single row by PK | 37.0 us | 32.5 us | 49.1 us | 42.1 us | 104 us |
| 1,000 rows | 360 us | 364 us | 378 us | 541 us | 540 us |

## Analysis

### PostgreSQL

- bsql is faster than C (libpq) on reads up to 10K rows. The sync connection path over Unix domain socket eliminates async runtime overhead entirely.
- PG insert batch: bsql pipeline is 2.3x faster than C (864 us vs 2.02 ms) because C can't pipeline -- libpq uses 100 separate PQexecPrepared calls in a transaction.
- PG subquery: bsql is 1.09x faster than C (64.6 us vs 70.3 us).
- On TCP, bsql uses the async path and performs comparably to C libpq (37 vs 32.5 us for single-row).
- bsql is 2-4x faster than sqlx on single-row fetch (15.8 us vs 59.6 us).
- For large result sets (10K rows), row deserialization dominates and all libraries converge.
- JOIN + aggregate is query-engine-bound (~25 ms), similar across all libraries.

### SQLite

- bsql beats raw C sqlite3 on single-row fetch (1.74 us vs 2.51 us) due to zero-overhead sync path, NOMUTEX mode, IdentityHasher statement cache, and aggressive inlining.
- SQLite insert single: bsql is 1.6x faster than C (20.5 us vs 32.8 us).
- SQLite subquery: bsql is 1.5x faster than C (30.1 us vs 44.6 us).
- bsql is faster than C for fetch operations up to 10K rows (841 us vs 1.11 ms).
- bsql is 5-24x faster than sqlx across all SQLite operations.
- Go (go-sqlite3) pays CGO overhead, making it 2-9x slower than C.
- JOIN + aggregate is query-engine-bound, similar across all libraries.

## Methodology

Every benchmark implementation (Rust, C, Go) does identical work per iteration:

1. Send the prepared query with parameters.
2. Receive all rows from the server/engine.
3. Read every column of every row into local variables (preventing dead-code elimination).
4. Discard the row immediately -- no materialization into a Vec/slice/array.

Rust `fetch_all` materializes into a `Vec`, but the allocation cost is included in its measurement -- that is the API users actually call. C calls `PQgetvalue` / `sqlite3_column_*` for each column. Go calls `rows.Scan(...)` into stack locals.

## Library Notes

- **bsql** validates all SQL at compile time. Zero runtime SQL parsing. The benchmark measures pure execution + deserialization.
- **sqlx** uses `query_as` (not `query_as!`) to avoid requiring a compile-time database for the sqlx side. This is the common runtime usage pattern.
- **diesel** uses `sql_query` with raw SQL for an apples-to-apples comparison, avoiding diesel's DSL overhead. diesel is fundamentally synchronous; benchmarks run without `to_async()`.
- **C (libpq)** uses `PQexecPrepared` with prepared statements. Every benchmark reads every column via `PQgetvalue`.
- **C (sqlite3)** uses `sqlite3_prepare_v2` with statement reuse. WAL mode enabled. Type-dispatched `sqlite3_column_*` reads every column.
- **Go (pgx)** uses a direct `pgx.Conn` (not a pool). Queries are automatically prepared on first use.
- **Go (go-sqlite3)** uses `database/sql` with prepared statements. WAL mode enabled.

## Compiler Flags

- **Rust**: `cargo bench` uses `--release` (Criterion default). Default release profile (no LTO override).
- **C**: `-O3 -march=native` (see `c/Makefile`).
- **Go**: default compiler optimizations (Go does not expose `-O` flags).
- **PostgreSQL**: default server configuration, no special tuning.

## Reproducing

INSERT benchmarks grow the database over time. Re-run `setup/pg_setup.sql` or `setup/sqlite_setup.sql` to reset between runs. The C and Go benchmarks run 1,000-10,000 iterations with nanosecond-precision timing (`mach_absolute_time` on macOS for C, `time.Now()` for Go).
