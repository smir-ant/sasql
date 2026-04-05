#!/bin/bash
# Fair PostgreSQL benchmark — per-runner warm-up + measure.
#
# Each runner warms up PG cache with its OWN queries immediately before
# measuring. This ensures every runner measures on equally hot cache,
# regardless of execution order.
#
# Pattern per runner: warm-up pass → CHECKPOINT → measure → cleanup
#
# Usage:
#   BENCH_DATABASE_URL="host=/tmp dbname=bench_db" \
#   BSQL_DATABASE_URL="postgres://user:pass@localhost/bench_db?host=/tmp" \
#   ./run_pg.sh

set -e

DB=${BENCH_DATABASE_URL:?"BENCH_DATABASE_URL must be set"}
BSQL=${BSQL_DATABASE_URL:?"BSQL_DATABASE_URL must be set"}

cleanup() {
    psql -h /tmp bench_db -c "DELETE FROM bench_users WHERE id > 10000; CHECKPOINT;" -q 2>/dev/null
}

echo "=== Reset database ==="
psql -h /tmp bench_db -f setup/pg_setup.sql -q 2>/dev/null
psql -h /tmp bench_db -c "GRANT ALL ON ALL TABLES IN SCHEMA public TO bsql; GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO bsql;" -q 2>/dev/null
echo ""

echo "=== C (libpq) ==="
BENCH_DATABASE_URL="$DB" ./c/pg_bench > /dev/null 2>&1; cleanup
BENCH_DATABASE_URL="$DB" ./c/pg_bench
cleanup
echo ""

echo "=== Go (pgx) ==="
(cd go && BENCH_DATABASE_URL="$DB" go run ./pg/) > /dev/null 2>&1; cleanup
(cd go && BENCH_DATABASE_URL="$DB" go run ./pg/)
cleanup
echo ""

echo "=== bsql (Rust) ==="
BENCH_DATABASE_URL="$BSQL" BSQL_DATABASE_URL="$BSQL" ./target/release/bench_bsql_perf > /dev/null 2>&1; cleanup
BENCH_DATABASE_URL="$BSQL" BSQL_DATABASE_URL="$BSQL" ./target/release/bench_bsql_perf
cleanup
echo ""

echo "=== diesel (Rust) ==="
BENCH_DATABASE_URL="$BSQL" ./target/release/bench_diesel_perf > /dev/null 2>&1; cleanup
BENCH_DATABASE_URL="$BSQL" ./target/release/bench_diesel_perf
cleanup
echo ""

echo "=== sqlx (Rust) ==="
BENCH_DATABASE_URL="$BSQL" ./target/release/bench_sqlx_perf > /dev/null 2>&1; cleanup
BENCH_DATABASE_URL="$BSQL" ./target/release/bench_sqlx_perf
cleanup
