#!/bin/bash
# Memory benchmark -- measures peak RSS for each library
# Usage: BENCH_DATABASE_URL="host=/tmp dbname=bench_db" ./mem/run_all.sh
#
# Requires PostgreSQL running with bench_users table populated.
# See setup/pg_setup.sql.

set -e

echo "Building release binaries..."
cargo build --release --bin mem_bsql_pg --bin mem_sqlx_pg --bin mem_diesel_pg 2>/dev/null

echo ""
echo "=== Peak RSS (bytes) ==="

echo ""
echo "bsql:"
/usr/bin/time -l ./target/release/mem_bsql_pg 2>&1 | grep "maximum resident"

echo ""
echo "sqlx:"
/usr/bin/time -l ./target/release/mem_sqlx_pg 2>&1 | grep "maximum resident"

echo ""
echo "diesel:"
/usr/bin/time -l ./target/release/mem_diesel_pg 2>&1 | grep "maximum resident"

echo ""
echo "C:"
/usr/bin/time -l ./c/pg_bench 2>&1 | grep "maximum resident"

echo ""
echo "Go:"
/usr/bin/time -l go run ./go/pg/ 2>&1 | grep "maximum resident"
