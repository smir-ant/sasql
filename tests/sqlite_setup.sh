#!/bin/bash
# Creates the SQLite test database for compile-time query! validation.
# Run this BEFORE `cargo test --features sqlite-bundled`.

set -e

DB_PATH="${1:-/tmp/bsql_test.db}"

rm -f "$DB_PATH"

sqlite3 "$DB_PATH" <<'SQL'
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT,
    score INTEGER,
    active INTEGER NOT NULL DEFAULT 1
);

INSERT INTO users (name, email, score) VALUES ('alice', 'a@test.com', 42);
INSERT INTO users (name, email, score) VALUES ('bob', NULL, NULL);

CREATE TABLE items (
    id INTEGER PRIMARY KEY,
    title TEXT NOT NULL,
    description TEXT,
    owner_id INTEGER NOT NULL REFERENCES users(id),
    data TEXT
);

INSERT INTO items (title, owner_id) VALUES ('Item 1', 1);
INSERT INTO items (title, description, owner_id) VALUES ('Item 2', 'desc', 2);
SQL

echo "SQLite test database created at $DB_PATH"
echo "Set BSQL_DATABASE_URL=sqlite://$DB_PATH before running tests"
