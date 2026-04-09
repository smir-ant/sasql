# Test Coverage Specification

What must be tested. Every item is a scenario that must have a passing test.
Organized by behavior category, not by backend.

## 1. Query Execution

### 1.1 fetch_all
- Returns Vec with correct rows
- Empty result → empty Vec
- Multiple rows returned correctly

### 1.2 fetch_one
- Returns single row
- 0 rows → Err
- 2+ rows → Err

### 1.3 fetch_optional
- Found → Some(row)
- Not found → None
- 2+ rows → Err

### 1.4 execute
- Returns affected count
- Affected = 0 (no match)
- Affected = 1
- Affected > 1 (batch update/delete)
- Via Pool
- Via Transaction
- Via PoolConnection

### 1.5 for_each (zero-alloc)
- Iterates all rows with borrowed fields
- Empty result → 0 iterations
- for_each_map collects results into Vec

### 1.6 fetch_stream (PG only)
- Streams rows one by one
- With parameters
- Drop mid-stream → connection discarded
- Fully consumed → connection returned to pool

### 1.7 query_as!
- Maps to target struct
- Nullable column → Option field
- fetch_all variant

## 2. Parameters

### 2.1 Scalar types
- i32, i64, bool, f32, f64
- &str / String
- bytea / BLOB

### 2.2 Auto-deref
- String variable → &str param
- Vec<T> variable → &[T] param

### 2.3 Option<T> (nullable params)
- None → SQL NULL
- Some(v) → value
- Option<i32> on integer column
- Option<&str> on text column

### 2.4 Arrays (PG only)
- Vec<i32> in ANY()
- Vec<String> / &[String]
- Empty array → 0 matches

### 2.5 Edge cases
- Empty string param (not NULL)
- NULL vs empty string distinction

## 3. Type System & Nullability

### 3.1 Column nullability
- NOT NULL column → T
- Nullable column → Option<T>
- LEFT JOIN columns → all Option<T>
- Cast (col::text) inherits NOT NULL from source

### 3.2 NOT NULL inference (computed expressions)
- COUNT(*) → i64 (not Option)
- ROW_NUMBER() OVER → i64 (not Option)
- SUM() on empty group → Option (correctly nullable)
- COALESCE(col, literal) → T
- CASE WHEN ... THEN literal ELSE literal END → T
- NOW(), CURRENT_TIMESTAMP → T

### 3.3 PG-specific types
- PG enum → String (without ::text cast)
- PG enum in JOIN context
- PG enum in subquery context
- JSONB auto-cast (&str → jsonb)
- JSON auto-cast (&str → json)
- Invalid JSON → PG error (not panic)
- UUID (feature-gated)
- Timestamp/Date/Time (feature-gated)
- Decimal (feature-gated)
- Array columns (int[], text[])

## 4. SQL Constructs

- Simple SELECT with WHERE
- INSERT with RETURNING
- UPDATE with WHERE
- DELETE with WHERE
- JOIN (INNER)
- LEFT JOIN
- Subquery in FROM
- CTE (WITH clause)
- UNION ALL
- GROUP BY + aggregate (COUNT, SUM)
- Window function (ROW_NUMBER OVER)
- ORDER BY + LIMIT
- Dynamic queries with optional clauses (PG)
- Sort enums (PG)

## 5. Transactions

- Begin + commit → persists
- Begin + rollback → discards
- Drop without commit → auto-rollback
- Execute in transaction → returns affected
- Error inside tx → aborted state
- Query after tx error → also fails
- Rollback after error → recovers
- Deferred pipeline + flush (PG)
- Savepoint + rollback_to (PG)
- Nested savepoints (PG)
- Isolation levels (PG)
- Independent transactions isolated (PG)

## 6. Error Handling

### 6.1 Compile-time errors
- Invalid table name
- Invalid column name
- Type mismatch (multiple variants)
- Parameter missing type annotation
- Parameter conflicting types
- DDL rejected (CREATE/DROP/ALTER)
- DELETE without WHERE
- UPDATE without WHERE
- SQL injection attempt
- Empty/whitespace-only SQL
- Multiple statements
- Missing feature flag

### 6.2 Runtime errors
- Unique constraint violation
- FK constraint violation
- Connection refused / wrong port
- Server disconnect mid-query
- Statement timeout
- Query after connection lost
- Invalid JSON → PG error

### 6.3 Error API
- QueryError constructable from user code
- QueryError with source
- is_unique_violation()
- is_foreign_key_violation()
- pg_code() accessor

## 7. Pool & Connection

### 7.1 Connection lifecycle
- Pool connect / open
- Pool acquire + use + release
- Multiple sequential reads

### 7.2 Pool limits
- Concurrent acquire up to max_size
- Acquire timeout when exhausted
- max_lifetime → connection replaced
- stale_timeout → idle evicted

### 7.3 Recovery
- Pool recovers after backend terminated
- Connection usable after statement timeout
- Pool status invariants under concurrent load

### 7.4 Configuration
- Builder with all options
- URL parsing (all params)
- pgbouncer unnamed statements (PG)
- Statement cache disabled (PG)
- In-memory isolation (SQLite)

## 8. Advanced Features

### 8.1 PG-specific
- LISTEN/NOTIFY
- Singleflight query coalescing
- Read/write split
- COPY protocol
- TLS with custom CA
- raw_query_params (dynamic SQL with bind)

### 8.2 CLI
- migrate --check
- check --verify-cache
- bsql clean

### 8.3 Macros
- #[bsql::pg_enum]
- #[bsql::test] (schema isolation)
- #[bsql::sort]

## 9. Performance Contracts

- for_each: zero heap allocations on hot path
- fetch_stream: constant memory for large result sets
- Statement cache: cache hit avoids Parse roundtrip
- Bind template: patching avoids Bind rebuild
- Thread-local buffer recycling
