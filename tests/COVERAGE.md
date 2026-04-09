# Test Coverage Specification

Every item is a scenario that must have a passing test.
Serves as test instruction for any backend (PG, SQLite, future MySQL).

---

## 1. fetch_all

### Happy path
- 0 rows → empty Vec
- 1 row → Vec with 1 element
- 2 rows → correct order preserved
- 100+ rows → all returned
- Multiple column types in one query (int + text + bool + nullable)
- Same query executed twice → same results (idempotent)

### Bad path
- Nonexistent table → compile error
- Nonexistent column → compile error
- Syntax error in SQL → compile error

### Edge cases
- All rows have NULL in nullable column
- Mix of NULL and non-NULL in same column
- Column name with underscore, digit
- Duplicate column names (SELECT a.id, b.id)
- Very long TEXT value (10KB+)
- Empty string vs NULL in same result set
- Unicode / emoji in text values (🎉, кириллица, 中文)

---

## 2. fetch_one

### Happy path
- Exactly 1 row → returns Row
- Row with all field types populated

### Bad path
- 0 rows → Err with clear message
- 2+ rows → Err with clear message
- Wrong column type in struct (query_as!) → compile error

### Edge cases
- fetch_one on a table with millions of rows + WHERE on PK (must use index, not scan)
- fetch_one with NULL in every nullable column

---

## 3. fetch_optional

### Happy path
- Found → Some(row)
- Not found → None

### Bad path
- 2+ rows → Err (not silently returns first)

### Edge cases
- fetch_optional on empty table → None
- fetch_optional with WHERE that sometimes matches, sometimes doesn't

---

## 4. execute

### Happy path
- INSERT → affected = 1
- UPDATE matching 1 row → affected = 1
- UPDATE matching 0 rows → affected = 0
- UPDATE matching N rows → affected = N
- DELETE matching 1 row → affected = 1
- DELETE matching 0 rows → affected = 0
- DELETE matching N rows → affected = N

### Via different targets
- execute via Pool
- execute via Transaction
- execute via PoolConnection (PG)

### Bad path
- INSERT violating UNIQUE → Err
- INSERT violating FK → Err
- INSERT violating NOT NULL → Err
- INSERT violating CHECK constraint → Err
- UPDATE with invalid value → Err
- DELETE on nonexistent table → compile error

### Edge cases
- execute on SELECT (what happens?)
- INSERT with RETURNING (should use fetch_one, not execute)
- Batch INSERT of 100+ rows via loop

---

## 5. for_each (zero-alloc iteration)

### Happy path
- Iterates all rows in order
- Borrowed fields (&str) accessible in closure
- Closure can accumulate state (counter, sum)
- for_each_map collects into Vec

### Bad path
- Closure returns Err → iteration stops, error propagated
- Empty result → closure never called

### Edge cases
- for_each on 10,000 rows (memory should stay constant)
- for_each with NULL values in borrowed fields
- Nested for_each (query inside for_each closure)

---

## 6. fetch_stream (PG)

### Happy path
- Streams rows one by one via next()
- All rows received in order
- With bind parameters
- Fully consumed → connection returned to pool

### Bad path
- Drop stream mid-iteration → connection discarded (not returned)
- Server error mid-stream → Err on next()

### Edge cases
- Stream 0 rows → first next() returns None
- Stream 1 row → one Some, then None
- Stream 100,000 rows → constant memory

---

## 7. query_as!

### Happy path
- Maps to user struct with correct field types
- fetch_one, fetch_all, fetch_optional all work
- Struct with all common types (i32, String, bool, Option<T>)

### Bad path
- Struct field type doesn't match column → compile error
- Struct missing a field → compile error
- Struct has extra field → compile error
- Nullable column but struct field is non-Option → compile error (clear message)

### Edge cases
- Struct with Option<String> for nullable column
- Struct with renamed fields (AS alias in SQL)

---

## 8. Parameters — Scalar Types

### Happy path
- i32, i64, i16
- f32, f64
- bool
- &str, String
- u32 (OID)
- &[u8], Vec<u8> (bytea)

### Bad path
- Wrong type for column → compile error
- i32 for int8 column → compile error
- &str for int4 column → compile error

### Edge cases
- i32::MAX, i32::MIN
- i64::MAX, i64::MIN
- f64::INFINITY, f64::NEG_INFINITY, f64::NAN
- Empty string ""
- Very long string (100KB)
- Empty bytea (zero bytes)
- Large bytea (1MB)
- bool true and false explicitly

---

## 9. Parameters — Auto-deref

### Happy path
- String variable → &str param
- Vec<i32> variable → &[i32] param
- Vec<String> variable → &[String] param

### Edge cases
- &String (double reference) → &str
- Box<str> → &str (if Deref chain works)
- Cow<str> → &str

---

## 10. Parameters — Option<T> (nullable)

### Happy path
- Option<i32> = None → SQL NULL
- Option<i32> = Some(42) → 42
- Option<&str> = None → SQL NULL
- Option<&str> = Some("hello") → "hello"
- Option<bool> = None / Some
- Option<f64> = None / Some

### Bad path
- Option<i32> on NOT NULL column → should this work? (PG accepts, inserts NULL → constraint error at runtime)

### Edge cases
- Option<String> = Some("".to_owned()) → empty string, not NULL
- Repeated None/Some in same query
- Option param used twice in same SQL ($x: Option<i32> in WHERE and SET)

---

## 11. Parameters — Arrays (PG)

### Happy path
- Vec<i32> in ANY()
- Vec<String> / &[String] in ANY()
- &[&str] in ANY()
- Vec<i64>, Vec<bool>, Vec<f32>, Vec<f64>

### Bad path
- Wrong element type → compile error

### Edge cases
- Empty array → 0 matches
- Array with 1 element
- Array with 1000 elements
- Array with duplicate values
- Array of empty strings

---

## 12. Parameters — JSONB/JSON auto-cast (PG)

### Happy path
- &str → jsonb column (auto-cast)
- &str → json column (auto-cast)
- Valid JSON string round-trips correctly
- JSON with nested objects/arrays

### Bad path
- Invalid JSON string → PG error (not panic)
- Empty string → PG error (not valid JSON)

### Edge cases
- JSON with unicode
- JSON with very long string values
- JSON null literal ("null") vs SQL NULL
- JSONB operators in WHERE (->>, @>)

---

## 13. Parameters — unnest / array functions (PG)

### Happy path
- unnest(Vec<String>) → rows
- unnest(Vec<i32>) → rows
- ANY($array_param)

### Edge cases
- unnest empty array → 0 rows
- unnest with NULL elements in array

---

## 14. Nullability — Column inference

### Happy path
- NOT NULL column → T
- Nullable column → Option<T>
- LEFT JOIN right-side → Option<T>
- RIGHT JOIN left-side → Option<T>
- FULL JOIN both sides → Option<T>

### Edge cases
- INNER JOIN → preserves original nullability
- Multiple LEFT JOINs → all right tables nullable
- Self-join (same table twice)
- Column aliased (AS) preserves nullability
- SELECT * expands to correct nullability per column

---

## 15. Nullability — NOT NULL inference for expressions

### Happy path
- COUNT(*) → i64
- COUNT(column) → i64
- ROW_NUMBER() OVER → i64
- RANK(), DENSE_RANK(), NTILE() → not null
- NOW(), CURRENT_TIMESTAMP → not null
- COALESCE(nullable, literal) → not null
- EXISTS(subquery) → bool
- Literal values (42, 'text', true) → not null
- CASE WHEN ... THEN literal ELSE literal END → not null

### Correctly nullable (must stay Option)
- SUM() → Option (empty group returns NULL)
- AVG() → Option
- MAX() → Option
- MIN() → Option
- COALESCE(nullable, nullable) → Option (no literal fallback)
- LAG(), LEAD() → Option
- Function with unknown nullability → Option

### Edge cases
- Nested COALESCE
- CASE with mixed nullable/literal branches
- Arithmetic on NOT NULL columns (a + b) → should be not null but currently nullable
- String concatenation (a || b)

---

## 16. Nullability — Cast inference

### Happy path
- column::text on NOT NULL column → String (not Option)
- CAST(column AS integer) on NOT NULL column → i32

### Safety
- column::text on nullable column → Option<String>
- Ambiguous column name (two tables, same name) → stays Option
- Schema-qualified name (public.col::text) → stays Option (not matched)
- Complex expression cast (lower(name)::text) → not matched, stays Option

---

## 17. PG enum types

### Happy path
- Enum column → String
- Enum in simple SELECT
- Enum in JOIN
- Enum in subquery
- Enum in CTE

### Edge cases
- Enum with many variants (20+)
- Enum value comparison in WHERE

---

## 18. SQL Constructs

### Basic DML
- SELECT with WHERE
- INSERT single row
- INSERT with RETURNING
- UPDATE with WHERE
- DELETE with WHERE
- INSERT ... ON CONFLICT DO NOTHING
- INSERT ... ON CONFLICT DO UPDATE

### Joins
- INNER JOIN
- LEFT JOIN
- RIGHT JOIN (PG)
- CROSS JOIN
- Multiple JOINs in one query
- Self-join

### Subqueries & CTEs
- Subquery in FROM
- Subquery in WHERE (EXISTS, IN)
- CTE (WITH clause)
- Multiple CTEs
- Recursive CTE (PG)

### Set operations
- UNION ALL
- UNION (deduplicated)
- INTERSECT
- EXCEPT

### Aggregation
- GROUP BY single column
- GROUP BY multiple columns
- GROUP BY with HAVING
- COUNT, SUM, AVG, MAX, MIN
- COUNT DISTINCT
- Window function (ROW_NUMBER, RANK)
- Window with PARTITION BY

### Other
- ORDER BY + LIMIT + OFFSET
- LIKE / ILIKE with parameters
- BETWEEN with parameters
- IN list with parameters
- IS NULL / IS NOT NULL in WHERE
- COALESCE in SELECT
- CASE WHEN in SELECT
- String concatenation (||)
- Arithmetic expressions in SELECT

### Dynamic queries (PG)
- 1 optional clause (Some)
- 1 optional clause (None)
- 2 optional clauses (all combinations)
- 3 optional clauses
- Optional clause with ILIKE
- Optional clause + base required params
- Dynamic query via Transaction
- Dynamic query via PoolConnection

---

## 19. Transactions

### Lifecycle
- Begin + commit → data persists
- Begin + rollback → data discarded
- Drop without commit → auto-rollback
- Begin + no operations + commit → noop
- Begin + no operations + rollback → noop

### Operations in transaction
- execute → returns affected count
- fetch_one inside tx
- fetch_all inside tx
- Multiple operations in same tx

### Error recovery
- Error inside tx → aborted state
- Query after error → "current transaction is aborted"
- Rollback after error → succeeds, connection usable
- New transaction after rollback → works

### Isolation
- Two concurrent transactions don't see each other's uncommitted data
- Read your own writes within tx

### Savepoints (PG)
- Savepoint + release
- Savepoint + rollback_to
- Nested savepoints (A inside B)
- Rollback to inner savepoint preserves outer changes

### Deferred pipeline (PG)
- Defer 1 operation + commit → flushed
- Defer N operations + commit → all flushed
- Defer + explicit flush → returns counts
- Defer + rollback → discarded
- Deferred count tracking
- Auto-flush before read

### Isolation levels (PG)
- READ COMMITTED (default)
- SERIALIZABLE
- REPEATABLE READ

---

## 20. Error Handling — Compile-time

- Invalid table name → error with "available tables" hint
- Invalid column name → error
- Type mismatch: &str for int4 → error
- Type mismatch: i32 for int8 → error
- Type mismatch: i32 for text → error
- Type mismatch: bool for text → error
- Type mismatch: i64 for int4 → error
- Type mismatch: &str for int column → error
- Parameter missing type (bare $name) → error
- Parameter empty type ($name:) → error
- Parameter conflicting types ($x: i32 used with $x: &str) → error
- DDL: CREATE TABLE → rejected
- DDL: DROP TABLE → rejected
- DDL: ALTER TABLE → rejected
- DDL: TRUNCATE → rejected
- DDL: GRANT → rejected
- DELETE without WHERE → rejected
- UPDATE without WHERE → rejected
- SQL injection (semicolons, multiple statements) → rejected
- Empty SQL string → error
- Whitespace-only SQL → error
- Comment-only SQL → error
- CTE without DML → error
- Manual positional params ($1 instead of $name: type) → error
- Optional clause with non-Option type → error
- Too many optional clauses → error
- Nested brackets in optional clause → error
- Unclosed bracket in optional clause → error
- Missing feature flag for time/uuid/decimal → error with hint

---

## 21. Error Handling — Runtime

### Constraint violations
- UNIQUE constraint → Err + is_unique_violation() = true
- FK constraint → Err + is_foreign_key_violation() = true
- NOT NULL constraint → Err
- CHECK constraint → Err

### Connection errors
- Wrong host → Err
- Wrong port → Err
- Wrong password → Err
- Wrong database → Err
- Connection refused → Err
- Connection timeout → Err

### Query errors
- Statement timeout → Err with pg_code 57014
- Division by zero → Err
- Invalid JSON → Err
- Server disconnect mid-query → Err
- Query after disconnect → Err

### Error API
- QueryError constructable from user code (pub fields)
- QueryError with source (pub source)
- is_unique_violation() true/false
- is_foreign_key_violation() true/false
- pg_code() returns correct code
- Display format includes message
- Debug format includes all fields

---

## 22. Pool & Connection

### Lifecycle
- Pool::connect / SqlitePool::open → success
- Pool with invalid URL → Err
- Pool acquire → PoolConnection
- PoolConnection query → works
- PoolConnection drop → returned to pool

### Limits
- max_size: acquire up to max → success
- max_size: acquire beyond max → blocks/timeout
- acquire_timeout: fires when pool exhausted
- max_lifetime: old connections replaced
- stale_timeout: idle connections evicted
- min_idle: pool pre-warms connections

### Status
- status.max_size correct
- status.idle + status.active = status.open
- status under concurrent load maintains invariants

### Recovery
- Backend killed → next query errors
- Pool creates new connection after kill
- Connection usable after statement timeout (outside tx)

### Concurrency
- 8 threads × 50 queries → no panic, no deadlock
- Concurrent acquire/release cycles → pool stable
- Pool close while connections checked out → graceful

### Configuration
- Builder: all options at once
- Builder: minimal options
- URL parsing: host, port, user, password, database
- URL parsing: sslmode=disable/prefer/require
- URL parsing: statement_timeout
- URL parsing: statement_cache=disabled (PG)
- URL parsing: sslrootcert, sslcert, sslkey (PG)
- URL parsing: host=/tmp (Unix socket, PG)

---

## 23. Advanced — PG

### LISTEN/NOTIFY
- Listen on channel → receive notification
- Multiple channels
- Notification payload
- Large payload
- Listener drop → cleanup

### Singleflight
- Identical concurrent queries → single roundtrip
- Different queries → separate roundtrips
- Singleflight via PoolConnection → not coalesced
- Singleflight via Transaction → not coalesced

### Read/write split
- SELECT → routes to replica (when configured)
- INSERT/UPDATE/DELETE → routes to primary
- Transaction → routes to primary

### COPY protocol
- COPY IN (bulk insert)
- COPY OUT (bulk export)
- COPY IN empty → noop
- COPY IN invalid data → error

### pgbouncer compatibility
- Unnamed statements (statement_cache=disabled)
- Statement cache empty after queries
- Same SQL twice without cache → both succeed
- URL parsing statement_cache param

### TLS
- Connect with sslmode=require
- Custom CA certificate
- Client certificate (mTLS)
- Connect to non-TLS server with sslmode=prefer → fallback

### Dynamic SQL
- raw_query_params: SELECT with params → rows
- raw_query_params: INSERT with params → affected
- raw_query_params: invalid SQL → error
- raw_query_params: no params → works

---

## 24. Advanced — SQLite

### Isolation
- In-memory DBs are isolated (separate pools)
- WAL mode readers don't block writer

### Transactions
- BEGIN/COMMIT via simple_exec
- BEGIN/ROLLBACK via simple_exec
- Nested BEGIN (savepoints)

### Schema
- CREATE TABLE + INSERT + SELECT roundtrip
- execute_batch (multiple statements)

---

## 25. CLI

### migrate --check
- Valid migration → all queries pass
- Breaking migration → reports failed queries
- Unreachable host → timeout error
- No cache directory → error
- Empty cache → nothing to check

### check --verify-cache
- Fresh cache → all pass
- Stale cache (schema changed) → reports drift
- Semicolon in cached SQL → rejected (tampering)

### bsql clean
- Removes all .bitcode files
- Empty directory → prints 0 removed
- Non-existent directory → error

---

## 26. Macros

### #[bsql::pg_enum]
- Derives correct FromSql/ToSql
- Label validation against pg_catalog
- Display, Debug, Clone, Copy, PartialEq, Eq, Hash

### #[bsql::test]
- PG: creates schema, applies fixtures, cleanup on drop
- SQLite: creates temp file, cleanup on drop
- Unique schema names across parallel tests
- Panic cleanup (Drop guard)

### #[bsql::sort]
- Generates sql() method
- Validates each fragment via PREPARE
- Works in query! with $[sort: EnumType]

---

## 27. Encoding / Decoding Edge Cases

- UTF-8 text with multibyte characters (кириллица, 中文, 🎉)
- ASCII control characters in text
- NUL byte in bytea
- Very large row (many columns)
- Column with reserved SQL keyword name
- Table with reserved SQL keyword name
- Schema-qualified table name
- Timestamp at epoch (1970-01-01)
- Timestamp far in future (year 9999)
- Date before epoch
- Negative interval
- Numeric with many decimal places
- Numeric zero, negative zero
- Array with NULL elements
- Empty array
- Nested arrays (PG multidimensional)
