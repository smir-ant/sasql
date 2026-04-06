# CREDO

This is the constitution of bsql. Not a spec. Not a wishlist. These are the non-negotiable principles that govern every design decision, every line of code, every dependency choice. If a proposed change violates a principle, the change loses.

---

### 1. If it compiles, the SQL is correct.

This is the load-bearing promise. Everything else in this document exists to serve it.

Every SQL string passed to `bsql::query!` is validated against a real PostgreSQL instance at compile time. Column names, table names, parameter types, nullability, return types --- all verified against `pg_catalog`. The binary that `cargo build` produces contains zero unverified SQL. Not "mostly verified." Not "verified if you used the right function." Zero.

**In practice:**
- Every column typo is a compile error. Every type mismatch is a compile error. Every nullable column is `Option<T>` --- no exceptions, no overrides.
- If the schema changes and a query becomes invalid, the next `cargo build` fails. Not the next deployment. Not the 3 AM page. The build.
- Offline mode validates against a cached schema snapshot. The snapshot is generated from a live database. There is no mode where SQL goes unchecked.

---

### 2. Checked by default. Escape hatch for the rare exception.

Every query through `query!` is validated at compile time. There is no `query()` function. The checked path is the only path for application queries.

For the rare cases that `query!` cannot express --- DDL (`CREATE INDEX CONCURRENTLY`), dynamic table names, migrations --- `Pool::raw_query()` and `Pool::raw_execute()` exist. They bypass compile-time validation entirely. They use PostgreSQL's simple query protocol (text, no binary, no prepared statements). They are explicitly documented as unsafe-for-SQL and should never be used with user input.

**In practice:**
- 99%+ of queries go through `query!` --- fully validated, type-safe, zero risk.
- `raw_query`/`raw_execute` exist for DDL, ad-hoc admin queries, and migrations.
- The crate does not export any function that takes `&str` SQL *and* returns typed results. `raw_query` returns `Vec<RawRow>` (text values) --- the type system reminds you this is unvalidated.
- If you find yourself using `raw_query` for SELECT queries, reconsider. `query!` probably handles it.

---

### 3. SQL is the language.

There is no DSL. No `.filter()`. No `.select()`. No query builder. You write PostgreSQL SQL --- CTEs, window functions, `LATERAL` joins, `DISTINCT ON`, `ANY()`, `unnest()` --- and the macro validates it. If you know PostgreSQL, you know bsql.

DSLs inevitably diverge from the SQL they model. They cannot express the full power of PostgreSQL without escape hatches. bsql avoids this problem by not having a DSL at all.

**In practice:**
- The macro is a validator and code generator. Not a query language.
- Complex queries (recursive CTEs, window partitions, subqueries) work on day one. No DSL extensions needed.
- The learning curve is PostgreSQL's, not bsql's. PostgreSQL's documentation is bsql's documentation for query syntax.

---

### 4. Every nanosecond matters.

Not because users notice nanoseconds. Because the mindset that says "nanoseconds don't matter" produces millisecond-level bloat through a thousand "doesn't matter" decisions. bsql fights for every cycle.

**In practice (implemented in v0.17):**
- **Zero-copy fetch**: `fetch()` returns borrowed `&str` fields pointing directly into the wire response buffer. No `String::to_owned()`. No heap allocation per text column.
- **Binary protocol**: PostgreSQL's binary wire format. `i32` is `i32::from_be_bytes()` --- one instruction. Timestamps are 8-byte memcpy.
- **SIMD UTF-8 validation**: `simdutf8` for bulk string validation.
- **Pipelining**: `INSERT` batch sends N Bind+Execute messages in one round-trip. 2.5x faster than C's per-query approach.
- **Thread-local buffer recycling**: response buffers and arenas recycled via thread-local pools. Zero malloc on the hot query path.
- **Monolithic execute path**: entire send+receive inlined in one function. No abstraction layers, no virtual dispatch.
- **Statement cache**: Vec-based O(n) cache with u64 hash keys. Faster than HashMap for < 30 entries. BindTemplate with `encode_at` for parameter patching without rebuild.

---

### 5. Allocator-agnostic. System allocator works fine.

bsql's allocation profile is already minimal: zero-copy fetch, thread-local buffer recycling, arena for streaming. The system allocator is sufficient. Peak RSS: 1.70 MB for 10K queries --- 3.8x less than C (libpq).

If you want further improvement, mimalloc or jemalloc can help with multi-threaded workloads. But bsql does not require or recommend a specific allocator.

---

### 6. rapidhash for all internal hashing.

Prepared statement names, query cache keys, deduplication hashes, schema fingerprints --- every hash computation in bsql uses rapidhash. It is the fastest hash function for short-to-medium strings (which is what SQL text and identifier names are). Better throughput than FNV-1a, wyhash, ahash, and xxhash3 on inputs under 256 bytes. Better distribution than FNV-1a. Zero cryptographic pretensions --- it is a hash for hash maps and identifiers, not for security.

**In practice:**
- Prepared statement naming: `s_{rapidhash(sql_text):016x}`. 64-bit hash, hex-encoded. Collision probability: ~5.4 * 10^-20 per pair. Negligible.
- The `rapidhash` crate is ~200 lines, no dependencies, no unsafe beyond the core hash function. Cost justified.
- FNV-1a (mentioned in early spec drafts) is replaced everywhere. rapidhash is strictly superior for our input profile.

---

### 7. bitcode for all persistence and serialization.

The offline schema cache (`.bsql/`), incremental validation state, any data that bsql writes to disk --- all serialized with bitcode. Not JSON. Not CBOR. Not MessagePack. Not bincode.

bitcode is the fastest and most compact binary serialization format in the Rust ecosystem. ~10x faster than serde_json for serialize/deserialize. ~3x more compact than JSON. ~1.5x more compact than bincode. Zero-copy deserialization support. The compile-time cache is read on every `cargo build` --- speed here directly reduces developer iteration time.

**In practice** *(implemented in v0.4)*:
- `.bsql/schema.bitcode` and `.bsql/queries.bitcode` replace the JSON files from the spec.
- `bsql prepare` writes bitcode. The proc macro reads bitcode. No JSON parsing during compilation.
- Schema cache for 50 queries loads in ~100us (bitcode) vs ~5ms (JSON). 50x improvement on every build.

---

### 8. Zero allocations where possible.

`fetch()` returns borrowed rows with `&str` fields. No `String` heap allocation per text column. Data lives in a response buffer owned by `BsqlRows` --- when `BsqlRows` drops, one deallocation frees everything.

`fetch_all()` exists for cases where owned `String` fields are needed (sending to another thread, storing long-term). But `fetch()` is the default and the fast path.

**In practice (implemented in v0.17):**
- `fetch()` on 10K rows: zero String allocations. All text fields are `&str` borrowed from the wire buffer.
- Response buffer recycled via thread-local pool. Second query on the same connection: zero malloc.
- Arena used only for streaming queries (chunk-based fetch). Regular queries bypass arena entirely.
- `fetch_one()` returns `BsqlSingleRef` --- access row via `.get()`. Data borrowed, not owned.

---

### 9. Dependencies are liabilities.

Every crate in `Cargo.toml` is an attack surface, a compile-time cost, a version conflict, and a maintenance burden. Dependencies are not badges of honor. They are debt.

**In practice:**
- Core runtime has 5 dependencies. Count them. Keep them minimal.
- Every dependency has `default-features = false`. Only the features actually used are enabled.
- Before adding a crate: can the standard library do it? Can we write 50 lines instead of pulling in 50,000? Is the crate maintained? When was the last commit? How many transitive dependencies does it bring?
- Periodic dependency audit. If something can be removed, it is removed.

---

### 10. Dynamic does not mean unchecked.

Optional clauses (`[AND col = $param: Option<T>]`) expand to 2^N SQL variants at compile time. Each variant is a complete, valid SQL statement. Each variant is independently validated against PostgreSQL via `PREPARE`. Each variant gets its own prepared statement at runtime.

The runtime dispatcher is a `match` on a bitflag --- one arm per combination, each arm pointing to a pre-validated query. No string concatenation. No SQL injection surface. No runtime parsing.

**In practice:**
- 4 optional filters = 16 variants. All 16 validated at compile time. All 16 prepared on first use.
- The variant selector is `match (a.is_some(), b.is_some(), c.is_some(), d.is_some())`. Compiles to a jump table. < 5ns dispatch overhead.
- Sort enums (`$[sort: EnumType]`) are spliced and validated per variant. The enum is exhaustive --- no default case, no fallback, no "unknown" sort.

---

### 11. Inline is king.

The query lives where it is used. In the function that calls it. Not in a `.sql` file across the repository. Not in a `queries/` directory. Not in a generated bindings file. Right here, in the Rust code, next to the business logic that needs the data.

**In practice:**
- `bsql::query! { SELECT ... }` in the handler function. The SQL is visible without file-hopping.
- IDE support: rust-analyzer expands the macro, autocompletes field names, shows types. The query is not a second-class citizen hidden in a separate file.
- Code review: the reviewer sees the SQL and the Rust that uses it in the same diff hunk.

---

### 12. Async by default. Sync when you need speed.

The public API is async --- `.fetch(&pool).await?`. This integrates naturally with tokio, actix-web, axum, and the Rust async ecosystem.

Under the hood, all database I/O is synchronous (blocking). The async wrapper resolves instantly --- zero async overhead. This gives the best of both worlds: async ergonomics for web servers, sync performance for the wire protocol.

For CLI tools, batch jobs, or latency-critical code paths: `bsql = { default-features = false, features = ["sync"] }` removes tokio entirely. Same `query!` macro, same zero-copy fetch, just `fn` instead of `async fn`.

**In practice (v0.18):**
- Default: `#[tokio::main]` + `.await` on all methods
- `feature = "sync"`: `fn main()`, no tokio, no `.await`
- Internal Connection: always sync (proven faster than async I/O for PG wire protocol)
- Pool: LIFO ordering, Condvar-based wait, std::thread for background tasks

---

### 13. Errors are first-class citizens.

A confusing error message is a bug. Error quality is a feature, not a nice-to-have.

**In practice:**
- Column typos: `"column 'naem' not found --- did you mean 'name'?"` with Levenshtein suggestions.
- Type mismatches: `"expected i32 (column tickets.id is INTEGER NOT NULL), found &str"` with exact span pointing.
- Missing tables: `"table 'tcikets' not found --- did you mean 'tickets'?"` with available tables listed.
- Optional clause errors: points to the specific clause and explains why the variant is invalid.
- All errors point to the exact token in the source file. Not the macro invocation. The token.
- `BsqlError` at runtime has variants: `Pool`, `Query`, `Decode`, `Connect`. Pattern matching, not string parsing.

---

### 14. Doc-tests are the contract.

Every public API has a doc-test or a `rust,ignore` example (for `query!` which requires a live database at compile time). If the doc-test is wrong, the API is wrong.

**In practice:**
- `lib.rs` doc examples use `rust,ignore` for `query!` (requires PG at compile time). Non-macro API examples compile and run.
- The `.bsql/` query cache is committed to the repository. Cloned repos auto-fallback to this cache for offline builds.
- 1801 tests (unit + integration) cover all public API paths. Doc examples are supplementary, not the sole test source.

---

### 15. No blind spots.

Every nullable column is `Option<T>`. Every parameter type is verified against `pg_catalog`. Every column in `SELECT *` is resolved to concrete types. Every cast is checked. Every function return type is looked up. No silent failures. No implicit conversions. No "it'll probably work."

**In practice:**
- `SELECT *` resolves to explicit columns at compile time. If a column is added to the table, the generated struct gains a field on the next build.
- `status::text` --- the cast target `text` is verified. `status::bogus` is a compile error.
- `COUNT(*)` returns `i64`, not "some integer." `SUM(nullable_col)` returns `Option<T>`, because it is nullable even if the column is NOT NULL (empty result set).
- Aggregate functions over nullable columns are `Option<Option<T>>`: outer Option for empty result, inner for NULL values. No, this is not over-engineered. This is correct.

---

### 16. Total query knowledge is a superpower.

bsql sees every `query!` the application executes --- at compile time and at runtime. `raw_query`/`raw_execute` are the exception, not the rule (DDL/migrations only). This near-complete visibility enables: singleflight coalescing, read/write splitting, statement cache optimization, and pipeline batching. The more queries go through `query!`, the more bsql can optimize.

---

### 17. Fail fast. Never wait and hope.

Timeouts are an admission of helplessness: "I don't know how long this will take, so I'll just cut it off." bsql does not wait and hope.

**Fail-fast by default, configurable wait:**
- Pool exhausted → immediate `PoolExhausted` error by default. Configurable `acquire_timeout` for burst tolerance (e.g., wait up to 50ms before failing). The caller decides the strategy.
- Dropped transaction without commit/rollback → connection discarded from pool, warning logged. The next pool user gets a clean connection.

**Where timeout is unavoidable** (external systems we do not control):
- TCP connect to PostgreSQL --- the network may be down. `connect_timeout` exists because TCP itself will wait forever. This is the only legitimate timeout in bsql.
- PostgreSQL's own `statement_timeout` --- set via `session_init`, enforced by PG, not by bsql. If a query takes too long, PG kills it. bsql reports the error.

Timeouts are not a design pattern. They are a last resort for external boundaries. Inside bsql's own code, every operation either succeeds, fails immediately, or is bounded by a resource it controls.

