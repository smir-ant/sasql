# sasql

- **100% compile-time SQL safety** — every query validated against a real PostgreSQL instance during `cargo build`
- **Zero escape hatches** — no `query()`, no `raw_sql()`, no backdoor. The unchecked path does not exist
- **Zero runtime SQL errors** — if the binary is produced, every query in it is correct
- **Zero unsafe code** — `#![forbid(unsafe_code)]` on every crate
- **Pure SQL** — write real PostgreSQL: CTEs, window functions, JOINs, subqueries. No DSL, no query builder
- **Fail-fast architecture** — pool never blocks, errors surface immediately, no timeouts as a design pattern

```rust
let id = 42i32;
let user = sasql::query!(
    "SELECT id, login, active FROM users WHERE id = $id: i32"
).fetch_one(&pool).await?;
// user.id: i32, user.login: String, user.active: bool
```

---

## Why sasql Exists

Every Rust SQL library lets you shoot yourself in the foot.

**sqlx** has `query()` — an unchecked runtime function sitting right next to the safe `query!()` macro. Same module, same import. One slip past code review, and your "compile-time safe" codebase has a runtime SQL error waiting in production.

**Diesel** has `sql_query()` — accepts arbitrary SQL strings with no validation. Every Diesel project that outgrows the DSL eventually calls it. The moment it does, the safety guarantee is void.

**SeaORM** has no compile-time SQL checking at all. Errors surface when queries hit PostgreSQL, not when code compiles.

**Cornucopia** is 100% safe but forces SQL into separate `.sql` files. No dynamic queries. Constant file-hopping between Rust and SQL.

sasql has no `query()`. No `raw_sql()`. No `sql_query()`. No backdoor. The function does not exist. If you need unchecked SQL, use `tokio-postgres` directly — sasql will not become the thing it replaces.

One unchecked query in a codebase of 500 checked queries breaks the guarantee. The guarantee is binary: **100% or meaningless.**

## Core Guarantees

| Guarantee | How |
|-----------|-----|
| **Every query is valid SQL** | `PREPARE` against live PostgreSQL at compile time |
| **Every column exists** | Verified against `pg_catalog` schema |
| **Every type is correct** | Parameter types checked against PG expectations |
| **Every nullable column is `Option<T>`** | Introspected from `pg_attribute.attnotnull` |
| **No unchecked SQL path exists** | Only `query!()` macro exported. No runtime SQL functions. |
| **Zero unsafe code** | `#![forbid(unsafe_code)]` on every crate |
| **Pool never blocks** | Fail-fast: immediate error on exhaustion, no timeouts |

## Quick Start

```toml
[dependencies]
sasql = { version = "0.2", features = ["time", "uuid"] }
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
```

```bash
export SASQL_DATABASE_URL="postgres://user:pass@localhost/mydb"
```

```rust
use sasql::Pool;

#[tokio::main]
async fn main() -> Result<(), sasql::SasqlError> {
    let pool = Pool::connect("postgres://user:pass@localhost/mydb").await?;

    // SELECT — compile-time validated, typed result
    let id = 1i32;
    let user = sasql::query!(
        "SELECT id, login, first_name FROM users WHERE id = $id: i32"
    ).fetch_one(&pool).await?;
    println!("{} ({})", user.first_name, user.login);

    // INSERT with RETURNING
    let title = "Fix the bug";
    let creator = 1i32;
    let ticket = sasql::query!(
        "INSERT INTO tickets (title, created_by_user_id)
         VALUES ($title: &str, $creator: i32)
         RETURNING id"
    ).fetch_one(&pool).await?;
    println!("Created ticket #{}", ticket.id);

    Ok(())
}
```

## Feature-Gated Types

Zero default features. Each type is opt-in.

| Feature | PostgreSQL | Rust |
|---------|-----------|------|
| `time` | TIMESTAMPTZ, TIMESTAMP, DATE, TIME | `time::OffsetDateTime`, `PrimitiveDateTime`, `Date`, `Time` |
| `chrono` | TIMESTAMPTZ, TIMESTAMP, DATE, TIME | `chrono::DateTime<Utc>`, `NaiveDateTime`, `NaiveDate`, `NaiveTime` |
| `uuid` | UUID | `uuid::Uuid` |
| `decimal` | NUMERIC / DECIMAL | `rust_decimal::Decimal` |

If a column requires a feature you haven't enabled, you get a clear compile error telling you which feature to add.

## PostgreSQL Enums

```rust
#[sasql::pg_enum]
enum TicketStatus {
    #[sql("new")]       New,
    #[sql("in_progress")] InProgress,
    #[sql("resolved")]  Resolved,
    #[sql("closed")]    Closed,
}
```

Type-safe mapping between PG enums and Rust enums. `accepts()` checks the specific PG type name — a `TicketStatus` will not silently accept a different enum type.

## How It Works

```
cargo build
    |
    v
sasql::query!("SELECT ...")
    |
    +-- Parse: extract $name: Type parameters
    +-- Connect to PostgreSQL (shared across all macros in one build)
    +-- PREPARE: validate SQL syntax, tables, columns
    +-- Introspect pg_catalog: column types, nullability
    +-- Check parameter types match schema
    +-- Generate: typed result struct + executor methods
    |
    v
Compiled binary with zero unverified SQL
```

## Base Type Mapping

| PostgreSQL | Rust | Nullable |
|-----------|------|----------|
| `BOOLEAN` | `bool` | `Option<bool>` |
| `SMALLINT` | `i16` | `Option<i16>` |
| `INTEGER` | `i32` | `Option<i32>` |
| `BIGINT` | `i64` | `Option<i64>` |
| `REAL` | `f32` | `Option<f32>` |
| `DOUBLE PRECISION` | `f64` | `Option<f64>` |
| `TEXT` / `VARCHAR` | `String` | `Option<String>` |
| `BYTEA` | `Vec<u8>` | `Option<Vec<u8>>` |
| `INTEGER[]` | `Vec<i32>` | (all array types supported) |

## Execution Methods

| Method | Returns | Errors when |
|--------|---------|------------|
| `.fetch_one(&pool)` | `T` | 0 rows or 2+ rows |
| `.fetch_all(&pool)` | `Vec<T>` | never |
| `.fetch_optional(&pool)` | `Option<T>` | 2+ rows |
| `.execute(&pool)` | `u64` | never |

## What sasql Is Not

- **Not an ORM.** No `User::find(42)`, no `user.save()`, no `belongs_to`.
- **Not a query builder.** No `.filter()`, `.select()`, `.join()`. Write SQL.
- **Not database-agnostic.** PostgreSQL. Period.
- **Not a migration tool.** Use whatever migration tool you prefer.

## Roadmap

- **v0.1** — Foundation: `query!` macro, compile-time validation, base types, pool
- **v0.2** — Full type system (current): `time`, `chrono`, `uuid`, `decimal`, custom PG enums, CI
- **v0.3** — Dynamic queries: `[optional clauses]`, sort enums, 2^N compile-time expansion
- **v0.4** — Offline mode: validate without a live database (`sasql prepare` + bitcode cache)
- **v0.5** — Transactions: `pool.begin()`, commit, rollback, drop-guard
- **v0.6** — Singleflight request coalescing, streaming, LISTEN/NOTIFY
- **v0.7** — Intelligence: cross-query analysis, EXPLAIN at compile time, read/write splitting
- **v1.0** — Stable release with arena allocation, binary protocol, SIMD optimizations

## About the Development Process

This project was built with [Claude Code](https://claude.ai/code). I could have hidden that. But ask yourself: would you trust a strong solo developer more, or a strong solo developer backed by an advisor with the collective knowledge of the entire software engineering field?

Here is what actually matters: rigorous specifications written before the first line of code. A CREDO with 17 non-negotiable principles that every design decision was checked against. Six rounds of architectural audit before implementation began. 166 tests covering unit, integration, and compile-fail scenarios. Every edge case challenged, every blind spot hunted down, every "it works on my machine" replaced with a proof.

The human factor is reduced, not eliminated. Without this process, I would not have considered bitcode for serialization, or arena allocation for result sets, or rapidhash over FNV-1a, or the fail-fast pool pattern over timeouts. I would have written tests that confirm the code handles specific inputs, not tests that prove the system rejects invalid ones. I would have shipped UTF-8 bugs in the SQL parser because I would have tested with ASCII and called it done.

The value is not in who wrote the code. The value is in the discipline: constant audits, clear specifications, uncompromising adherence to them, and test coverage that treats every untested path as a bug. That discipline is what makes sasql trustworthy — not the identity of the author or the tools used.

## License

MIT OR Apache-2.0
