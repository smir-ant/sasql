# sasql

**If it compiles, the SQL is correct.**

sasql is a Rust proc-macro library that validates every SQL query against a real PostgreSQL instance at compile time. No runtime SQL errors. No escape hatches. No discipline required.

---

## The Problem

Every existing Rust SQL library has a backdoor.

| Library | The backdoor |
|---------|-------------|
| **sqlx** | `query()` — the unchecked runtime function, same import path as `query!` |
| **Diesel** | `sql_query()` — raw strings, unchecked, used by every complex project |
| **SeaORM** | Zero compile-time checking. Errors surface when queries hit PostgreSQL |
| **Cornucopia** | SQL lives in separate `.sql` files. No dynamic queries. File-hopping hell |

One unchecked query in a codebase of 500 checked queries breaks the guarantee. The guarantee is binary: **100% or meaningless.**

## The Solution

```rust
let id = 42i32;
let user = sasql::query!(
    "SELECT id, login, active FROM users WHERE id = $id: i32"
).fetch_one(&pool).await?;

// user.id: i32
// user.login: String
// user.active: bool
```

That's it. Write SQL. Declare parameters inline. The macro does the rest:

1. Connects to PostgreSQL at `cargo build` time
2. Runs `PREPARE` to validate syntax, tables, columns, types
3. Introspects `pg_catalog` for nullability (`NOT NULL` = `T`, nullable = `Option<T>`)
4. Generates a typed Rust struct with named fields
5. If any of this fails, **the binary is not produced**

There is no `sasql::query()` function. There is no `raw_sql()`. There is no `trust_me_bro()`. The escape hatch does not exist.

## Features

**Compile-time safety:**
- Every column name verified against the real schema
- Every parameter type checked against PostgreSQL's expectations
- Nullable columns automatically become `Option<T>`
- Typos in column/table names produce "did you mean?" suggestions (planned)
- Invalid SQL = compile error, not runtime error

**Pure SQL — no DSL:**
- Write real PostgreSQL SQL: CTEs, JOINs, window functions, `DISTINCT ON`, subqueries
- No `.filter()`, no `.select()`, no query builder
- If you know PostgreSQL, you know sasql

**Developer experience:**
- Queries live inline, next to the code that uses them
- IDE autocomplete on result struct fields (via rust-analyzer)
- Clear error messages pointing to the exact problem
- `fetch_one`, `fetch_all`, `fetch_optional`, `execute` — nothing else to learn

**Reliability:**
- `#![forbid(unsafe_code)]` — zero unsafe in the entire codebase
- Fail-fast connection pool — never blocks, never waits, immediate error on exhaustion
- PgBouncer auto-detection with transparent fallback
- 118 tests: unit, integration, and compile-fail (trybuild)

## Quick Start

```toml
[dependencies]
sasql = "0.1"
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
```

Set the database URL for compile-time validation:

```bash
export SASQL_DATABASE_URL="postgres://user:pass@localhost/mydb"
```

```rust
use sasql::Pool;

#[tokio::main]
async fn main() -> Result<(), sasql::SasqlError> {
    let pool = Pool::connect("postgres://user:pass@localhost/mydb").await?;

    // SELECT
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

    // UPDATE
    let status = "resolved";
    let ticket_id = ticket.id;
    let affected = sasql::query!(
        "UPDATE tickets SET status = $status: &str WHERE id = $ticket_id: i32"
    ).execute(&pool).await?;

    println!("Updated {} row(s)", affected);

    Ok(())
}
```

## How It Works

```
cargo build
    │
    ▼
sasql::query!("SELECT ...")
    │
    ├─ Parse: extract $name: Type parameters
    ├─ Connect to PostgreSQL (shared across all macros)
    ├─ PREPARE: validate SQL syntax, tables, columns
    ├─ Introspect pg_catalog: column types, nullability
    ├─ Check parameter types match schema
    ├─ Generate: typed result struct + executor methods
    │
    ▼
Compiled binary with zero unverified SQL
```

The proc macro maintains a single PostgreSQL connection across all `query!` invocations in one `cargo build`. First invocation: ~5ms. Subsequent: ~0.5ms each.

## Type Mapping

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

Parameters accept both owned and borrowed types: `$name: &str` or `$name: String` for text, `$data: &[u8]` or `$data: Vec<u8>` for bytea.

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
- **Not database-agnostic.** PostgreSQL first. PostgreSQL only (for now).
- **Not a migration tool.** Use whatever migration tool you prefer.

## Roadmap

- **v0.1** — Foundation (current): `query!` macro, compile-time validation, base types, pool
- **v0.2** — Full type system: `time`, `chrono`, `uuid`, `decimal`, `json`, custom PG enums
- **v0.3** — Dynamic queries: `[optional clauses]`, sort enums, 2^N compile-time expansion
- **v0.4** — Offline mode: validate without a live database (`sasql prepare`)
- **v0.5** — Transactions: `pool.begin()`, commit, rollback, drop-guard
- **v0.6** — Singleflight, streaming, LISTEN/NOTIFY
- **v0.7** — Intelligence: cross-query analysis, EXPLAIN at compile time, read/write splitting
- **v1.0** — Stable release

## About the Development Process

Yes, this project was built with [Claude Code](https://claude.ai/code). I could have hidden that. But ask yourself: would you trust a strong solo developer more, or a strong solo developer backed by an advisor with the collective knowledge of the entire software engineering field?

Here is what actually matters: rigorous specifications written before the first line of code. A CREDO with 17 non-negotiable principles that every design decision was checked against. Six rounds of architectural audit before implementation began. 118 tests covering unit, integration, and compile-fail scenarios. Every edge case challenged, every blind spot hunted down, every "it works on my machine" replaced with a proof.

The human factor is reduced, not eliminated. Without this process, I would not have considered bitcode for serialization, or arena allocation for result sets, or rapidhash over FNV-1a, or the fail-fast pool pattern over timeouts. I would have written tests that confirm the code handles specific inputs, not tests that prove the system rejects invalid ones. I would have shipped UTF-8 bugs in the SQL parser because I would have tested with ASCII and called it done.

The value is not in who wrote the code. The value is in the discipline: constant audits, clear specifications, uncompromising adherence to them, and test coverage that treats every untested path as a bug. That discipline is what makes sasql trustworthy — not the identity of the author or the tools used.

## License

MIT OR Apache-2.0
