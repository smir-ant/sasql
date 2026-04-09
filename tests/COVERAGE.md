# Test Coverage Checklist

Tracks what scenarios are tested for each backend. Each row must have a test
for every applicable backend. Missing tests = technical debt.

Legend: ✅ = tested, ❌ = missing, — = not applicable

## Core Query API

| Scenario | PG | SQLite | Test location |
|---|---|---|---|
| fetch_all returns Vec<Row> | ✅ | ✅ | basic:select_fetch_all, sqlite_query:sqlite_fetch_all |
| fetch_one returns Row | ✅ | ✅ | basic:select_fetch_one, sqlite_query:sqlite_fetch_one |
| fetch_one on 0 rows → Err | ✅ | ✅ | basic:fetch_one_zero_rows_errors, sqlite_query:sqlite_fetch_one_empty_errors |
| fetch_optional found | ✅ | ✅ | basic:select_fetch_optional_found, sqlite_query:sqlite_fetch_optional_found |
| fetch_optional not found → None | ✅ | ✅ | basic:select_fetch_optional_not_found, sqlite_query:sqlite_fetch_optional_not_found |
| fetch_all empty → empty Vec | ✅ | ✅ | basic:fetch_all_empty_result, sqlite_query:sqlite_fetch_all_empty |
| execute returns affected count | ✅ | ✅ | basic:update_execute, sqlite_query:sqlite_execute |
| execute affected = 0 | ✅ | ❌ | basic:execute_returns_zero_for_no_match |
| execute affected > 1 | ✅ | ❌ | basic:execute_update_multiple_rows |
| nullable column → Option<T> | ✅ | ✅ | basic:select_nullable_column, sqlite_query:sqlite_nullable_column |
| NOT NULL column → T | ✅ | ✅ | basic:select_fetch_one, sqlite_query:sqlite_not_null_column |

## Parameters

| Scenario | PG | SQLite | Test location |
|---|---|---|---|
| String auto-deref → &str | ✅ | ✅ | basic:string_variable_accepted, sqlite_query:sqlite_string_auto_deref |
| Vec<T> auto-deref → &[T] | ✅ | ❌ | basic:vec_variable_accepted |
| Option<T> param None → NULL | ✅ | ❌ | basic:option_param_none_inserts_null |
| Option<T> param Some → value | ✅ | ❌ | basic:option_param_some_inserts_value |
| Option<i32> on integer column | ✅ | ❌ | basic:option_i32_none_sets_null |
| &[String] array param | ✅ | — | basic:slice_of_string_as_param |
| &[i32] array param (ANY) | ✅ | — | basic:array_param_with_any |
| Empty array param | ✅ | — | basic:empty_string_array_param |

## Type System

| Scenario | PG | SQLite | Test location |
|---|---|---|---|
| i32, i64, bool, f32, f64 | ✅ | ✅ | basic:select_multiple_types, sqlite_basic tests |
| String (TEXT) | ✅ | ✅ | basic, sqlite_query |
| bytea / BLOB | ✅ | ❌ | basic:bytea_column_type |
| Array types (int[], text[]) | ✅ | — | basic:array_column_type |
| PG enum → String | ✅ | — | types:pg_enum_without_text_cast |
| PG enum in JOIN | ✅ | — | types:pg_enum_in_join_context |
| PG enum in subquery | ✅ | — | types:pg_enum_in_subquery |
| JSONB auto-cast | ✅ | — | basic:jsonb_insert_and_select |
| JSON auto-cast | ✅ | — | basic:json_insert_and_select |
| Invalid JSON → error | ✅ | — | basic:jsonb_invalid_json_returns_error |
| UUID | ✅ | — | types:uuid_tests (feature-gated) |
| Timestamp | ✅ | — | types:time_tests (feature-gated) |
| Decimal | ✅ | — | types:decimal_tests (feature-gated) |

## Transactions

| Scenario | PG | SQLite | Test location |
|---|---|---|---|
| begin + commit | ✅ | ❌ | transactions:transaction_commit_persists |
| begin + rollback | ✅ | ❌ | transactions:transaction_rollback_discards |
| drop without commit → rollback | ✅ | — | transactions:transaction_drop_without_commit |
| execute in transaction | ✅ | ❌ | transactions:transaction_execute_returns_affected |
| savepoint + rollback_to | ✅ | — | transactions:savepoint_and_rollback_to |
| nested savepoints | ✅ | — | transactions:nested_savepoints |
| deferred pipeline | ✅ | — | transactions:transaction_defer_execute_commit |
| isolation levels | ✅ | — | transactions:set_isolation_serializable |

## Error Handling

| Scenario | PG | SQLite | Test location |
|---|---|---|---|
| Bad SQL → compile error | ✅ | ✅ | compile_fail tests, sqlite_basic:sqlite_error_bad_sql |
| Nonexistent table → error | ✅ | ✅ | compile_fail:invalid_table, sqlite_basic:sqlite_error_nonexistent_table |
| Unique constraint violation | ✅ | ❌ | basic:execute_unique_constraint_violation |
| FK violation | ✅ | ❌ | basic:execute_foreign_key_violation |
| Connection refused | ✅ | ❌ | integration:connect_wrong_port |
| QueryError constructable | ✅ | ✅ | basic:query_error_constructable (shared type) |

## Pool & Connection

| Scenario | PG | SQLite | Test location |
|---|---|---|---|
| Pool connect | ✅ | ✅ | basic:pool(), sqlite_basic:sqlite_open_memory |
| Pool acquire + use | ✅ | — | basic:pool_acquire_and_use |
| Pool max_size exhaustion | ✅ | — | integration:pool_concurrent_acquire |
| Pool acquire timeout | ✅ | — | integration:pool_acquire_timeout |
| Pool max_lifetime | ✅ | — | integration:pool_max_lifetime |
| Server disconnect → error | ✅ | — | integration:server_kill_backend |
| Statement timeout | ✅ | — | integration:statement_timeout |
| In-memory isolation | — | ✅ | sqlite_basic:sqlite_in_memory_isolation |

## Advanced PG Features

| Scenario | PG | Test location |
|---|---|---|
| Dynamic queries (optional clauses) | ✅ | dynamic:* (19 tests) |
| Sort enums | ✅ | (compile-time validated) |
| Singleflight coalescing | ✅ | singleflight:* (10 tests) |
| Read/write split | ✅ | read_write_split:* (7 tests) |
| LISTEN/NOTIFY | ✅ | listener:* (31 tests) |
| Streaming (fetch_stream) | ✅ | basic:fetch_stream_* |
| for_each (zero-alloc) | ✅ | (used in benchmarks) |
| pgbouncer unnamed statements | ✅ | integration:unnamed_statement_* (7 tests) |
| TLS custom CA | ✅ | (unit tests in tls_sync.rs) |
| raw_query_params | ✅ | basic:raw_query_params_* |
| query_as! with nullable | ✅ | basic:query_as_with_nullable_column |
| LEFT JOIN → Option<T> | ✅ | basic:left_join_right_side_is_nullable |
| Cast NOT NULL inference | ✅ | basic:cast_on_not_null_column |

## CLI

| Scenario | Test location |
|---|---|
| migrate --check | migrate.rs tests (43 total) |
| check --verify-cache | verify.rs tests |
| bsql clean | main.rs:cmd_clean_* |

## SQLite Gaps (need tests)

Priority items missing for SQLite parity:
1. ❌ execute affected = 0 (no matching WHERE)
2. ❌ execute affected > 1 (batch update)
3. ❌ Option<T> param None → NULL
4. ❌ Option<T> param Some → value
5. ❌ Vec<T> auto-deref
6. ❌ Transaction commit/rollback via query!
7. ❌ Unique constraint violation
8. ❌ for_each zero-alloc
9. ❌ query_as! with nullable
