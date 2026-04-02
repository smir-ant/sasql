//! Code generation for SQLite queries.
//!
//! Generates Rust code that executes queries via `bsql_core::SqlitePool`.
//! Structurally parallel to `codegen.rs` (PostgreSQL) but references SQLite
//! driver types for parameter binding and row decoding.
//!
//! Key differences from PostgreSQL codegen:
//! - Parameters use `bsql_driver_sqlite::codec::SqliteEncode` (not PG `Encode`)
//! - Row decoding uses SQLite arena format (LE i64 for integers, LE f64 for reals)
//! - Executor calls reference `bsql_core::SqlitePool` and `bsql_core::SqliteExecutor`
//! - No streaming support (SQLite pool uses dedicated threads, not async streams)

use proc_macro2::TokenStream;
use quote::{format_ident, quote};

use crate::parse::ParsedQuery;
use crate::validate::ValidationResult;
use crate::validate_sqlite::pg_to_sqlite_params;

/// Generate the complete Rust code for a SQLite `query!` invocation (static query).
pub fn generate_sqlite_query_code(
    parsed: &ParsedQuery,
    validation: &ValidationResult,
) -> TokenStream {
    let sqlite_sql = pg_to_sqlite_params(&parsed.positional_sql);
    let result_struct = gen_result_struct(parsed, validation);
    let executor_struct = gen_executor_struct(parsed);
    let executor_impls = gen_executor_impls(parsed, validation, &sqlite_sql);
    let constructor = gen_constructor(parsed);

    quote! {
        {
            #result_struct
            #executor_struct
            #executor_impls
            #constructor
        }
    }
}

// --- Result struct (identical structure to PG, different field types possible) ---

fn gen_result_struct(parsed: &ParsedQuery, validation: &ValidationResult) -> TokenStream {
    if validation.columns.is_empty() {
        return TokenStream::new();
    }

    let struct_name = result_struct_name(parsed);
    let deduped_names = deduplicate_column_names(&validation.columns);
    let fields = validation.columns.iter().enumerate().map(|(i, col)| {
        let field_name = format_ident!("{}", deduped_names[i]);
        let field_type = parse_result_type(&col.rust_type);
        quote! { pub #field_name: #field_type }
    });

    quote! {
        #[derive(Debug)]
        #[allow(non_camel_case_types)]
        pub struct #struct_name {
            #(#fields,)*
        }
    }
}

// --- Executor struct ---

fn gen_executor_struct(parsed: &ParsedQuery) -> TokenStream {
    let struct_name = executor_struct_name(parsed);

    let fields: Vec<TokenStream> = parsed
        .params
        .iter()
        .map(|p| {
            let name = param_ident(&p.name);
            let ty = inject_lifetime(&p.rust_type);
            quote! { #name: #ty }
        })
        .collect();

    quote! {
        #[must_use = "query is not executed until .fetch_one(), .fetch_all(), .fetch_optional(), or .execute() is called"]
        #[allow(non_camel_case_types)]
        struct #struct_name<'_bsql> {
            #(#fields,)*
            _marker: ::std::marker::PhantomData<&'_bsql ()>,
        }
    }
}

// --- Executor impl ---

fn gen_executor_impls(
    parsed: &ParsedQuery,
    validation: &ValidationResult,
    sqlite_sql: &str,
) -> TokenStream {
    let executor_name = executor_struct_name(parsed);
    let sql_lit = sqlite_sql;

    let is_select = parsed.kind == crate::parse::QueryKind::Select;

    // Build the params slice for SQLite: convert to ParamValue for channel transport
    let param_conversions: Vec<TokenStream> = parsed
        .params
        .iter()
        .map(|p| {
            let name = param_ident(&p.name);
            gen_param_to_param_value(&name, &p.rust_type)
        })
        .collect();

    let params_build = if param_conversions.is_empty() {
        quote! { let params: ::bsql_core::driver_sqlite::SmallVec<[::bsql_core::driver_sqlite::ParamValue; 8]> = ::bsql_core::driver_sqlite::SmallVec::new(); }
    } else {
        quote! {
            let params: ::bsql_core::driver_sqlite::SmallVec<[::bsql_core::driver_sqlite::ParamValue; 8]> = ::bsql_core::driver_sqlite::smallvec![
                #(#param_conversions),*
            ];
        }
    };

    // Compute sql_hash at compile time
    let sql_hash_val = bsql_core::rapid_hash_str(sqlite_sql);

    let has_columns = !validation.columns.is_empty();

    // Generate LIMIT 2 variant for fetch_one/fetch_optional
    let needs_limit = has_columns && is_select && !parsed.normalized_sql.contains(" limit ");

    let limited_sql = if needs_limit {
        format!("{sqlite_sql} LIMIT 2")
    } else {
        sqlite_sql.to_owned()
    };
    let limited_sql_lit = &limited_sql;
    let limited_sql_hash_val = bsql_core::rapid_hash_str(&limited_sql);

    let row_decode = if has_columns {
        gen_sqlite_row_decode(validation)
    } else {
        TokenStream::new()
    };

    // Determine which pool method to call
    let query_method = if is_select {
        quote! { query_readonly }
    } else {
        quote! { query_readwrite }
    };

    let fetch_methods = if has_columns {
        let result_name = result_struct_name(parsed);
        let qm = &query_method;

        quote! {
            pub async fn fetch_one(
                self,
                pool: &::bsql_core::SqlitePool,
            ) -> ::bsql_core::BsqlResult<#result_name> {
                #params_build
                let (result, arena) = pool.#qm(#limited_sql_lit, #limited_sql_hash_val, params).await?;
                if result.len() != 1 {
                    return Err(::bsql_core::error::QueryError::row_count(
                        "exactly 1 row",
                        result.len() as u64,
                    ));
                }
                Ok(#result_name { #row_decode })
            }

            pub async fn fetch_all(
                self,
                pool: &::bsql_core::SqlitePool,
            ) -> ::bsql_core::BsqlResult<Vec<#result_name>> {
                #params_build
                let (result, arena) = pool.#qm(#sql_lit, #sql_hash_val, params).await?;
                let mut out = Vec::with_capacity(result.len());
                for _bsql_row_idx in 0..result.len() {
                    out.push(#result_name { #row_decode });
                }
                Ok(out)
            }

            pub async fn fetch_optional(
                self,
                pool: &::bsql_core::SqlitePool,
            ) -> ::bsql_core::BsqlResult<Option<#result_name>> {
                #params_build
                let (result, arena) = pool.#qm(#limited_sql_lit, #limited_sql_hash_val, params).await?;
                match result.len() {
                    0 => Ok(None),
                    1 => {
                        let _bsql_row_idx: usize = 0;
                        Ok(Some(#result_name { #row_decode }))
                    }
                    n => Err(::bsql_core::error::QueryError::row_count(
                        "0 or 1 rows",
                        n as u64,
                    )),
                }
            }
        }
    } else {
        TokenStream::new()
    };

    let execute_method = quote! {
        pub async fn execute(
            self,
            pool: &::bsql_core::SqlitePool,
        ) -> ::bsql_core::BsqlResult<u64> {
            #params_build
            pool.execute_sql(#sql_lit, #sql_hash_val, params).await
        }
    };

    quote! {
        #[allow(non_camel_case_types)]
        impl<'_bsql> #executor_name<'_bsql> {
            #fetch_methods
            #execute_method
        }
    }
}

// --- Param to ParamValue conversion ---

fn gen_param_to_param_value(name: &proc_macro2::Ident, rust_type: &str) -> TokenStream {
    // Handle Option<T> params
    if let Some(inner) = rust_type
        .strip_prefix("Option<")
        .and_then(|s| s.strip_suffix('>'))
    {
        let inner_conv = param_value_conversion(quote! { v }, inner);
        return quote! {
            match &self.#name {
                Some(v) => #inner_conv,
                None => ::bsql_core::driver_sqlite::ParamValue::Null,
            }
        };
    }

    param_value_conversion(quote! { self.#name }, rust_type)
}

fn param_value_conversion(val_expr: TokenStream, rust_type: &str) -> TokenStream {
    match rust_type {
        "bool" => quote! { ::bsql_core::driver_sqlite::ParamValue::Bool(#val_expr) },
        "i8" => quote! { ::bsql_core::driver_sqlite::ParamValue::Int(i64::from(#val_expr)) },
        "i16" => quote! { ::bsql_core::driver_sqlite::ParamValue::Int(i64::from(#val_expr)) },
        "i32" => quote! { ::bsql_core::driver_sqlite::ParamValue::Int(i64::from(#val_expr)) },
        "i64" => quote! { ::bsql_core::driver_sqlite::ParamValue::Int(#val_expr) },
        "f32" => quote! { ::bsql_core::driver_sqlite::ParamValue::Real(f64::from(#val_expr)) },
        "f64" => quote! { ::bsql_core::driver_sqlite::ParamValue::Real(#val_expr) },
        "&str" => quote! { ::bsql_core::driver_sqlite::ParamValue::Text(#val_expr.to_owned()) },
        "String" => quote! { ::bsql_core::driver_sqlite::ParamValue::Text(#val_expr.clone()) },
        "&[u8]" => quote! { ::bsql_core::driver_sqlite::ParamValue::Blob(#val_expr.to_vec()) },
        "Vec<u8>" => quote! { ::bsql_core::driver_sqlite::ParamValue::Blob(#val_expr.clone()) },
        _ => {
            // Unknown type: attempt ToString for text storage
            quote! { ::bsql_core::driver_sqlite::ParamValue::Text(::std::string::ToString::to_string(&#val_expr)) }
        }
    }
}

// --- SQLite row decode ---

/// Generate row field decoding using SQLite QueryResult accessors.
///
/// SQLite arena format:
/// - INTEGER: 8 bytes, little-endian i64
/// - REAL: 8 bytes, little-endian f64
/// - TEXT: raw UTF-8 bytes
/// - BLOB: raw bytes
/// - NULL: nothing (indicated by length == -1)
fn gen_sqlite_row_decode(validation: &ValidationResult) -> TokenStream {
    let deduped_names = deduplicate_column_names(&validation.columns);
    let fields = deduped_names.iter().enumerate().map(|(i, name)| {
        let field_name = format_ident!("{}", name);
        let col_idx = i;
        let col = &validation.columns[i];
        let decode_expr = gen_sqlite_column_decode(col_idx, &col.rust_type);
        quote! { #field_name: #decode_expr }
    });

    quote! { #(#fields),* }
}

fn gen_sqlite_column_decode(idx: usize, rust_type: &str) -> TokenStream {
    if let Some(inner) = rust_type
        .strip_prefix("Option<")
        .and_then(|s| s.strip_suffix('>'))
    {
        gen_sqlite_nullable_decode(idx, inner)
    } else {
        gen_sqlite_not_null_decode(idx, rust_type)
    }
}

fn gen_sqlite_not_null_decode(idx: usize, rust_type: &str) -> TokenStream {
    let col_idx = idx.to_string();
    match rust_type {
        "bool" => {
            let err = gen_decode_error(&col_idx, "bool");
            quote! { result.get_bool(_bsql_row_idx, #idx, &arena).ok_or_else(|| #err)? }
        }
        "i64" => {
            let err = gen_decode_error(&col_idx, "i64");
            quote! { result.get_i64(_bsql_row_idx, #idx, &arena).ok_or_else(|| #err)? }
        }
        "f64" => {
            let err = gen_decode_error(&col_idx, "f64");
            quote! { result.get_f64(_bsql_row_idx, #idx, &arena).ok_or_else(|| #err)? }
        }
        "String" => {
            let err = gen_decode_error(&col_idx, "String");
            quote! { result.get_str(_bsql_row_idx, #idx, &arena).ok_or_else(|| #err)?.to_owned() }
        }
        "Vec<u8>" => {
            let err = gen_decode_error(&col_idx, "Vec<u8>");
            quote! { result.get_bytes(_bsql_row_idx, #idx, &arena).ok_or_else(|| #err)?.to_vec() }
        }
        _ => {
            // Fallback: try to read as string
            let err = gen_decode_error(&col_idx, rust_type);
            quote! { result.get_str(_bsql_row_idx, #idx, &arena).ok_or_else(|| #err)?.to_owned() }
        }
    }
}

fn gen_sqlite_nullable_decode(idx: usize, inner_type: &str) -> TokenStream {
    match inner_type {
        "bool" => quote! { result.get_bool(_bsql_row_idx, #idx, &arena) },
        "i64" => quote! { result.get_i64(_bsql_row_idx, #idx, &arena) },
        "f64" => quote! { result.get_f64(_bsql_row_idx, #idx, &arena) },
        "String" => quote! { result.get_str(_bsql_row_idx, #idx, &arena).map(|s| s.to_owned()) },
        "Vec<u8>" => quote! { result.get_bytes(_bsql_row_idx, #idx, &arena).map(|b| b.to_vec()) },
        _ => quote! { result.get_str(_bsql_row_idx, #idx, &arena).map(|s| s.to_owned()) },
    }
}

fn gen_decode_error(col_idx: &str, type_name: &str) -> TokenStream {
    quote! {
        ::bsql_core::error::DecodeError::with_source(
            #col_idx,
            #type_name,
            "NULL or invalid data",
            ::std::io::Error::new(::std::io::ErrorKind::InvalidData, concat!("expected NOT NULL ", #type_name)),
        )
    }
}

// --- Constructor ---

fn gen_constructor(parsed: &ParsedQuery) -> TokenStream {
    let executor_name = executor_struct_name(parsed);
    let field_inits = parsed.params.iter().map(|p| {
        let name = param_ident(&p.name);
        quote! { #name }
    });
    quote! { #executor_name { #(#field_inits,)* _marker: ::std::marker::PhantomData } }
}

// --- Shared helpers (mirrored from codegen.rs) ---

fn result_struct_name(parsed: &ParsedQuery) -> proc_macro2::Ident {
    format_ident!("BsqlResult_{}", &parsed.statement_name)
}

fn executor_struct_name(parsed: &ParsedQuery) -> proc_macro2::Ident {
    format_ident!("BsqlExecutor_{}", &parsed.statement_name)
}

fn inject_lifetime(type_str: &str) -> TokenStream {
    match syn::parse_str::<syn::Type>(type_str) {
        Ok(ty) => {
            let rewritten = add_lifetime_to_refs(ty);
            quote! { #rewritten }
        }
        Err(_) => {
            let msg = format!("internal error: cannot parse type `{type_str}`");
            quote! { compile_error!(#msg) }
        }
    }
}

fn add_lifetime_to_refs(ty: syn::Type) -> syn::Type {
    match ty {
        syn::Type::Reference(mut r) => {
            if r.lifetime.is_none() {
                r.lifetime = Some(syn::Lifetime::new("'_bsql", proc_macro2::Span::call_site()));
            }
            r.elem = Box::new(add_lifetime_to_refs(*r.elem));
            syn::Type::Reference(r)
        }
        syn::Type::Slice(mut s) => {
            s.elem = Box::new(add_lifetime_to_refs(*s.elem));
            syn::Type::Slice(s)
        }
        syn::Type::Path(mut p) => {
            for seg in &mut p.path.segments {
                if let syn::PathArguments::AngleBracketed(args) = &mut seg.arguments {
                    for arg in &mut args.args {
                        if let syn::GenericArgument::Type(inner) = arg {
                            *inner = add_lifetime_to_refs(inner.clone());
                        }
                    }
                }
            }
            syn::Type::Path(p)
        }
        other => other,
    }
}

fn parse_result_type(type_str: &str) -> TokenStream {
    match syn::parse_str::<syn::Type>(type_str) {
        Ok(ty) => quote! { #ty },
        Err(_) => {
            let msg = format!("internal error: cannot parse type `{type_str}`");
            quote! { compile_error!(#msg) }
        }
    }
}

const RUST_KEYWORDS: &[&str] = &[
    "as", "async", "await", "break", "const", "continue", "crate", "dyn", "else", "enum", "extern",
    "false", "fn", "for", "gen", "if", "impl", "in", "let", "loop", "match", "mod", "move", "mut",
    "pub", "raw", "ref", "return", "self", "Self", "static", "struct", "super", "trait", "true",
    "type", "unsafe", "use", "where", "while", "yield",
];

fn sanitize_param_name(name: &str) -> String {
    if RUST_KEYWORDS.contains(&name) {
        format!("{name}_")
    } else {
        name.to_owned()
    }
}

fn param_ident(name: &str) -> proc_macro2::Ident {
    format_ident!("{}", sanitize_param_name(name))
}

fn sanitize_column_name(name: &str, index: usize) -> String {
    if name == "?column?" || name.is_empty() {
        return format!("col_{index}");
    }
    let sanitized: String = name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();
    let sanitized = if sanitized.starts_with(|c: char| c.is_ascii_digit()) {
        format!("col_{sanitized}")
    } else {
        sanitized
    };
    if RUST_KEYWORDS.contains(&sanitized.as_str()) {
        format!("{sanitized}_")
    } else {
        sanitized
    }
}

fn deduplicate_column_names(columns: &[crate::validate::ColumnInfo]) -> Vec<String> {
    let names: Vec<String> = columns
        .iter()
        .enumerate()
        .map(|(i, col)| sanitize_column_name(&col.name, i))
        .collect();
    let mut final_names: Vec<String> = Vec::with_capacity(names.len());
    for name in &names {
        let mut candidate = name.clone();
        let mut suffix = 1u32;
        while final_names.contains(&candidate) {
            candidate = format!("{name}_{suffix}");
            suffix += 1;
        }
        final_names.push(candidate);
    }
    final_names
}
