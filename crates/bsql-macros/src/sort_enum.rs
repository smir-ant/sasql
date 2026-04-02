//! Implementation of the `#[bsql::sort]` attribute macro.
//!
//! Generates a sort enum whose variants map to SQL `ORDER BY` fragments.
//! Used with the `$[sort: EnumType]` placeholder syntax in `bsql::query!`.
//!
//! # Usage
//!
//! ```rust,ignore
//! #[bsql::sort]
//! pub enum TicketSort {
//!     #[sql("t.updated_at DESC, t.id DESC")]
//!     UpdatedAt,
//!     #[sql("t.deadline ASC NULLS LAST, t.id ASC")]
//!     Deadline,
//!     #[sql("t.id DESC")]
//!     Id,
//! }
//! ```
//!
//! Each variant must have a `#[sql("...")]` attribute containing the SQL
//! fragment to splice into the `ORDER BY` clause. The macro generates:
//! - The enum with `#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]`
//! - A `sql(&self) -> &'static str` method returning the SQL fragment
//!
//! Unlike `#[bsql::pg_enum]`, sort enums do NOT implement `Encode` — they
//! are not parameterized values. The SQL fragment is spliced directly into
//! the query string at compile time, and the runtime selects between
//! pre-validated SQL strings via a `match`.
//!
//! **Note:** Sort SQL fragments are NOT individually validated at compile time.
//! The query structure is validated but individual ORDER BY expressions are
//! verified only at runtime. Ensure your `#[sql("...")]` fragments reference
//! valid columns.

use proc_macro2::TokenStream;
use quote::quote;

/// A single parsed sort variant with its SQL fragment.
struct SortVariant {
    /// The Rust variant identifier.
    ident: syn::Ident,
    /// The SQL ORDER BY fragment (from `#[sql("...")]`).
    sql_fragment: String,
}

/// Parse and generate code for `#[bsql::sort]`.
pub fn expand_sort_enum(_attr: TokenStream, item: TokenStream) -> Result<TokenStream, syn::Error> {
    let input: syn::ItemEnum = syn::parse2(item)?;

    // Validate: must be a C-like enum (no fields on variants)
    for variant in &input.variants {
        if !matches!(variant.fields, syn::Fields::Unit) {
            return Err(syn::Error::new_spanned(
                variant,
                "sort enum only supports unit variants (no fields)",
            ));
        }
    }

    if input.variants.is_empty() {
        return Err(syn::Error::new_spanned(
            &input,
            "sort enum requires at least one variant",
        ));
    }

    let variants = parse_sort_variants(&input)?;

    let enum_name = &input.ident;
    let vis = &input.vis;

    // Preserve any existing attributes (doc comments, etc.) except #[sql(...)] on variants
    let enum_attrs: Vec<_> = input.attrs.iter().collect();

    // Build clean enum definition (strip #[sql] attrs from variants)
    let variant_defs = input.variants.iter().map(|v| {
        let attrs: Vec<_> = v
            .attrs
            .iter()
            .filter(|a| !a.path().is_ident("sql"))
            .collect();
        let ident = &v.ident;
        quote! { #(#attrs)* #ident }
    });

    // Generate sql() method arms
    let sql_arms: Vec<TokenStream> = variants
        .iter()
        .map(|v| {
            let ident = &v.ident;
            let sql_fragment = &v.sql_fragment;
            quote! { #enum_name::#ident => #sql_fragment }
        })
        .collect();

    // Generate Display impl (shows the SQL fragment)
    let display_arms: Vec<TokenStream> = variants
        .iter()
        .map(|v| {
            let ident = &v.ident;
            let sql_fragment = &v.sql_fragment;
            quote! { #enum_name::#ident => #sql_fragment, }
        })
        .collect();

    Ok(quote! {
        #(#enum_attrs)*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        #vis enum #enum_name {
            #(#variant_defs,)*
        }

        impl #enum_name {
            /// Returns the SQL `ORDER BY` fragment for this sort variant.
            pub fn sql(&self) -> &'static str {
                match self {
                    #(#sql_arms,)*
                }
            }
        }

        impl ::std::fmt::Display for #enum_name {
            fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                let label = match self {
                    #(#display_arms)*
                };
                f.write_str(label)
            }
        }
    })
}

/// Parse `#[sql("fragment")]` attributes from each variant.
fn parse_sort_variants(input: &syn::ItemEnum) -> Result<Vec<SortVariant>, syn::Error> {
    let mut variants = Vec::with_capacity(input.variants.len());

    for variant in &input.variants {
        let sql_fragment = extract_sql_attr(variant)?;
        variants.push(SortVariant {
            ident: variant.ident.clone(),
            sql_fragment,
        });
    }

    // Validate: no duplicate SQL fragments
    for (i, a) in variants.iter().enumerate() {
        for b in variants.iter().skip(i + 1) {
            if a.sql_fragment == b.sql_fragment {
                return Err(syn::Error::new_spanned(
                    &input.variants[i],
                    format!(
                        "duplicate SQL fragment \"{}\" on variants `{}` and `{}`",
                        a.sql_fragment, a.ident, b.ident
                    ),
                ));
            }
        }
    }

    Ok(variants)
}

/// Extract the SQL fragment from `#[sql("fragment")]` on a variant.
fn extract_sql_attr(variant: &syn::Variant) -> Result<String, syn::Error> {
    for attr in &variant.attrs {
        if attr.path().is_ident("sql") {
            let label: syn::LitStr = attr.parse_args()?;
            let value = label.value();
            if value.is_empty() {
                return Err(syn::Error::new_spanned(
                    attr,
                    "SQL fragment cannot be empty",
                ));
            }
            return Ok(value);
        }
    }
    Err(syn::Error::new_spanned(
        variant,
        format!(
            "variant `{}` is missing #[sql(\"...\")] attribute",
            variant.ident
        ),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_sort(tokens: TokenStream) -> TokenStream {
        expand_sort_enum(TokenStream::new(), tokens).expect("should parse")
    }

    #[test]
    fn basic_sort_enum_generates_code() {
        let input = quote! {
            enum TicketSort {
                #[sql("t.updated_at DESC, t.id DESC")]
                UpdatedAt,
                #[sql("t.deadline ASC NULLS LAST, t.id ASC")]
                Deadline,
                #[sql("t.id DESC")]
                Id,
            }
        };

        let output = parse_sort(input);
        let code = output.to_string();

        assert!(code.contains("enum TicketSort"), "missing enum: {code}");
        assert!(code.contains("fn sql"), "missing sql method: {code}");
        assert!(
            code.contains("t.updated_at DESC"),
            "missing sql fragment: {code}"
        );
        assert!(code.contains("Display"), "missing Display: {code}");
        assert!(code.contains("Debug"), "missing Debug derive: {code}");
        assert!(code.contains("Clone"), "missing Clone derive: {code}");
        assert!(code.contains("Copy"), "missing Copy derive: {code}");
    }

    #[test]
    fn missing_sql_attr_errors() {
        let input = quote! {
            enum Sort {
                #[sql("a DESC")]
                A,
                B,
            }
        };
        let result = expand_sort_enum(TokenStream::new(), input);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("missing #[sql"), "unexpected error: {err}");
    }

    #[test]
    fn empty_sql_fragment_errors() {
        let input = quote! {
            enum Sort {
                #[sql("")]
                A,
            }
        };
        let result = expand_sort_enum(TokenStream::new(), input);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("cannot be empty"), "unexpected error: {err}");
    }

    #[test]
    fn non_unit_variant_errors() {
        let input = quote! {
            enum Sort {
                #[sql("a")]
                A(i32),
            }
        };
        let result = expand_sort_enum(TokenStream::new(), input);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("unit variants"), "unexpected error: {err}");
    }

    #[test]
    fn empty_enum_errors() {
        let input = quote! {
            enum Sort {}
        };
        let result = expand_sort_enum(TokenStream::new(), input);
        assert!(result.is_err());
    }

    #[test]
    fn duplicate_sql_fragment_errors() {
        let input = quote! {
            enum Sort {
                #[sql("a DESC")]
                A,
                #[sql("a DESC")]
                B,
            }
        };
        let result = expand_sort_enum(TokenStream::new(), input);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("duplicate SQL fragment"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn visibility_preserved() {
        let input = quote! {
            pub enum Sort {
                #[sql("a")]
                A,
            }
        };
        let output = parse_sort(input);
        let code = output.to_string();
        assert!(code.contains("pub enum Sort"), "visibility lost: {code}");
    }

    #[test]
    fn no_encode_impl_generated() {
        let input = quote! {
            enum Sort {
                #[sql("a")]
                A,
            }
        };
        let output = parse_sort(input);
        let code = output.to_string();
        // Sort enums must NOT have Encode — they are spliced, not parameterized
        assert!(
            !code.contains("Encode"),
            "sort enum should not have Encode: {code}"
        );
    }

    #[test]
    fn single_variant() {
        let input = quote! {
            enum Sort {
                #[sql("id ASC")]
                Id,
            }
        };
        let output = parse_sort(input);
        let code = output.to_string();
        assert!(code.contains("id ASC"), "missing fragment: {code}");
    }

    #[test]
    fn struct_not_accepted() {
        let input = quote! {
            struct NotAnEnum {
                field: i32,
            }
        };
        let result = expand_sort_enum(TokenStream::new(), input);
        assert!(result.is_err(), "structs should be rejected");
    }
}
