//! Runtime support for sasql.
//!
//! This crate provides the types that `sasql::query!` generated code depends on:
//! error types, connection pool, and the executor trait.
//!
//! You should not depend on this crate directly — use [`sasql`] instead.

pub mod error;
pub mod types;

pub use error::{SasqlError, SasqlResult};
