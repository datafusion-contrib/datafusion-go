//! Native DataFusion bridge for the Go database/sql driver.
//!
//! The public C entry points live in abi. Internal modules own query policy,
//! parameter conversion, sessions, Arrow registration, and result lifetimes.
//! See docs/architecture.md for ownership and lock-order invariants.

#![deny(clippy::missing_safety_doc)]
#![deny(unsafe_op_in_unsafe_fn)]

mod abi;
mod error;
mod generated;
mod parameters;
mod query;
mod registration;
mod session;
mod stream;

pub use abi::*;

#[cfg(coverage)]
mod coverage_tests;
