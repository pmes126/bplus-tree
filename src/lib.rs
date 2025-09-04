//! B+ Tree library crate

pub mod bplustree;
pub(crate) mod layout;
pub mod storage;
pub(crate) mod tests;

pub mod api;

pub use api::{DbBytes, TypedDb};
