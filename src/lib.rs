//! B+ Tree library crate
#![allow(elided_lifetimes_in_paths)]

pub mod api;
pub mod bplustree;
pub mod codec;
pub mod database;
pub mod storage;

pub(crate) mod keyfmt;
pub(crate) mod layout;
pub(crate) mod page;
#[cfg(test)]
pub(crate) mod tests;

//pub use api::{DbBytes, TypedDb};
