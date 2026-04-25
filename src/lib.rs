//! B+ Tree library crate
#![allow(elided_lifetimes_in_paths)]

pub mod api;
pub mod codec;

pub(crate) mod bplustree;
pub(crate) mod database;
pub(crate) mod keyfmt;
pub(crate) mod layout;
pub(crate) mod page;
pub(crate) mod storage;
#[cfg(test)]
pub(crate) mod tests;
