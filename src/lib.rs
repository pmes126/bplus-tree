//! B+ Tree library crate
#![allow(elided_lifetimes_in_paths)]

#[allow(dead_code)]
pub mod api;
#[allow(dead_code)]
pub mod codec;

#[allow(dead_code)]
pub(crate) mod bplustree;
#[allow(dead_code)]
pub(crate) mod database;
#[allow(dead_code)]
pub(crate) mod keyfmt;
#[allow(dead_code)]
pub(crate) mod layout;
#[allow(dead_code)]
pub(crate) mod page;
#[allow(dead_code)]
pub(crate) mod storage;
#[cfg(test)]
pub(crate) mod tests;
