//! Public embedded API for the B+ tree key-value store.
//!
//! Surface:
//! - [`Db`]: opens/creates the database and hands out typed trees.
//! - [`Tree<K,V>`]: typed handle for `put`/`get`/`delete`/`range`.
//! - [`WriteTxn`]: batched write transaction with optimistic commit.
//! - [`RangeIter`]: concrete forward-range iterator, no boxing.
//!
//! The layering is purely synchronous. Async / gRPC transports should wrap this
//! core in a separate module, not be baked into the core trait surface.

pub mod db;

pub use crate::codec::kv::{KeyCodec, ValueCodec};
pub use db::{Db, RangeIter, Tree, WriteTxn};

use std::{fmt, str::FromStr};
use thiserror::Error;

/// Wire encoding used to compare and serialize keys in a B+ tree.
#[repr(u64)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyEncodingId {
    /// Big-endian unsigned 64-bit integer.
    BeU64 = 0,
    /// Big-endian signed 64-bit integer (sign-bit flip for order preservation).
    BeI64 = 1,
    /// UTF-8 string bytes.
    Utf8 = 2,
    /// Opaque raw byte slice with lexicographic ordering.
    RawBytes = 3,
}

impl fmt::Display for KeyEncodingId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            KeyEncodingId::BeU64 => "be_u64",
            KeyEncodingId::BeI64 => "be_i64",
            KeyEncodingId::Utf8 => "utf8",
            KeyEncodingId::RawBytes => "raw",
        })
    }
}

impl FromStr for KeyEncodingId {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "be_u64" => Ok(Self::BeU64),
            "be_i64" => Ok(Self::BeI64),
            "utf8" => Ok(Self::Utf8),
            "raw" => Ok(Self::RawBytes),
            other => Err(format!("unknown key encoding: {other}")),
        }
    }
}

impl TryFrom<u64> for KeyEncodingId {
    type Error = String;
    fn try_from(value: u64) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::BeU64),
            1 => Ok(Self::BeI64),
            2 => Ok(Self::Utf8),
            3 => Ok(Self::RawBytes),
            other => Err(format!("unknown key encoding id: {other}")),
        }
    }
}

/// Stable numeric identifier for a logical B+ tree within the database.
pub type TreeId = u64;

/// Inclusive lower and upper bounds on key byte lengths for a tree.
#[derive(Clone, Copy, Debug)]
pub struct KeyLimits {
    /// Minimum key length in bytes.
    pub min_len: u32,
    /// Maximum key length in bytes.
    pub max_len: u32,
}

/// Errors returned by the embedded API.
#[derive(Debug, Error)]
pub enum ApiError {
    /// I/O error from the storage backend.
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// Error from the B+ tree core (invariant violation, key not found, etc.).
    #[error("tree: {0}")]
    Tree(String),
    /// Error during commit (CAS conflict, metadata write failure, etc.).
    #[error("commit: {0}")]
    Commit(String),
    /// Error from the storage layer (page I/O, codec, etc.).
    #[error("storage: {0}")]
    Storage(String),
    /// The provided key type is incompatible with the tree's pinned encoding.
    #[error("key type incompatible with tree encoding {expected}")]
    IncompatibleKeyType {
        /// Encoding the tree was created with.
        expected: KeyEncodingId,
    },
    /// A value could not be decoded against its [`ValueCodec`].
    #[error("decode: {0}")]
    Decode(String),
    /// An internal invariant was violated.
    #[error("internal: {0}")]
    Internal(String),
    /// Scan range has end before start in key order.
    #[error("range request requires end >= start in key order")]
    BadRangeBounds,
    /// Write transaction exceeded its retry budget.
    #[error("transaction aborted after exhausting retries")]
    TxnAborted,
}

impl From<crate::bplustree::tree::TreeError> for ApiError {
    fn from(e: crate::bplustree::tree::TreeError) -> Self {
        ApiError::Tree(e.to_string())
    }
}

impl From<crate::bplustree::tree::CommitError> for ApiError {
    fn from(e: crate::bplustree::tree::CommitError) -> Self {
        ApiError::Commit(e.to_string())
    }
}

impl From<crate::storage::StorageError> for ApiError {
    fn from(e: crate::storage::StorageError) -> Self {
        ApiError::Storage(e.to_string())
    }
}
