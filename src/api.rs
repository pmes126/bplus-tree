//! Public API types shared across transport layers and internal database logic.

pub mod transport;

use bytes::Bytes;
use std::ops::Bound;
use std::{fmt, str::FromStr};
use thiserror::Error;

/// Wire encoding used to compare and serialize keys in a B+ tree.
#[repr(u64)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyEncodingId {
    /// Big-endian unsigned 64-bit integer.
    BeU64 = 0,
    /// ZigZag-encoded signed 64-bit integer.
    ZigZagI64 = 1,
    /// UTF-8 string bytes.
    Utf8 = 2,
    /// Opaque raw byte slice with lexicographic ordering.
    RawBytes = 3,
}

impl fmt::Display for KeyEncodingId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            KeyEncodingId::BeU64 => "be_u64",
            KeyEncodingId::ZigZagI64 => "zigzag_i64",
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
            "zigzag_i64" => Ok(Self::ZigZagI64),
            "utf8" => Ok(Self::Utf8),
            "raw" => Ok(Self::RawBytes),
            other => Err(format!("unknown key encoding: {}", other)),
        }
    }
}

impl TryFrom<u64> for KeyEncodingId {
    type Error = String;
    fn try_from(value: u64) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::BeU64),
            1 => Ok(Self::ZigZagI64),
            2 => Ok(Self::Utf8),
            3 => Ok(Self::RawBytes),
            other => Err(format!("unknown key encoding id: {}", other)),
        }
    }
}

/// Structural constraints on keys stored in a tree (fixed vs. variable length).
#[derive(Debug, Clone, Copy)]
pub struct KeyConstraints {
    /// Whether all keys have the same fixed length.
    pub fixed_key_len: bool,
    /// Exact key length in bytes when `fixed_key_len` is true; otherwise unused.
    pub key_len: u32,
    /// Maximum allowed key length in bytes.
    pub max_key_len: u32,
}

impl Default for KeyConstraints {
    fn default() -> Self {
        Self {
            fixed_key_len: false,
            key_len: 0,
            max_key_len: 1 << 20,
        }
    }
}

/// Errors returned by API and transport layer operations.
#[derive(Debug, Error)]
pub enum ApiError {
    /// Underlying transport connection failed.
    #[error("transport: {0}")]
    Transport(#[from] tonic::transport::Error),
    /// RPC call returned a non-OK status.
    #[error("rpc: {0}")]
    Rpc(#[from] tonic::Status),
    /// Key encoding string is not recognized.
    #[error("rpc: {0}")]
    UnknownEncoding(String),
    /// Key bytes could not be decoded for the tree's encoding.
    #[error("key type incompatible with tree encoding {0}")]
    Decode(String),
    /// Scan range has end before start in key order.
    #[error("range request requires end >= start in key order")]
    BadRangeBounds,
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

/// An opaque token used to resume a paginated scan from a prior position.
pub type ResumeToken = Bytes;

/// Direction for a range scan.
#[derive(Clone, Copy, Debug)]
pub enum Order {
    /// Forward (ascending key order).
    Fwd,
    /// Reverse (descending key order).
    Rev,
}

/// Half-open or fully-bounded key range for a scan request.
#[derive(Clone, Debug)]
pub struct KeyRange<'a> {
    /// Inclusive or exclusive lower bound of the range.
    pub start: Bound<&'a [u8]>,
    /// Inclusive or exclusive upper bound of the range.
    pub end: Bound<&'a [u8]>,
}

/// On-page layout strategy for storing keys in a B+ tree node.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum KeyFormatId {
    /// Keys stored verbatim with no compression.
    Raw,
    /// Keys compressed using prefix-restart encoding.
    PrefixRestarts,
}

/// Parameters that tune a specific [`KeyFormatId`] layout.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Default)]
pub struct KeyFormatParams {
    /// Number of keys between prefix restart points; only meaningful for [`KeyFormatId::PrefixRestarts`].
    pub restart_interval: u16,
}
