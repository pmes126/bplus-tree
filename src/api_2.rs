pub use typed::{DbClient, TypedClient};
pub use transport::{KvService, RawClient, TreeMeta};
pub use encoding::{KeyEncodingId, KeyConstraints};
pub use errors::ApiError;

pub mod transport; 
pub mod typed;

use std::{fmt, str::FromStr};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyEncodingId {
BeU64,
ZigZagI64,
Utf8,
RawBytes,
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

#[derive(Debug, Clone, Copy)]
pub struct KeyConstraints {
pub fixed_key_len: bool,
pub key_len: u32,
pub max_key_len: u32,
}

impl Default for KeyConstraints {
fn default() -> Self {
Self { fixed_key_len: false, key_len: 0, max_key_len: 1 << 20 }
}
}

use thiserror::Error;
use super::encoding::KeyEncodingId;

#[derive(Debug, Error)]
pub enum ApiError {
#[error("transport: {0}")]
Transport(#[from] tonic::transport::Error),


#[error("rpc: {0}")]
Rpc(#[from] tonic::Status),


#[error("unknown key encoding: {0}")]
UnknownEncoding(String),


#[error("key type incompatible with tree encoding {expected}")]
IncompatibleKeyType { expected: KeyEncodingId },


#[error("decode error: {0}")]
Decode(String),


#[error("range request requires end >= start in key order")]
BadRangeBounds,
}
