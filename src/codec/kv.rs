//! Self-encoding key/value traits for the embedded API.
//!
//! Encoded keys **must** preserve lexicographic byte ordering so that scans and
//! range queries work correctly. [`KeyCodec::ENCODING`] documents which wire
//! encoding the implementation produces so a [`crate::api::Tree`] can pin the
//! relationship at construction time.
//!
//! Implementations delegate to the buffer-oriented codecs in
//! [`super::bincode`] so that encoding logic lives in one place.

use crate::api::{ApiError, KeyEncodingId};
use crate::codec::bincode::{BeU64, RawBuf, Utf8};
use crate::codec::{KeyCodec as BufKeyCodec, ValueCodec as BufValueCodec};

/// Encode/decode a typed key as order-preserving bytes.
pub trait KeyCodec: Sized {
    /// The wire encoding produced by this codec.
    const ENCODING: KeyEncodingId;

    /// Encode a key into its on-disk representation.
    fn encode(&self) -> Vec<u8>;

    /// Decode a key from its on-disk representation.
    fn decode(bytes: &[u8]) -> Result<Self, ApiError>;
}

/// Encode/decode a typed value as opaque bytes.
pub trait ValueCodec: Sized {
    /// Encode a value into bytes.
    fn encode(&self) -> Vec<u8>;

    /// Decode a value from bytes.
    fn decode(bytes: &[u8]) -> Result<Self, ApiError>;
}

// ---------------------------------------------------------------------------
// Helper: buffer-encode via an existing BufKeyCodec / BufValueCodec impl
// ---------------------------------------------------------------------------

fn buf_encode_key<K, C: BufKeyCodec<K>>(key: &K) -> Vec<u8> {
    let len = C::encoded_len(key);
    let mut buf = vec![0u8; len];
    C::encode_key(key, &mut buf).expect("encode into correctly-sized buffer");
    buf
}

fn buf_decode_key<K, C: BufKeyCodec<K>>(bytes: &[u8]) -> Result<K, ApiError> {
    C::decode_key(bytes).map_err(|e| ApiError::Decode(e.to_string()))
}

fn buf_encode_value<V, C: BufValueCodec<V>>(value: &V) -> Vec<u8> {
    let len = C::encoded_len(value);
    let mut buf = vec![0u8; len];
    C::encode_value(value, &mut buf).expect("encode into correctly-sized buffer");
    buf
}

fn buf_decode_value<V, C: BufValueCodec<V>>(bytes: &[u8]) -> Result<V, ApiError> {
    C::decode_value(bytes).map_err(|e| ApiError::Decode(e.to_string()))
}

// ---------------------------------------------------------------------------
// Built-in KeyCodec impls — delegate to bincode codecs
// ---------------------------------------------------------------------------

impl KeyCodec for u64 {
    const ENCODING: KeyEncodingId = KeyEncodingId::BeU64;
    fn encode(&self) -> Vec<u8> {
        buf_encode_key::<u64, BeU64>(self)
    }
    fn decode(bytes: &[u8]) -> Result<Self, ApiError> {
        buf_decode_key::<u64, BeU64>(bytes)
    }
}

impl KeyCodec for i64 {
    const ENCODING: KeyEncodingId = KeyEncodingId::BeI64;
    fn encode(&self) -> Vec<u8> {
        buf_encode_key::<i64, BeU64>(self)
    }
    fn decode(bytes: &[u8]) -> Result<Self, ApiError> {
        buf_decode_key::<i64, BeU64>(bytes)
    }
}

impl KeyCodec for String {
    const ENCODING: KeyEncodingId = KeyEncodingId::Utf8;
    fn encode(&self) -> Vec<u8> {
        buf_encode_key::<String, Utf8>(self)
    }
    fn decode(bytes: &[u8]) -> Result<Self, ApiError> {
        buf_decode_key::<String, Utf8>(bytes)
    }
}

impl KeyCodec for Vec<u8> {
    const ENCODING: KeyEncodingId = KeyEncodingId::RawBytes;
    fn encode(&self) -> Vec<u8> {
        buf_encode_key::<Vec<u8>, RawBuf>(self)
    }
    fn decode(bytes: &[u8]) -> Result<Self, ApiError> {
        buf_decode_key::<Vec<u8>, RawBuf>(bytes)
    }
}

// ---------------------------------------------------------------------------
// Built-in ValueCodec impls — delegate to bincode codecs
// ---------------------------------------------------------------------------

impl ValueCodec for Vec<u8> {
    fn encode(&self) -> Vec<u8> {
        buf_encode_value::<Vec<u8>, RawBuf>(self)
    }
    fn decode(bytes: &[u8]) -> Result<Self, ApiError> {
        buf_decode_value::<Vec<u8>, RawBuf>(bytes)
    }
}

impl ValueCodec for String {
    fn encode(&self) -> Vec<u8> {
        buf_encode_value::<String, Utf8>(self)
    }
    fn decode(bytes: &[u8]) -> Result<Self, ApiError> {
        buf_decode_value::<String, Utf8>(bytes)
    }
}

impl ValueCodec for u64 {
    fn encode(&self) -> Vec<u8> {
        buf_encode_value::<u64, BeU64>(self)
    }
    fn decode(bytes: &[u8]) -> Result<Self, ApiError> {
        buf_decode_value::<u64, BeU64>(bytes)
    }
}
