pub mod bincode;

use crate::bplustree::node::Node;
use crate::layout::PAGE_SIZE;
use thiserror::Error;

/// Trait for node storage operations
pub trait KeyCodec {
    fn encode_key(&self, out: &mut [u8]) -> Result<usize, CodecError>;
    fn decode_key(buf: &[u8]) -> Self
    where
        Self: Sized;
    fn compare_encoded(a: &[u8], b: &[u8]) -> std::cmp::Ordering;
    fn encoded_len(&self) -> usize;
}

pub trait ValueCodec {
    fn encode_value(&self, out: &mut [u8]) -> Result<usize, CodecError>;
    fn decode_value(buf: &[u8]) -> Self
    where
        Self: Sized;
    fn encoded_len(&self) -> usize;
}

pub trait NodeCodec<K, V>
where
    K: KeyCodec + Ord,
    V: ValueCodec,
{
    fn encode(node: &Node<K, V>) -> Result<[u8; PAGE_SIZE], CodecError>;
    fn decode(buf: &[u8; PAGE_SIZE]) -> Result<Node<K, V>, CodecError>;
}

#[derive(Debug, Error)]
pub enum CodecError {
    #[error("Error decoding value: {msg}")]
    DecodeFailure { msg: String },

    #[error("Error encoding value: {msg}")]
    EncodeFailure { msg: String },

    #[error("Error converting from byte slice: {source}")]
    FromSliceError {
        #[from]
        source: std::array::TryFromSliceError,
    },

    #[error("IO error: {source}")]
    Io {
        #[from]
        source: std::io::Error,
    },
}
