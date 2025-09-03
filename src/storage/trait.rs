use crate::bplustree::{Node, NodeView};
use crate::layout::PAGE_SIZE;
use crate::storage::codec::CodecError;
use crate::storage::metadata::{Metadata, MetadataPage};
use anyhow::Result;
use std::path::Path;

/// Unified storage interface for B+ tree logic
pub trait PageStorage {
    /// Initializes the storage, creating necessary files or structures
    fn init<P: AsRef<Path>>(path: P) -> Result<Self, std::io::Error>
    where
        Self: Sized;

    /// Reads a page by ID into a fixed 4KB buffer
    fn read_page(&self, page_id: u64, target: &mut [u8; PAGE_SIZE]) -> Result<(), std::io::Error>;

    /// Writes a full 4KB page to disk and returns the offset
    fn write_page(&self, data: &[u8]) -> Result<u64, std::io::Error>;

    /// Writes a full 4KB page to disk at the given offset
    fn write_page_at_offset(&self, offset: u64, data: &[u8]) -> Result<u64, std::io::Error>;

    /// Ensures all writes are flushed to disk
    fn flush(&self) -> Result<(), std::io::Error>;

    /// Optional: allocates a new, unused page ID
    fn allocate_page(&self) -> Result<u64, std::io::Error>;

    fn free_page(&self, page_id: u64) -> Result<(), std::io::Error>;
}

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

pub trait NodeStorage<K, V>
where
    K: KeyCodec,
    V: ValueCodec,
{
    /// Reads a node from storage by its ID
    fn read_node(&self, id: u64) -> Result<Option<Node<K, V>>, anyhow::Error>;

    /// Writes a node to storage
    fn write_node(&self, node: &Node<K, V>) -> Result<u64, anyhow::Error>;

    /// Reads a node view (undecoded) from storage by its ID
    fn read_node_view(&self, id: u64) -> Result<Option<NodeView>, anyhow::Error>;

    /// Writes a node view (encoded) to storage by its ID
    fn write_node_view(&self, node_view: &NodeView) -> Result<u64, anyhow::Error>;

    /// Flushes any cached writes to persistent storage
    fn flush(&self) -> Result<(), std::io::Error>;

    /// Frees a node by its ID
    fn free_node(&self, id: u64) -> Result<(), std::io::Error>;
}

pub trait MetadataStorage {
    /// Reads metadata from a specific slot
    fn read_metadata(&self, slot: u8) -> Result<MetadataPage, std::io::Error>;

    /// Writes metadata to a specific slot
    fn write_metadata(&self, slot: u8, meta: &mut MetadataPage) -> Result<(), std::io::Error>;

    /// Reads the current root node ID from metadata
    fn read_current_root(&self) -> Result<u64, std::io::Error>;

    // Get the current metadata
    fn get_metadata(&self) -> Result<Metadata, std::io::Error>;

    // Commits the provided metadata to the oldest metadata slot and advances the transaction ID
    fn commit_metadata(
        &self,
        slot: u8,
        txn_id: u64,
        root: u64,
        height: usize,
        order: usize,
        size: usize,
    ) -> Result<(), std::io::Error>;

    // Commit metadata with a metadata object
    fn commit_metadata_with_object(
        &self,
        slot: u8,
        metadata: &Metadata,
    ) -> Result<(), std::io::Error>;
}
