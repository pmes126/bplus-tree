pub mod cache;
pub mod file_store;
pub mod page_store;
pub mod codec;
pub mod page;
pub mod metadata;

use crate::bplustree::Node;
use crate::layout::PAGE_SIZE;
use crate::storage::metadata::MetadataPage;

use std::io::Result;
/// Unified storage interface for B+ tree logic
pub trait PageStorage {
    /// Reads a page by ID into a fixed 4KB buffer
    fn read_page(&mut self, page_id: u64) -> Result<[u8; PAGE_SIZE]>;

    /// Writes a full 4KB page to disk and returns the offset
    fn write_page(&mut self, data: &[u8]) -> Result<u64>;
    
    /// Writes a full 4KB page to disk at the given offset
    fn write_page_at_offset(&mut self, offset: u64, data: &[u8]) -> Result<u64>;

    /// Ensures all writes are flushed to disk
    fn flush(&mut self) -> Result<()>;

    /// Optional: allocates a new, unused page ID
    fn allocate_page(&mut self) -> Result<u64>;
    
    fn free_page(&mut self, page_id: u64) -> Result<()>;
}

/// Trait for node storage operations
pub trait KeyCodec {
    fn encode_key(&self) -> &[u8];
    fn decode_key(buf: &[u8]) -> Self
    where
        Self: Sized;
    fn compare_encoded(a: &[u8], b: &[u8]) -> std::cmp::Ordering;
}

pub trait ValueCodec {
    fn encode_value(&self) -> &[u8];
    fn decode_value(buf: &[u8]) -> Self
    where
        Self: Sized;
}

pub trait NodeCodec<K, V>
where
    K: KeyCodec + Copy + Ord,
    V: ValueCodec + Copy,
{
    fn encode(node: &Node<K, V>) -> [u8; PAGE_SIZE];
    fn decode(buf: &[u8; PAGE_SIZE]) -> Node<K, V>;
}

pub trait NodeStorage<K, V>
where
    K: KeyCodec,
    V: ValueCodec,
{
    /// Reads a node from storage by its ID
    fn read_node(&mut self, id: u64) -> Result<Option<Node<K, V>>>;

    /// Writes a node to storage
    fn write_node(&mut self, node: &Node<K, V>) -> Result<u64>;

    /// Flushes any cached writes to persistent storage
    fn flush(&mut self) -> Result<()>;
}

pub trait MetadataStorage {
    /// Reads metadata from a specific slot
    fn read_meta(&mut self, slot: u8) -> Result<MetadataPage>;

    /// Writes metadata to a specific slot
    fn write_meta(&mut self, slot: u8, meta: &MetadataPage) -> Result<()>;

    /// Reads the current root node ID from metadata
    fn read_current_root(&mut self) -> Result<u64>;

    /// Commits a new root node ID to the metadata
    fn commit_root(&mut self, new_root: u64) -> Result<()>;
}
