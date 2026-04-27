//! Storage abstractions for the B+ tree engine.
//!
//! Defines [`PageStorage`], [`NodeStorage`], and [`HasEpoch`] — the three traits
//! that all concrete storage backends must implement.

use crate::bplustree::{NodeId, NodeView};
use crate::codec::CodecError;
use crate::layout::PAGE_SIZE;
use crate::storage::epoch::EpochManager;

use std::path::Path;

pub mod epoch;
pub mod file_page_storage;
pub mod metadata_manager;
pub mod paged_node_storage;

use thiserror::Error;

/// Errors that can be returned by the storage layer.
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum StorageError {
    /// An underlying I/O error from the file or page backend.
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// A node could not be encoded or decoded.
    #[error(transparent)]
    Codec(#[from] CodecError),
    /// A requested page or node ID was not found.
    #[error("page {pid} not found")]
    NotFound { pid: NodeId },
    /// An internal storage invariant was violated.
    #[error("storage invariant violated: {0}")]
    Invariant(&'static str),
}

/// Provides access to the shared [`EpochManager`] owned by a storage instance.
pub trait HasEpoch {
    /// Returns a reference to the epoch manager.
    fn epoch_mgr(&self) -> &std::sync::Arc<EpochManager>;
}

/// Unified page storage interface for the B+ tree engine.
pub trait PageStorage {
    /// Initializes the storage, creating necessary files or structures
    fn open<P: AsRef<Path>>(path: P) -> Result<Self, std::io::Error>
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

    /// Allocates a new, unused page ID
    fn allocate_page(&self) -> Result<u64, std::io::Error>;

    /// Frees a page ID for future reuse
    fn free_page(&self, page_id: u64) -> Result<(), std::io::Error>;

    /// Closes the storage, flushing any pending writes.
    fn close(&self) -> Result<(), std::io::Error>;

    /// Sets the next page ID to use for allocation.
    fn set_next_page_id(&self, next_page_id: u64) -> Result<(), std::io::Error>;

    /// Replaces the freelist with the given list of freed page IDs.
    fn set_freelist(&self, freed_pages: Vec<u64>) -> Result<(), std::io::Error>;

    /// Returns the current next page ID (allocation high-water mark).
    fn get_next_page_id(&self) -> u64;

    /// Returns a snapshot of the current freelist.
    fn get_freelist(&self) -> Vec<u64>;
}

/// Unified node storage interface for reading and writing encoded B+ tree nodes.
pub trait NodeStorage: Send + Sync + 'static {
    /// Reads a node view (undecoded) from storage by its ID
    fn read_node_view(&self, id: u64) -> Result<Option<NodeView>, StorageError>;

    /// Writes a node view (encoded) to storage by its ID
    fn write_node_view(&self, node_view: &NodeView) -> Result<u64, StorageError>;

    /// Writes a node view (encoded) to storage by its ID at a specific offset
    fn write_node_view_at_offset(
        &self,
        node_view: &NodeView,
        offset: u64,
    ) -> Result<u64, StorageError>;

    /// Flushes any cached writes to persistent storage
    fn flush(&self) -> Result<(), StorageError>;

    /// Frees a node by its ID
    fn free_node(&self, id: u64) -> Result<(), StorageError>;
}
