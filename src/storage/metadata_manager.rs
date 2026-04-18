//! Utilities for reading and writing double-buffered B+ tree metadata pages.

use thiserror::Error;
use zerocopy::AsBytes;

use crate::database::metadata::{
    Metadata, MetadataPage, calculate_checksum, new_metadata_page, new_metadata_page_with_object,
};
use crate::layout::PAGE_SIZE;
use crate::storage::PageStorage;

/// Errors that can occur when reading or writing a metadata page.
#[derive(Debug, Error)]
pub enum MetadataError {
    /// An underlying I/O error from the page backend.
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// The stored checksum does not match the computed checksum.
    #[error("metadata checksum mismatch")]
    ChecksumMismatch,
    /// The raw page bytes could not be interpreted as a metadata page.
    #[error("corrupt metadata page: {0}")]
    Corrupt(String),
}

/// Reads, writes, and commits double-buffered tree metadata pages.
pub struct MetadataManager;

impl MetadataManager {
    /// Reads the metadata page at `slot`, validates its checksum, and returns it.
    fn read_metadata<S: PageStorage>(
        storage: &S,
        slot: u64,
    ) -> Result<MetadataPage, MetadataError> {
        let mut buf = [0u8; PAGE_SIZE];
        storage.read_page(slot, &mut buf)?;

        let metadata =
            MetadataPage::from_bytes(&buf).map_err(|e| MetadataError::Corrupt(e.to_string()))?;

        let checksum = metadata.data.checksum;
        let calculated_checksum = calculate_checksum(metadata);
        if checksum != calculated_checksum {
            return Err(MetadataError::ChecksumMismatch);
        }
        Ok(*metadata)
    }

    /// Calculates the checksum and writes the metadata page to `slot`.
    pub fn write_metadata<S: PageStorage>(
        storage: &S,
        slot: u64,
        meta: &mut MetadataPage,
    ) -> Result<(), MetadataError> {
        let checksum = calculate_checksum(meta);
        meta.data.checksum = checksum;
        let buf = meta.as_bytes();
        storage.write_page_at_offset(slot, buf)?;
        Ok(())
    }

    /// Reads both metadata slots and returns the one with the higher transaction ID.
    pub fn read_active_meta<S: PageStorage>(
        storage: &S,
        meta_a: u64,
        meta_b: u64,
    ) -> Result<Metadata, MetadataError> {
        let meta0 = Self::read_metadata(storage, meta_a)?;
        let meta1 = Self::read_metadata(storage, meta_b)?;
        let active_meta = if meta0.data.txn_id >= meta1.data.txn_id {
            meta0
        } else {
            meta1
        };
        Ok(active_meta.data)
    }

    /// Returns the metadata with the higher transaction ID without validating the checksum.
    pub fn get_metadata<S: PageStorage>(
        storage: &S,
        meta_a: u64,
        meta_b: u64,
    ) -> Result<Metadata, MetadataError> {
        let meta0 = Self::read_metadata(storage, meta_a)?;
        let meta1 = Self::read_metadata(storage, meta_b)?;
        if meta0.data.txn_id >= meta1.data.txn_id {
            Ok(meta0.data)
        } else {
            Ok(meta1.data)
        }
    }

    /// Constructs a metadata page from individual fields and writes it to `slot`.
    #[allow(clippy::too_many_arguments)]
    pub fn commit_metadata<S: PageStorage>(
        storage: &S,
        slot: u64,
        txn_id: u64,
        id: u64,
        root: u64,
        height: u64,
        order: u64,
        size: u64,
    ) -> Result<(), MetadataError> {
        let mut metadata_page = new_metadata_page(root, txn_id, id, 0, height, order, size);
        Self::write_metadata(storage, slot, &mut metadata_page)
    }

    /// Writes a pre-built [`Metadata`] object to `slot`, calculating its checksum first.
    pub fn commit_metadata_with_object<S: PageStorage>(
        storage: &S,
        slot: u64,
        metadata: &Metadata,
    ) -> Result<(), MetadataError> {
        let mut metadata_page = new_metadata_page_with_object(metadata);
        Self::write_metadata(storage, slot, &mut metadata_page)
    }

    /// Allocates two metadata pages and writes initial metadata to both.
    #[allow(dead_code)]
    fn bootstrap_metadata<S: PageStorage>(
        storage: &S,
        id: u64,
        order: u64,
    ) -> Result<(u64, u64, Metadata), MetadataError> {
        let initial_txn_id = 1;
        let meta_a = storage.allocate_page()?;
        let meta_b = storage.allocate_page()?;
        let root_node_id = storage.allocate_page()?;

        let mut metadata_page_a =
            new_metadata_page(root_node_id, id, initial_txn_id, 0, 1, order, 0);
        let mut metadata_page_b =
            new_metadata_page(root_node_id, id, initial_txn_id - 1, 0, 1, order, 0);

        Self::write_metadata(storage, meta_a, &mut metadata_page_a)?;
        Self::write_metadata(storage, meta_b, &mut metadata_page_b)?;

        Ok((meta_a, meta_b, metadata_page_a.data))
    }
}
