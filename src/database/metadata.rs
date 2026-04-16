//! On-disk metadata page layout for a single B+ tree.
//!
//! Each tree reserves two fixed-size slots (A and B) in the data file for its
//! [`MetadataPage`].  Commits alternate between the slots so a crash always
//! leaves at least one valid copy.

use crate::bplustree::NodeId;
use crate::layout::PAGE_SIZE;
use std::io::{self};

use zerocopy::{AsBytes, FromBytes, FromZeroes};

/// Number of padding bytes appended to [`MetadataPage`] to fill a full page.
pub const PADDING_SIZE: usize = PAGE_SIZE - (std::mem::size_of::<Metadata>());

/// Committed state of a single B+ tree, stored as a fixed-size C struct.
#[repr(C)]
#[derive(AsBytes, FromBytes, FromZeroes, Debug, Clone, Copy)]
pub struct Metadata {
    /// Page ID of the current root node.
    pub root_node_id: NodeId,
    /// Stable numeric identifier for this tree (never reused).
    pub id: u64,
    /// Monotonically increasing transaction counter; incremented on every commit.
    pub txn_id: u64,
    /// Height of the B+ tree (0 = empty, 1 = root leaf only).
    pub height: u64,
    /// Branching factor / order of the B+ tree.
    pub order: u64,
    /// Approximate number of key-value entries in the tree.
    pub size: u64,
    /// CRC32 checksum covering all fields except this one.
    pub checksum: u64,
}

/// A [`Metadata`] value padded to exactly [`PAGE_SIZE`] bytes for direct I/O.
#[repr(C)]
#[derive(AsBytes, FromBytes, FromZeroes, Debug, Clone, Copy)]
pub struct MetadataPage {
    /// The tree metadata payload.
    pub data: Metadata,
    _padding: [u8; PADDING_SIZE],
}

/// Constructs a [`MetadataPage`] from individual field values.
pub fn new_metadata_page(
    root_id: u64,
    id: u64,
    txn_id: u64,
    checksum: u64,
    height: u64,
    order: u64,
    size: u64,
) -> MetadataPage {
    MetadataPage {
        data: Metadata {
            root_node_id: root_id,
            id,
            txn_id,
            height,
            order,
            size,
            checksum,
        },
        _padding: [0; PADDING_SIZE],
    }
}

/// Constructs a [`MetadataPage`] by wrapping an existing [`Metadata`] value.
pub fn new_metadata_page_with_object(meta: &Metadata) -> MetadataPage {
    MetadataPage {
        data: *meta,
        _padding: [0; PADDING_SIZE],
    }
}

impl MetadataPage {
    /// Interprets a raw page buffer as a [`MetadataPage`] reference.
    ///
    /// Returns an error if the buffer is misaligned or the wrong size.
    pub fn from_bytes(buf: &[u8; PAGE_SIZE]) -> Result<&Self, std::io::Error> {
        MetadataPage::ref_from(buf).ok_or(io::Error::new(
            io::ErrorKind::InvalidData,
            "Failed to decode MetadataPage",
        ))
    }
}

/// Computes the CRC32 checksum of a [`MetadataPage`], excluding the checksum field itself.
pub fn calculate_checksum(meta: &MetadataPage) -> u64 {
    use crc32fast::Hasher;
    let bytes = meta.data.as_bytes();
    let without_checksum = &bytes[..bytes.len() - (std::mem::size_of::<u64>())];
    let mut hasher = Hasher::new();
    hasher.update(without_checksum);
    hasher.finalize() as u64
}
