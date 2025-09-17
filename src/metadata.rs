use crate::bplustree::NodeId;
use crate::layout::PAGE_SIZE;
use std::io::{self};

use zerocopy::{AsBytes, FromBytes, FromZeroes};

pub const METADATA_PAGE_1: u8 = 0x00; // First metadata page slot
pub const METADATA_PAGE_2: u8 = 0x01; // Second metadata page slot
pub const INITIAL_PAGE_ID: u8 = 0x02; // Second metadata page slot
pub const PADDING_SIZE: usize = PAGE_SIZE - (std::mem::size_of::<Metadata>());

// Metadata structure for the B+ tree
#[derive(AsBytes, FromBytes, FromZeroes, Debug, Clone, Copy)]
#[repr(C)]
pub struct Metadata {
    pub root_node_id: NodeId,
    pub txn_id: u64,
    pub height: usize, // Height of the B+ tree
    pub order: usize,  // Order of the B+ tree
    pub size: usize,   // Size of the B+ tree
    pub checksum: u64, // Checksum for integrity verification
}

#[repr(C)]
#[derive(AsBytes, FromBytes, FromZeroes, Debug, Clone, Copy)]
pub struct MetadataPage {
    pub data: Metadata,           // Metadata structure
    _padding: [u8; PADDING_SIZE], // Padding to fill the rest of the page
}

pub fn new_metadata_page(
    root_id: u64,
    txn_id: u64,
    checksum: u64,
    height: usize,
    order: usize,
    size: usize,
) -> MetadataPage {
    MetadataPage {
        data: Metadata {
            root_node_id: root_id, // Initial root node ID
            txn_id,
            height,
            order,
            size,
            checksum,
        },
        _padding: [0; PADDING_SIZE], // Fill the rest of the page with zeros
    }
}

pub fn new_metadata_page_with_object(meta: &Metadata) -> MetadataPage {
    MetadataPage {
        data: *meta,
        _padding: [0; PADDING_SIZE], // Fill the rest of the page with zeros
    }
}

impl MetadataPage {
    pub fn from_bytes(buf: &[u8; PAGE_SIZE]) -> Result<&Self, std::io::Error> {
        MetadataPage::ref_from(buf).ok_or(io::Error::new(
            io::ErrorKind::InvalidData,
            "Failed to decode MetadataPage",
        ))
    }
}

pub fn calculate_checksum(meta: &MetadataPage) -> u64 {
    use crc32fast::Hasher;
    let bytes = meta.data.as_bytes();
    let without_checksum = &bytes[..bytes.len() - (std::mem::size_of::<u64>())];
    let mut hasher = Hasher::new();
    hasher.update(without_checksum);
    hasher.finalize() as u64
}
