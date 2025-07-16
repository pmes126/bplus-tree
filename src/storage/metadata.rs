use crate::bplustree::NodeId;
use crate::layout::{PAGE_SIZE};

pub const METADATA_TAG: u8 = 0xFF; // Metadata page tag
pub const ACTIVE_FLAG: u8 = 0x01; // Active flag for metadata page
pub const METADATA_PAGE_1: u8 = 0x00; // First metadata page slot
pub const METADATA_PAGE_2: u8 = 0x01; // Second metadata page slot
pub const INITIAL_PAGE_ID: u8 = 0x02; // Second metadata page slot
pub const PADDING_SIZE: usize = PAGE_SIZE - (std::mem::size_of::<Metadata>() + 1);

// Metadata structure for the B+ tree
#[repr(C)]
#[derive(Clone, Copy)]
pub struct Metadata {
    pub root_node_id: NodeId,
    pub txn_id: u64,
    pub checksum: u64,
    pub order: u8, // Order of the B+ tree
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct MetadataPage {
    pub data: Metadata,
    pub _padding: [u8; PADDING_SIZE],
}

pub fn new_metadata_page(root_id: u64, txn_id: u64, checksum: u64, order: u8) -> MetadataPage {
    MetadataPage {
        data: Metadata {
            root_node_id: root_id, // Initial root node ID
            txn_id, // Initial transaction ID
            checksum, // Placeholder for checksum
            order,
        },
        _padding: [0; PADDING_SIZE],
    }
}
