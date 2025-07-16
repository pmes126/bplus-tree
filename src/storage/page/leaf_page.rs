// A design for storing leaf nodes based on a Page-Local Heap
// [HEADER (fixed)]
// [RECORD OFFSETS: N * u16]
// [RECORD AREA: N × [klen][vlen][key][value]]
use std::io::{Error, ErrorKind};
use crate::layout::PAGE_SIZE;
use crate::layout::MAX_ENTRIES;
use crate::storage::page::LEAF_NODE_TAG;
use zerocopy::{AsBytes, FromBytes, FromZeroes};

pub const LEAF_NODE_VERSION: u8 = 0;
pub const HEADER_SIZE: usize = 12;

#[repr(C)]
#[derive(Clone, Copy, AsBytes, FromZeroes, FromBytes, Debug)]
pub struct LeafPageHeader {
    pub entry_count: u64,     // Number of key-value pairs
    pub next_node_id: u64,    // Right sibling node (if any)
    pub node_type: u64,       // Node type (LEAF_NODE_TAG)
    pub version: u64,         // Version of the leaf node
    pub free_start: u64,      // Offset for the next free space in the data area
}

#[repr(C)]
#[derive(Clone, Copy, AsBytes, FromZeroes, FromBytes, Debug)]
pub struct EntrySlots {
    pub offsets: [u16; MAX_ENTRIES], // Fixed-size array for entry slots
}

const HEADER_SIZE_BYTES: usize = std::mem::size_of::<LeafPageHeader>();
const ENTRY_SLOTS_SIZE: usize = std::mem::size_of::<EntrySlots>();
const DATA_SIZE: usize = PAGE_SIZE - HEADER_SIZE_BYTES - ENTRY_SLOTS_SIZE;
const LEN_VALUE_SIZE: usize = std::mem::size_of::<u16>();

#[repr(C)]
#[derive(Clone, Copy, AsBytes, FromZeroes, FromBytes, Debug)]
pub struct LeafData {
    pub blob: [u8; DATA_SIZE] // Fixed-size array for storing key-value pairs
}

#[repr(C)]
#[derive(Clone, Copy, AsBytes, FromZeroes, FromBytes, Debug)]
pub struct LeafPage {
    pub header: LeafPageHeader,
    pub slots: EntrySlots, // Slots for key-value pairs
    pub data: LeafData,
}

impl LeafPage {
    pub fn new() -> Self {
        LeafPage {
            header: LeafPageHeader {
                node_type: LEAF_NODE_TAG as u64,
                entry_count : 0,
                version: LEAF_NODE_VERSION as u64,
                next_node_id: 0, // Initially no right sibling
                free_start: 0,
            },
            slots: EntrySlots {
                offsets: [0u16; MAX_ENTRIES],
            },
            data: LeafData {
                blob: [0u8; DATA_SIZE],
            },
        }
    }

    pub fn from_bytes(buf: &[u8; PAGE_SIZE]) -> Result<&Self, Error> {
        LeafPage::ref_from(buf).ok_or(Error::new(ErrorKind::Other,"Invalid LeafPageRaw layout or alignment"))
    }

    pub fn is_full(&self) -> bool {
        self.header.entry_count >= MAX_ENTRIES as u64
    }


    // Insert values according to the Layout of [RECORD AREA: N × [klen][vlen][key][value]]
    pub fn insert_entry(&mut self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        if self.header.entry_count as usize >= MAX_ENTRIES {
            return Err(Error::new(ErrorKind::Other, "Leaf page full"));
        }

        let required_space = key.len() + value.len() + LEN_VALUE_SIZE * 2; // key_len +
        // value_len
        if self.header.free_start + required_space as u64 > DATA_SIZE as u64 {
            return Err(Error::new(ErrorKind::Other, "Not enough space in page"));
        }
        let data = &mut self.data.blob[..];

        // Current entry index of entry_count has offset at the free_start position
        self.slots.offsets[self.header.entry_count as usize] = self.header.free_start as u16;

        // Calculate offsets and lengths for key and value
        // Write the key length
        let key_len_offset = self.header.free_start as usize;
        let key_len = key.len() as u16;
        let raw = key_len.to_le_bytes();
        let end = key_len_offset + raw.len();

        data[key_len_offset..end].copy_from_slice(raw.as_ref());
        self.header.free_start += raw.len() as u64;

        // Write the value length
        let value_len_offset = self.header.free_start as usize;
        let value_len = value.len() as u16;
        let raw = value_len.to_le_bytes();
        let end = value_len_offset + raw.len();

        data[value_len_offset..end].copy_from_slice(raw.as_ref());
        self.header.free_start += raw.len() as u64;

        // Write the key
        let key_offset = self.header.free_start as usize;
        let raw = key.as_ref();
        let end = key_offset + raw.len();

        data[key_offset..end].copy_from_slice(raw);
        self.header.free_start += raw.len() as u64;

        // Write the value
        let value_offset = self.header.free_start as usize;
        let raw = value.as_ref();
        let end = value_offset + raw.len();

        data[value_offset..end].copy_from_slice(raw);
        self.header.free_start += raw.len() as u64;

        self.header.entry_count += 1;
        Ok(())
    }

    pub fn get_entry(&self, idx: usize) -> Result<(&[u8], &[u8]), Error> {
        if idx >= self.header.entry_count as usize {
            return Err(Error::new(ErrorKind::InvalidInput, "Index out of bounds"));
        }

        let key_len_offset = self.slots.offsets[idx] as usize;
        let arr: [u8; LEN_VALUE_SIZE] = self.data.blob[key_len_offset..(key_len_offset + LEN_VALUE_SIZE)].try_into().map_err(|_| Error::new(ErrorKind::Other, "Invalid key length slice"))?;
        let key_length = u16::from_le_bytes(arr);

        let value_len_offset = key_len_offset + LEN_VALUE_SIZE;
        let arr: [u8; LEN_VALUE_SIZE] = self.data.blob[value_len_offset..(value_len_offset + LEN_VALUE_SIZE)].try_into().map_err(|_| Error::new(ErrorKind::Other, "Invalid value length slice"))?;
        let value_length = u16::from_le_bytes(arr);

        let key_offset = value_len_offset + LEN_VALUE_SIZE;
        let key = &self.data.blob[key_offset..(key_offset + key_length as usize)];

        let value_offset = key_offset + key_length as usize;
        let value = &self.data.blob[value_offset..(value_offset + value_length as usize)];

        Ok((key, value))
    }

    pub fn len(&self) -> usize {
        self.header.entry_count as usize
    }

    pub fn to_bytes(&self) -> Result<&[u8; PAGE_SIZE], std::array::TryFromSliceError> {
        <&[u8; 4096]>::try_from(self.as_bytes())
    }
}
