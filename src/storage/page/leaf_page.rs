use crate::layout::PAGE_SIZE;
use crate::layout::MAX_ENTRIES;
use crate::storage::page::LEAF_NODE_TAG;
use crate::storage::page::PageCodecError;
use zerocopy::{AsBytes, FromBytes, FromZeroes};

pub const LEAF_NODE_VERSION: u8 = 0;

// A design for storing leaf nodes based on a Page-Local Heap
// [HEADER (fixed)]
// [RECORD OFFSETS: N * u16]
// [RECORD AREA: N × [klen][vlen][key][value]]
#[repr(C)]
#[derive(Clone, Copy, AsBytes, FromZeroes, FromBytes, Debug)]
pub struct LeafPageHeader {
    pub node_type: u64,       // Node type (LEAF_NODE_TAG)
    pub entry_count: u64,     // Number of key-value pairs
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

    pub fn from_bytes(buf: &[u8; PAGE_SIZE]) -> Result<&Self, PageCodecError> {
        LeafPage::ref_from(buf).ok_or(PageCodecError::FromBytesError{ msg: "Failed to convert bytes to LeafPage".to_string() })
    }

    pub fn is_full(&self) -> bool {
        self.header.entry_count >= MAX_ENTRIES as u64
    }


    // Insert values according to the Layout of [RECORD AREA: N × [klen][vlen][key][value]]
    pub fn insert_entry(&mut self, key: &[u8], value: &[u8]) -> Result<(), PageCodecError> {
        if self.header.entry_count as usize >= MAX_ENTRIES {
            return Err(PageCodecError::PageFull{
                msg: "LeafPage is full, cannot insert more entries".to_string(),
            });
        }

        let required_space = key.len() + value.len() + LEN_VALUE_SIZE * 2; // key_len +
        // value_len
        if self.header.free_start + required_space as u64 > DATA_SIZE as u64 {
            return Err(PageCodecError::PageFull{
                msg: "LeafPage is full, cannot insert more entries".to_string(),
            });
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

    pub fn get_entry(&self, idx: usize) -> Result<(&[u8], &[u8]), PageCodecError> {
        if idx >= self.header.entry_count as usize {
            return Err(PageCodecError::IndexOutOfBounds{
                msg: "Index out of bounds".to_string(),
            });
        }

        let key_len_offset = self.slots.offsets[idx] as usize;
        let arr: [u8; LEN_VALUE_SIZE] = self.data.blob[key_len_offset..(key_len_offset + LEN_VALUE_SIZE)].try_into().
            map_err(|_| PageCodecError::FromBytesError{ msg: "Failed to convert bytes to LeafPage".to_string() })?;

        let key_length = u16::from_le_bytes(arr);

        let value_len_offset = key_len_offset + LEN_VALUE_SIZE;
        let arr: [u8; LEN_VALUE_SIZE] = self.data.blob[value_len_offset..(value_len_offset + LEN_VALUE_SIZE)].try_into().
            map_err(|_| PageCodecError::FromBytesError{ msg: "Failed to read bytes as slice".to_string() })?;
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
        let bytes: &[u8] = self.as_bytes(); // borrow lives for the function scope
        let array: &[u8; PAGE_SIZE] = bytes.try_into()?; // also scoped
        Ok(array)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_leaf_page() {
        let mut page = LeafPage::new();
        let key = b"test_key";
        let val = b"test_value";

        // Insert an entry
        assert!(page.insert_entry(key, val).is_ok());

        // Retrieve the entry
        let (retrieved_key, retrieved_value) = page.get_entry(0).unwrap();
        assert_eq!(retrieved_key, key);
        assert_eq!(retrieved_value, val);
    }

    #[test]
    fn test_internal_page_multiples() {
        let mut page = LeafPage::new();
        let keys = ["key1", "key2key2", "key3key3key3"];
        let values = ["value1", "value2value2", "value3value3value3"];

        // Insert multiple entries
        for (&key, &value) in keys.iter().zip(&values) {
            assert!(page.insert_entry(key.as_bytes(), value.as_bytes()).is_ok());
        }

        // Retrieve the entries
        for (i, key) in keys.iter().enumerate() {
            let (retrieved_key, retrieved_value) = page.get_entry(i).unwrap();
            assert_eq!(retrieved_key, key.as_bytes());
            assert_eq!(retrieved_value, values[i].as_bytes());
        }
    }
}
