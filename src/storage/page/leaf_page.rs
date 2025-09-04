use crate::layout::MAX_ENTRIES;
use crate::layout::PAGE_SIZE;
use crate::storage::page::LEAF_NODE_TAG;
use crate::storage::page::PageCodecError;
use zerocopy::{AsBytes, FromBytes, FromZeroes};

pub const LEAF_NODE_VERSION: u8 = 0;

// A design for storing leaf nodes based on a Page-Local Heap
// [HEADER (fixed)]
// [ENTRY SLOTS (KV OFFSETS): N * u16]
// [RECORD AREA: N × [klen][vlen][key][value]]
#[repr(C)]
#[derive(Clone, Copy, AsBytes, FromZeroes, FromBytes, Debug)]
pub struct LeafPageHeader {
    pub node_type: u64,   // Node type (LEAF_NODE_TAG)
    pub entry_count: u64, // Number of key-value pairs
    pub version: u64,     // Version of the leaf node
    pub free_start: u64,  // Offset for the next free space in the data area
}

#[repr(C)]
#[derive(Clone, Copy, AsBytes, FromZeroes, FromBytes, Debug)]
pub struct EntrySlots {
    pub offsets: [u16; MAX_ENTRIES], // Fixed-size array for entry slots
}

const HEADER_SIZE_BYTES: usize = std::mem::size_of::<LeafPageHeader>();
const ENTRY_SLOTS_SIZE: usize = std::mem::size_of::<EntrySlots>();
const DATA_SIZE: usize = PAGE_SIZE - HEADER_SIZE_BYTES - ENTRY_SLOTS_SIZE;
const LEN_SIZE: usize = std::mem::size_of::<u16>();

#[repr(C)]
#[derive(Clone, Copy, AsBytes, FromZeroes, FromBytes, Debug)]
pub struct LeafData {
    pub blob: [u8; DATA_SIZE], // Fixed-size array for storing key-value pairs
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
                node_type: LEAF_NODE_TAG,
                entry_count: 0,
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
        LeafPage::ref_from(buf).ok_or(PageCodecError::FromBytesError {
            msg: "Failed to convert bytes to LeafPage".to_string(),
        })
    }

    pub fn is_full(&self) -> bool {
        self.header.entry_count >= MAX_ENTRIES as u64
    }

    // Insert values according to the Layout of [RECORD AREA: N × [klen][vlen][key][value]]
    pub fn insert_entry(&mut self, key: &[u8], value: &[u8]) -> Result<(), PageCodecError> {
        if self.header.entry_count as usize >= MAX_ENTRIES {
            return Err(PageCodecError::PageFull {
                msg: "LeafPage is full, cannot insert more entries".to_string(),
            });
        }

        let required_space = key.len() + value.len() + LEN_SIZE * 2;
        // value_len
        if self.header.free_start + required_space as u64 > DATA_SIZE as u64 {
            return Err(PageCodecError::PageFull {
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
        let raw = key;
        let end = key_offset + raw.len();

        data[key_offset..end].copy_from_slice(raw);
        self.header.free_start += raw.len() as u64;

        // Write the value
        let value_offset = self.header.free_start as usize;
        let raw = value;
        let end = value_offset + raw.len();

        data[value_offset..end].copy_from_slice(raw);
        self.header.free_start += raw.len() as u64;

        self.header.entry_count += 1;
        Ok(())
    }

    // Insert value at a specific index according to the Layout of [RECORD AREA: N × [klen][vlen][key][value]]
    pub fn insert_entry_at(&mut self, idx: usize, key: &[u8], value: &[u8]) -> Result<(), PageCodecError> {
        if idx > self.header.entry_count as usize {
            return Err(PageCodecError::IndexOutOfBounds {
                msg: "Provided insertion index is beyond entries size".to_string(),
            });
        }

        if self.header.entry_count as usize >= MAX_ENTRIES {
            return Err(PageCodecError::PageFull {
                msg: "LeafPage is full, cannot insert more entries".to_string(),
            });
        }

        let required_space = key.len() + value.len() + LEN_SIZE * 2;

        if self.header.free_start + required_space as u64 > DATA_SIZE as u64 {
            return Err(PageCodecError::PageFull {
                msg: "LeafPage is full, cannot insert more entries".to_string(),
            });
        }

        // We need to memcpy the contents from idx to idx + required space and write the key value
        // pair in the space between
        let insertion_point = self.slots.offsets[idx] as usize;
        let shift_count = self.header.free_start as usize - insertion_point;
        let src_idx = insertion_point; 
        let dst_idx = src_idx + required_space;
        self.data.blob.copy_within(src_idx..src_idx+shift_count, dst_idx);

        // All values from idx onwards should be shifted by 1 position to the right and have required_space
        // added to them
        let shift_count = self.header.entry_count as usize - idx;
        let src_idx = idx;
        let end_idx = src_idx + shift_count;
        let dest_idx = src_idx + 1;
        self.slots.offsets.copy_within(src_idx..end_idx, dest_idx);
        for i in dest_idx..dest_idx + shift_count {
            self.slots.offsets[i] += required_space as u16;
        }
        
        
        self.header.free_start += required_space as u64;

        let data = &mut self.data.blob[..];

        // Write the key length
        let key_len_offset = insertion_point;
        let key_len = key.len() as u16;
        let raw = key_len.to_le_bytes();
        let end = key_len_offset + raw.len();

        data[key_len_offset..end].copy_from_slice(raw.as_ref());

        // Write the value length
        let value_len_offset = end;
        let value_len = value.len() as u16;
        let raw = value_len.to_le_bytes();
        let end = value_len_offset + raw.len();

        data[value_len_offset..end].copy_from_slice(raw.as_ref());

        // Write the key
        let key_offset = end;
        let raw = key;
        let end = key_offset + raw.len();

        data[key_offset..end].copy_from_slice(raw);

        // Write the value
        let value_offset = end;
        let raw = value;
        let end = value_offset + raw.len();

        data[value_offset..end].copy_from_slice(raw);

        // Adjust count
        self.header.entry_count += 1;
        Ok(())
    }

    pub fn get_entry(&self, idx: usize) -> Result<(&[u8], &[u8]), PageCodecError> {
        if idx >= self.header.entry_count as usize {
            return Err(PageCodecError::IndexOutOfBounds {
                msg: "Index out of bounds".to_string(),
            });
        }

        let key_len_offset = self.slots.offsets[idx] as usize;
        let arr: [u8; LEN_SIZE] = self.data.blob[key_len_offset..(key_len_offset + LEN_SIZE)]
            .try_into()
            .map_err(|_| PageCodecError::FromBytesError {
                msg: "Failed to convert bytes to LeafPage".to_string(),
            })?;

        let key_length = u16::from_le_bytes(arr) as usize;

        let value_len_offset = key_len_offset + LEN_SIZE;
        let arr: [u8; LEN_SIZE] = self.data.blob[value_len_offset..(value_len_offset + LEN_SIZE)]
            .try_into()
            .map_err(|_| PageCodecError::FromBytesError {
                msg: "Failed to read bytes as slice".to_string(),
            })?;
        let value_length = u16::from_le_bytes(arr);

        let key_offset = value_len_offset + LEN_SIZE;
        let key = &self.data.blob[key_offset..(key_offset + key_length)];

        let value_offset = key_offset + key_length;
        let value = &self.data.blob[value_offset..(value_offset + value_length as usize)];

        Ok((key, value))
    }

    /// Return key bytes at slot i (no decode) klen vlen key value
    /// according to the Layout of [RECORD AREA: N × [klen][vlen][key][value]]
    #[inline]
    pub fn key_bytes_at(&self, i: usize) -> Result<&[u8], std::array::TryFromSliceError> {
        let off = self.slots.offsets[i] as usize;
        let len = u16::from_le_bytes(self.data.blob[off..off + LEN_SIZE].try_into()?) as usize;
        let k0 = off + LEN_SIZE * 2;
        Ok(&self.data.blob[k0..k0 + len])
    }

    /// Return value bytes at slot i (no decode)
    #[inline]
    pub fn value_bytes_at(&self, i: usize) -> Result<&[u8], std::array::TryFromSliceError> {
        let key_len_offset = self.slots.offsets[i] as usize;
        let arr: [u8; LEN_SIZE] =
            self.data.blob[key_len_offset..(key_len_offset + LEN_SIZE)].try_into()?;

        let key_length = u16::from_le_bytes(arr) as usize;

        let value_len_offset = key_len_offset + LEN_SIZE;
        let arr: [u8; LEN_SIZE] =
            self.data.blob[value_len_offset..(value_len_offset + LEN_SIZE)].try_into()?;
        let value_length = u16::from_le_bytes(arr);

        let key_offset = value_len_offset + LEN_SIZE;

        let value_offset = key_offset + key_length;
        let value = &self.data.blob[value_offset..(value_offset + value_length as usize)];

        Ok(value)
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.header.entry_count as usize
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.header.entry_count == 0
    }

    #[inline]
    pub fn to_bytes(&self) -> Result<&[u8; PAGE_SIZE], std::array::TryFromSliceError> {
        let array: &[u8; PAGE_SIZE] = self.as_bytes().try_into()?; // also scoped
        Ok(array)
    }

    pub fn drain(&mut self, idx: usize) -> Result<(), PageCodecError> {
        if idx >= self.header.entry_count as usize {
            return Err(PageCodecError::IndexOutOfBounds {
                msg: "Index out of bounds".to_string(),
            });
        }

        // Simply adjust the entry count to "remove" entries from idx onwards
        self.header.entry_count = idx as u64;

        Ok(())
    }

    pub fn split_off(&mut self, idx: usize) -> Result<LeafPage, PageCodecError> {
        if idx >= self.header.entry_count as usize {
            return Err(PageCodecError::IndexOutOfBounds {
                msg: "Index out of bounds".to_string(),
            });
        }

        let mut new_page = LeafPage::new();

        // Move entries from idx to the end to the new page
        for i in idx..self.header.entry_count as usize {
            let (key, value) = self.get_entry(i)?;
            new_page.insert_entry(key, value)?;
        }

        // Update the free_start of the original page to the offset of the idx entry
        self.header.free_start = self.slots.offsets[idx] as u64;
        // Clear the data area from free_start to the end
        self.data.blob[self.header.free_start as usize..].fill(0);
        // Clear old offsets
        self.slots.offsets[idx..self.header.entry_count as usize]
            .fill(0);
        // Adjust the entry count of the original page
        self.drain(idx)?;

        Ok(new_page)
    }
}

impl Default for LeafPage {
    fn default() -> Self {
        LeafPage::new()
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
    fn test_leaf_page_multiples() {
        let mut page = LeafPage::new();
        let keys = ["key1", "key2key2", "key3key3key3", "key4key4key4key4"];
        let values = [
            "value1",
            "value2value2",
            "value3value3value3",
            "value4value4value4value4",
        ];

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

    #[test]
    fn test_leaf_page_random_insterts() {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        let mut page = LeafPage::new();
        let iterations = 10;

        for i in 0..iterations {
            let mut key = format!("key{}", i);
            let mut value = format!("value{}", i);
            for _j in 0..i {
                key.push_str(&format!("key{}", i));
                value.push_str(&format!("value{}", i));
            }
            assert!(page.insert_entry(key.as_bytes(), value.as_bytes()).is_ok());
            let (retrieved_key, retrieved_value) = page.get_entry(i).unwrap();
            assert_eq!(retrieved_key, key.as_bytes());
            assert_eq!(retrieved_value, value.as_bytes());
        }
        let key = "SomeKeyWithRandomSize";
        let value = "SomeValueWithRandomSize";
        let idx_rand = rng.gen_range(0..iterations-1);
        let res = page.insert_entry_at(idx_rand, key.as_bytes(), value.as_bytes());
        assert!(res.is_ok());
        let (retrieved_key, retrieved_value) = page.get_entry(idx_rand).unwrap();
        assert_eq!(retrieved_value, value.as_bytes());
        assert_eq!(retrieved_key, key.as_bytes());
        
        for i in idx_rand+1..iterations {
            let mut key = format!("key{}", i);
            let mut value = format!("value{}", i);
            for _j in 0..i {
                key.push_str(&format!("key{}", i));
                value.push_str(&format!("value{}", i));
            }
            let (retrieved_key, retrieved_value) = page.get_entry(i+1).unwrap();
            assert_eq!(retrieved_key, key.as_bytes());
            assert_eq!(retrieved_value, value.as_bytes());
        }
    }
}
