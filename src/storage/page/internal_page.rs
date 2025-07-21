use crate::storage::page::INTERNAL_NODE_TAG;
use crate::storage::page::PageCodecError;
use crate::layout::PAGE_SIZE;
use crate::layout::MAX_ENTRIES;
use zerocopy::{AsBytes, FromBytes, FromZeroes};

#[repr(C)]
#[derive(Clone, Copy, AsBytes, FromZeroes, FromBytes, Debug)]
pub struct InternalPageHeader {
    pub node_type: u64,       // Node type (LEAF_NODE_TAG)
    pub entry_count:    u64,    // number of keys
    pub free_start:     u64,
    pub leftmost_child: u64, // leftmost child pointer, k keys for k+1 children
    pub key_offsets: [u16; MAX_ENTRIES],
}

const HEADER_SIZE_BYTES: usize = std::mem::size_of::<InternalPageHeader>();
const DATA_SIZE: usize = PAGE_SIZE - HEADER_SIZE_BYTES;
const LEN_KEY_SIZE: usize = std::mem::size_of::<u16>();
const CHILD_ID_SIZE: usize = std::mem::size_of::<u64>();

#[repr(C)]
#[derive(Clone, Copy, AsBytes, FromZeroes, FromBytes, Debug)]
pub struct Data {
    pub blob: [u8; DATA_SIZE],
}

#[repr(C)]
#[derive(Clone, Copy, AsBytes, FromZeroes, FromBytes, Debug)]
pub struct InternalPage {
    pub header: InternalPageHeader,
    pub data : Data,
}

impl InternalPage {
    pub fn new() -> Self {
        InternalPage {
            header: InternalPageHeader {
                node_type : INTERNAL_NODE_TAG,
                entry_count: 0,
                key_offsets: [0; MAX_ENTRIES],
                free_start: 0,
                leftmost_child: 0, // Initially no leftmost child
            },
            data: Data {
                blob: [0u8; DATA_SIZE],
            },
        }
    }

    pub fn from_bytes(buf: &[u8; PAGE_SIZE]) -> Result<&Self, PageCodecError> {
        InternalPage::ref_from(buf).ok_or(PageCodecError::FromBytesError{ msg: "Failed to convert bytes to LeafPage".to_string() })
    }

    // Store according to the layout => [klen][key][ptr] 
    pub fn insert_entry(&mut self, key: &[u8], child: u64) -> Result<(), PageCodecError> {
        if self.header.entry_count as usize >= MAX_ENTRIES {
            return Err(PageCodecError::PageFull{
                msg: "LeafPage is full, cannot insert more entries".to_string(),
            });
        }

        let required_space = key.len() + LEN_KEY_SIZE +  CHILD_ID_SIZE; // key_len +
        if self.header.free_start + required_space as u64 > DATA_SIZE as u64 {
            return Err(PageCodecError::PageFull{
                msg: "LeafPage is full, cannot insert more entries".to_string(),
            });
        }

        let data = &mut self.data.blob[..];
        
        // Current entry index of entry_count has offset at the free_start position
        self.header.key_offsets[self.header.entry_count as usize] = self.header.free_start as u16;

        let key_len_offset = self.header.free_start as usize;
        let key_len = key.len() as u16;
        let raw = key_len.to_le_bytes();
        let end = key_len_offset + raw.len();

        data[key_len_offset..end].copy_from_slice(raw.as_ref());
        self.header.free_start += raw.len() as u64;

        // Write the key
        let key_offset = self.header.free_start as usize;
        let raw = key.as_ref();
        let end = key_offset + raw.len();

        data[key_offset..end].copy_from_slice(raw);
        self.header.free_start += raw.len() as u64;

        // Write the child pointer
        let child_offset = self.header.free_start as usize;
        let raw = child.to_le_bytes();
        let end = child_offset + raw.len();

        data[child_offset..end].copy_from_slice(raw.as_ref());
        self.header.free_start += raw.len() as u64;

        // Adjust count
        self.header.entry_count += 1;
        Ok(())
    }


    // Read the fo key_offset->[key_len][key][ptr]
    pub fn get_entry(&self, idx: usize) -> Result<(&[u8], u64), PageCodecError> {
        // Read and decode the length of the key
        let key_len_offset = self.header.key_offsets[idx] as usize;
        let mut end = key_len_offset + LEN_KEY_SIZE;
        let arr: [u8; LEN_KEY_SIZE] = self.data.blob[key_len_offset..end].try_into().
            map_err(|_| PageCodecError::FromBytesError{ msg: "Failed to read bytes as slice".to_string() })?;
        let key_length = u16::from_le_bytes(arr);

        // Read the key
        let key_offset = key_len_offset + LEN_KEY_SIZE; // Move to the start of the key
        end = key_offset + key_length as usize;
        let key = &self.data.blob[key_offset..end];
        
        // Read and decode the child pointer
        let child_offset = key_offset + key_length as usize;
        end = child_offset + CHILD_ID_SIZE;
        let arr: [u8; CHILD_ID_SIZE] = self.data.blob[child_offset..end].try_into().
            map_err(|_| PageCodecError::FromBytesError{ msg: "Failed to read bytes as slice".to_string() })?;
        let child = u64::from_le_bytes(arr);
        Ok((key, child))
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
    fn test_internal_page() {
        let mut page = InternalPage::new();
        let key = b"test_key";
        let child = 42;

        // Insert an entry
        assert!(page.insert_entry(key, child).is_ok());

        // Retrieve the entry
        let (retrieved_key, retrieved_child) = page.get_entry(0).unwrap();
        assert_eq!(retrieved_key, key);
        assert_eq!(retrieved_child, child);
    }

    #[test]
    fn test_internal_page_multiples() {
        let mut page = InternalPage::new();
        let keys = ["key1", "key2key2", "key3key3key3"];
        let children = vec![1, 2, 3];

        // Insert multiple entries
        for (&key, &child) in keys.iter().zip(&children) {
            assert!(page.insert_entry(key.as_bytes(), child).is_ok());
        }

        // Retrieve the entries
        for (i, key) in keys.iter().enumerate() {
            let (retrieved_key, retrieved_child) = page.get_entry(i).unwrap();
            assert_eq!(retrieved_key, key.as_bytes());
            assert_eq!(retrieved_child, children[i]);
        }
    }
}
