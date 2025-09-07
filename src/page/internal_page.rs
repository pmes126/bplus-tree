use crate::layout::MAX_ENTRIES;
use crate::layout::PAGE_SIZE;
use crate::page::INTERNAL_NODE_TAG;
use crate::page::PageCodecError;
use zerocopy::{AsBytes, FromBytes, FromZeroes};

#[repr(C)]
#[derive(Clone, Copy, AsBytes, FromZeroes, FromBytes, Debug)]
pub struct InternalPageHeader {
    pub node_type: u64,   // Node type (LEAF_NODE_TAG)
    pub entry_count: u64, // number of keys
    pub free_start: u64,
    pub leftmost_child: u64, // leftmost child pointer, k keys for k+1 children
    pub key_offsets: [u16; MAX_ENTRIES],
}

const HEADER_SIZE_BYTES: usize = std::mem::size_of::<InternalPageHeader>();
const DATA_SIZE: usize = PAGE_SIZE - HEADER_SIZE_BYTES;
const LEN_SIZE: usize = std::mem::size_of::<u16>();
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
    pub data: Data,
}

impl InternalPage {
    pub fn new() -> Self {
        InternalPage {
            header: InternalPageHeader {
                node_type: INTERNAL_NODE_TAG,
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
        InternalPage::ref_from(buf).ok_or(PageCodecError::FromBytesError {
            msg: "Failed to convert bytes to LeafPage".to_string(),
        })
    }

    // Store according to the layout => [klen][key][ptr]
    pub fn insert_entry(&mut self, key: &[u8], child: u64) -> Result<(), PageCodecError> {
        if self.header.entry_count as usize >= MAX_ENTRIES {
            return Err(PageCodecError::PageFull {
                msg: "LeafPage is full, cannot insert more entries".to_string(),
            });
        }

        let required_space = key.len() + LEN_SIZE + CHILD_ID_SIZE;
        if self.header.free_start + required_space as u64 > DATA_SIZE as u64 {
            return Err(PageCodecError::PageFull {
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
        let raw = key;
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

    // Store at a specific index according to the layout => [klen][key][ptr]
    pub fn insert_entry_at(
        &mut self,
        idx: usize,
        key: &[u8],
        child: u64,
    ) -> Result<(), PageCodecError> {
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

        let required_space = key.len() + LEN_SIZE + CHILD_ID_SIZE;

        if self.header.free_start + required_space as u64 > DATA_SIZE as u64 {
            return Err(PageCodecError::PageFull {
                msg: "LeafPage is full, cannot insert more entries".to_string(),
            });
        }

        // We need to memcpy the contents from idx to idx + required space and write the key value
        // pair in the space between
        let insertion_point = self.header.key_offsets[idx] as usize;
        let shift_count = self.header.free_start as usize - insertion_point;
        let src_idx = insertion_point;
        let dst_idx = src_idx + required_space;
        self.data
            .blob
            .copy_within(src_idx..src_idx + shift_count, dst_idx);

        // All values from idx onwards should be shifted by 1 position to the right and have required_space
        // added to them
        let shift_count = self.header.entry_count as usize - idx;
        let src_idx = idx;
        let end_idx = src_idx + shift_count;
        let dest_idx = src_idx + 1;
        self.header
            .key_offsets
            .copy_within(src_idx..end_idx, dest_idx);
        for i in dest_idx..dest_idx + shift_count - 1 {
            self.header.key_offsets[i] += required_space as u16;
        }

        self.header.free_start += required_space as u64;

        let data = &mut self.data.blob[..];

        let key_len_offset = insertion_point;
        let key_len = key.len() as u16;
        let raw = key_len.to_le_bytes();
        let end = key_len_offset + raw.len();

        data[key_len_offset..end].copy_from_slice(raw.as_ref());

        // Write the key
        let key_offset = key_len_offset + raw.len();
        let raw = key;
        let end = key_offset + raw.len();

        data[key_offset..end].copy_from_slice(raw);

        // Write the child pointer
        let child_offset = end;
        let raw = child.to_le_bytes();
        let end = child_offset + raw.len();

        data[child_offset..end].copy_from_slice(raw.as_ref());

        // Adjust count
        self.header.entry_count += 1;
        self.header.key_offsets[self.header.entry_count as usize] = self.header.free_start as u16;
        Ok(())
    }

    // Read from the key_offset->[key_len][key][child_ptr]
    pub fn get_entry(&self, idx: usize) -> Result<(&[u8], u64), PageCodecError> {
        // Read and decode the length of the key
        let key_len_offset = self.header.key_offsets[idx] as usize;
        let mut end = key_len_offset + LEN_SIZE;
        let arr: [u8; LEN_SIZE] = self.data.blob[key_len_offset..end]
            .try_into()
            .map_err(|_| PageCodecError::FromBytesError {
                msg: "Failed to read bytes as slice".to_string(),
            })?;
        let key_length = u16::from_le_bytes(arr);

        // Read the key
        let key_offset = key_len_offset + LEN_SIZE; // Move to the start of the key
        end = key_offset + key_length as usize;
        let key = &self.data.blob[key_offset..end];

        // Read and decode the child pointer
        let child_offset = key_offset + key_length as usize;
        end = child_offset + CHILD_ID_SIZE;
        let arr: [u8; CHILD_ID_SIZE] =
            self.data.blob[child_offset..end].try_into().map_err(|_| {
                PageCodecError::FromBytesError {
                    msg: "Failed to read bytes as slice".to_string(),
                }
            })?;
        let child = u64::from_le_bytes(arr);
        Ok((key, child))
    }

    #[inline]
    pub fn key_bytes_at(&self, i: usize) -> Result<&[u8], std::array::TryFromSliceError> {
        let offset = self.header.key_offsets[i] as usize;

        let len =
            u16::from_le_bytes(self.data.blob[offset..offset + LEN_SIZE].try_into()?) as usize;

        let k0 = offset + LEN_SIZE;
        Ok(&self.data.blob[k0..k0 + len])
    }

    #[inline]
    pub fn child_bytes_at(&self, i: usize) -> Result<&[u8], std::array::TryFromSliceError> {
        let offset = self.header.key_offsets[i] as usize;
        let key_len =
            u16::from_le_bytes(self.data.blob[offset..offset + LEN_SIZE].try_into()?) as usize;
        let c0 = offset + LEN_SIZE + key_len;
        Ok(&self.data.blob[c0..c0 + CHILD_ID_SIZE])
    }

    #[inline]
    pub fn child_at(&self, i: usize) -> Result<u64, PageCodecError> {
        // Read and decode the length of the key
        let offset = self.header.key_offsets[i] as usize;
        let end = offset + LEN_SIZE;
        let arr: [u8; LEN_SIZE] =
            self.data.blob[offset..end]
                .try_into()
                .map_err(|_| PageCodecError::FromBytesError {
                    msg: "Failed to read bytes as slice".to_string(),
                })?;
        let key_length = u16::from_le_bytes(arr);

        let c0 = offset + LEN_SIZE + key_length as usize;
        let arr: [u8; CHILD_ID_SIZE] =
            self.data.blob[c0..c0 + CHILD_ID_SIZE]
                .try_into()
                .map_err(|_| PageCodecError::FromBytesError {
                    msg: "Failed to read bytes as slice".to_string(),
                })?;
        Ok(u64::from_le_bytes(arr))
    }

    #[inline]
    pub fn to_bytes(&self) -> Result<&[u8; PAGE_SIZE], std::array::TryFromSliceError> {
        let bytes: &[u8] = self.as_bytes(); // borrow lives for the function scope
        let array: &[u8; PAGE_SIZE] = bytes.try_into()?; // also scoped
        Ok(array)
    }

    pub fn split_off(&mut self, idx: usize) -> Result<InternalPage, PageCodecError> {
        if idx >= self.header.entry_count as usize {
            return Err(PageCodecError::IndexOutOfBounds {
                msg: "Split index is out of bounds".to_string(),
            });
        }

        let mut new_page = InternalPage::new();

        // Move entries from idx to the end to the new page
        for i in idx..self.header.entry_count as usize {
            let (key, child) = self.get_entry(i)?;
            new_page.insert_entry(key, child)?;
        }

        // Update the entry count of the original page
        self.header.entry_count = idx as u64;

        // Update the free_start of the original page to the offset of the idx entry
        self.header.free_start = self.header.key_offsets[idx] as u64;
        // Clear old offsets
        self.header.key_offsets[idx..self.header.entry_count as usize].fill(0);

        Ok(new_page)
    }
}

impl Default for InternalPage {
    fn default() -> Self {
        InternalPage::new()
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

    #[test]
    fn test_internal_page_random_insterts() {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        let mut page = InternalPage::new();
        let iterations = 10;

        for i in 0..iterations {
            let mut key = format!("key{}", i);
            for _j in 0..i {
                key.push_str(&format!("key{}", i));
            }
            assert!(page.insert_entry(key.as_bytes(), i).is_ok());
            let (retrieved_key, retrieved_value) = page.get_entry(i as usize).unwrap();
            assert_eq!(retrieved_key, key.as_bytes());
            assert_eq!(retrieved_value, i);
        }
        let key = "SomeKeyWithRandomSize";
        let idx_rand = rng.gen_range(0..iterations - 1) as usize;
        let res = page.insert_entry_at(idx_rand as usize, key.as_bytes(), 999);
        assert!(res.is_ok());
        let (retrieved_key, retrieved_value) = page.get_entry(idx_rand).unwrap();
        assert_eq!(retrieved_value, 999);
        assert_eq!(retrieved_key, key.as_bytes());
    }
}
