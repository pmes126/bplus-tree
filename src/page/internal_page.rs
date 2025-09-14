use crate::layout::MAX_ENTRIES;
use crate::layout::PAGE_SIZE;
use crate::page::INTERNAL_NODE_TAG;
use crate::page::PageError;
use crate::bplustree::node::NodeId;
use crate::keyfmt::KeyBlockFormat; // use the trait and resolve by id
use crate::keyfmt::resolve_key_format; // you implement: u8 -> &'static dyn KeyBlockFormat
use zerocopy::{AsBytes, FromBytes, FromZeroes};

//[ header ][ KEY BLOCK ][ CHILDREN ARRAY ][ free ... ]
//             ^ keys_end      ^ children_end
#[repr(C)]
#[derive(Clone, Copy, AsBytes, FromZeroes, FromBytes, Debug)]
pub struct Header {
    kind: u8,
    keyfmt_id: u8,
    key_count: u16,
    key_block_len: u16,
}

const CHILD_ID_SIZE: usize = core::mem::size_of::<NodeId>();
const LEN_SIZE: usize = std::mem::size_of::<u16>();
const HEADER_SIZE: usize = std::mem::size_of::<Header>();
const BUFFER_SIZE: usize = PAGE_SIZE - HEADER_SIZE;

// Borrowed/mutable view over a leaf page buffer.
#[repr(C)]
#[derive(Clone, Copy, AsBytes, FromZeroes, FromBytes, Debug)]
pub struct InternalPage{
    header: Header,
    buf: [u8; BUFFER_SIZE],
}


impl InternalPage {
    pub fn new(keyfmt_id: u8) -> Self {
        InternalPage {
            header: Header {
                kind: INTERNAL_NODE_TAG,
                keyfmt_id,
                key_count: 0,
                key_block_len: 0,
            },
            buf: [0u8; BUFFER_SIZE],
        }
    }

    #[inline]
    pub fn from_bytes(buf: &[u8; PAGE_SIZE]) -> Result<&Self, PageError> {
        InternalPage::ref_from(buf).ok_or(PageError::FromBytesError {
            msg: "Failed to convert bytes to LeafPage".to_string(),
        })
    }

    #[inline]
    pub fn to_bytes(&self) -> Result<&[u8; PAGE_SIZE], std::array::TryFromSliceError> {
        let array: &[u8; PAGE_SIZE] = self.as_bytes().try_into()?; // also scoped
        Ok(array)
    }

    // --- header accessors ---

    #[inline] pub fn key_count(&self) -> u16 { self.header.key_count }
    #[inline] fn keyfmt_id(&self) -> u8 { self.header.keyfmt_id }
    #[inline] fn set_key_count(&mut self, n: u16) { self.header.key_count = n; }

    #[inline] fn key_block_len(&self) -> u16 { self.header.key_block_len }
    #[inline] fn set_key_block_len(&mut self, n: u16) { self.header.key_block_len = n; }

    #[inline] fn keys_start(&self) -> usize { 0 } // <-- buf already excludes the header
    #[inline] fn keys_end(&self) -> usize { self.key_block_len() as usize }
    #[inline] fn cldrn_base(&self) -> usize { self.keys_end() }
    #[inline] fn children_size(&self) -> usize { self.key_count() as usize * (CHILD_ID_SIZE + 1) }
    #[inline] fn children_end(&self) -> usize { self.cldrn_base() + self.children_size() }

    // --- derived regions ---

    #[inline] fn key_block(&self) -> &[u8] { &self.buf[self.keys_start()..self.keys_end()] }

    // Resolve runtime key format
    pub fn fmt(&self) -> &dyn KeyBlockFormat {
        resolve_key_format(self.keyfmt_id())
            .expect("unknown key format id; register it in keyfmt::resolve_key_format")
    }

    // Lightweight view for calling the format
    fn key_run<'s>(&'s self) -> PageKeyRun<'s> {
        PageKeyRun { body: self.key_block(), fmt: self.fmt() }
    }
    
    // ---- search ----
    /// Lower bound on encoded key bytes; returns insertion index.
    pub fn lower_bound(&self, key_enc: &[u8], scratch: &mut Vec<u8>) -> Result<usize, usize> {
        self.fmt().seek(self.key_block(), key_enc, scratch)
    }
    
    /// Find slot for encoded key bytes; returns Result(idx existing, idx insertion).
    pub fn find_slot(&self, key_enc: &[u8], scratch: &mut Vec<u8>) -> Result<usize, usize> {
        self.key_run().seek(key_enc, scratch)
    }

    // -------- slot access --------
    // -------- insert (encoded key & value) --------
    pub fn insert_encoded(&mut self, key: &[u8], child: u64) -> Result<(), PageError> {
        // 1) find position
        let mut scratch = Vec::new();

        let idx = match self.find_slot(key, &mut scratch) {
            Ok(idx) => { 
               self.replace_child_at(idx, child)?;
               return Ok(());
            },
            Err(_idx) => _idx, // not found, use insertion point
        };
        self.insert_entry_at(idx, key, child);
        Ok(())
    }

    // Store at a specific index
    pub fn insert_entry_at(
        &mut self,
        idx: usize,
        key_enc: &[u8],
        child: u64,
    ) -> Result<(), PageError> {
        let mut scratch = Vec::new();

        // scratch
        // Plan and get delta_k
        let kb = self.key_block(); // &[u8]
        let (range, insert_bytes) = self.fmt().insert_plan(kb, idx, key_enc, &mut scratch);
        let delta_k = insert_bytes.len() as isize;

        // CAPACITY
        let keys_end_old = self.keys_end();
        let keys_end_new = (keys_end_old as isize + delta_k) as usize;
        if keys_end_new + self.children_size() > self.buf.len() {
            return Err(PageError::PageFull {});
        }

        // Move slot dir by Δk to stay flush
        self.move_child_dir(delta_k)?;

        // SPLICE inside the key-block region (one copy_within + one write)
        //
        // key block before: |<-- range --><-- rest -->| 
        // key block after:  |<-- insert_bytes --><--range--><-- rest -->|
        let ks = self.keys_start();
        let old_len = self.key_block_len() as usize;
        let new_len = (old_len as isize + delta_k) as usize;
        self.set_key_block_len(new_len as u16);

        // shift tail
        let tail_src_start = ks + range.start;
        let tail_src_end   = ks + old_len;
        let tail_dst_start = (tail_src_start as isize + delta_k) as usize;
        self.buf.copy_within(tail_src_start..tail_src_end, tail_dst_start);

        // write replacement bytes
        let hole_start = ks + range.start;

        self.buf[hole_start .. hole_start + insert_bytes.len()].copy_from_slice(&insert_bytes);

        // adjust format metadata (restart offsets etc.)
        //let kb_final = &mut self.buf[ks..ks + new_len];
        //self.fmt().adjust_after_splice(kb_final, range.start, delta_k, idx);

        // Append child
        self.child_dir_insert(idx, child)?;
        self.set_key_count(self.key_count() + 1);
        Ok(())
    }

    /// Return the *encoded key bytes* at index `idx`.
    pub fn get_key_at<'s>(&'s self, idx: usize, scratch: &'s mut Vec<u8>) -> Result<&'s [u8], PageError> {
        if idx >= self.key_count() as usize { return Err(PageError::IndexOutOfBounds {}); }
        Ok(self.fmt().decode_at(self.key_block(), idx, scratch))
    }

    #[inline]
    pub fn get_child_at(&self, idx: usize) -> Result<u64, PageError> {
        let child_offset = self.cldrn_base() + idx * CHILD_ID_SIZE;
        if child_offset + CHILD_ID_SIZE > self.buf.len() {
            return Err(PageError::IndexOutOfBounds {});
        }
        let raw: [u8; CHILD_ID_SIZE] = self.buf[child_offset..child_offset + CHILD_ID_SIZE]
            .try_into()
            .map_err(|_| PageError::FromBytesError {
                msg: "Failed to read child pointer".to_string(),
            })?;

        Ok(u64::from_le_bytes(raw))
    }

    // ====== internals ======

    // Move the child pointer array by delta bytes (positive = right, negative = left)
    pub fn move_child_dir(&mut self, delta: isize) -> Result<(), PageError> {
        if delta == 0 { return Ok(()); }
        let cldrn_start = self.cldrn_base();
        let cldrn_end   = self.children_end();
        let new_cldrn_end = (cldrn_end as isize + delta) as usize;
        if new_cldrn_end > self.buf.len() { return Err(PageError::PageFull{}); }

        self.buf.copy_within(
            cldrn_start..cldrn_end,
            (cldrn_start as isize + delta) as usize
        );
        Ok(())
    }

    // Insert a child pointer at idx, shifting all subsequent pointers right
    pub fn child_dir_insert(&mut self, idx: usize, child: u64) -> Result<(), PageError> {
        if idx > self.key_count() as usize + 1 as usize {
            return Err(PageError::IndexOutOfBounds {
            });
        }
        let cldrn_start = self.cldrn_base();
        let cldrn_end   = self.children_end();
        let insert_at = cldrn_start + idx * CHILD_ID_SIZE;
        // shift right
        self.buf.copy_within(
            insert_at..cldrn_end,
            insert_at + CHILD_ID_SIZE
        );
        // write new child
        let raw = child.to_le_bytes();
        let len = raw.len();
        self.buf[insert_at..insert_at + len].copy_from_slice(&raw);
        Ok(())
    }

    // Replace the child pointer at idx with the provided child pointer
    pub fn replace_child_at(&mut self, idx: usize, child: u64) -> Result<(), PageError> {
        if idx > self.key_count() as usize + 1 as usize {
            return Err(PageError::IndexOutOfBounds {
            });
        }
        let child_offset = self.cldrn_base() + idx * CHILD_ID_SIZE;
        

        let raw = child.to_le_bytes();
        let len = raw.len();
        self.buf[child_offset..child_offset + len].copy_from_slice(raw.as_ref());
        Ok(())
    }

    // Splits the page at idx, moving entries from idx to the end to a new page
    pub fn split_off(&mut self, idx: usize) -> Result<InternalPage, PageError> {
        //if idx >= self.header.entry_count as usize {
        //    return Err(PageError::IndexOutOfBounds {
        //    });
        //}

        //if idx == 0 {
        //    // If idx is 0, we are splitting all entries to the new page
        //    let mut new_page = InternalPage::new();
        //    std::mem::swap(&mut new_page, self);
        //    return Ok(new_page);
        //}

        let mut new_page = InternalPage::new(0);

        //// Move entries from idx to the end to the new page
        //for i in idx..self.header.entry_count as usize {
        //    let (key, child) = self.get_entry(i)?;
        //    new_page.insert_entry(key, child)?;
        //}

        //// The last child of the original page becomes the leftmost child of the new page
        //self.get_entry(idx - 1).map(|(_, child)| {
        //    new_page.header.leftmost_child = child;
        //})?;

        //// Update the entry count of the original page
        //self.header.entry_count = idx as u64;

        //// Update the free_start of the original page to the offset of the idx entry
        //self.header.free_start = self.header.offsets[idx] as u64;
        //// Clear old offsets
        //self.header.offsets[idx..self.header.entry_count as usize].fill(0);

        Ok(new_page)
    }
}

impl Default for InternalPage {
    fn default() -> Self {
        InternalPage::new(0) // Default key format id is 0
    }
}

// Tiny helper view handed to the KeyBlockFormat
struct PageKeyRun<'a> {
    body: &'a [u8],
    fmt:  &'a dyn KeyBlockFormat,
}

impl<'a> PageKeyRun<'a> {
    fn seek(&self, needle: &[u8], scratch: &mut Vec<u8>) -> Result<usize, usize> {
        self.fmt.seek(self.body, needle, scratch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_internal_page() {
        let mut page = InternalPage::new(0);
        let key = b"test_key";
        let child = 42;

        // Insert an entry
        assert!(page.insert_encoded(key, child).is_ok());

        // Retrieve the entry
        let (retrieved_key, retrieved_child) = page.get_entry(0).unwrap();
        assert_eq!(retrieved_key, key);
        assert_eq!(retrieved_child, child);
    }

    #[test]
    fn test_internal_page_multiples() {
        let mut page = InternalPage::new(0);
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
        let mut page = InternalPage::new(0);
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

    #[test]
    fn test_internal_page_removals() {
        let mut page = InternalPage::new(0);
        let keys = ["key1", "key2key2", "key3key3key3"];
        let children = vec![1, 2, 3];

        // Insert multiple entries
        for (&key, &child) in keys.iter().zip(&children) {
            assert!(page.insert_entry(key.as_bytes(), child).is_ok());
        }

        // Remove the second entry
        assert!(page.remove_entry_at(1).is_ok());

        // Check remaining entries
        let (retrieved_key, retrieved_child) = page.get_entry(0).unwrap();
        assert_eq!(retrieved_key, keys[0].as_bytes());
        assert_eq!(retrieved_child, children[0]);

        let (retrieved_key, retrieved_child) = page.get_entry(1).unwrap();
        assert_eq!(retrieved_key, keys[2].as_bytes());
        assert_eq!(retrieved_child, children[2]);

        // Ensure entry count is updated
        assert_eq!(page.header.entry_count, 2);
    }

    #[test]
    fn test_internal_page_split() {
        let mut page = InternalPage::new(0);
        let keys = ["key1", "key2key2", "key3key3key3", "key4key4key4key4", "key5key5key5key5key5"];
        let children = vec![1, 2, 3, 4, 5];
        // Insert multiple entries
        page.header.leftmost_child = 0;
        for (&key, &child) in keys.iter().zip(&children) {
            assert!(page.insert_entry(key.as_bytes(), child).is_ok());
        }
        // Split the page at index 2
        // This should move "key3key3key3" and "key4key4key4key4" to
        // the new page
        let new_page = page.split_off(2).unwrap();
        // Check original page entries
        assert_eq!(page.header.entry_count, 2);
        let (retrieved_key, retrieved_child) = page.get_entry(0).unwrap();
        assert_eq!(retrieved_key, keys[0].as_bytes());
        assert_eq!(retrieved_child, children[0]);
        let (retrieved_key, retrieved_child) = page.get_entry(1).unwrap();
        assert_eq!(retrieved_key, keys[1].as_bytes());
        assert_eq!(retrieved_child, children[1]);
        // Check new page entries
        assert_eq!(new_page.header.entry_count, 3);
        let (retrieved_key, retrieved_child) = new_page.get_entry(0).unwrap();
        assert_eq!(retrieved_key, keys[2].as_bytes());
        assert_eq!(retrieved_child, children[2]);
        let (retrieved_key, retrieved_child) = new_page.get_entry(1).unwrap();
        assert_eq!(retrieved_key, keys[3].as_bytes());
        assert_eq!(retrieved_child, children[3]);
        // Check leftmost child of new page
        assert_eq!(new_page.header.leftmost_child, children[1]);
    }
}
