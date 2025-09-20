//! page::internal — slotted internal page with pluggable key-block format.
//!
//! Layout (in a PAGE_SIZE buffer):
//! [ header ][ KEY BLOCK ][ CHILDREN ARRAY ][ free ... ]
//             ^ keys_end      ^ children_end
//!             keys_end = HEADER + key_block_len
//!             children_end = keys_end + (key_count + 1) * CHILD_ID_SIZE
//!
//! Invariants:
//! - keys_end <= children_start
//! - children_end <= PAGE_SIZE
//! - key_count == number of slots

use crate::bplustree::node::NodeId;
use crate::keyfmt::KeyBlockFormat; // use the trait and resolve by id
use crate::keyfmt::resolve_key_format; // you implement: u8 -> &'static dyn KeyBlockFormat
use crate::layout::PAGE_SIZE;
use crate::page::INTERNAL_NODE_TAG;
use crate::page::PageError;
use zerocopy::{AsBytes, FromBytes, FromZeroes};

#[inline]
fn read_u64_le(buf: &[u8]) -> u64 {
    u64::from_le_bytes(buf.try_into().unwrap()) // <-- read only the 8 bytes at `off`
}

#[inline]
fn write_u64_le(buf: &mut [u8], off: usize, v: u64) {
    let b = v.to_le_bytes();
    buf[off..off + std::mem::size_of::<u64>()].copy_from_slice(&b); // <-- write only the 8 bytes at `off`
}

#[repr(C)]
#[derive(Clone, Copy, AsBytes, FromZeroes, FromBytes, Debug)]
pub struct Header {
    kind: u8,
    keyfmt_id: u8,
    key_count: u16,
    key_block_len: u16,
}

const CHILD_ID_SIZE: usize = core::mem::size_of::<NodeId>();
const HEADER_SIZE: usize = std::mem::size_of::<Header>();
const BUFFER_SIZE: usize = PAGE_SIZE - HEADER_SIZE;

// Borrowed/mutable view over a leaf page buffer.
#[repr(C)]
#[derive(Clone, Copy, AsBytes, FromZeroes, FromBytes, Debug)]
pub struct InternalPage {
    header: Header,
    buf: [u8; BUFFER_SIZE],
}

impl InternalPage {
    pub fn new(keyfmt_id: u8) -> Self {
        InternalPage {
            header: Header {
                kind: INTERNAL_NODE_TAG,
                keyfmt_id,
                key_count: 0u16,
                key_block_len: 0u16,
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

    #[inline]
    pub fn kind(&self) -> u8 {
        self.header.kind
    }
    #[inline]
    pub fn key_count(&self) -> u16 {
        self.header.key_count
    }
    #[inline]
    fn keyfmt_id(&self) -> u8 {
        self.header.keyfmt_id
    }
    #[inline]
    fn set_key_count(&mut self, n: u16) {
        self.header.key_count = n;
    }

    #[inline]
    fn key_block_len(&self) -> u16 {
        self.header.key_block_len
    }
    #[inline]
    fn set_key_block_len(&mut self, n: u16) {
        self.header.key_block_len = n;
    }

    #[inline]
    fn keys_start(&self) -> usize {
        0
    } // <-- buf already excludes the header
    #[inline]
    fn keys_end(&self) -> usize {
        self.key_block_len() as usize
    }
    #[inline]
    fn children_base(&self) -> usize {
        self.keys_end()
    }
    #[inline]
    fn children_len(&self) -> usize {
        (self.key_count() as usize + 1) * CHILD_ID_SIZE
    }
    #[inline]
    fn children_end(&self) -> usize {
        self.children_base() + self.children_len()
    }

    // --- derived regions ---

    #[inline]
    fn key_block(&self) -> &[u8] {
        &self.buf[self.keys_start()..self.keys_end()]
    }

    // Resolve runtime key format
    pub fn fmt(&self) -> &dyn KeyBlockFormat {
        resolve_key_format(self.keyfmt_id())
            .expect("unknown key format id; register it in keyfmt::resolve_key_format")
    }

    // Lightweight view for calling the format
    fn key_run<'s>(&'s self) -> PageKeyRun<'s> {
        PageKeyRun {
            body: self.key_block(),
            fmt: self.fmt(),
        }
    }

    // ---- search ----
    /// Lower bound on encoded key bytes; returns insertion index.
    pub fn lower_bound(&self, key_enc: &[u8], scratch: &mut Vec<u8>) -> Result<usize, usize> {
        self.key_run().seek(key_enc, scratch)
    }

    /// Lower bound on encoded key bytes; returns insertion index.
    pub fn lower_bound_cmp(
        &self,
        key_enc: &[u8],
        scratch: &mut Vec<u8>,
        cmp: fn(&[u8], &[u8]) -> core::cmp::Ordering,
    ) -> Result<usize, usize> {
        self.fmt()
            .seek_with_cmp(self.key_block(), key_enc, scratch, cmp)
    }

    /// Find slot for encoded key bytes; returns Result(idx existing, idx insertion).
    pub fn find_slot(&self, key_enc: &[u8], scratch: &mut Vec<u8>) -> Result<usize, usize> {
        self.key_run().seek(key_enc, scratch)
    }

    // -------- slot access --------
    // -------- insert (encoded key & child) --------
    /// Insert a separator key at index `idx` (0..=key_count), shifting existing keys/children to
    /// the right. The new child pointer is written at `idx+1`.
    pub fn insert_separator(
        &mut self,
        idx: usize,
        key: &[u8],
        right_child: u64,
    ) -> Result<(), PageError> {
        if idx > self.key_count() as usize {
            return Err(PageError::IndexOutOfBounds {});
        }
        let mut scratch = Vec::new();

        // Plan splice in key block
        let (range, repl) = self
            .fmt()
            .insert_plan(self.key_block(), idx, key, &mut scratch);
        let delta_k = repl.len() as isize - (range.end - range.start) as isize;

        // Capacity checks
        let keys_end_old = self.keys_end();
        let keys_end_new = (keys_end_old as isize + delta_k) as usize;
        let children_end_new = keys_end_new + (self.key_count() as usize) * CHILD_ID_SIZE;
        if children_end_new > PAGE_SIZE {
            return Err(PageError::PageFull {});
        }

        let new_keys_end = (self.keys_end() as isize + delta_k) as usize;
        let new_children_len = (self.key_count() as usize) * CHILD_ID_SIZE;
        let new_used = new_keys_end + new_children_len;
        if new_used > PAGE_SIZE {
            return Err(PageError::PageFull {});
        }

        // 3) move children array by Δk to keep it flush after key block
        self.move_child_dir(delta_k)?;

        // 4) splice key block in-place (one copy_within + one copy)
        let ks = self.keys_start();
        let old_len = self.key_block_len() as usize;
        let new_len = (old_len as isize + delta_k) as usize;
        self.set_key_block_len(new_len as u16);

        let tail_src_start = ks + range.end;
        let tail_src_end = ks + old_len;
        let tail_dst = (tail_src_start as isize + delta_k) as usize;

        self.buf.copy_within(tail_src_start..tail_src_end, tail_dst);

        let hole_start = ks + range.start;
        self.buf[hole_start..hole_start + repl.len()].copy_from_slice(&repl);

        // 5) insert child pointer at idx+1 (shift right by one)
        self.children_shift_right_from(idx + 1);
        self.write_child_at(idx + 1, right_child)?;

        // 6) bump key_count
        self.set_key_count(self.key_count() + 1);

        // 7) let format adjust metadata (restart offsets etc.)
        //let kb_final = &mut self.buf[ks..ks + new_len];
        //self.fmt().adjust_after_splice(kb_final, range.start, delta_k, idx);

        Ok(())
    }

    /// Delete the separator at index `idx` (and child at `idx+1`)
    pub fn delete_separator(&mut self, idx: usize) -> Result<(), PageError> {
        if idx >= self.key_count() as usize {
            return Err(PageError::IndexOutOfBounds {});
        }
        let mut scratch = Vec::new();

        // PLAN for key-block deletion
        let (range, repl) = self.fmt().delete_plan(self.key_block(), idx, &mut scratch); // same idea as insert_plan
        let delta_k = repl.len() as isize - (range.end - range.start) as isize; // usually negative

        // capacity is fine when shrinking
        // splice key block
        let ks = self.keys_start();
        let old_len = self.key_block_len() as usize;
        let new_len = (old_len as isize + delta_k) as usize;

        // write replacement (often empty)
        //let hole_start = ks + range.start;
        //self.buf[hole_start .. hole_start + repl.len()].copy_from_slice(&repl);

        let tail_src_start = ks + range.end;
        let tail_src_end = ks + old_len + self.children_len(); // include children to move them
        // too
        let tail_dst_start = (tail_src_start as isize + delta_k) as usize;
        self.buf
            .copy_within(tail_src_start..tail_src_end, tail_dst_start);
        self.set_key_block_len(new_len as u16);

        // remove child at idx+1
        self.children_shift_left_from(idx + 1);

        // move children by Δk
        self.move_child_dir(delta_k)?;

        // dec key_count
        self.set_key_count(self.key_count() - 1);

        // adjust format metadata
        //let kb_final = &mut self.buf[ks..ks + new_len];
        //self.fmt().adjust_after_splice(kb_final, range.start, delta_k, idx);

        Ok(())
    }

    /// Deletes the key at index `key_count() -1 `  without changing the child - used as a part of
    /// a split to fix the left page invariants.
    pub fn pop_last_key(&mut self, scratch: &mut Vec<u8>) -> Result<Vec<u8>, PageError> {
        let idx = self.key_count() as usize - 1;
        let key = self.fmt().decode_at(self.key_block(), idx, scratch).to_vec();

        // PLAN for key-block deletion
        let (range, repl) = self.fmt().delete_plan(self.key_block(), idx, scratch);
        let delta_k = repl.len() as isize - (range.end - range.start) as isize; // usually negative

        // capacity is fine when shrinking
        // splice key block
        let ks = self.keys_start();
        let old_len = self.key_block_len() as usize;
        let new_len = (old_len as isize + delta_k) as usize;

        // write replacement (often empty)
        //let hole_start = ks + range.start;
        //self.buf[hole_start .. hole_start + repl.len()].copy_from_slice(&repl);

        let tail_src_start = ks + range.end;
        let tail_src_end = ks + old_len + self.children_len(); // include children to move them
        // too
        let tail_dst_start = (tail_src_start as isize + delta_k) as usize;
        self.buf
            .copy_within(tail_src_start..tail_src_end, tail_dst_start);
        self.set_key_block_len(new_len as u16);
        self.set_key_count(self.key_count() - 1);
        Ok(key)
    }

    /// Insert a new separator key (encoded bytes) and child pointer, finding the correct slot.
    pub fn insert_encoded(&mut self, key: &[u8], child: u64) -> Result<(), PageError> {
        let idx = match self.find_slot(key, &mut Vec::new()) {
            Ok(i) => i,  // key exists; insert after it
            Err(i) => i, // key not found; insert at i
        };
        self.insert_separator(idx, key, child)
    }

    // -------- child pointer array manipulation --------

    #[inline]
    /// Read the child pointer at index `idx` (0..=key_count).
    pub fn read_child_at(&self, idx: usize) -> Result<u64, PageError> {
        let offset = self.children_base() + idx * CHILD_ID_SIZE;
        if offset + CHILD_ID_SIZE > PAGE_SIZE {
            return Err(PageError::IndexOutOfBounds {});
        }
        Ok(read_u64_le(&self.buf[offset..offset + CHILD_ID_SIZE]))
    }

    /// Write the child pointer at index `idx` (0..=key_count).
    #[inline]
    pub fn write_child_at(&mut self, idx: usize, child: u64) -> Result<(), PageError> {
        let offset = self.children_base() + idx * CHILD_ID_SIZE;
        if offset + CHILD_ID_SIZE > PAGE_SIZE {
            return Err(PageError::IndexOutOfBounds {});
        }
        write_u64_le(&mut self.buf, offset, child);
        Ok(())
    }

    /// Write the leftmost child pointer (at index 0).
    #[inline]
    pub fn write_leftmost_child(&mut self, child: u64) -> Result<(), PageError> {
        self.write_child_at(0, child)
    }

    /// Replace the child pointer at idx with the provided child pointer
    #[inline]
    pub fn replace_child_at(&mut self, idx: usize, child: u64) -> Result<(), PageError> {
        self.write_child_at(idx, child)
    }

    fn children_shift_right_from(&mut self, from: usize) {
        let base = self.children_base();
        let len = self.key_count() as usize + 1; // current child count
        let src = base + from * CHILD_ID_SIZE;
        let dst = base + (from + 1) * CHILD_ID_SIZE;
        let bytes = (len - from) * CHILD_ID_SIZE;
        self.buf.copy_within(src..src + bytes, dst);
    }

    fn children_shift_left_from(&mut self, from: usize) {
        let base = self.children_base();
        let len = self.key_count() as usize + 1;
        let src = base + (from + 1) * CHILD_ID_SIZE;
        let dst = base + from * CHILD_ID_SIZE;
        let bytes = (len - from - 1) * CHILD_ID_SIZE;
        self.buf.copy_within(src..src + bytes, dst);
    }

    // Move the child pointer array by delta bytes (positive = right, negative = left)
    fn move_child_dir(&mut self, delta_k: isize) -> Result<(), PageError> {
        if delta_k == 0 {
            return Ok(());
        }
        let base = self.children_base();
        let end = self.children_end();
        if delta_k > 0 {
            let dk = delta_k as usize;
            if end + dk > PAGE_SIZE {
                return Err(PageError::PageFull {});
            }
            self.buf.copy_within(base..end, base + dk);
        } else {
            let dk = (-delta_k) as usize;
            let dst = base.saturating_sub(dk);
            self.buf.copy_within(base..end, dst);
        }
        Ok(())
    }

    // --------- key accessors ---------
    /// Return the *encoded key bytes* at index `idx`.
    #[inline]
    pub fn get_key_at<'s>(
        &'s self,
        idx: usize,
        scratch: &'s mut Vec<u8>,
    ) -> Result<&'s [u8], PageError> {
        if idx >= self.key_count() as usize {
            return Err(PageError::IndexOutOfBounds {});
        }
        Ok(self.fmt().decode_at(self.key_block(), idx, scratch))
    }

    // ---- splitting ----

    /// Split this leaf into `right`, returning the encoded separator (first key of `right`).
    /// Does *not* decode all keys; the format handles right-block fixups internally.
    pub fn split_off_into(
        &mut self,
        split_idx: usize,
        right: &mut InternalPage,
    ) -> Result<Vec<u8>, PageError> {
        let key_count = self.key_count() as usize;
        if split_idx == 0 || split_idx >= key_count {
            return Err(PageError::IndexOutOfBounds {});
        }
        let kb = self.key_block();

        // 1) ask the format to produce left/right key-block bytes
        let mut left_kb = Vec::new();
        let mut right_kb = Vec::new();
        self.fmt()
            .split_into(kb, split_idx, &mut left_kb, &mut right_kb);

        // 2) BEFORE we change key_count, snapshot the children for the right side
        let mut moved_cldrn = Vec::with_capacity(key_count + 1 - split_idx);
        for i in split_idx..key_count + 1 {
            moved_cldrn.push(self.read_child_at(i)?);
        }

        // 3) Shrink left page's key-block in place (move child-dir by Δk and overwrite)
        let old_len = kb.len();
        let delta_k = left_kb.len() as isize - old_len as isize; // negative
        self.move_child_dir(delta_k)?;
        let ks = self.keys_start();
        self.buf[ks..ks + left_kb.len()].copy_from_slice(&left_kb);
        self.set_key_block_len(left_kb.len() as u16);
        // Reduce key_count to the left count; we don't need to physically shift slots—just drop count.
        self.set_key_count(split_idx as u16);

        // 4) Init the right page with the right key-block
        {
            let ks_r = right.keys_start();
            right.buf[ks_r..ks_r + right_kb.len()].copy_from_slice(&right_kb);
            right.set_key_block_len(right_kb.len() as u16);
            right.set_key_count((key_count - split_idx) as u16);
        }

        // 5) Copy values referenced by moved slots into the right page's value arena,
        //    and write its slot dir in-order. Left page keeps old bytes as garbage.
        for (i, child_ptr) in moved_cldrn.iter().enumerate() {
            right.write_child_at(i, *child_ptr)?;
        }

        // 8) Separator = first key of right page (encoded key bytes)
        let mut scratch = Vec::new();
        let sep = self
            .fmt()
            .decode_at(right.key_block(), 0, &mut scratch)
            .to_vec();

        Ok(sep)
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
    fmt: &'a dyn KeyBlockFormat,
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

        let scratch = &mut Vec::new();
        // Retrieve the entry
        let retrieved_key = page.get_key_at(0, scratch).unwrap();
        let retrieved_child = page.read_child_at(1).unwrap();
        assert_eq!(
            String::from_utf8(retrieved_key.to_vec()),
            String::from_utf8(key.to_vec())
        );
        assert_eq!(retrieved_child, child);
    }

    #[test]
    fn test_internal_page_multiples() {
        let mut page = InternalPage::new(0);
        let mut keys: Vec<String> = Vec::new();
        let mut children: Vec<u64> = Vec::new();
        let iterations = 10;
        let scratch = &mut Vec::new();

        page.write_leftmost_child(0).unwrap(); // first child

        for i in 0..iterations {
            let mut key = format!("key{}", i);
            for _j in 0..i {
                key.push_str(&format!("key{}", i));
            }
            keys.push(key.clone());
            children.push(i as u64 + 1);
            assert!(
                page.insert_separator(i, key.as_bytes(), i as u64 + 1)
                    .is_ok()
            );
            let retrieved_key = page.get_key_at(i, scratch).unwrap();
            assert_eq!(retrieved_key, key.as_bytes());
            let retrieved_child = page.read_child_at(i + 1).unwrap();
            assert_eq!(retrieved_child, i as u64 + 1);
        }
        // Retrieve the entries
        for i in 0..iterations {
            let scratch = &mut Vec::new();
            let retrieved_key = page.get_key_at(i, scratch).unwrap();
            assert_eq!(retrieved_key, keys[i].as_bytes());
        }
    }

    #[test]
    fn test_internal_page_random_inserts() {
        use rand::Rng;
        let mut rng = rand::thread_rng();

        let mut page = InternalPage::new(0);
        let iterations = 10;
        let scratch = &mut Vec::new();

        page.write_leftmost_child(0).unwrap(); // first child
        for i in 0..iterations {
            let mut key = format!("key{}", i);
            for _j in 0..i {
                key.push_str(&format!("key{}", i));
            }
            assert!(
                page.insert_separator(i, key.as_bytes(), i as u64 + 1)
                    .is_ok()
            );
            let retrieved_key = page.get_key_at(i, scratch).unwrap();
            assert_eq!(retrieved_key, key.as_bytes());
            let retrieved_child = page.read_child_at(i + 1).unwrap();
            assert_eq!(retrieved_child, i as u64 + 1);
        }
        let key = "SomeKeyWithRandomSize";
        let idx_rand = rng.gen_range(0..iterations - 1) as usize;
        let res = page.insert_separator(idx_rand as usize, key.as_bytes(), 999);
        assert!(res.is_ok());
        let retrieved_value = page.read_child_at(idx_rand + 1).unwrap();
        let retrieved_key = page.get_key_at(idx_rand, scratch).unwrap();
        assert_eq!(retrieved_value, 999);
        assert_eq!(retrieved_key, key.as_bytes());
    }

    #[test]
    fn test_internal_page_removals() {
        use rand::Rng;
        let mut rng = rand::thread_rng();

        let mut page = InternalPage::new(0);
        let scratch = &mut Vec::new();
        let iterations = 10;
        let mut keys: Vec<String> = Vec::new();
        let mut children: Vec<u64> = Vec::new();

        page.write_leftmost_child(0).unwrap(); // first child

        for i in 0..iterations {
            let mut key = format!("key{}", i);
            for _j in 0..i {
                key.push_str(&format!("key{}", i));
            }
            keys.push(key.clone());
            children.push(i as u64 + 1);
            assert!(
                page.insert_separator(i, key.as_bytes(), i as u64 + 1)
                    .is_ok()
            );
            let retrieved_key = page.get_key_at(i, scratch).unwrap();
            assert_eq!(retrieved_key, key.as_bytes());
            let retrieved_child = page.read_child_at(i + 1).unwrap();
            assert_eq!(retrieved_child, i as u64 + 1);
        }

        while page.key_count() > 0 {
            let bound = page.key_count() as usize - 1;
            let idx = rng.gen_range(0..=bound) as usize;
            assert!(page.delete_separator(idx).is_ok());
            if page.key_count() == 0 {
                break;
            }
            if idx >= page.key_count() as usize {
                assert!(page.get_key_at(idx, scratch).is_err());
                continue;
            }
            let retrieved_key = page.get_key_at(idx, scratch).unwrap();
            assert_ne!(retrieved_key, keys[idx].as_bytes());
        }
    }

    #[test]
    fn test_internal_page_split() {
        use rand::Rng;
        let mut rng = rand::thread_rng();

        let mut page = InternalPage::new(0);
        let scratch = &mut Vec::new();
        let iterations = 10;
        let mut keys: Vec<String> = Vec::new();
        let mut children: Vec<u64> = Vec::new();

        page.write_leftmost_child(0).unwrap(); // first child

        for i in 0..iterations {
            let mut key = format!("key{}", i);
            for _j in 0..i {
                key.push_str(&format!("key{}", i));
            }
            keys.push(key.clone());
            children.push(i as u64 + 1);
            assert!(
                page.insert_separator(i, key.as_bytes(), i as u64 + 1)
                    .is_ok()
            );
            let retrieved_key = page.get_key_at(i, scratch).unwrap();
            assert_eq!(retrieved_key, key.as_bytes());
            let retrieved_child = page.read_child_at(i + 1).unwrap();
            assert_eq!(retrieved_child, i as u64 + 1);
        }

        let mut right = InternalPage::new(0);
        let split_idx = rng.gen_range(1..(page.key_count() as usize - 1));

        let sep = page.split_off_into(split_idx, &mut right).unwrap();
        let separator = right.get_key_at(0, scratch).unwrap();

        let scratch = &mut Vec::new();
        for i in 0..split_idx {
            let retrieved_key = page.get_key_at(i, scratch).unwrap();
            assert_eq!(retrieved_key, keys[i].as_bytes());
            let retrieved_child = page.read_child_at(i + 1).unwrap();
            assert_eq!(retrieved_child, children[i]);
        }

        let scratch = &mut Vec::new();
        for i in 0..(right.key_count() as usize) {
            let retrieved_key = right.get_key_at(i, scratch).unwrap();
            assert_eq!(retrieved_key, keys[split_idx + i].as_bytes());
            let retrieved_child = right.read_child_at(i + 1).unwrap();
            assert_eq!(retrieved_child, children[split_idx + i]);
        }

        assert_eq!(sep, separator);
    }
}
