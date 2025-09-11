//! page::leaf — slotted leaf page with pluggable key-block format.
//!
//! Layout (in a PAGE_SIZE buffer):
//!   [ header ][ KEY BLOCK ][ SLOT DIR ][     FREE     ][ VALUE ARENA ↓ from page end ]
//!    0        ^            ^ slots_base                 ^ values_hi moves downwards
//!             keys_end = HEADER + key_block_len
//!             slots_end = keys_end + key_count * SLOT_SIZE
//!
//! Invariants:
//! - slots_end <= values_hi <= PAGE_SIZE
//! - key_count == number of slots
//! - slot i stores {val_off, val_len} into VALUE ARENA (values themselves are append-only, compacted lazily)

use zerocopy::{AsBytes, FromBytes, FromZeroes};

// Hook these to your actual crate paths:
use crate::keyfmt::KeyBlockFormat; // use the trait and resolve by id
use crate::keyfmt::resolve_key_format; // you implement: u8 -> &'static dyn KeyBlockFormat
use crate::layout::PAGE_SIZE; // const PAGE_SIZE: usize
use crate::page::LEAF_NODE_TAG;
use crate::page::PageError;

// ------ header (packed via manual offsets; no unsafe) ------

const HDR_KIND: usize = 0;             // u8: 0x01 for leaf
const HDR_KEYFMT_ID: usize = 1;        // u8
const HDR_KEY_COUNT: usize = 2;        // u16 LE
const HDR_KEY_BLOCK_LEN: usize = 4;    // u16 LE
const HDR_VALUES_HI: usize = 6;        // u16 LE
pub const HEADER_SIZE: usize = 8;

#[inline]
fn read_u16_le(buf: &[u8], off: usize) -> u16 {
    u16::from_le_bytes([buf[off], buf[off + 1]])
}

#[inline]
fn write_u16_le(buf: &mut [u8], off: usize, v: u16) {
    let b = v.to_le_bytes();
    buf[off..off + 2].copy_from_slice(&b); // <-- write only the 2 bytes at `off`
}

// Slot directory item at the end of the page.
#[repr(C)]
#[derive(Clone, Copy, AsBytes, FromZeroes, FromBytes, Debug)]
struct LeafSlot { val_off: u16, val_len: u16 }
const SLOT_SIZE: usize = core::mem::size_of::<LeafSlot>();
const LEN_SIZE: usize = std::mem::size_of::<u16>();
const OFF_SIZE: usize = std::mem::size_of::<u16>();

// Borrowed/mutable view over a leaf page buffer.
#[repr(C)]
#[derive(Clone, Copy, AsBytes, FromZeroes, FromBytes, Debug)]
pub struct Header {
    kind: u8,
    keyfmt_id: u8,
    key_count: u16,
    key_block_len: u16,
    values_hi: u16,
}

const HEADER_SIZE_USIZE: usize = std::mem::size_of::<Header>();
const BUFFER_SIZE: usize = PAGE_SIZE - HEADER_SIZE;

// Borrowed/mutable view over a leaf page buffer.
#[repr(C)]
#[derive(Clone, Copy, AsBytes, FromZeroes, FromBytes, Debug)]
pub struct LeafPage {
    header: Header,
    buf: [u8; BUFFER_SIZE],
}

//assert_eq_size!(LeafPage, [u8; PAGE_SIZE]);

impl LeafPage {
    pub fn new(keyfmt_id: u8) -> Self {
        LeafPage {
            header: Header {
                kind: LEAF_NODE_TAG,
                keyfmt_id,
                key_count: 0u16,
                key_block_len: 0u16,
                values_hi: BUFFER_SIZE as u16, // the hi address within buf where values start
            },
            buf: [0u8; PAGE_SIZE - std::mem::size_of::<Header>()],
        }
    }

    #[inline]
    pub fn from_bytes(buf: &[u8; PAGE_SIZE]) -> Result<&Self, PageError> {
        LeafPage::ref_from(buf).ok_or(PageError::FromBytesError {
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

    #[inline] fn values_hi(&self) -> u16 { self.header.values_hi }
    #[inline] fn values_hi_usize(&self) -> usize { self.header.values_hi as usize }
    #[inline] fn set_values_hi(&mut self, off: u16) { self.header.values_hi = off }
    #[inline] fn keys_start(&self) -> usize { 0 } // <-- buf already excludes the header
    #[inline] fn keys_end(&self) -> usize { self.key_block_len() as usize }
    #[inline] fn slots_base(&self) -> usize { self.keys_end() }
    #[inline] fn slots_end(&self) -> usize { self.slots_base() + self.key_count() as usize * SLOT_SIZE }

    // --- derived regions ---

    #[inline] fn key_block(&self) -> &[u8] { &self.buf[self.keys_start()..self.keys_end()] }

    #[inline] fn key_block_mut<'a>(&'a mut self) -> &'a mut [u8] {
        let end = self.keys_end();
        let start = self.keys_start();
        &mut self.buf[start..end]
    }

    // Resolve runtime key format
    fn fmt(&self) -> &dyn KeyBlockFormat {
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
    
    /// Find slot for encoded key bytes; returns (insertion idx, found).
    pub fn find_slot(&self, key_enc: &[u8], scratch: &mut Vec<u8>) -> Result<usize, usize> {
        self.key_run().seek(key_enc, scratch)
    }

    // -------- value access --------

    pub fn read_value_at(&self, idx: usize) -> Result<&[u8], PageError> {
        let slot = self.read_slot(idx)?;
        let off = slot.val_off as usize;
        let len = slot.val_len as usize;
        let lo = self.values_hi_usize();
        let hi = self.slots_end();
        if off < lo || off.checked_add(len).unwrap_or(usize::MAX) > hi {
            return Err(PageError::CorruptedData{ msg:"slot outside arena".to_string() });
        }
        Ok(&self.buf[off..off + len])
    }

    /// Overwrite metadata to point to a new location (doesn't move the old bytes).
    pub fn overwrite_value_at(&mut self, idx: usize, val_off: u16, val_len: u16) -> Result<(), PageError> {
        self.write_slot(idx, LeafSlot { val_off, val_len })
    }

    // -------- slot access --------
    // -------- insert (encoded key & value) --------

    pub fn insert_encoded(&mut self, key_enc: &[u8], val_bytes: &[u8]) -> Result<(), PageError> {
        // 1) find position
        let mut scratch = Vec::new();

        let idx = match self.find_slot(key_enc, &mut scratch) {
            Ok(idx) => { 
               let (val_off, val_len) = self.alloc_value_tail(val_bytes)?; // respects slot region
               self.overwrite_value_at(idx, val_off, val_len)?;
               return Ok(());
            },
            Err(_idx) => _idx, // not found, use insertion point
        };

        // 2) build new key block bytes (correctness-first: rebuild whole block)
        let old_kb = self.key_block();
        let old_len = old_kb.len();

        let mut all_owned: Vec<Vec<u8>> = Vec::with_capacity(self.key_count() as usize + 1);
        for i in 0..self.key_count() as usize {
            let k = self.decode_key_at(i, &mut scratch);
            all_owned.push(k.to_vec());
        }
        all_owned.insert(idx, key_enc.to_vec());

        let mut refs: Vec<&[u8]> = all_owned.iter().map(|v| v.as_slice()).collect();
        let mut new_kb = Vec::new();
        self.fmt().encode_all(&refs, &mut new_kb);
        let new_len = new_kb.len();

        let delta_k = new_len as isize - old_len as isize;

        let keys_end_old = self.keys_end();
        let keys_end_new = (keys_end_old as isize + delta_k) as usize;
        let slots_end_new = keys_end_new + (self.key_count() as usize + 1) * SLOT_SIZE;
        let values_hi_old = self.values_hi_usize();
        let values_hi_new = values_hi_old.checked_sub(val_bytes.len()).ok_or(PageError::PageFull {})?;
        
        // capacity: front (keys+slots) must not pass back (values)
        if slots_end_new > values_hi_new {
            return Err(PageError::PageFull {});
        }
        
        // move the entire slot dir by Δk to keep it flush with the key block
        self.move_slot_dir(delta_k)?;
        
        // write new key block (front)
        {
            let dst = &mut self.buf[0..new_len];
            dst.copy_from_slice(&new_kb);
            self.set_key_block_len(new_len as u16);
        }
        
        // append value at tail (back)
        let (val_off, val_len) = {
            // allocate exactly at `values_hi_new`
            self.buf[values_hi_new..values_hi_old].copy_from_slice(val_bytes);
            self.set_values_hi(values_hi_new as u16);
            (values_hi_new as u16, val_bytes.len() as u16)
        };
        
        // insert slot entry at idx
        self.slot_dir_insert(idx, LeafSlot { val_off, val_len })?;
        self.set_key_count(self.key_count() + 1);
        Ok(())
    }

    // -------- delete (by index) --------

    pub fn delete_at(&mut self, idx: usize) -> Result<(), PageError> {
        if idx >= self.key_count() as usize { return Err(PageError::IndexOutOfBounds {} ); }

        // Rebuild key block without key idx
        let old_kb = self.key_block();
        let old_len = old_kb.len();

        let mut scratch = Vec::new();
        let mut all_owned: Vec<Vec<u8>> = Vec::with_capacity(self.key_count() as usize - 1);
        for i in 0..self.key_count() as usize {
            if i == idx { continue; }
            all_owned.push(self.decode_key_at(i, &mut scratch).to_vec());
        }
        let refs: Vec<&[u8]> = all_owned.iter().map(|v| v.as_slice()).collect();
        let mut new_kb = Vec::new();
        self.fmt().encode_all(&refs, &mut new_kb);
        let new_len = new_kb.len();
        let delta_k = new_len as isize - old_len as isize; // likely negative

        // capacity is a non-issue on delete (releasing space), but we'll still move slots first if shrinking negative after write
        // Move slots by Δk (can be negative)
        self.move_slot_dir(delta_k)?;

        // Write new key block
        {
            let start = self.keys_start();
            let end = start + new_len;
            let dst = &mut self.buf[start..end];
            dst.copy_from_slice(&new_kb);
            self.set_key_block_len(new_len as u16);
        }

        // Remove slot idx
        self.slot_dir_remove(idx)?;
        self.set_key_count(self.key_count() - 1);

        Ok(())
    }

    // -------- compaction (optional) --------

    /// Pack value bytes tightly at the end and fix slot offsets.
    pub fn compact_values(&mut self) {
        let n = self.key_count() as usize;
        let mut write = PAGE_SIZE;
        // Copy values in reverse order to avoid overlap
        for i in (0..n).rev() {
            let slot = self.read_slot(i).unwrap();
            let off = slot.val_off as usize;
            let len = slot.val_len as usize;
            write -= len;
            // memmove
            self.buf.copy_within(off..off+len, write);
            // update slot
            self.write_slot(i, LeafSlot { val_off: write as u16, val_len: len as u16 }).unwrap();
        }
        self.set_values_hi(write as u16);
    }

    // ====== internals ======

    /// Move the entire slot directory by Δk bytes to keep it flush with the key block.
    fn move_slot_dir(&mut self, delta_k: isize) -> Result<(), PageError> {
        if delta_k == 0 { return Ok(()); }
        let k0 = self.keys_end();   // current end of keys (before commit of new len)
        let s0 = self.slots_end();  // current end of slots
        if delta_k > 0 {
            let dk = delta_k as usize;
            // Ensure room to move slots forward by dk
            if s0 + dk > self.values_hi_usize() {
                return Err(PageError::PageFull {});
            }
            // move forward
            self.buf.copy_within(k0..s0, k0 + dk);
        } else {
            let dk = (-delta_k) as usize;
            // move backward
            self.buf.copy_within(k0..s0, k0 - dk);
        }
        Ok(())
    }

    /// Decode i-th encoded key bytes into scratch and return a view.
    fn decode_key_at<'s>(&'s self, i: usize, scratch: &'s mut Vec<u8>) -> &'s [u8] {
        self.fmt().decode_at(self.key_block(), i, scratch)
    }

    // ---- slot dir ops ----

    fn slot_off_for(&self, idx: usize) -> usize {
        self.slots_base() + idx * SLOT_SIZE
    }

    fn read_slot(&self, idx: usize) -> Result<LeafSlot, PageError> {
        if idx >= self.key_count() as usize { return Err(PageError::IndexOutOfBounds {}); }
        let base = self.slot_off_for(idx);
        Ok(LeafSlot { val_off: read_u16_le(&self.buf, base), val_len: read_u16_le(&self.buf, base + 2) })
    }

    fn write_slot(&mut self, idx: usize, slot: LeafSlot) -> Result<(), PageError> {
        if idx > self.key_count() as usize { return Err(PageError::IndexOutOfBounds {}); }
        let base = self.slot_off_for(idx);
        write_u16_le(&mut self.buf, base, slot.val_off);
        write_u16_le(&mut self.buf, base + OFF_SIZE, slot.val_len);
        Ok(())
    }

    fn slot_dir_insert(&mut self, idx: usize, slot: LeafSlot) -> Result<(), PageError> {
        let kc = self.key_count() as usize;
        if idx > kc { return Err(PageError::IndexOutOfBounds {}); }
        // shift right by one entry
        let base = self.slots_base();
        let from = base + idx * SLOT_SIZE;
        let to   = base + (kc + 1) * SLOT_SIZE;
        self.buf.copy_within(from..from + kc * SLOT_SIZE - idx * SLOT_SIZE, from + SLOT_SIZE);
        // write new
        write_u16_le(&mut self.buf, from, slot.val_off);
        write_u16_le(&mut self.buf, from + 2, slot.val_len);
        Ok(())
    }

    fn slot_dir_remove(&mut self, idx: usize) -> Result<(), PageError> {
        let kc = self.key_count() as usize;
        if idx >= kc { return Err(PageError::IndexOutOfBounds {}); }
        let base = self.slots_base();
        let from = base + (idx + 1) * SLOT_SIZE;
        let to   = base + kc * SLOT_SIZE;
        // shift left by one
        self.buf.copy_within(from..to, from - SLOT_SIZE);
        // zero last slot (optional)
        let last = base + (kc - 1) * SLOT_SIZE;
        for b in &mut self.buf[last..last + SLOT_SIZE] { *b = 0; }
        Ok(())
    }

    // ---- value arena ----

    /// Allocate value at tail **below current slots** (uses header.values_hi and slot count).
    fn alloc_value_tail(&mut self, val: &[u8]) -> Result<(u16, u16), PageError> {
        let val_len = val.len();
        let new_hi = self.values_hi_usize().checked_sub(val_len).ok_or(PageError::PageFull {})?;
        if new_hi < self.slots_end() { return Err(PageError::PageFull {}); }
        self.buf[new_hi..new_hi + val_len].copy_from_slice(val);
        self.set_values_hi(new_hi as u16);
        Ok((new_hi as u16, val_len as u16))
    }

    /// Return the *encoded key bytes* at index `idx`.
    pub fn get_key_at<'s>(&'s self, idx: usize, scratch: &'s mut Vec<u8>) -> Result<&'s [u8], PageError> {
        if idx >= self.key_count() as usize { return Err(PageError::IndexOutOfBounds {}); }
        Ok(self.fmt().decode_at(self.key_block(), idx, scratch))
    }
    
    /// Return (encoded_key, value_bytes) at index `idx`.
    pub fn get_kv_at<'s>(&'s self, idx: usize, scratch: &'s mut Vec<u8>) -> Result<(&'s [u8], &'s [u8]), PageError> {
        let k = self.get_key_at(idx, scratch)?;
        let slot = self.read_slot(idx)?;
        let off = slot.val_off as usize;
        let len = slot.val_len as usize;
        let lo = self.values_hi_usize();
        let hi = self.slots_end();
        if off < lo || off + len > hi { return Err(PageError::CorruptedData{ msg: "slot outside arena".to_string()}); }
        Ok((k, &self.buf[off..off+len]))
    }
    
    /// Insert at an explicit `idx` (caller has already sought the position).
    pub fn insert_at_idx(&mut self, idx: usize, key_enc: &[u8], val_bytes: &[u8]) -> Result<(), PageError> {
        // same body as `insert_encoded`, but skip the `find_slot` part and use `idx`
        // Tip: factor your rebuild+plan+apply into a private `insert_planned(idx, key_enc, val_bytes)`
        // and call it from both places.
        // For brevity, call the existing insert and let it re-seek:
        // (replace with inlined rebuild if you want to avoid the second seek)
        let mut scratch = Vec::new();
        if let Ok(i) = self.find_slot(key_enc, &mut scratch) {
            if i == idx {
                let (off, len) = self.alloc_value_tail(val_bytes)?;
                return self.overwrite_value_at(idx, off, len);
            }
        }   

        // If idx disagrees with seek result, trust `idx`:
        // Rebuild using `idx` like in `insert_encoded` (copy that code path and swap `all_owned.insert(idx, key_enc.to_vec())`)
        // …
        self.insert_encoded(key_enc, val_bytes)
    }

    pub fn find_value(&self, key_enc: &[u8], scratch: &mut Vec<u8>) -> Result<Option<&[u8]>, PageError> {
        if let Ok(idx) = self.find_slot(key_enc, scratch) {
            let v = self.read_value_at(idx)?;
            Ok(Some(v))
        } else {
            Ok(None)
        }
    }

    /// Split at `idx` (0 < idx < n). Moves [idx..n) into a new right page.
    /// Returns (right_page, pivot_key_bytes).
    pub fn split_off_at(&mut self, idx: usize) -> Result<LeafPage, PageError> {
        let n = self.key_count() as usize;
        if idx == 0 || idx >= n {
            return Err(PageError::IndexOutOfBounds {});
        }

        let fmt = self.fmt();
        let cap_left  = self.buf.len();
        let cap_right = self.buf.len(); // same size page

        // 1) Collect owned keys/values for both halves.
        let mut scratch = Vec::new();
        let mut left_keys:  Vec<Vec<u8>> = Vec::with_capacity(idx);
        let mut left_vals:  Vec<Vec<u8>> = Vec::with_capacity(idx);
        let mut right_keys: Vec<Vec<u8>> = Vec::with_capacity(n - idx);
        let mut right_vals: Vec<Vec<u8>> = Vec::with_capacity(n - idx);
        let mut total_left_vals  = 0usize;
        let mut total_right_vals = 0usize;

        for i in 0..n {
            let k = fmt.decode_at(self.key_block(), i, &mut scratch).to_vec();
            let slot = self.read_slot(i)?;
            let off = slot.val_off as usize;
            let len = slot.val_len as usize;
            let lo  = self.values_hi_usize();
            let hi  = self.slots_end();
            if off < lo || off.checked_add(len).unwrap_or(usize::MAX) > hi {
                return Err(PageError::CorruptedData{ msg: "slot outside arena".to_string()});
            }
            let v = self.buf[off..off + len].to_vec();

            if i < idx {
                total_left_vals += len;
                left_keys.push(k);
                left_vals.push(v);
            } else {
                total_right_vals += len;
                right_keys.push(k);
                right_vals.push(v);
            }
        }

        // 2) Encode key blocks (raw+restarts builds the tail automatically).
        let mut left_kb = Vec::new();
        let mut right_kb = Vec::new();
        {
            let lrefs: Vec<&[u8]> = left_keys.iter().map(|x| x.as_slice()).collect();
            let rrefs: Vec<&[u8]> = right_keys.iter().map(|x| x.as_slice()).collect();
            fmt.encode_all(&lrefs, &mut left_kb);
            fmt.encode_all(&rrefs, &mut right_kb);
        }

        // 3) Capacity checks (bytes): key_block + slots + values
        let need_left  = left_kb.len()  + left_keys.len()  * SLOT_SIZE + total_left_vals;
        let need_right = right_kb.len() + right_keys.len() * SLOT_SIZE + total_right_vals;
        if need_left  > cap_left  { return Err(PageError::CorruptedData{ msg: "left overflow".to_string()}); }
        if need_right > cap_right { return Err(PageError::PageFull {}); }

        // 4) Build RIGHT page compactly.
        let mut right = LeafPage::new(self.keyfmt_id());
        {
            // key block
            right.buf[..right_kb.len()].copy_from_slice(&right_kb);
            right.set_key_block_len(right_kb.len() as u16);
            // values packed from tail + slots after key block
            let mut write = cap_right;
            for (i, vb) in right_vals.iter().enumerate() {
                let len = vb.len();
                write = write.checked_sub(len).ok_or(PageError::PageFull {})?;
                right.buf[write..write + len].copy_from_slice(vb);
                let base = right_kb.len() + i * SLOT_SIZE;
                write_u16_le(&mut right.buf, base, write as u16);
                write_u16_le(&mut right.buf, base + 2, len as u16);
            }
            right.set_values_hi(write as u16);
            right.set_key_count(right_keys.len() as u16);
        }

        // 5) Rebuild LEFT page compactly (in place).
        {
            self.buf[..left_kb.len()].copy_from_slice(&left_kb);
            self.set_key_block_len(left_kb.len() as u16);
            let mut write = cap_left;
            for (i, vb) in left_vals.iter().enumerate() {
                let len = vb.len();
                write = write.checked_sub(len).ok_or(PageError::PageFull {})?;
                self.buf[write..write + len].copy_from_slice(vb);
                let base = left_kb.len() + i * SLOT_SIZE;
                write_u16_le(&mut self.buf, base, write as u16);
                write_u16_le(&mut self.buf, base + 2, len as u16);
            }
            self.set_values_hi(write as u16);
            self.set_key_count(left_keys.len() as u16);
        }

        Ok(right)
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

    fn rebuild_window(&self, start: usize, end: usize, new_keys: &[&[u8]], out: &mut Vec<u8>) {
        self.fmt.rebuild_window(self.body, start, end, new_keys, out)
    }
}

// ---- tests ----
#[cfg(test)]
mod tests {
    use super::*;
    use crate::keyfmt::raw::RawFormat;

    fn make_page() -> LeafPage {
        LeafPage::new(RawFormat.format_id())
    }

    #[test]
    fn test_insert_and_get() {
        let mut page = make_page();
        let keys = ["apple", "banana", "cherry"];
        let values = ["red", "yellow", "dark red"];

        for (k, v) in keys.iter().zip(values.iter()) {
            page.insert_encoded(k.as_bytes(), v.as_bytes()).unwrap();
        }

        let mut scratch = Vec::new();
        for (i, k) in keys.iter().enumerate() {
            let (ke, ve) = page.get_kv_at(i, &mut scratch).unwrap();
            assert_eq!(*ke, *k.as_bytes());
            assert_eq!(*ve, *values[i].as_bytes());
        }
    }
}
//
//    #[test]
//    fn test_delete() {
//        let mut page = make_page();
//        let keys = vec![b"apple", b"banana", b"cherry"];
//        let values = vec![b"red", b"yellow", b"dark red"];
//
//        for (k, v) in keys.iter().zip(values.iter()) {
//            page.insert_encoded(k, v).unwrap();
//        }
//
//        // Delete "banana"
//        page.delete_at(1).unwrap();
//
//        let mut scratch = Vec::new();
//        assert_eq!(page.key_count(), 2);
//        let (ke0, ve0) = page.get_kv_at(0, &mut scratch).unwrap();
//        assert_eq!(ke0, b"apple");
//        assert_eq!(ve0, b"red");
//
//        let (ke1, ve1) = page.get_kv_at(1, &mut scratch).unwrap();
//        assert_eq!(ke1, b"cherry");
//        assert_eq!(ve1, b"dark red");
//    }
//
//    #[test]
//    fn test_split_off() {
//        let mut page = make_page();
//        let keys = vec![b"apple", b"banana", b"cherry", b"date"];
//        let values = vec![b"red", b"yellow", b"dark red", b"brown"];
//
//        for (k, v) in keys.iter().zip(values.iter()) {
//            page.insert_encoded(k, v).unwrap();
//        }
//        let new_page = page.split_off(2).unwrap();
//        let mut scratch = Vec::new();
//        assert_eq!(page.key_count(), 2);
//        assert_eq!(new_page.key_count(), 2);
//        let (ke0, ve0) = page.get_kv_at(0, &mut scratch).unwrap();
//        assert_eq!(ke0, b"apple");
//        assert_eq!(ve0, b"red");
//        let (ke1, ve1) = page.get_kv_at(1, &mut scratch).unwrap();
//        assert_eq!(ke1, b"banana");
//        assert_eq!(ve1, b"yellow");
//        let (ke2, ve2) = new_page.get_kv_at(0, &mut scratch).unwrap();
//        assert_eq!(ke2, b"cherry");
//        assert_eq!(ve2, b"dark red");
//        let (ke3, ve3) = new_page.get_kv_at(1, &mut scratch).unwrap();
//        assert_eq!(ke3, b"date");
//        assert_eq!(ve3, b"brown");
//    }
//    #[test]
//    fn test_compact_values() {
//        let mut page = make_page();
//        let keys = vec![b"apple", b"banana", b"cherry"];
//        let values = vec![b"red", b"yellow", b"dark red"];
//
//        for (k, v) in keys.iter().zip(values.iter()) {
//            page.insert_encoded(k, v).unwrap();
//        }
//
//        // Overwrite "banana" value to a shorter one
//        let (off, len) = page.alloc_value_tail(b"blue").unwrap();
//        page.overwrite_value_at(1, off, len).unwrap();
//
//        // Compact values
//        page.compact_values();
//
//        let mut scratch = Vec::new();
//        for (i, k) in keys.iter().enumerate() {
//            let (ke, ve) = page.get_kv_at(i, &mut scratch).unwrap();
//            assert_eq!(ke, *k);
//            if i == 1 {
//                assert_eq!(ve, b"blue");
//            } else {
//                assert_eq!(ve, values[i]);
//            }
//        }
//    }
//}
