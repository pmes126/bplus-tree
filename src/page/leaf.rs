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
use crate::keyfmt::KeyFormat; // use the trait and resolve by id
use crate::keyfmt::ScratchBuf;
use crate::keyfmt::resolve_key_format; // you implement: u8 -> &'static dyn KeyBlockFormat
use crate::layout::PAGE_SIZE; // const PAGE_SIZE: usize
use crate::page::LEAF_NODE_TAG;
use crate::page::PageError;

use std::convert::TryInto;
use std::fmt;
use std::fmt::Debug;

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
struct LeafSlot {
    val_off: u16,
    val_len: u16,
}
pub(crate) const SLOT_SIZE: usize = core::mem::size_of::<LeafSlot>();
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

pub(crate) const HEADER_SIZE: usize = std::mem::size_of::<Header>();
pub(crate) const BUFFER_SIZE: usize = PAGE_SIZE - HEADER_SIZE;

// Borrowed/mutable view over a leaf page buffer.
#[repr(C)]
#[derive(Clone, Copy, AsBytes, FromZeroes, FromBytes)]
pub struct LeafPage {
    header: Header,
    buf: [u8; BUFFER_SIZE],
}

impl LeafPage {
    pub fn new(keyfmt_id: KeyFormat) -> Self {
        LeafPage {
            header: Header {
                kind: LEAF_NODE_TAG,
                keyfmt_id: keyfmt_id.id(),
                key_count: 0u16,
                key_block_len: 0u16,
                values_hi: BUFFER_SIZE as u16, // the hi address within buf where values start
            },
            buf: [0u8; PAGE_SIZE - HEADER_SIZE],
        }
    }

    #[inline]
    pub fn from_bytes(buf: &[u8; PAGE_SIZE]) -> Result<&Self, PageError> {
        LeafPage::ref_from(buf).ok_or(PageError::FromBytesError {
            msg: "Failed to convert bytes to LeafPage".to_string(),
        })
    }

    #[inline]
    #[allow(clippy::wrong_self_convention)]
    pub fn to_bytes(&self) -> Result<&[u8; PAGE_SIZE], std::array::TryFromSliceError> {
        let array: &[u8; PAGE_SIZE] = self.as_bytes().try_into()?;
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
    pub fn keyfmt_id(&self) -> u8 {
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
    fn values_hi_usize(&self) -> usize {
        self.header.values_hi as usize
    }

    #[inline]
    fn set_values_hi(&mut self, off: u16) {
        self.header.values_hi = off
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
    fn slots_base(&self) -> usize {
        self.keys_end()
    }

    #[inline]
    fn slots_end(&self) -> usize {
        self.slots_base() + self.key_count() as usize * SLOT_SIZE
    }

    /// Returns the number of bytes consumed by keys, slots, and values (excludes header).
    pub fn used_bytes(&self) -> usize {
        let key_block = self.key_block_len() as usize;
        let slots = self.key_count() as usize * SLOT_SIZE;
        let values = BUFFER_SIZE - self.values_hi_usize();
        key_block + slots + values
    }

    // --- derived regions ---

    #[inline]
    fn key_block(&self) -> &[u8] {
        &self.buf[self.keys_start()..self.keys_end()]
    }

    // Resolve runtime key format
    pub fn key_fmt(&self) -> &dyn KeyBlockFormat {
        resolve_key_format(self.keyfmt_id())
            .expect("unknown key format id; register it in keyfmt::resolve_key_format")
    }

    // Lightweight view for calling the format
    fn key_run<'s>(&'s self) -> PageKeyRun<'s> {
        PageKeyRun {
            body: self.key_block(),
            fmt: self.key_fmt(),
        }
    }

    // ---- search ----
    /// Lower bound on encoded key bytes; returns insertion index.
    pub fn lower_bound(&self, key_enc: &[u8], scratch: &mut ScratchBuf) -> Result<usize, usize> {
        self.key_fmt().seek(self.key_block(), key_enc, scratch)
    }

    /// Lower bound on encoded key bytes; returns insertion index.
    pub fn lower_bound_cmp(
        &self,
        key_enc: &[u8],
        scratch: &mut ScratchBuf,
        cmp: fn(&[u8], &[u8]) -> core::cmp::Ordering,
    ) -> Result<usize, usize> {
        self.key_fmt()
            .seek_with_cmp(self.key_block(), key_enc, scratch, cmp)
    }

    /// Find slot for encoded key bytes; returns Result(idx existing, idx insertion).
    pub fn find_slot(&self, key_enc: &[u8], scratch: &mut ScratchBuf) -> Result<usize, usize> {
        self.key_run().seek(key_enc, scratch)
    }

    // -------- value access --------

    /// Reads a value at a specific index.
    pub fn read_value_at(&self, idx: usize) -> Result<&[u8], PageError> {
        if idx >= self.key_count() as usize {
            return Err(PageError::IndexOutOfBounds {});
        }
        let slot = self.read_slot(idx)?;
        let off = slot.val_off as usize;
        let len = slot.val_len as usize;
        let lo = self.values_hi_usize();
        if off < lo {
            return Err(PageError::CorruptedData {
                msg: format!("slot offset {} outside bounds", off).to_string(),
            });
        }
        Ok(&self.buf[off..off + len])
    }

    /// Overwrite metadata to point to a new location (doesn't move the old bytes).
    pub fn overwrite_slot_at(
        &mut self,
        idx: usize,
        val_off: u16,
        val_len: u16,
    ) -> Result<(), PageError> {
        debug_assert!(idx < self.key_count() as usize);
        self.write_slot(idx, LeafSlot { val_off, val_len })
    }

    // -------- slot access --------
    // -------- insert (encoded key & value) --------
    /// insert encoded key bytes and value bytes at index `idx`
    pub fn insert_at(
        &mut self,
        idx: usize,
        key_enc: &[u8],
        val_bytes: &[u8],
    ) -> Result<(), PageError> {
        if idx > self.key_count() as usize {
            return Err(PageError::IndexOutOfBounds {});
        }
        let mut scratch = ScratchBuf::new();

        // scratch
        // Plan and get delta_k
        let kb = self.key_block(); // &[u8]
        let (range, insert_bytes) = self.key_fmt().insert_plan(kb, idx, key_enc, &mut scratch);
        let delta_k = insert_bytes.len() as isize;

        // CAPACITY
        let keys_end_old = self.keys_end();
        let keys_end_new = (keys_end_old as isize + delta_k) as usize;
        let slots_end_new = keys_end_new + (self.key_count() as usize + 1) * SLOT_SIZE;
        let values_hi_new = self
            .values_hi_usize()
            .checked_sub(val_bytes.len())
            .ok_or(PageError::PageFull {})?;
        if slots_end_new > values_hi_new {
            return Err(PageError::PageFull {});
        }

        // Move slot dir by Δk to stay flush
        self.move_slot_dir(delta_k)?;

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
        let tail_src_end = ks + old_len;
        let tail_dst_start = (tail_src_start as isize + delta_k) as usize;
        self.buf
            .copy_within(tail_src_start..tail_src_end, tail_dst_start);

        // write replacement bytes
        let hole_start = ks + range.start;

        self.buf[hole_start..hole_start + insert_bytes.len()].copy_from_slice(&insert_bytes);

        // adjust format metadata (restart offsets etc.)
        //let kb_final = &mut self.buf[ks..ks + new_len];
        //self.fmt().adjust_after_splice(kb_final, range.start, delta_k, idx);

        // Append value + insert slot
        let (val_off, val_len) = self.alloc_value_tail(val_bytes)?;
        self.slot_dir_insert(idx, LeafSlot { val_off, val_len })?;
        self.set_key_count(self.key_count() + 1);
        Ok(())
    }

    /// insert or overwrite by encoded key bytes and  value bytes
    pub fn insert_encoded(&mut self, key_enc: &[u8], val_bytes: &[u8]) -> Result<(), PageError> {
        // 1) find position
        let mut scratch = ScratchBuf::new();

        let idx = match self.find_slot(key_enc, &mut scratch) {
            Ok(idx) => {
                let (val_off, val_len) = self.alloc_value_tail(val_bytes)?; // respects slot region
                self.overwrite_slot_at(idx, val_off, val_len)?;
                return Ok(());
            }
            Err(_idx) => _idx, // not found, use insertion point
        };
        debug_assert!(idx <= self.key_count() as usize);
        self.insert_at(idx, key_enc, val_bytes)
    }

    /// overwrite key at index `idx` with new key bytes
    pub fn replace_key_at(&mut self, idx: usize, key_bytes: &[u8]) -> Result<(), PageError> {
        if idx >= self.key_count() as usize {
            return Err(PageError::IndexOutOfBounds {});
        }
        let mut scratch = ScratchBuf::new();

        // Plan and get delta_k
        let kb = self.key_block(); // &[u8]
        let (range, repl) = self
            .key_fmt()
            .replace_plan(kb, idx, key_bytes, &mut scratch); // same idea as insert_plan
        let delta_k = repl.len() as isize - (range.end - range.start) as isize; // usually negative

        // CAPACITY
        let keys_end_old = self.keys_end();
        let keys_end_new = (keys_end_old as isize + delta_k) as usize;
        let slots_end_new = keys_end_new + self.key_count() as usize * SLOT_SIZE;
        let values_hi_new = self
            .values_hi_usize()
            .checked_sub(0) // no value change
            .ok_or(PageError::PageFull {})?;
        if slots_end_new > values_hi_new {
            return Err(PageError::PageFull {});
        }

        // Move slot dir by Δk to stay flush
        self.move_slot_dir(delta_k)?;

        // SPLICE inside the key-block region (one copy_within + one write)
        //
        // key block before: |<-- range --><-- rest -->|
        // key block after:  |<-- insert_bytes --><--range--><-- rest -->|
        let ks = self.keys_start();
        let old_len = self.key_block_len() as usize;
        let new_len = (old_len as isize + delta_k) as usize;
        self.set_key_block_len(new_len as u16);

        // shift tail
        let tail_src_start = ks + range.end;
        let tail_src_end = ks + old_len;
        let tail_dst_start = (tail_src_start as isize + delta_k) as usize;
        self.buf
            .copy_within(tail_src_start..tail_src_end, tail_dst_start);

        // write replacement bytes
        let hole_start = ks + range.start;

        self.buf[hole_start..hole_start + repl.len()].copy_from_slice(&repl);

        // adjust format metadata (restart offsets etc.)
        //let kb_final = &mut self.buf[ks..ks + new_len];
        //self.fmt().adjust_after_splice
        Ok(())
    }

    pub fn insert_key_at(&mut self, idx: usize, key_enc: &[u8]) -> Result<(), PageError> {
        if idx > self.key_count() as usize {
            return Err(PageError::IndexOutOfBounds {});
        }
        let mut scratch = ScratchBuf::new();

        // scratch
        // Plan and get delta_k
        let kb = self.key_block(); // &[u8]
        let (range, insert_bytes) = self.key_fmt().insert_plan(kb, idx, key_enc, &mut scratch);
        let delta_k = insert_bytes.len() as isize;

        // CAPACITY
        let keys_end_old = self.keys_end();
        let keys_end_new = (keys_end_old as isize + delta_k) as usize;
        let slots_end_new = keys_end_new + (self.key_count() as usize + 1) * SLOT_SIZE;
        if slots_end_new > self.values_hi_usize() {
            return Err(PageError::PageFull {});
        }

        // Move slot dir by Δk to stay flush
        self.move_slot_dir(delta_k)?;

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
        let tail_src_end = ks + old_len;
        let tail_dst_start = (tail_src_start as isize + delta_k) as usize;
        self.buf
            .copy_within(tail_src_start..tail_src_end, tail_dst_start);

        // write replacement bytes
        let hole_start = ks + range.start;

        self.buf[hole_start..hole_start + insert_bytes.len()].copy_from_slice(&insert_bytes);

        // adjust format metadata (restart offsets etc.)
        //let kb_final = &mut self.buf[ks..ks + new_len];
        //self.fmt().adjust_after_splice(kb_final, range.start, delta_k, idx);

        self.set_key_count(self.key_count() + 1);
        Ok(())
    }

    /// delete key and return its encoded bytes at index `idx`
    pub fn delete_key_at(&mut self, idx: usize, _scratch: &mut [u8]) -> Result<Vec<u8>, PageError> {
        if idx >= self.key_count() as usize {
            return Err(PageError::IndexOutOfBounds {});
        }
        let mut scratch = ScratchBuf::new();
        let key = self
            .key_fmt()
            .decode_at(self.key_block(), idx, &mut scratch)
            .to_vec();

        // Plan and get delta_k
        let kb = self.key_block(); // &[u8]
        let (range, repl) = self.key_fmt().delete_plan(kb, idx, &mut scratch); // same idea as insert_plan
        let delta_k = repl.len() as isize - (range.end - range.start) as isize; // usually negative

        // SPLICE inside the key-block region (one copy_within + one write)
        //
        // key block before: |<-- range --><-- rest -->|
        // key block after:  |<-- insert_bytes --><--range--><-- rest -->|
        let ks = self.keys_start();
        let new_len: u16 = (self.key_block_len() as isize + delta_k)
            .try_into()
            .map_err(|_e| PageError::CorruptedData {
                msg: "block length out of range".to_string(),
            })?;

        // Shift tail part
        let tail_src_start = ks + range.end;
        //let tail_src_end   = self.keys_end();
        let tail_src_end = self.slots_end(); // shift everthing in the key block + slot dir
        let tail_dst = ks + range.start;
        self.buf.copy_within(tail_src_start..tail_src_end, tail_dst);

        // adjust format metadata (restart offsets etc.)
        //let kb_final = &mut self.buf[ks..ks + new_len];
        //self.fmt().adjust_after_splice(kb_final, range.start, delta_k, idx);

        // Adjust key block length
        self.set_key_count(self.key_count().saturating_sub(1));
        self.set_key_block_len(new_len);
        Ok(key)
    }

    /// overwrite value at index `idx` with new value bytes (doesn't move old bytes).
    pub fn overwrite_value_at(&mut self, idx: usize, val_bytes: &[u8]) -> Result<(), PageError> {
        let (val_off, val_len) = self.alloc_value_tail(val_bytes)?; // respects slot region
        self.overwrite_slot_at(idx, val_off, val_len)
    }

    /// Append a key, value  pair, can be used during bulk loading.
    pub fn append(&mut self, key_enc: &[u8], val: &[u8]) -> Result<(), PageError> {
        // plan as an append
        let kb = self.key_block();
        let (range, repl) = self.key_fmt().insert_plan(
            kb,
            self.key_count() as usize,
            key_enc,
            &mut ScratchBuf::new(),
        );
        debug_assert_eq!(range.start, kb.len());
        debug_assert_eq!(range.end, kb.len());
        let delta_k = repl.len() as isize;

        // capacity check: keys grow by delta_k; slots grow by 1; values by val.len()
        let keys_end_new = (self.keys_end() as isize + delta_k) as usize;
        let slots_end_new = keys_end_new + (self.key_count() as usize + 1) * SLOT_SIZE;
        let values_hi_new = self
            .values_hi_usize()
            .checked_sub(val.len())
            .ok_or(PageError::PageFull {})?;
        if slots_end_new > values_hi_new {
            return Err(PageError::PageFull {});
        }

        // move slot dir by delta_k (kept flush)
        self.move_slot_dir(delta_k)?;

        // append key bytes (no tail shift)
        let ks = self.keys_start();
        let old_len = self.key_block_len() as usize;
        let new_len = old_len + repl.len();
        self.buf[ks + old_len..ks + new_len].copy_from_slice(&repl);
        self.set_key_block_len(new_len as u16);

        // append value + write slot at the end
        let (off, len) = self.alloc_value_tail(val)?;
        self.write_slot(
            self.key_count() as usize,
            LeafSlot {
                val_off: off,
                val_len: len,
            },
        )?;
        self.set_key_count(self.key_count() + 1);
        Ok(())
    }

    /// Return the *encoded key bytes* at index `idx`.
    pub fn get_key_at<'s>(
        &'s self,
        idx: usize,
        scratch: &mut ScratchBuf,
    ) -> Result<&'s [u8], PageError> {
        if idx >= self.key_count() as usize {
            return Err(PageError::IndexOutOfBounds {});
        }
        Ok(self.key_fmt().decode_at(self.key_block(), idx, scratch))
    }

    /// Return (encoded_key, value_bytes) at index `idx`.
    pub fn get_kv_at<'s>(
        &'s self,
        idx: usize,
        scratch: &'s mut ScratchBuf,
    ) -> Result<(&'s [u8], &'s [u8]), PageError> {
        if idx >= self.key_count() as usize {
            return Err(PageError::IndexOutOfBounds {});
        }
        let k = self.get_key_at(idx, scratch)?;
        let v = self.read_value_at(idx)?;
        Ok((k, v))
    }

    pub fn find_value(
        &self,
        key_enc: &[u8],
        scratch: &mut ScratchBuf,
    ) -> Result<Option<&[u8]>, PageError> {
        if let Ok(idx) = self.find_slot(key_enc, scratch) {
            let v = self.read_value_at(idx)?;
            Ok(Some(v))
        } else {
            Ok(None)
        }
    }

    // -------- delete (by index) --------

    /// delete key and value by encoded key bytes
    pub fn delete(&mut self, key_enc: &[u8]) -> Result<(), PageError> {
        // 1) find position
        let mut scratch = ScratchBuf::new();

        let idx = match self.find_slot(key_enc, &mut scratch) {
            Ok(idx) => idx,
            Err(_idx) => _idx, // not found, use insertion point
        };
        self.delete_at(idx)
    }

    /// Delete key and value at index `idx`.
    pub fn delete_at(&mut self, idx: usize) -> Result<(), PageError> {
        if idx >= self.key_count() as usize {
            return Err(PageError::IndexOutOfBounds {});
        }
        let mut scratch = ScratchBuf::new();

        // Plan and get delta_k
        let kb = self.key_block(); // &[u8]
        let (range, repl) = self.key_fmt().delete_plan(kb, idx, &mut scratch); // same idea as insert_plan
        let delta_k = repl.len() as isize - (range.end - range.start) as isize; // usually negative

        // SPLICE inside the key-block region (one copy_within + one write)
        //
        // key block before: |<-- range --><-- rest -->|
        // key block after:  |<-- insert_bytes --><--range--><-- rest -->|
        let ks = self.keys_start();
        let new_len: u16 = (self.key_block_len() as isize + delta_k)
            .try_into()
            .map_err(|_e| PageError::CorruptedData {
                msg: "block length out of range".to_string(),
            })?;

        // remove value slot
        self.slot_dir_remove(idx)?;
        // Shift tail part
        let tail_src_start = ks + range.end;
        //let tail_src_end   = self.keys_end();
        let tail_src_end = self.slots_end(); // shift everthing in the key block + slot dir
        let tail_dst = ks + range.start;
        self.buf.copy_within(tail_src_start..tail_src_end, tail_dst);

        // adjust format metadata (restart offsets etc.)
        //let kb_final = &mut self.buf[ks..ks + new_len];
        //self.fmt().adjust_after_splice(kb_final, range.start, delta_k, idx);

        // Adjust key block length
        self.set_key_count(self.key_count().saturating_sub(1));
        self.set_key_block_len(new_len);
        self.compact_values()?;
        Ok(())
    }

    // -------- compaction (optional) --------

    /// Pack value bytes tightly at the end and fix slot offsets. Should be called periodiacally
    /// after deletes and before merges.
    pub fn compact_values(&mut self) -> Result<(), PageError> {
        let n = self.key_count() as usize;
        if n == 0 {
            self.set_values_hi(BUFFER_SIZE as u16);
            return Ok(());
        }
        let mut dst = BUFFER_SIZE;
        // 1) Snapshot all (slot_index, off, len)
        let mut items: Vec<(usize, usize, usize)> = Vec::with_capacity(n);
        for i in 0..n {
            let s = self.read_slot(i)?;
            items.push((i, s.val_off as usize, s.val_len as usize));
        }

        // 2) Sort by old offset ASC (we'll iterate DESC to move toward higher addresses)
        items.sort_unstable_by_key(|&(_, off, _)| off);
        for &(idx, off, len) in items.iter().rev() {
            dst -= len;
            self.buf.copy_within(off..off + len, dst);
            // update slot
            self.write_slot(
                idx,
                LeafSlot {
                    val_off: dst as u16,
                    val_len: len as u16,
                },
            )?;
        }
        self.set_values_hi(dst as u16);
        Ok(())
    }

    // ====== internals ======
    // ---- slot dir ops ----

    // Move the entire slot directory by Δk bytes to keep it flush with the key block.
    fn move_slot_dir(&mut self, delta_k: isize) -> Result<(), PageError> {
        if delta_k == 0 {
            return Ok(());
        }
        let from = self.slots_base();
        let to = self.slots_end();
        let dst = (from as isize + delta_k) as usize;
        self.buf.copy_within(from..to, dst);
        Ok(())
    }

    fn slot_off_for(&self, idx: usize) -> usize {
        self.slots_base() + idx * SLOT_SIZE
    }

    fn read_slot(&self, idx: usize) -> Result<LeafSlot, PageError> {
        if idx >= self.key_count() as usize {
            return Err(PageError::IndexOutOfBounds {});
        }
        let base = self.slot_off_for(idx);
        Ok(LeafSlot {
            val_off: read_u16_le(&self.buf, base),
            val_len: read_u16_le(&self.buf, base + OFF_SIZE),
        })
    }

    fn write_slot(&mut self, idx: usize, slot: LeafSlot) -> Result<(), PageError> {
        if idx > self.key_count() as usize {
            return Err(PageError::IndexOutOfBounds {});
        }
        let base = self.slot_off_for(idx);
        write_u16_le(&mut self.buf, base, slot.val_off);
        write_u16_le(&mut self.buf, base + OFF_SIZE, slot.val_len);
        Ok(())
    }

    fn slot_dir_insert(&mut self, idx: usize, slot: LeafSlot) -> Result<(), PageError> {
        let kc = self.key_count() as usize;
        if idx > kc {
            return Err(PageError::IndexOutOfBounds {});
        }
        // shift right by one entry
        let base = self.slots_base();
        let from = base + idx * SLOT_SIZE;
        let to = base + kc * SLOT_SIZE;
        self.buf.copy_within(from..to, from + SLOT_SIZE);
        // write new
        write_u16_le(&mut self.buf, from, slot.val_off);
        write_u16_le(&mut self.buf, from + LEN_SIZE, slot.val_len);
        Ok(())
    }

    // Remove slot at idx, shifting left.
    // |-- idx --|-- idx+1 --| ... |-- last --|
    // |len off  |len off    | ... |len off   |
    // |<---------- kc * SLOT_SIZE ----------->|
    fn slot_dir_remove(&mut self, idx: usize) -> Result<(), PageError> {
        let kc = self.key_count() as usize;
        if idx >= kc {
            return Err(PageError::IndexOutOfBounds {});
        }
        let base = self.slots_base(); // keys_end

        if idx == kc - 1 {
            // last slot, nothing to shift, just zero it (optional)
            //let last = base + (kc - 1) * SLOT_SIZE;
            //for b in &mut self.buf[last..last + SLOT_SIZE] { *b = 0; }
            return Ok(());
        }

        let from = base + (idx + 1) * SLOT_SIZE;
        let to = base + (kc) * SLOT_SIZE;
        let dest = base + idx * SLOT_SIZE;
        // shift left by one
        self.buf.copy_within(from..to, dest);
        // zero last slot (optional)
        //let last = base + (kc - 1) * SLOT_SIZE;
        //for b in &mut self.buf[last..last + SLOT_SIZE] { *b = 0; }
        Ok(())
    }

    // ---- value arena ----

    // Allocate value at tail **below current slots** (uses header.values_hi and slot count).
    fn alloc_value_tail(&mut self, val: &[u8]) -> Result<(u16, u16), PageError> {
        let val_len = val.len();
        let new_hi = self
            .values_hi_usize()
            .checked_sub(val_len)
            .ok_or(PageError::PageFull {})?;
        if new_hi < self.slots_end() {
            return Err(PageError::PageFull {});
        }
        self.buf[new_hi..new_hi + val_len].copy_from_slice(val);
        self.set_values_hi(new_hi as u16);
        Ok((new_hi as u16, val_len as u16))
    }

    // ---- splitting ----

    /// Split this leaf into `right`, returning the encoded separator (first key of `right`).
    /// Does *not* decode all keys; the format handles right-block fixups internally.
    /// The original page keeps all keys/values below `split_idx`, the `right` page gets the rest.
    pub fn split_off_into(
        &mut self,
        split_idx: usize,
        right: &mut LeafPage,
    ) -> Result<Vec<u8>, PageError> {
        let key_count = self.key_count() as usize;
        let kb = self.key_block(); // entries region only

        // 1) ask the format to produce left/right key-block bytes
        let mut left_kb = Vec::new();
        let mut right_kb = Vec::new();
        self.key_fmt()
            .split_into(kb, split_idx, &mut left_kb, &mut right_kb);

        // 2) BEFORE we change key_count, snapshot the slots for the right side
        let mut moved_slots = Vec::with_capacity(key_count - split_idx);
        for i in split_idx..key_count {
            moved_slots.push(self.read_slot(i)?);
        }

        // 3) Shrink left page's key-block in place (move slot-dir by Δk and overwrite)
        let old_len = kb.len();
        let delta_k = left_kb.len() as isize - old_len as isize; // negative
        self.move_slot_dir(delta_k)?;
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
        for (i, slot) in moved_slots.iter().enumerate() {
            let off = slot.val_off as usize;
            let len = slot.val_len as usize;
            let v = &self.buf[off..off + len];
            let (new_off, new_len) = right.alloc_value_tail(v)?;
            right.write_slot(
                i,
                LeafSlot {
                    val_off: new_off,
                    val_len: new_len,
                },
            )?;
        }

        // 8) Separator = first key of right page (encoded key bytes)
        let mut scratch = ScratchBuf::new();
        let sep = self
            .key_fmt()
            .decode_at(right.key_block(), 0, &mut scratch)
            .to_vec();

        Ok(sep)
    }
}

// Tiny helper view handed to the KeyBlockFormat
struct PageKeyRun<'a> {
    body: &'a [u8],
    fmt: &'a dyn KeyBlockFormat,
}

impl<'a> PageKeyRun<'a> {
    fn seek(&self, needle: &[u8], scratch: &mut ScratchBuf) -> Result<usize, usize> {
        self.fmt.seek(self.body, needle, scratch)
    }
}

impl fmt::Debug for LeafPage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let keys_end = self.keys_end();
        let slots_base = self.slots_base();
        let slots_end = self.slots_end();
        let values_hi = self.values_hi_usize();
        let key_count = self.key_count() as usize;
        let key_block_len = self.key_block_len() as usize;
        let alternate = f.alternate();

        let mut dbg = f.debug_struct("LeafPage");
        dbg.field("fmt_id", &self.keyfmt_id())
            .field("keys", &key_count)
            .field("key_block_len", &key_block_len)
            .field("keys_end", &keys_end)
            .field("slots_base", &slots_base)
            .field("slots_end", &slots_end)
            .field("values_hi", &values_hi)
            .field("free_bytes", &values_hi.saturating_sub(slots_end));

        // Pretty mode: show a tiny preview
        if alternate {
            // first few keys (encoded previews)
            let fmt_impl = self.key_fmt();
            let mut scratch = ScratchBuf::new();
            let mut previews: Vec<String> = Vec::new();
            let sample = key_count.min(4);
            for i in 0..sample {
                let k = fmt_impl.decode_at(self.key_block(), i, &mut scratch);
                previews.push(k.iter().map(|b| format!("{:02x}", b)).collect());
            }
            dbg.field("keys_preview(hex)", &previews);

            // first few value lengths
            let mut v_lens: Vec<usize> = Vec::new();
            for i in 0..sample {
                if let Ok(slot) = self.read_slot(i) {
                    v_lens.push(slot.val_len as usize);
                }
            }
            dbg.field("value_lens", &v_lens);
        }

        dbg.finish()
    }
}

// ---- tests ----
#[cfg(test)]
mod tests {
    use super::*;
    use crate::keyfmt::raw::RawFormat;

    fn make_page() -> LeafPage {
        LeafPage::new(KeyFormat::Raw(RawFormat))
    }

    #[test]
    fn test_insert_and_get() {
        let mut page = make_page();
        let keys = ["apple", "banana", "blueberry", "cherry"];
        let values = ["red", "yellow", "blue", "dark red"];

        let kv_len = keys.len();

        for (k, v) in keys.iter().zip(values.iter()) {
            page.insert_encoded(k.as_bytes(), v.as_bytes()).unwrap();
        }

        let mut scratch = ScratchBuf::new();

        for i in 0..kv_len {
            let idx = page
                .lower_bound(keys[i].as_bytes(), &mut scratch)
                .expect("inserted value not found");
            let val = page.read_value_at(idx).expect("Could not read value");
            assert_eq!(*values[i].as_bytes(), *val);
        }
    }

    #[test]
    fn test_get_key_at_idx() {
        let mut page = make_page();

        let keys = ["apple", "banana", "blueberry", "cherry"];
        let values = ["red", "yellow", "blue", "dark red"];

        for (k, v) in keys.iter().zip(values.iter()) {
            page.insert_encoded(k.as_bytes(), v.as_bytes()).unwrap();
        }
        let mut scratch = ScratchBuf::new();

        for k in &keys {
            let idx = page.lower_bound(k.as_bytes(), &mut scratch);
            let s = match idx {
                Ok(i) => i,
                Err(i) => i,
            };
            let key = page
                .get_key_at(s, &mut scratch)
                .expect("Could not retrieve key");
            assert_eq!(*k.as_bytes(), *key);
        }
    }

    #[test]
    fn test_get_kv_at_idx() {
        let mut page = make_page();
        let keys = ["apple", "cherry", "banana", "blueberry"];
        let values = ["red", "dark red", "yellow", "blue"];
        let keys_sorted = ["apple", "banana", "blueberry", "cherry"];
        let values_sorted = ["red", "yellow", "blue", "dark red"];

        let kv_len = keys.len();

        for (k, v) in keys.iter().zip(values.iter()) {
            page.insert_encoded(k.as_bytes(), v.as_bytes()).unwrap();
        }

        let mut scratch = ScratchBuf::new();

        for i in 0..kv_len {
            let key = page
                .get_key_at(i, &mut scratch)
                .expect("Cannot retrieve key at idx");
            assert_eq!(*keys_sorted[i].as_bytes(), *key);
            let (k, v) = page
                .get_kv_at(i, &mut scratch)
                .expect("Cannot retrieve KV entry at idx");
            assert_eq!(*keys_sorted[i].as_bytes(), *k);
            assert_eq!(*values_sorted[i].as_bytes(), *v);
        }
    }

    #[test]
    fn test_delete() {
        let mut page = make_page();
        let keys = ["apple", "banana", "cherry"];
        let values = ["red", "yellow", "dark red"];

        for (k, v) in keys.iter().zip(values.iter()) {
            page.insert_encoded(k.as_bytes(), v.as_bytes()).unwrap();
        }

        page.delete_at(1).unwrap();

        let mut scratch = ScratchBuf::new();
        assert_eq!(page.key_count(), 2);
        let (ke0, ve0) = page.get_kv_at(0, &mut scratch).unwrap();

        assert_eq!(ke0, b"apple");
        assert_eq!(ve0, b"red");

        let (ke1, ve1) = page.get_kv_at(1, &mut scratch).unwrap();
        assert_eq!(ke1, b"cherry");
        assert_eq!(ve1, b"dark red");

        page.delete_at(1).unwrap();
        let res = page.get_kv_at(1, &mut scratch);
        assert!(res.is_err());

        let (ke0, ve0) = page.get_kv_at(0, &mut scratch).unwrap();
        assert_eq!(ke0, b"apple");
        assert_eq!(ve0, b"red");
    }

    #[test]
    fn test_split_off() {
        let mut page = make_page();
        let mut new_page = make_page();
        let keys = ["apple", "avocado", "banana", "cherry", "date"];
        let values = ["red", "green", "yellow", "dark red", "brown"];

        for (k, v) in keys.iter().zip(values.iter()) {
            page.insert_encoded(k.as_bytes(), v.as_bytes()).unwrap();
        }
        page.split_off_into(2, &mut new_page).unwrap();
        assert_eq!(page.kind(), LEAF_NODE_TAG);
        let mut scratch = ScratchBuf::new();
        assert_eq!(page.key_count(), 2);
        assert_eq!(new_page.key_count(), 3);

        let (ke0, ve0) = page.get_kv_at(0, &mut scratch).unwrap();
        assert_eq!(ke0, b"apple");
        assert_eq!(ve0, b"red");

        let (ke1, ve1) = page.get_kv_at(1, &mut scratch).unwrap();
        assert_eq!(ke1, b"avocado");
        assert_eq!(ve1, b"green");

        // new page, values  in it are (split_ix=2, inclusive): banana, cherry, date
        let (ke2, ve2) = new_page.get_kv_at(0, &mut scratch).unwrap();
        assert_eq!(ke2, b"banana");
        assert_eq!(ve2, b"yellow");
        let (ke3, ve3) = new_page.get_kv_at(1, &mut scratch).unwrap();
        assert_eq!(ke3, b"cherry");
        assert_eq!(ve3, b"dark red");
        let (ke4, ve4) = new_page.get_kv_at(2, &mut scratch).unwrap();
        assert_eq!(ke4, b"date");
        assert_eq!(ve4, b"brown");
    }

    #[test]
    fn test_replace_key() {
        let mut page = make_page();
        let keys = ["apple", "banana", "cherry"];
        let values = ["red", "yellow", "dark red"];

        for (k, v) in keys.iter().zip(values.iter()) {
            page.insert_encoded(k.as_bytes(), v.as_bytes()).unwrap();
        }

        // Replace "banana" with "blueberry"
        page.replace_key_at(1, "blueberry".as_bytes()).unwrap();

        let mut scratch = ScratchBuf::new();
        for (i, k) in ["apple", "blueberry", "cherry"].iter().enumerate() {
            let (ke, ve) = page.get_kv_at(i, &mut scratch).unwrap();
            assert_eq!(ke, k.as_bytes());
            assert_eq!(ve, values[i].as_bytes());
        }
    }

    #[test]
    fn test_append() {
        let mut page = make_page();
        let keys = ["apple", "banana", "cherry", "blueberry"];
        let values = ["red", "yellow", "dark red", "blue"];

        for (k, v) in keys.iter().zip(values.iter()) {
            page.append(k.as_bytes(), v.as_bytes()).unwrap();
        }

        let mut scratch = ScratchBuf::new();
        for (i, k) in keys.iter().enumerate() {
            let (ke, ve) = page.get_kv_at(i, &mut scratch).unwrap();
            assert_eq!(ke, k.as_bytes());
            assert_eq!(ve, values[i].as_bytes());
        }
    }

    //#[test]
    //fn test_compact_values() {
    //    let mut page = make_page();
    //    let keys = vec![b"apple", b"banana", b"cherry"];
    //    let values = vec![b"red", b"yellow", b"dark red"];

    //    for (k, v) in keys.iter().zip(values.iter()) {
    //        page.insert_encoded(k, v).unwrap();
    //    }

    //    // Overwrite "banana" value to a shorter one
    //    let (off, len) = page.alloc_value_tail(b"blue").unwrap();
    //    page.overwrite_value_at(1, off, len).unwrap();

    //    // Compact values
    //    page.compact_values();

    //    let mut scratch = ScratchBuf::new();
    //    for (i, k) in keys.iter().enumerate() {
    //        let (ke, ve) = page.get_kv_at(i, &mut scratch).unwrap();
    //        assert_eq!(ke, *k);
    //        if i == 1 {
    //            assert_eq!(ve, b"blue");
    //        } else {
    //            assert_eq!(ve, values[i]);
    //        }
    //    }
    //}
}
