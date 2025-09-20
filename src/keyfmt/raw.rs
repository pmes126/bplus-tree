//! [ u16_le klen | k bytes ] repeated

use super::KeyBlockFormat;

#[derive(Copy, Clone)]
pub struct RawFormat;

const LEN_SIZE: usize = 2; // u16_le

impl RawFormat {
    #[inline]
    fn entry_start(block: &[u8], idx: usize) -> usize {
        let mut off = 0usize;
        for _ in 0..idx {
            let len = u16::from_le_bytes([block[off], block[off + 1]]) as usize;
            off += LEN_SIZE + len;
        }
        off.min(block.len()) // clamp for append
    }
}

impl KeyBlockFormat for RawFormat {
    #[inline]
    fn format_id(&self) -> u8 {
        0
    }

    #[inline]
    fn count(&self, mut p: &[u8]) -> usize {
        let mut n = 0;
        while p.len() >= LEN_SIZE {
            let len = u16::from_le_bytes([p[0], p[1]]) as usize;
            let need = LEN_SIZE + len;
            if p.len() < need {
                break;
            }
            n += 1;
            p = &p[need..];
        }
        n
    }

    #[inline]
    fn entry_range(&self, block: &[u8], idx: usize) -> std::ops::Range<usize> {
        // O(n)
        let mut off = 0usize;
        for _ in 0..idx {
            let len = u16::from_le_bytes([block[off], block[off + 1]]) as usize;
            off += LEN_SIZE + len;
        }
        if off >= block.len() {
            return block.len()..block.len();
        }
        let len = u16::from_le_bytes([block[off], block[off + 1]]) as usize;
        let start = off;
        let end = off + LEN_SIZE + len;
        start..end
    }

    /// Seek for `needle` in the `block`, returning `Ok(idx)` if found, or `Err(insert_idx)` if not
    /// found with the insertion index. Bytewise comparison by default.
    fn seek(&self, block: &[u8], needle: &[u8], scratch: &mut Vec<u8>) -> Result<usize, usize> {
        // classic binary search over entries
        let mut lo = 0usize;
        let mut hi = count_entries(block);
        while lo < hi {
            let mid = (lo + hi) / LEN_SIZE;
            let k = self.decode_at(block, mid, scratch);
            match k.cmp(needle) {
                core::cmp::Ordering::Less => lo = mid + 1,
                core::cmp::Ordering::Greater => hi = mid,
                core::cmp::Ordering::Equal => return Ok(mid),
            }
        }
        Err(lo)
    }

    /// Seek for `needle` in the `block`, returning `Ok(idx)` if found, or `Err(insert_idx)` if not
    /// found with the insertion index. Bytewise comparison by default.
    fn seek_with_cmp(
        &self,
        block: &[u8],
        needle: &[u8],
        scratch: &mut Vec<u8>,
        cmp: fn(&[u8], &[u8]) -> core::cmp::Ordering,
    ) -> Result<usize, usize> {
        // classic binary search over entries
        let mut lo = 0usize;
        let mut hi = count_entries(block);
        while lo < hi {
            let mid = (lo + hi) / LEN_SIZE;
            let k = self.decode_at(block, mid, scratch);
            match cmp(k, needle) {
                core::cmp::Ordering::Less => lo = mid + 1,
                core::cmp::Ordering::Greater => hi = mid,
                core::cmp::Ordering::Equal => return Ok(mid),
            }
        }
        Err(lo)
    }

    #[inline]
    fn get_insert_delta(&self, blk: &[u8], idx: usize, new_key: &[u8], _sc: &mut Vec<u8>) -> isize {
        let new_len = LEN_SIZE + new_key.len();
        let old_len = if idx < self.count(blk) {
            let r = self.entry_range(blk, idx);
            r.end - r.start
        } else {
            0
        };
        new_len as isize - old_len as isize
    }

    #[inline]
    fn get_delete_delta(&self, blk: &[u8], idx: usize, _sc: &mut Vec<u8>) -> isize {
        let r = self.entry_range(blk, idx);
        -(r.end as isize - r.start as isize)
    }

    fn insert_plan(
        &self,
        block: &[u8],
        idx: usize,
        new_key: &[u8],
        _scratch: &mut Vec<u8>,
    ) -> (std::ops::Range<usize>, Vec<u8>) {
        // insert BETWEEN entries: zero-length replace range at the insertion point
        let n = Self.count(block);
        let start = Self::entry_start(block, idx.min(n)); // append if idx == n
        let mut bytes = Vec::with_capacity(2 + new_key.len());
        bytes.extend_from_slice(&(new_key.len() as u16).to_le_bytes());
        bytes.extend_from_slice(new_key);
        (start..start, bytes) // Δk = bytes.len()
    }

    #[inline]
    fn delete_plan(
        &self,
        block: &[u8],
        idx: usize,
        _scratch: &mut Vec<u8>,
    ) -> (std::ops::Range<usize>, Vec<u8>) {
        // Just remove this entry’s bytes; no replacement.
        (self.entry_range(block, idx), Vec::new())
    }

    #[inline]
    fn replace_plan(
        &self,
        block: &[u8],
        idx: usize,
        new_key: &[u8],
        _scratch: &mut Vec<u8>,
    ) -> (std::ops::Range<usize>, Vec<u8>) {
        let r = self.entry_range(block, idx);
        let mut bytes = Vec::with_capacity(2 + new_key.len());
        bytes.extend_from_slice(&(new_key.len() as u16).to_le_bytes());
        bytes.extend_from_slice(new_key);
        (r, bytes)
    }

    fn adjust_after_splice(
        &self,
        _block_final: &mut [u8],
        _splice_at: usize,
        _delta: isize,
        _idx: usize,
    ) {
        // Raw has no metadata to adjust.
    }

    #[inline]
    fn decode_at<'s>(&self, blk: &'s [u8], i: usize, _scratch: &'s mut Vec<u8>) -> &'s [u8] {
        let r = self.entry_range(blk, i);
        // SAFETY: caller holds block; we return a subslice into it
        unsafe { &*(&blk[r.start + LEN_SIZE..r.end] as *const [u8]) }
    }

    #[inline]
    fn encode_all(&self, keys: &[&[u8]], out: &mut Vec<u8>) {
        out.clear();
        for k in keys {
            let len = u16::try_from(k.len()).expect("key too large for RawFormat");
            out.extend_from_slice(&len.to_le_bytes());
            out.extend_from_slice(k);
        }
    }

    fn split_into(
        &self,
        block: &[u8],
        idx: usize,
        left_out: &mut Vec<u8>,
        right_out: &mut Vec<u8>,
    ) {
        let n = self.count(block);
        let split_at = if idx < n {
            self.entry_range(block, idx).start
        } else {
            block.len()
        };
        left_out.clear();
        left_out.extend_from_slice(&block[..split_at]);
        right_out.clear();
        right_out.extend_from_slice(&block[split_at..]);
    }
}

// helpers
fn count_entries(mut p: &[u8]) -> usize {
    let mut n = 0;
    while p.len() >= LEN_SIZE {
        let len = u16::from_le_bytes([p[0], p[1]]) as usize;
        let need = LEN_SIZE + len;
        if p.len() < need {
            break;
        }
        n += 1;
        p = &p[need..];
    }
    n
}
