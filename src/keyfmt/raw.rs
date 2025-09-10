//! [ u16_le klen | k bytes ] repeated

use super::{KeyBlockFormat, KeyFmtError};
use crate::keyfmt::varint; // optional if you later switch to varints
use crate::page::LEN_SIZE;

#[derive(Copy, Clone)]
pub struct RawFormat;

impl KeyBlockFormat for RawFormat {
    fn format_id(&self) -> u8 { 0 }

    fn seek(&self, blk: &[u8], needle: &[u8], scratch: &mut Vec<u8>) -> (usize, bool) {
        // classic binary search over entries
        let mut lo = 0usize;
        let mut hi = count_entries(blk);
        while lo < hi {
            let mid = (lo + hi) / 2;
            let k = decode_at_idx(blk, mid, scratch);
            match k.cmp(needle) {
                core::cmp::Ordering::Less    => lo = mid + 1,
                core::cmp::Ordering::Greater => hi = mid,
                core::cmp::Ordering::Equal   => return (mid, true),
            }
        }
        (lo, false)
    }

    fn decode_at<'s>(&self, blk: &[u8], i: usize, scratch: &'s mut Vec<u8>) -> &'s [u8] {
        decode_at_idx(blk, i, scratch)
    }

    fn encode_all(&self, keys: &[&[u8]], out: &mut Vec<u8>) {
        out.clear();
        for k in keys {
            let len = u16::try_from(k.len()).expect("key too large for RawFormat");
            out.extend_from_slice(&len.to_le_bytes());
            out.extend_from_slice(k);
        }
    }
}

// helpers
fn count_entries(mut p: &[u8]) -> usize {
    let mut n = 0;
    while p.len() >= 2 {
        let len = u16::from_le_bytes([p[0], p[1]]) as usize;
        let need = 2 + len;
        if p.len() < need { break; }
        n += 1;
        p = &p[need..];
    }
    n
}

// Decode the i-th entry into scratch and return a view
fn decode_at_idx<'s>(blk: &[u8], mut i: usize, _scratch: &'s mut Vec<u8>) -> &'s [u8] {
    let mut off = 0usize;
    while i > 0 {
        let len = u16::from_le_bytes([blk[off], blk[off+1]]) as usize;
        off += 2 + len;
        i -= 1;
    }
    let len = u16::from_le_bytes([blk[off], blk[off+1]]) as usize;
    let start = off + 2;
    &blk[start..start+len]
}
