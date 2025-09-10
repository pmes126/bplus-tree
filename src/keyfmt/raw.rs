//! [ u16_le klen | k bytes ] repeated

use super::{KeyBlockFormat, KeyFmtError};

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

    fn decode_at<'s>(&self, blk: &'s [u8], i: usize, scratch: &'s mut Vec<u8>) -> &'s [u8] {
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

    fn rebuild_window(
            &self,
            _block: &[u8],
            _start: usize,
            _end: usize,
            _new_keys: &[&[u8]],
            _out: &mut Vec<u8>,
        ) {
        
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
fn decode_at_idx<'s>(blk: &'s [u8], mut i: usize, _scratch: &'s mut Vec<u8>) -> &'s [u8] {
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


// src/keyfmt/raw.rs
//use super::KeyBlockFormat;
//
//#[derive(Copy, Clone)]
//pub struct RawFormat { pub restart_interval: u16 }
//
//impl KeyBlockFormat for RawFormat {
//    fn format_id(&self) -> u8 { 0 }
//
//    fn seek(&self, blk: &[u8], needle: &[u8], _sc: &mut Vec<u8>) -> (usize, bool) {
//        let Some((entries_end, restarts_off, rcount)) = tail(blk) else { return (0, false) };
//        if rcount == 0 { return (0, false); }
//        // 1) binary search restart blocks by first key
//        let mut lo = 0usize;
//        let mut hi = rcount;
//        while lo < hi {
//            let mid = (lo + hi) / 2;
//            let off = restart_off(blk, restarts_off, mid);
//            let k0 = entry_key(blk, off);
//            match k0.cmp(needle) {
//                core::cmp::Ordering::Greater => hi = mid,
//                _ => lo = mid + 1,
//            }
//        }
//        let block = lo.saturating_sub(1);
//        let mut off = restart_off(blk, restarts_off, block);
//        let mut idx = block * self.restart_interval as usize;
//
//        // 2) scan within block (≤ R entries or until entries_end)
//        while off < entries_end {
//            let (k, next) = entry_key_next(blk, off);
//            match k.cmp(needle) {
//                core::cmp::Ordering::Equal   => return (idx, true),
//                core::cmp::Ordering::Greater => return (idx, false),
//                core::cmp::Ordering::Less    => { off = next; idx += 1; }
//            }
//            if idx % self.restart_interval as usize == 0 { break; } // hit next block
//        }
//        (idx, false)
//    }
//
//    fn decode_at<'s>(&self, blk: &[u8], i: usize, _sc: &'s mut Vec<u8>) -> &'s [u8] {
//        let (_entries_end, restarts_off, _rcount) = tail(blk).expect("corrupt");
//        let block   = i / self.restart_interval as usize;
//        let inblock = i % self.restart_interval as usize;
//        let mut off = restart_off(blk, restarts_off, block);
//        for _ in 0..inblock {
//            let len = u16::from_le_bytes([blk[off], blk[off+1]]) as usize;
//            off += 2 + len;
//        }
//        let len = u16::from_le_bytes([blk[off], blk[off+1]]) as usize;
//        &blk[off+2 .. off+2+len]
//    }
//
//    fn encode_all(&self, keys: &[&[u8]], out: &mut Vec<u8>) {
//        out.clear();
//        let mut restarts: Vec<u32> = Vec::new();
//        let mut off = 0usize;
//        for (i, k) in keys.iter().enumerate() {
//            if i % self.restart_interval as usize == 0 {
//                restarts.push(off as u32);
//            }
//            let len = u16::try_from(k.len()).expect("key too large");
//            out.extend_from_slice(&len.to_le_bytes());
//            out.extend_from_slice(k);
//            off += 2 + k.len();
//        }
//        // tail: restart offsets + count
//        for r in &restarts { out.extend_from_slice(&r.to_le_bytes()); }
//        out.extend_from_slice(&(restarts.len() as u32).to_le_bytes());
//    }
//}
//
//// ---- private helpers ----
//fn tail(blk: &[u8]) -> Option<(usize, usize, usize)> {
//    if blk.len() < 4 { return None; }
//    let rcount = u32::from_le_bytes(blk[blk.len()-4..].try_into().ok()?) as usize;
//    let bytes  = rcount.checked_mul(4)?;
//    if blk.len() < 4 + bytes { return None; }
//    let restarts_off = blk.len() - 4 - bytes;
//    Some((restarts_off, restarts_off, rcount))
//}
//fn restart_off(blk: &[u8], restarts_off: usize, i: usize) -> usize {
//    let p = restarts_off + i * 4;
//    u32::from_le_bytes(blk[p..p+4].try_into().unwrap()) as usize
//}
//#[inline] fn entry_key<'a>(blk: &'a [u8], off: usize) -> &'a [u8] {
//    let len = u16::from_le_bytes([blk[off], blk[off+1]]) as usize;
//    &blk[off+2 .. off+2+len]
//}
//#[inline] fn entry_key_next<'a>(blk: &'a [u8], off: usize) -> (&'a [u8], usize) {
//    let len = u16::from_le_bytes([blk[off], blk[off+1]]) as usize;
//    (&blk[off+2 .. off+2+len], off + 2 + len)
//}
//
