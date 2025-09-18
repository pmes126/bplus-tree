//! Key block format with prefix compression and restart points.
//! [ entries ... ][ restart_offsets (u32 LE) ... ][ u32 LE restart_count ]
//entry = varint(shared) | varint(suffix_len) | suffix_bytes
//At restart entries: shared=0, entire key is in suffix.

//use super::{KeyBlockFormat, KeyFmtError};
//use super::varint::{read_uvar, write_uvar};
//
//#[derive(Copy, Clone)]
//pub struct PrefixFormat {
//    pub restart_interval: u16,
//}
//
//impl KeyBlockFormat for PrefixFormat {
//    fn format_id(&self) -> u8 { 1 }
//
//    fn seek(&self, blk: &[u8], needle: &[u8], scratch: &mut Vec<u8>) -> (usize, bool) {
//        let (entries_end, restarts, rcount) = tail(blk).unwrap_or((0, 0, 0));
//        // 1) binsearch restart blocks by first key in each block
//        let mut lo = 0usize;
//        let mut hi = rcount;
//        while lo < hi {
//            let mid = (lo + hi) / 2;
//            let start = restart_off(blk, restarts, mid);
//            let k0 = decode_at_from(blk, start, 0, scratch); // first key of block
//            match k0.cmp(needle) {
//                core::cmp::Ordering::Greater => hi = mid,
//                _ => lo = mid + 1, // <= needle
//            }
//        }
//        let block = lo.saturating_sub(1);
//        let mut off = restart_off(blk, restarts, block);
//        // 2) scan inside block
//        let mut idx = block * self.restart_interval as usize;
//        let mut cur = scratch;
//        cur.clear();
//        // first key at restart is full
//        let (shared, mut suf, mut n) = read_entry(blk, off).expect("corrupt");
//        debug_assert_eq!(shared, 0);
//        cur.extend_from_slice(suf);
//        match cur.as_slice().cmp(needle) {
//            core::cmp::Ordering::Equal => return (idx, true),
//            core::cmp::Ordering::Greater => return (idx, false),
//            core::cmp::Ordering::Less => { /* continue */ }
//        }
//        off += n; idx += 1;
//        // walk rest of block until next restart or end
//        while off < entries_end && !is_restart_boundary(blk, restarts, off) {
//            let (sh, suf2, n2) = read_entry(blk, off).expect("corrupt");
//            cur.truncate(sh as usize);
//            cur.extend_from_slice(suf2);
//            match cur.as_slice().cmp(needle) {
//                core::cmp::Ordering::Equal   => return (idx, true),
//                core::cmp::Ordering::Less    => { off += n2; idx += 1; }
//                core::cmp::Ordering::Greater => return (idx, false),
//            }
//        }
//        (idx, false)
//    }
//
//    fn decode_at<'s>(&self, blk: &[u8], i: usize, scratch: &'s mut Vec<u8>) -> &'s [u8] {
//        let (entries_end, restarts, _rcount) = tail(blk).expect("corrupt");
//        let (block, in_block) = div_mod(i, self.restart_interval as usize);
//        let mut off = restart_off(blk, restarts, block);
//        scratch.clear();
//        // base key at restart
//        let (shared, suf, n0) = read_entry(blk, off).expect("corrupt");
//        debug_assert_eq!(shared, 0);
//        scratch.extend_from_slice(suf);
//        off += n0;
//        for _ in 0..in_block {
//            let (sh, suf2, n) = read_entry(blk, off).expect("corrupt");
//            scratch.truncate(sh as usize);
//            scratch.extend_from_slice(suf2);
//            off += n;
//        }
//        scratch.as_slice()
//    }
//
//    fn encode_all(&self, keys: &[&[u8]], out: &mut Vec<u8>) {
//        out.clear();
//        let mut restarts: Vec<u32> = Vec::new();
//        let mut base_off = 0usize;
//
//        let mut prev: &[u8] = &[];
//        for (i, k) in keys.iter().enumerate() {
//            if i % self.restart_interval as usize == 0 {
//                // new restart
//                restarts.push(base_off as u32);
//                prev = &[];
//            }
//            let shared = common_prefix(prev, k);
//            let suffix = &k[shared..];
//            base_off += write_uvar(out, shared as u64);
//            base_off += write_uvar(out, suffix.len() as u64);
//            out.extend_from_slice(suffix);
//            base_off += suffix.len();
//            prev = k;
//        }
//
//        // append restart table and count
//        for r in &restarts {
//            out.extend_from_slice(&r.to_le_bytes());
//        }
//        out.extend_from_slice(&(restarts.len() as u32).to_le_bytes());
//    }
//
//    fn rebuild_window(
//            &self,
//            _block: &[u8],
//            _start: usize,
//            _end: usize,
//            _new_keys: &[&[u8]],
//            _out: &mut Vec<u8>,
//        ) {
//
//    }
//}
//
//// --- private helpers ---
//fn div_mod(i: usize, d: usize) -> (usize, usize) { (i / d, i % d) }
//
//fn common_prefix(a: &[u8], b: &[u8]) -> usize {
//    let n = a.len().min(b.len());
//    for i in 0..n {
//        if a[i] != b[i] { return i; }
//    }
//    n
//}
//fn tail(blk: &[u8]) -> Option<(usize, usize, usize)> {
//    if blk.len() < 4 { return None; }
//    let rcount = u32::from_le_bytes(blk[blk.len()-4..].try_into().unwrap()) as usize;
//    let restarts_size = rcount * 4;
//    if blk.len() < 4 + restarts_size { return None; }
//    let restarts = blk.len() - 4 - restarts_size;
//    Some((restarts, restarts, rcount))
//}
//fn restart_off(blk: &[u8], restarts: usize, idx: usize) -> usize {
//    let off = restarts + idx * 4;
//    u32::from_le_bytes(blk[off..off+4].try_into().unwrap()) as usize
//}
//fn is_restart_boundary(blk: &[u8], restarts: usize, off: usize) -> bool {
//    // naive: scan restart table for exact off; fine for small blocks
//    let rcount = (blk.len() - restarts - 4) / 4;
//    for i in 0..rcount {
//        if restart_off(blk, restarts, i) == off { return true; }
//    }
//    false
//}
//fn read_entry<'a>(blk: &'a [u8], off: usize) -> Option<(u64, &'a [u8], usize)> {
//    let (shared, n1) = read_uvar(&blk[off..])?;
//    let (suf_len, n2) = read_uvar(&blk[off+n1..])?;
//    let start = off + n1 + n2;
//    let end = start + suf_len as usize;
//    if end > blk.len() { return None; }
//    Some((shared, &blk[start..end], (n1 + n2) + suf_len as usize))
//}
