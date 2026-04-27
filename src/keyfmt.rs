pub mod prefix;
pub mod raw;

use smallvec::SmallVec;

/// Inline capacity for scratch buffers used during key decoding and search.
/// 256 bytes covers virtually all practical key sizes without heap allocation.
pub const SCRATCH_CAP: usize = 256;

/// Stack-allocated scratch buffer for key operations. Spills to heap only if a
/// key exceeds [`SCRATCH_CAP`] bytes.
pub type ScratchBuf = SmallVec<[u8; SCRATCH_CAP]>;

#[derive(Debug, thiserror::Error)]
#[allow(dead_code)]
pub enum KeyFmtError {
    #[error("truncated")]
    Truncated,
    #[error("corrupt: {0}")]
    Corrupt(&'static str),
}

pub trait KeyBlockFormat: Send + Sync + 'static {
    /// Stable on-disk id; store this in the page header.
    fn format_id(&self) -> u8;

    // -----------layout / capacity--------
    // -----------lookups / scans (read-only)--------
    /// Binary search in the key block; returns (insertion idx, found).
    fn seek(&self, block: &[u8], needle: &[u8], scratch: &mut ScratchBuf) -> Result<usize, usize>;
    /// Binary search in the key block with a provided comparator; returns (insertion idx, found).
    fn seek_with_cmp(
        &self,
        block: &[u8],
        needle: &[u8],
        scratch: &mut ScratchBuf,
        cmp: fn(&[u8], &[u8]) -> core::cmp::Ordering,
    ) -> Result<usize, usize>;
    /// Decode the i-th *encoded key bytes* into `scratch` and return a view.
    //fn decode_at(&self, block: &[u8], i: usize, scratch: &mut ScratchBuf) -> &[u8];
    fn decode_at<'s>(&self, blk: &'s [u8], i: usize, _scratch: &mut ScratchBuf) -> &'s [u8];
    /// Decodes the length of an entry and returns the Range of bytes for the entry.
    fn entry_range(&self, block: &[u8], idx: usize) -> std::ops::Range<usize>;
    /// Count the number of entries in the block.
    fn count(&self, p: &[u8]) -> usize;
    // ---------- plan phase (no mutation) ----------
    /// Byte delta if we insert `new_key` at logical index `idx`.
    /// Positive = grows, negative = shrinks.
    fn get_insert_delta(
        &self,
        block: &[u8],
        idx: usize,
        new_key: &[u8],
        scratch: &mut ScratchBuf,
    ) -> isize;
    /// Byte delta if we delete the key at `idx`.
    fn get_delete_delta(&self, block: &[u8], idx: usize, scratch: &mut ScratchBuf) -> isize;
    /// Re-encode the entire block from a sorted list of encoded keys.
    /// (Start with this; optimize to window rebuild later.)
    fn encode_all(&self, keys: &[&[u8]], out: &mut Vec<u8>);
    // ---------- mutate phase (do mutation) ----------
    /// PLAN: return the byte range in the `block` to replace, this is occupied by the previous
    /// value and the exact bytes to insert there.
    /// `delta = insert_bytes.len() as isize - (range.end - range.start) as isize`
    fn insert_plan(
        &self,
        block: &[u8],
        idx: usize,
        new_key: &[u8],
        scratch: &mut ScratchBuf,
    ) -> (std::ops::Range<usize>, Vec<u8>);
    /// PLAN: return the byte range in the `block` to remove, and the exact bytes to insert there.
    fn delete_plan(
        &self,
        block: &[u8],
        idx: usize,
        scratch: &mut ScratchBuf,
    ) -> (std::ops::Range<usize>, Vec<u8>);
    /// PLAN: return the byte range in the `block` to replace, and the exact bytes to insert there.
    fn replace_plan(
        &self,
        block: &[u8],
        idx: usize,
        new_key: &[u8],
        scratch: &mut ScratchBuf,
    ) -> (std::ops::Range<usize>, Vec<u8>);
    /// After the splice was applied to the page buffer, adjust any **format metadata**
    /// inside the final key-block (e.g., restart offsets) affected by the splice.
    /// - `splice_at` is the start byte within the key-block where you inserted/replaced
    /// - `delta` is the net size change (positive = grew)
    fn adjust_after_splice(
        &self,
        block_final: &mut [u8],
        splice_at: usize,
        delta: isize,
        idx: usize,
    );
    /// Split the key block at logical entry `idx`, writing valid left/right blocks.
    /// Implementations must avoid full re-encode:
    /// - Raw: just slice at the entry boundary.
    /// - Raw+Restarts: slice + keep only restart offsets on each side, shifting them relative to side.
    /// - Prefix+Restarts: left = prefix of entries (no change); right = make entry `idx` a restart
    ///   (re-encode *only* that first right entry), keep subsequent entry bytes as-is, and rebuild
    ///   the right restart table relative to the new block. No need to decode all keys.
    fn split_into(&self, block: &[u8], idx: usize, left_out: &mut Vec<u8>, right_out: &mut Vec<u8>);
}

/// Runtime-configurable enum (handy for TreeConfig);
#[repr(u8)]
#[derive(Debug, Clone, Copy)]
pub enum KeyFormat {
    Raw(raw::RawFormat) = 0,
    //Prefix(prefix::PrefixFormat),
}

impl KeyFormat {
    pub fn as_dyn(&self) -> &dyn KeyBlockFormat {
        match self {
            KeyFormat::Raw(f) => f,
            // KeyFormat::Prefix(f) => f,
        }
    }
    pub fn id(&self) -> u8 {
        self.as_dyn().format_id()
    }
    pub fn from_id(id: u8) -> Option<Self> {
        match id {
            0 => Some(Self::Raw(raw::RawFormat)),
            //1 => Some(Self::Prefix(prefix::PrefixFormat { restart_interval: 16 })),
            _ => None,
        }
    }
}

/// Static singletons → used by pages to resolve `key_format_id`
/// If you want per-page params (e.g., restart_interval), put them in the header and
/// pass them through page → format; otherwise, fix them here.
pub static RAW_FORMAT: raw::RawFormat = raw::RawFormat;
//pub static PREFIX_FORMAT: prefix::PrefixFormat = prefix::PrefixFormat { restart_interval: 16 };

/// Simple resolver used by pages (leaf/internal) to map header `key_format_id` to a format.
pub fn resolve_key_format(id: u8) -> Option<&'static dyn KeyBlockFormat> {
    match id {
        0 => Some(&RAW_FORMAT),
        //1 => Some(&PREFIX_FORMAT),
        _ => None,
    }
}

#[allow(dead_code)]
pub fn key_format_to_u8(fmt: &dyn KeyBlockFormat) -> u8 {
    fmt.format_id()
}
