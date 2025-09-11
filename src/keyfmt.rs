pub mod prefix;
pub mod raw;

#[derive(Debug, thiserror::Error)]
pub enum KeyFmtError {
    #[error("truncated")] Truncated,
    #[error("corrupt: {0}")] Corrupt(&'static str),
}

pub trait KeyBlockFormat: Send + Sync + 'static {
    /// Stable on-disk id; store this in the page header.
    fn format_id(&self) -> u8;

    /// Binary search in the key block; returns (insertion idx, found).
    fn seek(&self, block: &[u8], needle: &[u8], scratch: &mut Vec<u8>) -> Result<usize, usize>;

    /// Decode the i-th *encoded key bytes* into `scratch` and return a view.
    fn decode_at<'s>(&self, block: &'s [u8], i: usize, scratch: &'s mut Vec<u8>) -> &'s [u8];

    /// Re-encode the entire block from a sorted list of encoded keys.
    /// (Start with this; optimize to window rebuild later.)
    fn encode_all(&self, keys: &[&[u8]], out: &mut Vec<u8>);

    /// Re-encode a small window [start..end) replacing it with `new_keys` (sorted).
    /// Write bytes into `out` (caller will splice them back into the page region).
    fn rebuild_window(
        &self,
        block: &[u8],
        start: usize,
        end: usize,
        new_keys: &[&[u8]],
        out: &mut Vec<u8>,
    );
}

/// Runtime-configurable enum (handy for TreeConfig);
pub enum KeyFormat {
    Raw(raw::RawFormat),
    //Prefix(prefix::PrefixFormat),
}

impl KeyFormat {
    pub fn as_dyn(&self) -> &dyn KeyBlockFormat {
        match self {
            KeyFormat::Raw(f) => f,
          // KeyFormat::Prefix(f) => f,
        }
    }
    pub fn id(&self) -> u8 { self.as_dyn().format_id() }
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
