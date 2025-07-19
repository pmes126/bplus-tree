pub mod leaf_page;
pub mod internal_page;

pub use leaf_page::LeafPage;
pub use internal_page::InternalPage;
use thiserror::Error;

pub const LEAF_NODE_TAG: u64 = 1;
pub const INTERNAL_NODE_TAG: u64 = 0;

#[derive(Debug, Error)]
pub enum PageCodecError {
    #[error("Slice too short: {msg}")]
    OffsetOutOfBounds {
        msg: String,
    },
    #[error("PageFull: {msg}")]
    PageFull {
        msg: String,
    },
    #[error("Error decoding value: {msg}")]
    IndexOutOfBounds {
        msg: String,
    },
    #[error("Error encoding value: {msg}")]
    InvalidPageSize {
        msg: String,
    },
    #[error("Error converting from byte slice: {source}")]
    SliceTooShort {
        #[from]
        source: std::array::TryFromSliceError,
    },
    #[error("Error converting from byte slice")]
    FromBytesError {
        msg: String,
    },
    #[error("IO error: {source}")]
    Io {
        #[from]
        source: std::io::Error,
    },
}
