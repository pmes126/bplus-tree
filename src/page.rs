pub mod internal_page;
pub mod leaf;

pub use internal_page::InternalPage;
pub use leaf::LeafPage;

pub const LEAF_NODE_TAG: u8 = 1;
pub const INTERNAL_NODE_TAG: u8 = 0;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum PageError {
    #[error("Slice too short: {msg}")]
    OffsetOutOfBounds { msg: String },
    #[error("PageFull: ")]
    PageFull {},
    #[error("Index out of bounds")]
    IndexOutOfBounds {},
    #[error("Error encoding value: {msg}")]
    InvalidPageSize { msg: String },
    #[error("Error converting from byte slice: {source}")]
    SliceTooShort {
        #[from]
        source: std::array::TryFromSliceError,
    },
    #[error("Corrupted page data: {msg}")]
    CorruptedData { msg: String },
    #[error("Error converting from byte slice")]
    FromBytesError { msg: String },
    #[error("IO error: {source}")]
    Io {
        #[from]
        source: std::io::Error,
    },
}
