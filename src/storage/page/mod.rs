pub mod leaf_page;
pub mod internal_page;

pub use leaf_page::LeafPage;
pub use internal_page::InternalPage;

pub const LEAF_NODE_TAG: u8 = 1;
pub const INTERNAL_NODE_TAG: u8 = 0;

#[derive(Debug)]
pub enum PageCodecError {
    OffsetOutOfBounds(String),
    IndexOutOfBounds(String),
    InvalidPageSize,
    SliceTooShort(String),
    PageFull,
    FromBytesError(String),
}

impl std::fmt::Display for PageCodecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PageCodecError::OffsetOutOfBounds(msg) => write!(f, "Offset out of bounds: {}", msg),
            PageCodecError::IndexOutOfBounds(msg) => write!(f, "Index out of bounds: {}", msg),
            PageCodecError::InvalidPageSize => write!(f, "Invalid page size"),
            PageCodecError::SliceTooShort(msg) => write!(f, "Slice too short: {}", msg),
            PageCodecError::PageFull => write!(f, "Page is full"),
            PageCodecError::FromBytesError(msg) => write!(f, "Error converting from bytes: {}", msg),
        }
    }
}
