pub mod leaf_page;
pub mod internal_page;

pub use leaf_page::LeafPage;
pub use internal_page::InternalPage;

pub const LEAF_NODE_TAG: u8 = 1;
pub const INTERNAL_NODE_TAG: u8 = 0;
