// Constants for page size and metadata layout
pub(crate) const PAGE_SIZE: usize = 4096;

pub(crate) const SLOT_A_OFFSET: usize = 0;
pub(crate) const SLOT_B_OFFSET: usize = 64;
pub(crate) const ACTIVE_FLAG_OFFSET: usize = 4094;
pub(crate) const MAX_ENTRIES: usize = 248; // fits ~4KB with padding
