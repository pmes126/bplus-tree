//! File-backed implementation of [`PageStorage`].

use std::fs::{File, OpenOptions};
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::layout::PAGE_SIZE;
use crate::storage::PageStorage;

/// Page IDs 0–15 are reserved for internal metadata; user allocations start here.
const INITIAL_PAGE_ID: u32 = 16;

/// Magic number identifying a freelist snapshot file ("FLS1" in little-endian).
#[allow(dead_code)]
const FREE_LIST_SNAPSHOT_MAGIC: u32 = 0x314C5346;

/// A [`PageStorage`] backend that reads and writes fixed-size pages to a single flat file.
///
/// # Memory ordering
///
/// `next_page_id` uses `SeqCst` for all operations.  Strictly speaking `Relaxed`
/// would suffice — uniqueness is guaranteed by `fetch_add` atomicity, and page
/// data is synchronized by `fdatasync` rather than by memory ordering — but the
/// counter is not on the hot path and `SeqCst` makes the intent unambiguous.
pub struct FilePageStorage {
    file: Arc<File>,
    /// In-memory list of freed page IDs available for reuse.
    pub freed_pages: Mutex<Vec<u64>>,
    /// Monotonically increasing counter for allocating new page IDs.
    pub next_page_id: AtomicU64,
}

impl FilePageStorage {
    /// Flushes data pages to disk without flushing file metadata.
    pub fn flush(&self) -> Result<(), std::io::Error> {
        self.file.sync_data()
    }

    /// Flushes data to disk and closes the storage file.
    pub fn close(&self) -> Result<(), std::io::Error> {
        self.flush()
    }
}

impl Drop for FilePageStorage {
    fn drop(&mut self) {
        if let Err(e) = self.close() {
            eprintln!("Error closing PageStore: {}", e);
        }
    }
}

impl PageStorage for FilePageStorage {
    fn open<P: AsRef<Path>>(path: P) -> Result<Self, std::io::Error>
    where
        Self: Sized,
    {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;

        Ok(Self {
            file: Arc::new(file),
            freed_pages: Mutex::new(Vec::new()),
            next_page_id: AtomicU64::new(INITIAL_PAGE_ID as u64),
        })
    }

    fn close(&self) -> Result<(), std::io::Error> {
        self.flush()
    }

    fn read_page(&self, page_id: u64, target: &mut [u8; PAGE_SIZE]) -> Result<(), std::io::Error> {
        let offset = page_id * PAGE_SIZE as u64;
        self.file.read_exact_at(target, offset)?;
        Ok(())
    }

    fn write_page(&self, data: &[u8]) -> Result<u64, std::io::Error> {
        assert_eq!(data.len(), PAGE_SIZE);
        let page_id = self.allocate_page()?;
        let offset = page_id * PAGE_SIZE as u64;
        self.file.write_all_at(data, offset)?;
        Ok(page_id)
    }

    fn write_page_at_offset(&self, offset: u64, data: &[u8]) -> Result<u64, std::io::Error> {
        assert_eq!(data.len(), PAGE_SIZE);
        let page_offset = offset * PAGE_SIZE as u64;
        self.file.write_all_at(data, page_offset)?;
        Ok(offset)
    }

    fn allocate_page(&self) -> Result<u64, std::io::Error> {
        let mut freed = self.freed_pages.lock().unwrap();
        if let Some(page_id) = freed.pop() {
            Ok(page_id)
        } else {
            Ok(self.next_page_id.fetch_add(1, Ordering::SeqCst))
        }
    }

    fn free_page(&self, page_id: u64) -> Result<(), std::io::Error> {
        if page_id < INITIAL_PAGE_ID.into() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Cannot free initial pages",
            ));
        }
        let mut freed = self.freed_pages.lock().unwrap();
        freed.push(page_id);
        Ok(())
    }

    fn flush(&self) -> Result<(), std::io::Error> {
        self.flush()
    }

    fn set_next_page_id(&self, next_page_id: u64) -> Result<(), std::io::Error> {
        self.next_page_id.store(next_page_id, Ordering::SeqCst);
        Ok(())
    }

    fn set_freelist(&self, freed_pages: Vec<u64>) -> Result<(), std::io::Error> {
        let mut freed = self.freed_pages.lock().unwrap();
        *freed = freed_pages;
        Ok(())
    }

    fn get_next_page_id(&self) -> u64 {
        self.next_page_id.load(Ordering::SeqCst)
    }

    fn get_freelist(&self) -> Vec<u64> {
        self.freed_pages.lock().unwrap().clone()
    }
}
