use std::fs::{File, OpenOptions};
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::layout::PAGE_SIZE;
use crate::storage::PageStorage;
use crate::storage::metadata::INITIAL_PAGE_ID;

pub struct PageStore {
    file: Arc<File>,
    freed_pages: Mutex<Vec<u64>>,
    next_page_id: AtomicU64,
}

impl PageStore {
    pub fn flush(&self) -> Result<(), std::io::Error> {
        self.file.sync_data()
    }

    pub fn close(&self) -> Result<(), std::io::Error> {
        self.flush()
    }
}

impl Drop for PageStore {
    fn drop(&mut self) {
        if let Err(e) = self.close() {
            eprintln!("Error closing PageStore: {}", e);
        }
    }
}

impl PageStorage for PageStore {
    fn init<P: AsRef<Path>>(path: P) -> Result<Self, std::io::Error> where Self: Sized {
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
}
