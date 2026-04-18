#![allow(dead_code)]

use crate::bplustree::NodeView;
use crate::database::metadata::MetadataPage;
use crate::layout::PAGE_SIZE;
use crate::storage::epoch::EpochManager;
use crate::storage::{HasEpoch, NodeStorage, PageStorage, StorageError};

use std::path::Path;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use zerocopy::FromBytes;

/// Shared mutable state captured by all clones of a [`TestStorage`].
#[derive(Default, Debug)]
pub struct StorageState {
    /// Record of every metadata page written via [`PageStorage::write_page_at_offset`].
    /// Each tuple is `(slot, txn_id, root_id, height, order, size)`.
    pub commits: Vec<(u8, u64, u64, u64, u64, u64)>,
    /// Number of successful `flush` calls.
    pub flushes: u64,
    /// Page IDs passed to `free_node` or `free_page`.
    pub freed: Vec<u64>,
}

/// A thread-safe, in-memory fake storage backend with failure injection.
///
/// Implements both [`NodeStorage`] and [`PageStorage`] so a single instance can
/// serve as both the `S` and `P` type parameters of a [`BPlusTree`].
#[derive(Clone)]
pub struct TestStorage {
    pub state: Arc<Mutex<StorageState>>,
    /// When `true`, the next `write_page_at_offset` returns an I/O error.
    pub fail_commit: Arc<AtomicBool>,
    /// When `true`, the next `flush` returns an I/O error.
    pub fail_flush: Arc<AtomicBool>,
    /// Monotonically increasing counter used by `allocate_page` and `write_page`.
    pub next_page_id: Arc<AtomicU64>,
    /// Epoch manager shared with any tree that borrows this storage.
    pub epoch_mgr: Arc<EpochManager>,
}

impl TestStorage {
    /// Creates a new [`TestStorage`] with a fresh epoch manager.
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(StorageState::default())),
            fail_commit: Arc::new(AtomicBool::new(false)),
            fail_flush: Arc::new(AtomicBool::new(false)),
            next_page_id: Arc::new(AtomicU64::new(16)),
            epoch_mgr: EpochManager::new_shared(),
        }
    }

    // --- Failure injection ---

    /// Enables or disables forced commit (metadata write) failures.
    pub fn inject_commit_failure(&self, on: bool) {
        self.fail_commit.store(on, Ordering::Relaxed);
    }

    /// Enables or disables forced flush failures.
    pub fn inject_flush_failure(&self, on: bool) {
        self.fail_flush.store(on, Ordering::Relaxed);
    }

    // --- Introspection ---

    /// Returns the most recent commit record, if any.
    pub fn last_commit(&self) -> Option<(u8, u64, u64, u64, u64, u64)> {
        self.state.lock().unwrap().commits.last().copied()
    }

    /// Returns all commit records in the order they were written.
    pub fn all_commits(&self) -> Vec<(u8, u64, u64, u64, u64, u64)> {
        self.state.lock().unwrap().commits.clone()
    }

    /// Returns the number of successful `flush` calls.
    pub fn flush_count(&self) -> u64 {
        self.state.lock().unwrap().flushes
    }

    /// Returns all page IDs that have been freed.
    pub fn freed_pages(&self) -> Vec<u64> {
        self.state.lock().unwrap().freed.clone()
    }
}

impl Default for TestStorage {
    fn default() -> Self {
        Self::new()
    }
}

// --- HasEpoch ---

impl HasEpoch for TestStorage {
    fn epoch_mgr(&self) -> &Arc<EpochManager> {
        &self.epoch_mgr
    }
}

// --- NodeStorage ---

impl NodeStorage for TestStorage {
    fn read_node_view(&self, _id: u64) -> Result<Option<NodeView>, StorageError> {
        Ok(None)
    }

    fn write_node_view(&self, _node_view: &NodeView) -> Result<u64, StorageError> {
        Ok(self.next_page_id.fetch_add(1, Ordering::SeqCst))
    }

    fn write_node_view_at_offset(
        &self,
        _node_view: &NodeView,
        offset: u64,
    ) -> Result<u64, StorageError> {
        Ok(offset)
    }

    fn flush(&self) -> Result<(), StorageError> {
        if self.fail_flush.load(Ordering::Relaxed) {
            return Err(StorageError::Io(std::io::Error::other(
                "flush (injected failure)",
            )));
        }
        self.state.lock().unwrap().flushes += 1;
        Ok(())
    }

    fn free_node(&self, pid: u64) -> Result<(), StorageError> {
        self.state.lock().unwrap().freed.push(pid);
        Ok(())
    }
}

// --- PageStorage ---

impl PageStorage for TestStorage {
    fn open<P: AsRef<Path>>(_path: P) -> Result<Self, std::io::Error>
    where
        Self: Sized,
    {
        Ok(Self::new())
    }

    fn close(&self) -> Result<(), std::io::Error> {
        Ok(())
    }

    fn read_page(&self, _page_id: u64, target: &mut [u8; PAGE_SIZE]) -> Result<(), std::io::Error> {
        target.fill(0);
        Ok(())
    }

    fn write_page(&self, _data: &[u8]) -> Result<u64, std::io::Error> {
        Ok(self.next_page_id.fetch_add(1, Ordering::SeqCst))
    }

    /// Intercepts metadata page writes to record commit details for test assertions.
    fn write_page_at_offset(&self, offset: u64, data: &[u8]) -> Result<u64, std::io::Error> {
        if self.fail_commit.load(Ordering::Relaxed) {
            return Err(std::io::Error::other("commit (injected failure)"));
        }
        // Decode as MetadataPage and record for assertions.
        if data.len() == PAGE_SIZE {
            if let Some(page) = MetadataPage::ref_from(data) {
                let m = &page.data;
                self.state.lock().unwrap().commits.push((
                    offset as u8,
                    m.txn_id,
                    m.root_node_id,
                    m.height,
                    m.order,
                    m.size,
                ));
            }
        }
        Ok(offset)
    }

    fn allocate_page(&self) -> Result<u64, std::io::Error> {
        Ok(self.next_page_id.fetch_add(1, Ordering::SeqCst))
    }

    fn free_page(&self, page_id: u64) -> Result<(), std::io::Error> {
        self.state.lock().unwrap().freed.push(page_id);
        Ok(())
    }

    fn flush(&self) -> Result<(), std::io::Error> {
        if self.fail_flush.load(Ordering::Relaxed) {
            return Err(std::io::Error::other("flush (injected failure)"));
        }
        self.state.lock().unwrap().flushes += 1;
        Ok(())
    }

    fn set_next_page_id(&self, next_page_id: u64) -> Result<(), std::io::Error> {
        self.next_page_id.store(next_page_id, Ordering::SeqCst);
        Ok(())
    }

    fn set_freelist(&self, _freed_pages: Vec<u64>) -> Result<(), std::io::Error> {
        Ok(())
    }

    fn get_next_page_id(&self) -> u64 {
        self.next_page_id.load(Ordering::SeqCst)
    }

    fn get_freelist(&self) -> Vec<u64> {
        Vec::new()
    }
}
