//! [`NodeStorage`] implementation backed by a [`PageStorage`] instance.
//!
//! This is the pluggable node encoding strategy. Different implementations
//! can use different codecs (e.g. prefix-compressed pages) while delegating
//! raw page I/O to the underlying [`PageStorage`].
//!
//! Includes an in-memory read cache (`RwLock<HashMap>`) that eliminates
//! repeated `pread` syscalls for hot pages. COW semantics guarantee that a
//! page's content never changes once written, so cache entries are always
//! valid until the page is freed and potentially reallocated.

use crate::bplustree::NodeView;
use crate::codec::bincode::NoopNodeViewCodec;
use crate::layout::PAGE_SIZE;
use crate::storage::epoch::EpochManager;
use crate::storage::{HasEpoch, NodeStorage, PageStorage, StorageError};

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, RwLock};

/// A [`NodeStorage`] that encodes node views as pages and delegates I/O to a [`PageStorage`].
///
/// Maintains an in-memory cache of decoded [`NodeView`]s keyed by page ID.
/// Cache correctness relies on COW: a page ID's content is immutable once
/// written. Entries are evicted only when [`free_node`] is called (the page
/// is reclaimed by epoch-based GC and may be reallocated).
pub struct PagedNodeStorage<S: PageStorage> {
    store: Arc<S>,
    epoch_mgr: Arc<EpochManager>,
    cache: RwLock<HashMap<u64, NodeView>>,
}

impl<S: PageStorage + Send + Sync + 'static> HasEpoch for PagedNodeStorage<S> {
    fn epoch_mgr(&self) -> &Arc<EpochManager> {
        &self.epoch_mgr
    }
}

impl<S: PageStorage + Send + Sync + 'static> PagedNodeStorage<S> {
    /// Opens (or creates) a [`PagedNodeStorage`] from the given data path.
    ///
    /// Creates its own [`EpochManager`]. Used by standalone callers (tests,
    /// benchmarks) that don't go through [`Database`][crate::database::Database].
    pub fn new<P: AsRef<Path>>(storage_path: P, _manifest_path: P) -> Result<Self, std::io::Error> {
        Ok(Self {
            store: Arc::new(S::open(storage_path)?),
            epoch_mgr: Arc::new(EpochManager::new()),
            cache: RwLock::new(HashMap::new()),
        })
    }

    /// Wraps a shared [`PageStorage`] with a shared epoch manager.
    ///
    /// Use this when the caller already owns the storage and epoch manager
    /// (e.g. [`Database`][crate::database::Database]) and wants to provide a
    /// pluggable [`NodeStorage`] over the same page file.
    pub fn from_parts(store: Arc<S>, epoch_mgr: Arc<EpochManager>) -> Self {
        Self {
            store,
            epoch_mgr,
            cache: RwLock::new(HashMap::new()),
        }
    }

    /// Returns a reference to the underlying page storage.
    pub fn page_storage(&self) -> &S {
        &self.store
    }

    /// Returns the shared [`Arc`] handle to the underlying page storage.
    pub fn page_storage_shared(&self) -> Arc<S> {
        Arc::clone(&self.store)
    }
}

impl<S: PageStorage + Send + Sync + 'static> NodeStorage for PagedNodeStorage<S> {
    fn read_node_view(&self, page_id: u64) -> Result<Option<NodeView>, StorageError> {
        // Fast path: check cache under a shared read lock.
        {
            let cache = self.cache.read().unwrap();
            if let Some(&view) = cache.get(&page_id) {
                return Ok(Some(view));
            }
        }

        // Slow path: read from disk, decode, and populate cache.
        let mut buf = [0u8; PAGE_SIZE];
        self.store.read_page(page_id, &mut buf)?;
        let mut view = NoopNodeViewCodec::decode(&buf)?;
        view.set_page_id(page_id);

        let mut cache = self.cache.write().unwrap();
        cache.insert(page_id, view);

        Ok(Some(view))
    }

    fn write_node_view(&self, node_view: &NodeView) -> Result<u64, StorageError> {
        let buf = NoopNodeViewCodec::encode(node_view)?;
        let page_id = self.store.write_page(buf)?;

        // Populate cache with the written node (already decoded).
        let mut cached = *node_view;
        cached.set_page_id(page_id);
        let mut cache = self.cache.write().unwrap();
        cache.insert(page_id, cached);

        Ok(page_id)
    }

    fn write_node_view_at_offset(
        &self,
        node_view: &NodeView,
        offset: u64,
    ) -> Result<u64, StorageError> {
        let buf = NoopNodeViewCodec::encode(node_view)?;
        let page_id = self.store.write_page_at_offset(offset, buf)?;

        // Update cache entry for this page ID.
        let mut cached = *node_view;
        cached.set_page_id(page_id);
        let mut cache = self.cache.write().unwrap();
        cache.insert(page_id, cached);

        Ok(page_id)
    }

    fn flush(&self) -> Result<(), StorageError> {
        self.store.flush().map_err(StorageError::Io)
    }

    fn free_node(&self, id: u64) -> Result<(), StorageError> {
        // Evict from cache before freeing — the page ID may be reallocated.
        {
            let mut cache = self.cache.write().unwrap();
            cache.remove(&id);
        }
        self.store.free_page(id).map_err(StorageError::Io)
    }
}
