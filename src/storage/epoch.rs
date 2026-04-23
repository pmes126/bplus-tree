#![allow(dead_code)]

use crate::bplustree::NodeId;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::ThreadId;

/// A monotonically increasing epoch counter value.
pub type Epoch = u64;

/// Number of commits between automatic epoch advances for deferred reclamation.
pub const COMMIT_COUNT: u64 = 10;

/// Tracks active reader epochs and coordinates deferred page reclamation.
///
/// # Memory ordering
///
/// - **`global_epoch`**: advanced by writers with `SeqCst` (`fetch_add`) to
///   ensure the new epoch is visible before any deferred pages are tagged.
///   Readers load with `Acquire` in `pin()` to see all COW page writes that
///   preceded the epoch advance.
///
/// - **`active_readers`** / **`deferred_pages`**: protected by `Mutex`.  The
///   mutex lock/unlock provides implicit Acquire/Release barriers.
#[derive(Debug)]
pub struct EpochManager {
    global_epoch: AtomicU64,
    active_readers: Mutex<HashMap<ThreadId, Epoch>>,
    /// Mapping from epoch to the node IDs deferred for reclamation at that epoch.
    deferred_pages: Mutex<BTreeMap<u64, Vec<NodeId>>>,
}

impl EpochManager {
    /// Creates a new [`EpochManager`] starting at epoch 1.
    pub fn new() -> Self {
        Self {
            global_epoch: AtomicU64::new(1),
            active_readers: Mutex::new(HashMap::new()),
            deferred_pages: Mutex::new(BTreeMap::new()),
        }
    }

    /// Creates a new [`EpochManager`] wrapped in an [`Arc`].
    pub fn new_shared() -> Arc<Self> {
        Arc::new(Self {
            global_epoch: AtomicU64::new(1),
            active_readers: Mutex::new(HashMap::new()),
            deferred_pages: Mutex::new(BTreeMap::new()),
        })
    }

    /// Advances the global epoch by one and returns the new value.
    pub fn advance(&self) -> u64 {
        self.global_epoch.fetch_add(1, Ordering::SeqCst) + 1
    }

    /// Returns the current global epoch value.
    pub fn current(&self) -> u64 {
        self.global_epoch.load(Ordering::SeqCst)
    }

    /// Registers a page ID for deferred reclamation at the given epoch.
    pub fn add_reclaim_candidate(&self, epoch: u64, page_id: u64) {
        self.deferred_pages
            .lock()
            .unwrap()
            .entry(epoch)
            .or_default()
            .push(page_id);
    }

    /// Pins the current thread to the current epoch and returns a guard that unpins on drop.
    ///
    /// The epoch load and reader registration happen under the same lock to
    /// prevent a TOCTOU race: without this, a writer could call
    /// `oldest_active()` between our epoch load and our registration, see no
    /// readers, and free pages we are about to walk.
    pub fn pin(self: &Arc<Self>) -> ReaderGuard {
        let mut readers = self.active_readers.lock().unwrap();
        let epoch = self.global_epoch.load(Ordering::Acquire);
        let tid = std::thread::current().id();
        readers.insert(tid, epoch);
        drop(readers);

        ReaderGuard {
            epoch_mgr: Arc::clone(self),
            tid,
            epoch,
        }
    }

    /// Unpins the current thread from its epoch.
    pub fn unpin(&self) {
        let tid = std::thread::current().id();
        self.active_readers.lock().unwrap().remove(&tid);
    }

    /// Unpins a specific thread by its ID.
    pub fn unpin_by_id(&self, tid: ThreadId) {
        self.active_readers.lock().unwrap().remove(&tid);
    }

    /// Returns the minimum epoch still pinned by any active reader.
    pub fn oldest_active(&self) -> Epoch {
        let readers = self.active_readers.lock().unwrap();
        readers
            .values()
            .copied()
            .min()
            .unwrap_or(self.global_epoch.load(Ordering::Relaxed))
    }

    /// Collects and returns all page IDs deferred at epochs up to and including `safe_epoch`.
    pub fn reclaim(&self, safe_epoch: Epoch) -> Vec<NodeId> {
        let mut reclaimed = vec![];
        let to_reclaim: Vec<u64> = self
            .deferred_pages
            .lock()
            .unwrap()
            .range(..=safe_epoch)
            .map(|(e, _)| *e)
            .collect();

        for epoch in to_reclaim {
            if let Some(pages) = self.deferred_pages.lock().unwrap().remove(&epoch) {
                reclaimed.extend(pages);
            }
        }
        reclaimed
    }

    /// Returns the epoch currently pinned by the calling thread, if any.
    pub fn get_current_thread_epoch(&self) -> Option<Epoch> {
        let tid = std::thread::current().id();
        self.active_readers.lock().unwrap().get(&tid).copied()
    }

    #[cfg(test)]
    /// Returns a snapshot of the active readers map; for testing only.
    pub fn get_active_readers(&self) -> HashMap<ThreadId, Epoch> {
        self.active_readers.lock().unwrap().clone()
    }

    #[cfg(test)]
    /// Returns a snapshot of the deferred pages map; for testing only.
    pub fn get_deferred_pages(&self) -> BTreeMap<u64, Vec<NodeId>> {
        self.deferred_pages.lock().unwrap().clone()
    }

    #[cfg(test)]
    /// Returns the current global epoch value; for testing only.
    pub fn get_global_epoch(&self) -> Epoch {
        self.global_epoch.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    /// Overrides all active reader epochs to `epoch`; for testing only.
    pub fn set_oldest_active(&self, epoch: Epoch) {
        let mut readers = self.active_readers.lock().unwrap();
        for (_, e) in readers.iter_mut() {
            *e = epoch;
        }
    }

    #[cfg(test)]
    /// Sets the deferred reclamation list for `epoch`; for testing only.
    pub fn set_reclaim_list(&self, epoch: u64, pages: Vec<NodeId>) {
        let mut deferred = self.deferred_pages.lock().unwrap();
        deferred.insert(epoch, pages);
    }
}

impl Default for EpochManager {
    fn default() -> Self {
        EpochManager::new()
    }
}

/// An RAII guard that unpins the current thread from its epoch on drop.
pub struct ReaderGuard {
    epoch_mgr: Arc<EpochManager>,
    tid: ThreadId,
    epoch: Epoch,
}

impl ReaderGuard {
    /// Returns the epoch this guard is pinned to.
    fn epoch(&self) -> Epoch {
        self.epoch
    }
}

impl Drop for ReaderGuard {
    fn drop(&mut self) {
        self.epoch_mgr.unpin_by_id(self.tid);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_epoch_reclamation_flow() {
        let mgr = EpochManager::new_shared();

        let mut epochs = vec![];
        let iterations = 10;
        for _ in 0..iterations {
            let g = mgr.pin();
            epochs.push(g.epoch());
            mgr.advance();
        }

        for epoch in epochs[0..iterations - 1].iter() {
            let reclaim_id = *epoch;
            mgr.add_reclaim_candidate(*epoch, reclaim_id);
        }

        assert_eq!(mgr.active_readers.lock().unwrap().len(), 0);

        let safe = mgr.oldest_active();
        assert!(safe >= *epochs.last().unwrap());

        let reclaimed = mgr.reclaim(safe);
        assert!(reclaimed.len() == epochs.len() - 1);
    }

    #[test]
    fn test_epoch_manager_basic() {
        let mgr = EpochManager::new_shared();
        let initial_epoch = mgr.current();

        // Pin a reader
        let guard = mgr.pin();
        assert_eq!(guard.epoch(), initial_epoch);
        assert_eq!(mgr.get_current_thread_epoch(), Some(initial_epoch));

        // Advance the epoch
        let new_epoch = mgr.advance();
        assert_eq!(new_epoch, initial_epoch + 1);
        assert_eq!(mgr.current(), new_epoch);

        // Unpin the reader
        drop(guard);
        assert_eq!(mgr.get_current_thread_epoch(), None);
    }
}
