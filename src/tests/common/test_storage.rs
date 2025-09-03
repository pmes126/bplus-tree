#![allow(dead_code)]
use crate::bplustree::{Node, NodeView};
use crate::storage::MetadataStorage;
use crate::storage::NodeStorage;
use crate::storage::metadata::Metadata;
use crate::storage::metadata::MetadataPage;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};

// Import your trait (adjust the path to your crate’s module layout)
#[derive(Default, Debug)]
pub struct StorageState {
    commits: Vec<(u8, u64, u64, usize, usize, usize)>, // (slot, txn_id, root_id, height, order, size)
    flushes: u64,
    freed: Vec<u64>,
}

/// A simple, thread-safe fake Storage with logging + failure injection.
#[derive(Clone)]
pub struct TestStorage {
    pub state: Arc<Mutex<StorageState>>,
    pub fail_commit: Arc<AtomicBool>,
    pub fail_flush: Arc<AtomicBool>,
    root_node_id: u64, // This can be used to simulate a root node ID
}

impl TestStorage {
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(StorageState::default())),
            fail_commit: Arc::new(AtomicBool::new(false)),
            fail_flush: Arc::new(AtomicBool::new(false)),
            root_node_id: 2, // Initialize with a default root node ID
        }
    }

    // ------------ Failure injection ------------

    pub fn inject_commit_failure(&self, on: bool) {
        self.fail_commit.store(on, Ordering::Relaxed);
    }
    pub fn inject_flush_failure(&self, on: bool) {
        self.fail_flush.store(on, Ordering::Relaxed);
    }

    // ------------ Introspection / assertions ------------

    /// Returns the last (slot, txn_id, root_id, height, order, size).
    pub fn last_commit(&self) -> Option<(u8, u64, u64, usize, usize, usize)> {
        self.state.lock().unwrap().commits.last().copied()
    }

    pub fn all_commits(&self) -> Vec<(u8, u64, u64, usize, usize, usize)> {
        self.state.lock().unwrap().commits.clone()
    }

    pub fn flush_count(&self) -> u64 {
        self.state.lock().unwrap().flushes
    }

    pub fn freed_pages(&self) -> Vec<u64> {
        self.state.lock().unwrap().freed.clone()
    }
}

impl MetadataStorage for TestStorage {
    fn commit_metadata(
        &self,
        slot: u8,
        txn_id: u64,
        root_id: u64,
        height: usize,
        order: usize,
        size: usize,
    ) -> Result<(), std::io::Error> {
        if self.fail_commit.load(Ordering::Relaxed) {
            return Err(std::io::Error::other(
                "commit_metadata_with_object (injected failure)",
            ));
        }
        self.state
            .lock()
            .unwrap()
            .commits
            .push((slot, txn_id, root_id, height, order, size));
        Ok(())
    }

    fn write_metadata(&self, slot: u8, meta: &mut MetadataPage) -> Result<(), std::io::Error> {
        // Simulate writing metadata by just logging it
        self.state.lock().unwrap().commits.push((
            slot,
            meta.data.txn_id,
            meta.data.root_node_id,
            meta.data.height,
            meta.data.order,
            meta.data.size,
        ));
        Ok(())
    }

    fn read_metadata(&self, _slot: u8) -> Result<MetadataPage, std::io::Error> {
        // Simulate reading metadata by returning a dummy page
        let dummy_page: MetadataPage = unsafe { std::mem::zeroed() };
        Ok(dummy_page)
    }

    fn read_current_root(&self) -> Result<u64, std::io::Error> {
        // Simulate reading the current root by returning a dummy value
        Ok(0)
    }

    fn get_metadata(&self) -> Result<Metadata, std::io::Error> {
        // Simulate getting metadata by returning a dummy value
        Ok(Metadata {
            txn_id: 0,
            root_node_id: 0,
            height: 0,
            order: 0,
            size: 0,
            checksum: 0,
        })
    }

    fn commit_metadata_with_object(
        &self,
        slot: u8,
        metadata: &Metadata,
    ) -> Result<(), std::io::Error> {
        if self.fail_commit.load(Ordering::Relaxed) {
            return Err(std::io::Error::other(
                "commit_metadata_with_object (injected failure)",
            ));
        }
        // Simulate writing metadata by just logging it
        self.state.lock().unwrap().commits.push((
            slot,
            metadata.txn_id,
            metadata.root_node_id,
            metadata.height,
            metadata.order,
            metadata.size,
        ));
        Ok(())
    }
}

impl<K, V> NodeStorage<K, V> for TestStorage
where
    K: crate::storage::KeyCodec + Ord,
    V: crate::storage::ValueCodec,
{
    fn read_node(&self, _id: u64) -> Result<Option<Node<K, V>>, anyhow::Error> {
        // Simulate reading a node by returning None
        Ok(None)
    }

    fn write_node(&self, _node: &Node<K, V>) -> Result<u64, anyhow::Error> {
        // Simulate writing a node by returning a dummy ID
        Ok(0)
    }

    fn read_node_view(&self, _id: u64) -> Result<Option<NodeView>, anyhow::Error> {
        // Simulate reading a node view by returning None
        Ok(None)
    }

    fn write_node_view(&self, _node_view: &NodeView) -> Result<u64, anyhow::Error> {
        // Simulate writing a node view by returning a dummy ID
        Ok(0)
    }

    fn flush(&self) -> Result<(), std::io::Error> {
        if self.fail_flush.load(Ordering::Relaxed) {
            return Err(std::io::Error::other("flush (injected failure)"));
        }
        self.state.lock().unwrap().flushes += 1;
        Ok(())
    }

    fn free_node(&self, pid: u64) -> Result<(), std::io::Error> {
        self.state.lock().unwrap().freed.push(pid);
        Ok(())
    }
}
