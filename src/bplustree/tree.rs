#![allow(dead_code)]

use std::fmt::Debug;
use std::sync::Arc;
use std::sync::atomic::{AtomicPtr, AtomicU64, AtomicUsize, Ordering};

use crate::bplustree::BPlusTreeIter;
use crate::bplustree::EpochManager;
use crate::bplustree::epoch::COMMIT_COUNT;
use crate::bplustree::{Node, NodeView};
use crate::codec::{CodecError, KeyCodec, ValueCodec};
use crate::metadata;
use crate::metadata::{
    Metadata, {METADATA_PAGE_1, METADATA_PAGE_2},
};
use crate::storage::{MetadataStorage, NodeStorage, StorageError};
use std::result::Result;
use thiserror::Error;

pub type NodeId = u64; // Type for node IDs
pub type PathNode = (NodeId, usize); // Type for path nodes (node ID and index in parent)

fn print_vec<T: std::fmt::Debug>(vec: &Vec<T>, msg: &str) {
    println!("{}: {:?}", msg, vec);
}

/// Result of inserting into a B+ tree node
pub enum InsertResult<N> {
    /// Node was updated in-place
    Updated(N),
    // Node was inserted
    Inserted(N),
}

/// Result of deleting from a B+ tree node
#[derive(Debug, Error)]
pub enum DeleteResult<N> {
    /// Node was updated in-place (no underflow)
    Deleted(N),
    /// Key was not found in the node
    NotFound,
}

pub enum SplitResult<N> {
    SplitNodes {
        left_node: N,  // Left half (including inserted key)
        right_node: N, // Right half
        split_key: Vec<u8>,  // First key of right node, to push into parent
    },
}

#[derive(Debug, Error)]
pub enum TreeError {
    #[error("Bad input: {0}")]
    BadInput(String),

    #[error("Failed to initialize backend: {0}")]
    BackendAny(String),

    #[error("Node Not Found: {0}")]
    NodeNotFound(String),

    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Codec(#[from] CodecError),
    #[error(transparent)]
    Any(#[from] anyhow::Error),
}

#[derive(Debug, Error)]
pub enum CommitError {
    #[error("Commit failed after {0} retries")]
    MaxRetries(usize),

    #[error("Commit aborted due to node not found: {0}")]
    NodeNotFound(String),

    #[error("Commit aborted due to IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Commit aborted due to codec error: {0}")]
    Codec(#[from] CodecError),

    #[error("Commit aborted due to root mismatch, try rebasing")]
    RebaseRequired,

    #[error("Test Commit error")]
    Injected,
}

pub trait TxnTracker {
    fn reclaim(&mut self, node_id: NodeId);
    fn add_new(&mut self, node_id: NodeId);
    fn record_staged_height(&mut self, height: usize);
    fn record_staged_size(&mut self, size: usize);
}

#[derive(Debug, Clone)]
pub struct MetadataSnapshot {
    pub root_id: NodeId,
    pub height: usize,
    pub size: usize,
    pub txn_id: u64,
    pub order: usize,
}

#[derive(Debug, Clone)]
pub struct StagedMetadata {
    pub root_id: NodeId,
    pub height: usize,
    pub size: usize,
}

// Pointer to the committed metadata
pub struct BaseVersion {
    pub committed_ptr: *const Metadata,
}

pub struct TreeConfig {
    pub page_size: usize,  // 4096 default (runtime, don't const-generic this yet)
    pub key_format_id: u8, // 0=Raw, 1=Raw+Restarts, 2=Prefix+Restarts (or whatever mapping)
    pub restart_interval: u16, // used by Prefix; ignored by others (or constrain via keyfmt)
    pub target_fill_bytes: usize, // split/merge by bytes, not count
}

/// B+ tree structure with generic key and value types, and a storage backend
pub struct BPlusTree<K, V, S>
where
    K: Ord + Clone,
    V: Clone,
    S: NodeStorage<K, V> + MetadataStorage + Send + Sync + 'static,
{
    max_keys: usize,
    min_internal_keys: usize,
    min_leaf_keys: usize,
    storage: S,
    epoch_mgr: Arc<EpochManager>, // Epoch manager for transaction management
    commit_count: AtomicUsize,    // Number of commits made to the tree
    txn_id: AtomicU64,            // Slot of metadata storage
    committed: AtomicPtr<Metadata>, // Pointer to the committed metadata,
    // Phantom data to hold the types of keys and values
    phantom: std::marker::PhantomData<(K, V)>,
}

#[derive(Default)]
pub struct TransactionTracker {
    pub reclaimed: Vec<NodeId>,
    pub added: Vec<NodeId>,
    pub staged_height: Option<usize>,
    pub staged_size: Option<usize>,
}

impl TransactionTracker {
    pub fn new() -> Self {
        Self {
            reclaimed: Vec::new(),
            added: Vec::new(),
            staged_height: None,
            staged_size: None,
        }
    }
}

impl TxnTracker for TransactionTracker {
    fn reclaim(&mut self, node_id: NodeId) {
        self.reclaimed.push(node_id);
    }
    fn add_new(&mut self, node_id: NodeId) {
        self.added.push(node_id);
    }
    fn record_staged_height(&mut self, height: usize) {
        self.staged_height = Some(height);
    }
    fn record_staged_size(&mut self, size: usize) {
        self.staged_size = Some(size);
    }
}

#[derive(Debug)]
pub struct WriteResult {
    pub new_root_id: NodeId,
    pub reclaimed_nodes: Vec<NodeId>,
    pub staged_nodes: Vec<NodeId>,
    pub new_height: usize,
    pub new_size: usize,
}

pub struct SharedBPlusTree<K, V, S>
where
    K: Ord + Clone,
    V: Clone,
    S: NodeStorage<K, V> + MetadataStorage + Send + Sync + 'static,
{
    inner: Arc<BPlusTree<K, V, S>>,
}

impl<K, V, S> Clone for SharedBPlusTree<K, V, S>
where
    K: Ord + Clone,
    V: Clone,
    S: NodeStorage<K, V> + MetadataStorage + Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<K: Debug, V: Debug, S> SharedBPlusTree<K, V, S>
where
    K: Clone + Ord,
    V: Clone,
    S: NodeStorage<K, V> + MetadataStorage + Send + Sync + 'static,
{
    pub fn new(tree: BPlusTree<K, V, S>) -> Self {
        Self {
            inner: Arc::new(tree),
        }
    }

    pub fn from_arc(tree: Arc<BPlusTree<K, V, S>>) -> Self {
        Self { inner: tree }
    }

    pub fn insert_with_root(
        &self,
        key: K,
        value: V,
        root_id: NodeId,
    ) -> Result<WriteResult, TreeError> {
        let mut collector = TransactionTracker::new();
        let new_root_id = self
            .inner
            .insert_inner(key, value, root_id, &mut collector)?;
        let write_res = WriteResult {
            new_root_id,
            reclaimed_nodes: std::mem::take(&mut collector.reclaimed),
            staged_nodes: std::mem::take(&mut collector.added),
            new_height: collector.staged_height.unwrap_or(self.inner.get_height()),
            new_size: collector.staged_size.unwrap_or(self.inner.get_size()),
        };
        Ok(write_res)
    }

    pub fn insert(&self, key: K, value: V) -> Result<WriteResult, TreeError> {
        let root_id = self.inner.get_root_id();
        self.insert_with_root(key, value, root_id)
    }

    pub fn delete_with_root(&self, key: &K, root_id: NodeId) -> Result<WriteResult, TreeError> {
        let mut collector = TransactionTracker::new();
        let delete_res = self.inner.delete_inner(key, root_id, &mut collector)?;
        let DeleteResult::Deleted(new_root_id) = delete_res else {
            return Err(TreeError::BackendAny(format!(
                "Failed to delete key: {:?}",
                delete_res
            )));
        };
        let write_res = WriteResult {
            new_root_id,
            reclaimed_nodes: std::mem::take(&mut collector.reclaimed),
            staged_nodes: std::mem::take(&mut collector.added),
            new_height: collector.staged_height.unwrap_or(self.inner.get_height()),
            new_size: collector.staged_size.unwrap_or(self.inner.get_size()),
        };
        Ok(write_res)
    }

    pub fn search(&self, key: &K) -> Result<Option<V>, TreeError> {
        self.inner.search(key)
    }

    pub fn search_with_root(&self, key: &K, root_id: NodeId) -> Result<Option<V>, TreeError> {
        self.inner.search_inner(key, root_id)
    }

    pub fn get_root_id(&self) -> NodeId {
        self.inner.get_root_id()
    }

    pub fn get_height(&self) -> usize {
        self.inner.get_height()
    }

    pub fn get_size(&self) -> usize {
        self.inner.get_size()
    }

    pub fn get_txn_id(&self) -> NodeId {
        self.inner.txn_id.load(Ordering::SeqCst)
    }

    pub fn flush(&mut self) -> Result<(), TreeError> {
        self.inner.storage.flush()?;
        Ok(())
    }

    pub fn get_snapshot(&self) -> MetadataSnapshot {
        self.inner.get_snapshot()
    }

    pub fn try_commit(
        &self,
        version: &BaseVersion,
        new_metadata: StagedMetadata,
    ) -> Result<(), CommitError> {
        self.inner.try_commit(version, new_metadata)
    }

    pub fn get_metadata_ptr(&self) -> *const Metadata {
        self.inner.committed.load(Ordering::SeqCst)
    }

    pub fn get_metadata(&self) -> &Metadata {
        unsafe { &*self.inner.committed.load(Ordering::Acquire) }
    }

    pub fn arc(&self) -> Arc<BPlusTree<K, V, S>> {
        Arc::clone(&self.inner)
    }

    #[allow(clippy::should_implement_trait)]
    pub fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }

    pub fn traverse(&self) -> Result<Vec<(K, V)>, TreeError> {
        self.inner.traverse()
    }

    pub fn search_range_at_root<'a>(
        &'a self,
        root_id: NodeId,
        start: &K,
        end: &K,
    ) -> Result<Option<BPlusTreeIter<'a, K, V, S>>, TreeError> {
        self.inner.search_range(root_id, start, end)
    }

    pub fn search_in_range<'a>(
        &'a self,
        start: &K,
        end: &K,
    ) -> Result<Option<BPlusTreeIter<'a, K, V, S>>, TreeError> {
        let root_id = self.inner.get_root_id();
        self.inner.search_range(root_id, start, end)
    }

    pub fn get_epoch_mgr(&self) -> Arc<EpochManager> {
        Arc::clone(&self.inner.epoch_mgr)
    }

    pub fn reclaim_node(&self, node_id: NodeId) -> Result<(), TreeError> {
        self.inner.reclaim_node(node_id)
    }
}

// BPlusTree implementation
impl<K: Debug, V: Debug, S> BPlusTree<K, V, S>
where
    K: Clone + Ord,
    V: Clone,
    S: NodeStorage<K, V> + MetadataStorage + Send + Sync + 'static,
{
    pub fn new(storage: S, order: usize) -> Result<BPlusTree<K, V, S>, TreeError> {
        let root_node = Node::Leaf {
            keys: Vec::with_capacity(order),
            values: Vec::with_capacity(order),
        };
        if order < 2 {
            return Err(TreeError::BadInput("Order must be at least 2".to_string()));
        }
        // Initialize the root node ID
        let init_id = storage
            .write_node(&root_node)
            .map_err(|e| TreeError::BackendAny(e.to_string()))?;
        let init_txn_id = 1; // Initial transaction ID
        let md1 = Metadata {
            root_node_id: init_id,
            txn_id: init_txn_id, // Initial transaction ID
            height: 1,           // Initial height
            checksum: 0,         // Placeholder for checksum
            size: 0,             // Initial size
            order,
        };
        let md2 = Metadata {
            root_node_id: init_id,
            txn_id: init_txn_id, // Initial transaction ID
            height: 1,           // Initial height
            checksum: 0,         // Placeholder for checksum
            size: 0,             // Initial size
            order,
        };
        let mut metadata_1 = metadata::new_metadata_page_with_object(&md1);
        let mut metadata_2 = metadata::new_metadata_page_with_object(&md2);
        storage.write_metadata(METADATA_PAGE_1, &mut metadata_1)?;
        storage.write_metadata(METADATA_PAGE_2, &mut metadata_2)?;

        let md_ptr = Box::new(md1); // Convert metadata to raw pointer

        Ok(Self {
            storage,
            max_keys: order - 1,
            min_internal_keys: order.div_ceil(2) - 1, // Ensure min_internal_keys is at least 2
            min_leaf_keys: (order - 1).div_ceil(2),   // Ensure min_keys is at least 1
            epoch_mgr: EpochManager::new_shared(),    // Initialize the epoch manager
            commit_count: AtomicUsize::new(0),        // Initialize commit count
            txn_id: AtomicU64::new(init_txn_id),      // Initialize transaction ID
            committed: AtomicPtr::new(Box::into_raw(md_ptr)), // Initialize committed pointer
            phantom: std::marker::PhantomData,
        })
    }

    pub fn load(storage: S) -> Result<BPlusTree<K, V, S>, TreeError> {
        println!("Loading B+ tree with root ID from storage");
        let md = storage.get_metadata()?;
        let md_ptr = Box::new(md);
        let order = md.order;
        debug_assert!(order >= 2);

        let max_keys = order - 1;
        let min_internal_keys = (order - 1).saturating_div(2); // Ensure min_internal_keys is at 
        let min_leaf_keys = order.saturating_div(2); // Ensure min_keys is at least 1

        Ok(Self {
            storage,
            max_keys,
            min_internal_keys,
            min_leaf_keys,
            epoch_mgr: EpochManager::new_shared(), // Initialize the epoch manager
            commit_count: AtomicUsize::new(0),     // Initialize commit count
            txn_id: AtomicU64::new(md.txn_id),     // Initialize transaction ID
            committed: AtomicPtr::new(Box::into_raw(md_ptr)), // Initialize committed pointer
            phantom: std::marker::PhantomData,
        })
    }

    #[cfg(any(test, feature = "testing"))]
    pub fn new_with_deps(storage: S, epoch_mgr: EpochManager, order: usize) -> Self {
        let meta = Metadata {
            root_node_id: 2,
            txn_id: 0,   // Initial transaction ID
            height: 1,   // Initial height
            checksum: 0, // Placeholder for checksum
            size: 0,     // Initial size
            order,
        };
        Self {
            storage,
            epoch_mgr: Arc::new(epoch_mgr),
            commit_count: 0.into(),
            max_keys: order - 1,
            min_internal_keys: order.div_ceil(2) - 1, // Ensure min_internal_keys is at least 2
            min_leaf_keys: (order - 1).div_ceil(2),   // Ensure min_keys is at least 1
            txn_id: AtomicU64::new(1),                // Initial transaction ID
            committed: AtomicPtr::new(Box::into_raw(Box::new(meta))), // Initialize committed pointer
            phantom: std::marker::PhantomData,
        }
    }

    // Reads a node from the B+ tree storage, using the cache if available.
    fn read_node(&self, id: NodeId) -> Result<Option<Node<K, V>>, TreeError> {
        self.storage
            .read_node(id)
            .map_err(|e| TreeError::BackendAny(format!("failed to read node {}:", e)))
    }

    // Writes a node to the B+ tree storage and updates the cache.
    fn write_node(
        &self,
        node: &Node<K, V>,
        tracker: &mut impl TxnTracker,
    ) -> Result<u64, TreeError> {
        let new_id = self
            .storage
            .write_node(node)
            .map_err(|e| TreeError::BackendAny(format!("failed to write node {}:", e)))?;
        tracker.add_new(new_id);
        Ok(new_id)
    }

    // Writes a node to the B+ tree storage and updates the cache.
    fn write_node_view(
        &self,
        node: &NodeView,
        tracker: &mut impl TxnTracker,
    ) -> Result<u64, TreeError> {
        let new_id = self
            .storage
            .write_node_view(node)
            .map_err(|e| TreeError::BackendAny(format!("failed to write node {}:", e)))?;
        tracker.add_new(new_id);
        Ok(new_id)
    }

    // Returns the path of where a key should be inserted, without decoding the nodes for
    // efficiency.
    pub fn get_insertion_path(
        &self,
        key: &K,
        root_id: NodeId,
    ) -> Result<(Vec<PathNode>, bool), TreeError> {
        let mut path = vec![];
        let mut current_id = root_id;
        let mut encode_buf = vec![0u8; S::KC::encoded_len(key)];

        S::KC::encode_key(key, encode_buf.as_mut())
            .map_err(|e| CodecError::EncodeFailure { msg: e.to_string() })?;
        // Find insertion point
        loop {
            match self.storage.read_node_view(current_id)? {
                Some(node) => match &node {
                    NodeView::Leaf { .. } => {
                        let mut found = false;
                        let i = match node
                            .lower_bound_cmp(encode_buf.as_ref(), S::KC::compare_encoded)
                        {
                            Ok(i) => {
                                found = true;
                                i
                            }
                            Err(i) => i,
                        };
                        path.push((current_id, i)); // Record the current node and index
                        return Ok((path, found));
                    }
                    NodeView::Internal { .. } => {
                        // Find the insertion point in the internal node
                        let i = match node
                            .lower_bound_cmp(encode_buf.as_ref(), S::KC::compare_encoded)
                        {
                            Ok(i) => i + 1,
                            Err(i) => i,
                        };
                        path.push((current_id, i)); // Record the current node and index
                        let child = node.child_ptr_at(i)?; // Move to the child node
                        if let Some(child_id) = child {
                            current_id = child_id; // Continue iteration
                        } else {
                            TreeError::BackendAny(format!(
                                "Internal node cannot retrieve child at index {}",
                                i
                            ));
                        }
                    }
                },
                None => {
                    // Node not found, this should not happen as we are traversing the path
                    return Err(TreeError::BackendAny(
                        "Node not found while getting insertion path".to_string(),
                    ));
                }
            }
        }
    }

    // Inserts a key-value pair into the B+ tree, acquiring an epoch guard to ensure consistency.
    pub fn insert(
        &self,
        key: K,
        value: V,
        track: &mut impl TxnTracker,
    ) -> Result<NodeId, TreeError> {
        let root_id = self.get_root_id();
        self.insert_inner(key, value, root_id, track)
    }

    // Inserts a key-value pair into the B+ tree.
    pub fn insert_inner(
        &self,
        key: K,
        value: V,
        root_id: NodeId,
        track: &mut impl TxnTracker,
    ) -> Result<NodeId, TreeError> {
        let mut key_buf = vec![0u8; S::KC::encoded_len(&key)];
        let mut val_buf = vec![0u8; S::VC::encoded_len(&value)];
        S::KC::encode_key(&key, key_buf.as_mut())
            .map_err(|e| CodecError::EncodeFailure { msg: e.to_string() })?;
        S::VC::encode_value(&value, val_buf.as_mut())
            .map_err(|e| CodecError::EncodeFailure { msg: e.to_string() })?;

        let _guard = self.epoch_mgr.pin();
        let (mut path, found) = self.get_insertion_path(&key, root_id)?;
        let (leaf_node_id, idx) = path.pop().ok_or_else(|| {
            TreeError::BackendAny("Insertion path is empty, tree might be corrupted".to_string())
        })?;
        let mut leaf_node = self.storage.read_node_view(leaf_node_id)?.ok_or_else(|| {
            TreeError::NodeNotFound(format!("Leaf node with ID {} not found", leaf_node_id))
        })?;

        let NodeView::Leaf { .. } = &mut leaf_node else {
            return Err(TreeError::BackendAny(
                "Expected a leaf node for insertion".to_string(),
            ));
        };

        if found {
            leaf_node.replace_at(idx, &val_buf)?;
        } else {
            leaf_node.insert_at(idx, &key_buf, &val_buf)?;
        }

        track.record_staged_size(self.get_size() + 1); // Update staged size
        track.record_staged_height(self.get_height()); // Update staged height - could be increased later

        if leaf_node.keys_len() > self.max_keys {
            self.handle_leaf_split(path, leaf_node, track)
        } else {
            self.write_and_propagate_view(path, &leaf_node, track)
        }
    }

    // Handles the split of a leaf node when it exceeds the maximum number of keys.
    fn handle_leaf_split(
        &self,
        path: Vec<(NodeId, usize)>,
        leaf_node: NodeView,
        track: &mut impl TxnTracker,
    ) -> Result<NodeId, TreeError> {
        let SplitResult::SplitNodes {
            left_node,
            right_node,
            split_key,
        } = self.split_leaf_node_view(leaf_node)?;
        let right_id = self.write_node_view(&right_node, track)?;
        let left_id = self.write_node_view(&left_node, track)?;

        self.propagate_split(path, left_id, right_id, split_key, track)
    }

    // Splits a leaf node into two nodes and returns the new right node, the left node, and the
    // first key of the right node to be pushed up to the parent.
    fn split_leaf_node_view(
        &self,
        mut leaf_node: NodeView,
    ) -> Result<SplitResult<NodeView>, TreeError> {
        // Equally split the keys and values between the two nodes.
        if let NodeView::Leaf { .. } = &mut leaf_node {
            let mid = leaf_node.keys_len() / 2;
            let split_idx = mid; // Index to split the keys and values
            let right_node = leaf_node.split_off(split_idx)?;
            let split_key = right_node.first_key()?;

            Ok(SplitResult::SplitNodes {
                left_node: leaf_node,
                right_node,
                split_key,
            })
        } else {
            Err(TreeError::BackendAny(
                "Expected a leaf node for splitting".to_string(),
            ))
        }
    }

    fn split_internal_node_view(
        &self,
        mut internal_node: NodeView,
    ) -> Result<SplitResult<NodeView>, TreeError> {
        if let NodeView::Internal { .. } = &mut internal_node {
            let mid = internal_node.keys_len() / 2;
            let split_idx = mid + 1; // Index to split the keys and values
            let right_node = internal_node.split_off(split_idx)?;
            let split_key = internal_node.pop_key()?.ok_or_else(|| {
                TreeError::BackendAny("Internal node has no mid keys for split".to_string())
            })?;

            Ok(SplitResult::SplitNodes {
                left_node: internal_node,
                right_node,
                split_key,
            })
        } else {
            Err(TreeError::BackendAny(
                "Expected an internal node for splitting".to_string(),
            ))
        }
    }

    // Writes a node and propagates the update to the parent nodes.
    fn write_and_propagate(
        &self,
        path: Vec<(u64, usize)>,
        node: &Node<K, V>,
        track: &mut impl TxnTracker,
    ) -> Result<NodeId, TreeError> {
        let new_node_id = self.write_node(node, track)?;
        if path.is_empty() {
            Ok(new_node_id)
        } else {
            let new_root = self.propagate_node_update(path, new_node_id, track)?;
            Ok(new_root)
        }
    }

    // Writes a node view and propagates the update to the parent nodes.
    fn write_and_propagate_view(
        &self,
        path: Vec<(u64, usize)>,
        node: &NodeView,
        track: &mut impl TxnTracker,
    ) -> Result<NodeId, TreeError> {
        let new_node_id = self.write_node_view(node, track)?;
        if path.is_empty() {
            Ok(new_node_id)
        } else {
            let new_root = self.propagate_node_view_update(path, new_node_id, track)?;
            Ok(new_root)
        }
    }

    // Propagates an update to the parent nodes after a node has been updated or split.
    fn propagate_node_update(
        &self,
        mut path: Vec<(NodeId, usize)>,
        mut updated_child_id: NodeId,
        track: &mut impl TxnTracker,
    ) -> Result<NodeId, TreeError> {
        while let Some((parent_id, insert_pos)) = path.pop() {
            let mut parent_node = self.read_node(parent_id)?.ok_or_else(|| {
                TreeError::NodeNotFound(format!("Parent node {} not found", parent_id).to_string())
            })?;
            let Node::Internal {
                ref mut children, ..
            } = parent_node
            else {
                return Err(TreeError::BackendAny(
                    "Expected internal node while updating parents".to_string(),
                ));
            };
            if insert_pos >= children.len() {
                return Err(TreeError::BackendAny(format!(
                    "Insert position {} out of bounds for children in node {}",
                    insert_pos, parent_id
                )));
            }
            // Reclaim the original child node and update the child pointer
            track.reclaim(children[insert_pos]);
            children[insert_pos] = updated_child_id;
            // Propagate up the path
            updated_child_id = self.write_node(&parent_node, track)?;
        }
        Ok(updated_child_id) // Return the new root ID
    }

    // Propagates an update to a node view
    fn propagate_node_view_update(
        &self,
        mut path: Vec<(NodeId, usize)>,
        mut updated_child_id: NodeId,
        track: &mut impl TxnTracker,
    ) -> Result<NodeId, TreeError> {
        while let Some((parent_id, insert_pos)) = path.pop() {
            let mut parent_node = self.storage.read_node_view(parent_id)?.ok_or_else(|| {
                TreeError::NodeNotFound(format!("Parent node {} not found", parent_id).to_string())
            })?;

            let NodeView::Internal { .. } = parent_node else {
                return Err(TreeError::BackendAny(
                    "Expected internal node while updating parents".to_string(),
                ));
            };
            if insert_pos > parent_node.keys_len() + 1 {
                return Err(TreeError::BackendAny(format!(
                    "Insert position {} out of bounds for children in node {}",
                    insert_pos, parent_id
                )));
            }
            // Reclaim the original child node and update the child pointer
            track.reclaim(parent_node.child_ptr_at(insert_pos)?.ok_or_else(|| {
                TreeError::BackendAny(format!(
                    "Child pointer at index {} in node {} is None",
                    insert_pos, parent_id
                ))
            })?);
            parent_node.replace_child_at(insert_pos, updated_child_id)?;
            // Propagate up the path
            updated_child_id = self.write_node_view(&parent_node, track)?;
        }
        Ok(updated_child_id) // Return the new root ID
    }

    // Insert into a parent node, the path is the collection of the nodes that are parent to the
    // leaf, try inserting in a lifo manner.
    fn propagate_split(
        &self,
        mut path: Vec<(NodeId, usize)>,
        mut left: NodeId,
        mut right: NodeId,
        //mut key: K,
        mut key: Vec<u8>,
        track: &mut impl TxnTracker,
    ) -> Result<NodeId, TreeError> {
        while let Some((parent_id, insert_pos)) = path.pop() {
            let Some(mut node) = self.storage.read_node_view(parent_id)? else {
                return Err(TreeError::NodeNotFound(
                    format!("Parent node {} not found", parent_id).to_string(),
                ));
            };
            let NodeView::Internal { .. } = &mut node else {
                return Err(TreeError::BackendAny(
                    "Expected internal node in propagation path".to_string(),
                ));
            };
            // reclaim the  child at insert_pos
            // insert encoded key + right child at insert_pos
            // replace left child at insert_pos
            let left_child_prev = node.child_ptr_at(insert_pos)?.ok_or_else(|| {
                TreeError::BackendAny(format!(
                    "Child pointer at index {} in node {} is None",
                    insert_pos, parent_id
                ))
            })?;
            //let mut encode_buf = vec![0u8; S::KC::encoded_len(&key)];
            //S::KC::encode_key(&key, &mut encode_buf)?;
            track.reclaim(left_child_prev);
            node.insert_separator_at(insert_pos, &key, right)?;
            node.replace_child_at(insert_pos, left)?;

            // if there is no further overflow we can just propagate the update and return
            if node.keys_len() <= self.max_keys {
                return self.write_and_propagate_view(path, &node, track);
            }
            // Handle internal node split
            let SplitResult::SplitNodes {
                left_node,
                right_node,
                split_key,
            } = self.split_internal_node_view(node)?;

            left = self.write_node_view(&left_node, track)?;
            right = self.write_node_view(&right_node, track)?;
            key = split_key;
        }

        let mut new_root = NodeView::new_internal(0u8);
        new_root.write_leftmost_child(left)?;
        new_root.insert_separator_at(0, &key, right)?;
        
        let new_root_id = self.write_node_view(&new_root, track)?;
        track.record_staged_height(self.get_height() + 1); // Update staged height

        Ok(new_root_id)
    }
    // Search for a key in the B+ tree, acquiring an epoch guard to ensure consistency.
    pub fn search(&self, key: &K) -> Result<Option<V>, TreeError> {
        let root_id = self.get_root_id();
        self.search_inner(key, root_id)
    }

    // Search for a key and return the value if exists, without decoding the nodes for efficiency.
    pub fn search_inner(&self, key: &K, root_id: NodeId) -> Result<Option<V>, TreeError> {
        let _guard = self.epoch_mgr.pin();
        let mut current_id = root_id;

        let mut encode_buf = vec![0u8; S::KC::encoded_len(key)];
        S::KC::encode_key(key, encode_buf.as_mut())?;
        // Find insertion point
        loop {
            match self
                .storage
                .read_node_view(current_id)
                .map_err(|e| TreeError::BackendAny(e.to_string()))?
            {
                Some(node) => match &node {
                    NodeView::Leaf { .. } => {
                        match node.lower_bound_cmp(encode_buf.as_ref(), S::KC::compare_encoded) {
                            Ok(i) => {
                                let Some(vb) = node.value_bytes_at(i)? else {
                                    return Ok(None);
                                };
                                let value = S::VC::decode_value(vb)?;
                                return Ok(Some(value));
                            }
                            Err(_i) => {
                                return Ok(None); // Key not found
                            }
                        };
                    }
                    NodeView::Internal { .. } => {
                        // Find the insertion point in the internal node
                        let i = match node
                            .lower_bound_cmp(encode_buf.as_ref(), S::KC::compare_encoded)
                        {
                            Ok(i) => i + 1,
                            Err(i) => i,
                        };
                        let child = node.child_ptr_at(i)?; // Move to the child node
                        if let Some(child_id) = child {
                            current_id = child_id; // Continue iteration
                        } else {
                            TreeError::BackendAny(format!(
                                "Internal node cannot retrieve child at index {}",
                                i
                            ));
                        }
                    }
                },
                None => {
                    // Node not found, this should not happen as we are traversing the path
                    return Err(TreeError::BackendAny(
                        "Node not found while getting insertion path".to_string(),
                    ));
                }
            }
        }
    }

    // Searches for a range of keys in the B+ tree and returns an iterator over the key-value
    // pairs.
    pub fn search_range<'a>(
        &'a self,
        root_id: NodeId,
        start: &K,
        end: &K,
    ) -> Result<Option<BPlusTreeIter<'a, K, V, S>>, TreeError> {
        if start > end {
            return Ok(None); // Invalid range
        }
        let _guard = self.epoch_mgr.pin();
        Ok(Some(BPlusTreeIter::new(
            &self.storage,
            root_id,
            self.epoch_mgr.clone(),
            start,
            end,
        )))
    }

    // Deletes a key from the B+ tree.
    pub fn delete(
        &mut self,
        key: &K,
        root_id: NodeId,
        track: &mut impl TxnTracker,
    ) -> Result<NodeId, TreeError> {
        let res = self.delete_inner(key, root_id, track)?;
        match res {
            DeleteResult::NotFound => Err(TreeError::BackendAny(
                "Key not found for deletion".to_string(),
            )), // Key not found, return current root
            DeleteResult::Deleted(new_root_id) => Ok(new_root_id),
        }
    }

    // Delete the key value pair and handle underflow of leaf nodes
    pub fn delete_inner(
        &self,
        key: &K,
        root_id: NodeId,
        track: &mut impl TxnTracker,
    ) -> Result<DeleteResult<NodeId>, TreeError> {
        let _guard = self.epoch_mgr.pin();

        let (mut path, found) = self.get_insertion_path(key, root_id)?;
        let (leaf_node_id, idx) = path.pop().ok_or_else(|| {
            TreeError::BackendAny("Insertion path is empty, tree might be corrupted".to_string())
        })?;

        if !found {
            return Ok(DeleteResult::NotFound); // Key not found
        }

        let mut leaf_node = self.storage.read_node_view(leaf_node_id)?.ok_or_else(|| {
            TreeError::NodeNotFound(format!("Leaf node with ID {} not found", leaf_node_id))
        })?;

        let NodeView::Leaf { .. } = &mut leaf_node else {
            return Err(TreeError::BackendAny(
                "Expected a leaf node for deletion".to_string(),
            ));
        };

        leaf_node.delete_at(idx)?;

        track.record_staged_size(self.get_size().saturating_sub(1));
        track.record_staged_height(self.get_height()); // Update staged height, it may be decreased later

        // no underflow if the node has enough keys or it is the root node
        if leaf_node.entry_count() >= self.min_leaf_keys || path.is_empty() {
            let new_root_id = self.write_and_propagate_view(path, &leaf_node, track)?;
            return Ok(DeleteResult::Deleted(new_root_id));
        }

        // materialize the leaf node for easy underflow handling
        let node = Node::from_node_view::<S::KC, S::VC>(leaf_node)?;
        let new_root_id = self.handle_underflow(path, node, track)?;
        Ok(DeleteResult::Deleted(new_root_id))
    }

    // Handles underflow of a node after deletion, trying to borrow from siblings or merge with them.
    fn handle_underflow(
        &self,
        mut path: Vec<(NodeId, usize)>,
        mut node: Node<K, V>,
        track: &mut impl TxnTracker,
    ) -> Result<NodeId, TreeError> {
        while let Some((parent_id, idx)) = path.pop() {
            let Some(mut parent_node) = self.read_node(parent_id)? else {
                return Err(TreeError::NodeNotFound(
                    format!("Parent node {} not found", parent_id).to_string(),
                ));
            };
            {
                let Node::Internal {
                    keys: ref mut parent_keys,
                    ref mut children,
                } = parent_node
                else {
                    return Err(TreeError::BackendAny(
                        "Expected internal node as parent".to_string(),
                    ));
                };
                // If the root has only one child, replace the root with that child
                if path.is_empty() && children.len() == 1 {
                    return Ok(children[0]);
                }
                // Try borrowing from left or right sibling, on success just propagate the update,
                // no change in number of keys in the parent node
                if idx > 0
                    && self.try_borrow_from_left(&mut node, parent_keys, children, idx, track)?
                {
                    return self.write_and_propagate(path, &parent_node, track);
                }
                if (idx < children.len() - 1)
                    && self.try_borrow_from_right(&mut node, parent_keys, children, idx, track)?
                {
                    return self.write_and_propagate(path, &parent_node, track);
                }
                // Try to merge with left or right sibling
                let mut merged = None;
                if let Some(id) =
                    self.try_merge_with_left(&mut node, parent_keys, children, idx, track)?
                {
                    merged = Some(id);
                } else if let Some(id) =
                    self.try_merge_with_right(&mut node, parent_keys, children, idx, track)?
                {
                    merged = Some(id);
                }
                // We should have merged with a sibling or borrowed from it otherwise invalid state
                if merged.is_some() {
                    // the parent node underflowed after merge
                    if parent_keys.len() < self.min_internal_keys {
                        // handle root node underflow
                        if path.is_empty() {
                            if children.len() == 1 {
                                track.reclaim(parent_id);
                                track.record_staged_height(self.get_height().saturating_sub(1));
                                return Ok(children[0]); // If the root has only one child, replace the root with that child
                            } else {
                                return self.write_and_propagate(path, &parent_node, track);
                            }
                        }
                        // Continue handling underflow
                        node = parent_node;
                        continue;
                    } else {
                        // Parent node didn't underflow, just write the updated parent node
                        return self.write_and_propagate(path, &parent_node, track);
                    }
                }
            }
        }
        Err(TreeError::BackendAny(
            "Node underflow couldn't be resolved".to_string(),
        ))
    }

    // Tries to borrow a key from the left sibling of the current node.
    fn try_borrow_from_left(
        &self,
        node: &mut Node<K, V>,
        parent_keys: &mut [K],
        children: &mut [NodeId],
        idx: usize,
        track: &mut impl TxnTracker,
    ) -> Result<bool, TreeError> {
        if idx == 0 {
            return Ok(false);
        }
        let parent_key_idx = idx - 1; // The key in the parent node that separates the two children
        let left_child_idx = idx - 1; // The index of the left sibling in the children array
        let left_sibling_id = children[left_child_idx];
        let Some(mut left_sibling) = self.read_node(left_sibling_id)? else {
            return Err(TreeError::NodeNotFound(
                format!("Left sibling id: {} not found", left_sibling_id).to_string(),
            ));
        };
        match (&mut left_sibling, &mut *node) {
            (
                Node::Leaf {
                    keys: left_keys,
                    values: left_values,
                    ..
                },
                Node::Leaf {
                    keys: right_keys,
                    values: right_values,
                    ..
                },
            ) => {
                if left_keys.len() > self.min_leaf_keys {
                    let borrowed_key = left_keys.pop().ok_or_else(|| {
                        TreeError::BackendAny("Left sibling has no keys to borrow".to_string())
                    })?;
                    let borrowed_value = left_values.pop().ok_or_else(|| {
                        TreeError::BackendAny("Left sibling has no values to borrow".to_string())
                    })?;
                    right_keys.insert(0, borrowed_key.clone());
                    right_values.insert(0, borrowed_value);
                    // Update the separator key with the borrowed key - separator should alwasy be the first key of the right child
                    parent_keys[parent_key_idx] = borrowed_key;
                } else {
                    return Ok(false);
                }
            }
            (
                Node::Internal {
                    keys: left_keys,
                    children: left_children,
                },
                Node::Internal {
                    keys: right_keys,
                    children: right_children,
                },
            ) => {
                if left_keys.len() > self.min_internal_keys {
                    let borrowed_key = left_keys.pop().ok_or_else(|| {
                        TreeError::BackendAny("Left sibling has no keys to borrow".to_string())
                    })?;
                    let borrowed_child = left_children.pop().ok_or_else(|| {
                        TreeError::BackendAny("Left sibling has no children to borrow".to_string())
                    })?;
                    right_keys.insert(0, parent_keys[parent_key_idx].clone());
                    right_children.insert(0, borrowed_child);
                    // Update the parent key with the borrowed key
                    parent_keys[parent_key_idx] = borrowed_key;
                } else {
                    return Ok(false);
                }
            }
            _ => {
                return Err(TreeError::BackendAny(
                    "Expected matching node types for borrowing".to_string(),
                ));
            }
        };
        let new_node_id = self.write_node(node, track)?;
        let new_left_node_id = self.write_node(&left_sibling, track)?;

        track.reclaim(children[left_child_idx]);
        children[left_child_idx] = new_left_node_id;
        track.reclaim(children[idx]);
        children[idx] = new_node_id;

        Ok(true)
    }

    // Tries to borrow a key from the right sibling of the current node.
    fn try_borrow_from_right(
        &self,
        node: &mut Node<K, V>,
        parent_keys: &mut [K],
        children: &mut [NodeId],
        idx: usize,
        track: &mut impl TxnTracker,
    ) -> Result<bool, TreeError> {
        if idx >= children.len() {
            return Ok(false); // No right sibling to borrow from
        }
        let parent_key_idx = idx; // The key in the parent node that separates the two children
        let right_sibling_id = children[idx + 1];
        let Some(mut right_sibling) = self.read_node(right_sibling_id)? else {
            return Err(TreeError::NodeNotFound(
                "Right sibling not found".to_string(),
            ));
        };
        match (&mut *node, &mut right_sibling) {
            (
                Node::Leaf {
                    keys: left_keys,
                    values: left_values,
                    ..
                },
                Node::Leaf {
                    keys: right_keys,
                    values: right_values,
                    ..
                },
            ) => {
                if right_keys.len() > self.min_leaf_keys {
                    // Borrow from the right sibling
                    let borrowed_key = right_keys.remove(0);
                    let borrowed_value = right_values.remove(0);
                    let new_separator_key = right_keys[0].clone(); // The first key of the right
                    // sibling becomes the new separator key
                    left_keys.push(borrowed_key);
                    left_values.push(borrowed_value);
                    // Update the separator key with the first key  of the right sibling
                    parent_keys[parent_key_idx] = new_separator_key; // Update the parent key with the new key
                } else {
                    return Ok(false); // Not enough keys to borrow
                }
            }
            (
                Node::Internal {
                    keys: left_keys,
                    children: left_children,
                },
                Node::Internal {
                    keys: right_keys,
                    children: right_children,
                },
            ) => {
                if right_keys.len() > self.min_internal_keys {
                    // Steps for Internal node are diffent we need to swap the first key of the
                    // right sibling with the separator from parent
                    // 1. Move separator key from parent to the left node
                    left_keys.push(parent_keys[parent_key_idx].clone());
                    // 2. Update the parent key with the first key of the right sibling
                    parent_keys[parent_key_idx] = right_keys.remove(0);
                    // 3. Borrow a child from the right sibling
                    let borrowed_child = right_children.remove(0);
                    left_children.push(borrowed_child);
                } else {
                    return Ok(false); // Not enough keys to borrow
                }
            }
            _ => {
                return Err(TreeError::BackendAny(
                    "Expected matching node types for borrowing".to_string(),
                ));
            }
        }
        // Write the updated nodes back to storage
        let new_node_id = self.write_node(node, track)?;
        let new_right_node_id = self.write_node(&right_sibling, track)?;

        track.reclaim(children[idx]);
        children[idx] = new_node_id;
        track.reclaim(children[idx + 1]);
        children[idx + 1] = new_right_node_id;

        Ok(true)
    }

    // Tries to merge the current node with its left sibling if possible.
    fn try_merge_with_left(
        &self,
        node: &mut Node<K, V>,
        parent_keys: &mut Vec<K>,
        children: &mut Vec<NodeId>,
        idx: usize,
        track: &mut impl TxnTracker,
    ) -> Result<Option<NodeId>, TreeError> {
        if idx == 0 {
            return Ok(None);
        }
        let left_sibling_id = children[idx - 1];
        let parent_key_idx = idx - 1; // The key in the parent node that separates the two children
        let Some(mut left_sibling) = self.read_node(left_sibling_id)? else {
            return Err(TreeError::NodeNotFound(
                "Left sibling not found".to_string(),
            ));
        };
        match (&mut left_sibling, &mut *node) {
            (
                Node::Leaf {
                    keys: left_keys, ..
                },
                Node::Leaf {
                    keys: right_keys, ..
                },
            ) => {
                // Check if the total number of keys exceeds the maximum allowed
                if left_keys.len() + right_keys.len() > self.max_keys {
                    return Ok(None); // Cannot merge, total keys exceed max keys
                }
                // Merge the current node with the left sibling
                let merged_node = self.merge_nodes(&mut left_sibling, node)?;
                let merged_node_id = self.write_node(&merged_node, track)?;
                // Update the parent node
                track.reclaim(children[idx]); // Reclaim the left sibling node
                children.remove(idx);
                track.reclaim(children[idx - 1]); // Reclaim the left sibling node
                children[idx - 1] = merged_node_id; // Update the left sibling with the merged node ID
                // Update the parent keys
                if !parent_keys.is_empty() {
                    parent_keys.remove(parent_key_idx); // Update the parent key with the first key of the merged node
                }
                Ok(Some(merged_node_id))
            }
            (
                Node::Internal {
                    keys: left_keys, ..
                },
                Node::Internal {
                    keys: right_keys, ..
                },
            ) => {
                // Check if the total number of keys exceeds the maximum allowed
                if left_keys.len() + right_keys.len() > self.max_keys {
                    return Ok(None); // Cannot merge, total keys exceed max keys
                }
                let seperator_key = parent_keys.remove(parent_key_idx); // The key that separates
                // The two children has to be removed and added to the left sibling
                left_keys.push(seperator_key); // Add the separator key to the left sibling
                // Merge the left sibling with the current node
                let merged_node = self.merge_nodes(&mut left_sibling, node)?;
                let merged_node_id = self.write_node(&merged_node, track)?;
                // Update the parent node
                track.reclaim(children[idx]); // Reclaim the left sibling node
                children.remove(idx);
                track.reclaim(children[idx - 1]); // Reclaim the left sibling node
                children[idx - 1] = merged_node_id; // Update the left sibling with the merged node
                Ok(Some(merged_node_id))
            }
            _ => Err(TreeError::BackendAny(
                "Expected matching node types for merging".to_string(),
            )),
        }
    }

    // Tries to merge the current node with its right sibling if possible.
    fn try_merge_with_right(
        &self,
        node: &mut Node<K, V>,
        parent_keys: &mut Vec<K>,
        children: &mut Vec<NodeId>,
        idx: usize,
        track: &mut impl TxnTracker,
    ) -> Result<Option<NodeId>, TreeError> {
        // Check if there is a right sibling to merge with
        let right_idx = idx + 1;
        if right_idx >= children.len() {
            return Ok(None);
        }

        let right_sibling_id = children[right_idx];
        let parent_key_idx = idx; // The key in the parent node that separates the two children
        let Some(mut right_sibling) = self.read_node(right_sibling_id)? else {
            return Err(TreeError::NodeNotFound(
                "Left sibling not found".to_string(),
            ));
        };
        match (&mut *node, &mut right_sibling) {
            (
                Node::Leaf {
                    keys: left_keys, ..
                },
                Node::Leaf {
                    keys: right_keys, ..
                },
            ) => {
                // Check if the total number of keys exceeds the maximum allowed
                if left_keys.len() + right_keys.len() > self.max_keys {
                    return Ok(None); // Cannot merge, total keys exceed max keys
                }
                // Merge the current node with the left sibling
                let merged_node = self.merge_nodes(node, &mut right_sibling)?;
                let merged_node_id = self.write_node(&merged_node, track)?;
                // Update the parent node
                track.reclaim(children[right_idx]); // Reclaim the right sibling node
                children.remove(right_idx); // Remove the current node
                track.reclaim(children[idx]); // Reclaim the left sibling node
                children[idx] = merged_node_id; // Update the left sibling with the merged node
                // Update the parent keys
                if !parent_keys.is_empty() {
                    parent_keys.remove(parent_key_idx); // Update the parent key with the first key of the merged node
                }
                Ok(Some(merged_node_id))
            }
            (
                Node::Internal {
                    keys: left_keys, ..
                },
                Node::Internal {
                    keys: right_keys, ..
                },
            ) => {
                // Check if the total number of keys exceeds the maximum allowed
                if left_keys.len() + right_keys.len() > self.max_keys {
                    return Ok(None); // Cannot merge, total keys exceed max keys
                }
                let seperator_key = parent_keys.remove(parent_key_idx); // The key that separates
                // the two children has to be removed and added to the left sibling
                left_keys.push(seperator_key); // Add the separator key to the left sibling
                // Merge the current node with the right sibling
                let merged_node = self.merge_nodes(node, &mut right_sibling)?;
                let merged_node_id = self.write_node(&merged_node, track)?;
                // Update the parent node
                track.reclaim(children[right_idx]); // Reclaim the right sibling node
                children.remove(right_idx); // Remove the right sibling
                track.reclaim(children[idx]); // Reclaim the left sibling node
                children[idx] = merged_node_id; // Update the left sibling with the merged node
                Ok(Some(merged_node_id))
            }
            _ => Err(TreeError::BackendAny(
                "Expected matching node types for merging".to_string(),
            )),
        }
    }

    // Merges two nodes (left and right) into a single node, returning the new node ID.
    pub fn merge_nodes(
        &self,
        left_node: &mut Node<K, V>,
        right_node: &mut Node<K, V>,
    ) -> Result<Node<K, V>, TreeError> {
        match (&mut *left_node, right_node) {
            // Match on a new mutable reference to the left node
            (
                Node::Leaf {
                    keys: left_keys,
                    values: left_values,
                },
                Node::Leaf {
                    keys: right_keys,
                    values: right_values,
                },
            ) => {
                // Check if the total number of keys exceeds the maximum allowed
                if left_keys.len() + right_keys.len() > self.max_keys {
                    return Err(TreeError::BackendAny(
                        "Cannot merge leaf nodes, total keys exceed max keys".to_string(),
                    ));
                }
                // Merge the two leaf nodes
                left_keys.append(right_keys); // Move keys from right to left
                left_values.append(right_values); // Move values from right to left
                Ok(Node::Leaf {
                    keys: std::mem::take(left_keys),
                    values: std::mem::take(left_values),
                })
            }
            (
                Node::Internal {
                    keys: left_keys,
                    children: left_children,
                },
                Node::Internal {
                    keys: right_keys,
                    children: right_children,
                },
            ) => {
                if left_keys.len() + right_keys.len() > self.max_keys {
                    return Err(TreeError::BackendAny(
                        "Cannot merge internal nodes, total keys exceed max keys".to_string(),
                    ));
                }
                left_keys.append(right_keys);
                left_children.append(right_children);
                // Update the parent node with the new node ID
                Ok(Node::Internal {
                    keys: std::mem::take(left_keys),
                    children: std::mem::take(left_children),
                })
            }
            _ => Err(TreeError::BackendAny(
                "Expected leaf nodes for merging".to_string(),
            )),
        }
    }

    // Reclaims a node by adding it to the reclamation candidates for the current epoch.
    pub fn reclaim_node(&self, node_id: NodeId) -> Result<(), TreeError> {
        let epoch = self.epoch_mgr.get_current_thread_epoch().ok_or_else(|| {
            TreeError::BackendAny("Failed to get epoch for current thread".to_string())
        })?;
        self.epoch_mgr.add_reclaim_candidate(epoch, node_id);
        Ok(())
    }

    // Returns the current valid snapshot of the tree's state.
    pub fn get_snapshot(&self) -> MetadataSnapshot {
        let ptr = self.committed.load(Ordering::Acquire);
        let meta = unsafe { &*ptr };
        MetadataSnapshot {
            root_id: meta.root_node_id,
            height: meta.height,
            size: meta.size,
            txn_id: meta.txn_id,
            order: meta.order,
        }
    }

    // Version of commit to be used for single threaded commits, use for testing and debugging
    pub fn commit(
        &self,
        new_root_id: NodeId,
        _height: usize,
        _size: usize,
    ) -> Result<(), TreeError> {
        // Now commit the new root
        // 1. Write new metadata (to double-buffered slot)
        let new_txn_id = self.txn_id.fetch_add(1, Ordering::SeqCst) + 1;
        let target_slot = new_txn_id % 2;

        // Commit the metadata for the new root
        self.storage.commit_metadata(
            target_slot as u8,
            self.txn_id.load(Ordering::Relaxed),
            new_root_id,
            self.get_height(),
            self.get_order(),
            self.get_size(),
        )?;

        let current_ptr = self.committed.load(Ordering::Acquire);
        let current = unsafe { &mut *current_ptr };
        // Flush the storage to ensure all changes are written
        self.storage.flush()?;

        current.root_node_id = new_root_id;
        current.txn_id = new_txn_id;

        self.commit_count.fetch_add(1, Ordering::Relaxed);

        let _new_epoch = self.epoch_mgr.advance(); // Bump the epoch for transaction management

        let safe_epoch = self.epoch_mgr.oldest_active();
        let reclaimed = self.epoch_mgr.reclaim(safe_epoch);
        for pid in reclaimed {
            self.storage.free_node(pid)?;
        }

        if (self.commit_count.load(Ordering::Relaxed) as u64) % COMMIT_COUNT == 0 {
            self.epoch_mgr.advance(); // Pin new epoch for reclamation
        }
        Ok(())
    }

    // Attempts to commit the transaction with the given metadata.
    pub fn try_commit(
        &self,
        base_version: &BaseVersion,
        new_meta: StagedMetadata,
    ) -> Result<(), CommitError> {
        #[cfg(any(test, feature = "testing"))]
        {
            let injected: Result<(), CommitError> = Ok(());
            fail::fail_point!("tree::commit::try_commit_failure", |_| {
                injected = Err(CommitError::Injected);
                println!("Injected failure in try_commit");
            });
            injected?; // returns early if the failpoint was enabled
        }

        let expected = base_version.committed_ptr;
        // load current committed metadata
        let current_ptr = self.committed.load(Ordering::Acquire);
        let current = unsafe { &*current_ptr };
        // Assign new txn_id here - the current txn_id in the committed metadata is the actual
        // sequence number.
        let new_txn_id = current.txn_id + 1;
        let metadata = Metadata {
            root_node_id: new_meta.root_id,
            height: new_meta.height,
            size: new_meta.size,
            txn_id: new_txn_id,
            checksum: 0,
            order: current.order,
        };

        // 1. Prepare new metadata box and pointer
        let boxed = Box::new(metadata);
        let new_ptr = Box::into_raw(boxed);
        // 2. Atomically commit metadata pointer
        let result = self.committed.compare_exchange(
            expected as *mut Metadata,
            new_ptr,
            Ordering::SeqCst,
            Ordering::Relaxed,
        );

        match result {
            // ✅ cas has succeeded, proceed with commit to storage
            Ok(old_ptr) => {
                // 3. Commit metadata to the double-buffered slot
                let slot = new_txn_id % 2;

                let res = self
                    .storage
                    .commit_metadata_with_object(slot as u8, &metadata);
                if let Err(e) = res {
                    // ❌ commit failed, restore old metadata
                    unsafe {
                        drop(Box::from_raw(new_ptr));
                    } // Discard new metadata
                    self.committed.store(current_ptr, Ordering::Release); // Restore old metadata
                    return Err(CommitError::Io(e));
                }
                self.storage.flush()?; // flush to disk
                // 4. Advance the epoch manager
                self.epoch_mgr.advance();
                let safe_epoch = self.epoch_mgr.oldest_active();
                let reclaimed = self.epoch_mgr.reclaim(safe_epoch);
                for nid in reclaimed {
                    self.storage.free_node(nid)?;
                }
                if (self.commit_count.load(Ordering::Relaxed) as u64) % COMMIT_COUNT == 0 {
                    self.epoch_mgr.advance(); // Pin new epoch for reclamation
                }

                unsafe {
                    // Reclaim old metadata
                    drop(Box::from_raw(old_ptr));
                }

                Ok(())
            }
            Err(_) => {
                // ❌ Lost race, discard new metadata
                unsafe {
                    drop(Box::from_raw(new_ptr));
                }
                Err(CommitError::RebaseRequired)
            }
        }
    }

    // Safe accessor for current committed metadata
    pub fn metadata(&self) -> &Metadata {
        unsafe { &*self.committed.load(Ordering::Acquire) }
    }

    // Safe accessor for current committed metadata
    pub fn metadata_ptr(&self) -> *const Metadata {
        unsafe { &*self.committed.load(Ordering::Acquire) }
    }

    // Returns a snapshot of the current metadata, useful for read-only operations.
    pub fn snapshot(&self) -> Metadata {
        let ptr = self.committed.load(Ordering::Acquire);
        let meta = unsafe { &*ptr };

        Metadata {
            root_node_id: meta.root_node_id,
            height: meta.height,
            size: meta.size,
            checksum: meta.checksum,
            txn_id: meta.txn_id,
            order: meta.order,
        }
    }

    // Traverses the B+ tree and returns all key-value pairs in a vector.
    pub fn traverse(&self) -> Result<Vec<(K, V)>, TreeError> {
        let mut result = Vec::new();
        let root_id = self.get_root_id();
        if root_id == 0 {
            return Ok(result); // Empty tree
        }
        let _guard = self.epoch_mgr.pin();
        self.traverse_inner(root_id, &mut result)?;
        Ok(result)
    }

    // Recursive implementation of traversal.
    pub fn traverse_inner(
        &self,
        node_id: NodeId,
        result: &mut Vec<(K, V)>,
    ) -> Result<(), TreeError> {
        match self.read_node(node_id)? {
            Some(Node::Internal { keys, children }) => {
                for (i, child_id) in children.iter().enumerate() {
                    if i <= keys.len() {
                        self.traverse_inner(*child_id, result)?;
                    }
                }
            }
            Some(Node::Leaf { keys, values, .. }) => {
                for (key, value) in keys.iter().zip(values.iter()) {
                    result.push((key.clone(), value.clone()));
                }
            }
            None => return Err(TreeError::NodeNotFound("Node not found".to_string())),
        }
        Ok(())
    }

    // Helpers to get metadata information
    pub fn get_txn_id(&self) -> u64 {
        self.txn_id.load(Ordering::Relaxed)
    }

    pub fn get_root_id(&self) -> NodeId {
        let ptr = self.committed.load(Ordering::Acquire);
        let meta = unsafe { &*ptr };
        meta.root_node_id
    }

    pub fn get_height(&self) -> usize {
        let ptr = self.committed.load(Ordering::Acquire);
        let meta = unsafe { &*ptr };
        meta.height
    }

    pub fn get_size(&self) -> usize {
        let ptr = self.committed.load(Ordering::Acquire);
        let meta = unsafe { &*ptr };
        meta.size
    }

    pub fn get_order(&self) -> usize {
        let ptr = self.committed.load(Ordering::Acquire);
        let meta = unsafe { &*ptr };
        meta.order
    }

    #[cfg(any(test, feature = "testing"))]
    pub fn test_force_publish(&self, metadata: &Metadata) {
        let boxed = Box::new(*metadata);
        let new_ptr = Box::into_raw(boxed);
        self.committed.store(new_ptr, Ordering::SeqCst);
    }

    #[cfg(any(test, feature = "testing"))]
    pub fn get_epoch_mgr(&self) -> Arc<EpochManager> {
        Arc::clone(&self.epoch_mgr)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::common::{test_storage::TestStorage, test_tree};

    // test commit happy path
    #[test]
    fn commit_happy_path() {
        let storage = TestStorage::new(); // Reset the test storage state
        let test_harness = test_tree::<u64, u64, TestStorage>(storage, 128);
        let tree = test_harness.tree;

        let base = BaseVersion {
            committed_ptr: tree.metadata_ptr(),
        };
        let staged = StagedMetadata {
            root_id: 42,
            height: 3,
            size: 10,
        };

        let res = tree.try_commit(&base, staged);

        assert!(res.is_ok(), "Commit should succeed");

        let m = tree.metadata();
        assert_eq!(m.root_node_id, 42);
        assert_eq!(m.txn_id, 2); // txn_id is initialized at 1 so txn_id should be 2
    }

    // commit should succeed if the base version is the current committed version
    #[test]
    fn commit_happy_path_2() {
        let storage = TestStorage::new(); // Reset the test storage state
        let h = test_tree::<u64, u64, TestStorage>(storage, 128);
        let base = BaseVersion {
            committed_ptr: h.tree.metadata_ptr(),
        };

        let staged = StagedMetadata {
            root_id: 42,
            height: 3,
            size: 10,
        };
        h.tree.try_commit(&base, staged).expect("commit ok");

        let m = h.tree.metadata();
        assert_eq!(m.root_node_id, 42);
        assert_eq!(m.height, 3);
        assert_eq!(m.size, 10);
        assert_eq!(m.txn_id, 2);

        let (slot, txn, rid, hgt, _ord, sz) = h.storage.last_commit().unwrap();
        assert_eq!(slot, (txn % 2) as u8);
        assert_eq!(txn, 2);
        assert_eq!(rid, 42);
        assert_eq!(hgt, 3);
        assert_eq!(sz, 10);

        assert_eq!(h.storage.flush_count(), 1);
    }

    // commit should fail if the base version is not the current committed version
    #[test]
    fn commit_aborts_on_conflict() {
        let storage = TestStorage::new(); // Reset the test storage state
        storage.inject_commit_failure(true);
        let test_harness = test_tree::<u64, u64, TestStorage>(storage, 128);
        let tree = test_harness.tree;
        let _mocks = test_harness.storage;
        let base = BaseVersion {
            committed_ptr: tree.metadata_ptr(),
        };
        let staged = StagedMetadata {
            root_id: 42,
            height: 3,
            size: 10,
        };

        let result = tree.try_commit(&base, staged);
        println!("Commit result: {:?}", result);
        assert!(result.is_err());
    }

    // txn_id should be strictly monotonic, each commit should increment the txn_id
    #[test]
    fn txn_id_is_strictly_monotonic() {
        let storage = TestStorage::new(); // Reset the test storage state
        let h = test_tree::<u64, Vec<u8>, TestStorage>(storage, 128);
        let mut prev = h.tree.metadata().txn_id;

        for i in 0..5 {
            loop {
                let base = BaseVersion {
                    committed_ptr: h.tree.metadata_ptr(),
                };
                if h.tree
                    .try_commit(
                        &base,
                        StagedMetadata {
                            root_id: 100 + i,
                            height: 3,
                            size: i as usize,
                        },
                    )
                    .is_ok()
                {
                    break;
                }
            }
            let now = h.tree.metadata().txn_id;
            assert_eq!(now, prev + 1);
            prev = now;
        }
    }

    // The slot should follow the transaction ID modulo 2
    #[test]
    fn slot_follows_txn_mod2() {
        let storage = TestStorage::new(); // Reset the test storage state
        let h = test_tree::<u64, Vec<u8>, TestStorage>(storage, 128);
        for i in 0..6 {
            let base = BaseVersion {
                committed_ptr: h.tree.metadata_ptr(),
            };
            h.tree
                .try_commit(
                    &base,
                    StagedMetadata {
                        root_id: 200 + i,
                        height: 3,
                        size: i as usize,
                    },
                )
                .unwrap();
            let (slot, txn, ..) = h.storage.last_commit().unwrap();
            assert_eq!(slot, (txn % 2) as u8);
        }
    }

    // commit should abort if there is a storage failure and restore the tree state (metadata ptr)
    #[test]
    fn commit_metadata_write_failure_is_abort() {
        let storage = TestStorage::new(); // Reset the test storage state
        storage.inject_commit_failure(true);
        let test_harness = test_tree::<u64, u64, TestStorage>(storage, 128);
        let tree = test_harness.tree;
        let _mocks = test_harness.storage;
        let base = BaseVersion {
            committed_ptr: tree.metadata_ptr(),
        };
        let staged = StagedMetadata {
            root_id: 42,
            height: 3,
            size: 10,
        };

        let md_before = tree.metadata(); // Ensure metadata is still valid
        let result = tree.try_commit(&base, staged);
        assert!(result.is_err(), "Commit should fail due to storage failure");
        let md_after = tree.metadata(); // Ensure metadata is still valid
        assert_eq!(
            md_before.root_node_id, md_after.root_node_id,
            "Root node ID should not change on commit failure"
        );
    }

    // commit should publish data regardless of a storage flush failure
    #[test]
    fn flush_failure_after_cas_keeps_published_state() {
        let storage = TestStorage::new(); // Reset the test storage state
        storage.inject_flush_failure(true);
        let test_harness = test_tree::<u64, u64, TestStorage>(storage, 128);
        let tree = test_harness.tree;
        let _mocks = test_harness.storage;
        let base = BaseVersion {
            committed_ptr: tree.metadata_ptr(),
        };
        let staged = StagedMetadata {
            root_id: 42,
            height: 3,
            size: 10,
        };

        let md_before = tree.metadata(); // Ensure metadata is still valid
        let result = tree.try_commit(&base, staged);
        assert!(result.is_err(), "Commit should fail due to flush failure");
        let md_after = tree.metadata(); // Ensure metadata is still valid
        assert_ne!(
            md_before.root_node_id, md_after.root_node_id,
            "Metadata should be published regardless of flush failure"
        );
    }
}
