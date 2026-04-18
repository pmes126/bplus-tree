use std::fmt::Debug;
use std::sync::Arc;
use std::sync::atomic::{AtomicPtr, AtomicU64, AtomicUsize, Ordering};

use crate::api::{KeyEncodingId, TreeId};
use crate::bplustree::NodeView;
use crate::bplustree::node_view::NodeViewError;
use crate::database::catalog::TreeMeta;
use crate::database::metadata::Metadata;
use crate::keyfmt::KeyFormat;
use crate::page::{LeafPage, PageError};
use crate::storage::epoch::COMMIT_COUNT;
use crate::storage::epoch::EpochManager;
use crate::storage::metadata_manager::{MetadataError, MetadataManager};
use crate::storage::{HasEpoch, NodeStorage, PageStorage, StorageError};

use std::result::Result;
use thiserror::Error;
use zerocopy::AsBytes;

/// Numeric identifier for a B+ tree node (page ID).
pub type NodeId = u64;
/// A node on the traversal path, represented as (node ID, index in parent).
pub type PathNode = (NodeId, usize);

/// Result of inserting into a B+ tree node.
pub enum InsertResult<N> {
    /// Node was updated in-place.
    Updated(N),
    /// Node was inserted.
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

/// Result of splitting a B+ tree node.
pub enum SplitResult<N> {
    /// The two halves and the key to push up into the parent.
    SplitNodes {
        /// Left half, including the newly inserted key.
        left_node: N,
        /// Right half.
        right_node: N,
        /// First key of the right node, to be pushed into the parent.
        split_key: Vec<u8>,
    },
}

/// Errors that can occur during B+ tree operations.
#[derive(Debug, Error)]
pub enum TreeError {
    /// The caller supplied an invalid key or parameter.
    #[error("bad input: {0}")]
    BadInput(String),
    /// An internal tree invariant was violated (indicates a bug or corruption).
    #[error("tree invariant violated: {0}")]
    Invariant(&'static str),
    /// A required node was not found in storage.
    #[error("node not found: {0}")]
    NodeNotFound(String),
    /// A storage-layer error during node I/O.
    #[error(transparent)]
    Storage(#[from] StorageError),
    /// A metadata page read or write failed.
    #[error(transparent)]
    Metadata(#[from] MetadataError),
    /// A node-view operation failed (wrong kind or underlying page error).
    #[error(transparent)]
    NodeView(#[from] NodeViewError),
    /// The key-value pair is too large to fit in a single page.
    #[error(
        "entry too large: key ({key_len} bytes) + value ({val_len} bytes) exceeds max {max_len} bytes"
    )]
    EntryTooLarge {
        key_len: usize,
        val_len: usize,
        max_len: usize,
    },
}

/// Per-entry overhead in a leaf page: u16 key-length prefix + LeafSlot (val_off + val_len).
const LEAF_PER_ENTRY_OVERHEAD: usize = std::mem::size_of::<u16>() + crate::page::leaf::SLOT_SIZE;

/// Maximum combined key + value size (in bytes) that can be inserted into a
/// leaf page.
///
/// Each leaf entry occupies:
///   - key block: `size_of::<u16>()` (length prefix) + key bytes
///   - slot directory: `SLOT_SIZE` bytes (u16 offset + u16 length)
///   - value arena: value bytes
///
/// We require that at least **two** entries fit in a single page so that every
/// split produces two valid pages.  Therefore:
///
///   `max_entry_payload = LEAF_BUFFER_SIZE / 2 - LEAF_PER_ENTRY_OVERHEAD`
pub const MAX_ENTRY_PAYLOAD: usize = crate::page::leaf::BUFFER_SIZE / 2 - LEAF_PER_ENTRY_OVERHEAD;

/// Errors that can occur when committing a transaction.
#[derive(Debug, Error)]
pub enum CommitError {
    /// The metadata write failed; the commit was rolled back.
    #[error("metadata write failed: {0}")]
    Metadata(#[from] MetadataError),
    /// A storage error occurred during flush or node reclamation.
    #[error("storage error during commit: {0}")]
    Storage(#[from] StorageError),
    /// The base version is stale; the caller should rebase and retry.
    #[error("stale base — rebase and retry")]
    RebaseRequired,
    /// Commit was aborted by a test failpoint.
    #[error("commit aborted (test injection)")]
    Injected,
}

/// Collects per-transaction accounting: nodes added, reclaimed, and staged tree shape.
pub trait TxnTracker {
    /// Records a node ID to be reclaimed after the transaction commits.
    fn reclaim(&mut self, node_id: NodeId);
    /// Records a newly written node ID as part of this transaction.
    fn add_new(&mut self, node_id: NodeId);
    /// Records the staged tree height for this transaction.
    fn record_staged_height(&mut self, height: u64);
    /// Records the staged entry count for this transaction.
    fn record_staged_size(&mut self, size: u64);
}

/// A point-in-time snapshot of committed tree metadata.
#[derive(Debug, Clone)]
pub struct MetadataSnapshot {
    /// Root node page ID at snapshot time.
    pub root_id: NodeId,
    /// Tree height at snapshot time.
    pub height: u64,
    /// Approximate number of entries at snapshot time.
    pub size: u64,
    /// Transaction ID of the snapshot.
    pub txn_id: u64,
    /// B+ tree order (maximum number of children per internal node).
    pub order: u64,
}

/// Staged (not yet committed) metadata produced by a write transaction.
#[derive(Debug, Clone)]
pub struct StagedMetadata {
    /// New root node page ID.
    pub root_id: NodeId,
    /// New tree height.
    pub height: u64,
    /// New approximate entry count.
    pub size: u64,
}

/// Holds a raw pointer to the committed metadata used as the compare-exchange base.
pub struct BaseVersion {
    /// Raw pointer to the committed [`Metadata`] at transaction start.
    pub committed_ptr: *const Metadata,
}

/// Runtime configuration for a B+ tree instance.
pub struct TreeConfig {
    /// Page size in bytes (4096 by default).
    pub page_size: usize,
    /// Key-format identifier: 0 = Raw, 1 = Raw+Restarts, 2 = Prefix+Restarts.
    pub key_format_id: u8,
    /// Restart interval used by prefix formats; ignored by raw formats.
    pub restart_interval: u16,
    /// Target page fill in bytes; splits and merges are triggered by byte budget, not key count.
    pub target_fill_bytes: usize,
}

/// B+ tree with generic storage backends for nodes and pages.
///
/// # Memory ordering
///
/// The concurrency model uses a single atomic pointer (`committed`) as the
/// publish point for new tree roots.  Ordering contracts:
///
/// - **`committed`** (`AtomicPtr<Metadata>`): writers publish via `compare_exchange`
///   with `SeqCst` success ordering, ensuring a total order across all CAS
///   operations.  Readers load with `Acquire` so that all COW page writes
///   performed before the pointer was published are visible.  Rollback stores
///   (on metadata-write failure) use `Release` to pair with readers' `Acquire`.
///
/// - **`commit_count`** (`AtomicUsize`): heuristic trigger for epoch advancement.
///   Uses `Relaxed` — no other memory location depends on its value.
///
/// - **`txn_id`** (`AtomicU64`): only used by the single-threaded debug `commit()`
///   path.  The production path (`try_commit`) reads the transaction ID from the
///   `Metadata` struct behind the atomic pointer instead.
pub struct BPlusTree<'s, S, P>
where
    S: NodeStorage + Send + Sync + 'static,
    P: PageStorage + Send + Sync + 'static,
{
    id: TreeId,
    storage: &'s S,
    page_storage: &'s P,
    epoch_mgr: Arc<EpochManager>,
    #[allow(dead_code)]
    key_encoding: KeyEncodingId,
    #[allow(dead_code)]
    encoding_version: u16,
    key_format: KeyFormat,
    meta_a: u64,
    meta_b: u64,
    max_keys: usize,
    min_internal_keys: usize,
    min_leaf_keys: usize,
    commit_count: AtomicUsize,
    txn_id: AtomicU64,
    committed: AtomicPtr<Metadata>,
}

impl<S, P> Drop for BPlusTree<'_, S, P>
where
    S: NodeStorage + Send + Sync + 'static,
    P: PageStorage + Send + Sync + 'static,
{
    fn drop(&mut self) {
        let ptr = self.committed.load(Ordering::Acquire);
        if !ptr.is_null() {
            unsafe {
                drop(Box::from_raw(ptr));
            }
        }
    }
}

/// Default [`TxnTracker`] implementation that accumulates node accounting in memory.
#[derive(Default)]
pub struct TransactionTracker {
    /// Node IDs to be freed after the transaction commits.
    pub reclaimed: Vec<NodeId>,
    /// Node IDs written during this transaction.
    pub added: Vec<NodeId>,
    /// Staged tree height, if updated.
    pub staged_height: Option<u64>,
    /// Staged entry count, if updated.
    pub staged_size: Option<u64>,
}

impl TransactionTracker {
    /// Creates a new, empty [`TransactionTracker`].
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
    fn record_staged_height(&mut self, height: u64) {
        self.staged_height = Some(height);
    }
    fn record_staged_size(&mut self, size: u64) {
        self.staged_size = Some(size);
    }
}

/// Result of a write operation (insert or delete).
#[derive(Debug)]
pub struct WriteResult {
    /// New root node ID after the write.
    pub new_root_id: NodeId,
    /// Node IDs that can be freed after commit.
    pub reclaimed_nodes: Vec<NodeId>,
    /// Node IDs written speculatively during this operation.
    pub staged_nodes: Vec<NodeId>,
    /// New tree height after the write.
    pub new_height: u64,
    /// New approximate entry count after the write.
    pub new_size: u64,
}

/// A cheaply clonable, shared handle to a [`BPlusTree`].
pub struct SharedBPlusTree<'s, S, P>
where
    S: NodeStorage + Send + Sync + 'static,
    P: PageStorage + Send + Sync + 'static,
{
    inner: Arc<BPlusTree<'s, S, P>>,
}

impl<'s, S, P> Clone for SharedBPlusTree<'s, S, P>
where
    S: NodeStorage + HasEpoch + Send + Sync + 'static,
    P: PageStorage + Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<'s, S, P> SharedBPlusTree<'s, S, P>
where
    S: NodeStorage + HasEpoch + Send + Sync + 'static,
    P: PageStorage + Send + Sync + 'static,
{
    /// Creates a new shared handle by wrapping `tree` in an [`Arc`].
    pub fn new(tree: BPlusTree<'s, S, P>) -> Self {
        Self {
            inner: Arc::new(tree),
        }
    }

    /// Creates a shared handle from an existing [`Arc`].
    pub fn from_arc(tree: Arc<BPlusTree<'s, S, P>>) -> Self {
        Self { inner: tree }
    }

    /// Inserts a key-value pair starting from the given root node ID.
    pub fn put_with_root<K: AsRef<[u8]>, V: AsRef<[u8]>>(
        &self,
        key: K,
        value: V,
        root_id: NodeId,
    ) -> Result<WriteResult, TreeError> {
        let mut collector = TransactionTracker::new();
        let new_root_id = self.inner.put_inner(key, value, root_id, &mut collector)?;
        let write_res = WriteResult {
            new_root_id,
            reclaimed_nodes: std::mem::take(&mut collector.reclaimed),
            staged_nodes: std::mem::take(&mut collector.added),
            new_height: collector.staged_height.unwrap_or(self.inner.get_height()),
            new_size: collector.staged_size.unwrap_or(self.inner.get_size()),
        };
        Ok(write_res)
    }

    /// Inserts a key-value pair using the current committed root.
    pub fn put<K: AsRef<[u8]>, V: AsRef<[u8]>>(
        &self,
        key: K,
        value: V,
    ) -> Result<WriteResult, TreeError> {
        let root_id = self.inner.get_root_id();
        self.put_with_root(key, value, root_id)
    }

    /// Deletes a key starting from the given root node ID.
    pub fn delete_with_root<K: AsRef<[u8]>>(
        &self,
        key: &K,
        root_id: NodeId,
    ) -> Result<WriteResult, TreeError> {
        let mut collector = TransactionTracker::new();
        let delete_res = self.inner.delete_inner(key, root_id, &mut collector)?;
        let DeleteResult::Deleted(new_root_id) = delete_res else {
            return Err(TreeError::NodeNotFound(
                "key not found for deletion".to_string(),
            ));
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

    /// Searches for a key using the current committed root.
    pub fn search<K: AsRef<[u8]>>(&self, key: K) -> Result<Option<Vec<u8>>, TreeError> {
        self.inner.get(key)
    }

    /// Searches for a key starting from the given root node ID.
    pub fn search_with_root<K: AsRef<[u8]>>(
        &self,
        key: &K,
        root_id: NodeId,
    ) -> Result<Option<Vec<u8>>, TreeError> {
        self.inner.get_inner(key, root_id)
    }

    /// Returns the current committed root node ID.
    pub fn get_root_id(&self) -> NodeId {
        self.inner.get_root_id()
    }

    /// Returns the current committed tree height.
    pub fn get_height(&self) -> u64 {
        self.inner.get_height()
    }

    /// Returns the current approximate entry count.
    pub fn get_size(&self) -> u64 {
        self.inner.get_size()
    }

    /// Returns the current committed transaction ID.
    pub fn get_txn_id(&self) -> NodeId {
        self.inner.txn_id.load(Ordering::SeqCst)
    }

    /// Flushes pending node storage writes to disk.
    pub fn flush(&mut self) -> Result<(), TreeError> {
        self.inner.storage.flush()?;
        Ok(())
    }

    /// Returns a snapshot of the current committed metadata.
    pub fn get_snapshot(&self) -> MetadataSnapshot {
        self.inner.get_snapshot()
    }

    /// Attempts to commit the staged metadata via a compare-and-exchange.
    pub fn try_commit(
        &self,
        version: &BaseVersion,
        new_metadata: StagedMetadata,
    ) -> Result<(), CommitError> {
        self.inner.try_commit(version, new_metadata)
    }

    /// Returns a raw pointer to the committed metadata.
    pub fn get_metadata_ptr(&self) -> *const Metadata {
        self.inner.committed.load(Ordering::SeqCst)
    }

    /// Returns a reference to the committed metadata.
    pub fn get_metadata(&self) -> &Metadata {
        unsafe { &*self.inner.committed.load(Ordering::Acquire) }
    }

    /// Returns a clone of the inner [`Arc`].
    pub fn arc(&self) -> Arc<BPlusTree<S, P>> {
        Arc::clone(&self.inner)
    }

    #[allow(clippy::should_implement_trait)]
    /// Returns a new shared handle pointing to the same tree.
    pub fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }

    /// Alias for [`put_with_root`] using the conventional `insert` vocabulary.
    pub fn insert_with_root<K: AsRef<[u8]>, V: AsRef<[u8]>>(
        &self,
        key: K,
        value: V,
        root_id: NodeId,
    ) -> Result<WriteResult, TreeError> {
        self.put_with_root(key, value, root_id)
    }

    /// Alias for [`put`] using the conventional `insert` vocabulary.
    pub fn insert<K: AsRef<[u8]>, V: AsRef<[u8]>>(
        &self,
        key: K,
        value: V,
    ) -> Result<WriteResult, TreeError> {
        self.put(key, value)
    }

    /// Returns a forward range iterator scanning `[start, end)`.
    ///
    /// Pass `None` for `end` to scan from `start` to the end of the tree.
    pub fn search_range(
        &self,
        start: &[u8],
        end: Option<&[u8]>,
    ) -> Result<super::iterator::BPlusTreeIter<'_, S>, TreeError> {
        let root_id = self.inner.get_root_id();
        super::iterator::BPlusTreeIter::new(
            self.inner.storage,
            root_id,
            &self.inner.epoch_mgr,
            start,
            end,
        )
    }

    /// Returns a clone of the shared epoch manager.
    pub fn epoch_mgr(&self) -> Arc<EpochManager> {
        Arc::clone(&self.inner.epoch_mgr)
    }

    /// Reclaims a node by registering it for deferred freeing.
    pub fn reclaim_node(&self, node_id: NodeId) -> Result<(), TreeError> {
        self.inner.reclaim_node(node_id)
    }
}

impl<'s, S, P> BPlusTree<'s, S, P>
where
    S: NodeStorage + Send + Sync + 'static,
    P: PageStorage + Send + Sync + 'static,
{
    /// Creates a new [`BPlusTree`] from its storage backends and catalog metadata.
    ///
    /// Writes an initial empty leaf node and sets up the committed metadata pointer.
    pub fn new(
        storage: &'s S,
        page_storage: &'s P,
        meta: &TreeMeta,
        epoch_mgr: Arc<EpochManager>,
    ) -> Result<BPlusTree<'s, S, P>, TreeError> {
        let keyfmt = meta.keyfmt_id;
        let root_node = NodeView::Leaf {
            page: LeafPage::new(keyfmt),
        };
        let init_id = storage.write_node_view(&root_node)?;
        storage.write_node_view_at_offset(&root_node, meta.root_id)?;

        let init_txn_id = 1;

        let md1 = Metadata {
            root_node_id: init_id,
            txn_id: init_txn_id,
            height: 1,
            checksum: 0,
            size: 0,
            order: meta.order,
            id: meta.id,
        };

        let md_ptr = Box::new(md1);

        Ok(Self {
            id: meta.id,
            storage,
            page_storage,
            epoch_mgr,
            key_encoding: meta.key_encoding,
            key_format: meta.keyfmt_id,
            encoding_version: meta.format_version,
            meta_a: meta.meta_a,
            meta_b: meta.meta_b,
            max_keys: meta.order as usize - 1,
            min_internal_keys: (meta.order as usize).div_ceil(2) - 1,
            min_leaf_keys: (meta.order as usize - 1).div_ceil(2),
            commit_count: AtomicUsize::new(0),
            txn_id: AtomicU64::new(init_txn_id),
            committed: AtomicPtr::new(Box::into_raw(md_ptr)),
        })
    }

    /// Opens a [`BPlusTree`] over existing on-disk data without writing any nodes or metadata.
    ///
    /// Unlike [`new`], `open` does not initialise a root node, making it safe to call when
    /// reopening a previously persisted tree after a crash or restart.
    #[allow(clippy::too_many_arguments)]
    pub fn open(
        storage: &'s S,
        page_storage: &'s P,
        meta: Metadata,
        meta_a: u64,
        meta_b: u64,
        key_format: KeyFormat,
        key_encoding: KeyEncodingId,
        epoch_mgr: Arc<EpochManager>,
    ) -> BPlusTree<'s, S, P> {
        let id = meta.id;
        let txn_id = meta.txn_id;
        let order = meta.order;
        let md_ptr = Box::into_raw(Box::new(meta));
        Self {
            id,
            storage,
            page_storage,
            epoch_mgr,
            key_encoding,
            key_format,
            encoding_version: 1,
            meta_a,
            meta_b,
            max_keys: order as usize - 1,
            min_internal_keys: (order as usize).div_ceil(2) - 1,
            min_leaf_keys: (order as usize - 1).div_ceil(2),
            commit_count: AtomicUsize::new(0),
            txn_id: AtomicU64::new(txn_id),
            committed: AtomicPtr::new(md_ptr),
        }
    }

    /// Reads a node view from storage by its ID.
    #[allow(dead_code)]
    fn read_node_view(&self, id: NodeId) -> Result<Option<NodeView>, TreeError> {
        Ok(self.storage.read_node_view(id)?)
    }

    /// Writes a node view to storage and records it with the transaction tracker.
    fn write_node_view(
        &self,
        node: &NodeView,
        tracker: &mut impl TxnTracker,
    ) -> Result<u64, TreeError> {
        let new_id = self.storage.write_node_view(node)?;
        tracker.add_new(new_id);
        Ok(new_id)
    }

    /// Returns the path to the leaf where `key` belongs, without fully decoding nodes.
    pub fn get_insertion_path<K: AsRef<[u8]>>(
        &self,
        key: K,
        root_id: NodeId,
    ) -> Result<(Vec<PathNode>, bool), TreeError> {
        let mut path = vec![];
        let mut current_id = root_id;
        // Find insertion point
        loop {
            match self.storage.read_node_view(current_id)? {
                Some(node) => match &node {
                    NodeView::Leaf { .. } => {
                        let mut found = false;
                        let i = match node.lower_bound(key.as_ref()) {
                            Ok(i) => {
                                found = true;
                                i
                            }
                            Err(i) => i,
                        };
                        path.push((current_id, i));
                        return Ok((path, found));
                    }
                    NodeView::Internal { .. } => {
                        let i = match node.lower_bound(key.as_ref()) {
                            Ok(i) => i + 1,
                            Err(i) => i,
                        };
                        path.push((current_id, i));
                        let child_id = node.child_ptr_at(i)?;
                        current_id = child_id;
                    }
                },
                None => {
                    // Node not found, this should not happen as we are traversing the path
                    return Err(TreeError::Invariant("node not found while traversing path"));
                }
            }
        }
    }

    /// Inserts a key-value pair using the current committed root.
    pub fn put<K: AsRef<[u8]>, V: AsRef<[u8]>>(
        &self,
        key: K,
        value: V,
        track: &mut impl TxnTracker,
    ) -> Result<NodeId, TreeError> {
        let root_id = self.get_root_id();
        self.put_inner(key, value, root_id, track)
    }

    /// Inserts a key-value pair starting from `root_id`.
    pub fn put_inner<K: AsRef<[u8]>, V: AsRef<[u8]>>(
        &self,
        key: K,
        value: V,
        root_id: NodeId,
        track: &mut impl TxnTracker,
    ) -> Result<NodeId, TreeError> {
        let key_bytes = key.as_ref();
        let val_bytes = value.as_ref();

        // Reject entries that can never fit — no amount of splitting helps.
        let payload = key_bytes.len() + val_bytes.len();
        if payload > MAX_ENTRY_PAYLOAD {
            return Err(TreeError::EntryTooLarge {
                key_len: key_bytes.len(),
                val_len: val_bytes.len(),
                max_len: MAX_ENTRY_PAYLOAD,
            });
        }

        let _guard = self.epoch_mgr.pin();
        let mut current_root = root_id;

        loop {
            let (mut path, found) = self.get_insertion_path(key_bytes, current_root)?;
            let (leaf_node_id, idx) = path
                .pop()
                .ok_or(TreeError::Invariant("insertion path is empty"))?;
            let mut leaf_node = self.storage.read_node_view(leaf_node_id)?.ok_or_else(|| {
                TreeError::NodeNotFound(format!("Leaf node with ID {} not found", leaf_node_id))
            })?;

            let NodeView::Leaf { .. } = &mut leaf_node else {
                return Err(TreeError::Invariant(
                    "expected leaf node at insertion point",
                ));
            };

            if found {
                match leaf_node.replace_at(idx, val_bytes) {
                    Ok(()) => {}
                    Err(NodeViewError::Page(PageError::PageFull {})) => {
                        // Value arena full (new value larger than old).
                        // Split the leaf, then retry — the loop will find the
                        // key again in the correct half and overwrite there.
                        current_root = self.handle_leaf_split(path, leaf_node, track)?;
                        continue;
                    }
                    Err(e) => return Err(e.into()),
                }
            } else {
                match leaf_node.insert_at(idx, key_bytes, val_bytes) {
                    Ok(()) => {}
                    Err(NodeViewError::Page(PageError::PageFull {})) => {
                        // Page is physically full before reaching max_keys.
                        // Split the leaf (without the new entry), propagate the
                        // split upward, then loop to retry the insert from the
                        // new root.  This handles recursive physical splits
                        // naturally — each iteration makes progress by reducing
                        // entries per leaf.
                        current_root = self.handle_leaf_split(path, leaf_node, track)?;
                        continue;
                    }
                    Err(e) => return Err(e.into()),
                }
            }

            track.record_staged_size(self.get_size() + 1);
            track.record_staged_height(self.get_height());

            if leaf_node.keys_len() > self.max_keys {
                return self.handle_leaf_split(path, leaf_node, track);
            } else {
                return self.write_and_propagate(path, &leaf_node, track);
            }
        }
    }

    /// Splits an overfull leaf node and propagates the new separator key upward.
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
        } = self.split_leaf_node(leaf_node)?;
        let right_id = self.write_node_view(&right_node, track)?;
        let left_id = self.write_node_view(&left_node, track)?;

        self.propagate_split(path, left_id, right_id, split_key, track)
    }

    /// Splits a leaf node in half and returns both halves plus the first key of the right half.
    fn split_leaf_node(&self, mut leaf_node: NodeView) -> Result<SplitResult<NodeView>, TreeError> {
        if let NodeView::Leaf { .. } = &mut leaf_node {
            let mid = leaf_node.keys_len() / 2;
            let split_idx = mid;
            let right_node = leaf_node.split_off(split_idx)?;
            let split_key = right_node.first_key()?;

            Ok(SplitResult::SplitNodes {
                left_node: leaf_node,
                right_node,
                split_key,
            })
        } else {
            Err(TreeError::Invariant("expected leaf node for splitting"))
        }
    }

    /// Splits an internal node in half and returns both halves plus the middle key to push up.
    fn split_internal_node(
        &self,
        mut internal_node: NodeView,
    ) -> Result<SplitResult<NodeView>, TreeError> {
        if let NodeView::Internal { .. } = &mut internal_node {
            let mid = internal_node.keys_len() / 2;
            let split_idx = mid + 1;
            let right_node = internal_node.split_off(split_idx)?;
            let split_key = internal_node.pop_key()?.ok_or(TreeError::Invariant(
                "internal node has no mid key for split",
            ))?;

            Ok(SplitResult::SplitNodes {
                left_node: internal_node,
                right_node,
                split_key,
            })
        } else {
            Err(TreeError::Invariant("expected internal node for splitting"))
        }
    }

    /// Writes a node and propagates the new ID up the parent path.
    fn write_and_propagate(
        &self,
        path: Vec<(u64, usize)>,
        node: &NodeView,
        track: &mut impl TxnTracker,
    ) -> Result<NodeId, TreeError> {
        let new_node_id = self.write_node_view(node, track)?;
        if path.is_empty() {
            Ok(new_node_id)
        } else {
            let new_root = self.propagate_node_update(path, new_node_id, track)?;
            Ok(new_root)
        }
    }

    /// Propagates a node update up the parent path, rewriting each ancestor.
    fn propagate_node_update(
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
                return Err(TreeError::Invariant(
                    "expected internal node while updating parents",
                ));
            };
            if insert_pos > parent_node.keys_len() + 1 {
                return Err(TreeError::Invariant(
                    "insert position out of bounds for parent node",
                ));
            }
            // Reclaim the original child and replace its pointer.
            track.reclaim(parent_node.child_ptr_at(insert_pos)?);
            parent_node.replace_child_at(insert_pos, updated_child_id)?;
            updated_child_id = self.write_node_view(&parent_node, track)?;
        }
        Ok(updated_child_id)
    }

    /// Propagates a node split up the parent path, creating a new root if needed.
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
                return Err(TreeError::Invariant(
                    "expected internal node in propagation path",
                ));
            };
            let left_child_prev = node.child_ptr_at(insert_pos)?;
            track.reclaim(left_child_prev);

            // Replace the old child with the new left child (no size change).
            node.replace_child_at(insert_pos, left)?;

            match node.insert_separator_at(insert_pos, &key, right) {
                Ok(()) => {
                    // No further overflow: write the updated parent and return.
                    if node.keys_len() <= self.max_keys {
                        return self.write_and_propagate(path, &node, track);
                    }
                    // Parent itself overflowed (logical); split it and continue up.
                    let SplitResult::SplitNodes {
                        left_node,
                        right_node,
                        split_key,
                    } = self.split_internal_node(node)?;

                    left = self.write_node_view(&left_node, track)?;
                    right = self.write_node_view(&right_node, track)?;
                    key = split_key;
                }
                Err(NodeViewError::Page(PageError::PageFull {})) => {
                    // Internal page is physically full before reaching max_keys.
                    // The old child at insert_pos has already been replaced with `left`.
                    // Split the node first, then insert separator+right into the correct half.
                    let SplitResult::SplitNodes {
                        mut left_node,
                        mut right_node,
                        split_key,
                    } = self.split_internal_node(node)?;

                    if key.as_slice() < split_key.as_slice() {
                        // Find insertion position in the left half.
                        let idx = match left_node.lower_bound(&key) {
                            Ok(i) => i + 1,
                            Err(i) => i,
                        };
                        left_node.insert_separator_at(idx, &key, right)?;
                    } else {
                        // Find insertion position in the right half.
                        let idx = match right_node.lower_bound(&key) {
                            Ok(i) => i + 1,
                            Err(i) => i,
                        };
                        right_node.insert_separator_at(idx, &key, right)?;
                    }

                    left = self.write_node_view(&left_node, track)?;
                    right = self.write_node_view(&right_node, track)?;
                    key = split_key;
                }
                Err(e) => return Err(e.into()),
            }
        }

        let mut new_root = NodeView::new_internal(self.key_format);
        new_root.write_leftmost_child(left)?;
        new_root.insert_separator_at(0, &key, right)?;

        let new_root_id = self.write_node_view(&new_root, track)?;
        track.record_staged_height(self.get_height() + 1);

        Ok(new_root_id)
    }

    /// Searches for a key and returns its value, or `None` if not found.
    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> Result<Option<Vec<u8>>, TreeError> {
        let root_id = self.get_root_id();
        self.get_inner(key, root_id)
    }

    /// Searches for a key starting from `root_id` and returns its value, or `None` if not found.
    pub fn get_inner<K: AsRef<[u8]>>(
        &self,
        key: K,
        root_id: NodeId,
    ) -> Result<Option<Vec<u8>>, TreeError> {
        let _guard = self.epoch_mgr.pin();
        let mut current_id = root_id;

        loop {
            match self.storage.read_node_view(current_id)? {
                Some(node) => match &node {
                    NodeView::Leaf { .. } => {
                        match node.lower_bound(key.as_ref()) {
                            Ok(i) => {
                                let vb = node.value_bytes_at(i)?;
                                return Ok(Some(vb.to_vec()));
                            }
                            Err(_i) => {
                                return Ok(None);
                            }
                        };
                    }
                    NodeView::Internal { .. } => {
                        let i = match node.lower_bound(key.as_ref()) {
                            Ok(i) => i + 1,
                            Err(i) => i,
                        };
                        current_id = node.child_ptr_at(i)?;
                    }
                },
                None => {
                    return Err(TreeError::Invariant("node not found during search"));
                }
            }
        }
    }

    /*
        // Searches for a range of keys in the B+ tree and returns an iterator over the key-value
        // pairs.
        //<K: AsRef<[u8]>, V: AsRef<[u8]>>
        pub fn search_range<'a, K: AsRef<[u8]>>(
            &'a self,
            root_id: NodeId,
            start: K,
            end: K,
        ) -> Result<Option<BPlusTreeIter<'a, S>>, TreeError> {
            let _guard = self.storage.epoch_mgr().pin();
            Ok(Some(BPlusTreeIter::new(
                &self.storage,
                root_id,
                self.storage.epoch_mgr().clone(),
                start,
                end,
            )))
        }
    */

    /// Deletes a key from the tree, returning an error if the key is not found.
    pub fn delete<K: AsRef<[u8]>>(
        &mut self,
        key: K,
        root_id: NodeId,
        track: &mut impl TxnTracker,
    ) -> Result<NodeId, TreeError> {
        let res = self.delete_inner(key, root_id, track)?;
        match res {
            DeleteResult::NotFound => Err(TreeError::NodeNotFound("key not found".to_string())),
            DeleteResult::Deleted(new_root_id) => Ok(new_root_id),
        }
    }

    /// Deletes a key starting from `root_id` and handles any resulting leaf underflow.
    pub fn delete_inner<K: AsRef<[u8]>>(
        &self,
        key: K,
        root_id: NodeId,
        track: &mut impl TxnTracker,
    ) -> Result<DeleteResult<NodeId>, TreeError> {
        let _guard = self.epoch_mgr.pin();

        let (mut path, found) = self.get_insertion_path(key, root_id)?;
        let (leaf_node_id, idx) = path
            .pop()
            .ok_or(TreeError::Invariant("insertion path is empty"))?;

        if !found {
            return Ok(DeleteResult::NotFound);
        }

        let mut leaf_node = self.storage.read_node_view(leaf_node_id)?.ok_or_else(|| {
            TreeError::NodeNotFound(format!("Leaf node with ID {} not found", leaf_node_id))
        })?;

        let NodeView::Leaf { .. } = &mut leaf_node else {
            return Err(TreeError::Invariant("expected leaf node at deletion point"));
        };

        leaf_node.delete_at(idx)?;

        track.record_staged_size(self.get_size().saturating_sub(1));
        // Height may decrease later if the root collapses.
        track.record_staged_height(self.get_height());

        // No underflow if the node has enough keys or it is the root.
        if leaf_node.entry_count() >= self.min_leaf_keys || path.is_empty() {
            let new_root_id = self.write_and_propagate(path, &leaf_node, track)?;
            return Ok(DeleteResult::Deleted(new_root_id));
        }

        let new_root_id = self.handle_underflow(path, leaf_node, track)?;
        Ok(DeleteResult::Deleted(new_root_id))
    }

    /// Resolves underflow for leaf and internal nodes by borrowing or merging with siblings.
    fn handle_underflow(
        &self,
        mut path: Vec<(NodeId, usize)>,
        mut node: NodeView,
        track: &mut impl TxnTracker,
    ) -> Result<NodeId, TreeError> {
        while let Some((parent_id, idx)) = path.pop() {
            let Some(mut parent_node) = self.storage.read_node_view(parent_id)? else {
                return Err(TreeError::NodeNotFound(
                    format!("Parent node {} not found", parent_id).to_string(),
                ));
            };
            {
                let NodeView::Internal { .. } = &mut parent_node else {
                    return Err(TreeError::Invariant("expected internal node as parent"));
                };
                // If the root has only one child left, collapse it.
                if path.is_empty() && parent_node.children_len()? == 1 {
                    parent_node.child_ptr_at(0)?;
                }
                // Try borrowing from a sibling first; on success the parent key count is unchanged.
                if idx > 0 && self.try_borrow_from_left(&mut node, &mut parent_node, idx, track)? {
                    return self.write_and_propagate(path, &parent_node, track);
                }
                if (idx < parent_node.keys_len())
                    && self.try_borrow_from_right(&mut node, &mut parent_node, idx, track)?
                {
                    return self.write_and_propagate(path, &parent_node, track);
                }
                // Borrowing failed; try merging with a sibling.
                let mut merged = None;
                if let Some(id) =
                    self.try_merge_with_left(&mut node, &mut parent_node, idx, track)?
                {
                    merged = Some(id);
                } else if let Some(id) =
                    self.try_merge_with_right(&mut node, &mut parent_node, idx, track)?
                {
                    merged = Some(id);
                }
                if merged.is_some() {
                    // Parent underflowed after merge.
                    if parent_node.keys_len() < self.min_internal_keys {
                        // Handle root underflow: collapse the root if it has a single child.
                        if path.is_empty() {
                            if parent_node.children_len()? == 1 {
                                track.reclaim(parent_id);
                                track.record_staged_height(self.get_height().saturating_sub(1));
                                return Ok(parent_node.child_ptr_at(0)?);
                            } else {
                                return self.write_and_propagate(path, &parent_node, track);
                            }
                        }
                        // Continue resolving underflow up the tree.
                        node = parent_node;
                        continue;
                    } else {
                        // Parent is still balanced; write and return.
                        return self.write_and_propagate(path, &parent_node, track);
                    }
                }
                // Neither borrow nor merge succeeded.  With variable-size
                // entries the combined physical size of two underfull nodes
                // can exceed a page even when the key count is within bounds.
                // Accept the underfull node as-is rather than violating an
                // invariant — the tree remains correct, just slightly unbalanced.
                let new_node_id = self.write_node_view(&node, track)?;
                let current_child_id = parent_node.child_ptr_at(idx)?;
                track.reclaim(current_child_id);
                parent_node.replace_child_at(idx, new_node_id)?;
                return self.write_and_propagate(path, &parent_node, track);
            }
        }
        Err(TreeError::Invariant("node underflow could not be resolved"))
    }

    /// Attempts to borrow a key (and child pointer for internal nodes) from the left sibling.
    fn try_borrow_from_left(
        &self,
        node: &mut NodeView,
        parent_node: &mut NodeView,
        idx: usize,
        track: &mut impl TxnTracker,
    ) -> Result<bool, TreeError> {
        if idx == 0 {
            return Ok(false);
        }
        // The separator key in the parent sits one position to the left of the current child.
        let parent_key_idx = idx - 1;
        let left_child_idx = idx - 1;
        let left_sibling_id = parent_node.child_ptr_at(left_child_idx)?;
        let Some(mut left_sibling) = self.storage.read_node_view(left_sibling_id)? else {
            return Err(TreeError::NodeNotFound(
                format!("Left sibling id: {} not found", left_sibling_id).to_string(),
            ));
        };
        match (&mut left_sibling, &mut *node) {
            (NodeView::Leaf { .. }, NodeView::Leaf { .. }) => {
                if left_sibling.keys_len() > self.min_leaf_keys {
                    let borrowed_key = left_sibling.key_bytes_at(left_sibling.keys_len() - 1)?;
                    let borrowed_value =
                        left_sibling.value_bytes_at(left_sibling.keys_len() - 1)?;
                    match node.insert_at(0, borrowed_key, borrowed_value) {
                        Ok(()) => {}
                        Err(NodeViewError::Page(PageError::PageFull {})) => return Ok(false),
                        Err(e) => return Err(e.into()),
                    }

                    // The separator must always equal the first key of the right child.
                    parent_node.replace_key_at(parent_key_idx, borrowed_key)?;
                    left_sibling.delete_at(left_sibling.keys_len() - 1)?;
                } else {
                    return Ok(false);
                }
            }
            (NodeView::Internal { .. }, NodeView::Internal { .. }) => {
                if left_sibling.keys_len() > self.min_internal_keys {
                    let borrowed_key = left_sibling.key_bytes_at(left_sibling.keys_len() - 1)?;
                    let borrowed_child =
                        left_sibling.child_ptr_at(left_sibling.children_len()? - 1)?;
                    let separator_key = parent_node.key_bytes_at(parent_key_idx)?;
                    match node.push_front(separator_key, borrowed_child) {
                        Ok(()) => {}
                        Err(NodeViewError::Page(PageError::PageFull {})) => return Ok(false),
                        Err(e) => return Err(e.into()),
                    }

                    // Update the parent key with the borrowed key
                    parent_node.replace_key_at(parent_key_idx, borrowed_key)?;
                    left_sibling.delete_at(left_sibling.keys_len() - 1)?;
                } else {
                    return Ok(false);
                }
            }
            _ => {
                return Err(TreeError::Invariant("mismatched node types for borrow"));
            }
        };
        let new_node_id = self.write_node_view(node, track)?;
        let new_left_node_id = self.write_node_view(&left_sibling, track)?;

        track.reclaim(left_sibling_id);
        parent_node.replace_child_at(left_child_idx, new_left_node_id)?;

        let current_child_id = parent_node.child_ptr_at(idx)?;
        track.reclaim(current_child_id);

        parent_node.replace_child_at(idx, new_node_id)?;

        Ok(true)
    }

    /// Attempts to borrow a key (and child pointer for internal nodes) from the right sibling.
    fn try_borrow_from_right(
        &self,
        node: &mut NodeView,
        parent_node: &mut NodeView,
        idx: usize,
        track: &mut impl TxnTracker,
    ) -> Result<bool, TreeError> {
        if idx >= parent_node.keys_len() {
            return Ok(false);
        }
        let parent_key_idx = idx;
        let right_sibling_id = parent_node.child_ptr_at(idx + 1)?;
        let Some(mut right_sibling) = self.storage.read_node_view(right_sibling_id)? else {
            return Err(TreeError::NodeNotFound(
                format!("Right sibling id: {} not found", right_sibling_id).to_string(),
            ));
        };
        match (&mut *node, &mut right_sibling) {
            (NodeView::Leaf { .. }, NodeView::Leaf { .. }) => {
                if right_sibling.keys_len() > self.min_leaf_keys {
                    let borrowed_key = right_sibling.key_bytes_at(0)?;
                    let borrowed_value = right_sibling.value_bytes_at(0)?;
                    match node.insert_at(
                        node.keys_len(),
                        borrowed_key.as_bytes(),
                        borrowed_value.as_bytes(),
                    ) {
                        Ok(()) => {}
                        Err(NodeViewError::Page(PageError::PageFull {})) => return Ok(false),
                        Err(e) => return Err(e.into()),
                    }
                    right_sibling.delete_at(0)?;

                    // Separator must equal the new first key of the right sibling.
                    let separator_key = right_sibling.key_bytes_at(0)?;
                    parent_node.replace_key_at(parent_key_idx, separator_key.as_bytes())?;
                } else {
                    return Ok(false);
                }
            }
            (NodeView::Internal { .. }, NodeView::Internal { .. }) => {
                if right_sibling.keys_len() > self.min_internal_keys {
                    // Rotate: pull separator from parent down into the left node,
                    // push the right sibling's first key up into the parent.
                    let separator_key = parent_node.key_at(parent_key_idx)?;
                    let right_first_key = right_sibling.delete_key_at(0)?;
                    parent_node.replace_key_at(parent_key_idx, right_first_key.as_bytes())?;
                    let borrowed_child = right_sibling.child_ptr_at(0)?;
                    node.insert_separator_at(node.keys_len(), &separator_key, borrowed_child)?;
                    right_sibling.delete_child_at(0)?;
                } else {
                    return Ok(false);
                }
            }
            _ => {
                return Err(TreeError::Invariant("mismatched node types for borrow"));
            }
        }
        let new_node_id = self.write_node_view(node, track)?;
        let new_right_node_id = self.write_node_view(&right_sibling, track)?;

        track.reclaim(right_sibling_id);
        parent_node.replace_child_at(idx + 1, new_right_node_id)?;
        let current_child_id = parent_node.child_ptr_at(idx)?;
        track.reclaim(current_child_id);
        parent_node.replace_child_at(idx, new_node_id)?;

        Ok(true)
    }

    /// Attempts to merge the current node into its left sibling.
    fn try_merge_with_left(
        &self,
        node: &mut NodeView,
        parent_node: &mut NodeView,
        idx: usize,
        track: &mut impl TxnTracker,
    ) -> Result<Option<NodeId>, TreeError> {
        if idx == 0 {
            return Ok(None);
        }
        let left_child_idx = idx - 1;
        let left_sibling_id = parent_node.child_ptr_at(left_child_idx)?;
        let parent_key_idx = idx - 1;
        let Some(mut left_sibling) = self.storage.read_node_view(left_sibling_id)? else {
            return Err(TreeError::NodeNotFound(
                format!("Left sibling id: {} not found", left_sibling_id).to_string(),
            ));
        };
        match (&mut left_sibling, &mut *node) {
            (NodeView::Leaf { .. }, NodeView::Leaf { .. }) => {
                if left_sibling.keys_len() + node.keys_len() > self.max_keys
                    || !left_sibling.can_merge_physically(node)
                {
                    return Ok(None);
                }
                let merged_node = self.merge_nodes_view(&mut left_sibling, node)?;
                let merged_node_id = self.write_node_view(merged_node, track)?;

                track.reclaim(parent_node.child_ptr_at(idx)?);
                parent_node.delete_child_at(idx)?;
                track.reclaim(left_sibling_id);
                parent_node.replace_child_at(left_child_idx, merged_node_id)?;
                if parent_node.keys_len() > 0 {
                    parent_node.delete_key_at(parent_key_idx)?;
                }
                Ok(Some(merged_node_id))
            }
            (NodeView::Internal { .. }, NodeView::Internal { .. }) => {
                if left_sibling.keys_len() + node.keys_len() > self.max_keys
                    || !left_sibling.can_merge_physically(node)
                {
                    return Ok(None);
                }
                // Pull the separator key from the parent down into the left sibling.
                let seperator_key = parent_node.delete_key_at(parent_key_idx)?;
                left_sibling.insert_separator_at(
                    node.keys_len() + 1,
                    seperator_key.as_bytes(),
                    node.child_ptr_at(0)?,
                )?;
                let merged_node = self.merge_nodes_view(&mut left_sibling, node)?;
                let merged_node_id = self.write_node_view(merged_node, track)?;
                track.reclaim(parent_node.child_ptr_at(idx)?);
                parent_node.delete_child_at(idx)?;
                track.reclaim(left_sibling_id);
                parent_node.replace_child_at(left_child_idx, merged_node_id)?;
                Ok(Some(merged_node_id))
            }
            _ => Err(TreeError::Invariant("mismatched node types for merge")),
        }
    }

    /// Attempts to merge the current node with its right sibling.
    fn try_merge_with_right(
        &self,
        node: &mut NodeView,
        parent_node: &mut NodeView,
        idx: usize,
        track: &mut impl TxnTracker,
    ) -> Result<Option<NodeId>, TreeError> {
        let right_idx = idx + 1;
        if right_idx >= parent_node.children_len()? {
            return Ok(None);
        }

        let right_sibling_id = parent_node.child_ptr_at(right_idx)?;
        let parent_key_idx = idx;
        let Some(mut right_sibling) = self.storage.read_node_view(right_sibling_id)? else {
            return Err(TreeError::NodeNotFound(
                format!("Right sibling id: {} not found", right_sibling_id).to_string(),
            ));
        };
        match (&mut *node, &mut right_sibling) {
            (NodeView::Leaf { .. }, NodeView::Leaf { .. }) => {
                if node.keys_len() + right_sibling.keys_len() > self.max_keys
                    || !node.can_merge_physically(&right_sibling)
                {
                    return Ok(None);
                }
                let merged_node = self.merge_nodes_view(node, &mut right_sibling)?;
                let merged_node_id = self.write_node_view(merged_node, track)?;
                track.reclaim(parent_node.child_ptr_at(right_idx)?);
                parent_node.delete_child_at(right_idx)?;
                track.reclaim(parent_node.child_ptr_at(idx)?);
                parent_node.replace_child_at(idx, merged_node_id)?;
                if parent_node.keys_len() > 0 {
                    parent_node.delete_key_at(parent_key_idx)?;
                }
                Ok(Some(merged_node_id))
            }
            (NodeView::Internal { .. }, NodeView::Internal { .. }) => {
                if node.keys_len() + right_sibling.keys_len() > self.max_keys
                    || !node.can_merge_physically(&right_sibling)
                {
                    return Ok(None);
                }

                // Pull the separator key from the parent down into the left node,
                // then absorb the right sibling's first child pointer.
                let seperator_key = parent_node.delete_key_at(parent_key_idx)?;
                node.insert_separator_at(
                    node.keys_len(),
                    seperator_key.as_bytes(),
                    right_sibling.child_ptr_at(0)?,
                )?;

                let merged_node = self.merge_nodes_view(node, &mut right_sibling)?;
                let merged_node_id = self.write_node_view(merged_node, track)?;

                track.reclaim(parent_node.child_ptr_at(right_idx)?);
                parent_node.delete_child_at(right_idx)?;
                track.reclaim(parent_node.child_ptr_at(idx)?);
                parent_node.replace_child_at(idx, merged_node_id)?;
                Ok(Some(merged_node_id))
            }
            _ => Err(TreeError::Invariant("mismatched node types for merge")),
        }
    }

    /// Merges `right_node` into `left_node` and returns the combined node.
    pub fn merge_nodes_view(
        &'s self,
        left_node: &'s mut NodeView,
        right_node: &'s mut NodeView,
    ) -> Result<&'s NodeView, TreeError> {
        match (&mut *left_node, &mut *right_node) {
            (NodeView::Leaf { .. }, NodeView::Leaf { .. }) => {
                if left_node.keys_len() + right_node.keys_len() > self.max_keys {
                    return Err(TreeError::Invariant("merge would exceed max keys"));
                }
                // TODO: merge into the emptier node.
                left_node.merge_into(right_node)?;
                Ok(left_node)
            }
            (NodeView::Internal { .. }, NodeView::Internal { .. }) => {
                if left_node.keys_len() + right_node.keys_len() > self.max_keys {
                    return Err(TreeError::Invariant("merge would exceed max keys"));
                }
                // TODO: merge into the emptier node.
                left_node.merge_into(right_node)?;
                Ok(left_node)
            }
            _ => Err(TreeError::Invariant("mismatched node types for merge")),
        }
    }

    /// Registers a node for deferred reclamation after the current epoch retires.
    pub fn reclaim_node(&self, node_id: NodeId) -> Result<(), TreeError> {
        let epoch = self
            .epoch_mgr
            .get_current_thread_epoch()
            .ok_or(TreeError::Invariant(
                "failed to get epoch for current thread",
            ))?;
        self.epoch_mgr.add_reclaim_candidate(epoch, node_id);
        Ok(())
    }

    /// Returns a snapshot of the current committed tree state.
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

    /// Commits new metadata in a single-threaded context; intended for testing only.
    ///
    /// This method mutates through a shared pointer without CAS and is
    /// unsound under concurrent access.  Gated behind `#[cfg(test)]` to
    /// prevent accidental use in production code.
    #[cfg(test)]
    pub fn commit(&self, new_root_id: NodeId, _height: u64, _size: u64) -> Result<(), TreeError> {
        let new_txn_id = self.txn_id.fetch_add(1, Ordering::SeqCst) + 1;
        // Write to whichever of the two metadata slots this transaction maps to.
        let target_slot = if new_txn_id % 2 == 0 {
            self.meta_a
        } else {
            self.meta_b
        };

        MetadataManager::commit_metadata(
            self.page_storage,
            target_slot,
            new_txn_id,
            self.id,
            new_root_id,
            self.get_height(),
            self.get_order(),
            self.get_size(),
        )?;

        let current_ptr = self.committed.load(Ordering::Acquire);
        let current = unsafe { &mut *current_ptr };
        self.storage.flush()?;

        current.root_node_id = new_root_id;
        current.txn_id = new_txn_id;

        self.commit_count.fetch_add(1, Ordering::Relaxed);

        let _new_epoch = self.epoch_mgr.advance();

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

    /// Attempts to commit staged metadata via a lock-free compare-and-exchange.
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
        let current_ptr = self.committed.load(Ordering::Acquire);
        let current = unsafe { &*current_ptr };
        // The txn_id stored in committed metadata is the authoritative sequence number.
        let new_txn_id = current.txn_id + 1;
        let metadata = Metadata {
            root_node_id: new_meta.root_id,
            id: self.id,
            height: new_meta.height,
            size: new_meta.size,
            txn_id: new_txn_id,
            checksum: 0,
            order: current.order,
        };

        let boxed = Box::new(metadata);
        let new_ptr = Box::into_raw(boxed);
        let result = self.committed.compare_exchange(
            expected as *mut Metadata,
            new_ptr,
            Ordering::SeqCst,
            Ordering::Relaxed,
        );

        match result {
            // CAS succeeded; write metadata to the double-buffered slot.
            Ok(old_ptr) => {
                let slot = if new_txn_id % 2 == 0 {
                    self.meta_a
                } else {
                    self.meta_b
                };

                let res = MetadataManager::commit_metadata_with_object(
                    self.page_storage,
                    slot,
                    &metadata,
                );
                if let Err(e) = res {
                    // Metadata write failed; restore the previous pointer.
                    unsafe {
                        drop(Box::from_raw(new_ptr));
                    }
                    self.committed.store(current_ptr, Ordering::Release);
                    return Err(CommitError::Metadata(e));
                }
                self.storage.flush()?;
                // Reclamation of old nodes is deferred until after the new metadata is durable and
                // visible.
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
                    drop(Box::from_raw(old_ptr));
                }

                Ok(())
            }
            Err(_) => {
                // CAS lost the race; discard the speculative metadata.
                unsafe {
                    drop(Box::from_raw(new_ptr));
                }
                Err(CommitError::RebaseRequired)
            }
        }
    }

    /// Returns a reference to the current committed metadata.
    pub fn metadata(&self) -> &Metadata {
        unsafe { &*self.committed.load(Ordering::Acquire) }
    }

    /// Returns a raw pointer to the current committed metadata.
    pub fn metadata_ptr(&self) -> *const Metadata {
        unsafe { &*self.committed.load(Ordering::Acquire) }
    }

    /// Returns a copy of the current committed metadata.
    pub fn snapshot(&self) -> Metadata {
        let ptr = self.committed.load(Ordering::Acquire);
        let meta = unsafe { &*ptr };

        Metadata {
            root_node_id: meta.root_node_id,
            id: meta.id,
            height: meta.height,
            size: meta.size,
            checksum: meta.checksum,
            txn_id: meta.txn_id,
            order: meta.order,
        }
    }

    //pub fn traverse(&self) -> Result<Vec<(&[u8], &[u8])>, TreeError> {
    //    let mut result = Vec::new();
    //    let root_id = self.get_root_id();
    //    if root_id == 0 {
    //        return Ok(result); // Empty tree
    //    }
    //    let _guard = self.epoch_mgr.pin();
    //    self.traverse_inner(root_id, &mut result)?;
    //    Ok(result)
    //}

    // Recursive implementation of traversal.
    //pub fn traverse_inner(
    //    &self,
    //    node_id: NodeId,
    //    result: &mut Vec<(&[u8], &[u8])>,
    //) -> Result<(), TreeError> {
    //    match self.read_node(node_id)? {
    //        Some(Node::Internal { keys, children }) => {
    //            for (i, child_id) in children.iter().enumerate() {
    //                if i <= keys.len() {
    //                    self.traverse_inner(*child_id, result)?;
    //                }
    //            }
    //        }
    //        Some(Node::Leaf { keys, values, .. }) => {
    //            for (key, value) in keys.iter().zip(values.iter()) {
    //                result.push((key.clone(), value.clone()));
    //            }
    //        }
    //        None => return Err(TreeError::NodeNotFound("Node not found".to_string())),
    //    }
    //    Ok(())
    //}

    /// Returns the current committed transaction ID.
    pub fn get_txn_id(&self) -> u64 {
        self.txn_id.load(Ordering::Relaxed)
    }

    /// Returns the current committed root node ID.
    pub fn get_root_id(&self) -> NodeId {
        let ptr = self.committed.load(Ordering::Acquire);
        let meta = unsafe { &*ptr };
        meta.root_node_id
    }

    /// Returns the current committed tree height.
    pub fn get_height(&self) -> u64 {
        let ptr = self.committed.load(Ordering::Acquire);
        let meta = unsafe { &*ptr };
        meta.height
    }

    /// Returns the current approximate entry count.
    pub fn get_size(&self) -> u64 {
        let ptr = self.committed.load(Ordering::Acquire);
        let meta = unsafe { &*ptr };
        meta.size
    }

    /// Returns the B+ tree order (maximum number of children per internal node).
    pub fn get_order(&self) -> u64 {
        let ptr = self.committed.load(Ordering::Acquire);
        let meta = unsafe { &*ptr };
        meta.order
    }

    #[cfg(any(test, feature = "testing"))]
    /// Force-publishes the given metadata without a CAS; for testing only.
    pub fn test_force_publish(&self, metadata: &Metadata) {
        let old_ptr = self
            .committed
            .swap(Box::into_raw(Box::new(*metadata)), Ordering::SeqCst);
        if !old_ptr.is_null() {
            unsafe {
                drop(Box::from_raw(old_ptr));
            }
        }
    }

    #[cfg(any(test, feature = "testing"))]
    /// Returns the epoch manager; for testing only.
    pub fn get_epoch_mgr(&self) -> Arc<EpochManager> {
        Arc::clone(&self.epoch_mgr)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::common::{test_storage::TestStorage, test_tree};

    #[test]
    fn commit_happy_path() {
        let storage = TestStorage::new();
        let test_harness = test_tree::<TestStorage>(storage, 128);
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
        let h = test_tree::<TestStorage>(storage, 128);
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
        let test_harness = test_tree::<TestStorage>(storage, 128);
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
        let h = test_tree::<TestStorage>(storage, 128);
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
                            size: i,
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
        let h = test_tree::<TestStorage>(storage, 128);
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
                        size: i,
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
        let test_harness = test_tree::<TestStorage>(storage, 128);
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
        let test_harness = test_tree::<TestStorage>(storage, 128);
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
