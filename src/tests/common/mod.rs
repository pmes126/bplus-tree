//! Shared test helpers for initialising B+ trees in unit and integration tests.

#![allow(dead_code, unused_imports)]

pub mod test_epoch;
pub mod test_storage;

use crate::api::KeyEncodingId;
use crate::bplustree::NodeView;
use crate::bplustree::transaction::WriteTransaction;
use crate::bplustree::tree::{BPlusTree, SharedBPlusTree};
use crate::database::metadata::Metadata;
use crate::keyfmt::KeyFormat;
use crate::keyfmt::raw::RawFormat;
use crate::page::LeafPage;
use crate::storage::epoch::EpochManager;
use crate::storage::file_page_storage::FilePageStorage;
use crate::storage::metadata_manager::MetadataManager;
use crate::storage::paged_node_storage::PagedNodeStorage;
use crate::storage::{HasEpoch, NodeStorage, PageStorage};

use std::sync::Arc;
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// Page IDs used by the test meta file
// ---------------------------------------------------------------------------

/// Page slot in the metadata file used as slot A.
const TEST_META_A: u64 = 0;
/// Page slot in the metadata file used as slot B.
const TEST_META_B: u64 = 1;
/// Stable tree ID assigned to all test trees.
const TEST_TREE_ID: u64 = 1;

// ---------------------------------------------------------------------------
// In-memory harness (TestStorage)
// ---------------------------------------------------------------------------

/// Holds a [`BPlusTree`] together with a reference to its underlying storage
/// so tests can inspect storage state (flush counts, freed pages, commits).
pub struct TestHarness<S, P>
where
    S: NodeStorage + HasEpoch + Send + Sync + 'static,
    P: PageStorage + Send + Sync + 'static,
{
    /// Arc-wrapped tree backed by shared storage.
    pub tree: Arc<BPlusTree<S, P>>,
    /// Shared reference to the node-storage instance used by `tree`.
    pub storage: Arc<S>,
}

/// Creates an in-memory [`TestHarness`] using `storage` for both node and page I/O.
///
/// `S` must implement both [`NodeStorage`] and [`PageStorage`] (e.g. [`TestStorage`]).
#[cfg(any(test, feature = "testing"))]
pub fn test_tree<S>(storage: S, order: u64) -> TestHarness<S, S>
where
    S: NodeStorage + PageStorage + HasEpoch + Send + Sync + 'static,
{
    let storage = Arc::new(storage);
    let epoch_mgr = storage.epoch_mgr().clone();
    let meta = fake_metadata(order);
    let tree = BPlusTree::open(
        Arc::clone(&storage),
        Arc::clone(&storage),
        meta,
        TEST_META_A,
        TEST_META_B,
        KeyFormat::Raw(RawFormat),
        KeyEncodingId::RawBytes,
        epoch_mgr,
    );
    TestHarness {
        tree: Arc::new(tree),
        storage,
    }
}

/// Like [`test_tree`] but uses a caller-supplied epoch manager.
///
/// Useful when a test needs to pre-seed epoch state before the tree is created.
#[cfg(any(test, feature = "testing"))]
pub fn test_tree_with_epoch<S>(
    storage: S,
    epoch_mgr: Arc<EpochManager>,
    order: u64,
) -> TestHarness<S, S>
where
    S: NodeStorage + PageStorage + HasEpoch + Send + Sync + 'static,
{
    let storage = Arc::new(storage);
    let meta = fake_metadata(order);
    let tree = BPlusTree::open(
        Arc::clone(&storage),
        Arc::clone(&storage),
        meta,
        TEST_META_A,
        TEST_META_B,
        KeyFormat::Raw(RawFormat),
        KeyEncodingId::RawBytes,
        epoch_mgr,
    );
    TestHarness {
        tree: Arc::new(tree),
        storage,
    }
}

/// Creates a [`WriteTransaction`] rooted at the current committed state of `tree`.
#[cfg(any(test, feature = "testing"))]
pub fn test_trx<S, P>(tree: SharedBPlusTree<S, P>) -> WriteTransaction
where
    S: NodeStorage + HasEpoch + Send + Sync + 'static,
    P: PageStorage + Send + Sync + 'static,
{
    WriteTransaction::new(tree.clone())
}

// ---------------------------------------------------------------------------
// File-backed helpers
// ---------------------------------------------------------------------------

/// Creates a fresh file-backed tree in `dir` and returns a shared handle to it.
///
/// Node pages are written to `data.db`; metadata slots A/B are written to
/// `meta.db`.  Both files are created in the temporary directory.
#[cfg(any(test, feature = "testing"))]
pub fn make_tree(
    dir: &TempDir,
    order: u64,
) -> anyhow::Result<SharedBPlusTree<PagedNodeStorage<FilePageStorage>, FilePageStorage>> {
    let data_path = dir.path().join("data.db");
    let manifest_path = dir.path().join("data.manifest");
    let meta_path = dir.path().join("meta.db");

    let node_storage = Arc::new(PagedNodeStorage::<FilePageStorage>::new(
        &data_path,
        &manifest_path,
    )?);
    let page_storage = Arc::new(FilePageStorage::open(&meta_path)?);

    // Write an initial blank root leaf node.
    let key_format = KeyFormat::Raw(RawFormat);
    let root_view = NodeView::Leaf {
        page: LeafPage::new(key_format),
        page_id: None,
    };
    let root_id = node_storage
        .write_node_view(&root_view)
        .map_err(|e| anyhow::anyhow!("write root: {e}"))?;

    // Persist initial metadata to both A/B slots so load_tree can read them.
    let init_meta = Metadata {
        root_node_id: root_id,
        id: TEST_TREE_ID,
        txn_id: 1,
        height: 1,
        order,
        size: 0,
        checksum: 0,
    };
    MetadataManager::commit_metadata_with_object(&*page_storage, TEST_META_A, &init_meta)?;
    MetadataManager::commit_metadata_with_object(&*page_storage, TEST_META_B, &init_meta)?;

    let epoch_mgr = node_storage.epoch_mgr().clone();
    let tree = BPlusTree::open(
        node_storage,
        page_storage,
        init_meta,
        TEST_META_A,
        TEST_META_B,
        key_format,
        KeyEncodingId::RawBytes,
        epoch_mgr,
    );
    Ok(SharedBPlusTree::new(tree))
}

/// Reopens an existing file-backed tree from `dir` created by [`make_tree`].
///
/// Reads the active metadata slot to restore committed `(root_id, height, size)`.
#[cfg(any(test, feature = "testing"))]
pub fn load_tree(
    dir: &TempDir,
) -> anyhow::Result<SharedBPlusTree<PagedNodeStorage<FilePageStorage>, FilePageStorage>> {
    let data_path = dir.path().join("data.db");
    let manifest_path = dir.path().join("data.manifest");
    let meta_path = dir.path().join("meta.db");

    let node_storage = Arc::new(PagedNodeStorage::<FilePageStorage>::new(
        &data_path,
        &manifest_path,
    )?);
    let page_storage = Arc::new(FilePageStorage::open(&meta_path)?);

    // Recover committed state from the double-buffered metadata slots.
    let meta = MetadataManager::read_active_meta(&*page_storage, TEST_META_A, TEST_META_B)?;

    let epoch_mgr = node_storage.epoch_mgr().clone();
    let tree = BPlusTree::open(
        node_storage,
        page_storage,
        meta,
        TEST_META_A,
        TEST_META_B,
        KeyFormat::Raw(RawFormat),
        KeyEncodingId::RawBytes,
        epoch_mgr,
    );
    Ok(SharedBPlusTree::new(tree))
}

// ---------------------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------------------

/// Builds an initial [`Metadata`] value for in-memory (non-file) test trees.
///
/// The root node ID is left as 0; [`TestStorage`] ignores all page reads so
/// the tree never actually dereferences it.
fn fake_metadata(order: u64) -> Metadata {
    Metadata {
        root_node_id: 0,
        id: TEST_TREE_ID,
        txn_id: 1,
        height: 1,
        order,
        size: 0,
        checksum: 0,
    }
}
