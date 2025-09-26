#![allow(dead_code)]

use crate::bplustree::EpochManager;
use crate::bplustree::transaction::WriteTransaction;
use crate::bplustree::tree::BPlusTree;
use crate::bplustree::tree::SharedBPlusTree;
use crate::codec::{KeyCodecDefault, ValueCodecDefault};
use crate::storage::file_store::FileStore;
use crate::storage::page_store::PageStore;
use crate::storage::{MetadataStorage, NodeStorage};

use std::fmt::Debug;
use std::sync::Arc;
use tempfile::TempDir;

pub mod test_epoch;
pub mod test_storage;

pub struct TestHarness<S: Send + Sync>
where
    S: NodeStorage + MetadataStorage + Send + Sync + 'static,
{
    pub tree: Arc<BPlusTree<S>>,
    pub storage: S,
}

#[cfg(any(test, feature = "testing"))]
pub fn test_tree<S>(storage: S, order: usize) -> TestHarness<S>
where
    S: NodeStorage + MetadataStorage + Send + Sync + Clone + 'static,
{
    let fmt = crate::keyfmt::KeyFormat::Raw(crate::keyfmt::raw::RawFormat);
    let tree =
        BPlusTree::<S>::new(storage.clone(), order, fmt).expect("Failed to create BPlusTree");

    TestHarness {
        tree: std::sync::Arc::new(tree),
        storage,
    }
}

#[cfg(any(test, feature = "testing"))]
pub fn test_tree_with_epoch<S>(
    storage: S,
    epoch_manager: EpochManager,
    order: usize,
) -> TestHarness<S>
where
    S: NodeStorage + MetadataStorage + Send + Sync + Clone + 'static,
{
    let tree = Arc::new(BPlusTree::new_with_deps(
        storage.clone(),
        epoch_manager,
        order, // order
    ));

    TestHarness { tree, storage }
}

#[cfg(any(test, feature = "testing"))]
pub fn make_tree(
    dir: &TempDir,
    order: usize,
) -> Result<SharedBPlusTree<FileStore<PageStore>>, anyhow::Error> {
    let file_path = dir.path().join("tree.data");

    let fmt = crate::keyfmt::KeyFormat::Raw(crate::keyfmt::raw::RawFormat);
    let store: FileStore<PageStore> = FileStore::<PageStore>::new(file_path)?;
    let tree = BPlusTree::<FileStore<PageStore>>::new(store, order, fmt)?;
    Ok(SharedBPlusTree::new(tree))
}

#[cfg(any(test, feature = "testing"))]
pub fn load_tree(
    dir: &TempDir,
) -> Result<SharedBPlusTree<FileStore<PageStore>>, anyhow::Error> {
    let file_path = dir.path().join("tree.data");
    let store: FileStore<PageStore> = FileStore::<PageStore>::new(file_path)?;
    let tree = BPlusTree::<FileStore<PageStore>>::load(store)?;
    Ok(SharedBPlusTree::new(tree))
}

#[cfg(any(test, feature = "testing"))]
pub fn test_trx<S>(tree: SharedBPlusTree<S>) -> WriteTransaction
where
    S: NodeStorage + MetadataStorage + Send + Sync,
{
    WriteTransaction::new(tree.clone())
}
