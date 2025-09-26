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

pub struct TestHarness<K, V, S: Send + Sync>
where
    K: Clone + Ord,
    V: Clone,
    S: NodeStorage<K, V> + MetadataStorage + Send + Sync + 'static,
{
    pub tree: Arc<BPlusTree<K, V, S>>,
    pub storage: S,
}

#[cfg(any(test, feature = "testing"))]
pub fn test_tree<K, V, S>(storage: S, order: usize) -> TestHarness<K, V, S>
where
    K: Clone + Ord + Debug,
    V: Clone + Debug,
    S: NodeStorage<K, V> + MetadataStorage + Send + Sync + Clone + 'static,
{
    let fmt = crate::keyfmt::KeyFormat::Raw(crate::keyfmt::raw::RawFormat);
    let tree =
        BPlusTree::<K, V, S>::new(storage.clone(), order, fmt).expect("Failed to create BPlusTree");

    TestHarness {
        tree: std::sync::Arc::new(tree),
        storage,
    }
}

#[cfg(any(test, feature = "testing"))]
pub fn test_tree_with_epoch<K, V, S>(
    storage: S,
    epoch_manager: EpochManager,
    order: usize,
) -> TestHarness<K, V, S>
where
    K: Clone + Ord + Debug,
    V: Clone + Debug,
    S: NodeStorage<K, V> + MetadataStorage + Send + Sync + Clone + 'static,
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
) -> Result<SharedBPlusTree<u64, String, FileStore<PageStore>>, anyhow::Error> {
    let file_path = dir.path().join("tree.data");

    let fmt = crate::keyfmt::KeyFormat::Raw(crate::keyfmt::raw::RawFormat);
    let store: FileStore<PageStore> = FileStore::<PageStore>::new(file_path)?;
    let tree = BPlusTree::<u64, String, FileStore<PageStore>>::new(store, order, fmt)?;
    Ok(SharedBPlusTree::new(tree))
}

#[cfg(any(test, feature = "testing"))]
pub fn make_tree_generic<K, V>(
    dir: &TempDir,
    order: usize,
) -> Result<SharedBPlusTree<K, V, FileStore<PageStore>>, anyhow::Error>
where
    K: Debug + Ord + Clone,
    V: Debug + Clone,
    (): KeyCodecDefault<K> + ValueCodecDefault<V>,
{
    let file_path = dir.path().join("tree.data");
    let fmt = crate::keyfmt::KeyFormat::Raw(crate::keyfmt::raw::RawFormat);

    let store: FileStore<PageStore> = FileStore::<PageStore>::new(file_path)?;
    let tree = BPlusTree::<K, V, FileStore<PageStore>>::new(store, order, fmt)?;
    Ok(SharedBPlusTree::new(tree))
}

#[cfg(any(test, feature = "testing"))]
pub fn load_tree(
    dir: &TempDir,
) -> Result<SharedBPlusTree<u64, String, FileStore<PageStore>>, anyhow::Error> {
    let file_path = dir.path().join("tree.data");
    let store: FileStore<PageStore> = FileStore::<PageStore>::new(file_path)?;
    let tree = BPlusTree::<u64, String, FileStore<PageStore>>::load(store)?;
    Ok(SharedBPlusTree::new(tree))
}

#[cfg(any(test, feature = "testing"))]
pub fn test_trx<K, V, S>(tree: SharedBPlusTree<K, V, S>) -> WriteTransaction<K, V>
where
    K: Clone + Ord + Debug,
    V: Clone + Debug,
    S: NodeStorage<K, V> + MetadataStorage + Send + Sync,
{
    WriteTransaction::new(tree.clone())
}
