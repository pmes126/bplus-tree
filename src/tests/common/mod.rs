use std::sync::Arc;
use std::fmt::Debug;
use crate::storage::NodeStorage;
use crate::bplustree::tree::BPlusTree;
use crate::bplustree::EpochManager;
use crate::storage::{KeyCodec, ValueCodec, MetadataStorage};
use crate::tests::common::test_storage::{TestStorage, StorageState};

pub mod test_storage;
pub mod test_epoch;

pub struct TestHarness<K, V, S: Send + Sync>
    where
    K: KeyCodec + Ord,
    V: ValueCodec,
    S: NodeStorage<K, V> + MetadataStorage + Send + Sync + 'static,
{
    pub tree: Arc<BPlusTree<K, V, S>>,
    pub storage: S,
}

#[cfg(any(test, feature = "testing"))]
pub fn test_tree_with_noop_storage<K, V>(
    order: usize,
) -> TestHarness<K, V, TestStorage>
where
    K: KeyCodec + Clone + Ord + Debug + 'static,
    V: ValueCodec + Clone + Debug + 'static,
{
    let storage = TestStorage::new();

    let tree = Arc::new(BPlusTree::new_with_deps(
        storage.clone(),
        EpochManager::new(),
        order, // order
    ));
    TestHarness {
        tree,
        storage,
    }
}


#[cfg(any(test, feature = "testing"))]
pub fn test_tree<K, V, S>(
    storage: S,
    order: usize,
) -> TestHarness<K, V, S>
where
    K: KeyCodec + Clone + Ord + std::fmt::Debug + 'static,
    V: ValueCodec + Clone + std::fmt::Debug + 'static,
    S: NodeStorage<K, V> + MetadataStorage + Send + Sync + Clone + 'static,
{
    let tree = BPlusTree::<K, V, S>::new(storage.clone(), order)
        .expect("Failed to create BPlusTree");

    TestHarness {
        tree: std::sync::Arc::new(tree),
        storage, 
    }
}
