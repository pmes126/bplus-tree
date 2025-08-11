mod common;
use common::*;
use your_crate::{StagedMetadata, BaseVersion};

#[test]
fn commit_happy_path() {
    let storage = common::test_storage::TestStorage::new();
    let epoch = common::test_epoch::TestEpoch::new();
    let tree = common::test_tree(storage.clone(), epoch.clone());

    let base = tree.committed_ptr();
    let staged = StagedMetadata { root_id: 42, height: 3, size: 10 };
    tree.try_commit(base, staged).unwrap();
    assert_eq!(tree.metadata().root_id, 42);
}

