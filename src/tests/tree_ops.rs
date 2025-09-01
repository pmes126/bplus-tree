use crate::bplustree::tree::StagedMetadata;
use crate::bplustree::tree::{BPlusTree, BaseVersion, CommitError, SharedBPlusTree};
use crate::storage::file_store::FileStore;
use crate::storage::page_store::PageStore;
use crate::tests::common::{load_tree, make_tree, make_tree_generic};

use anyhow::Result;
use rand::Rng;
use rand::seq::SliceRandom;
use rand::thread_rng;
use tempfile::TempDir;

#[test]
fn commit_persists_and_survives_reopen() {
    let dir = TempDir::new().unwrap();
    let tree = make_tree(&dir, 16).expect("create tree");

    // capture base
    let md = tree.get_metadata_ptr();
    let base = BaseVersion { committed_ptr: md };
    let staged = StagedMetadata {
        root_id: 42,
        height: 3,
        size: 10,
    };

    // commit (real file IO under the hood)
    tree.try_commit(&base, staged).expect("commit ok");

    let m = tree.get_metadata();
    // verify in-memory state
    assert_eq!(m.root_node_id, 42);
    assert_eq!(m.height, 3);
    assert_eq!(m.size, 10);
    assert_eq!(m.txn_id, 2);

    // Drop and reopen to validate on-disk metadata (double-buffer + checksum)
    drop(tree);
    let tree2 = load_tree(&dir).expect("reopen tree");

    let m2 = tree2.get_metadata();
    assert_eq!(m2.root_node_id, 42);
    assert_eq!(m2.txn_id, 2);
}

#[test]
fn commit_and_load_tree() -> Result<()> {
    let dir = TempDir::new().unwrap();
    let order = 4;
    let multiplier = 10; // Number of times to insert
    let iterations = order * multiplier;
    let tree = make_tree(&dir, order).expect("create tree");
    let base = BaseVersion {
        committed_ptr: tree.get_metadata(),
    };
    let mut root_id = tree.get_root_id();
    let mut height = tree.get_height();
    let mut size = tree.get_size();

    for i in 0..iterations {
        let key = i as u64;
        let value = format!("value_{}", i);
        let res = tree.insert_with_root(key, value.clone(), root_id);
        assert!(res.is_ok(), "Node should be inserted successfully");
        root_id = res.unwrap().new_root_id;
        height = tree.get_height();
        size = tree.get_size();
    }

    // Commit the changes
    assert!(
        tree.get_root_id() != root_id,
        "Root ID should be unchanged before commit {}",
        tree.get_root_id()
    );
    let track = StagedMetadata {
        root_id,
        height,
        size,
    };

    tree.try_commit(&base, track)?;
    assert!(
        tree.get_root_id() == root_id,
        "Root ID should be correct after commit {}",
        tree.get_root_id()
    );
    for i in 0..iterations {
        let key = i as u64;
        let res = tree.search(&key)?;
        assert!(res.is_some(), "Committed tree should have the key {}", key);
    }
    // Load the tree from storage
    let loaded_tree = load_tree(&dir)?;
    let root_id = loaded_tree.get_root_id();
    assert!(root_id != 0, "Loaded tree should have a valid root ID");
    // Verify the loaded tree
    for i in 0..iterations {
        let key = i as u64;
        let value = format!("value_{}", i);
        let res = loaded_tree.search(&key)?;
        assert!(res.is_some(), "Loaded tree should have the key {}", key);
        assert_eq!(
            loaded_tree.search(&key)?,
            Some(value),
            "Loaded tree should have the correct value for key {}",
            key
        );
    }
    Ok(())
}

#[test]
fn write_and_read_value() -> Result<(), anyhow::Error> {
    let dir = TempDir::new().unwrap();
    let tree = make_tree(&dir, 3).expect("create tree");
    let key = 1u64;
    let value = "a".to_string();
    let res = tree.insert(key, value.clone());
    assert!(res.is_ok(), "Value should be inserted successfully");
    let root_id = res.unwrap().new_root_id;
    let base = BaseVersion {
        committed_ptr: tree.get_metadata(),
    };
    tree.try_commit(
        &base,
        StagedMetadata {
            root_id,
            height: tree.get_height(),
            size: tree.get_size(),
        },
    )?;
    let res = tree.search(&key)?;
    assert!(res.is_some(), "Value should be read successfully");
    assert_eq!(res.unwrap(), value, "Value should match the inserted value");
    Ok(())
}

#[test]
fn write_and_read_values_multiple() -> Result<(), anyhow::Error> {
    let dir = TempDir::new().unwrap();
    let order = 20;
    let tree = make_tree(&dir, order).expect("create tree");

    let mut root_id = tree.get_root_id();
    for i in 0..order - 1 {
        let key = i as u64;
        let value = format!("value_{}", i);
        let res = tree.insert_with_root(key, value.clone(), root_id);
        assert!(res.is_ok(), "Value should be inserted successfully");
        root_id = res.unwrap().new_root_id; // Update root_id after each insert
        let res = tree.search_with_root(&key, root_id)?;
        assert!(res.is_some(), "Value should be read successfully");
        assert_eq!(res.unwrap(), value, "Value should match the inserted value");
    }
    for i in 0..order - 1 {
        let key = i as u64;
        let value = format!("value_{}", i);
        let res = tree.search_with_root(&key, root_id)?;
        assert!(res.is_some(), "Value should be read successfully");
        assert_eq!(res.unwrap(), value, "Value should match the inserted value");
    }
    Ok(())
}

#[test]
fn write_and_read_multiple_string_as_key() -> Result<(), anyhow::Error> {
    let dir = TempDir::new().unwrap();
    let order = 20;
    let tree = make_tree_generic::<String, String>(&dir, order).expect("create tree");

    let mut root_id = tree.get_root_id();

    for i in 0..order - 1 {
        let key = format!("key_{}", i);
        let value = format!("value_{}", i);
        let res = tree.insert_with_root(key.clone(), value.clone(), root_id);
        assert!(res.is_ok(), "Value should be inserted successfully");
        root_id = res.unwrap().new_root_id; // Update root_id after each insert
        let res = tree.search_with_root(&key, root_id)?;
        assert!(res.is_some(), "Value should be read successfully");
        assert_eq!(res.unwrap(), value, "Value should match the inserted value");
    }
    for i in 0..order - 1 {
        let key = format!("key_{}", i);
        let value = format!("value_{}", i);
        let res = tree.search_with_root(&key, root_id)?;
        assert!(res.is_some(), "Value should be read successfully");
        assert_eq!(res.unwrap(), value, "Value should match the inserted value");
    }
    Ok(())
}

#[test]
fn write_and_read_string_as_key() -> Result<(), anyhow::Error> {
    let dir = TempDir::new().unwrap();
    let tree = make_tree_generic::<String, String>(&dir, 3).expect("create tree");
    let key = "key1".to_string();
    let value = "value1".to_string();
    let res = tree.insert(key.clone(), value.clone());
    assert!(res.is_ok(), "Node should be inserted successfully");
    let root_id = res.unwrap().new_root_id;
    let base = BaseVersion {
        committed_ptr: tree.get_metadata(),
    };
    tree.try_commit(
        &base,
        StagedMetadata {
            root_id,
            height: tree.get_height(),
            size: tree.get_size(),
        },
    )?;
    let res = tree.search(&key)?;
    assert!(res.is_some(), "Node should be read successfully");
    assert_eq!(res.unwrap(), value, "Value should match the inserted value");
    Ok(())
}

#[test]
fn write_and_read_values_with_overflow() -> Result<(), anyhow::Error> {
    let dir = TempDir::new().unwrap();
    let order = 3; // minimum B+ tree order will cause overflows    
    let tree = make_tree(&dir, order).expect("create tree");
    let multiplier = 1000; // Number of times to insert times the order - this will cause
    let mut root_id = tree.get_root_id();
    // overflows
    for i in 0..order * multiplier {
        let key = i as u64;
        let value = format!("value_{}", i);
        let res = tree.insert_with_root(key, value.clone(), root_id);
        assert!(res.is_ok(), "Value should be inserted successfully");
        root_id = res.unwrap().new_root_id; // Update root_id after each insert
        let res = tree.search_with_root(&key, root_id)?;
        assert!(res.is_some(), "Value should be read successfully");
        assert_eq!(res.unwrap(), value, "Value should match the inserted value");
    }
    for i in 0..order * multiplier {
        let key = i as u64;
        let value = format!("value_{}", i);
        let res = tree.search_with_root(&key, root_id)?;
        assert!(res.is_some(), "Value should be read successfully");
        assert_eq!(res.unwrap(), value, "Value should match the inserted value");
    }
    Ok(())
}

#[test]
fn write_and_delete_lockstep() -> Result<(), anyhow::Error> {
    let dir = TempDir::new().unwrap();
    let order = 3; // B+ tree order
    let multiplier = 2; // Number of times to insert and delete
    let tree = make_tree(&dir, order).expect("create tree");
    let mut root_id = tree.get_root_id();
    let bound = order as u64 * multiplier;
    for i in 0..bound {
        let key = i;
        let value = format!("value_{}", i);
        let res = tree.insert_with_root(key, value.clone(), root_id);
        assert!(res.is_ok(), "Node should be inserted successfully");
        root_id = res.unwrap().new_root_id; // Update root_id after each insert
    }
    for i in 0..bound {
        let key = i;
        let res = tree.delete_with_root(&key, root_id);
        assert!(res.is_ok(), "Node should be inserted successfully");
        root_id = res.unwrap().new_root_id; // Update root_id after each delete
        let res = tree.search_with_root(&key, root_id)?;
        assert!(
            res.is_none(),
            "Key {} should be deleted successfully res none {}",
            key,
            res.is_none()
        );

        let mut rng = thread_rng();
        if bound == i + 1 {
            return Ok(()); // No more keys to search
        }
        let key_rand = rng.gen_range(i + 1..bound);
        let res = tree.search_with_root(&(key_rand), root_id)?;
        assert!(
            res.is_some(),
            "Key {} should be present res some {}",
            key_rand,
            res.is_some()
        );
    }
    Ok(())
}

#[test]
fn write_and_delete_values() -> Result<(), anyhow::Error> {
    let dir = TempDir::new().unwrap();
    let order = 10; // B+ tree order
    let multiplier = 200_u64; // Number of times to insert and delete
    let tree = make_tree(&dir, order).expect("create tree");
    let mut root_id = tree.get_root_id();
    // Inserting values
    for i in 0..order as u64 * multiplier {
        let key = i;
        let value = format!("value_{}", i);
        let res = tree.insert_with_root(key, value.clone(), root_id);
        assert!(res.is_ok(), "Node should be inserted successfully");
        root_id = res.unwrap().new_root_id; // Update root_id after each insert
    }
    let mut size = tree.get_size();
    // Deleting all values
    for i in 0..order as u64 * multiplier {
        let key = i;
        let res = tree.delete_with_root(&key, root_id);
        assert!(res.is_ok(), "Key should be deleted successfully");
        let r = res.unwrap();
        root_id = r.new_root_id; // Update root_id after each delete
        size = r.new_size; // Update size after each delete
        let res = tree.search_with_root(&key, root_id)?;
        assert!(
            res.is_none(),
            "Key {} should be deleted successfully res none {}",
            key,
            res.is_none()
        );
    }

    let base = BaseVersion {
        committed_ptr: tree.get_metadata(),
    };
    // Commit the changes
    let track = StagedMetadata {
        root_id,
        height: tree.get_height(),
        size,
    };
    tree.try_commit(&base, track)?;
    // Check that the tree is empty after all deletions
    let res = tree.traverse()?;

    assert!(res.is_empty(), "Tree should be empty after all deletions");

    for i in 0..order as u64 * multiplier {
        let key = i;
        let res = tree.search(&key)?;
        assert!(
            res.is_none(),
            "Key {} should be deleted successfully res none {}",
            key,
            res.is_none()
        );
    }
    Ok(())
}

#[test]
fn write_and_delete_values_random() -> Result<(), anyhow::Error> {
    let dir = TempDir::new().unwrap();
    let order = 10; // B+ tree order
    let multiplier = 200_u64; // Number of times to insert and delete
    let tree = make_tree(&dir, order).expect("create tree");
    let mut root_id = tree.get_root_id();

    for i in 0..order as u64 * multiplier {
        let key = i;
        let value = format!("value_{}", i);
        let res = tree.insert_with_root(key, value.clone(), root_id);
        assert!(res.is_ok(), "Node should be inserted successfully");
        root_id = res.unwrap().new_root_id; // Update root_id after each insert
    }
    let mut values_to_delete: Vec<u64> = (0..(order as u64) * multiplier).collect();
    let mut rng = thread_rng();
    values_to_delete.shuffle(&mut rng);

    for i in values_to_delete {
        let key = i;
        let res = tree.delete_with_root(&key, root_id)?;
        root_id = res.new_root_id; // Update root_id after each delete
        let res = tree.search(&key)?;
        assert!(res.is_none(), "Node should be deleted successfully");
    }
    Ok(())
}

#[test]
fn test_height_increase_decrease() -> Result<(), anyhow::Error> {
    let dir = TempDir::new().unwrap();
    let order = 5; // B+ tree order
    let multiplier = 20_u64; // Number of times to insert and delete
    let tree = make_tree(&dir, order).expect("create tree");
    let mut root_id = tree.get_root_id();
    #[allow(unused_assignments)]
    let mut height = tree.get_height();
    #[allow(unused_assignments)]
    let mut size = tree.get_size();

    // No height increase on inserts up to order - 1
    let iterations = order * multiplier as usize;
    for i in 0..order - 1 {
        let key = i as u64;
        let value = format!("value_{}", i);
        let res = tree.insert_with_root(key, value.clone(), root_id)?;
        root_id = res.new_root_id; // Update root_id after each insert
        height = res.new_height;
        size = res.new_size;
        let base = BaseVersion {
            committed_ptr: tree.get_metadata(),
        };
        tree.try_commit(
            &base,
            StagedMetadata {
                root_id,
                height,
                size,
            },
        )?;
    }
    root_id = tree.get_root_id();
    assert_eq!(
        tree.get_height(),
        1,
        "Height should be 1 after inserting {} nodes",
        order - 1
    );

    for i in 0..order - 1 {
        let base = BaseVersion {
            committed_ptr: tree.get_metadata(),
        };
        let key = i as u64;
        let res = tree.delete_with_root(&key, root_id)?;
        root_id = res.new_root_id; // Update root_id after each delete
        height = res.new_height;
        size = res.new_size;
        tree.try_commit(
            &base,
            StagedMetadata {
                root_id,
                height,
                size,
            },
        )?;
    }
    assert_eq!(
        tree.get_height(),
        1,
        "Height should remain 1 after deleting all nodes"
    );

    for i in 0..iterations {
        let base = BaseVersion {
            committed_ptr: tree.get_metadata(),
        };
        let key = i as u64;
        let value = format!("value_{}", i);
        let res = tree.insert_with_root(key, value.clone(), root_id)?;
        root_id = res.new_root_id; // Update root_id after each insert
        height = res.new_height;
        size = res.new_size;
        tree.try_commit(
            &base,
            StagedMetadata {
                root_id,
                height,
                size,
            },
        )?;
    }
    for i in 0..iterations {
        let base = BaseVersion {
            committed_ptr: tree.get_metadata(),
        };
        let key = i as u64;
        let res = tree.delete_with_root(&key, root_id)?;
        root_id = res.new_root_id; // Update root_id after each delete
        height = res.new_height;
        size = res.new_size;
        tree.try_commit(
            &base,
            StagedMetadata {
                root_id,
                height,
                size,
            },
        )?;
    }
    assert_eq!(
        tree.get_height(),
        1,
        "Height should remain 1 after deleting all nodes"
    );
    Ok(())
}

#[test]
fn insert_duplicate_keys_should_overwrite_value() -> Result<()> {
    let dir = TempDir::new().unwrap();
    let file_path = dir.path().join("tree.data");

    let order = 4; // B+ tree order
    let store: FileStore<PageStore> = FileStore::<PageStore>::new(file_path)?;
    let tree = SharedBPlusTree::new(BPlusTree::<String, String, FileStore<PageStore>>::new(
        store, order,
    )?);
    let mut root_id = tree.get_root_id();

    for i in 0..order {
        let key = format!("key_{}", i);
        let value = format!("value_{}", i);
        let value_updated = format!("value_upd_{}", i);
        let res = tree.insert_with_root(key.clone(), value.clone(), root_id)?;
        root_id = res.new_root_id; // Update root_id after each insert
        assert_eq!(
            tree.search_with_root(&key, root_id)?,
            Some(value.clone()),
            "Value should be inserted successfully"
        );
        let res = tree.insert_with_root(key.clone(), value_updated.clone(), root_id);
        assert!(res.is_ok(), "Node should be inserted successfully");
        root_id = res.unwrap().new_root_id; // Update root_id after each insert
        assert_eq!(
            tree.search_with_root(&key, root_id)?,
            Some(value_updated),
            "Value should be updated for duplicate key"
        );
    }
    Ok(())
}

#[test]
fn range_search_test() -> Result<()> {
    let dir = TempDir::new().unwrap();
    let order = 4; // B+ tree order
    let multiplier = 20_u64; // Number of times to insert and delete
    let tree = make_tree(&dir, order).expect("create tree");
    let mut root_id = tree.get_root_id();
    let base = BaseVersion {
        committed_ptr: tree.get_metadata(),
    };
    let iterations = order * multiplier as usize;
    {
        for i in 0..iterations {
            let key = i as u64;
            let value = format!("value_{}", i);
            let res = tree.insert_with_root(key, value.clone(), root_id)?;
            root_id = res.new_root_id; // Update root_id after each insert
        }
        tree.try_commit(
            &base,
            StagedMetadata {
                root_id,
                height: tree.get_height(),
                size: tree.get_size(),
            },
        )?;
        assert!(
            tree.get_root_id() == root_id,
            "Root ID should be correct after commit {}",
            tree.get_root_id()
        );

        // Perform range search
        let start = 0;
        let end = iterations as u64 - 1;
        let res = tree.search_in_range(&start, &end)?;
        assert!(res.is_some(), "Range search should be successful");
        for (i, value) in res.unwrap().enumerate() {
            let (key, val) = value?;

            assert_eq!(key, i as u64, "Key should match the index in range search");
            assert_eq!(
                val,
                format!("value_{}", i),
                "Value should match the inserted value in range search"
            );
        }

        let start_rand = rand::thread_rng().gen_range(0..(iterations / 2) as u64);
        let end_rand = rand::thread_rng().gen_range(start_rand..iterations as u64);
        let res = tree.search_in_range(&start_rand, &end_rand)?;
        for (i, value) in res.unwrap().enumerate() {
            let (key, val) = value?;
            assert_eq!(
                key,
                start_rand + i as u64,
                "Key should match the index in range search"
            );
            assert_eq!(
                val,
                format!("value_{}", start_rand + i as u64),
                "Value should match the inserted value in range search"
            );
        }
    }
    Ok(())
}

#[test]
fn commits_toggle_metadata_slots_and_increment_txn() {
    let dir = TempDir::new().unwrap();
    let order = 16;
    let tree = make_tree(&dir, order).expect("create tree");

    let mut last_txn = tree.get_metadata().txn_id;

    for i in 0..order - 1 {
        loop {
            let base = BaseVersion {
                committed_ptr: tree.get_metadata_ptr(),
            };
            let staged = StagedMetadata {
                root_id: 100 + i as u64,
                height: 3,
                size: i,
            };
            match tree.try_commit(&base, staged) {
                Ok(()) => break,
                Err(CommitError::RebaseRequired) => continue, // shouldn't happen, single thread
                Err(e) => panic!("unexpected error: {e:?}"),
            }
        }

        let m = tree.get_metadata();
        assert_eq!(m.root_node_id, 100 + i as u64);
        assert_eq!(m.txn_id, last_txn + 1);

        last_txn = m.txn_id;
    }

    // Reopen and verify final state persisted
    drop(tree);
    let tree2 = load_tree(&dir).expect("reopen tree");
    let m2 = tree2.get_metadata();
    assert_eq!(m2.root_node_id, 100 + (order - 2) as u64);
    assert_eq!(m2.txn_id, last_txn);
}

//TODO: Implement functionality to recover from corrupt metadata blocks
#[test]
fn recovery_picks_latest_valid_metadatapage_when_one_is_corrupt() {}

#[test]
fn concurrent_writers_retry_until_success() {
    use std::thread;
    let dir = TempDir::new().unwrap();
    let order = 16; // B+ tree order
    let num_threads = 15; // Number of concurrent threads
    let iterations = 250; // Number of commits per thread
    let tree = make_tree(&dir, order).expect("create tree");

    let threads: Vec<_> = (0..num_threads)
        .map(|tid| {
            let t = tree.clone();
            thread::spawn(move || {
                let mut ok = 0u64; // number of successful commits
                for i in 0..iterations {
                    // simple staged value per-thread
                    let staged = StagedMetadata {
                        root_id: 1000 + (tid * 1000 + i) as u64,
                        height: 3,
                        size: (tid * 1000 + i) as usize,
                    };
                    loop {
                        let base = BaseVersion {
                            committed_ptr: t.get_metadata_ptr(),
                        };
                        match t.try_commit(&base, staged.clone()) {
                            Ok(()) => {
                                ok += 1;
                                break;
                            }
                            Err(CommitError::RebaseRequired) => continue,
                            Err(e) => {
                                println!("HERE HERE");
                                panic!("unexpected IO error: {e:?}");
                            }
                        }
                    }
                }
                ok
            })
        })
        .collect();

    let total_ok: u64 = threads.into_iter().map(|h| h.join().unwrap()).sum();

    // Monotonic txn_id equals # of successful commits.
    assert_eq!(
        tree.get_metadata().txn_id,
        total_ok + 1, // txn_id starts at 1
        "Total successful commits should match the final txn_id"
    );
}
