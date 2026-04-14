use crate::bplustree::tree::{BaseVersion, CommitError, StagedMetadata};
use crate::tests::common::{load_tree, make_tree, make_tree_generic};

use anyhow::Result;
use rand::Rng;
use rand::seq::SliceRandom;
use rand::thread_rng;
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// Encoding helpers
// ---------------------------------------------------------------------------

/// Encodes a `u64` as a big-endian byte array for use as a tree key.
fn k(i: u64) -> [u8; 8] {
    i.to_be_bytes()
}

/// Returns the test value string for key `i`.
fn v_str(i: u64) -> String {
    format!("value_{}", i)
}

/// Returns the test value for key `i` as raw bytes.
fn v_bytes(i: u64) -> Vec<u8> {
    v_str(i).into_bytes()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[test]
fn commit_persists_and_survives_reopen() {
    let dir = TempDir::new().unwrap();
    let tree = make_tree(&dir, 16).expect("create tree");

    let base = BaseVersion {
        committed_ptr: tree.get_metadata_ptr(),
    };
    let staged = StagedMetadata {
        root_id: 42,
        height: 3,
        size: 10,
    };
    tree.try_commit(&base, staged).expect("commit ok");

    let m = tree.get_metadata();
    assert_eq!(m.root_node_id, 42);
    assert_eq!(m.height, 3);
    assert_eq!(m.size, 10);
    assert_eq!(m.txn_id, 2);

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
    let iterations = order * 10;
    let tree = make_tree(&dir, order).expect("create tree");
    let base = BaseVersion {
        committed_ptr: tree.get_metadata(),
    };
    let mut root_id = tree.get_root_id();

    for i in 0..iterations {
        let res = tree.insert_with_root(k(i as u64), v_bytes(i as u64), root_id);
        assert!(res.is_ok(), "Insert should succeed");
        root_id = res.unwrap().new_root_id;
    }

    let height = tree.get_height();
    let size = tree.get_size();

    tree.try_commit(&base, StagedMetadata { root_id, height, size })?;

    for i in 0..iterations {
        let res = tree.search(&k(i as u64))?;
        assert!(res.is_some(), "Committed tree should have key {}", i);
    }

    let loaded = load_tree(&dir)?;
    assert!(loaded.get_root_id() != 0, "Loaded tree should have a valid root");

    for i in 0..iterations {
        let res = loaded.search(&k(i as u64))?;
        assert!(res.is_some(), "Loaded tree should have key {}", i);
        assert_eq!(
            res.unwrap(),
            v_bytes(i as u64),
            "Loaded tree should have the correct value for key {}",
            i
        );
    }
    Ok(())
}

#[test]
fn write_and_read_value() -> Result<(), anyhow::Error> {
    let dir = TempDir::new().unwrap();
    let tree = make_tree(&dir, 3).expect("create tree");
    let res = tree.insert(k(1), v_bytes(1));
    assert!(res.is_ok(), "Insert should succeed");
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
    let res = tree.search(&k(1))?;
    assert!(res.is_some(), "Value should be found after commit");
    assert_eq!(res.unwrap(), v_bytes(1), "Value should match what was inserted");
    Ok(())
}

#[test]
fn write_and_read_values_multiple() -> Result<(), anyhow::Error> {
    let dir = TempDir::new().unwrap();
    let order = 20;
    let tree = make_tree(&dir, order).expect("create tree");
    let mut root_id = tree.get_root_id();

    for i in 0..order - 1 {
        let key = k(i as u64);
        let value = v_bytes(i as u64);
        let res = tree.insert_with_root(key, value.clone(), root_id);
        assert!(res.is_ok(), "Insert should succeed");
        root_id = res.unwrap().new_root_id;
        let res = tree.search_with_root(&key, root_id)?;
        assert!(res.is_some(), "Value should be readable immediately");
        assert_eq!(res.unwrap(), value, "Value should match");
    }
    for i in 0..order - 1 {
        let key = k(i as u64);
        let res = tree.search_with_root(&key, root_id)?;
        assert!(res.is_some(), "Value should still be present");
        assert_eq!(res.unwrap(), v_bytes(i as u64), "Value should match");
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
        let key = format!("key_{:04}", i); // zero-pad for lexicographic ordering
        let value = format!("value_{}", i);
        let res = tree.insert_with_root(key.as_bytes(), value.as_bytes(), root_id);
        assert!(res.is_ok(), "Insert should succeed");
        root_id = res.unwrap().new_root_id;
        let res = tree.search_with_root(&key.as_bytes(), root_id)?;
        assert!(res.is_some(), "Value should be readable immediately");
        assert_eq!(res.unwrap(), value.as_bytes(), "Value should match");
    }
    for i in 0..order - 1 {
        let key = format!("key_{:04}", i);
        let value = format!("value_{}", i);
        let res = tree.search_with_root(&key.as_bytes(), root_id)?;
        assert!(res.is_some(), "Value should still be present");
        assert_eq!(res.unwrap(), value.as_bytes(), "Value should match");
    }
    Ok(())
}

#[test]
fn write_and_read_string_as_key() -> Result<(), anyhow::Error> {
    let dir = TempDir::new().unwrap();
    let tree = make_tree_generic::<String, String>(&dir, 3).expect("create tree");
    let key = "key1";
    let value = "value1";
    let res = tree.insert(key.as_bytes(), value.as_bytes());
    assert!(res.is_ok(), "Insert should succeed");
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
    let res = tree.search(&key.as_bytes())?;
    assert!(res.is_some(), "Value should be found after commit");
    assert_eq!(res.unwrap(), value.as_bytes(), "Value should match");
    Ok(())
}

#[test]
fn write_and_read_values_with_overflow() -> Result<(), anyhow::Error> {
    let dir = TempDir::new().unwrap();
    let order = 3;
    let tree = make_tree(&dir, order).expect("create tree");
    let mut root_id = tree.get_root_id();

    for i in 0..order * 1000 {
        let key = k(i as u64);
        let value = v_bytes(i as u64);
        let res = tree.insert_with_root(key, value.clone(), root_id);
        assert!(res.is_ok(), "Insert should succeed");
        root_id = res.unwrap().new_root_id;
        let res = tree.search_with_root(&key, root_id)?;
        assert!(res.is_some(), "Value should be readable immediately");
        assert_eq!(res.unwrap(), value, "Value should match");
    }
    for i in 0..order * 1000 {
        let res = tree.search_with_root(&k(i as u64), root_id)?;
        assert!(res.is_some(), "Value should still be present");
        assert_eq!(res.unwrap(), v_bytes(i as u64), "Value should match");
    }
    Ok(())
}

#[test]
fn write_and_delete_lockstep() -> Result<(), anyhow::Error> {
    let dir = TempDir::new().unwrap();
    let order = 3;
    let tree = make_tree(&dir, order).expect("create tree");
    let mut root_id = tree.get_root_id();
    let bound = order as u64 * 2;

    for i in 0..bound {
        let res = tree.insert_with_root(k(i), v_bytes(i), root_id);
        assert!(res.is_ok(), "Insert should succeed");
        root_id = res.unwrap().new_root_id;
    }
    for i in 0..bound {
        let res = tree.delete_with_root(&k(i), root_id);
        assert!(res.is_ok(), "Delete should succeed");
        root_id = res.unwrap().new_root_id;
        assert!(
            tree.search_with_root(&k(i), root_id)?.is_none(),
            "Key {} should be gone after deletion",
            i
        );
        if bound == i + 1 {
            return Ok(());
        }
        let key_rand = rand::thread_rng().gen_range(i + 1..bound);
        assert!(
            tree.search_with_root(&k(key_rand), root_id)?.is_some(),
            "Key {} should still be present",
            key_rand
        );
    }
    Ok(())
}

#[test]
fn write_and_delete_values() -> Result<(), anyhow::Error> {
    let dir = TempDir::new().unwrap();
    let order = 10;
    let multiplier = 200_u64;
    let tree = make_tree(&dir, order).expect("create tree");
    let mut root_id = tree.get_root_id();

    for i in 0..order as u64 * multiplier {
        let res = tree.insert_with_root(k(i), v_bytes(i), root_id);
        assert!(res.is_ok(), "Insert should succeed");
        root_id = res.unwrap().new_root_id;
    }

    let mut size = tree.get_size();
    for i in 0..order as u64 * multiplier {
        let res = tree.delete_with_root(&k(i), root_id);
        assert!(res.is_ok(), "Delete should succeed");
        let r = res.unwrap();
        root_id = r.new_root_id;
        size = r.new_size;
        assert!(
            tree.search_with_root(&k(i), root_id)?.is_none(),
            "Key {} should be gone after deletion",
            i
        );
    }

    let base = BaseVersion {
        committed_ptr: tree.get_metadata(),
    };
    tree.try_commit(&base, StagedMetadata { root_id, height: tree.get_height(), size })?;

    for i in 0..order as u64 * multiplier {
        assert!(
            tree.search(&k(i))?.is_none(),
            "Key {} should be absent after full deletion + commit",
            i
        );
    }
    Ok(())
}

#[test]
fn write_and_delete_values_random() -> Result<(), anyhow::Error> {
    let dir = TempDir::new().unwrap();
    let order = 10;
    let multiplier = 9_u64;
    let tree = make_tree(&dir, order).expect("create tree");
    let mut root_id = tree.get_root_id();

    for i in 0..order as u64 * multiplier {
        let res = tree.insert_with_root(k(i), v_bytes(i), root_id);
        assert!(res.is_ok(), "Insert should succeed");
        root_id = res.unwrap().new_root_id;
    }

    let mut values_to_delete: Vec<u64> = (0..order as u64 * multiplier).collect();
    values_to_delete.shuffle(&mut thread_rng());

    for i in values_to_delete {
        let res = tree.delete_with_root(&k(i), root_id)?;
        root_id = res.new_root_id;
        assert!(tree.search(&k(i))?.is_none(), "Key {} should be gone", i);
    }
    Ok(())
}

#[test]
fn test_height_increase_decrease() -> Result<(), anyhow::Error> {
    let dir = TempDir::new().unwrap();
    let order = 5;
    let multiplier = 20_u64;
    let tree = make_tree(&dir, order).expect("create tree");
    let mut root_id = tree.get_root_id();

    // Fill to just below split threshold — height stays at 1.
    for i in 0..order - 1 {
        let res = tree.insert_with_root(k(i as u64), v_bytes(i as u64), root_id)?;
        root_id = res.new_root_id;
        tree.try_commit(
            &BaseVersion { committed_ptr: tree.get_metadata() },
            StagedMetadata { root_id, height: res.new_height, size: res.new_size },
        )?;
    }
    root_id = tree.get_root_id();
    assert_eq!(tree.get_height(), 1, "Height should be 1 after {} inserts", order - 1);

    // Delete all — height stays at 1.
    for i in 0..order - 1 {
        let res = tree.delete_with_root(&k(i as u64), root_id)?;
        root_id = res.new_root_id;
        tree.try_commit(
            &BaseVersion { committed_ptr: tree.get_metadata() },
            StagedMetadata { root_id, height: res.new_height, size: res.new_size },
        )?;
    }
    assert_eq!(tree.get_height(), 1, "Height should remain 1 after all deletions");

    // Large insert/delete cycle.
    let iterations = order * multiplier as usize;
    for i in 0..iterations {
        let res = tree.insert_with_root(k(i as u64), v_bytes(i as u64), root_id)?;
        root_id = res.new_root_id;
        tree.try_commit(
            &BaseVersion { committed_ptr: tree.get_metadata() },
            StagedMetadata { root_id, height: res.new_height, size: res.new_size },
        )?;
    }
    for i in 0..iterations {
        let res = tree.delete_with_root(&k(i as u64), root_id)?;
        root_id = res.new_root_id;
        tree.try_commit(
            &BaseVersion { committed_ptr: tree.get_metadata() },
            StagedMetadata { root_id, height: res.new_height, size: res.new_size },
        )?;
    }
    assert_eq!(tree.get_height(), 1, "Height should remain 1 after full delete cycle");
    Ok(())
}

/// Old test that relied on the 3-type-parameter `BPlusTree::new` API.
/// TODO: rewrite once a typed facade over SharedBPlusTree is available.
#[test]
#[ignore = "requires typed BPlusTree API not yet ported to current architecture"]
fn insert_duplicate_keys_should_overwrite_value() {}

/// Requires `search_in_range` which is not yet implemented on `SharedBPlusTree`.
#[test]
#[ignore = "search_in_range is not yet implemented"]
fn range_search_test() {}

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
                Err(CommitError::RebaseRequired) => continue,
                Err(e) => panic!("unexpected error: {e:?}"),
            }
        }

        let m = tree.get_metadata();
        assert_eq!(m.root_node_id, 100 + i as u64);
        assert_eq!(m.txn_id, last_txn + 1);
        last_txn = m.txn_id;
    }

    drop(tree);
    let tree2 = load_tree(&dir).expect("reopen tree");
    let m2 = tree2.get_metadata();
    assert_eq!(m2.root_node_id, 100 + (order - 2) as u64);
    assert_eq!(m2.txn_id, last_txn);
}

// TODO: implement once corrupt-metadata recovery is built.
#[test]
fn recovery_picks_latest_valid_metadatapage_when_one_is_corrupt() {}

#[test]
fn concurrent_writers_retry_until_success() {
    use std::thread;
    let dir = TempDir::new().unwrap();
    let order = 16;
    let num_threads = 15;
    let iterations = 250;
    let tree = make_tree(&dir, order).expect("create tree");

    let threads: Vec<_> = (0..num_threads)
        .map(|tid| {
            let t = tree.clone();
            thread::spawn(move || {
                let mut ok = 0u64;
                for i in 0..iterations {
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
                            Ok(()) => { ok += 1; break; }
                            Err(CommitError::RebaseRequired) => continue,
                            Err(e) => panic!("unexpected IO error: {e:?}"),
                        }
                    }
                }
                ok
            })
        })
        .collect();

    let total_ok: u64 = threads.into_iter().map(|h| h.join().unwrap()).sum();

    assert_eq!(
        tree.get_metadata().txn_id,
        total_ok + 1,
        "txn_id should equal total successful commits + initial 1"
    );
}
