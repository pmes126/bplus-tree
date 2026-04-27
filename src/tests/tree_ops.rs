use crate::bplustree::tree::{BaseVersion, CommitError, StagedMetadata};
use crate::tests::common::{load_tree, make_tree};

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
        let res = tree.insert_with_root(k(i), v_bytes(i), root_id);
        assert!(res.is_ok(), "Insert should succeed");
        root_id = res.unwrap().new_root_id;
    }

    let height = tree.get_height();
    let size = tree.get_size();

    tree.try_commit(
        &base,
        StagedMetadata {
            root_id,
            height,
            size,
        },
    )?;

    for i in 0..iterations {
        let res = tree.search(k(i))?;
        assert!(res.is_some(), "Committed tree should have key {}", i);
    }

    let loaded = load_tree(&dir)?;
    assert!(
        loaded.get_root_id() != 0,
        "Loaded tree should have a valid root"
    );

    for i in 0..iterations {
        let res = loaded.search(k(i))?;
        assert!(res.is_some(), "Loaded tree should have key {}", i);
        assert_eq!(
            res.unwrap(),
            v_bytes(i),
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
    let res = tree.search(k(1))?;
    assert!(res.is_some(), "Value should be found after commit");
    assert_eq!(
        res.unwrap(),
        v_bytes(1),
        "Value should match what was inserted"
    );
    Ok(())
}

#[test]
fn write_and_read_values_multiple() -> Result<(), anyhow::Error> {
    let dir = TempDir::new().unwrap();
    let order = 20;
    let tree = make_tree(&dir, order).expect("create tree");
    let mut root_id = tree.get_root_id();

    for i in 0..order - 1 {
        let key = k(i);
        let value = v_bytes(i);
        let res = tree.insert_with_root(key, value.clone(), root_id);
        assert!(res.is_ok(), "Insert should succeed");
        root_id = res.unwrap().new_root_id;
        let res = tree.search_with_root(&key, root_id)?;
        assert!(res.is_some(), "Value should be readable immediately");
        assert_eq!(res.unwrap(), value, "Value should match");
    }
    for i in 0..order - 1 {
        let key = k(i);
        let res = tree.search_with_root(&key, root_id)?;
        assert!(res.is_some(), "Value should still be present");
        assert_eq!(res.unwrap(), v_bytes(i), "Value should match");
    }
    Ok(())
}

#[test]
fn write_and_read_multiple_string_as_key() -> Result<(), anyhow::Error> {
    let dir = TempDir::new().unwrap();
    let order = 20;
    let tree = make_tree(&dir, order).expect("create tree");
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
    let tree = make_tree(&dir, 3).expect("create tree");
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
    let res = tree.search(key.as_bytes())?;
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
        let key = k(i);
        let value = v_bytes(i);
        let res = tree.insert_with_root(key, value.clone(), root_id);
        assert!(res.is_ok(), "Insert should succeed");
        root_id = res.unwrap().new_root_id;
        let res = tree.search_with_root(&key, root_id)?;
        assert!(res.is_some(), "Value should be readable immediately");
        assert_eq!(res.unwrap(), value, "Value should match");
    }
    for i in 0..order * 1000 {
        let res = tree.search_with_root(&k(i), root_id)?;
        assert!(res.is_some(), "Value should still be present");
        assert_eq!(res.unwrap(), v_bytes(i), "Value should match");
    }
    Ok(())
}

#[test]
fn write_and_delete_lockstep() -> Result<(), anyhow::Error> {
    let dir = TempDir::new().unwrap();
    let order = 3;
    let tree = make_tree(&dir, order).expect("create tree");
    let mut root_id = tree.get_root_id();
    let bound = order * 2;

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

    for i in 0..order * multiplier {
        let res = tree.insert_with_root(k(i), v_bytes(i), root_id);
        assert!(res.is_ok(), "Insert should succeed");
        root_id = res.unwrap().new_root_id;
    }

    let mut size = tree.get_size();
    for i in 0..order * multiplier {
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
    tree.try_commit(
        &base,
        StagedMetadata {
            root_id,
            height: tree.get_height(),
            size,
        },
    )?;

    for i in 0..order * multiplier {
        assert!(
            tree.search(k(i))?.is_none(),
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

    for i in 0..order * multiplier {
        let res = tree.insert_with_root(k(i), v_bytes(i), root_id);
        assert!(res.is_ok(), "Insert should succeed");
        root_id = res.unwrap().new_root_id;
    }

    let mut values_to_delete: Vec<u64> = (0..order * multiplier).collect();
    values_to_delete.shuffle(&mut thread_rng());

    for i in values_to_delete {
        let res = tree.delete_with_root(&k(i), root_id)?;
        root_id = res.new_root_id;
        assert!(tree.search(k(i))?.is_none(), "Key {} should be gone", i);
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
        let res = tree.insert_with_root(k(i), v_bytes(i), root_id)?;
        root_id = res.new_root_id;
        tree.try_commit(
            &BaseVersion {
                committed_ptr: tree.get_metadata(),
            },
            StagedMetadata {
                root_id,
                height: res.new_height,
                size: res.new_size,
            },
        )?;
    }
    root_id = tree.get_root_id();
    assert_eq!(
        tree.get_height(),
        1,
        "Height should be 1 after {} inserts",
        order - 1
    );

    // Delete all — height stays at 1.
    for i in 0..order - 1 {
        let res = tree.delete_with_root(&k(i), root_id)?;
        root_id = res.new_root_id;
        tree.try_commit(
            &BaseVersion {
                committed_ptr: tree.get_metadata(),
            },
            StagedMetadata {
                root_id,
                height: res.new_height,
                size: res.new_size,
            },
        )?;
    }
    assert_eq!(
        tree.get_height(),
        1,
        "Height should remain 1 after all deletions"
    );

    // Large insert/delete cycle.
    let iterations = order * multiplier;
    for i in 0..iterations {
        let res = tree.insert_with_root(k(i), v_bytes(i), root_id)?;
        root_id = res.new_root_id;
        tree.try_commit(
            &BaseVersion {
                committed_ptr: tree.get_metadata(),
            },
            StagedMetadata {
                root_id,
                height: res.new_height,
                size: res.new_size,
            },
        )?;
    }
    for i in 0..iterations {
        let res = tree.delete_with_root(&k(i), root_id)?;
        root_id = res.new_root_id;
        tree.try_commit(
            &BaseVersion {
                committed_ptr: tree.get_metadata(),
            },
            StagedMetadata {
                root_id,
                height: res.new_height,
                size: res.new_size,
            },
        )?;
    }
    assert_eq!(
        tree.get_height(),
        1,
        "Height should remain 1 after full delete cycle"
    );
    Ok(())
}

/// Inserting the same key twice should overwrite the value, not duplicate it.
#[test]
fn insert_duplicate_keys_should_overwrite_value() {
    let dir = TempDir::new().unwrap();
    let tree = make_tree(&dir, 16).expect("create tree");
    let mut root_id = tree.get_root_id();

    let res = tree.insert_with_root(k(42), b"first", root_id).unwrap();
    root_id = res.new_root_id;

    let res = tree.insert_with_root(k(42), b"second", root_id).unwrap();
    root_id = res.new_root_id;

    let base = BaseVersion {
        committed_ptr: tree.get_metadata(),
    };
    tree.try_commit(
        &base,
        StagedMetadata {
            root_id,
            height: res.new_height,
            size: res.new_size,
        },
    )
    .unwrap();

    let val = tree.search(k(42)).unwrap();
    assert_eq!(
        val,
        Some(b"second".to_vec()),
        "second insert should overwrite the first"
    );
}

/// Range scan via `search_range` returns entries in key order within the given bounds.
#[test]
fn range_search_test() {
    let dir = TempDir::new().unwrap();
    let order = 8;
    let n = 50u64;
    let tree = make_tree(&dir, order).expect("create tree");
    let mut root_id = tree.get_root_id();

    for i in 0..n {
        let res = tree.insert_with_root(k(i), v_bytes(i), root_id).unwrap();
        root_id = res.new_root_id;
    }

    let base = BaseVersion {
        committed_ptr: tree.get_metadata(),
    };
    tree.try_commit(
        &base,
        StagedMetadata {
            root_id,
            height: tree.get_height(),
            size: n,
        },
    )
    .unwrap();

    // Bounded range [10, 20).
    let results: Vec<_> = tree
        .search_range(&k(10), Some(&k(20)))
        .expect("range")
        .collect::<Result<Vec<_>, _>>()
        .expect("iterate");

    assert_eq!(results.len(), 10, "range [10,20) should yield 10 entries");
    for (offset, (key, val)) in results.iter().enumerate() {
        let expected = 10 + offset as u64;
        assert_eq!(
            key.as_slice(),
            &k(expected),
            "key mismatch at offset {offset}"
        );
        assert_eq!(val, &v_bytes(expected), "value mismatch at offset {offset}");
    }

    // Unbounded range from key 45 to end.
    let tail: Vec<_> = tree
        .search_range(&k(45), None)
        .expect("range")
        .collect::<Result<Vec<_>, _>>()
        .expect("iterate");

    assert_eq!(tail.len(), 5, "range [45, end) should yield 5 entries");
    assert_eq!(tail.first().unwrap().0.as_slice(), &k(45));
    assert_eq!(tail.last().unwrap().0.as_slice(), &k(49));

    // Range beyond all keys.
    let empty: Vec<_> = tree
        .search_range(&k(100), Some(&k(200)))
        .expect("range")
        .collect::<Result<Vec<_>, _>>()
        .expect("iterate");

    assert!(empty.is_empty(), "range beyond all keys should be empty");
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
                root_id: 100 + i,
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
        assert_eq!(m.root_node_id, 100 + i);
        assert_eq!(m.txn_id, last_txn + 1);
        last_txn = m.txn_id;
    }

    drop(tree);
    let tree2 = load_tree(&dir).expect("reopen tree");
    let m2 = tree2.get_metadata();
    assert_eq!(m2.root_node_id, 100 + (order - 2));
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
                        root_id: 1000 + (tid * 1000 + i),
                        height: 3,
                        size: (tid * 1000 + i),
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

// ---------------------------------------------------------------------------
// contains_key (tree layer)
// ---------------------------------------------------------------------------

#[test]
fn contains_key_hit_and_miss() -> Result<()> {
    let dir = TempDir::new().unwrap();
    let tree = make_tree(&dir, 16).expect("create tree");

    // Insert keys and commit.
    let mut root_id = tree.get_root_id();
    for i in 0..20 {
        let res = tree.insert_with_root(k(i), v_bytes(i), root_id)?;
        root_id = res.new_root_id;
    }
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

    // Hits.
    for i in 0..20 {
        assert!(tree.contains_key(k(i))?, "key {i} should exist");
    }

    // Misses.
    for i in 20..30 {
        assert!(!tree.contains_key(k(i))?, "key {i} should not exist");
    }

    Ok(())
}

#[test]
fn contains_key_after_delete() -> Result<()> {
    let dir = TempDir::new().unwrap();
    let tree = make_tree(&dir, 8).expect("create tree");

    // Insert keys and commit.
    let mut root_id = tree.get_root_id();
    for i in 0..10 {
        let res = tree.insert_with_root(k(i), v_bytes(i), root_id)?;
        root_id = res.new_root_id;
    }
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

    // Delete every other key.
    root_id = tree.get_root_id();
    for i in (0..10).step_by(2) {
        let res = tree.delete_with_root(&k(i), root_id)?;
        root_id = res.new_root_id;
    }
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

    for i in 0..10 {
        let expected = i % 2 != 0;
        assert_eq!(
            tree.contains_key(k(i))?,
            expected,
            "key {i} existence mismatch"
        );
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Page cache correctness
// ---------------------------------------------------------------------------

/// After inserting and then deleting enough keys to trigger epoch reclamation,
/// reads must return fresh data — not stale cached nodes from before the GC
/// freed and potentially reallocated those page IDs.
#[test]
fn cache_returns_fresh_data_after_reclaim_and_reuse() -> Result<()> {
    let dir = TempDir::new().unwrap();
    let order = 4; // small order → more splits → more page churn
    let tree = make_tree(&dir, order).expect("create tree");

    // Phase 1: populate the tree so the cache fills up.
    let n = order * 8;
    let mut root_id = tree.get_root_id();
    for i in 0..n {
        let res = tree.insert_with_root(k(i), v_bytes(i), root_id)?;
        root_id = res.new_root_id;
    }
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

    // Verify all keys are present via committed root.
    for i in 0..n {
        assert!(
            tree.search(k(i))?.is_some(),
            "key {i} should exist after insert"
        );
    }

    // Phase 2: delete all keys. This generates retired pages.
    root_id = tree.get_root_id();
    for i in 0..n {
        let res = tree.delete_with_root(&k(i), root_id)?;
        root_id = res.new_root_id;
    }
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

    // Run reclamation to free old pages (and evict them from cache).
    tree.reclaim_deferred()?;

    // Phase 3: re-insert with different values. If page IDs from phase 1 are
    // reallocated, the cache must NOT return the old phase-1 node views.
    root_id = tree.get_root_id();
    for i in 0..n {
        let res = tree.insert_with_root(k(i), format!("new_{i}").into_bytes(), root_id)?;
        root_id = res.new_root_id;
    }
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

    // Verify we get the new values, not stale cached ones.
    for i in 0..n {
        let val = tree.search(k(i))?;
        assert!(val.is_some(), "key {i} should exist after re-insert");
        let expected = format!("new_{i}").into_bytes();
        assert_eq!(
            val.unwrap(),
            expected,
            "key {i} should have the new value, not a stale cached one"
        );
    }

    Ok(())
}

/// Concurrent readers and a writer should not see stale cached data.
/// One thread writes keys while others read — readers should never get
/// values that are internally inconsistent (e.g. a node from an old epoch
/// mixed with a node from a new epoch).
#[test]
fn cache_concurrent_read_write() -> Result<()> {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::thread;

    let dir = TempDir::new().unwrap();
    let tree = make_tree(&dir, 16).expect("create tree");

    // Seed some initial data and commit.
    let mut root_id = tree.get_root_id();
    for i in 0..50 {
        let res = tree.insert_with_root(k(i), v_bytes(i), root_id)?;
        root_id = res.new_root_id;
    }
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

    let done = Arc::new(AtomicBool::new(false));

    // Spawn readers that continuously check existing keys via committed root.
    let readers: Vec<_> = (0..3)
        .map(|_| {
            let t = tree.clone();
            let d = Arc::clone(&done);
            thread::spawn(move || {
                let mut reads = 0u64;
                while !d.load(Ordering::Relaxed) {
                    // Read a key that was inserted before readers started.
                    // Since writers commit each update, the reader may see
                    // either the original or the updated value.
                    for i in 0..50 {
                        if let Ok(Some(val)) = t.search(k(i)) {
                            assert!(
                                val == v_bytes(i) || val == format!("upd_{i}").into_bytes(),
                                "unexpected value for key {i}: {:?}",
                                val
                            );
                            reads += 1;
                        }
                    }
                }
                reads
            })
        })
        .collect();

    // Writer updates existing keys, committing each one.
    root_id = tree.get_root_id();
    for i in 0..50 {
        let res = tree.insert_with_root(k(i), format!("upd_{i}").into_bytes(), root_id)?;
        root_id = res.new_root_id;
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
    }

    done.store(true, Ordering::Relaxed);
    for h in readers {
        let reads = h.join().expect("reader thread panicked");
        assert!(reads > 0, "reader should have completed at least one read");
    }

    Ok(())
}
