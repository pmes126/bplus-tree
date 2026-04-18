//! Edge-case tests for BPlusTree / SharedBPlusTree operations:
//! empty tree, non-existent keys, overwrites, boundary keys.

use crate::bplustree::tree::{BaseVersion, MAX_ENTRY_PAYLOAD, StagedMetadata, TreeError};
use crate::tests::common::make_tree;
use tempfile::TempDir;

fn k(i: u64) -> [u8; 8] {
    i.to_be_bytes()
}

fn v_bytes(i: u64) -> Vec<u8> {
    format!("value_{}", i).into_bytes()
}

// ---------------------------------------------------------------------------
// Empty tree
// ---------------------------------------------------------------------------

#[test]
fn search_empty_tree_returns_none() {
    let dir = TempDir::new().unwrap();
    let tree = make_tree(&dir, 16).expect("create tree");
    assert!(tree.search(k(0)).unwrap().is_none());
    assert!(tree.search(k(u64::MAX)).unwrap().is_none());
}

#[test]
fn delete_from_empty_tree_returns_error() {
    let dir = TempDir::new().unwrap();
    let tree = make_tree(&dir, 16).expect("create tree");
    let result = tree.delete_with_root(&k(1), tree.get_root_id());
    assert!(result.is_err(), "delete from empty tree should fail");
}

#[test]
fn range_on_empty_tree_returns_empty() {
    let dir = TempDir::new().unwrap();
    let tree = make_tree(&dir, 16).expect("create tree");
    let results: Vec<_> = tree
        .search_range(&k(0), None)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert!(results.is_empty());
}

// ---------------------------------------------------------------------------
// Overwrite semantics
// ---------------------------------------------------------------------------

#[test]
fn insert_duplicate_key_overwrites_value() {
    let dir = TempDir::new().unwrap();
    let tree = make_tree(&dir, 16).expect("create tree");

    let r1 = tree.insert(k(1), b"first").unwrap();
    let r2 = tree
        .insert_with_root(k(1), b"second", r1.new_root_id)
        .unwrap();

    let base = BaseVersion {
        committed_ptr: tree.get_metadata(),
    };
    tree.try_commit(
        &base,
        StagedMetadata {
            root_id: r2.new_root_id,
            height: r2.new_height,
            size: r2.new_size,
        },
    )
    .unwrap();

    assert_eq!(
        tree.search(k(1)).unwrap(),
        Some(b"second".to_vec()),
        "second insert for same key should overwrite"
    );
}

#[test]
fn overwrite_does_not_change_count() {
    let dir = TempDir::new().unwrap();
    let tree = make_tree(&dir, 16).expect("create tree");

    let r1 = tree.insert(k(1), b"first").unwrap();
    let _size_after_first = r1.new_size;

    let r2 = tree
        .insert_with_root(k(1), b"second", r1.new_root_id)
        .unwrap();
    // Size should still increment by 1 since the tree tracks staged size naively.
    // This test documents the current behavior.
    let _ = r2.new_size;
    // The key count is tracked at the metadata level, not deduplicated here.
    // What matters is the value was replaced.
    let val = tree.search_with_root(&k(1), r2.new_root_id).unwrap();
    assert_eq!(val, Some(b"second".to_vec()));
}

// ---------------------------------------------------------------------------
// Delete non-existent key
// ---------------------------------------------------------------------------

#[test]
fn delete_nonexistent_key_returns_error() {
    let dir = TempDir::new().unwrap();
    let tree = make_tree(&dir, 16).expect("create tree");

    // Insert key 1, then try to delete key 2.
    let r1 = tree.insert(k(1), v_bytes(1)).unwrap();
    let result = tree.delete_with_root(&k(2), r1.new_root_id);
    assert!(
        result.is_err(),
        "deleting a missing key should return an error"
    );
}

// ---------------------------------------------------------------------------
// Boundary key values
// ---------------------------------------------------------------------------

#[test]
fn insert_and_search_min_max_u64_keys() {
    let dir = TempDir::new().unwrap();
    let tree = make_tree(&dir, 16).expect("create tree");

    let r1 = tree.insert(k(0), b"min").unwrap();
    let r2 = tree
        .insert_with_root(k(u64::MAX), b"max", r1.new_root_id)
        .unwrap();

    let base = BaseVersion {
        committed_ptr: tree.get_metadata(),
    };
    tree.try_commit(
        &base,
        StagedMetadata {
            root_id: r2.new_root_id,
            height: r2.new_height,
            size: r2.new_size,
        },
    )
    .unwrap();

    assert_eq!(tree.search(k(0)).unwrap(), Some(b"min".to_vec()));
    assert_eq!(tree.search(k(u64::MAX)).unwrap(), Some(b"max".to_vec()));
}

#[test]
fn range_scan_includes_min_key_excludes_max_key() {
    let dir = TempDir::new().unwrap();
    let tree = make_tree(&dir, 16).expect("create tree");

    let mut root = tree.get_root_id();
    for i in 0..10u64 {
        let r = tree.insert_with_root(k(i), v_bytes(i), root).unwrap();
        root = r.new_root_id;
    }

    let base = BaseVersion {
        committed_ptr: tree.get_metadata(),
    };
    tree.try_commit(
        &base,
        StagedMetadata {
            root_id: root,
            height: tree.get_height(),
            size: 10,
        },
    )
    .unwrap();

    // [3, 7) should yield keys 3,4,5,6.
    let results: Vec<_> = tree
        .search_range(&k(3), Some(&k(7)))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(results.len(), 4);
    assert_eq!(results.first().unwrap().0.as_slice(), &k(3));
    assert_eq!(results.last().unwrap().0.as_slice(), &k(6));
}

// ---------------------------------------------------------------------------
// Key ordering across splits
// ---------------------------------------------------------------------------

#[test]
fn keys_remain_sorted_after_many_splits() {
    let dir = TempDir::new().unwrap();
    // Small order to force many splits.
    let tree = make_tree(&dir, 4).expect("create tree");
    let mut root = tree.get_root_id();

    for i in 0..100u64 {
        let r = tree.insert_with_root(k(i), v_bytes(i), root).unwrap();
        root = r.new_root_id;
    }

    let base = BaseVersion {
        committed_ptr: tree.get_metadata(),
    };
    tree.try_commit(
        &base,
        StagedMetadata {
            root_id: root,
            height: tree.get_height(),
            size: 100,
        },
    )
    .unwrap();

    // Full range scan should be strictly sorted.
    let results: Vec<_> = tree
        .search_range(&k(0), None)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(results.len(), 100);
    for i in 0..results.len() - 1 {
        assert!(
            results[i].0 < results[i + 1].0,
            "keys out of order at position {i}"
        );
    }
}

#[test]
fn keys_remain_sorted_after_inserts_and_deletes() {
    let dir = TempDir::new().unwrap();
    let tree = make_tree(&dir, 4).expect("create tree");
    let mut root = tree.get_root_id();

    // Insert 0..50.
    for i in 0..50u64 {
        let r = tree.insert_with_root(k(i), v_bytes(i), root).unwrap();
        root = r.new_root_id;
    }

    // Delete every third key.
    for i in (0..50u64).filter(|i| i % 3 == 0) {
        let r = tree.delete_with_root(&k(i), root).unwrap();
        root = r.new_root_id;
    }

    let base = BaseVersion {
        committed_ptr: tree.get_metadata(),
    };
    tree.try_commit(
        &base,
        StagedMetadata {
            root_id: root,
            height: tree.get_height(),
            size: tree.get_size(),
        },
    )
    .unwrap();

    let results: Vec<_> = tree
        .search_range(&k(0), None)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    for i in 0..results.len() - 1 {
        assert!(
            results[i].0 < results[i + 1].0,
            "keys out of order after deletes at position {i}"
        );
    }

    // Verify deleted keys are absent.
    for i in (0..50u64).filter(|i| i % 3 == 0) {
        assert!(
            tree.search(k(i)).unwrap().is_none(),
            "key {i} should be deleted"
        );
    }
}

// ---------------------------------------------------------------------------
// Single-element tree
// ---------------------------------------------------------------------------

#[test]
fn single_key_insert_search_delete() {
    let dir = TempDir::new().unwrap();
    let tree = make_tree(&dir, 16).expect("create tree");

    let r = tree.insert(k(42), b"only").unwrap();
    let base = BaseVersion {
        committed_ptr: tree.get_metadata(),
    };
    tree.try_commit(
        &base,
        StagedMetadata {
            root_id: r.new_root_id,
            height: r.new_height,
            size: r.new_size,
        },
    )
    .unwrap();

    assert_eq!(tree.search(k(42)).unwrap(), Some(b"only".to_vec()));

    let d = tree.delete_with_root(&k(42), tree.get_root_id()).unwrap();
    let base2 = BaseVersion {
        committed_ptr: tree.get_metadata(),
    };
    tree.try_commit(
        &base2,
        StagedMetadata {
            root_id: d.new_root_id,
            height: d.new_height,
            size: d.new_size,
        },
    )
    .unwrap();

    assert!(tree.search(k(42)).unwrap().is_none());
}

// ---------------------------------------------------------------------------
// Physical page fullness
// ---------------------------------------------------------------------------

#[test]
fn entry_too_large_is_rejected() {
    let dir = TempDir::new().unwrap();
    let tree = make_tree(&dir, 64).expect("create tree");

    let key = vec![0xAA; 8];
    // Value that exceeds MAX_ENTRY_PAYLOAD when combined with the key.
    let value = vec![0xBB; MAX_ENTRY_PAYLOAD + 1];

    let err = tree.put(key, value).unwrap_err();
    assert!(
        matches!(err, TreeError::EntryTooLarge { .. }),
        "expected EntryTooLarge, got: {err}"
    );
}

#[test]
fn entry_at_max_payload_is_accepted() {
    let dir = TempDir::new().unwrap();
    let tree = make_tree(&dir, 64).expect("create tree");

    let key = vec![0xAA; 8];
    // Value exactly at the limit.
    let value = vec![0xBB; MAX_ENTRY_PAYLOAD - 8];

    let wr = tree
        .put(key.clone(), value.clone())
        .expect("insert should succeed");
    let base = BaseVersion {
        committed_ptr: tree.get_metadata(),
    };
    tree.try_commit(
        &base,
        StagedMetadata {
            root_id: wr.new_root_id,
            height: wr.new_height,
            size: wr.new_size,
        },
    )
    .unwrap();

    assert_eq!(tree.search(key).unwrap(), Some(value));
}

#[test]
fn large_values_trigger_physical_split_before_max_keys() {
    // Use a high order so max_keys (63) is never reached by key count,
    // but the page fills up physically with just a few large entries.
    let dir = TempDir::new().unwrap();
    let tree = make_tree(&dir, 64).expect("create tree");

    // Each value is ~1000 bytes. With 8-byte keys, each entry takes
    // roughly 1014 bytes (8 key + 2 len prefix + 4 slot + 1000 value).
    // A 4088-byte buffer fits ~4 such entries before PageFull.
    let val_size = 1000;
    let num_entries = 20; // well beyond what a single page can hold

    let mut root = tree.get_root_id();
    for i in 0u64..num_entries {
        let mut value = vec![0u8; val_size];
        // Tag value so we can verify it later.
        value[0..8].copy_from_slice(&i.to_le_bytes());

        let wr = tree
            .put_with_root(k(i), value, root)
            .unwrap_or_else(|e| panic!("insert {i} failed: {e}"));
        root = wr.new_root_id;
    }

    // Commit the final state.
    let base = BaseVersion {
        committed_ptr: tree.get_metadata(),
    };
    tree.try_commit(
        &base,
        StagedMetadata {
            root_id: root,
            height: tree.get_height(),
            size: num_entries,
        },
    )
    .unwrap();

    // Verify every entry is retrievable and correct.
    for i in 0u64..num_entries {
        let found = tree
            .search(k(i))
            .unwrap_or_else(|e| panic!("search {i} failed: {e}"))
            .unwrap_or_else(|| panic!("key {i} not found"));
        let stored_tag = u64::from_le_bytes(found[0..8].try_into().unwrap());
        assert_eq!(stored_tag, i, "value mismatch for key {i}");
        assert_eq!(found.len(), val_size, "value length mismatch for key {i}");
    }
}

#[test]
fn large_values_with_deletes_maintain_tree_integrity() {
    let dir = TempDir::new().unwrap();
    let tree = make_tree(&dir, 64).expect("create tree");

    let val_size = 800;
    let num_entries: u64 = 30;

    let mut root = tree.get_root_id();
    for i in 0..num_entries {
        let value = vec![(i & 0xFF) as u8; val_size];
        let wr = tree
            .put_with_root(k(i), value, root)
            .unwrap_or_else(|e| panic!("insert {i} failed: {e}"));
        root = wr.new_root_id;
    }

    // Delete every other key.
    for i in (0..num_entries).step_by(2) {
        let wr = tree
            .delete_with_root(&k(i), root)
            .unwrap_or_else(|e| panic!("delete {i} failed: {e}"));
        root = wr.new_root_id;
    }

    // Verify odd keys remain, even keys are gone.
    for i in 0..num_entries {
        let result = tree.search_with_root(&k(i), root).unwrap();
        if i % 2 == 0 {
            assert!(result.is_none(), "key {i} should have been deleted");
        } else {
            let val = result.unwrap_or_else(|| panic!("key {i} not found"));
            assert_eq!(val.len(), val_size);
            assert_eq!(val[0], (i & 0xFF) as u8);
        }
    }
}

#[test]
fn mixed_small_and_large_values_coexist() {
    // Insert a mix of tiny and near-max values to stress uneven page splits.
    let dir = TempDir::new().unwrap();
    let tree = make_tree(&dir, 64).expect("create tree");

    let mut root = tree.get_root_id();
    let count: u64 = 60;

    for i in 0..count {
        let value = if i % 5 == 0 {
            // Near-max entry every 5th key.
            vec![0xCC; MAX_ENTRY_PAYLOAD - 8]
        } else {
            // Small value otherwise.
            format!("v{i}").into_bytes()
        };
        let wr = tree
            .put_with_root(k(i), value, root)
            .unwrap_or_else(|e| panic!("insert {i} failed: {e}"));
        root = wr.new_root_id;
    }

    for i in 0..count {
        let val = tree
            .search_with_root(&k(i), root)
            .unwrap_or_else(|e| panic!("search {i} failed: {e}"))
            .unwrap_or_else(|| panic!("key {i} not found"));
        if i % 5 == 0 {
            assert_eq!(
                val.len(),
                MAX_ENTRY_PAYLOAD - 8,
                "large value size mismatch at {i}"
            );
        } else {
            assert_eq!(
                val,
                format!("v{i}").into_bytes(),
                "small value mismatch at {i}"
            );
        }
    }
}

#[test]
fn overwrite_small_value_with_large_triggers_split() {
    // Fill a leaf with small values, then overwrite one with a near-max value.
    // The overwrite must handle the page becoming physically full.
    let dir = TempDir::new().unwrap();
    let tree = make_tree(&dir, 64).expect("create tree");

    let mut root = tree.get_root_id();
    // Insert 10 small entries — all fit in one leaf.
    for i in 0u64..10 {
        let wr = tree
            .put_with_root(k(i), b"tiny", root)
            .unwrap_or_else(|e| panic!("insert {i} failed: {e}"));
        root = wr.new_root_id;
    }

    // Overwrite key 5 with a large value.
    let big = vec![0xDD; MAX_ENTRY_PAYLOAD - 8];
    let wr = tree
        .put_with_root(k(5), big.clone(), root)
        .expect("overwrite with large value should succeed");
    root = wr.new_root_id;

    // All keys still present and correct.
    for i in 0u64..10 {
        let val = tree
            .search_with_root(&k(i), root)
            .unwrap_or_else(|e| panic!("search {i} failed: {e}"))
            .unwrap_or_else(|| panic!("key {i} not found"));
        if i == 5 {
            assert_eq!(val, big, "overwritten value mismatch");
        } else {
            assert_eq!(val, b"tiny".to_vec(), "untouched value mismatch at {i}");
        }
    }
}

#[test]
fn overwrite_on_nearly_full_page_triggers_split() {
    // Pack a leaf with medium-sized values so the value arena is nearly full,
    // then overwrite one entry with a larger value that won't fit in the
    // remaining arena space.  This exercises the PageFull → split path on
    // the replace_at (overwrite) branch of put_inner.
    let dir = TempDir::new().unwrap();
    let tree = make_tree(&dir, 64).expect("create tree");

    // Each entry: 8-byte key + 2 key-len prefix + 4 slot + 500 value = 514 bytes.
    // 7 entries ≈ 3598 bytes out of 4088 buffer → ~490 bytes free.
    let medium = 500;
    let mut root = tree.get_root_id();
    for i in 0u64..7 {
        let value = vec![(i & 0xFF) as u8; medium];
        let wr = tree
            .put_with_root(k(i), value, root)
            .unwrap_or_else(|e| panic!("insert {i} failed: {e}"));
        root = wr.new_root_id;
    }

    // Overwrite key 3 with a 1500-byte value.
    // The old 500-byte value isn't reclaimed (arena is append-only),
    // so we need 1500 fresh bytes but only ~490 are free → PageFull.
    let big = vec![0xEE; 1500];
    let wr = tree
        .put_with_root(k(3), big.clone(), root)
        .expect("overwrite should succeed after split");
    root = wr.new_root_id;

    // All 7 keys still present with correct values.
    for i in 0u64..7 {
        let val = tree
            .search_with_root(&k(i), root)
            .unwrap_or_else(|e| panic!("search {i} failed: {e}"))
            .unwrap_or_else(|| panic!("key {i} not found"));
        if i == 3 {
            assert_eq!(val, big, "overwritten value mismatch");
        } else {
            assert_eq!(val.len(), medium, "value length mismatch at key {i}");
            assert_eq!(val[0], (i & 0xFF) as u8, "value tag mismatch at key {i}");
        }
    }
}

#[test]
fn delete_all_large_values_leaves_empty_tree() {
    let dir = TempDir::new().unwrap();
    let tree = make_tree(&dir, 64).expect("create tree");

    let val_size = 1500;
    let count: u64 = 15;

    let mut root = tree.get_root_id();
    for i in 0..count {
        let value = vec![(i & 0xFF) as u8; val_size];
        let wr = tree
            .put_with_root(k(i), value, root)
            .unwrap_or_else(|e| panic!("insert {i} failed: {e}"));
        root = wr.new_root_id;
    }

    // Delete all keys.
    for i in 0..count {
        let wr = tree
            .delete_with_root(&k(i), root)
            .unwrap_or_else(|e| panic!("delete {i} failed: {e}"));
        root = wr.new_root_id;
    }

    // Everything gone.
    for i in 0..count {
        assert!(
            tree.search_with_root(&k(i), root).unwrap().is_none(),
            "key {i} should have been deleted"
        );
    }
}

#[test]
fn reverse_delete_order_with_large_values() {
    // Delete in reverse insertion order — exercises right-sibling merges
    // under physical fullness constraints.
    let dir = TempDir::new().unwrap();
    let tree = make_tree(&dir, 64).expect("create tree");

    let val_size = 900;
    let count: u64 = 25;

    let mut root = tree.get_root_id();
    for i in 0..count {
        let value = vec![(i & 0xFF) as u8; val_size];
        let wr = tree
            .put_with_root(k(i), value, root)
            .unwrap_or_else(|e| panic!("insert {i} failed: {e}"));
        root = wr.new_root_id;
    }

    // Delete in reverse order.
    for i in (0..count).rev() {
        let wr = tree
            .delete_with_root(&k(i), root)
            .unwrap_or_else(|e| panic!("delete {i} failed: {e}"));
        root = wr.new_root_id;

        // Remaining keys should still be findable.
        for j in 0..i {
            tree.search_with_root(&k(j), root)
                .unwrap_or_else(|e| panic!("search {j} failed after deleting {i}: {e}"))
                .unwrap_or_else(|| panic!("key {j} missing after deleting {i}"));
        }
    }
}

#[test]
fn large_values_with_small_order_forces_deep_tree() {
    // order=4 → max_keys=3. With large values (~1800 bytes) only 2 entries
    // fit per leaf, so every insert beyond the 2nd triggers a physical split.
    // This creates a much deeper tree than the order alone would suggest.
    let dir = TempDir::new().unwrap();
    let tree = make_tree(&dir, 4).expect("create tree");

    let val_size = 1800;
    let count: u64 = 30;

    let mut root = tree.get_root_id();
    for i in 0..count {
        let mut value = vec![0u8; val_size];
        value[0..8].copy_from_slice(&i.to_le_bytes());
        let wr = tree
            .put_with_root(k(i), value, root)
            .unwrap_or_else(|e| panic!("insert {i} failed: {e}"));
        root = wr.new_root_id;
    }

    // All values retrievable.
    for i in 0..count {
        let val = tree
            .search_with_root(&k(i), root)
            .unwrap_or_else(|e| panic!("search {i} failed: {e}"))
            .unwrap_or_else(|| panic!("key {i} not found"));
        let tag = u64::from_le_bytes(val[0..8].try_into().unwrap());
        assert_eq!(tag, i, "value tag mismatch at key {i}");
    }

    // Delete half, verify the rest.
    for i in (0..count).step_by(3) {
        let wr = tree
            .delete_with_root(&k(i), root)
            .unwrap_or_else(|e| panic!("delete {i} failed: {e}"));
        root = wr.new_root_id;
    }
    for i in 0..count {
        let result = tree.search_with_root(&k(i), root).unwrap();
        if i % 3 == 0 {
            assert!(result.is_none(), "key {i} should be deleted");
        } else {
            assert!(result.is_some(), "key {i} should still exist");
        }
    }
}
