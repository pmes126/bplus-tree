use crate::bplustree::tree::{BaseVersion, StagedMetadata};
use crate::tests::common::make_tree;

use tempfile::TempDir;

// ---------------------------------------------------------------------------
// Encoding helpers
// ---------------------------------------------------------------------------

fn k(i: u64) -> [u8; 8] {
    i.to_be_bytes()
}

fn v_str(i: u64) -> String {
    format!("value_{}", i)
}

fn v_bytes(i: u64) -> Vec<u8> {
    v_str(i).into_bytes()
}

/// Helper: insert keys 0..n into the tree and commit.
fn populate_and_commit(
    dir: &TempDir,
    order: u64,
    n: u64,
) -> crate::bplustree::tree::SharedBPlusTree<
    'static,
    crate::storage::paged_node_storage::PagedNodeStorage<
        crate::storage::file_page_storage::FilePageStorage,
    >,
    crate::storage::file_page_storage::FilePageStorage,
> {
    let tree = make_tree(dir, order).expect("create tree");
    let mut root_id = tree.get_root_id();

    for i in 0..n {
        let res = tree
            .insert_with_root(k(i), v_bytes(i), root_id)
            .expect("insert");
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
    .expect("commit");

    tree
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[test]
fn range_full_scan() {
    let dir = TempDir::new().unwrap();
    let n = 100u64;
    let tree = populate_and_commit(&dir, 8, n);

    let results: Vec<_> = tree
        .search_range(&k(0), None)
        .expect("range")
        .collect::<Result<Vec<_>, _>>()
        .expect("iterate");

    assert_eq!(results.len(), n as usize);
    for (i, (key, val)) in results.iter().enumerate() {
        assert_eq!(key.as_slice(), &k(i as u64), "key mismatch at position {i}");
        assert_eq!(val, &v_bytes(i as u64), "value mismatch at position {i}");
    }
}

#[test]
fn range_bounded() {
    let dir = TempDir::new().unwrap();
    let n = 100u64;
    let tree = populate_and_commit(&dir, 8, n);

    // Scan [10, 20)
    let results: Vec<_> = tree
        .search_range(&k(10), Some(&k(20)))
        .expect("range")
        .collect::<Result<Vec<_>, _>>()
        .expect("iterate");

    assert_eq!(results.len(), 10);
    for (offset, (key, val)) in results.iter().enumerate() {
        let expected = 10 + offset as u64;
        assert_eq!(key.as_slice(), &k(expected));
        assert_eq!(val, &v_bytes(expected));
    }
}

#[test]
fn range_empty_tree() {
    let dir = TempDir::new().unwrap();
    let tree = make_tree(&dir, 8).expect("create tree");

    let results: Vec<_> = tree
        .search_range(&k(0), None)
        .expect("range")
        .collect::<Result<Vec<_>, _>>()
        .expect("iterate");

    assert!(results.is_empty(), "empty tree should yield no results");
}

#[test]
fn range_no_matches() {
    let dir = TempDir::new().unwrap();
    let tree = populate_and_commit(&dir, 8, 50);

    // All keys are 0..50; scan [100, 200) should yield nothing.
    let results: Vec<_> = tree
        .search_range(&k(100), Some(&k(200)))
        .expect("range")
        .collect::<Result<Vec<_>, _>>()
        .expect("iterate");

    assert!(results.is_empty(), "range beyond all keys should be empty");
}

#[test]
fn range_single_element() {
    let dir = TempDir::new().unwrap();
    let tree = populate_and_commit(&dir, 8, 50);

    // Scan [25, 26) should yield exactly key 25.
    let results: Vec<_> = tree
        .search_range(&k(25), Some(&k(26)))
        .expect("range")
        .collect::<Result<Vec<_>, _>>()
        .expect("iterate");

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].0.as_slice(), &k(25));
    assert_eq!(results[0].1, v_bytes(25));
}

#[test]
fn range_start_equals_end_yields_nothing() {
    let dir = TempDir::new().unwrap();
    let tree = populate_and_commit(&dir, 8, 50);

    // [25, 25) is an empty interval.
    let results: Vec<_> = tree
        .search_range(&k(25), Some(&k(25)))
        .expect("range")
        .collect::<Result<Vec<_>, _>>()
        .expect("iterate");

    assert!(results.is_empty());
}

#[test]
fn range_from_middle_to_end() {
    let dir = TempDir::new().unwrap();
    let n = 100u64;
    let tree = populate_and_commit(&dir, 8, n);

    // Unbounded scan from key 90 onward.
    let results: Vec<_> = tree
        .search_range(&k(90), None)
        .expect("range")
        .collect::<Result<Vec<_>, _>>()
        .expect("iterate");

    assert_eq!(results.len(), 10);
    for (offset, (key, _)) in results.iter().enumerate() {
        assert_eq!(key.as_slice(), &k(90 + offset as u64));
    }
}

#[test]
fn range_first_key_only() {
    let dir = TempDir::new().unwrap();
    let tree = populate_and_commit(&dir, 8, 50);

    let results: Vec<_> = tree
        .search_range(&k(0), Some(&k(1)))
        .expect("range")
        .collect::<Result<Vec<_>, _>>()
        .expect("iterate");

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].0.as_slice(), &k(0));
}

#[test]
fn range_last_key_only() {
    let dir = TempDir::new().unwrap();
    let n = 50u64;
    let tree = populate_and_commit(&dir, 8, n);

    let results: Vec<_> = tree
        .search_range(&k(n - 1), Some(&k(n)))
        .expect("range")
        .collect::<Result<Vec<_>, _>>()
        .expect("iterate");

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].0.as_slice(), &k(n - 1));
}

#[test]
fn range_with_small_order_forces_many_leaf_transitions() {
    let dir = TempDir::new().unwrap();
    // Order 3 means max 2 keys per leaf — many leaf transitions.
    let n = 200u64;
    let tree = populate_and_commit(&dir, 3, n);

    let results: Vec<_> = tree
        .search_range(&k(0), None)
        .expect("range")
        .collect::<Result<Vec<_>, _>>()
        .expect("iterate");

    assert_eq!(results.len(), n as usize);
    for (i, (key, val)) in results.iter().enumerate() {
        assert_eq!(key.as_slice(), &k(i as u64), "key mismatch at {i}");
        assert_eq!(val, &v_bytes(i as u64), "value mismatch at {i}");
    }
}

#[test]
fn range_with_large_order() {
    let dir = TempDir::new().unwrap();
    // Order 64 means many keys fit in a single leaf.
    let n = 500u64;
    let tree = populate_and_commit(&dir, 64, n);

    // Bounded sub-range.
    let results: Vec<_> = tree
        .search_range(&k(100), Some(&k(400)))
        .expect("range")
        .collect::<Result<Vec<_>, _>>()
        .expect("iterate");

    assert_eq!(results.len(), 300);
    for (offset, (key, _)) in results.iter().enumerate() {
        assert_eq!(key.as_slice(), &k(100 + offset as u64));
    }
}

#[test]
fn range_string_keys() {
    let dir = TempDir::new().unwrap();
    let order = 8;
    let tree = make_tree(&dir, order).expect("create tree");
    let mut root_id = tree.get_root_id();

    let keys: Vec<String> = (0..50).map(|i| format!("key_{:04}", i)).collect();
    let vals: Vec<String> = (0..50).map(|i| format!("val_{}", i)).collect();

    for (key, val) in keys.iter().zip(vals.iter()) {
        let res = tree
            .insert_with_root(key.as_bytes(), val.as_bytes(), root_id)
            .expect("insert");
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
            size: 50,
        },
    )
    .expect("commit");

    // Scan [key_0010, key_0020)
    let start = "key_0010";
    let end = "key_0020";
    let results: Vec<_> = tree
        .search_range(start.as_bytes(), Some(end.as_bytes()))
        .expect("range")
        .collect::<Result<Vec<_>, _>>()
        .expect("iterate");

    assert_eq!(results.len(), 10);
    for (offset, (key_bytes, val_bytes)) in results.iter().enumerate() {
        let expected_key = format!("key_{:04}", 10 + offset);
        let expected_val = format!("val_{}", 10 + offset);
        assert_eq!(std::str::from_utf8(key_bytes).unwrap(), expected_key);
        assert_eq!(std::str::from_utf8(val_bytes).unwrap(), expected_val);
    }
}

#[test]
fn range_yields_correct_order_after_inserts_and_deletes() {
    let dir = TempDir::new().unwrap();
    let tree = populate_and_commit(&dir, 8, 100);

    // Delete even keys.
    let mut root_id = tree.get_root_id();
    for i in (0..100u64).filter(|i| i % 2 == 0) {
        let res = tree.delete_with_root(&k(i), root_id).expect("delete");
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
            size: 50,
        },
    )
    .expect("commit");

    let results: Vec<_> = tree
        .search_range(&k(0), None)
        .expect("range")
        .collect::<Result<Vec<_>, _>>()
        .expect("iterate");

    assert_eq!(results.len(), 50);
    for (offset, (key, _)) in results.iter().enumerate() {
        let expected = 1 + offset as u64 * 2; // odd keys: 1, 3, 5, ...
        assert_eq!(
            key.as_slice(),
            &k(expected),
            "expected odd key at offset {offset}"
        );
    }
}

#[test]
fn range_iter_respects_snapshot_isolation() {
    let dir = TempDir::new().unwrap();
    let tree = populate_and_commit(&dir, 8, 50);

    // Start a range scan (pins epoch).
    let mut iter = tree.search_range(&k(0), None).expect("range");

    // Read the first entry.
    let first = iter.next().unwrap().expect("first entry");
    assert_eq!(first.0.as_slice(), &k(0));

    // Insert more data and commit while the iterator is alive.
    let mut root_id = tree.get_root_id();
    for i in 50..60u64 {
        let res = tree
            .insert_with_root(k(i), v_bytes(i), root_id)
            .expect("insert");
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
            size: 60,
        },
    )
    .expect("commit");

    // Drain the rest of the iterator — it should still see the original 50 keys,
    // not the 10 new ones (snapshot isolation via epoch pinning).
    let rest: Vec<_> = iter.collect::<Result<Vec<_>, _>>().expect("iterate");
    // first + rest should be exactly 50.
    assert_eq!(
        1 + rest.len(),
        50,
        "iterator should see snapshot of 50 keys, not 60"
    );
}
