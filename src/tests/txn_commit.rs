#![cfg(test)]

use crate::bplustree::transaction::WriteTransaction;
use crate::tests::common;
use rand::seq::SliceRandom;
use rand::thread_rng;
use std::thread;
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// Key/value encoding helpers
// ---------------------------------------------------------------------------

/// Encodes a `u64` as a big-endian byte array for use as a tree key.
fn k(i: u64) -> [u8; 8] {
    i.to_be_bytes()
}

/// Returns the value string for key `i`.
fn v_str(i: u64) -> String {
    format!("value_{}", i)
}

/// Returns the value for key `i` as raw bytes.
fn v_bytes(i: u64) -> Vec<u8> {
    v_str(i).into_bytes()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[test]
fn commit_happy_path() {
    let dir = TempDir::new().unwrap();
    let order = 16;
    let tree = common::make_tree(&dir, order).expect("create tree");
    let mut trx = WriteTransaction::new(tree.clone());

    for i in 0..100u64 {
        trx.insert(k(i), v_bytes(i));
    }

    trx.commit(&tree).expect("commit");

    for i in 0..100u64 {
        assert_eq!(tree.search(k(i)).expect("get"), Some(v_bytes(i)));
    }
}

#[test]
fn commit_with_random_inserts() {
    let dir = TempDir::new().unwrap();
    let order = 16;
    let tree = common::make_tree(&dir, order).expect("create tree");
    let mut trx = WriteTransaction::new(tree.clone());

    let mut keys: Vec<u64> = (0..100).collect();
    keys.shuffle(&mut thread_rng());

    for &key in &keys {
        trx.insert(k(key), v_bytes(key));
    }

    trx.commit(&tree).expect("commit");

    for &key in &keys {
        assert_eq!(tree.search(k(key)).expect("get"), Some(v_bytes(key)));
    }
}

#[test]
fn contending_parallel_transactions() {
    let dir = TempDir::new().unwrap();
    let order = 16;
    let tree = common::make_tree(&dir, order).expect("create tree");
    thread::scope(|s| {
        for i in 0..10u64 {
            let t = tree.clone();
            s.spawn(move || {
                let mut trx = WriteTransaction::new(t.clone());
                for j in 0..100u64 {
                    trx.insert(k(i * 100 + j), v_bytes(i * 100 + j));
                }
                trx.commit(&t).expect("commit");
            });
        }
    });
    for i in 0..1000u64 {
        assert_eq!(tree.search(k(i)).expect("get"), Some(v_bytes(i)));
    }
}

#[test]
fn commit_with_conflicting_transactions() {
    let dir = TempDir::new().unwrap();
    let order = 16;
    let tree = common::make_tree(&dir, order).expect("create tree");

    let mut t1 = WriteTransaction::new(tree.clone());
    let mut t2 = WriteTransaction::new(tree.clone());

    t1.insert(k(42), b"value_42_t1");
    t2.insert(k(42), b"value_42_t2");

    t1.commit(&tree).expect("commit t1");
    // t2 will rebase on top of t1's commit and win.
    t2.commit(&tree).expect("commit t2");

    tree.search(k(42)).expect("get").map_or_else(
        || panic!("Key 42 should exist after both commits"),
        |value| assert_eq!(value, b"value_42_t2", "Last writer (t2) should win"),
    );
}

#[test]
fn commit_failure_should_reclaim_nodes() {
    let dir = TempDir::new().unwrap();
    let order = 16;
    let tree = common::make_tree(&dir, order).expect("create tree");

    let mut trx = WriteTransaction::new(tree.clone());
    for i in 0..10u64 {
        trx.insert(k(i), v_bytes(i));
    }
    trx.commit(&tree).expect("commit");

    // Overwrite all values in a second transaction.
    let mut trx2 = WriteTransaction::new(tree.clone());
    for i in 0..10u64 {
        trx2.insert(k(i), v_bytes(i * 2));
    }
    trx2.commit(&tree).expect("commit overwrite");

    let deferred = tree.epoch_mgr().get_deferred_pages();
    assert!(
        deferred.is_empty(),
        "Deferred pages should be empty after successful commit"
    );
}

#[test]
fn noop_tx_commit_no_side_effects() {
    let dir = TempDir::new().unwrap();
    let order = 16;
    let tree = common::make_tree(&dir, order).expect("create tree");
    let root_before = tree.get_root_id();
    let mut trx = WriteTransaction::new(tree.clone());

    trx.commit(&tree).expect("commit with no operations");

    assert_eq!(
        tree.get_root_id(),
        root_before,
        "Root should be unchanged after a no-op commit"
    );
}
