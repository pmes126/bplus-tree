//! Edge-case tests for WriteTransaction: mixed ops, overwrites, retry exhaustion.

use crate::bplustree::transaction::WriteTransaction;
use crate::tests::common;
use tempfile::TempDir;

fn k(i: u64) -> [u8; 8] {
    i.to_be_bytes()
}

fn v_bytes(i: u64) -> Vec<u8> {
    format!("value_{}", i).into_bytes()
}

// ---------------------------------------------------------------------------
// Mixed insert + delete within a single transaction
// ---------------------------------------------------------------------------

#[test]
fn txn_insert_then_delete_same_key() {
    let dir = TempDir::new().unwrap();
    let tree = common::make_tree(&dir, 16).expect("create tree");

    let mut txn = WriteTransaction::new(tree.clone());
    txn.insert(k(1), v_bytes(1));
    txn.delete(k(1));
    txn.commit(&tree).expect("commit");

    assert!(
        tree.search(k(1)).unwrap().is_none(),
        "key inserted then deleted in same txn should be absent"
    );
}

#[test]
fn txn_delete_then_reinsert_same_key() {
    let dir = TempDir::new().unwrap();
    let tree = common::make_tree(&dir, 16).expect("create tree");

    // Seed the tree.
    let mut seed = WriteTransaction::new(tree.clone());
    seed.insert(k(1), b"original");
    seed.commit(&tree).expect("seed commit");

    // Delete then re-insert.
    let mut txn = WriteTransaction::new(tree.clone());
    txn.delete(k(1));
    txn.insert(k(1), b"replaced");
    txn.commit(&tree).expect("commit");

    assert_eq!(
        tree.search(k(1)).unwrap(),
        Some(b"replaced".to_vec()),
        "re-inserted value should be visible"
    );
}

#[test]
fn txn_multiple_overwrites_of_same_key() {
    let dir = TempDir::new().unwrap();
    let tree = common::make_tree(&dir, 16).expect("create tree");

    let mut txn = WriteTransaction::new(tree.clone());
    txn.insert(k(1), b"first");
    txn.insert(k(1), b"second");
    txn.insert(k(1), b"third");
    txn.commit(&tree).expect("commit");

    assert_eq!(
        tree.search(k(1)).unwrap(),
        Some(b"third".to_vec()),
        "last write in batch should win"
    );
}

// ---------------------------------------------------------------------------
// Large batch transaction
// ---------------------------------------------------------------------------

#[test]
fn txn_large_batch() {
    let dir = TempDir::new().unwrap();
    let tree = common::make_tree(&dir, 8).expect("create tree");
    let n = 500u64;

    let mut txn = WriteTransaction::new(tree.clone());
    for i in 0..n {
        txn.insert(k(i), v_bytes(i));
    }
    txn.commit(&tree).expect("commit");

    for i in 0..n {
        assert_eq!(tree.search(k(i)).unwrap(), Some(v_bytes(i)));
    }
}

#[test]
fn txn_large_batch_delete() {
    let dir = TempDir::new().unwrap();
    let tree = common::make_tree(&dir, 8).expect("create tree");
    let n = 200u64;

    // Seed.
    let mut seed = WriteTransaction::new(tree.clone());
    for i in 0..n {
        seed.insert(k(i), v_bytes(i));
    }
    seed.commit(&tree).expect("seed commit");

    // Delete all in one batch.
    let mut txn = WriteTransaction::new(tree.clone());
    for i in 0..n {
        txn.delete(k(i));
    }
    txn.commit(&tree).expect("delete commit");

    for i in 0..n {
        assert!(tree.search(k(i)).unwrap().is_none());
    }
}

// ---------------------------------------------------------------------------
// Interleaved insert and delete across disjoint keys
// ---------------------------------------------------------------------------

#[test]
fn txn_interleaved_ops_on_disjoint_keys() {
    let dir = TempDir::new().unwrap();
    let tree = common::make_tree(&dir, 16).expect("create tree");

    // Seed even keys.
    let mut seed = WriteTransaction::new(tree.clone());
    for i in (0..20u64).filter(|i| i % 2 == 0) {
        seed.insert(k(i), v_bytes(i));
    }
    seed.commit(&tree).expect("seed commit");

    // In one txn: delete even keys, insert odd keys.
    let mut txn = WriteTransaction::new(tree.clone());
    for i in (0..20u64).filter(|i| i % 2 == 0) {
        txn.delete(k(i));
    }
    for i in (0..20u64).filter(|i| i % 2 != 0) {
        txn.insert(k(i), v_bytes(i));
    }
    txn.commit(&tree).expect("commit");

    for i in 0..20u64 {
        if i % 2 == 0 {
            assert!(
                tree.search(k(i)).unwrap().is_none(),
                "even key {i} should be deleted"
            );
        } else {
            assert_eq!(
                tree.search(k(i)).unwrap(),
                Some(v_bytes(i)),
                "odd key {i} should be present"
            );
        }
    }
}
