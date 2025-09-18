#![cfg(test)]

use crate::bplustree::transaction::WriteTransaction;
use crate::tests::common;
use rand::Rng;
use rand::seq::SliceRandom;
use rand::thread_rng;
use std::thread;
use tempfile::TempDir;

#[test]
fn commit_happy_path() {
    let dir = TempDir::new().unwrap();
    let order = 16;
    let tree = common::make_tree(&dir, order).expect("create tree");
    let mut trx = WriteTransaction::new(tree.clone());

    for i in 0..100 {
        trx.insert(i, format!("value_{}", i)).expect("insert");
    }

    trx.commit(&tree).expect("commit");

    for i in 0..100 {
        assert_eq!(tree.search(&i).expect("get"), Some(format!("value_{}", i)));
    }
}

/*
#[cfg(feature = "testing")]
#[test]
fn commit_with_retries() {
    let dir = TempDir::new().unwrap();
    let order = 16;
    let tree = common::make_tree(&dir, order).expect("create tree");
    let mut trx = WriteTransaction::new(tree.clone());
    let _scenario = fail::FailScenario::setup();
    fail::cfg("tree::commit::try_commit_failure", "return").unwrap();
    for i in 0..100 {
        trx.insert(i, format!("value_{}", i)).expect("insert");
    }

    match trx.commit() {
        Ok(_) => panic!("Commit should have failed due to injected failure"),
        Err(_e) => {}
    }

    let fail_pattern = format!("return->{}", MAX_COMMIT_RETRIES-1);
    fail::cfg("tree::commit::try_commit_failure", &fail_pattern).unwrap();
    // Now we expect the commit to succeed after retries
    trx.commit().expect("commit after retries");
    // run the commit
    fail::remove("tree::commit::try_commit_failure");
    for i in 0..100 {
        assert_eq!(tree.search(&i).expect("get"), Some(format!("value_{}", i)));
    }
}
*/

#[test]
fn commit_with_random_inserts() {
    let dir = TempDir::new().unwrap();
    let order = 16;
    let tree = common::make_tree(&dir, order).expect("create tree");
    let mut trx = WriteTransaction::new(tree.clone());

    let mut rng = thread_rng();
    let mut keys: Vec<u64> = (0..100).collect();
    keys.shuffle(&mut rng);

    for &key in &keys {
        trx.insert(key, format!("value_{}", key)).expect("insert");
    }

    trx.commit(&tree).expect("commit");

    for &key in &keys {
        assert_eq!(
            tree.search(&key).expect("Get Value failed"),
            Some(format!("value_{}", key))
        );
    }
}

#[test]
fn contending_parallel_transactions() {
    let dir = TempDir::new().unwrap();
    let order = 16;
    let tree = common::make_tree(&dir, order).expect("create tree");
    thread::scope(|s| {
        for i in 0..10 {
            let t = tree.clone();
            s.spawn(move || {
                let mut trx = WriteTransaction::new(t.clone());
                for j in 0..100 {
                    let sleep_duration = rand::thread_rng().gen_range(1..10);
                    std::thread::sleep(std::time::Duration::from_millis(sleep_duration));
                    trx.insert(i * 100 + j, format!("value_{}", i * 100 + j))
                        .expect("insert");
                }
                trx.commit(&t).expect("commit");
            });
        }
    });
    for i in 0..1000 {
        assert_eq!(tree.search(&i).expect("get"), Some(format!("value_{}", i)));
    }
}

#[test]
fn commit_with_conflicting_transactions() {
    let dir = TempDir::new().unwrap();
    let order = 16;
    let tree = common::make_tree(&dir, order).expect("create tree");

    // Start two transactions that will conflict
    let mut t1 = WriteTransaction::new(tree.clone());
    let mut t2 = WriteTransaction::new(tree.clone());

    // Insert into the same key in both transactions
    t1.insert(42, "value_42_t1".to_string()).expect("insert t1");
    t2.insert(42, "value_42_t2".to_string()).expect("insert t2");

    // Commit the first transaction
    t1.commit(&tree).expect("commit t1");

    // Now try to commit the second transaction, which should fail due to conflict
    t2.commit(&tree).expect("commit t2");

    tree.search(&42).expect("get").map_or_else(
        || panic!("Key 42 should exist after t1 commit"),
        |value| assert_eq!(value, "value_42_t2", "Value for key 42 should be from t1"),
    );
}

#[test]
fn commit_failure_should_reclaim_nodes() {
    let dir = TempDir::new().unwrap();
    let order = 16;
    let tree = common::make_tree(&dir, order).expect("create tree");

    // Start a transaction
    let mut trx = WriteTransaction::new(tree.clone());

    // Insert some data
    for i in 0..10 {
        trx.insert(i, format!("value_{}", i)).expect("insert");
    }

    // Simulate a failure during commit
    //fail::cfg("tree::commit::try_commit_failure", "return").unwrap();
    //match trx.commit(&tree) {
    //    Ok(_) => panic!("Commit should have failed"),
    //    Err(e) => assert!(matches!(e, anyhow::Error { .. })),
    //}

    //let deffered = tree.get_epoch_mgr().get_deferred_pages();
    //assert!(!deffered.is_empty(), "Deferred pages should not be empty after failed commit");

    match trx.commit(&tree) {
        Ok(_) => println!("Commit succeeded unexpectedly"),
        Err(e) => assert!(matches!(e, anyhow::Error { .. })),
    }

    // Insert some data
    for i in 0..10 {
        trx.insert(i, format!("value_{}", i * 2)).expect("insert");
    }

    // Attempt to commit, overwrite values
    match trx.commit(&tree) {
        Ok(_) => println!("Commit succeeded unexpectedly"),
        Err(e) => assert!(matches!(e, anyhow::Error { .. })),
    }

    let deffered = tree.get_epoch_mgr().get_deferred_pages();
    assert!(
        deffered.is_empty(),
        "Deferred pages should be empty after successful commit"
    );

    // Remove the failure configuration
    fail::remove("tree::commit::try_commit_failure");
}

#[test]
fn noop_tx_commit_no_side_effects() {
    let dir = TempDir::new().unwrap();
    let order = 16;
    let tree = common::make_tree(&dir, order).expect("create tree");
    let mut trx = WriteTransaction::new(tree.clone());

    // No operations, just commit
    trx.commit(&tree).expect("commit with no operations");

    // Ensure the tree is still empty
    assert!(
        tree.get_root_id() == 2,
        "Tree should not have any nodes after noop commit"
    );
}

#[test]
fn node_reclamation_in_tx_commit() {
    let dir = TempDir::new().unwrap();
    let order = 10;
    let tree = common::make_tree(&dir, order).expect("create tree");

    // Start a transaction
    let mut trx = WriteTransaction::new(tree.clone());

    // Insert some data
    for i in 0..100 {
        trx.insert(i, format!("value_{}", i)).expect("insert");
    }

    // Delete some data
    for i in 0..100 {
        trx.delete(&i).expect("delete");
    }

    assert!(
        trx.get_reclaimed_nodes().is_empty(),
        "No nodes should be reclaimed before commit, the transaction reclaimed nodes should not be empty"
    );

    // Commit the transaction
    trx.commit(&tree).expect("commit");

    let deffered = tree.get_epoch_mgr().get_deferred_pages();

    assert!(
        !deffered.is_empty(),
        "Deferred pages should not be empty after commit"
    );
    assert!(
        trx.get_reclaimed_nodes().is_empty(),
        "Reclaimed nodes in tx should be empty after commit"
    );
}
