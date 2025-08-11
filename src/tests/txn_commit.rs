use crate::bplustree::tree::{SharedBPlusTree, BPlusTree, BaseVersion, CommitError};
use crate::bplustree::tree::StagedMetadata;
use crate::bplustree::transaction::WriteTransaction;
use crate::storage::{KeyCodec, ValueCodec};
use crate::tests::common;

use anyhow::Result;
use tempfile::TempDir;
use std::fmt::Debug;
use rand::seq::SliceRandom;
use rand::thread_rng;
use rand::Rng;

#[test]
fn commit_happy_path() {
    let dir = TempDir::new().unwrap();
    let order = 16;
    let tree = common::make_tree(&dir, order).expect("create tree");
    let mut trx = WriteTransaction::new(tree.clone());

    for i in 0..100 {
        trx.insert(i,  format!("value_{}", i)).expect("insert");
    }

    trx.commit().expect("commit");

    let _root_id = tree.get_root_id();
    for i in 0..100 {
        assert_eq!(tree.search(&i).expect("get"), Some(format!("value_{}", i)));
    }
}

//#[test]
//fn commit_with_retries() {
//    let dir = TempDir::new().unwrap();
//    let order = 16;
//    let tree = common::make_tree(&dir, order).expect("create tree");
//    let mut trx = WriteTransaction::new(tree.clone());
//
//    for i in 0..100 {
//        trx.insert(i, format!("value_{}", i)).expect("insert");
//    }
//
//    // Simulate a failure on the first commit attempt
//    trx.fail_commit = true;
//
//    // Retry logic
//    let mut retries = 0;
//    while retries < MAX_COMMIT_RETRIES {
//        match trx.commit() {
//            Ok(_) => break,
//            Err(e) => {
//                if retries == MAX_COMMIT_RETRIES - 1 {
//                    panic!("Failed to commit after {} retries: {}", retries + 1, e);
//                }
//                retries += 1;
//            }
//        }
//    }
//
//    for i in 0..100 {
//        assert_eq!(tree.search(&i).expect("get"), Some(format!("value_{}", i)));
//    }
//}
