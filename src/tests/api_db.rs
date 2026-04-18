//! Tests for the public embedded API: Db, Tree<K,V>, WriteTxn, RangeIter.

use crate::api::Db;
use crate::database::manifest::reader::ManifestReader;
use crate::database::manifest::writer::ManifestWriter;
use crate::database::manifest::{ManifestRec, TAG_DELETE_TREE};
use crate::database::{self, DatabaseError};
use crate::storage::file_page_storage::FilePageStorage;
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// Db lifecycle
// ---------------------------------------------------------------------------

#[test]
fn open_creates_database_in_empty_dir() {
    let dir = TempDir::new().unwrap();
    let db = Db::open(dir.path());
    assert!(db.is_ok(), "Db::open should succeed on an empty directory");
}

#[test]
fn create_tree_returns_handle() {
    let dir = TempDir::new().unwrap();
    let db = Db::open(dir.path()).unwrap();
    let tree = db.create_tree::<u64, String>("test", 64);
    assert!(tree.is_ok(), "create_tree should return a Tree handle");
}

#[test]
fn open_tree_after_create() {
    let dir = TempDir::new().unwrap();
    let db = Db::open(dir.path()).unwrap();
    db.create_tree::<u64, String>("my_tree", 32).unwrap();
    let reopened = db.open_tree::<u64, String>("my_tree");
    assert!(
        reopened.is_ok(),
        "open_tree should find a previously created tree"
    );
}

#[test]
fn open_tree_missing_returns_error() {
    let dir = TempDir::new().unwrap();
    let db = Db::open(dir.path()).unwrap();
    let result = db.open_tree::<u64, String>("nonexistent");
    assert!(result.is_err(), "open_tree on missing tree should fail");
}

#[test]
fn tree_open_or_create_creates_when_absent() {
    let dir = TempDir::new().unwrap();
    let db = Db::open(dir.path()).unwrap();
    let tree = db.tree::<u64, String>("auto", 64);
    assert!(tree.is_ok(), "tree() should create when absent");
}

#[test]
fn tree_open_or_create_opens_when_present() {
    let dir = TempDir::new().unwrap();
    let db = Db::open(dir.path()).unwrap();
    let _t1 = db.create_tree::<u64, String>("reuse", 64).unwrap();

    // tree() should succeed (open existing), not create a duplicate.
    let t2 = db.tree::<u64, String>("reuse", 64);
    assert!(
        t2.is_ok(),
        "tree() should open an existing tree without error"
    );
}

// ---------------------------------------------------------------------------
// Tree<K,V> typed CRUD
// ---------------------------------------------------------------------------

#[test]
fn put_and_get_u64_string() {
    let dir = TempDir::new().unwrap();
    let db = Db::open(dir.path()).unwrap();
    let tree = db.create_tree::<u64, String>("t", 64).unwrap();

    tree.put(&42, &"answer".to_string()).unwrap();
    assert_eq!(tree.get(&42).unwrap().as_deref(), Some("answer"));
}

#[test]
fn put_and_get_string_string() {
    let dir = TempDir::new().unwrap();
    let db = Db::open(dir.path()).unwrap();
    let tree = db.create_tree::<String, String>("t", 64).unwrap();

    tree.put(&"hello".to_string(), &"world".to_string())
        .unwrap();
    assert_eq!(
        tree.get(&"hello".to_string()).unwrap().as_deref(),
        Some("world")
    );
}

#[test]
fn put_and_get_bytes() {
    let dir = TempDir::new().unwrap();
    let db = Db::open(dir.path()).unwrap();
    let tree = db.create_tree::<Vec<u8>, Vec<u8>>("t", 64).unwrap();

    tree.put(&b"key".to_vec(), &b"val".to_vec()).unwrap();
    assert_eq!(tree.get(&b"key".to_vec()).unwrap(), Some(b"val".to_vec()));
}

#[test]
fn put_and_get_i64_key() {
    let dir = TempDir::new().unwrap();
    let db = Db::open(dir.path()).unwrap();
    let tree = db.create_tree::<i64, String>("t", 64).unwrap();

    tree.put(&-42, &"negative".to_string()).unwrap();
    tree.put(&0, &"zero".to_string()).unwrap();
    tree.put(&42, &"positive".to_string()).unwrap();

    assert_eq!(tree.get(&-42).unwrap().as_deref(), Some("negative"));
    assert_eq!(tree.get(&0).unwrap().as_deref(), Some("zero"));
    assert_eq!(tree.get(&42).unwrap().as_deref(), Some("positive"));
}

#[test]
fn get_missing_key_returns_none() {
    let dir = TempDir::new().unwrap();
    let db = Db::open(dir.path()).unwrap();
    let tree = db.create_tree::<u64, String>("t", 64).unwrap();
    assert_eq!(tree.get(&999).unwrap(), None);
}

#[test]
fn put_overwrites_existing_value() {
    let dir = TempDir::new().unwrap();
    let db = Db::open(dir.path()).unwrap();
    let tree = db.create_tree::<u64, String>("t", 64).unwrap();

    tree.put(&1, &"first".to_string()).unwrap();
    tree.put(&1, &"second".to_string()).unwrap();
    assert_eq!(tree.get(&1).unwrap().as_deref(), Some("second"));
}

#[test]
fn delete_removes_key() {
    let dir = TempDir::new().unwrap();
    let db = Db::open(dir.path()).unwrap();
    let tree = db.create_tree::<u64, String>("t", 64).unwrap();

    tree.put(&1, &"val".to_string()).unwrap();
    tree.delete(&1).unwrap();
    assert_eq!(tree.get(&1).unwrap(), None);
}

#[test]
fn delete_missing_key_returns_error() {
    let dir = TempDir::new().unwrap();
    let db = Db::open(dir.path()).unwrap();
    let tree = db.create_tree::<u64, String>("t", 64).unwrap();
    let result = tree.delete(&999);
    assert!(result.is_err(), "delete of non-existent key should fail");
}

#[test]
fn len_and_is_empty() {
    let dir = TempDir::new().unwrap();
    let db = Db::open(dir.path()).unwrap();
    let tree = db.create_tree::<u64, String>("t", 64).unwrap();

    assert!(tree.is_empty());
    assert_eq!(tree.len(), 0);

    tree.put(&1, &"a".to_string()).unwrap();
    assert!(!tree.is_empty());
}

// ---------------------------------------------------------------------------
// WriteTxn
// ---------------------------------------------------------------------------

#[test]
fn write_txn_batch_commit() {
    let dir = TempDir::new().unwrap();
    let db = Db::open(dir.path()).unwrap();
    let tree = db.create_tree::<u64, String>("t", 64).unwrap();

    let mut txn = tree.txn();
    txn.insert(&1, &"one".to_string());
    txn.insert(&2, &"two".to_string());
    txn.insert(&3, &"three".to_string());
    txn.commit().unwrap();

    assert_eq!(tree.get(&1).unwrap().as_deref(), Some("one"));
    assert_eq!(tree.get(&2).unwrap().as_deref(), Some("two"));
    assert_eq!(tree.get(&3).unwrap().as_deref(), Some("three"));
}

#[test]
fn write_txn_mixed_insert_delete() {
    let dir = TempDir::new().unwrap();
    let db = Db::open(dir.path()).unwrap();
    let tree = db.create_tree::<u64, String>("t", 64).unwrap();

    // Seed some data.
    tree.put(&1, &"one".to_string()).unwrap();
    tree.put(&2, &"two".to_string()).unwrap();

    // Transaction: insert 3, delete 1.
    let mut txn = tree.txn();
    txn.insert(&3, &"three".to_string());
    txn.delete(&1);
    txn.commit().unwrap();

    assert_eq!(tree.get(&1).unwrap(), None);
    assert_eq!(tree.get(&2).unwrap().as_deref(), Some("two"));
    assert_eq!(tree.get(&3).unwrap().as_deref(), Some("three"));
}

#[test]
fn write_txn_overwrite_same_key_in_batch() {
    let dir = TempDir::new().unwrap();
    let db = Db::open(dir.path()).unwrap();
    let tree = db.create_tree::<u64, String>("t", 64).unwrap();

    let mut txn = tree.txn();
    txn.insert(&1, &"first".to_string());
    txn.insert(&1, &"second".to_string());
    txn.commit().unwrap();

    assert_eq!(
        tree.get(&1).unwrap().as_deref(),
        Some("second"),
        "last write in batch should win"
    );
}

#[test]
fn write_txn_empty_commit_is_noop() {
    let dir = TempDir::new().unwrap();
    let db = Db::open(dir.path()).unwrap();
    let tree = db.create_tree::<u64, String>("t", 64).unwrap();
    tree.put(&1, &"one".to_string()).unwrap();

    let txn = tree.txn();
    txn.commit().unwrap();

    assert_eq!(tree.get(&1).unwrap().as_deref(), Some("one"));
}

// ---------------------------------------------------------------------------
// RangeIter through typed API
// ---------------------------------------------------------------------------

#[test]
fn range_typed_u64_string() {
    let dir = TempDir::new().unwrap();
    let db = Db::open(dir.path()).unwrap();
    let tree = db.create_tree::<u64, String>("t", 64).unwrap();

    for i in 0u64..20 {
        tree.put(&i, &format!("val_{i}")).unwrap();
    }

    let results: Vec<_> = tree
        .range(&5, &10)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(results.len(), 5);
    for (offset, (k, v)) in results.iter().enumerate() {
        let expected = 5 + offset as u64;
        assert_eq!(*k, expected);
        assert_eq!(v, &format!("val_{expected}"));
    }
}

#[test]
fn range_from_typed() {
    let dir = TempDir::new().unwrap();
    let db = Db::open(dir.path()).unwrap();
    let tree = db.create_tree::<u64, String>("t", 64).unwrap();

    for i in 0u64..10 {
        tree.put(&i, &format!("v{i}")).unwrap();
    }

    let results: Vec<_> = tree
        .range_from(&7)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(results.len(), 3);
    assert_eq!(results[0].0, 7);
    assert_eq!(results[2].0, 9);
}

#[test]
fn range_empty_result() {
    let dir = TempDir::new().unwrap();
    let db = Db::open(dir.path()).unwrap();
    let tree = db.create_tree::<u64, String>("t", 64).unwrap();

    for i in 0u64..10 {
        tree.put(&i, &format!("v{i}")).unwrap();
    }

    let results: Vec<_> = tree
        .range(&100, &200)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert!(results.is_empty());
}

// ---------------------------------------------------------------------------
// Multiple named trees
// ---------------------------------------------------------------------------

#[test]
fn multiple_named_trees_are_independent() {
    let dir = TempDir::new().unwrap();
    let db = Db::open(dir.path()).unwrap();
    let users = db.create_tree::<u64, String>("users", 64).unwrap();
    let events = db.create_tree::<u64, String>("events", 64).unwrap();

    users.put(&1, &"alice".to_string()).unwrap();
    events.put(&1, &"login".to_string()).unwrap();

    assert_eq!(users.get(&1).unwrap().as_deref(), Some("alice"));
    assert_eq!(events.get(&1).unwrap().as_deref(), Some("login"));

    // Keys don't leak across trees.
    users.delete(&1).unwrap();
    assert_eq!(users.get(&1).unwrap(), None);
    assert_eq!(
        events.get(&1).unwrap().as_deref(),
        Some("login"),
        "delete in one tree must not affect another"
    );
}

// ---------------------------------------------------------------------------
// Freelist persistence
// ---------------------------------------------------------------------------

#[test]
fn freelist_persists_across_close_and_reopen() {
    let dir = TempDir::new().unwrap();

    // Phase 1: create a tree, insert keys, delete some to generate freed pages.
    {
        let db = Db::open(dir.path()).unwrap();
        let tree = db.create_tree::<u64, String>("data", 8).unwrap();

        for i in 0u64..50 {
            tree.put(&i, &format!("value-{i}")).unwrap();
        }
        // Delete half the keys to free pages.
        for i in 0u64..25 {
            tree.delete(&i).unwrap();
        }

        // Close checkpoints the freelist.
        unsafe { db.close() };
    }

    // The snapshot file should exist.
    assert!(
        dir.path().join("freelist.snapshot").exists(),
        "freelist.snapshot should be written on close"
    );

    // Phase 2: reopen — the freed pages should be restored and reused.
    {
        let db = Db::open(dir.path()).unwrap();
        let tree = db.open_tree::<u64, String>("data").unwrap();

        // Surviving keys are still accessible.
        for i in 25u64..50 {
            assert_eq!(
                tree.get(&i).unwrap().as_deref(),
                Some(format!("value-{i}").as_str()),
                "key {i} should survive close/reopen"
            );
        }

        // Insert new keys — these should reuse freed page IDs rather than
        // growing the file. We can't easily assert on page IDs from the API,
        // but we verify correctness: the inserts succeed and reads work.
        for i in 100u64..150 {
            tree.put(&i, &format!("new-{i}")).unwrap();
        }
        for i in 100u64..150 {
            assert_eq!(
                tree.get(&i).unwrap().as_deref(),
                Some(format!("new-{i}").as_str()),
            );
        }

        unsafe { db.close() };
    }
}

// ---------------------------------------------------------------------------
// File locking
// ---------------------------------------------------------------------------

#[test]
fn concurrent_open_returns_locked_error() {
    let dir = TempDir::new().unwrap();

    // First open succeeds.
    let _db1 = database::open::<FilePageStorage, _>(dir.path()).expect("first open should succeed");

    // Second open on the same directory should fail with Locked.
    match database::open::<FilePageStorage, _>(dir.path()) {
        Err(DatabaseError::Locked) => {} // expected
        Err(e) => panic!("expected DatabaseError::Locked, got: {e:?}"),
        Ok(_) => panic!("second open should have failed with Locked"),
    }
}

#[test]
fn lock_released_after_drop() {
    let dir = TempDir::new().unwrap();

    {
        let _db =
            database::open::<FilePageStorage, _>(dir.path()).expect("first open should succeed");
        // _db dropped here, releasing the lock.
    }

    // Re-opening should succeed after the previous Database was dropped.
    let _db2 = database::open::<FilePageStorage, _>(dir.path())
        .expect("re-open after drop should succeed");
}

// ---------------------------------------------------------------------------
// Manifest CRC framing
// ---------------------------------------------------------------------------

#[test]
fn manifest_roundtrip_with_crc() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("manifest.log");

    // Write two records.
    {
        let mut w = ManifestWriter::open(&path, 0).unwrap();
        w.append(ManifestRec::DeleteTree { seq: 0, id: 42 })
            .unwrap();
        w.append(ManifestRec::DeleteTree { seq: 0, id: 99 })
            .unwrap();
        w.fsync().unwrap();
    }

    // Read them back.
    let mut r = ManifestReader::open(&path).unwrap();
    let rec1 = r.read_next().unwrap().expect("should read first record");
    let rec2 = r.read_next().unwrap().expect("should read second record");
    assert!(r.read_next().unwrap().is_none(), "no more records");

    match rec1 {
        ManifestRec::DeleteTree { id, .. } => assert_eq!(id, 42),
        other => panic!("unexpected record: {other:?}"),
    }
    match rec2 {
        ManifestRec::DeleteTree { id, .. } => assert_eq!(id, 99),
        other => panic!("unexpected record: {other:?}"),
    }
}

#[test]
fn manifest_truncated_record_returns_none() {
    use std::io::Write;

    let dir = TempDir::new().unwrap();
    let path = dir.path().join("manifest.log");

    // Write a valid record, then append a partial (truncated) one.
    {
        let mut w = ManifestWriter::open(&path, 0).unwrap();
        w.append(ManifestRec::DeleteTree { seq: 0, id: 1 }).unwrap();
        w.fsync().unwrap();
    }
    // Append a few garbage bytes to simulate a crash mid-write.
    {
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap();
        f.write_all(&[TAG_DELETE_TREE, 0x10, 0x00, 0x00, 0x00])
            .unwrap(); // tag + bogus length
    }

    let mut r = ManifestReader::open(&path).unwrap();
    assert!(
        r.read_next().unwrap().is_some(),
        "first record should be valid"
    );
    // The truncated trailing record should be treated as end-of-valid-data.
    assert!(r.read_next().unwrap().is_none(), "truncated record → None");
}

#[test]
fn manifest_corrupted_crc_returns_error() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("manifest.log");

    // Write one valid record.
    {
        let mut w = ManifestWriter::open(&path, 0).unwrap();
        w.append(ManifestRec::DeleteTree { seq: 0, id: 7 }).unwrap();
        w.fsync().unwrap();
    }

    // Corrupt the last 4 bytes (the CRC) by flipping bits.
    {
        let data = std::fs::read(&path).unwrap();
        let mut corrupted = data;
        let len = corrupted.len();
        corrupted[len - 1] ^= 0xFF;
        std::fs::write(&path, &corrupted).unwrap();
    }

    let mut r = ManifestReader::open(&path).unwrap();
    let err = r.read_next().expect_err("corrupted CRC should be an error");
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    assert!(
        err.to_string().contains("CRC mismatch"),
        "error message should mention CRC: {err}"
    );
}
