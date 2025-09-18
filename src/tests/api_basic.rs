use crate::api::{DbBytes, WriteTxnBytes};
use crate::storage::{file_store::FileStore, page_store::PageStore};

#[test]
fn api_crud_and_scan() {
    let path = std::env::temp_dir().join(format!("bpt-api-{}.db", std::process::id()));
    let store = FileStore::<PageStore>::new(&path).unwrap();
    let db = DbBytes::new(store, 64).unwrap();

    // CRUD
    db.put(b"a1", b"v1").unwrap();
    db.put(b"b2", b"v2").unwrap();
    assert_eq!(db.get(b"a1").unwrap(), Some(b"v1".to_vec()));
    assert_eq!(db.get(b"missing").unwrap(), None);
    db.delete(b"a1").unwrap();
    assert_eq!(db.get(b"a1").unwrap(), None);
    db.delete(b"b2").unwrap();
    assert_eq!(db.get(b"b2").unwrap(), None);

    // Scan
    db.put(b"a2", b"v3").unwrap();
    db.put(b"c3", b"v4").unwrap();
    let res = db.get(b"a2").unwrap();
    assert_eq!(res, Some(b"v3".to_vec()));
    let res = db.get(b"c3").unwrap();
    assert_eq!(res, Some(b"v4".to_vec()));

    let mut rows = db.scan_range(b"a", b"d").unwrap().unwrap();
    let first = rows.next().unwrap();
    
    assert!(first.is_ok());
    let first = first.unwrap();
    assert!(first.0 == b"a2".to_vec() && first.1 == b"v3".to_vec());
    
    let second = rows.next().unwrap();

    assert!(second.is_ok());
    let second = second.unwrap();
    assert_eq!(second.0, b"c3");
    assert_eq!(second.1, b"v4");
}

#[test]
fn api_write_txn_batch_commit() {
    let path = std::env::temp_dir().join(format!("bpt-api-txn-{}.db", std::process::id()));
    let store = FileStore::<PageStore>::new(&path).unwrap();
    let db = DbBytes::new(store, 64).unwrap();

    // Begin txn and do multiple ops
    let mut w: WriteTxnBytes = db.begin_write().unwrap();
    w.insert(b"k1".to_vec(), b"v1".to_vec()).unwrap();
    w.insert(b"k2".to_vec(), b"v2".to_vec()).unwrap();
    w.delete(&b"k1".to_vec()).unwrap();
    // Read within txn (k2 visible via staged root)
    w.commit(db.get_inner()).unwrap();

    assert_eq!(db.get(b"k1").unwrap(), None);
    assert_eq!(db.get(b"k2").unwrap(), Some(b"v2".to_vec()));
}
