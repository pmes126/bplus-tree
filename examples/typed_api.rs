//! Example: bytes-level API usage
use bplustree::api::DbBuilder;
use bplustree::storage::{file_store::FileStore, page_store::PageStore};

fn main() -> anyhow::Result<()> {
    // Real app should pass a persistent file path
    let db_path = std::env::temp_dir().join(format!("bplustree-{}.db", std::process::id()));

    // Storage backend
    let store = FileStore::<PageStore>::new(&db_path)?;

    let db = DbBuilder::new(store)
        .order(64)
        .build_typed::<u64, String>()?;

    let k1 = 1u64;
    let v1 = "Some String value".to_string();
    let k2 = 2u64;
    let v2 = "Some Other String value".to_string();

    let mut txn = db.begin_write()?;
    txn.insert(k1, v1.clone())?;
    txn.insert(k2, v2.clone())?;
    txn.commit()?;

    assert_eq!(db.get(&k1)?, Some(v1));
    assert_eq!(db.get(&k2)?, Some(v2));

    let rows = db.scan_range(&k1, &k2)?.unwrap();
    for res in rows {
        let (k, v) = res?;
        println!("{} {}", k, v);
    }

    Ok(())
}
