//! Example: bytes-level API usage
use bplustree::api::DbBuilder;
use bplustree::storage::{file_store::FileStore, page_store::PageStore};

fn main() -> anyhow::Result<()> {
    // Real app should pass a persistent file path
    let db_path = std::env::temp_dir().join(format!("bplustree-{}.db", std::process::id()));

    // Storage backend
    let store = FileStore::<PageStore>::new(&db_path)?;

    let db = DbBuilder::new(store).order(64).build_bytes()?;

    db.put(b"alpha", b"1")?;
    db.put(b"beta", b"2")?;

    //assert_eq!(db.get(b"alpha")?, Some(b"1".to_vec()));

    let rows = db.scan_range_collect(b"a", b"c", 100)?.unwrap();
    for (k, v) in rows {
        println!(
            "{} -> {}",
            String::from_utf8_lossy(&k),
            String::from_utf8_lossy(&v)
        );
    }

    Ok(())
}
