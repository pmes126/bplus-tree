//! Bytes-level embedded API example.
//!
//! Opens a database, creates a raw-bytes tree, inserts entries, reads them back.

use bplustree::api::Db;

fn main() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let db = Db::open(dir.path())?;
    let tree = db.create_tree::<Vec<u8>, Vec<u8>>("data", 64)?;

    tree.put(&b"alpha".to_vec(), &b"1".to_vec())?;
    tree.put(&b"beta".to_vec(), &b"2".to_vec())?;

    let alpha = tree.get(&b"alpha".to_vec())?;
    let beta = tree.get(&b"beta".to_vec())?;
    println!(
        "alpha -> {}",
        alpha
            .as_deref()
            .map(String::from_utf8_lossy)
            .unwrap_or_default()
    );
    println!(
        "beta  -> {}",
        beta.as_deref()
            .map(String::from_utf8_lossy)
            .unwrap_or_default()
    );

    Ok(())
}
