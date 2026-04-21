//! Typed embedded API example: `u64` keys and `String` values, with a batched
//! write transaction.

use bplus_tree::api::Db;

fn main() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let db = Db::open(dir.path())?;
    let tree = db.create_tree::<u64, String>("users", 64)?;

    let mut txn = tree.txn();
    txn.insert(&1u64, &"first".to_string());
    txn.insert(&2u64, &"second".to_string());
    txn.commit()?;

    assert_eq!(tree.get(&1u64)?.as_deref(), Some("first"));
    assert_eq!(tree.get(&2u64)?.as_deref(), Some("second"));

    println!("size = {}", tree.len());
    Ok(())
}
