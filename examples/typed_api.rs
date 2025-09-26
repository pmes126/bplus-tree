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
    txn.commit(db.get_inner())?;

    assert_eq!(db.get(&k1)?, Some(v1));
    assert_eq!(db.get(&k2)?, Some(v2));

    let rows = db.scan_range(&k1, &k2)?.unwrap();
    for res in rows {
        let (k, v) = res?;
        println!("{} {}", k, v);
    }

    Ok(())
}

//use crate::api::{
//  encoding::{KeyEncodingId, KeyConstraints},
//  typed::TypedClientGeneric as Client,
//  inproc::{InprocRaw, BytesKv},
//};
//
//// 1) Spin up your bytes B+-tree (engine) however you already do it
//let engine = Arc::new(MyBytesTree::open("bplustree.db")?); // implement BytesKv for this type
//
//// 2) Register it in the in-proc transport with pinned encoding metadata
//let raw = InprocRaw::new()
//    .register(
//        "users",
//        KeyEncodingId::BeU64,
//        KeyConstraints { fixed_key_len: true, key_len: 8, max_key_len: 8 },
//        engine,
//    );
//
//// 3) Build a typed client over the in-proc transport
//let db = Client::with_raw(raw);
//
//// 4) Bind with types matching the tree’s encoding
//let users = db.bind::<u64, String>("users").await?;
//
//// 5) Use it like normal
//users.put(2u64, "SomeString".to_owned()).await?;
//let v = users.get(2u64).await?;

