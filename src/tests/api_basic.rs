// TODO: these tests target a high-level bytes API (DbBytes, WriteTxnBytes, DbClient) and a
// network client (DbClient) that have not yet been implemented. They are gated until those
// APIs exist.

#[cfg(feature = "api_v1")]
mod api_v1 {
    use crate::api::{DbBytes, WriteTxnBytes};
    use crate::storage::{
        file_page_storage::FilePageStorage, paged_node_storage::PagedNodeStorage,
    };

    #[test]
    fn api_crud_and_scan() {
        let path = std::env::temp_dir().join(format!("bpt-api-{}.db", std::process::id()));
        let store =
            PagedNodeStorage::<FilePageStorage>::new(&path, &path.with_extension("manifest"))
                .unwrap();
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
        let store =
            PagedNodeStorage::<FilePageStorage>::new(&path, &path.with_extension("manifest"))
                .unwrap();
        let db = DbBytes::new(store, 64).unwrap();

        let mut w: WriteTxnBytes = db.begin_write().unwrap();
        w.insert(b"k1".to_vec(), b"v1".to_vec()).unwrap();
        w.insert(b"k2".to_vec(), b"v2".to_vec()).unwrap();
        w.delete(&b"k1".to_vec()).unwrap();
        w.commit(db.get_inner()).unwrap();

        assert_eq!(db.get(b"k1").unwrap(), None);
        assert_eq!(db.get(b"k2").unwrap(), Some(b"v2".to_vec()));
    }

    #[tokio::test]
    async fn smoke_put_get_u64_utf8() {
        use crate::api::{DbClient, KeyConstraints, KeyEncodingId};
        let db = DbClient::connect("http://127.0.0.1:50051").await.unwrap();
        let users = db.bind::<u64, String>("users").await.unwrap();
        users.put(42u64, "val".to_string()).await.unwrap();
        let got = users.get(42u64).await.unwrap();
        assert_eq!(got.as_deref(), Some("val"));
    }
}
