use self::storage::{file_store::FileStore, page_store::PageStore};
use crate::bplustree::tree::{BPlusTree, SharedBPlusTree};
use ::bplustree::*;
use criterion::{Criterion, criterion_group, criterion_main};
use tempfile::TempDir;
fn benchmark_insert(c: &mut Criterion) {
    c.bench_function("insert 1 million keys", |b| {
        b.iter(|| {
            let dir = TempDir::new().unwrap();
            let file_path = dir.path().join("bplustree.db");
            let store = FileStore::<PageStore>::new(&file_path).expect("create storage");
            let order = 16;
            let tree = BPlusTree::<u64, String, FileStore<PageStore>>::new(store, order)
                .expect("create tree");
            let st = SharedBPlusTree::new(tree);

            for i in 0..1_000_000 {
                st.insert(i, format!("val_{}", i)).unwrap();
            }
        });
    });
}

criterion_group!(benches, benchmark_insert);
criterion_main!(benches);
