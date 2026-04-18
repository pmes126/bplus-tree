use bplustree::api::Db;
use criterion::{Criterion, criterion_group, criterion_main};
use std::sync::Arc;
use std::thread;

const N: u64 = 10_000;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Creates a pre-populated tree with `N` u64→String entries.
fn populated_db() -> (tempfile::TempDir, Db) {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path()).expect("open db");
    let tree = db
        .create_tree::<u64, String>("bench", 64)
        .expect("create tree");
    let mut txn = tree.txn();
    for i in 0..N {
        txn.insert(&i, &format!("val_{i}"));
    }
    txn.commit().unwrap();
    (dir, db)
}

// ---------------------------------------------------------------------------
// Insert benchmarks
// ---------------------------------------------------------------------------

fn benchmark_insert(c: &mut Criterion) {
    c.bench_function("insert 10k keys", |b| {
        b.iter(|| {
            let dir = tempfile::tempdir().unwrap();
            let db = Db::open(dir.path()).expect("open db");
            let tree = db
                .create_tree::<u64, String>("bench", 64)
                .expect("create tree");

            for i in 0..N {
                tree.put(&i, &format!("val_{i}")).unwrap();
            }
        });
    });
}

fn benchmark_insert_txn(c: &mut Criterion) {
    c.bench_function("insert 10k keys (batched txn)", |b| {
        b.iter(|| {
            let dir = tempfile::tempdir().unwrap();
            let db = Db::open(dir.path()).expect("open db");
            let tree = db
                .create_tree::<u64, String>("bench", 64)
                .expect("create tree");

            let mut txn = tree.txn();
            for i in 0..N {
                txn.insert(&i, &format!("val_{i}"));
            }
            txn.commit().unwrap();
        });
    });
}

// ---------------------------------------------------------------------------
// Get benchmarks
// ---------------------------------------------------------------------------

fn benchmark_get(c: &mut Criterion) {
    let (_dir, db) = populated_db();
    let tree = db.open_tree::<u64, String>("bench").unwrap();

    c.bench_function("get 10k keys (sequential)", |b| {
        b.iter(|| {
            for i in 0..N {
                tree.get(&i).unwrap();
            }
        });
    });
}

// ---------------------------------------------------------------------------
// Range scan benchmarks
// ---------------------------------------------------------------------------

fn benchmark_range_full(c: &mut Criterion) {
    let (_dir, db) = populated_db();
    let tree = db.open_tree::<u64, String>("bench").unwrap();

    c.bench_function("range scan full (10k keys)", |b| {
        b.iter(|| {
            let iter = tree.range(&0u64, &N).unwrap();
            for entry in iter {
                entry.unwrap();
            }
        });
    });
}

fn benchmark_range_slice(c: &mut Criterion) {
    let (_dir, db) = populated_db();
    let tree = db.open_tree::<u64, String>("bench").unwrap();

    c.bench_function("range scan 1k slice", |b| {
        b.iter(|| {
            let iter = tree.range(&4_000u64, &5_000u64).unwrap();
            for entry in iter {
                entry.unwrap();
            }
        });
    });
}

// ---------------------------------------------------------------------------
// Delete benchmarks
// ---------------------------------------------------------------------------

fn benchmark_delete(c: &mut Criterion) {
    c.bench_function("delete 10k keys", |b| {
        b.iter(|| {
            let (_dir, db) = populated_db();
            let tree = db.open_tree::<u64, String>("bench").unwrap();

            for i in 0..N {
                tree.delete(&i).unwrap();
            }
        });
    });
}

fn benchmark_delete_txn(c: &mut Criterion) {
    c.bench_function("delete 10k keys (batched txn)", |b| {
        b.iter(|| {
            let (_dir, db) = populated_db();
            let tree = db.open_tree::<u64, String>("bench").unwrap();

            let mut txn = tree.txn();
            for i in 0..N {
                txn.delete(&i);
            }
            txn.commit().unwrap();
        });
    });
}

// ---------------------------------------------------------------------------
// Mixed read/write benchmark
// ---------------------------------------------------------------------------

fn benchmark_mixed_read_write(c: &mut Criterion) {
    let (_dir, db) = populated_db();
    let tree = db.open_tree::<u64, String>("bench").unwrap();

    c.bench_function("mixed read/write (50/50, 10k ops)", |b| {
        b.iter(|| {
            for i in 0..N {
                if i % 2 == 0 {
                    tree.get(&(i / 2)).unwrap();
                } else {
                    tree.put(&(N + i), &format!("new_{i}")).unwrap();
                }
            }
        });
    });
}

// ---------------------------------------------------------------------------
// Large values benchmark
// ---------------------------------------------------------------------------

fn benchmark_large_values(c: &mut Criterion) {
    // ~1KB values — stress the page layout without exceeding page size
    let large_val = "x".repeat(1024);

    c.bench_function("insert 1k keys with 1KB values", |b| {
        b.iter(|| {
            let dir = tempfile::tempdir().unwrap();
            let db = Db::open(dir.path()).expect("open db");
            let tree = db
                .create_tree::<u64, String>("bench", 64)
                .expect("create tree");

            let mut txn = tree.txn();
            for i in 0..1_000u64 {
                txn.insert(&i, &large_val);
            }
            txn.commit().unwrap();
        });
    });
}

// ---------------------------------------------------------------------------
// String keys benchmark
// ---------------------------------------------------------------------------

fn benchmark_string_keys(c: &mut Criterion) {
    c.bench_function("insert 10k string keys", |b| {
        b.iter(|| {
            let dir = tempfile::tempdir().unwrap();
            let db = Db::open(dir.path()).expect("open db");
            let tree = db
                .create_tree::<String, String>("bench", 64)
                .expect("create tree");

            let mut txn = tree.txn();
            for i in 0..N {
                txn.insert(&format!("key_{i:06}"), &format!("val_{i}"));
            }
            txn.commit().unwrap();
        });
    });
}

fn benchmark_string_keys_get(c: &mut Criterion) {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path()).expect("open db");
    let tree = db
        .create_tree::<String, String>("bench", 64)
        .expect("create tree");

    let mut txn = tree.txn();
    for i in 0..N {
        txn.insert(&format!("key_{i:06}"), &format!("val_{i}"));
    }
    txn.commit().unwrap();

    c.bench_function("get 10k string keys", |b| {
        b.iter(|| {
            for i in 0..N {
                tree.get(&format!("key_{i:06}")).unwrap();
            }
        });
    });
}

// ---------------------------------------------------------------------------
// Concurrent writers benchmark
// ---------------------------------------------------------------------------

fn benchmark_concurrent_writers(c: &mut Criterion) {
    let num_threads = 4;
    let per_thread = 2_500u64;

    c.bench_function("concurrent insert (4 threads, 10k total)", |b| {
        b.iter(|| {
            let dir = tempfile::tempdir().unwrap();
            let db = Arc::new(Db::open(dir.path()).expect("open db"));
            let tree = Arc::new(
                db.create_tree::<u64, String>("bench", 64)
                    .expect("create tree"),
            );

            let handles: Vec<_> = (0..num_threads)
                .map(|t| {
                    let tree = Arc::clone(&tree);
                    thread::spawn(move || {
                        let base = t as u64 * per_thread;
                        for i in 0..per_thread {
                            tree.put(&(base + i), &format!("val_{}", base + i)).unwrap();
                        }
                    })
                })
                .collect();

            for h in handles {
                h.join().unwrap();
            }
        });
    });
}

// ---------------------------------------------------------------------------
// Groups
// ---------------------------------------------------------------------------

criterion_group!(
    benches,
    benchmark_insert,
    benchmark_insert_txn,
    benchmark_get,
    benchmark_range_full,
    benchmark_range_slice,
    benchmark_delete,
    benchmark_delete_txn,
    benchmark_mixed_read_write,
    benchmark_large_values,
    benchmark_string_keys,
    benchmark_string_keys_get,
    benchmark_concurrent_writers,
);
criterion_main!(benches);
