//! Stress tests for concurrent writers.
//!
//! These reproduce the invariant violation ("expected internal node while
//! updating parents") that occurs under multi-threaded put workloads.

use crate::api::Db;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use tempfile::TempDir;

/// Disk-backed temp dir to avoid RAM-backed tmpfs pressure.
fn stress_tempdir() -> TempDir {
    let base = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target/test_tmp");
    std::fs::create_dir_all(&base).unwrap();
    tempfile::tempdir_in(base).unwrap()
}

/// Baseline: single-threaded put of 2000 keys to rule out tree logic bugs.
#[test]
fn single_thread_put_2000() {
    for round in 0..5 {
        let dir = stress_tempdir();
        let db = Db::open(dir.path()).expect("open db");
        let tree = db
            .create_tree::<u64, String>("baseline", 64)
            .expect("create tree");

        for i in 0..2000u64 {
            tree.put(&i, &format!("val_{i}")).unwrap();
        }

        let mut missing = Vec::new();
        for i in 0..2000u64 {
            match tree.get(&i) {
                Ok(Some(v)) if v == format!("val_{i}") => {}
                Ok(Some(v)) => missing.push(format!("{i}(wrong:{v})")),
                Ok(None) => missing.push(format!("{i}")),
                Err(e) => missing.push(format!("{i}(err:{e})")),
            }
        }
        assert!(
            missing.is_empty(),
            "round {round}: single-thread missing: {missing:?}"
        );
    }
}

#[test]
fn concurrent_put_4_threads() {
    let num_threads = 4;
    let per_thread = 500u64;
    let rounds = 50;

    for round in 0..rounds {
        let dir = stress_tempdir();
        let db = Arc::new(Db::open(dir.path()).expect("open db"));
        let tree = Arc::new(
            db.create_tree::<u64, String>("stress", 64)
                .expect("create tree"),
        );

        let handles: Vec<_> = (0..num_threads)
            .map(|t| {
                let tree = Arc::clone(&tree);
                thread::spawn(move || {
                    let base = t as u64 * per_thread;
                    for i in 0..per_thread {
                        tree.put(&(base + i), &format!("val_{}", base + i))
                            .unwrap_or_else(|e| {
                                panic!("round {round} thread {t} key {}: {e}", base + i)
                            });
                    }
                })
            })
            .collect();

        for (i, h) in handles.into_iter().enumerate() {
            h.join()
                .unwrap_or_else(|e| panic!("round {round} thread {i} panicked: {e:?}"));
        }

        // Verify every key is present
        let read_tree = db.open_tree::<u64, String>("stress").unwrap();
        let mut missing = Vec::new();
        for t in 0..num_threads {
            let base = t as u64 * per_thread;
            for i in 0..per_thread {
                let key = base + i;
                match read_tree.get(&key) {
                    Ok(Some(v)) if v == format!("val_{key}") => {}
                    Ok(Some(v)) => missing.push(format!("{key}(wrong:{v})")),
                    Ok(None) => missing.push(format!("{key}")),
                    Err(e) => missing.push(format!("{key}(err:{e})")),
                }
            }
        }
        if !missing.is_empty() {
            panic!(
                "round {round}: {}/{} keys missing or wrong: {:?}",
                missing.len(),
                num_threads as u64 * per_thread,
                &missing[..missing.len().min(30)]
            );
        }

        eprintln!("concurrent_put_4_threads: round {}/{rounds} ok", round + 1);
    }
}
