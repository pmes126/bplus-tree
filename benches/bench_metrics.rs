//! Application-level performance metrics for the B+ tree.
//!
//! Unlike Criterion (wall-clock only), this reports tree height, write
//! amplification, disk overhead, ops/sec, and scaling curves — the numbers
//! that actually matter for a storage engine.
//!
//! Run via: `just bench-metrics` or `cargo bench --bench bench_metrics`

use bplus_tree::api::Db;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use tempfile::TempDir;

const N: u64 = 5_000;

fn bench_tempdir() -> TempDir {
    let base = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target/bench_tmp");
    std::fs::create_dir_all(&base).unwrap();
    tempfile::tempdir_in(base).unwrap()
}

fn dir_size(path: &std::path::Path) -> u64 {
    std::fs::read_dir(path)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter_map(|e| e.metadata().ok().map(|m| m.len()))
        .sum()
}

/// Populates a tree with N entries via unbatched puts (accurate metadata).
fn populated_tree(dir: &TempDir) -> (Db, bplus_tree::api::db::Tree<u64, String>, u64) {
    let db = Db::open(dir.path()).unwrap();
    let tree = db.create_tree::<u64, String>("bench", 64).unwrap();
    let mut data_bytes = 0u64;
    for i in 0..N {
        let val = format!("val_{i}");
        data_bytes += 8 + val.len() as u64;
        tree.put(&i, &val).unwrap();
    }
    (db, tree, data_bytes)
}

// ---------------------------------------------------------------------------
// Metric display
// ---------------------------------------------------------------------------

struct WriteMetrics {
    label: &'static str,
    ops: u64,
    height: u64,
    disk_bytes: u64,
    data_bytes: u64,
    elapsed: std::time::Duration,
}

impl WriteMetrics {
    fn print(&self) {
        let write_amp = self.disk_bytes as f64 / self.data_bytes.max(1) as f64;
        let bpe = self.disk_bytes as f64 / self.ops.max(1) as f64;
        println!();
        println!("=== {} ===", self.label);
        println!("  ops:              {}", self.ops);
        println!("  tree height:      {}", self.height);
        println!(
            "  disk size:        {:.1} KB",
            self.disk_bytes as f64 / 1024.0
        );
        println!(
            "  raw data:         {:.1} KB",
            self.data_bytes as f64 / 1024.0
        );
        println!("  write amp:        {:.2}x", write_amp);
        println!("  bytes/entry:      {:.1}", bpe);
        println!("  space overhead:   {:.1}%", (write_amp - 1.0) * 100.0);
        println!(
            "  throughput:       {:.0} ops/sec",
            self.ops as f64 / self.elapsed.as_secs_f64()
        );
        println!(
            "  elapsed:          {:.3} ms",
            self.elapsed.as_secs_f64() * 1000.0
        );
    }
}

// ---------------------------------------------------------------------------
// Write benchmarks
// ---------------------------------------------------------------------------

fn measure_insert_batched() {
    let dir = bench_tempdir();
    let db = Db::open(dir.path()).unwrap();
    let tree = db.create_tree::<u64, String>("bench", 64).unwrap();

    let mut data_bytes = 0u64;
    let start = std::time::Instant::now();
    let mut txn = tree.txn();
    for i in 0..N {
        let val = format!("val_{i}");
        data_bytes += 8 + val.len() as u64;
        txn.insert(&i, &val);
    }
    txn.commit().unwrap();
    let elapsed = start.elapsed();

    WriteMetrics {
        label: "insert (batched txn, 5k u64 keys)",
        ops: N,
        height: tree.height(),
        disk_bytes: dir_size(dir.path()),
        data_bytes,
        elapsed,
    }
    .print();
}

fn measure_insert_unbatched() {
    let dir = bench_tempdir();
    let db = Db::open(dir.path()).unwrap();
    let tree = db.create_tree::<u64, String>("bench", 64).unwrap();

    let mut data_bytes = 0u64;
    let start = std::time::Instant::now();
    for i in 0..N {
        let val = format!("val_{i}");
        data_bytes += 8 + val.len() as u64;
        tree.put(&i, &val).unwrap();
    }
    let elapsed = start.elapsed();

    WriteMetrics {
        label: "insert (unbatched, 5k u64 keys)",
        ops: N,
        height: tree.height(),
        disk_bytes: dir_size(dir.path()),
        data_bytes,
        elapsed,
    }
    .print();
}

fn measure_large_values() {
    let dir = bench_tempdir();
    let db = Db::open(dir.path()).unwrap();
    let tree = db.create_tree::<u64, String>("bench", 64).unwrap();

    let large_val = "x".repeat(1024);
    let count = 1_000u64;
    let data_bytes = count * (8 + 1024);

    let start = std::time::Instant::now();
    for i in 0..count {
        tree.put(&i, &large_val).unwrap();
    }
    let elapsed = start.elapsed();

    WriteMetrics {
        label: "insert 1KB values (unbatched, 1k keys)",
        ops: count,
        height: tree.height(),
        disk_bytes: dir_size(dir.path()),
        data_bytes,
        elapsed,
    }
    .print();
}

fn measure_string_keys() {
    let dir = bench_tempdir();
    let db = Db::open(dir.path()).unwrap();
    let tree = db.create_tree::<String, String>("bench", 64).unwrap();

    let mut data_bytes = 0u64;
    let start = std::time::Instant::now();
    for i in 0..N {
        let key = format!("key_{i:06}");
        let val = format!("val_{i}");
        data_bytes += key.len() as u64 + val.len() as u64;
        tree.put(&key, &val).unwrap();
    }
    let elapsed = start.elapsed();

    WriteMetrics {
        label: "insert string keys (unbatched, 5k keys)",
        ops: N,
        height: tree.height(),
        disk_bytes: dir_size(dir.path()),
        data_bytes,
        elapsed,
    }
    .print();
}

// ---------------------------------------------------------------------------
// Read benchmarks
// ---------------------------------------------------------------------------

fn measure_point_get() {
    let dir = bench_tempdir();
    let (_db, tree, _) = populated_tree(&dir);

    let start = std::time::Instant::now();
    for i in 0..N {
        tree.get(&i).unwrap();
    }
    let elapsed = start.elapsed();

    println!();
    println!("=== point get (5k sequential lookups) ===");
    println!("  tree height:      {}", tree.height());
    println!("  entries:          {}", tree.len());
    println!(
        "  throughput:       {:.0} ops/sec",
        N as f64 / elapsed.as_secs_f64()
    );
    println!(
        "  avg latency:      {:.0} ns/op",
        elapsed.as_nanos() as f64 / N as f64
    );
    println!(
        "  elapsed:          {:.3} ms",
        elapsed.as_secs_f64() * 1000.0
    );
}

fn measure_point_get_missing() {
    let dir = bench_tempdir();
    let (_db, tree, _) = populated_tree(&dir);

    let start = std::time::Instant::now();
    for i in N..(N * 2) {
        let _ = tree.get(&i);
    }
    let elapsed = start.elapsed();

    println!();
    println!("=== point get missing keys (5k lookups) ===");
    println!("  tree height:      {}", tree.height());
    println!(
        "  throughput:       {:.0} ops/sec",
        N as f64 / elapsed.as_secs_f64()
    );
    println!(
        "  avg latency:      {:.0} ns/op",
        elapsed.as_nanos() as f64 / N as f64
    );
    println!(
        "  elapsed:          {:.3} ms",
        elapsed.as_secs_f64() * 1000.0
    );
}

fn measure_range_scan() {
    let dir = bench_tempdir();
    let (_db, tree, _) = populated_tree(&dir);

    let start = std::time::Instant::now();
    let mut count = 0u64;
    for entry in tree.range(&0u64, &N).unwrap() {
        entry.unwrap();
        count += 1;
    }
    let elapsed = start.elapsed();

    println!();
    println!("=== full range scan ({count} entries) ===");
    println!("  tree height:      {}", tree.height());
    println!(
        "  scan rate:        {:.0} entries/sec",
        count as f64 / elapsed.as_secs_f64()
    );
    println!(
        "  avg latency:      {:.0} ns/entry",
        elapsed.as_nanos() as f64 / count as f64
    );
    println!(
        "  elapsed:          {:.3} ms",
        elapsed.as_secs_f64() * 1000.0
    );
}

fn measure_range_scan_slice() {
    let dir = bench_tempdir();
    let (_db, tree, _) = populated_tree(&dir);

    let start = std::time::Instant::now();
    let mut count = 0u64;
    for entry in tree.range(&(N - 1000), &N).unwrap() {
        entry.unwrap();
        count += 1;
    }
    let elapsed = start.elapsed();

    println!();
    println!("=== range scan 1k-entry slice ===");
    println!("  tree height:      {}", tree.height());
    println!("  entries scanned:  {count}");
    println!(
        "  scan rate:        {:.0} entries/sec",
        count as f64 / elapsed.as_secs_f64()
    );
    println!(
        "  avg latency:      {:.0} ns/entry",
        elapsed.as_nanos() as f64 / count as f64
    );
    println!(
        "  elapsed:          {:.3} ms",
        elapsed.as_secs_f64() * 1000.0
    );
}

// ---------------------------------------------------------------------------
// Delete benchmarks
// ---------------------------------------------------------------------------

fn measure_delete() {
    let dir = bench_tempdir();
    let (_db, tree, _) = populated_tree(&dir);

    let disk_before = dir_size(dir.path());
    let height_before = tree.height();

    let start = std::time::Instant::now();
    for i in 0..N {
        tree.delete(&i).unwrap();
    }
    let elapsed = start.elapsed();

    let disk_after = dir_size(dir.path());

    println!();
    println!("=== delete (unbatched, 5k keys) ===");
    println!("  height before:    {height_before}");
    println!("  height after:     {}", tree.height());
    println!("  entries after:    {}", tree.len());
    println!("  disk before:      {:.1} KB", disk_before as f64 / 1024.0);
    println!("  disk after:       {:.1} KB", disk_after as f64 / 1024.0);
    println!(
        "  disk delta:       {:.1} KB",
        (disk_after as i64 - disk_before as i64) as f64 / 1024.0
    );
    println!(
        "  throughput:       {:.0} ops/sec",
        N as f64 / elapsed.as_secs_f64()
    );
    println!(
        "  elapsed:          {:.3} ms",
        elapsed.as_secs_f64() * 1000.0
    );
}

// ---------------------------------------------------------------------------
// Concurrent writers
// ---------------------------------------------------------------------------

fn measure_concurrent_insert() {
    let num_threads = 4usize;
    let per_thread = (N as usize) / num_threads;

    let dir = bench_tempdir();
    let db = Arc::new(Db::open(dir.path()).unwrap());
    let tree = Arc::new(db.create_tree::<u64, String>("bench", 64).unwrap());

    let start = std::time::Instant::now();
    let handles: Vec<_> = (0..num_threads)
        .map(|t| {
            let tree = Arc::clone(&tree);
            thread::spawn(move || {
                let base = (t * per_thread) as u64;
                for i in 0..per_thread as u64 {
                    tree.put(&(base + i), &format!("val_{}", base + i)).unwrap();
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }
    let elapsed = start.elapsed();

    println!();
    println!("=== concurrent insert ({num_threads} threads, {N} total) ===");
    println!("  tree height:      {}", tree.height());
    println!("  entries:          {}", tree.len());
    println!(
        "  throughput:       {:.0} ops/sec",
        N as f64 / elapsed.as_secs_f64()
    );
    println!(
        "  elapsed:          {:.3} ms",
        elapsed.as_secs_f64() * 1000.0
    );
}

// ---------------------------------------------------------------------------
// Scaling curves
// ---------------------------------------------------------------------------

fn measure_scaling() {
    println!();
    println!("=== scaling: height & write amplification vs entry count ===");
    println!(
        "  {:>10}  {:>6}  {:>12}  {:>12}  {:>10}  {:>12}",
        "entries", "height", "disk (KB)", "data (KB)", "write amp", "bytes/entry"
    );

    for &count in &[100u64, 500, 1_000, 2_500, 5_000, 10_000, 25_000] {
        let dir = bench_tempdir();
        let db = Db::open(dir.path()).unwrap();
        let tree = db.create_tree::<u64, String>("bench", 64).unwrap();

        let mut data_bytes = 0u64;
        for i in 0..count {
            let val = format!("val_{i}");
            data_bytes += 8 + val.len() as u64;
            tree.put(&i, &val).unwrap();
        }

        let disk = dir_size(dir.path());
        println!(
            "  {:>10}  {:>6}  {:>12.1}  {:>12.1}  {:>10.2}x  {:>12.1}",
            count,
            tree.height(),
            disk as f64 / 1024.0,
            data_bytes as f64 / 1024.0,
            disk as f64 / data_bytes as f64,
            disk as f64 / count as f64,
        );
    }
}

fn main() {
    println!("B+ Tree Performance Metrics");
    println!("============================");
    println!("(page size: 4096 bytes)");

    measure_insert_batched();
    measure_insert_unbatched();
    measure_large_values();
    measure_string_keys();
    measure_point_get();
    measure_point_get_missing();
    measure_range_scan();
    measure_range_scan_slice();
    measure_delete();
    measure_concurrent_insert();
    measure_scaling();

    println!();
}
