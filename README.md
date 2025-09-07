# cow-bptree

> Status: **alpha** — APIs may change.  
> MSRV: **1.77**  
> License: **MIT OR Apache-2.0**

Embedded, copy-on-write **B+-tree** key-value store in Rust.  
Zero network. **Multi-writer** with optimistic commits (CAS). **Snapshot** readers via epochs. Streaming range scans.

> Status: early but usable. API surface is small; internals are evolving.

---

## Why

- **Predictable perf:** slotted pages, bounded fanout, no surprise heap storms.
- **Multi-writer:** high write concurrency without blocking, multiple writers proceed in parallel; losers just retry.
- **Crash-safe:** atomic metadata updates; no fsck.
- **Space-efficient:** copy-on-write, epoch-based reclamation.
- **Embedded-first:** link it like a library, call `get/put/delete/scan`.
- **Clean API:** bytes-level for engines; typed façade for apps.
- **Real snapshots:** readers pin epochs and never block writers.

---

## Features

- COW page mutation via in-memory views over `[u8; PAGE_SIZE]`, persisted with `PageStorage` and epoch-pinned read snapshots
- **Multiple concurrent writers** (optimistic concurrency; CAS on metadata)
- Range scans with a streaming iterator
- Batched write transaction (stage → commit → reclaim)
- Pluggable storage via `NodeStorage` / `MetadataStorage`
- Built-in codecs for `Vec<u8>`, `u64` (big-endian), `String` (UTF-8)
- `insert`, `get`, `remove`, `range` (forward).
- Generic `Codec` for K/V encode/decode.
---

## Quick start

## Installation

Add the crate (once published):

```toml
[dependencies]
cow-bptree = "0.1"
```

### Build
```bash
cargo build
```

### Bytes-level API
```rust
use bplustree::DbBytes;
use bplustree::storage::{file_store::FileStore, page_store::PageStore};

let path  = std::env::temp_dir().join("bpt.db");
let store = FileStore::<PageStore>::new(&path)?;
let db    = DbBytes::new(store, /*order*/ 64)?;

// CRUD
db.put(b"alpha", b"1")?;
assert_eq!(db.get(b"alpha")?, Some(b"1".to_vec()));
db.delete(b"alpha")?;

// Streaming scan [a, c)
if let Some(mut it) = db.scan_range(b"a", b"c")? {
    while let Some((k, v)) = it.next() {
        // ...
    }
}
```

### Typed façade (uses your codecs)
```rust
use bplustree::api::TypedDb;
use bplustree::bplustree::tree::BPlusTree;
use bplustree::storage::{file_store::FileStore, page_store::PageStore};

let path  = std::env::temp_dir().join("bpt.db");
let store = FileStore::<PageStore>::new(&path)?;
let tree  = BPlusTree::<u64, String, _>::new(store, 64)?;
let kv    = TypedDb::from_tree(tree);

kv.put(42, "answer".into())?;
assert_eq!(kv.get(&42)?, Some("answer".into()));
```

### Batched write transaction (OCC)
```rust
// Bytes
let mut w = db.begin_write()?;
w.put(b"k1".to_vec(), b"v1".to_vec())?;
w.delete(&b"k1".to_vec())?;
w.commit()?;

// Typed
let mut t = kv.begin_write()?;
t.put(1u64, "a".into())?;
t.delete(&1)?;
t.commit()?;
```

### Example & tests
```bash
cargo run --example bytes_api
cargo test --tests
```

### Benchmarks (Criterion)
```bash
cargo bench
```
Reports under `target/criterion/...`.

---

## Multi-writer semantics (OCC)

Multiple writers run in parallel:

1. Capture a **base version** (committed metadata pointer).
2. Apply writes on a staged tree (COW pages).
3. **Commit** by CAS-ing the metadata pointer.

If another writer published first, commit returns a **stale base** error → retry against the latest base (re-run read/compute/apply inside the loop). Readers never block writers.

---

## Epoch-based reclamation (short)

Readers pin an **epoch** while walking a snapshot; writers retire old pages with the **current epoch** at commit. A reclaimer frees pages only after all readers older than that epoch have unpinned. No blocking, no UAF.

**How it flows**

1. **Pin**: reader grabs `epoch_now` and reads from the committed root seen at pin.
2. **Write**: writer builds a staged tree (COW), collects `reclaimed_nodes`.
3. **Commit**: writer `CAS`-publishes new metadata `(root_id, height, size)`. On success, tag each reclaimed page with `retire_epoch = epoch_now`.
4. **GC**: periodically compute `min_pinned` across threads; free any page with `retire_epoch < min_pinned`.

**Notes**

* Multiple writers are fine: if a `CAS` fails (stale base), the writer retries from the latest root.
* Long scans keep old pages alive (by design). Keep scans bounded or reclaim opportunistically.
* On disk, “free” = return to a freelist; the file doesn’t shrink immediately. Crash safety comes from atomic metadata publish—after restart, only the last committed root is visible.
* Key ordering must be preserved by codecs (e.g., big-endian numerics), or scans will be wrong.

**Atomics (rule of thumb)**

* Publish commit with **Release**; readers load root/metadata with **Acquire**.
* Reader pins/unpins use Release; reclaimer reads pins with Acquire.

---

## API surface (embedded)

### Bytes-level
- `DbBytes<S>::new(storage, order) -> Result<Self>`
- `get(&[u8]) -> Result<Option<Vec<u8>>>`
- `put(&[u8], &[u8]) -> Result<()>`
- `delete(&[u8]) -> Result<()>`
- `scan_range(start, end) -> Result<Option<BytesIter>>`
- `begin_write() -> Result<WriteTxnBytes<'_, S>>`

### Typed
- `TypedDb::from_tree(BPlusTree<K,V,S>)`
- `get(&K) -> Result<Option<V>>`
- `put(K, V) -> Result<()>`
- `delete(&K) -> Result<()>`
- `scan_range(&start, &end) -> Result<Option<TypedIter<'_, K,V,S>>>`
- `begin_write() -> Result<TypedWriteTxn<'_, K,V,S>>`

> Iterators yield `(key, value)`; stop when `next()` returns `None`.  
> `delete` returns `Result<()>`; “not found” surfaces as an engine error.

---

## Design sketch

- **On-page layout:** fixed header → slot directory → packed data region.  
  Leaves: `(key, value)`. Internals: `(key, right_child)` + `leftmost_child` in header.
- **Ordering:** **lexicographic**; key codec must preserve order (big-endian numerics, UTF‑8 strings).
- **COW:** writes clone touched pages. Commit swaps `(root_id, height, size)` atomically.
- **Epochs:** readers pin an epoch; GC reclaims dead pages post-commit.

---

## Project layout

```
src/
  api.rs                     # bytes + typed façade, iterators, write txn (OCC)
  lib.rs
  bplustree/
    tree.rs                  # BPlusTree/SharedBPlusTree, commit/try_commit, search/insert/delete
    iterator.rs              # BPlusTreeIter
    node.rs                  # NodeId, node helpers
    epoch.rs                 # reader pins
    ...                      # internals
  storage/
    trait.rs                 # NodeStorage, MetadataStorage, codecs
    file_store.rs            # file-backed storage
    page_store.rs            # page IO
    page/                    # on-page layouts (leaf/internal)
benches/
  bench_insert.rs            # Criterion
examples/
  bytes_api.rs               # minimal embedded usage
tests/
  api_basic.rs               # CRUD/scan/txn tests
```

---

## Gotchas

- **Order-preserving keys:** if your codec doesn’t preserve order, scans are wrong.
- **Commit conflicts:** normal under load. Handle `CommitError::StaleBase` by retrying.
- **Large values:** consider overflow pages for jumbo blobs.
- **Durability:** depends on storage `sync_all()`; tune fsync policy to your needs.

---

## Roadmap

- Prefix scans & `RangeBounds` helpers
- Page data compaction
- Background GC tuning
- Optional network service + driver (gRPC) after the embedded API hardens

---

## License

Dual-licensed under MIT or Apache-2.0.
You may choose either license.

---
## Contribution
PRs welcome! Please open an issue for discussion first.

---

## TODO / Known Gaps

Priorities: P0 = must-have for public use, P1 = nice soon, P2 = later

- [x] P0: Epoch-based reclamation, make sure leak-free on panic paths
- [x] P0: Recovery path on startup: detect last good metadata/page epoch; handle torn writes; checksum validation
- [x] P0: More Benchmarks (Criterion)
- [x] P0: CI (build, test, lint)
- [x] P1: More tests (edge cases, error paths)
- [x] P1: More provided codecs (u32, i64, etc.)
- [x] P1: Fuzz testing (e.g., cargo-fuzz)
- [x] P1: More storage backends (in-memory, etc.)
- [x] P1: Page size tuning (configurable PAGE_SIZE)
- [x] P1: Write transaction batching (multiple ops per commit)
- [x] P1: Better error types (distinguish not-found, conflicts, etc.)
- [x] P1: More documentation (design doc, usage guide)
- [x] P1: More examples (complex usage patterns)
- [x] P2: Prefix scans & RangeBounds helpers
- [x] P2: Background GC tuning (adaptive frequency)
- [x] P2: Optional network service + driver (gRPC)
- [x] P2: Support for large values (overflow pages) Or calculate max value size based on order and page size
- [x] P2: Performance optimizations (hot paths)
- [x] P2: Async support (async storage backends)  
- [x] P2: Monitoring & metrics (operation counts, latencies)
- [x] P2: Advanced features (transactions, secondary indexes)
- [x] P2: API stability (semver, deprecation policy)
- [x] P2: Contributor guidelines (code of conduct, contributing doc)
- [x] P2: Roadmap & vision (long-term goals)
- [x] P2: Prefix compression / fence keys on internal nodes.
