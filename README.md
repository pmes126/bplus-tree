# cow-bptree

> Status: **alpha** — APIs may change.
> License: **MIT OR Apache-2.0**

Embedded, copy-on-write **B+-tree** key-value store in Rust.
Synchronous, zero-network. **Multi-writer** with optimistic commits (CAS).
**Snapshot** readers via epoch-based reclamation.

---

## Why

- **Multi-writer:** concurrent writers proceed in parallel; losers retry via OCC.
- **Crash-safe:** COW + atomic metadata publish; no WAL, no fsck.
- **Predictable perf:** slotted pages, bounded fanout, no heap storms.
- **Epoch snapshots:** readers pin an epoch and never block writers.
- **Embedded-first:** link as a library, call `put`/`get`/`delete`.
- **Pluggable encoding:** `NodeStorage` is a swappable node encoding strategy on top of raw `PageStorage`.

---

## Features

- Copy-on-write page mutation via `NodeView` over `[u8; 4096]` pages
- Multiple concurrent writers (optimistic concurrency; CAS on metadata)
- Batched write transactions (stage &rarr; commit &rarr; reclaim)
- Cursor-based range iteration (parent-stack traversal, no sibling pointers)
- Physical fullness handling: large values trigger page splits before reaching max keys
- Pluggable node encoding via `NodeStorage` trait; raw page I/O via `PageStorage` trait
- Multi-tree support: one database directory, many named trees
- Manifest-based crash recovery with CRC-framed catalog log
- Superblock and metadata page CRC validation
- Exclusive file locking to prevent multi-process corruption
- Built-in order-preserving codecs for `u64`, `i64`, `String`, `Vec<u8>`
- Typed `Tree<K, V>` API with `KeyCodec` / `ValueCodec` traits

---

## Quick start

```toml
[dependencies]
cow-bptree = "0.1"
```

### Build & test

```bash
cargo build
cargo test --tests
cargo run --example bytes_api
cargo run --example typed_api
cargo bench
```

### Bytes-level API

```rust
use bplustree::api::Db;

let dir = tempfile::tempdir()?;
let db = Db::open(dir.path())?;
let tree = db.create_tree::<Vec<u8>, Vec<u8>>("data", 64)?;

tree.put(&b"alpha".to_vec(), &b"1".to_vec())?;
tree.put(&b"beta".to_vec(), &b"2".to_vec())?;

let val = tree.get(&b"alpha".to_vec())?;
assert_eq!(val.as_deref(), Some(&b"1"[..]));

tree.delete(&b"alpha".to_vec())?;
```

### Typed API

```rust
use bplustree::api::Db;

let dir = tempfile::tempdir()?;
let db = Db::open(dir.path())?;
let tree = db.create_tree::<u64, String>("users", 64)?;

tree.put(&42, &"answer".to_string())?;
assert_eq!(tree.get(&42)?.as_deref(), Some("answer"));
```

### Batched write transaction

```rust
let tree = db.create_tree::<u64, String>("events", 64)?;

let mut txn = tree.txn();
txn.insert(&1, &"first".to_string());
txn.insert(&2, &"second".to_string());
txn.commit()?;  // atomic CAS; retries internally on conflict
```

---

## API surface

### `Db`

- `Db::open(dir)` — opens or creates a database
- `db.create_tree::<K, V>(name, order)` — creates a named tree
- `db.open_tree::<K, V>(name)` — opens an existing tree
- `db.tree::<K, V>(name, order)` — open-or-create

### `Tree<K, V>`

- `tree.put(&key, &value)` — insert or replace
- `tree.get(&key)` — lookup, returns `Option<V>`
- `tree.delete(&key)` — remove
- `tree.txn()` — start a batched `WriteTxn`
- `tree.range(&start, &end)` — forward range scan `[start, end)`
- `tree.range_from(&start)` — forward range scan from `start` to end of tree
- `tree.len()` / `tree.is_empty()`

### `WriteTxn<K, V>`

- `txn.insert(&key, &value)` — stage an insert
- `txn.delete(&key)` — stage a delete
- `txn.commit()` — atomically apply all staged operations

> `delete` returns an error if the key is not found.
> `commit` returns `Err(ApiError::TxnAborted)` if the retry budget is exhausted.

---

## Multi-writer semantics (OCC)

Multiple writers run in parallel:

1. Capture a **base version** (committed metadata pointer).
2. Apply writes on a staged tree (COW pages).
3. **Commit** by CAS-ing the metadata pointer.

If another writer published first, the transaction rebases from the latest root and
retries (up to a configurable limit). Readers never block writers.

---

## Durability and fsync

Each commit follows a strict sequence:

1. **CAS publish** — the new metadata pointer becomes visible to in-process readers
   immediately (atomic swap, no disk I/O).
2. **Write metadata page** — the new `(root_id, height, size, txn_id)` is written to the
   inactive A/B metadata slot via positional `write_all_at()` (kernel page cache, not yet
   durable).
3. **`fdatasync()`** — a single `sync_data()` call flushes all dirty pages in the data
   file to disk: both the COW node pages written during the transaction and the metadata
   page from step 2.

### Crash safety

The **A/B metadata slot alternation** provides atomic commit semantics without a WAL.
Each commit writes to `slot = txn_id % 2`, leaving the previous slot untouched. On
recovery, `MetadataManager::read_active_meta` reads both slots and picks the one with
the highest `txn_id` and a valid CRC32 checksum.

- **Crash before `fdatasync()`** — the new metadata page may not be on disk. Recovery
  reads the old slot, which is still valid. The tree rolls back to the prior commit.
- **Torn write to new slot** — the CRC32 checksum detects it. Recovery falls back to
  the old slot.
- **Crash after `fdatasync()`** — both node pages and metadata are durable. Recovery
  picks the new slot.

### Known side effect

`sync_data()` operates on the entire file descriptor, not a byte range. This means a
commit also flushes speculative COW pages written by other concurrent writers that have
not yet committed. Those pages are harmless (orphaned if the writer never commits) but
represent minor wasted I/O under concurrent write workloads. This is inherent to the
single-file, shared page pool design and is not a correctness issue.

---

## Epoch-based reclamation

Readers pin an **epoch** while walking a snapshot; writers retire old pages with the
**current epoch** at commit. A reclaimer frees pages only after all readers older than
that epoch have unpinned. No blocking, no use-after-free.

1. **Pin**: reader grabs `epoch_now` and reads from the committed root.
2. **Write**: writer builds a staged tree (COW), collects reclaimed node IDs.
3. **Commit**: writer CAS-publishes new metadata `(root_id, height, size)`. On success,
   tag each reclaimed page with `retire_epoch = epoch_now`.
4. **GC**: compute `min_pinned` across threads; free any page with
   `retire_epoch < min_pinned`.

---

## On-disk layout

```
<dir>/
  data.db            # all pages: superblock, tree nodes, metadata slots
  manifest.log       # append-only CRC-framed catalog log
  freelist.snapshot   # optional; written on graceful shutdown
  db.lock            # exclusive flock held while the database is open
```

### Recovery path

`database::open` acquires an exclusive file lock (`db.lock`), validates the superblock
(page 0, including CRC-32C), replays the CRC-framed manifest to rebuild the in-memory
catalog, then reconciles each tree's catalog entry against its on-disk A/B metadata pages
(source of truth for `root_id`, `height`, `size` after a crash). If a freelist snapshot
exists, freed page IDs are restored so they can be reused.

### Key components

- **Superblock** (page 0): magic number, format version, generation counter, CRC-32C.
- **Manifest**: append-only log of `CreateTree`, `RenameTree`, `DeleteTree`,
  `Checkpoint` records. Each record is CRC-framed; truncated trailing records (crash
  mid-write) are silently skipped, CRC mismatches are reported as corruption.
- **Catalog**: in-memory map of `TreeId -> TreeMeta`, rebuilt by replaying the manifest.
- **Per-tree metadata**: A/B alternating pages storing `(root_node_id, height, size, txn_id)`.
  Commit writes to the inactive slot; readers always see a consistent pair.
  Each page is CRC32-validated on read.
- **File lock**: exclusive `flock` on `db.lock` prevents concurrent access from multiple
  processes.

---

## Architecture

For a detailed description of the architecture, design decisions, and trade-offs, see
[ARCHITECTURE.md](ARCHITECTURE.md).

```
src/
  api.rs, api/                  # Db, Tree<K,V>, WriteTxn, ApiError
  codec.rs, codec/              # KeyCodec/ValueCodec traits, bincode codecs, kv (API codecs)
  database.rs, database/        # Database, catalog, manifest, metadata, superblock
  bplustree/                    # BPlusTree core: search, insert, delete, commit, transaction
  storage.rs, storage/          # PageStorage, NodeStorage, FilePageStorage, PagedNodeStorage, EpochManager
  page.rs, page/                # Slotted page layouts (leaf, internal)
  keyfmt.rs, keyfmt/            # Key encoding formats (raw, prefix-compressed)
  layout.rs                     # PAGE_SIZE constant
examples/
  bytes_api.rs                  # Vec<u8> key/value CRUD
  typed_api.rs                  # u64/String with batched transaction
  example.rs                    # async HTTP fetch + store
benches/
  bench_insert.rs               # Criterion benchmarks
```

### Layer overview (bottom to top)

**Page layer** (`page/`, `layout.rs`): fixed 4 KB slotted pages. Header &rarr; slot
directory &rarr; packed data region. Leaf pages store `(key, value)` pairs; internal
pages store `(key, right_child)` with `leftmost_child` in the header.

**Storage layer** (`storage.rs`, `storage/`): `PageStorage` trait for raw page I/O;
`NodeStorage` trait for encoded node I/O (pluggable encoding strategy).
`FilePageStorage` is the concrete file-backed `PageStorage`.
`PagedNodeStorage<S>` wraps any `PageStorage` into a `NodeStorage`.

**Database layer** (`database.rs`, `database/`): `Database<S>` owns a
`PagedNodeStorage<S>` for node encoding and an `Arc<S>` for raw metadata I/O (both
share the same underlying storage instance). Manages the superblock, manifest, catalog,
and tree lifecycle.

**B+ tree core** (`bplustree/`): `BPlusTree` / `SharedBPlusTree` — search, insert,
delete, commit with CAS. `WriteTransaction` buffers operations for batched atomic
commits.

**API layer** (`api.rs`, `api/`): `Db` opens a `Database`, leaks it for `'static`
lifetime, and hands out typed `Tree<K, V>` handles. Purely synchronous.

---

## Design trade-offs: COW, sibling pointers, and batched writes

### Why COW?

Copy-on-write is the foundation of the concurrency model. Every write clones only the
pages it touches (leaf + ancestors), then atomically publishes a new root via CAS on the
metadata pointer. Readers never block writers because they see a consistent snapshot
pinned at their epoch. This is the same approach used by LMDB, BoltDB, and redb in
production.

### Sibling pointers and range iteration

Traditional B+ trees link leaves with `next`/`prev` pointers for fast sequential scans.
Under COW this creates a cascade problem: COW-copying one leaf gives it a new page ID,
which invalidates its left sibling's `next` pointer, forcing a COW-copy of that sibling
too, and so on through the entire leaf chain.

The standard solution (used by LMDB, BoltDB, redb) is to not use sibling pointers at
all. Range iteration instead uses a **cursor** that maintains a stack of
`(node_id, index)` frames from root to leaf. When a leaf is exhausted, the cursor pops
up to the parent, advances the index, and descends back down. The cost is O(log n) per
leaf transition in the worst case, but in practice the tree height is 3-5 even for
millions of keys, and parent pages are hot in cache.

This cursor-based iterator is implemented in `BPlusTreeIter` and exposed through
`tree.range()` and `tree.range_from()`.

### Batched writes

The `WriteTransaction` buffers operations and replays them against the current root at
commit time. If the CAS fails (another writer committed first), it rebases from the new
root and retries. This is correct for the OCC model. Two potential improvements for
large batches:

- **Sort the batch by key** before replay, so leaf access is sequential and minimises
  the number of distinct COW page copies.
- **Bulk-load path** for initial data ingestion: build subtrees bottom-up rather than
  inserting through the tree one key at a time.

### Physical fullness and large values

The tree handles both logical overflow (`keys_len() > max_keys`) and physical overflow
(`PageFull` from the slotted page layer). Large values can fill a 4 KB page before
reaching the tree order, triggering page splits at the physical level. Entries are
validated upfront: `key_len + val_len` must not exceed `MAX_ENTRY_PAYLOAD` (2038 bytes),
guaranteeing that at least two entries always fit per page so splits produce valid halves.

### Where this design fits

- **Embedded databases** (the LMDB/redb/BoltDB niche) where the store is linked as a
  library, not accessed over a network.
- **Read-heavy workloads** where readers must never block and always see consistent
  snapshots.
- **Crash safety without a WAL**: COW gives atomic commits for free since old pages
  survive until the new root is published.
- **Low-to-moderate write contention**: OCC retries are cheap when conflicts are rare.

### Where it struggles

- **Write-heavy workloads with high contention**: OCC retries discard and redo all
  speculative work.
- **Large sequential bulk loads**: COW copies O(height) pages per insert; a bulk-load
  path would amortise this.
- **Values larger than ~2 KB**: entries must fit within `MAX_ENTRY_PAYLOAD` (2038 bytes).
  Overflow pages or external value storage are not yet implemented.

---

## Gotchas

- **Order-preserving keys:** if your codec doesn't preserve lexicographic order, scans will be wrong.
- **Commit conflicts:** normal under load. `WriteTxn` retries automatically up to a budget.
- **Entry size limit:** key + value must fit within 2038 bytes (`MAX_ENTRY_PAYLOAD`).
  Entries exceeding this limit are rejected with `TreeError::EntryTooLarge`.

---

## Roadmap

- **Prefix-compressed key block format (`PrefixRestarts`)** — Keys are currently stored
  verbatim in each slot. When keys share long common prefixes, this wastes significant
  page space. Prefix compression stores the shared prefix once and only the differing
  suffix per key, with periodic restart points for random access within the block. This
  increases key density per page and reduces I/O for prefix-heavy workloads.

- **Sorted batch replay for write transactions** — `WriteTxn` currently replays buffered
  operations in insertion order. Sorting the batch by key before replay means consecutive
  inserts land in the same (or nearby) leaves, so the already-staged COW pages are reused
  rather than cloning a different leaf for each insert. Fewer COW copies, better cache
  locality, and lower write amplification for large batches.

- **Bulk-load path for large initial imports** — Inserting N keys one-by-one through the
  tree incurs O(height) COW copies per key. A bulk-load path sorts all keys upfront,
  fills leaves left-to-right, and builds internal nodes bottom-up. Orders of magnitude
  faster for initial data ingestion compared to incremental inserts.

- **Overflow pages for values exceeding `MAX_ENTRY_PAYLOAD`** — Currently key + value
  must fit within 2038 bytes. Overflow pages would store large values across multiple
  linked pages, removing this size constraint. This is standard in production B-trees
  (SQLite, LMDB).

- **Fuzz testing (`cargo-fuzz`)** — Use coverage-guided fuzzing to generate random
  sequences of inserts, deletes, splits, and merges, then verify tree invariants hold
  after each operation. Catches edge cases in the slotted page layout and codec
  encode/decode roundtrips that hand-written tests are unlikely to cover.

- **Configurable page size** — Currently hardcoded to 4 KB. Some workloads benefit from
  larger pages (16 KB, 64 KB) for fewer tree levels and better sequential throughput;
  smaller pages reduce write amplification under update-heavy workloads. Making this
  configurable requires storing the page size in the superblock and threading it through
  the page layer.

- **Sharded epoch pinning** — `EpochManager::pin()`/`unpin()` currently acquire a
  central `Mutex<HashMap<ThreadId, Epoch>>` on every read operation. Under high reader
  concurrency this serialises the pin/unpin brackets even though the tree walk itself is
  lock-free. Replace with per-thread atomic slots (a `Vec<AtomicU64>` indexed by a
  thread-claimed slot) so that pin is a single atomic store and `oldest_active()` is a
  lock-free scan. This is the approach used by crossbeam-epoch and similar libraries.

---

## License

Dual-licensed under MIT or Apache-2.0. You may choose either license.
