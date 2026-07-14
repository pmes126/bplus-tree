# bplus_store

> Status: **beta** — core APIs are stable; internal storage traits may still change.
> License: **MIT OR Apache-2.0**

Embedded, copy-on-write **B+-tree** key-value store in Rust.
Synchronous, zero-network. **Multi-writer** with optimistic commits (CAS).
**Snapshot** readers via epoch-based reclamation.

---

## Why

An embedded key-value store in the same design space as
[LMDB](https://www.symas.com/lmdb),
[BoltDB](https://github.com/etcd-io/bbolt), and
[redb](https://github.com/cberner/redb) — crash-safe COW B+ trees with
snapshot isolation. Where it differs:

- **Multi-writer OCC:** LMDB and BoltDB serialise all writes behind a single
  writer lock. `bplus_store` lets multiple writers proceed in parallel and
  resolves conflicts at commit time via CAS on a 128-bit metadata word. Under
  low-to-moderate contention this gives near-linear write throughput scaling.
- **No WAL:** crash safety comes from COW page immutability + A/B metadata
  slot alternation with CRC validation. No write-ahead log to tune, compact,
  or replay.
- **Bounded page cache:** `PagedNodeStorage` keeps decoded `NodeView`s in a
  bounded CLOCK-Pro cache (via `quick_cache`). Pages are immutable while live
  (COW guarantee), so cached entries never go stale. The cache is bounded to a
  configurable number of pages (default 16,384 ≈ 64 MB), and CLOCK-Pro provides
  scan resistance — range scans over cold leaves won't evict hot root/internal
  nodes.
- **Epoch-based snapshot readers:** readers pin an epoch and walk a consistent
  snapshot without holding any locks. Writers retire old pages; a reclaimer
  frees them only after all pinned readers have advanced.
- **Layered storage traits:** `PageStorage` (raw page I/O) and `NodeStorage`
  (encoded node I/O) are separate traits, making the node encoding strategy
  pluggable without touching the page layer.

---

## Features

- Copy-on-write page mutation via `NodeView` over `[u8; 4096]` pages
- Multiple concurrent writers (optimistic concurrency; CAS on metadata)
- Batched write transactions (stage &rarr; commit &rarr; reclaim)
- Bounded page cache (CLOCK-Pro) with COW-coherent eviction via epoch GC
- Cursor-based range iteration (parent-stack traversal, no sibling pointers)
- Physical fullness handling: large values trigger page splits before reaching max keys
- Pluggable node encoding via `NodeStorage` trait; raw page I/O via `PageStorage` trait
- Multi-tree support: one database directory, many named trees (create, rename, drop, list)
- Manifest-based crash recovery with CRC-framed catalog log
- Superblock and metadata page CRC validation
- Exclusive file locking to prevent multi-process corruption
- Built-in order-preserving codecs for `u64`, `i64`, `String`, `Vec<u8>`
- Typed `Tree<K, V>` API with `KeyCodec` / `ValueCodec` traits
- Thread-safe handles via `Arc`-based storage ownership (no `unsafe` in the public API)

---

## Quick start

```toml
[dependencies]
bplus_store = "0.7.1"
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
use bplus_store::api::Db;

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
use bplus_store::api::Db;

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
- `db.rename_tree(old, new)` — rename a tree (recorded in manifest)
- `db.drop_tree(name)` — remove a tree from the catalog
- `db.list_trees()` — returns all tree names
- `db.close()` — checkpoints the freelist and drops the database
- `db.format_version()` — on-disk format version from the superblock

### `Tree<K, V>`

- `tree.put(&key, &value)` — insert or replace
- `tree.get(&key)` — lookup, returns `Option<V>`
- `tree.contains_key(&key)` — check existence without decoding the value
- `tree.delete(&key)` — remove
- `tree.txn()` — start a batched `WriteTxn`
- `tree.range(&start, &end)` — forward range scan `[start, end)`
- `tree.range_from(&start)` — forward range scan from `start` to end of tree
- `tree.len()` / `tree.is_empty()`
- `tree.height()` — current tree height (1 = single leaf)

### `WriteTxn<K, V>`

- `txn.insert(&key, &value)` — stage an insert
- `txn.delete(&key)` — stage a delete
- `txn.commit()` — atomically apply all staged operations

> `delete` returns an error if the key is not found.
> `commit` returns `Err(ApiError::TxnAborted)` if the retry budget is exhausted.

---

## Multi-writer semantics (OCC)

Writers run in parallel: each captures the committed **128-bit metadata word**, applies its
writes on a staged COW tree, and commits via `compare_exchange` on a single `AtomicU128`
packing `(root_id, height, txn_id)`. If another writer published first, the transaction
rebases from the latest root and retries (bounded). Readers never block writers, and the
monotonic `txn_id` doubles as an ABA guard (no old-pointer retirement needed). Full protocol
in [ARCHITECTURE.md](ARCHITECTURE.md).

---

## Durability and fsync

Each commit: (1) **CAS-publish** the new 128-bit metadata word (visible to in-process readers
immediately); (2) **write** the new metadata to the inactive A/B slot; (3) **`fdatasync()`** once,
flushing both the COW node pages and the metadata page.

Crash safety comes from **A/B metadata-slot alternation + CRC32**, no WAL: a commit writes
`slot = txn_id % 2`, leaving the previous slot intact, and recovery picks the slot with the
highest `txn_id` and a valid CRC. A crash before `fdatasync` (or a torn write) simply rolls
back to the prior commit — the only cost is possibly leaking a few unreachable pages (wasted
space, never data loss). The `sync_data()` fd-flush behaviour and the `O_DIRECT` trade-off are
covered in [ARCHITECTURE.md](ARCHITECTURE.md).

---

## Epoch-based reclamation

Readers pin an **epoch** while walking a snapshot; writers tag retired pages with the current
epoch at commit; a reclaimer frees a page only once every reader older than its retire-epoch
has unpinned. No locks on the read path, no use-after-free. Pin/GC details in
[ARCHITECTURE.md](ARCHITECTURE.md).

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

- **Superblock** (page 0): magic, format version, generation counter, CRC-32C.
- **Manifest**: append-only, CRC-framed log of tree-lifecycle records (`CreateTree`,
  `RenameTree`, `DeleteTree`, `Checkpoint`); truncated trailing records are skipped, CRC
  mismatches reported as corruption.
- **Catalog**: in-memory `TreeId -> TreeMeta`, rebuilt from the manifest.
- **Per-tree metadata**: CRC-validated A/B pages holding `(root_node_id, height, size, txn_id)`;
  commit writes the inactive slot.
- **File lock**: exclusive `flock` on `db.lock`.

---

## Architecture

For a detailed description of the architecture, design decisions, and trade-offs, see
[ARCHITECTURE.md](ARCHITECTURE.md).

```
src/
  api.rs, api/                  # Db, Tree<K,V>, WriteTxn, ApiError
  codec.rs, codec/              # KeyCodec/ValueCodec traits, bincode codecs, kv (API codecs)
  database.rs, database/        # Database, catalog, manifest (reader/writer), metadata, superblock
  bplustree/                    # BPlusTree core: search, insert, delete, commit, transaction
  storage.rs, storage/          # PageStorage, NodeStorage, FilePageStorage, PagedNodeStorage,
                                #   EpochManager, MetadataManager, page cache
  page.rs, page/                # Slotted page layouts (leaf, internal)
  keyfmt.rs, keyfmt/            # Key encoding formats (raw, prefix-compressed)
  layout.rs                     # PAGE_SIZE constant
examples/
  bytes_api.rs                  # Vec<u8> key/value CRUD
  typed_api.rs                  # u64/String with batched transaction
  concurrent_web_store.rs       # Multi-threaded HTTP fetch + concurrent tree writes
  example.rs                    # async HTTP fetch + store
benches/
  bench_insert.rs               # Criterion benchmarks
```

### Layers (bottom → top)

**page** (4 KB slotted pages) → **storage** (`PageStorage` raw I/O · `NodeStorage` encoded I/O ·
`PagedNodeStorage` decoded-node cache) → **database** (`Database<S>`: superblock, manifest,
catalog, tree lifecycle) → **bplustree** (`BPlusTree`: search / insert / delete / CAS-commit,
`WriteTransaction` for batched commits) → **api** (`Db` hands out `Arc`-shared, synchronous
`Tree<K, V>` handles). Each layer is written up in [ARCHITECTURE.md](ARCHITECTURE.md).

---

## Design trade-offs

The core choices — explained in depth in [ARCHITECTURE.md](ARCHITECTURE.md):

- **COW** — every write clones only the touched pages (leaf + ancestors) and publishes a new
  root by CAS; readers see a consistent epoch-pinned snapshot and never block. Same approach as
  LMDB / BoltDB / redb.
- **No sibling pointers** — `next`/`prev` links would cascade-invalidate under COW (a copied
  leaf's new page ID breaks its sibling's pointer). Range scans instead use a **cursor** over a
  root-to-leaf `(node_id, index)` stack (`BPlusTreeIter`); O(log n) per leaf transition, cheap
  since height is 3–5 and parent pages stay hot.
- **Batched OCC writes** — `WriteTransaction` buffers ops and replays against the current root,
  retrying on CAS conflict (a future sort-by-key/bulk-load path would cut COW copies).
- **Physical fullness** — large values can fill a 4 KB page before the tree order is reached,
  splitting at the page level; entries are capped at `MAX_ENTRY_PAYLOAD` (2038 bytes) so two
  always fit per page and splits produce valid halves.

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
  smaller pages reduce write amplification under update-heavy workloads. The superblock
  already records the page size (`page_size`); what remains is making `PAGE_SIZE` a
  runtime value rather than a compile-time `const` and threading it through the page layer.

- **Deferred value compaction on delete** — The slotted leaf page currently
  compacts the value arena on every delete. Under COW this is just in-memory
  byte shuffling, but batched deletes pay the cost repeatedly. Deferring
  compaction until the page needs the space (e.g. before an insert that doesn't
  fit, or before a merge) would avoid redundant repacking within a single
  transaction.

- **Node merge / rebalancing on delete** — Deletes currently remove entries and compact
  within a leaf, but underfull nodes are not merged with (or rebalanced against) a sibling.
  Under delete-heavy workloads this leaves the tree with underfilled pages — extra height
  and wasted space — and deviates from the classic B+tree minimum-occupancy invariant.
  Implementing merge-on-underflow (borrow from a sibling, or merge two underfull nodes and
  drop the separator key from the parent) restores occupancy and keeps the tree compact.

- **Sharded epoch pinning** — `EpochManager::pin()`/`unpin()` currently acquire a
  central `Mutex<HashMap<ThreadId, Epoch>>` on every read operation. Under high reader
  concurrency this serialises the pin/unpin brackets even though the tree walk itself is
  lock-free. Replace with per-thread atomic slots (a `Vec<AtomicU64>` indexed by a
  thread-claimed slot) so that pin is a single atomic store and `oldest_active()` is a
  lock-free scan. This is the approach used by crossbeam-epoch and similar libraries.

---

## License

Dual-licensed under MIT or Apache-2.0. You may choose either license.

## Contact

Paris Mesidis — pmesidis@gmail.com
