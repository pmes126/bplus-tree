# Architecture

This document describes the internal architecture of the embedded copy-on-write
B+-tree key-value store, its on-disk format, concurrency model, and the design
trade-offs behind each layer.

## Layer overview

```
┌─────────────────────────────────────────────────┐
│  API layer        Db, Tree<K,V>, WriteTxn       │  src/api/db.rs
├─────────────────────────────────────────────────┤
│  Transaction      WriteTransaction, TxnTracker  │  src/bplustree/transaction.rs
├─────────────────────────────────────────────────┤
│  B+ tree core     BPlusTree, SharedBPlusTree    │  src/bplustree/tree.rs
│                   search, insert, delete,       │
│                   split, merge, commit          │
├─────────────────────────────────────────────────┤
│  Database         Catalog, ManifestLog,         │  src/database.rs
│                   Metadata, Superblock          │  src/database/
├─────────────────────────────────────────────────┤
│  Storage          PageStorage, NodeStorage,     │  src/storage.rs
│                   EpochManager, MetadataManager │  src/storage/
├─────────────────────────────────────────────────┤
│  Page layout      LeafPage, InternalPage        │  src/page/leaf.rs
│                   NodeView                      │  src/page/internal.rs
│                                                 │  src/bplustree/node_view.rs
├─────────────────────────────────────────────────┤
│  Key encoding     KeyBlockFormat, RawFormat     │  src/keyfmt/
├─────────────────────────────────────────────────┤
│  Codec            KeyCodec, ValueCodec          │  src/codec/
└─────────────────────────────────────────────────┘
```

---

## On-disk layout

A database lives in a single directory with three files:

```
<dir>/
  data.db            # All pages: nodes, metadata slots, superblock
  manifest.log       # Append-only catalog change log
  freelist.snapshot  # Optional; written on graceful shutdown
  db.lock            # Exclusive flock held while the database is open
```

### Superblock (page 0 of `data.db`)

```
 0      4      8          16         24         32         40    44    48
 ┌──────┬──────┬──────────┬──────────┬──────────┬──────────┬─────┬─────┐
 │magic │vers  │ gen_id   │page_size │next_pid  │fl_head   │crc32│ pad │
 │"SUPR"│  1   │  u64     │  u64     │  u64     │  u64     │ u32 │ u32 │
 └──────┴──────┴──────────┴──────────┴──────────┴──────────┴─────┴─────┘
```

The CRC-32C covers bytes 0..40 (everything before the checksum field).
Validated on every open; a mismatch is a hard error.

### Manifest log

Each record is CRC-framed:

```
 ┌─────┬─────────┬──────────────────┬─────────┐
 │ tag │ len(LE) │     payload      │ crc32c  │
 │ 1B  │  4B     │     len bytes    │  4B     │
 └─────┴─────────┴──────────────────┴─────────┘
```

Record types:
- `CreateTree` (tag 1) — name, key encoding, format, order, metadata slot IDs, initial root
- `DeleteTree` (tag 2) — tree ID
- `RenameTree` (tag 3) — tree ID, new name

On recovery the manifest is replayed sequentially to rebuild the in-memory
catalog. Truncated trailing records (crash mid-write) are silently skipped.
A CRC mismatch on a complete record is reported as corruption.

**Trade-off:** An append-only log is simple and fast for writes, but recovery
time grows linearly with history. A future compaction pass could collapse the
log into a single checkpoint record.

### Metadata pages (A/B double-buffering)

Each tree has two metadata page slots (`meta_a`, `meta_b`). Commits alternate
between them based on `txn_id % 2`. Each page stores:

```
root_node_id | tree_id | txn_id | height | order | size | crc32
```

The CRC is validated on read. On recovery, the page with the higher valid
`txn_id` wins.

**Trade-off:** Double-buffering means a crash can never corrupt both copies.
The cost is two pages per tree — negligible for typical tree counts.

---

## Catalog and manifest log

### What the catalog is

A `Store` can host multiple independent B+-trees, each identified by a logical
name (e.g. `"users"`, `"sessions"`). The **catalog** is the in-memory routing
table that maps those names to the information needed to open each tree:

```
Catalog
  by_name:  HashMap<String, TreeId>       "users" → 0xA3F1…
  metas:    HashMap<TreeId, TreeMeta>      0xA3F1… → { meta_a, meta_b,
                                                        key_encoding,
                                                        key_format, order, … }
```

`TreeMeta` holds the metadata slot page IDs (`meta_a`, `meta_b`), the key
encoding and format, the tree order, and a cached snapshot of `root_id`,
`height`, and `size` (the authoritative values live in the metadata pages).

The catalog is **purely in-memory** — it is never written to disk as a single
structure. Instead, it is reconstructed on every open by replaying the manifest
log.

### Why the manifest log is needed

The `data.db` file stores B+-tree nodes and per-tree metadata pages, but it
does not store the mapping from tree names to their metadata page locations.
Without that mapping, the database cannot discover which trees exist or where
their metadata lives.

The manifest log solves this. It is an append-only sequence of records that
describes every catalog mutation:

- **`CreateTree`** — records the tree's name, ID, key encoding, format, order,
  and the page IDs of its two metadata slots.
- **`RenameTree`** — maps an existing tree ID to a new logical name.
- **`DeleteTree`** — removes a tree from the catalog.

On recovery, the log is replayed record-by-record to rebuild the catalog from
scratch. Each record is self-contained, so replay is a simple fold:

```
empty catalog  →  apply CreateTree("users", …)
               →  apply CreateTree("sessions", …)
               →  apply RenameTree("sessions" → "active_sessions")
               →  final catalog
```

After replay, for each tree the database reads both metadata pages (A/B) and
picks the one with the higher valid `txn_id` — this reconciles the catalog's
cached `root_id`/`height`/`size` with the true committed state.

### Why not store the catalog in a page?

An alternative design would write the full catalog to a fixed page (or a
dedicated B+-tree). The manifest log approach was chosen because:

1. **Crash safety is simpler.** An append is atomic at the filesystem level
   (write + fsync). Updating a catalog page in-place would need its own
   double-buffering or WAL to avoid torn writes.
2. **No size limit on tree count.** A single page can only hold a fixed number
   of tree entries. The log grows naturally.
3. **Rename and delete are cheap.** They append a small record rather than
   rewriting a page.

The cost is that recovery time is linear in the number of manifest records. For
typical usage (tens to hundreds of trees, created once) this is negligible. If
it became a problem, a compaction pass could collapse the log into a single
checkpoint record.

---

## Page layout

All on-disk data is stored in fixed `PAGE_SIZE = 4096` byte blocks. This
matches the OS virtual memory page and typical filesystem block size, so
reads and writes are naturally aligned with no read-modify-write amplification.

### Leaf page

```
 ┌──────────┬───────────┬──────────┬──────────┬──────────────────────┐
 │  Header  │ KEY BLOCK │ SLOT DIR │   FREE   │    VALUE ARENA  ←    │
 │  10 B    │  var      │  var     │          │    (grows downward)  │
 └──────────┴───────────┴──────────┴──────────┴──────────────────────┘
 0       10         keys_end    slots_end    values_hi           4096
```

**Header** (10 bytes): `kind(u8) | keyfmt_id(u8) | key_count(u16) | key_block_len(u16) | values_hi(u16)`

**Key block**: Length-prefixed keys packed sequentially: `[u16_le klen | key bytes]...`

**Slot directory**: One `LeafSlot` (4 bytes) per entry: `[val_off: u16, val_len: u16]`,
pointing into the value arena.

**Value arena**: Grows downward from the end of the page buffer. Values are
append-only within a page — an overwrite allocates new space and the old bytes
become garbage. A `compact_values()` pass reclaims the dead space when needed.

**Invariant**: `slots_end <= values_hi` — when this would be violated,
`insert_at` / `replace_at` returns `PageFull`.

**Constants**: `HEADER_SIZE = 10`, `BUFFER_SIZE = 4086`, `SLOT_SIZE = 4`

### Internal page

```
 ┌──────────┬───────────┬────────────────┬───────┐
 │  Header  │ KEY BLOCK │ CHILDREN ARRAY │ FREE  │
 │   8 B    │   var     │    var         │       │
 └──────────┴───────────┴────────────────┴───────┘
 0        8        keys_end          children_end    4096
```

**Header** (8 bytes): `kind(u8) | keyfmt_id(u8) | key_count(u16) | key_block_len(u16)`

**Key block**: Same format as leaf pages.

**Children array**: `key_count + 1` child pointers, each a `u64` node ID (8 bytes).

**Constants**: `HEADER_SIZE = 8`, `BUFFER_SIZE = 4088`, `CHILD_ID_SIZE = 8`

---

## Copy-on-write semantics

Every write operation clones the pages it touches rather than mutating in place.
This is the central design decision and affects nearly every other component.

### Why copy-on-write?

The alternative to COW is **in-place mutation** — the approach used by most
traditional B+-tree implementations (e.g. InnoDB, Berkeley DB). In-place
mutation overwrites existing pages directly, which means:

- A crash mid-write can leave a page in a torn, half-written state.
- A **write-ahead log (WAL)** is required to recover from torn writes: before
  mutating a page, write the intended change to a sequential log, fsync the
  log, then apply the change. On crash, replay the log to redo or undo
  incomplete operations.
- Readers that access a page while it is being mutated need **locking** (shared
  read locks, exclusive write locks) or latching at the page level to see a
  consistent state.

COW eliminates both problems at the source:

1. **No torn writes.** The old page is never modified. A new page is written to
   a fresh location. If the write completes, the new page is valid. If the
   process crashes mid-write, the old page is still intact — no recovery
   needed.

2. **No WAL.** Because old pages are untouched, there is nothing to undo or
   redo. Crash safety comes from the atomicity of the metadata pointer swap:
   the tree either points to the old root (pre-commit state) or the new root
   (post-commit state), never to an intermediate state.

3. **Lock-free readers.** A reader captures the current root pointer and
   traverses a frozen snapshot of the tree. Writers create new pages in
   parallel without disturbing that snapshot. No read locks, no reader-writer
   contention, no priority inversion.

4. **Free snapshots.** Every committed root is a complete, immutable snapshot.
   Implementing multi-version concurrency control (MVCC) or point-in-time reads
   is straightforward — just hold onto an older root pointer.

### Where COW shines

COW B+-trees are well-suited for:

- **Read-heavy workloads.** Readers never block and never contend with writers.
  If 95% of operations are reads, COW lets them proceed at full speed with no
  synchronization overhead.
- **Embedded databases.** No WAL means fewer files, simpler recovery, and less
  code surface. The entire commit fits in a single metadata pointer swap.
- **Short-lived write transactions.** Each commit clones `O(height)` pages.
  For a tree of height 3–4 (typical for millions of keys), that's 3–4 page
  allocations per commit — negligible for individual puts.
- **Concurrent readers with occasional writers.** The epoch-based GC ensures
  old pages stay alive as long as any reader needs them, with no lock
  coordination.

### Where COW is costly

- **Write amplification.** Every mutation clones the entire root-to-leaf path,
  even if only one byte changed. A tree of height 4 writes 4 × 4 KB = 16 KB
  per insert. In-place mutation writes only the single leaf page (4 KB), plus
  the WAL entry. However for shallow trees and moderate write rates, the simplicity
  and concurrency benefits of COW can significantly outweigh the extra I/O.
- **Garbage generation.** Every commit produces `O(height)` dead pages that
  must be tracked and eventually reclaimed. The epoch-based GC adds bookkeeping
  overhead and memory pressure from deferred frees.
- **Sustained high-throughput writes.** Under continuous heavy writes, the page
  allocator and GC become bottlenecks. In-place mutation with a batched WAL
  can amortize I/O more effectively in this regime.

For this project — an embedded store with moderate write rates and potentially
many concurrent readers — COW is the right trade-off.

### Why changes must propagate upward

In a COW tree, a modified leaf is written to a **new page** with a new page ID.
But its parent still holds the **old** page ID as a child pointer. If we stop
here, the parent points to the stale, pre-mutation leaf — the write is lost.

So the parent must be updated with the new child pointer. But updating the
parent means writing the parent to a new page too (COW — we never mutate in
place). Now the grandparent has a stale pointer to the old parent. This
continues all the way to the root:

```
Before insert(K):                 After insert(K):

      [Root: P5]                       [Root': P9]       ← new root
       /      \                         /      \
    [P3]      [P4]                  [P3]      [P8]       ← new parent
    /  \      /  \                  /  \      /  \
  [P1] [P2] [L1] [L2]            [P1] [P2] [L1] [L3]    ← new leaf

  Old pages: P5, P4, L2  →  deferred for GC
  New pages: P9, P8, L3  →  written to fresh locations
```

The commit atomically swaps the root pointer from P5 to P9. After the swap:
- Readers that started before the commit still follow P5 → P4 → L2 (old
  snapshot, still valid until the epoch lets the GC reclaim those pages).
- New readers follow P9 → P8 → L3 (new snapshot with the insert).

The upward propagation is the fundamental cost of COW: every write touches
`O(height)` pages. But B+-trees are deliberately wide and shallow — with
an order of 64 and 4 KB pages, a tree of height 4 can hold tens of millions
of entries. Three to four page copies per write is a modest price for the
simplicity and concurrency benefits.

### Write path (`put_inner`)

1. **Validate**: reject entries where `key_len + val_len > MAX_ENTRY_PAYLOAD` (2038 bytes).
   This guarantees at least two entries fit per leaf, so splits always produce
   valid halves.
2. **Pin epoch**: acquire an epoch guard so the GC knows this thread is reading.
3. **Walk to leaf**: `get_insertion_path()` descends from the root, recording
   `(NodeId, child_index)` pairs for each internal node visited.
4. **Insert or replace**: attempt the operation on the leaf.
   - If `PageFull`: split the leaf and retry from the new root (loop).
   - If `keys_len() > max_keys`: logical overflow — split and propagate.
   - Otherwise: write the modified leaf and propagate the new node ID upward.
5. **Propagate**: each ancestor in the path gets a fresh copy with the updated
   child pointer. The old page IDs are recorded for deferred reclamation.

### Split propagation

`propagate_split` walks up the saved path. At each internal node it replaces
the old child pointer with the left half and inserts a separator + right-half
pointer. If the internal node itself overflows (logically or physically), it
splits too and continues upward. If the path is exhausted, a new root is
created.

---

## Physical fullness vs. logical overflow

The tree order (`max_keys`) and the physical page capacity (4096 bytes) are
independent constraints. With small keys and values, `max_keys` is hit first.
With large values, the page fills physically before reaching `max_keys`.

The tree handles both:

| Trigger | Where detected | Action |
|---------|---------------|--------|
| `insert_at` returns `PageFull` | `put_inner` loop | Split leaf, retry from new root |
| `replace_at` returns `PageFull` | `put_inner` loop | Split leaf, retry from new root |
| `keys_len() > max_keys` | After successful insert | Split leaf, propagate |
| `insert_separator_at` returns `PageFull` | `propagate_split` | Split internal node, insert separator into correct half |
| Borrow target returns `PageFull` | `try_borrow_from_{left,right}` | Skip borrow, try merge instead |
| Merge would exceed page capacity | `try_merge_with_{left,right}` | `can_merge_physically()` pre-check; skip merge |
| Both borrow and merge fail | `handle_underflow` | Accept underfull node as-is |

**`MAX_ENTRY_PAYLOAD = BUFFER_SIZE / 2 - PER_ENTRY_OVERHEAD = 2038 bytes`**

This limit ensures two entries always fit per page, so every split produces
two non-empty halves.

**Trade-off:** The accept-underfull fallback means pages can stay below the
minimum fill factor after deletes of large values. This wastes space but
preserves correctness. A future compaction or rebalancing pass could address
this.

---

## Commit protocol

Commits use compare-and-swap (CAS) on an atomic metadata pointer for
optimistic concurrency control.

```
Writer                              Shared state
──────                              ────────────
1. Load committed ptr (Acquire)     ← committed: AtomicPtr<Metadata>
2. Apply writes on COW tree
3. Build new Metadata
4. CAS committed ptr (SeqCst)       → committed (if unchanged)
   ├─ Success: write to meta slot,
   │           advance epoch, GC
   └─ Failure: free speculative
               pages, return StaleBase
```

### Memory ordering

- `committed` pointer: writers use `SeqCst` for the CAS, readers use `Acquire`
- Epoch counter (`global_epoch`): `SeqCst` for advances, `Acquire` for loads
- Reader pin/unpin: protected by mutex (implicit Release/Acquire)

**Trade-off:** `SeqCst` on the CAS is stronger than strictly necessary (a
release-acquire pair would suffice for the pointer swap) but makes the
ordering intent unambiguous and the CAS is not on the hot path.

### Transactions (`WriteTransaction`)

A `WriteTransaction` buffers insert and delete operations, then replays them
against the current root at commit time. If the CAS fails (another writer
committed first), the speculative pages are freed at epoch 0 and the
transaction retries from the new root, up to `MAX_COMMIT_RETRIES = 10`.

**Trade-off:** Optimistic concurrency works well when contention is low.
Under high contention, writers retry and waste allocation — but for an
embedded database, the typical access pattern is single-writer or
low-contention multi-writer.

---

## Epoch-based reclamation

COW creates a garbage collection problem: old pages can't be freed immediately
because concurrent readers may still be traversing them.

```
Global epoch: 5

Writer commits:                    Readers:
  advance epoch → 6                 pin(5)  ← still reading epoch-5 snapshot
  defer pages [A, B] at epoch 5     pin(6)
  advance epoch → 7                 unpin(5)
  reclaim: oldest_active = 6
  → pages at epoch 5 are safe       → free [A, B]
```

1. **Readers** call `pin()` before walking the tree, recording the current
   global epoch.
2. **Writers** tag retired pages with the epoch at which they were replaced,
   then advance the global epoch.
3. **Reclamation** runs after each commit: find the oldest pinned epoch;
   free all pages deferred at earlier epochs.

**Trade-off:** A long-lived reader pin prevents all subsequent GC. This is
acceptable for short-lived read operations but could cause page exhaustion
if a reader holds a pin indefinitely. A future improvement could add
pin-timeout or reader-staleness detection.

---

## Storage trait hierarchy

```
PageStorage              Low-level page I/O (read, write, allocate, free)
    │
    ├── FilePageStorage   Concrete: single flat file, flock-protected
    │
NodeStorage              Higher-level: NodeView encode/decode
    │
    ├── PagedNodeStorage  Wraps PageStorage + codec + EpochManager
    │
HasEpoch                 Access to shared EpochManager
```

`PageStorage` is the extension point for alternative backends (in-memory,
memory-mapped, distributed). The tree core and transaction layer are generic
over `S: PageStorage`.

---

## Key encoding

Keys are stored in a pluggable **key block format** (`KeyBlockFormat` trait)
that controls how keys are packed, searched, and split within a page.

Currently implemented:
- **`RawFormat`** (id 0): length-prefixed keys `[u16_le klen | key bytes]`.
  Simple and general-purpose. O(n) entry lookup by index, O(log n) binary
  search by key.

Planned:
- **`PrefixRestarts`**: prefix-compressed keys with restart points for faster
  seeking in pages with many similar keys.

**Key codecs** (`KeyCodec` trait) encode typed keys into bytes:
- `BeU64` — big-endian u64 (preserves numeric order)
- `ZigZagI64` — zig-zag encoded i64 (preserves signed order)
- `Utf8` — raw UTF-8 bytes (preserves lexicographic order)
- `RawBytes` — passthrough

**Critical invariant:** all key codecs must be order-preserving. The tree
relies on bytewise comparison for binary search and range scans. A codec
that doesn't preserve order will silently corrupt the tree.

---

## File locking

`database::open()` acquires an exclusive `flock` on `db.lock` before touching
any data files. The lock is held for the lifetime of the `Database` struct and
released automatically on drop. A second process attempting to open the same
directory receives `DatabaseError::Locked`.

**Trade-off:** `flock` is advisory on most Unix systems — a misbehaving process
could bypass it. Mandatory locking (e.g., `O_EXCL` on the data file) would be
stronger but less portable and more fragile. Advisory locking is the standard
approach used by SQLite, LMDB, and RocksDB.

---

## Recovery

On `Database::open`:

1. Validate the superblock (magic, version, CRC).
2. Replay the manifest log to rebuild the in-memory catalog.
3. For each tree, read both metadata pages (A/B) and pick the one with the
   higher valid `txn_id` — this is the source of truth for `root_id`, `height`,
   and `size`.
4. Restore the freelist from `freelist.snapshot` if present.

The manifest log, metadata double-buffering, and CRC framing together ensure
that the database can recover to a consistent state after a crash at any point
during a write.

**What's durable after commit:** The metadata page write + fsync is the commit
point. If the process crashes after the CAS but before the metadata fsync,
the next open will see the previous transaction's metadata — the speculative
pages become leaked (not in the freelist, not reachable from any root). A
future consistency check could detect and reclaim these.
