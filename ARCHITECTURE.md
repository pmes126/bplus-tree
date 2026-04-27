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

A `Database` can host multiple independent B+-trees, each identified by a logical
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

### Why not a simple sequential layout?

A simpler page format would store entries as a flat sequence of
`[key_len | key | val_len | val]` records packed one after another. This is
how many tutorials and prototypes lay out B-tree nodes. The slotted-page
layout used here is more complex but solves several concrete problems that
the sequential approach cannot.

**O(1) positional access.**
Binary search on keys produces an index (e.g. "the 5th entry"). With the
slot directory, jumping to entry 5's value is a single 4-byte read from
`slots[5]` to get the offset and length — O(1). With sequential layout,
finding the 5th entry requires scanning from the start, skipping 4
variable-length records — O(n). Every binary search hit pays this scan
cost, which defeats the purpose of binary search.

**Key-only search without touching values.**
The key block packs keys contiguously in memory: `[klen|key][klen|key]...`.
During a tree traversal (search, insert point lookup, range seek), the CPU
only needs to read and compare keys. Values are never loaded. With
sequential layout, keys and values are interleaved —
`[klen|key|vlen|val][klen|key|vlen|val]...` — so scanning keys requires
reading (or at least skipping over) every value. For pages with large values
this means touching most of the page's bytes just to find a key, polluting
CPU cache lines with value data that won't be used.

The slotted layout gives keys spatial locality: they sit in a contiguous
region at the front of the page. A binary search over 30 keys in a
~500-byte key block fits in a few cache lines. In the sequential layout,
those same 30 keys might be scattered across 4 KB of interleaved key-value
data, causing cache misses on every comparison.

**In-place value replacement.**
When a key's value is updated, the new value may be a different size. With
sequential layout, replacing a 10-byte value with a 20-byte value requires
shifting all subsequent entries forward by 10 bytes — O(n) memmove on
every update. With the value arena, the new value is simply appended at
the current `values_hi` watermark (growing downward), and the slot's offset
and length are updated to point to it. The old value bytes become dead space,
reclaimable by `compact_values()` when the page runs low on free space.
This makes updates O(1) regardless of value size change.

**Deletion without data movement.**
Deleting an entry from a sequential layout requires shifting all subsequent
entries backward to close the gap — O(n) memmove. With the slotted layout,
deletion removes the key from the key block and the slot from the slot
directory (both require shifting, but keys and slots are smaller than full
entries), while the value bytes in the arena are simply abandoned as dead
space. More importantly, the slot directory means entry indices remain
stable during the operation — other slots don't need their offsets
recalculated.

**Decoupled key and value growth.**
The key block grows forward from the header; the value arena grows backward
from the end of the page. They share the free space in the middle. This
means a page can accommodate either many small values or fewer large values
without any layout change — the free space pool is unified. With sequential
layout, there is no concept of shared free space; you just pack until you
run out of room, and fragmentation from updates is harder to manage.

**Trade-off:** The slotted layout costs 4 bytes per entry for the slot
directory, and `compact_values()` adds implementation complexity. For a
page with 40 entries, that's 160 bytes of overhead (~4% of the page). This
is a small price for O(1) access and cache-friendly key search.

**In summary:** the sequential layout works fine for read-once,
write-once pages with fixed-size values. The slotted-page layout is
designed for pages that are searched (binary search needs O(1) index
access), updated (value replacement without shifting), and deleted from
(no entry-shifting memmove) — which is exactly what B-tree leaf nodes do.

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
    ├── PagedNodeStorage  Wraps PageStorage + codec + read cache + EpochManager
    │
HasEpoch                 Access to shared EpochManager
```

`PageStorage` is the extension point for alternative backends (in-memory,
memory-mapped, distributed). The tree core and transaction layer are generic
over `S: PageStorage`.

### Page cache (`PagedNodeStorage`)

`PagedNodeStorage` maintains an in-memory read cache of decoded `NodeView`s
keyed by page ID (`RwLock<HashMap<u64, NodeView>>`). Cache correctness relies
on COW semantics: a page ID's content is immutable once written, so cache
entries never go stale while the page is live.

- **Read path**: shared read-lock check → hit returns immediately; miss falls
  through to `pread` + decode → write-lock insert.
- **Write path**: after writing to disk, the encoded node is inserted into the
  cache under a write lock so subsequent reads avoid the syscall.
- **Eviction**: entries are evicted only when `free_node` is called (the page
  is reclaimed by epoch-based GC and may be reallocated to different content).

The cache is unbounded. Because COW produces a bounded number of live pages
(reachable from the current root plus pages pinned by active readers), the
cache size is implicitly bounded by the live page set.

`NodeView` is `Copy + Clone`, so cache hits return by value with no
reference-counting or lifetime entanglement.

**Trade-off:** An LRU cache would bound memory more explicitly, but the
standard `lru` crate's `LruCache::get` requires `&mut self` (it updates
recency on every access), which would degrade `RwLock<LruCache>` to
effectively a `Mutex` — every read takes an exclusive lock. The unbounded
`HashMap` under `RwLock` keeps reads truly concurrent at the cost of relying
on epoch GC for implicit bounding.

---

## Key encoding

Keys are stored in a pluggable **key block format** (`KeyBlockFormat` trait)
that controls how keys are packed, searched, and split within a page.

Currently implemented:
- **`RawFormat`** (id 0): length-prefixed keys `[u16_le klen | key bytes]`.
  Simple and general-purpose. O(n) entry lookup by index. Binary search
  (`seek`) builds an ephemeral offset table in a single linear scan, then
  uses O(1) random access for each probe — total cost is O(n + log n) per
  search, dominated by the single scan. The offset table is a stack-allocated
  `SmallVec<[u16; 512]>` (`OffsetTable`), avoiding heap allocation for
  typical page densities.

Experimental (dead code, not wired into `KeyFormat`):
- `prefix.rs` — prefix-compressed keys.

This exists as a prototype but is not enabled. For typical key sizes (8-byte
u64, short strings) the overhead of prefix compression does not pay off —
the key block is already small enough to fit in a few cache lines.

### Scratch buffers

Hot-path key operations (seek, decode, insert planning) accept a `&mut
ScratchBuf` parameter — a stack-allocated `SmallVec<[u8; 256]>` that avoids
heap allocation for keys up to 256 bytes. Keys exceeding this threshold
transparently spill to the heap. The type and capacity constant are defined
in `src/keyfmt.rs`.

**Key codecs** (`KeyCodec` trait) encode typed keys into bytes:
- `BeU64` — big-endian u64 (preserves numeric order)
- `BeI64` — sign-bit-flip + big-endian i64 (preserves signed numeric order)
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

---

## Space amplification

Space amplification (SA) is the ratio of the on-disk footprint to the logical
data stored. An SA of 1.0x means every byte on disk is user data; anything
above that is overhead. This is closely related to — but distinct from — *write
amplification* (total bytes written vs logical data). This engine's benchmark
measures SA via `dir_size() / data_bytes`. Because `data.db` never shrinks
(freed pages are recycled in-memory but the file keeps its high-water mark),
SA reflects peak disk usage rather than steady-state live data.

### Sources of amplification in this engine

**1. Page granularity.**
Every write is a full 4096-byte page, even if you're only storing a 20-byte
key-value pair. A single insert into a leaf that has room writes the entire
4096-byte page.

**2. Copy-on-write path cloning.**
Every mutation clones every page on the root-to-leaf path. For a tree of
height 3, a single insert writes 3 × 4096 = 12,288 bytes — the leaf plus
every internal node above it. An in-place-mutation B-tree would write only the
single leaf page (4096 bytes) plus a small WAL entry.

**3. Unbatched commits.**
Each individual `put()` is a full commit cycle: COW the path, write metadata,
fsync. The batched `WriteTxn` amortizes metadata and fsync overhead across all
operations in the batch, but each insert within the batch still COWs the full
path independently.

**4. COW debris accumulation.**
Within a transaction, old pages from COW clones and splits are never reused —
they pile up in `data.db`. The freelist only reclaims pages after commit, and
even then only when no reader is pinned at an earlier epoch. This means
`data.db` grows monotonically during a transaction, even though the logical
tree size may be stable.

**5. Metadata and manifest overhead.**
The manifest log, superblock, per-tree metadata pages (A/B), and freelist
snapshot all consume disk space that isn't user data. For small trees this
overhead is proportionally large.

### Measured space amplification

The `bench_metrics` benchmark (`just bench-metrics`) measures SA directly:

```
space_amp = total_file_size(db_directory) / sum(key_len + value_len)
```

Typical results with u64 keys and short string values:

| Entries | Height | Disk (KB) | Data (KB) | Space Amp | Bytes/Entry |
|---------|--------|-----------|-----------|-----------|-------------|
|     100 |      2 |     496   |       1.4 |   365x    |      5,080  |
|   1,000 |      2 |   4,208   |      14.5 |   289x    |      4,309  |
|   5,000 |      3 |  20,732   |      77.0 |   269x    |      4,246  |
|  25,000 |      3 | 103,308   |     404.2 |   256x    |      4,232  |

The ~4,200 bytes/entry figure means roughly one full 4096-byte page per entry
on disk. This makes sense: with COW, every unbatched `put()` creates `height`
new pages, and the old pages are never reclaimed during measurement.

Space amplification *decreases* as entry count grows because:
- The manifest, superblock, and metadata overhead is amortized over more entries.
- Leaves fill more densely before splitting, so the ratio of useful data per
  page improves.
- Tree height grows logarithmically, so the per-insert COW overhead (height
  pages) grows slower than the data volume.

### How this compares

| Engine type                          | Typical SA |
|--------------------------------------|-----------|
| LSM-tree (RocksDB, LevelDB)         |  10–30x   |
| In-place B-tree + WAL (InnoDB, SQLite) |  2–5x  |
| COW B-tree (LMDB, btrfs)            |   3–10x   |
| **This engine (current)**            | **250–680x** |

The gap is large. The primary reason is that old COW pages accumulate in the
data file indefinitely — there is no compaction or page-reuse within a
transaction, and `data.db` never shrinks.

### Why batched txn SA is worse than unbatched

Counter-intuitively, a batched transaction inserting 5,000 keys has *higher*
space amplification (~677x) than 5,000 individual unbatched puts (~269x).

This happens because the batched path replays all operations against a single
root chain. Each `put_with_root` call COWs the full root-to-leaf path, and all
the intermediate COW debris — pre-split pages, old internal nodes, superceded
leaves — accumulates in `data.db` within a single transaction. Epoch-based
reclamation can't free these pages until after commit, so the file keeps
growing with every operation.

The unbatched path, by contrast, commits after each put. Each commit advances
the epoch, which may allow the reclaimer to free old pages (if no readers are
pinned). Over 5,000 individual commits, some old pages get recycled and their
disk slots reused, keeping the file smaller than the batched case where all
debris persists until the single final commit.

### Effect of key ordering on space amplification

Sorting keys before a batched insert helps, though not for the most obvious
reason. The per-insert COW cost is O(height) pages regardless of key order —
every insert rewrites the root-to-leaf path. What changes is *which* pages are
touched:

**Random key order:**
- Each insert may land in a different leaf, requiring COW of a different
  root-to-leaf path. With N inserts touching M distinct leaves, you generate
  up to M × height intermediate pages.
- Splits happen at unpredictable leaves throughout the tree. Each split
  produces two half-full pages plus a new separator propagated upward. Splits
  scattered across many leaves create debris at every level of the tree.

**Sorted key order:**
- Consecutive inserts land in the *same* rightmost leaf until it fills and
  splits. The COW path is the same rightmost path every time, so intermediate
  internal-node copies rewrite the same logical path rather than scattering
  across the tree.
- Splits only happen at the rightmost leaf. This produces a clean, left-to-right
  fill pattern: completed left-sibling pages are never touched again, so their
  COW debris is minimal.
- The tree grows in a single direction, which means fewer total unique pages
  are allocated compared to random insertion.

In practice, sorted inserts reduce SA modestly (by reducing the number of
distinct internal-node copies created during splits). But the fundamental COW
cost — O(height) pages per insert — remains.

**The real win from sorted keys is enabling bulk loading.** If the engine knows
keys arrive in order, it can build the tree bottom-up: fill each leaf to
capacity, write it once, and construct internal nodes after the fact. This
eliminates split-and-propagate overhead entirely and brings space amplification
close to the theoretical minimum (total pages × 4096 / total data bytes).
Bulk loading is not currently implemented.

### Possible improvements

Several approaches could reduce space amplification:

1. **Intra-transaction page reuse.** When a COW clone supersedes a page within
   the same uncommitted transaction, the old page could be immediately reused
   (no reader can reference it since the transaction hasn't committed). This
   would prevent debris accumulation during large batches.

2. **Bulk loading / merge-rebuild for sorted inserts.** For an empty tree,
   build leaves left-to-right, filling each to capacity, then construct
   internal nodes in a single bottom-up pass. For a populated tree, this
   becomes a merge-rebuild: scan the existing tree via `RangeIter` (already
   sorted), merge the sorted incoming keys with the existing stream (like
   merge sort's merge step), and build new leaves bottom-up from the merged
   output. This is essentially a full tree rewrite, so it's most beneficial
   when the incoming batch is large relative to the existing tree (roughly
   >20-30% of existing entries). For smaller batches, the per-key insert
   path is more efficient since it only touches affected pages. A fractional
   variant — rebuilding only the leaf ranges touched by new keys — could
   offer a middle ground but adds implementation complexity.

   **Merge-rebuild design (not yet implemented):**

   *Phase 1 — Merged stream.* Two sorted inputs: the existing tree (via
   `RangeIter`, already in key order) and the incoming batch (sorted by key).
   Merge them like merge sort's merge step: advance whichever has the smaller
   key. On duplicate keys, the incoming value wins (upsert). Delete ops in
   the batch cause the key to be skipped entirely. This is fully streaming —
   only one entry from each side is held in memory at a time.

   *Phase 2 — Build leaves left-to-right.* Walk the merged stream and pack
   entries into leaf pages. Fill each leaf to ~85% capacity (leaving slack
   for future individual inserts that don't warrant a full rebuild). When a
   leaf is full, write it once via `storage.write_node_view()` and record
   its first key and page ID as a separator for the parent level.

   *Phase 3 — Build internal nodes bottom-up.* Take the separators from the
   leaf level and pack them into internal pages the same way. The first
   child pointer becomes `leftmost_child`; subsequent separators are packed
   until the page is full. Repeat upward until a single root node remains.
   Each internal page is written exactly once.

   *Phase 4 — Commit.* CAS the metadata pointer with the new root page ID,
   tree height (number of levels built), and entry count. The old tree's
   entire page set becomes reclaimable via the epoch manager — no changes
   to the concurrency model are needed.

   Expected impact (5,000 entries, u64 keys, short values):

   | Metric          | Per-key insert | Merge-rebuild |
   |-----------------|---------------|---------------|
   | Pages written   | ~15,000       | ~129          |
   | Disk footprint  | ~20 MB        | ~516 KB       |
   | Space amp       | ~269x         | ~6.7x         |

   The improvement grows with batch size since per-key insert is
   O(N × height) pages while merge-rebuild is O(N / fan-out) pages.

   Crash safety: if the process crashes during the build, the old tree is
   intact (metadata was never swapped). Orphaned new pages are leaked,
   same as any failed COW transaction.

   Key decision: when to use merge-rebuild vs per-key insert. A simple
   heuristic is `batch_size > tree.len() * 0.2` — if the batch is more
   than ~20% of the existing tree, rebuild; otherwise use the normal path.

3. **Online compaction.** A background process that rewrites the data file,
   discarding unreachable pages and packing live pages contiguously. This
   reclaims space from accumulated debris without changing the write path.

4. **Delta encoding / WAL hybrid.** Buffer small mutations in a write-ahead
   log and apply them in bulk to pages periodically. This amortizes the
   per-page overhead across many mutations, at the cost of more complex
   recovery.

5. **Page-level deduplication.** If two COW clones of the same page are
   identical (e.g., an internal node rewritten with the same child pointers),
   detect this and reuse the existing page. Requires content hashing.

---

## Concurrency bugs and fixes

This section documents four concurrency bugs discovered during stress testing
with concurrent writers, their root causes, and the fixes applied.

### 1. TOCTOU race in `EpochManager::pin()`

**Symptom:** Under concurrent writes, the reclaimer could free pages that an
active reader was about to traverse, causing stale reads or invariant violations.

**Root cause:** The original `pin()` loaded the global epoch and then, in a
separate step, inserted the thread into the `active_readers` map. Between these
two operations, a writer could call `oldest_active()`, see zero readers, and
reclaim pages at the epoch the reader was about to register for.

```
Reader thread                   Writer thread
─────────────                   ─────────────
epoch = global_epoch.load()     
                                oldest_active() → no readers → reclaim epoch 5
readers.insert(tid, epoch=5)    
// too late — pages are freed
```

**Fix:** The epoch load and reader registration now happen under the same mutex
lock. A writer calling `oldest_active()` will either see the reader already
registered (if it acquired the lock after the reader) or the reader will see the
post-advance epoch (if the writer advanced before the reader acquired the lock).

### 2. Nested epoch pin removing outer guard's registration

**Symptom:** No observed failure in practice — this is a defensive fix.

**Root cause:** `EpochManager` uses `HashMap<ThreadId, Epoch>` — one entry per
thread. `pin()` calls `readers.insert(tid, epoch)` and `ReaderGuard::Drop` calls
`readers.remove(tid)`. If a public method (e.g. `SharedBPlusTree::put`) pins the
epoch and then calls an inner method (e.g. `put_inner`) that also pins, the
situation is:

1. Outer `pin()` inserts `(thread_7, epoch=5)`.
2. Inner `pin()` inserts `(thread_7, epoch=5)` — no-op, same key/value.
3. Inner `ReaderGuard` drops → `readers.remove(thread_7)`. **Entry is gone.**
4. Outer guard is still alive, but the thread is no longer in the readers map.

A concurrent writer calling `oldest_active()` at this point would see no reader
for this thread and could reclaim pages the thread still references.

In the current code this window is effectively zero — the inner method returns
and the outer guard drops immediately after, with no page accesses in between.
The fix is defensive: inner methods (`put_inner`, `get_inner`, `delete_inner`)
no longer pin, and document "caller must hold an epoch guard". Pins are placed
only at the outermost public entry points.

### 3. Missing epoch guard in `WriteTransaction::commit`

**Symptom:** Under concurrent writes, the `commit` method's tree walk could read
freed pages, causing invariant violations ("expected internal node while updating
parents") or silently reading garbage data.

**Root cause:** `WriteTransaction::commit` replays buffered operations by calling
`put_with_root` / `delete_with_root`, which delegate to inner methods that
expect the caller to hold an epoch guard. The transaction's `commit` did not pin
an epoch, so the root and all pages reachable from it could be reclaimed by a
concurrent commit's GC pass while the transaction was walking them.

**Fix:** The tree walk section of `commit` is now wrapped in an epoch guard. The
guard is pinned before reading `initial_root_id` and dropped before calling
`try_commit`, so the commit's own epoch advance and reclamation pass are not
blocked by the pin.

### 4. ABA problem on `AtomicPtr<Metadata>`

**Symptom:** Under concurrent writes, keys were silently lost. A stress test with
4 threads × 500 keys × 50 rounds consistently showed 1-5 missing keys per round.

**Root cause:** The CAS on `committed: AtomicPtr<Metadata>` compares raw pointer
values (memory addresses), not the data they point to. After a successful CAS,
the old metadata `Box` was freed immediately via `drop(Box::from_raw(old_ptr))`.
This returned the heap address to the allocator, which could reuse it for a
future `Box::new(Metadata)`, creating a classic ABA cycle:

```
Writer A                        Writer B                        Writer C
────────                        ────────                        ────────
reads committed → 0x7f00
(saves as base_version)
starts slow tree walk...
                                reads committed → 0x7f00
                                CAS(0x7f00 → 0x7f80) ✓
                                drop(Box(0x7f00))
                                // 0x7f00 is free

                                                                reads committed → 0x7f80
                                                                Box::new(Metadata)
                                                                // allocator returns 0x7f00!
                                                                CAS(0x7f80 → 0x7f00) ✓
                                                                // committed = 0x7f00 again

CAS(expected=0x7f00, new=0x7f90)
// committed is 0x7f00 (from C)
// addresses match → CAS succeeds!
// Writer A overwrites C's tree
// with a root from a stale snapshot.
// All of B's and C's keys are lost.
```

The ABA problem occurs because the CAS cannot distinguish between the original
`0x7f00` (which Writer A based its work on) and the recycled `0x7f00` (which now
holds Writer C's metadata). The pointer value is the same, but it points to
completely different data.

**Fix:** Old metadata pointers are never freed after a successful CAS. Instead,
they are pushed into a `retired_meta: Mutex<Vec<RetiredPtr>>` list. Since the
address is never returned to the allocator, it can never be reused for a new
`Box<Metadata>`, and a stale writer's CAS will always fail (the committed
pointer has moved to a genuinely new address). All retired pointers are freed
when the `BPlusTree` is dropped.

The `RetiredPtr` newtype wraps the raw `*mut Metadata` and implements `Send`
so it can be stored in a `Mutex<Vec<_>>` on a `Send + Sync` struct.

**Trade-off:** Retired metadata boxes (40 bytes each) accumulate for the
lifetime of the tree — one per successful commit. For typical workloads this
is negligible (10,000 commits = 400 KB). A future improvement could use a
tagged pointer or generation counter to eliminate the ABA problem without
retaining old allocations, but the current approach is simple and correct.