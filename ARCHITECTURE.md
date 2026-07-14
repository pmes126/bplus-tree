# Architecture

This document describes the internal architecture of the embedded copy-on-write
B+-tree key-value store, its on-disk format, concurrency model, and the design
trade-offs behind each layer.

## Design motivation

bplus_store exists to solve a specific problem: **concurrent readers with
predictable, bounded resource usage in memory-constrained environments.**

The standard approach for concurrent read access in embedded storage engines
is memory-mapped I/O (mmap). LMDB, the most prominent COW B-tree
implementation, relies on mmap for zero-copy reads — readers access pages
directly through mapped memory, and COW semantics ensure they never see
torn writes. This works well on systems with generous virtual address space
and predictable memory pressure, but breaks down in constrained environments:

- **Virtual address space limits.** mmap requires address space proportional
  to the database size. On 32-bit systems, containers with cgroup memory
  limits, or WASM runtimes, this is a hard constraint.
- **Unpredictable I/O latency.** Mapped page accesses can trigger page faults
  that are invisible to the application. The kernel decides when to evict
  pages and when to fault them back in. Under memory pressure, this causes
  latency spikes that the application cannot anticipate or control.
- **No memory budgeting.** The application cannot bound how much physical
  memory the engine consumes. The OS manages the resident set, which means
  one database can starve co-located processes — a problem in multi-tenant
  or edge deployments.

bplus_store takes a different approach: **COW semantics for concurrency,
explicit I/O (pread/pwrite) for control, and a bounded page cache for
performance.**

- **COW** gives readers the same guarantee as LMDB: no locks, no blocking,
  snapshot isolation via immutable page references. Writers never disturb
  active readers.
- **Epoch-based reclamation** (borrowed from concurrent data structure
  literature — crossbeam-epoch, RCU) replaces reference counting or
  free-space bitmaps for tracking which pages are still in use. Readers
  pin a lightweight epoch guard, not a lock or a mapped region.
- **pread-based I/O with a page cache** means the application decides
  exactly how much memory the engine uses. Hot pages (root nodes,
  frequently accessed internal nodes) stay in cache; cold pages are read
  on demand. No kernel surprises, no uncontrolled resident set growth.
- **No mmap dependency** makes the engine portable to environments where
  mmap is unavailable or problematic: WASM runtimes, sandboxed processes,
  embedded devices, and IoT platforms.

The trade-off is explicit: bplus_store pays syscall overhead per cache miss
(one pread per uncached page access), while mmap-based engines access cached
pages at memory speed. For read-heavy workloads with good cache hit rates —
which describes most B-tree access patterns, since upper tree levels are
accessed on every operation — the overhead is small. For workloads that
scan the entire database with poor locality, mmap will be faster.

### How this compares

| Engine | Concurrent readers | I/O model | Memory control | WAL required |
|--------|-------------------|-----------|----------------|-------------|
| SQLite | Readers block writers (WAL mode: concurrent) | pread/pwrite | Application-controlled | Yes |
| LMDB | Lock-free (COW + mmap) | mmap | OS-controlled | No |
| RocksDB | Lock-free (MVCC + LSM) | pread/pwrite + mmap | Mixed | Yes (WAL) |
| **bplus_store** | **Lock-free (COW + epoch)** | **pread/pwrite** | **Application-controlled** | **No** |

bplus_store combines LMDB's lock-free reader model with SQLite's explicit
I/O control. The architecture is: **LMDB's COW concurrency model +
RocksDB-style manifest-based catalog + crossbeam-style epoch-based
reclamation + regular file I/O instead of mmap.**

### Target environments

- Containers and microservices with hard memory limits (cgroups)
- Edge nodes and IoT devices with limited RAM
- Multi-tenant systems where one database must not starve another
- WASM and sandboxed runtimes where mmap is unavailable
- Embedded applications that need concurrent reads with predictable latency

---

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
│                   EpochManager, MetadataManager,│  src/storage/
│                   PageCache (in PagedNodeStorage)│
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

**Header** (8 bytes): `kind(u8) | keyfmt_id(u8) | key_count(u16) | key_block_len(u16) | values_hi(u16)`

**Key block**: Length-prefixed keys packed sequentially: `[u16_le klen | key bytes]...`

**Slot directory**: One `LeafSlot` (4 bytes) per entry: `[val_off: u16, val_len: u16]`,
pointing into the value arena.

**Value arena**: Grows downward from the end of the page buffer. Values are
append-only within a page — an overwrite allocates new space and the old bytes
become garbage. A `compact_values()` pass reclaims the dead space when needed.

**Invariant**: `slots_end <= values_hi` — when this would be violated,
`insert_at` / `replace_at` returns `PageFull`.

**Constants**: `HEADER_SIZE = 8`, `BUFFER_SIZE = 4088`, `SLOT_SIZE = 4`

### Internal page

```
 ┌──────────┬───────────┬────────────────┬───────┐
 │  Header  │ KEY BLOCK │ CHILDREN ARRAY │ FREE  │
 │   8 B    │   var     │    var         │       │
 └──────────┴───────────┴────────────────┴───────┘
 0        8        keys_end          children_end    4096
```

**Header** (6 bytes): `kind(u8) | keyfmt_id(u8) | key_count(u16) | key_block_len(u16)`

**Key block**: Same format as leaf pages.

**Children array**: `key_count + 1` child pointers, each a `u64` node ID (8 bytes).

**Constants**: `HEADER_SIZE = 6`, `BUFFER_SIZE = 4090`, `CHILD_ID_SIZE = 8`

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
   redo. Crash safety comes from the atomicity of the metadata word swap:
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
  code surface. The entire commit fits in a single 128-bit metadata word swap.
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

### Dirty page optimisation (lazy COW)

Within a single transaction, the same page can be modified multiple times
(e.g. two inserts landing in the same leaf, or a split that rewrites an
internal node that was already COW-cloned earlier in the batch). Naively,
each modification would allocate a new page and propagate the new ID all the
way to the root — wasting pages and I/O on intermediate states that no
reader will ever see.

The dirty page optimisation avoids this. Every page allocated during a
transaction is added to a `dirty_pages: HashSet<NodeId>` in the
`TransactionTracker`. When a page is about to be written:

1. **`write_and_propagate`** checks `is_dirty(page_id)`. If the page is
   dirty, it is rewritten **in place** at its existing page ID via
   `write_node_view_at_offset` — no new allocation. Since the page ID is
   unchanged, no parent pointer update is needed, so propagation is skipped
   entirely.
2. **`propagate_node_update`** applies the same check as it walks up the
   ancestor path. When it encounters a dirty parent, it rewrites the parent
   in place and **breaks the loop early** — all ancestors above already
   point to this parent's (unchanged) page ID.

This is safe because dirty pages belong to the current uncommitted
transaction — no reader can reference them (the committed metadata word still
points to the previous root). The result is that a batch of N inserts into
the same leaf region produces far fewer page allocations than N × height,
and the COW debris that would otherwise accumulate within the transaction is
eliminated.

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

Commits use compare-and-swap (CAS) on a 128-bit atomic metadata word for
optimistic concurrency control.

```
Writer                              Shared state
──────                              ────────────
1. Load committed word (Acquire)    ← committed: AtomicU128 {root_id,height,txn_id}
2. Apply writes on COW tree
3. Build new Metadata
4. CAS committed word (SeqCst)      → committed (if unchanged)
   ├─ Success: write to meta slot,
   │           advance epoch, GC
   └─ Failure: free speculative
               pages, return StaleBase
```

### Memory ordering

- `committed` word (`AtomicU128`): writers use `SeqCst` for the compare-exchange, readers use `Acquire`
- Epoch counter (`global_epoch`): `SeqCst` for advances, `Acquire` for loads
- Reader pin/unpin: protected by mutex (implicit Release/Acquire)

**Trade-off:** `SeqCst` on the CAS is stronger than strictly necessary (a
release-acquire pair would suffice for the word swap) but makes the
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

`PagedNodeStorage` maintains a bounded in-memory read cache of decoded
`NodeView`s keyed by page ID, using `quick_cache::sync::Cache` (a concurrent
CLOCK-Pro implementation). Cache correctness relies on COW semantics: a page
ID's content is immutable once written, so cache entries never go stale while
the page is live.

- **Read path**: lock-free lookup via `quick_cache` → hit returns immediately;
  miss falls through to `pread` + decode → insert into cache.
- **Write path**: after writing to disk, the encoded node is inserted into the
  cache so subsequent reads avoid the syscall.
- **Eviction**: the cache is bounded to a configurable capacity (default
  `DEFAULT_CACHE_CAPACITY = 16,384` pages ≈ 64 MB for 4 KB pages). When the
  cache is full, CLOCK-Pro selects victims based on access frequency and
  recency. Additionally, entries are explicitly removed by `free_node` when
  epoch-based GC reclaims a page (since the page ID may be reallocated).

`NodeView` is `Copy + Clone`, so cache hits return by value with no
reference-counting or lifetime entanglement.

**Why CLOCK-Pro?** B+-tree workloads have a natural hot set: root and upper
internal nodes are accessed on every operation, while leaf pages vary with the
key distribution. A naive LRU cache is vulnerable to **scan pollution** — a
single full range scan evicts the hot internal nodes, degrading subsequent
point lookups until they're re-cached. CLOCK-Pro is scan-resistant because it
distinguishes **frequency** from **recency**: a page accessed many times (high
frequency — e.g. root/internal nodes) is protected even if it hasn't been
touched in a few milliseconds, while a page accessed only once (low frequency
— e.g. a leaf visited during a sequential scan) is treated as cold and
evicted first regardless of how recently it was touched. This means a range
scan over 10,000 cold leaves won't evict the 3–4 internal nodes that every
point lookup depends on.

**Why not the standard `lru` crate?** `LruCache::get` requires `&mut self`
(it updates recency on every access), which would degrade `RwLock<LruCache>`
to effectively a `Mutex` — every read takes an exclusive lock. `quick_cache`
uses internal sharding and lock-free reads for concurrent access without
external synchronization.

**Performance trade-off:** The CLOCK-Pro bookkeeping adds ~78 ns per cache
hit compared to a plain `HashMap` lookup (~15% overhead on sequential gets
of 5k keys). This is negligible relative to the cost of a cache miss (one
`pread` syscall: 2–10 µs on SSD). The bounded memory guarantee and scan
resistance justify the per-access overhead for any workload that doesn't fit
entirely in cache.

**Capacity tuning:** `from_parts_with_capacity(store, epoch_mgr, capacity)`
allows callers to set a custom cache size. The right capacity depends on the
working set: for a tree with N live pages, a cache that holds the internal
nodes (roughly `N / fan_out`) guarantees that only leaf-level misses hit disk.
The default (16,384 pages = 64 MB) covers trees up to ~1M entries at typical
fan-out.

**Double-caching and `O_DIRECT`:** because I/O uses buffered `pread`/`pwrite`,
hot pages are cached twice — here *and* in the kernel page cache. `O_DIRECT`
(which bypasses the kernel cache) is relevant **only for RAM-constrained
deployments**: it removes that duplication and hands the reclaimed memory to
this cache. It is **not a general optimisation** — it does not make I/O faster,
it forfeits kernel read-ahead (sequential scans would need their own prefetch),
and it requires sector-aligned buffers. It is only worthwhile when memory is
scarce *and* the working set exceeds this cache; for the typical embedded,
single-process use case it is not worth the complexity. Durability continues to
rely on `fdatasync` either way.

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

### Sources of amplification

1. **Page granularity** — every write is a full 4096-byte page, even for a 20-byte entry.
2. **COW path cloning** — each mutation clones the whole root-to-leaf path (height × 4 KB per
   insert), where an in-place B-tree would rewrite one leaf + a small WAL entry.
3. **Unbatched commits** — each `put()` is a full COW-path + metadata + fsync cycle; `WriteTxn`
   amortises metadata/fsync but still COWs the full path per insert.
4. **COW debris** — old clones/splits pile up in `data.db` within a transaction; the freelist
   reclaims only after commit *and* only once no earlier-epoch reader is pinned, so the file
   grows monotonically mid-transaction.
5. **Metadata/manifest overhead** — manifest, superblock, A/B metadata, freelist snapshot;
   proportionally large for small trees.

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

SA *decreases* as entries grow: fixed metadata/superblock overhead amortises, leaves fill more
densely before splitting, and height grows only logarithmically while data grows linearly.

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

### Why a batched txn's SA is worse than unbatched

Counter-intuitively, batch-inserting 5,000 keys amplifies *more* (~677x) than 5,000 individual
puts (~269x). A batch replays every op against one root chain, and all intermediate COW debris
(pre-split pages, superseded internals/leaves) accumulates until the *single* final commit —
epoch reclamation can't free any of it until then. Unbatched puts commit after each op, so the
epoch advances and the reclaimer can recycle old pages along the way, keeping the file smaller.

### Effect of key ordering

Per-insert COW cost is O(height) pages regardless of order — what changes is *which* pages.
**Random** inserts scatter splits across many leaves, creating debris at every level; **sorted**
inserts land in the same rightmost leaf until it splits, so completed left pages are never
touched again (clean left-to-right fill, fewer distinct internal-node copies). The effect is
modest — the O(height)-per-insert cost remains. **The real win from sorted keys is enabling bulk
loading** (build bottom-up, write each page once), which removes split-and-propagate overhead
entirely. Not currently implemented.

### Possible improvements

Several approaches could reduce space amplification:

1. **Bulk load / merge-rebuild for large sorted batches.** Build (or rebuild) the tree
   bottom-up: stream-merge the incoming batch with the existing tree (`RangeIter`, already
   sorted), pack leaves left-to-right to ~85%, build internal nodes bottom-up, then CAS to the
   new root (the old page set becomes epoch-reclaimable, no concurrency-model changes). Worth it
   when the batch is a large fraction (~>20%) of the tree; otherwise the per-key path is cheaper.
   Crash-safe — the old tree stays intact until the final CAS. Expected impact (5,000 entries):

   | Metric         | Per-key insert | Merge-rebuild |
   |----------------|----------------|---------------|
   | Pages written  | ~15,000        | ~129          |
   | Disk footprint | ~20 MB         | ~516 KB       |
   | Space amp      | ~269x          | ~6.7x         |

2. **Online compaction** — background rewrite of `data.db`, dropping unreachable pages and
   packing live ones (see [Future improvements](#future-improvements)).
3. **Delta / WAL hybrid** — buffer small mutations in a log and apply in bulk (amortises
   per-page overhead; more complex recovery).
4. **Page-level dedup** — detect and reuse an identical COW clone via content hashing.

---

## Concurrency bugs and fixes

Four concurrency bugs found during stress testing with concurrent writers — symptom, root
cause, fix.

### 1. TOCTOU race in `EpochManager::pin()`

**Symptom:** the reclaimer freed pages an active reader was about to traverse (stale reads /
invariant violations).

**Root cause:** `pin()` loaded the global epoch and *then*, as a separate step, registered the
thread in `active_readers`. Between the two, a writer could call `oldest_active()`, see no
readers, and reclaim the epoch the reader was about to register for:

```
Reader                            Writer
epoch = global_epoch.load()
                                  oldest_active() → no readers → reclaim epoch 5
readers.insert(tid, epoch=5)      // too late — pages already freed
```

**Fix:** the epoch load and registration now happen under the *same* mutex lock — so a writer's
`oldest_active()` either sees the reader already registered, or the reader sees the post-advance
epoch.

### 2. Nested epoch pin removing the outer guard's registration

**Symptom:** none observed — a defensive fix.

**Root cause:** `active_readers` is a `HashMap<ThreadId, Epoch>` (one entry per thread). If an
outer `pin()` and an inner `pin()` (e.g. `put` → `put_inner`) both register the same thread, the
inner `ReaderGuard::Drop` calls `readers.remove(tid)` — erasing the entry while the **outer**
guard is still live, so a concurrent `oldest_active()` sees no reader for that thread.

**Fix:** inner methods (`put_inner` / `get_inner` / `delete_inner`) no longer pin (documented as
"caller must hold a guard"); pins live only at the outermost public entry points.

### 3. Missing epoch guard in `WriteTransaction::commit`

**Symptom:** `commit`'s tree walk read freed pages ("expected internal node" invariant violations,
or garbage data).

**Root cause:** `commit` replays buffered ops via inner methods that *assume* the caller holds a
guard — but `commit` didn't pin one, so a concurrent commit's GC could reclaim the root and its
reachable pages mid-walk.

**Fix:** the tree-walk portion of `commit` is wrapped in an epoch guard, pinned before reading
`initial_root_id` and dropped before `try_commit` (so the commit's own reclamation isn't blocked).

### 4. ABA problem on the metadata CAS (`AtomicPtr` → `AtomicU128`)

**Symptom:** silently lost keys — a 4-thread × 500-key × 50-round stress test lost 1–5 keys/round.

**Root cause (original design):** `committed` was an `AtomicPtr<Metadata>`; the CAS compared raw
addresses, and the old `Box` was freed on success. The allocator could then hand the *same*
address to a later `Box::new(Metadata)`, so a slow writer's CAS (`expected = old_addr`) would
match a **recycled** pointer holding entirely different data — succeeding when it should fail and
overwriting a newer tree with a stale root. Classic ABA: same address, different data.

**Fix (current design):** `committed` is now an `AtomicU128` holding a **packed word**
`(root_id, height, txn_id)` instead of a pointer. `txn_id` increases monotonically, so every
published word is unique and a stale `compare_exchange` (comparing the full 128-bit value) can
never match a recycled one. ABA is eliminated at the source: **no heap pointers to free, no
`retired_meta` list, no `unsafe impl Send`** — just an integer CAS.

**Trade-off:** `AtomicU128` isn't natively lock-free everywhere, so the engine uses
`portable_atomic` (lock fallback only on targets without a 128-bit CAS; x86-64 and AArch64 have
it). In return, the old design's per-commit pointer retention and raw-pointer `unsafe` are gone.

---

## Future improvements

### Write-ahead log (WAL)

COW provides crash safety without a WAL: old pages are never modified, and
the A/B metadata slot swap is the atomic commit point. The only gap is
**page leaks** — if the process crashes after the CAS but before `fdatasync`,
newly allocated pages become unreachable from any root. These orphans waste
space but never cause data loss, and a startup reachability scan could
reclaim them.

A WAL is therefore **not needed for durability**, but becomes valuable for
two other reasons:

- **Replication.** A commit log is exactly the stream followers need to replay.
  The per-commit `fsync` a WAL requires is unavoidable when shipping changes
  to replicas, so the cost is already paid. This makes WAL the natural
  foundation for a single-leader log-shipping replication layer (see
  [Replication](#replication) below).
- **Leak recovery.** Recording `CommitIntent` / `CommitComplete` pairs lets
  recovery distinguish completed commits from crashed ones and free any
  leaked pages without a full tree scan.
- **Group commit.** Multiple concurrent transactions can batch their WAL
  entries into a single fsync, amortizing the cost of durable writes.

### Compaction and space reclamation

The data file (`data.db`) never shrinks — freed pages are returned to the
in-memory freelist and reused by future allocations, but the file retains
its high-water mark. After a delete-heavy workload the file may be much
larger than the live data, even though most of the excess pages are on the
freelist and will be recycled. The disk space is reclaimable only by
rewriting the file.

A compaction process could rewrite `data.db`, copying only live pages into
a new file and truncating the result. This would require:

- A page-level reachability walk from the current root.
- A remapping pass that assigns new contiguous page IDs to live pages.
- An atomic swap of the compacted file for the original.

### Replication

Adding a replication layer would turn bplus_store from an embedded storage
engine into a distributed database primitive. The WAL described above is the
natural replication log — each committed record already contains the page
writes and metadata changes a follower needs to replay. A minimal
single-leader log-shipping design:

- The leader appends committed mutations to the WAL (already `fsync`'d for
  durability).
- Followers tail the WAL stream, apply page writes to their local data file,
  and update their committed metadata to match.
- Followers serve read-only queries from their local snapshot, providing
  read scaling and fault tolerance.

This pairs naturally with the existing epoch-based concurrency model —
followers pin their local epoch while serving reads, and the WAL provides
the ordering guarantee that a follower's snapshot is always a consistent
prefix of the leader's history.