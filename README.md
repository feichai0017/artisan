# holt

> A carefully crafted **adaptive radix tree** for path-shaped metadata.

`holt` is an embedded Rust library for storing **hierarchical
keys** — file paths, S3 object names, multi-tenant namespaces,
time-bucketed identifiers — with sub-microsecond lookups, per-blob
concurrency, and crash-safe persistence.

It targets workloads where:

- Keys are **hierarchical / path-shaped** (so prefix compression pays).
- The dominant access is **point lookup + prefix range scan**.
- Concurrency is **high** (many readers + writers across disjoint
  subtrees).
- Latency is **micro-critical** — no LSM compaction stalls, no
  single-writer locks.

It is **not** a general-purpose KV store; if you need full-text or
vector similarity, reach for the right tool. For this shape, holt
should beat LMDB / RocksDB / SQLite on its target workload.

## When to reach for holt

| Engine        | Data structure        | Persistence       | Concurrency        | Notes                                                |
|---------------|-----------------------|-------------------|--------------------|------------------------------------------------------|
| LMDB          | B+tree                | mmap              | Single-writer MVCC | Battle-tested; page chasing for short hot keys.      |
| RocksDB       | LSM                   | SST + WAL         | MVCC               | Compaction stalls; large hot dataset is RAM-heavy.   |
| SQLite        | B-tree                | File              | Single writer      | Convenient, but writer is the bottleneck under load. |
| Sled          | Hybrid LSM            | Log-structured    | Lock-free          | Rust-native, largely unmaintained.                   |
| **holt**      | **Adaptive Radix Tree** | **512 KB blobs** | **Per-blob 3-mode latch** | **Path compression + lookup is O(key.len)** |

ART's lookup cost is `O(key.len)`, not `O(log N)`. For short hot keys
(< 64 bytes), that beats any tree-based competitor. The per-blob
HybridLatch lets N readers traverse disjoint subtrees in parallel
without coordinating.

## Project status

**v0.1 in active development.** The algorithm core (insert / lookup /
erase / rename / range / txn / compact + multi-blob crossings),
persistent backend, physiological WAL with batched transactions,
and the stateful `Tree::range` iterator (prefix anchoring +
`start_after` + S3 delimiter) are all landed. 202 tests (unit +
property-based + crash-and-replay) pass on Ubuntu + macOS CI.

See [`CHANGELOG.md`](CHANGELOG.md) for the per-feature breakdown
and [`ROADMAP.md`](ROADMAP.md) for what's queued (io_uring backend,
background checkpointer, SIMD CRC32, MVCC snapshots).

`cargo bench --bench main` runs a side-by-side comparison with
RocksDB and SQLite across three metadata workload shapes — see
[`benches/README.md`](benches/README.md) for the methodology and
headline numbers.

## Quick taste

```rust
use holt::{Tree, TreeBuilder, TreeConfig, RangeEntry};

// Persistent (default), Unix-only.
let tree = TreeBuilder::new("/var/lib/myapp/meta.holt")
    .buffer_pool_size(128)
    .open()?;

// Or in-memory:
let tree = Tree::open(TreeConfig::memory())?;

tree.put(b"img/01.jpg", b"rgb_data_blob_id_abc")?;
let value = tree.get(b"img/01.jpg")?.unwrap();
tree.delete(b"img/01.jpg")?;
tree.rename(b"old/path", b"new/path", false)?; // atomic

// Batched, crash-atomic transaction (one WAL record).
tree.txn(|batch| {
    batch.put(b"a", b"1");
    batch.put(b"b", b"2");
    batch.delete(b"c");
})?;

// S3-style listing with prefix + delimiter rollup.
for entry in tree.range().prefix(b"img/").delimiter(b'/') {
    match entry? {
        RangeEntry::Key { key, .. } => println!("leaf {key:?}"),
        RangeEntry::CommonPrefix(p) => println!("dir  {p:?}"),
    }
}

tree.checkpoint()?;   // flush WAL + write through + truncate
```

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│ Public API: Tree, TreeBuilder, TxnBatch, RangeIter           │
├──────────────────────────────────────────────────────────────┤
│ Engine: insert / lookup / erase / range / merge / migrate    │
├──────────────────────────────────────────────────────────────┤
│ Concurrency: HybridLatch (3-mode optimistic / shared / excl) │
├──────────────────────────────────────────────────────────────┤
│ Journal: physiological WAL (11 TxnOp variants) + replay      │
├──────────────────────────────────────────────────────────────┤
│ Store: BufferManager (LRU pin/commit) + BlobFrame (512 KB)   │
├──────────────────────────────────────────────────────────────┤
│ Layout: 9 NodeType variants + bit-packed SlotEntry + Header  │
├──────────────────────────────────────────────────────────────┤
│ Backend: MemoryBackend + PersistentBackend (O_DIRECT / NOCACHE) │
└──────────────────────────────────────────────────────────────┘
```

See [`ARCHITECTURE.md`](ARCHITECTURE.md) for the deep dive.
The design draws on Leis et al.'s ART paper (ICDE 2013) for the
four-node-size scheme and LeanStore (ICDE 2018) for the HybridLatch
contract.

## Not on the roadmap

holt is **just the metadata engine** — single-node, embed-in-
your-process, Unix-only. Out of scope:

- **Windows** — `compile_error!`s the crate (Unix `O_DIRECT` /
  `F_NOCACHE` has no Windows analog worth carrying).
- **Object-storage frontend / S3 layer** — no RPC server, no
  multi-tenant bucket registry, no distributed checkpointer.
- **SQL / vector / full-text** — combine with a domain-appropriate
  engine (`+ FAISS` for vectors, `+ Tantivy` for full-text).
- **Replication / consensus** — build above; we'll expose hooks
  (change feed, snapshot transfer) but won't ship Raft.
- **Network server** — this is a library; wrap it in your own RPC.

## License

Licensed under the [MIT licence](LICENSE).
