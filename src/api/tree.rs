//! Public `Tree` type — the main user-facing API.
//!
//! ## Internal key encoding
//!
//! Every user-supplied key is padded with a trailing `\0` byte
//! before reaching the walker. This is a standard ART trick to
//! resolve the "strict prefix" case where one key (e.g. `"abc"`)
//! is a prefix of another (e.g. `"abcdef"`): the terminator
//! guarantees the two keys diverge somewhere inside the radix
//! tree (at the `\0` vs `'d'` byte in this example).
//!
//! ## Concurrency model
//!
//! Tree owns an `Arc<BufferManager>`. The BM keeps each cached
//! blob behind its own `RwLock<AlignedBlobBuf>`, so:
//!
//! - **Reads** (`get`) pin the relevant blobs and walk the cached
//!   buffer under shared read-guards. Lock-free against the writer
//!   lock; readers on different blobs progress in parallel, and
//!   readers on the *same* blob also progress in parallel.
//! - **Writes** (`put` / `delete` / `rename`) serialise through
//!   `write_lock` (a process-wide `Mutex<()>`) for the duration of
//!   the walker pass. Inside, the walker pins each touched blob
//!   for **exclusive** write access via the BM's `RwLock`.
//!
//! Per-blob `HybridLatch` (replacing the `Mutex<()>` write_lock
//! with optimistic-concurrency reads + restart) lands in Stage 6
//! phase 2b.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use super::config::{Storage, TreeConfig};
use super::errors::{Error, Result};
use crate::engine;
use crate::layout::{BlobGuid, PAGE_SIZE};
use crate::store::backend::{AlignedBlobBuf, Backend, MemoryBackend};
use crate::store::{BlobFrame, BufferManager};

#[cfg(unix)]
use crate::store::backend::PersistentBackend;

/// An `artisan` tree — your handle to one metadata store.
///
/// Clone the handle to share the same backing store: the
/// `BufferManager` is held via `Arc`. Reads run lock-free against
/// the writer mutex; writers serialise through `write_lock`.
#[derive(Clone)]
pub struct Tree {
    cfg: TreeConfig,
    backend: Arc<BufferManager>,
    /// GUID of the blob holding the tree root. v0.1 uses a fixed
    /// sentinel; multi-tenant trees (post-v0.1) will allocate
    /// per-tree root GUIDs from a manifest.
    root_guid: BlobGuid,
    /// Serialises mutators (`put` / `delete` / `rename`) against
    /// each other. Readers never take this lock — they coordinate
    /// only with the per-blob `RwLock` inside the BM. Stage 6 phase
    /// 2b replaces it with per-blob `HybridLatch` for fully
    /// concurrent writers on disjoint subtrees.
    write_lock: Arc<Mutex<()>>,
    /// Monotonically-increasing sequence stamped on every new
    /// leaf. Stage 5 ties this to the WAL record number.
    next_seq: Arc<AtomicU64>,
}

impl std::fmt::Debug for Tree {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Tree")
            .field("storage", &self.cfg.storage)
            .field("root_guid", &self.root_guid)
            .finish_non_exhaustive()
    }
}

/// Fixed GUID of the root blob in v0.1. Multi-root trees
/// (post-v0.1) will allocate per-tree root GUIDs from a manifest.
pub(crate) const ROOT_BLOB_GUID: BlobGuid = [0; 16];

/// Append the engine's internal terminator byte (`\0`) to a
/// user-supplied key. See the module docs.
#[inline]
fn pad_key(key: &[u8]) -> Vec<u8> {
    let mut padded = Vec::with_capacity(key.len() + 1);
    padded.extend_from_slice(key);
    padded.push(0u8);
    padded
}

impl Tree {
    /// Open a tree using the supplied configuration.
    ///
    /// `TreeConfig::new("/path")` opens a persistent tree at
    /// `"/path"` (the default). `TreeConfig::memory()` opens an
    /// in-memory tree.
    ///
    /// On non-Unix platforms, persistent mode is unavailable;
    /// passing a `Storage::Persistent` config there returns
    /// [`Error::NotYetImplemented`] — fall back to
    /// `TreeConfig::memory()` or supply your own [`Backend`] via
    /// [`Tree::open_with_backend`].
    pub fn open(cfg: TreeConfig) -> Result<Self> {
        let backend: Arc<dyn Backend> = match &cfg.storage {
            Storage::Memory => Arc::new(MemoryBackend::new()),
            Storage::Persistent { dir } => {
                #[cfg(unix)]
                {
                    Arc::new(PersistentBackend::open(dir)?)
                }
                #[cfg(not(unix))]
                {
                    let _ = dir;
                    return Err(Error::NotYetImplemented(
                        "PersistentBackend is Unix-only; use TreeConfig::memory() or supply a Backend via Tree::open_with_backend",
                    ));
                }
            }
        };
        Self::open_with_backend(cfg, backend)
    }

    /// Open a tree with a caller-supplied [`Backend`].
    ///
    /// The supplied backend is **transparently wrapped** with a
    /// [`BufferManager`] of `cfg.buffer_pool_size` blobs.
    /// `BufferManager` owns the in-memory blob cache; the walker
    /// pins blobs from it for both reads and writes — no separate
    /// root buffer in `Tree`.
    ///
    /// If the backend doesn't yet contain a root blob, initialises
    /// an empty one and writes it through, flushing before
    /// returning.
    pub fn open_with_backend(cfg: TreeConfig, backend: Arc<dyn Backend>) -> Result<Self> {
        let bm: Arc<BufferManager> = Arc::new(BufferManager::new(
            backend,
            cfg.buffer_pool_size,
        ));
        let root_guid = ROOT_BLOB_GUID;
        if !bm.has_blob(root_guid)? {
            // Seed an empty root blob and write it through.
            let mut scratch = AlignedBlobBuf::zeroed();
            BlobFrame::init(scratch.as_mut_slice(), root_guid)?;
            bm.write_blob(root_guid, &scratch)?;
            bm.flush()?;
        }
        Ok(Self {
            cfg,
            backend: bm,
            root_guid,
            write_lock: Arc::new(Mutex::new(())),
            next_seq: Arc::new(AtomicU64::new(1)),
        })
    }

    /// Look up `key`. Returns the value bytes, or `None` if no leaf
    /// matches.
    ///
    /// **Zero-copy and lock-free against the writer lock**: pins
    /// each blob via the [`BufferManager`] and walks the cached
    /// buffer under a shared `RwLock` read guard. N readers on
    /// different blobs progress in parallel; readers on the same
    /// blob also progress in parallel via the read-half.
    ///
    /// Transparently follows `BlobNode` crossings — the lookup may
    /// span multiple blobs when the tree has been split by
    /// spillover.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let padded = pad_key(key);
        engine::lookup_multi(&self.backend, self.root_guid, &padded)
    }

    /// Insert or replace `(key, value)`. Returns the previous value
    /// if the key already existed.
    ///
    /// Walks across [`BlobNode`] crossings. When any blob hits
    /// `AllocError::OutOfSpace`, the walker automatically migrates
    /// a subtree out via `splitBlob` and retries — so trees may
    /// grow well past the 512 KB single-blob limit without caller
    /// involvement.
    ///
    /// Mutates the BM-pinned root buffer in place under an
    /// exclusive write guard; the durable write to the inner
    /// backend happens when `flush_on_write` is `true` (the
    /// default) via [`BufferManager::commit`]. Newly-created child
    /// blobs are **always** written through the backend at the
    /// moment of spillover, so crash-recovery never finds a
    /// dangling `BlobNode` pointing at nothing.
    ///
    /// [`BlobNode`]: crate::layout::BlobNode
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<Option<Vec<u8>>> {
        let padded = pad_key(key);
        let seq = self.next_seq.fetch_add(1, Ordering::SeqCst);
        let _w = self.write_lock.lock().unwrap();
        let outcome = engine::insert_multi(
            &self.backend,
            self.root_guid,
            &padded,
            value,
            seq,
        )?;
        if self.cfg.flush_on_write {
            self.backend.commit(self.root_guid)?;
        }
        Ok(outcome.previous)
    }

    /// Remove `key`. Returns the value that was stored at `key`, or
    /// `None` if no leaf matched.
    ///
    /// Walks across [`BlobNode`] crossings. When a child blob
    /// becomes empty as a result of the erase, its parent's
    /// `BlobNode` is freed and the orphaned child blob is dropped
    /// from cache + the inner backend — no GC pass needed.
    ///
    /// [`BlobNode`]: crate::layout::BlobNode
    pub fn delete(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let padded = pad_key(key);
        let _w = self.write_lock.lock().unwrap();
        let outcome = engine::erase_multi(&self.backend, self.root_guid, &padded)?;
        if self.cfg.flush_on_write {
            self.backend.commit(self.root_guid)?;
        }
        Ok(outcome.previous)
    }

    /// Move the value at `src` to `dst` in a single atomic step.
    ///
    /// - Returns [`Error::NotFound`] if `src` has no leaf.
    /// - Returns [`Error::DstExists`] if `dst` already has a leaf
    ///   **and** `force` is `false`.
    /// - When `force` is `true`, any existing leaf at `dst` is
    ///   overwritten.
    ///
    /// Atomic with respect to other writers (`write_lock` is held
    /// for the whole sequence). Stage 5 (WAL) will swap for a
    /// dedicated `RenameTxnOp` so the child-blob writes between
    /// erase and insert commit as one journal record.
    pub fn rename(&self, src: &[u8], dst: &[u8], force: bool) -> Result<()> {
        let src_padded = pad_key(src);
        let dst_padded = pad_key(dst);

        let seq = self.next_seq.fetch_add(1, Ordering::SeqCst);
        let _w = self.write_lock.lock().unwrap();

        // Probe src across all blobs — zero-copy via BM pin.
        let value = match engine::lookup_multi(&self.backend, self.root_guid, &src_padded)? {
            Some(v) => v,
            None => return Err(Error::NotFound),
        };

        // Same key? No-op (seq is already bumped).
        if src == dst {
            return Ok(());
        }

        // Probe dst across all blobs unless overwrite is allowed.
        if !force
            && engine::lookup_multi(&self.backend, self.root_guid, &dst_padded)?.is_some()
        {
            return Err(Error::DstExists);
        }

        // erase(src) + insert(dst, value). Both walk through
        // `BlobNode` crossings and commit any touched child blobs.
        engine::erase_multi(&self.backend, self.root_guid, &src_padded)?;
        engine::insert_multi(
            &self.backend,
            self.root_guid,
            &dst_padded,
            &value,
            seq,
        )?;

        if self.cfg.flush_on_write {
            self.backend.commit(self.root_guid)?;
        }
        Ok(())
    }

    /// Force-commit the BM-cached root blob through to the inner
    /// backend, then run the backend's own durability protocol
    /// (`fdatasync` on persistent; no-op on memory).
    pub fn checkpoint(&self) -> Result<()> {
        self.backend.commit(self.root_guid)?;
        self.backend.flush()?;
        Ok(())
    }

    /// Borrow the active configuration.
    #[must_use]
    pub fn config(&self) -> &TreeConfig {
        &self.cfg
    }

    /// Total bytes a single blob frame consumes — useful for
    /// capacity sizing.
    #[must_use]
    pub const fn page_size() -> u32 {
        PAGE_SIZE
    }
}
