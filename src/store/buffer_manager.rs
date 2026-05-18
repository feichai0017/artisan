//! `BufferManager` — LRU-bounded blob cache (Stage 6 phase 1).
//!
//! Sits between a [`Tree`](crate::Tree) and its underlying
//! [`Backend`]. Itself implements `Backend`, so it's a transparent
//! drop-in: callers see the same `read_blob` / `write_blob` /
//! `flush` API, but reads of recently-touched blobs hit the cache
//! and skip the inner backend's I/O.
//!
//! ## Mode: write-through
//!
//! Writes go to **both** the cache and the inner backend in one
//! call. This keeps existing `flush_on_write` semantics intact
//! (every `Tree::put` still writes through to storage) and gives
//! the caching benefit on the read path without changing
//! durability. A future revision will add **write-back** mode
//! with dirty tracking + a background checkpointer (Stage 6
//! phase 3).
//!
//! ## Per-blob locking
//!
//! Each cached blob has its own `RwLock<AlignedBlobBuf>`. Reads
//! and writes on **different** blobs progress without
//! coordinating — the only shared lock is on the cache's HashMap
//! / LRU bookkeeping, which is held for very short windows.
//! On the **same** blob, N readers can run concurrently while
//! writers take exclusive. Full optimistic-concurrency reads via
//! `HybridLatch` ship later in Stage 6 phase 2.
//!
//! ## Pin-and-operate
//!
//! Callers that want to operate on a blob without an intervening
//! 512 KB memcpy use [`BufferManager::pin`] — it returns an
//! `Arc<CachedBlob>` holding the buffer alive in cache. The
//! `Arc`'s strong count keeps eviction at bay. From there:
//!
//! - [`CachedBlob::read`] → `RwLockReadGuard<AlignedBlobBuf>`,
//!   wrap with `BlobFrameRef::wrap(guard.as_slice())` for zero-
//!   copy traversal.
//! - [`CachedBlob::write`] → `RwLockWriteGuard<AlignedBlobBuf>`,
//!   wrap with `BlobFrame::wrap(guard.as_mut_slice())` for in-
//!   place mutation. Don't forget to write-back via `write_blob`
//!   afterwards so the inner backend sees the change.
//!
//! ## Eviction
//!
//! When the cache exceeds `capacity` blobs, the oldest unpinned
//! entry is dropped (LRU policy). "Unpinned" means no outstanding
//! `Arc<CachedBlob>` references outside the cache itself —
//! `Arc::strong_count(entry) == 1` — so eviction skips entries
//! currently being walked under a `pin()`. The cache may
//! temporarily exceed `capacity` while every entry is pinned;
//! it shrinks back as readers drop their handles.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::api::errors::Result;
use crate::layout::BlobGuid;

use super::backend::{AlignedBlobBuf, Backend};

/// LRU-bounded blob cache; see the module docs.
pub struct BufferManager {
    backend: Arc<dyn Backend>,
    capacity: usize,
    state: Mutex<BufferManagerState>,
}

struct BufferManagerState {
    cache: HashMap<BlobGuid, Arc<CachedBlob>>,
    /// LRU list. Back = most recently used; front = oldest.
    lru: VecDeque<BlobGuid>,
}

/// A single cached blob. Callers obtain one via
/// [`BufferManager::pin`] and then take a read/write guard on it
/// to access the underlying 512 KB buffer with zero copies.
///
/// Holding the `Arc<CachedBlob>` prevents the entry from being
/// evicted, so traversals that pin a blob can borrow into it for
/// as long as the pin is alive.
pub struct CachedBlob {
    buf: RwLock<AlignedBlobBuf>,
}

impl CachedBlob {
    /// Shared read access to the underlying buffer. N concurrent
    /// readers across different threads progress in parallel.
    pub fn read(&self) -> RwLockReadGuard<'_, AlignedBlobBuf> {
        self.buf.read().expect("CachedBlob RwLock poisoned")
    }

    /// Exclusive write access to the underlying buffer.
    pub fn write(&self) -> RwLockWriteGuard<'_, AlignedBlobBuf> {
        self.buf.write().expect("CachedBlob RwLock poisoned")
    }
}

impl BufferManager {
    /// Wrap `backend` with a cache of at most `capacity` blobs
    /// (each blob is 512 KB on the heap). A `capacity` of 0 is
    /// clamped to 1.
    #[must_use]
    pub fn new(backend: Arc<dyn Backend>, capacity: usize) -> Self {
        Self {
            backend,
            capacity: capacity.max(1),
            state: Mutex::new(BufferManagerState {
                cache: HashMap::new(),
                lru: VecDeque::new(),
            }),
        }
    }

    /// Maximum number of blobs the cache will retain before
    /// evicting LRU entries.
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Current number of cached blobs.
    #[must_use]
    pub fn cached_count(&self) -> usize {
        self.state.lock().unwrap().cache.len()
    }

    /// Drop every cached entry. The inner backend is untouched.
    /// Useful for tests and to release memory under pressure.
    pub fn clear(&self) {
        let mut state = self.state.lock().unwrap();
        state.cache.clear();
        state.lru.clear();
    }

    /// Internal: look up `guid` in the cache. On a hit, touches
    /// the LRU (moves to back) and returns the entry.
    fn get_cached(&self, guid: BlobGuid) -> Option<Arc<CachedBlob>> {
        let mut state = self.state.lock().unwrap();
        if let Some(entry) = state.cache.get(&guid).cloned() {
            // Move to back of LRU.
            if let Some(pos) = state.lru.iter().position(|g| *g == guid) {
                state.lru.remove(pos);
            }
            state.lru.push_back(guid);
            Some(entry)
        } else {
            None
        }
    }

    /// Internal: insert a freshly-loaded blob into the cache.
    /// Idempotent under concurrent inserts.
    fn insert_into_cache(&self, guid: BlobGuid, contents: &AlignedBlobBuf) {
        let mut state = self.state.lock().unwrap();
        if state.cache.contains_key(&guid) {
            // Another thread populated the cache between our miss
            // and now; touch the LRU and bail.
            if let Some(pos) = state.lru.iter().position(|g| *g == guid) {
                state.lru.remove(pos);
            }
            state.lru.push_back(guid);
            return;
        }
        let entry = Arc::new(CachedBlob {
            buf: RwLock::new(contents.clone()),
        });
        state.cache.insert(guid, entry);
        state.lru.push_back(guid);
        // Evict if over capacity.
        while state.cache.len() > self.capacity {
            if !Self::try_evict_lru(&mut state) {
                break;
            }
        }
    }

    /// Internal: drop the LRU-most cache entry if it's evictable
    /// (no outstanding `Arc` references outside the cache itself).
    /// Returns `true` if an entry was dropped.
    fn try_evict_lru(state: &mut BufferManagerState) -> bool {
        let mut victim_idx = None;
        for (i, guid) in state.lru.iter().enumerate() {
            if let Some(entry) = state.cache.get(guid) {
                if Arc::strong_count(entry) <= 1 {
                    victim_idx = Some((i, *guid));
                    break;
                }
            }
        }
        if let Some((idx, guid)) = victim_idx {
            state.lru.remove(idx);
            state.cache.remove(&guid);
            true
        } else {
            false
        }
    }

    /// Internal: drop `guid` from cache (no-op if not cached).
    fn evict_from_cache(&self, guid: BlobGuid) {
        let mut state = self.state.lock().unwrap();
        state.cache.remove(&guid);
        if let Some(pos) = state.lru.iter().position(|g| *g == guid) {
            state.lru.remove(pos);
        }
    }

    /// Pin a blob in cache and return an `Arc<CachedBlob>` over it.
    ///
    /// On a cache miss, the blob is loaded from the inner backend
    /// into a fresh cache entry first. The returned `Arc` keeps the
    /// entry alive (and unevictable) until it is dropped — callers
    /// should hold pins only as long as they're actively traversing
    /// or mutating, so eviction can make progress under pressure.
    ///
    /// From the returned handle, use [`CachedBlob::read`] for
    /// shared access (compatible with `BlobFrameRef::wrap`) or
    /// [`CachedBlob::write`] for exclusive access (compatible with
    /// `BlobFrame::wrap`).
    pub fn pin(&self, guid: BlobGuid) -> Result<Arc<CachedBlob>> {
        if let Some(entry) = self.get_cached(guid) {
            return Ok(entry);
        }
        // Cache miss — load from inner backend, then take a second
        // lookup so the cache, not our scratch buffer, owns the
        // canonical entry.
        let mut scratch = AlignedBlobBuf::zeroed();
        self.backend.read_blob(guid, &mut scratch)?;
        self.insert_into_cache(guid, &scratch);
        // Almost always cached now; if another thread evicted it
        // in the gap, fall back to a fresh insert with our scratch.
        if let Some(entry) = self.get_cached(guid) {
            return Ok(entry);
        }
        // Pathological: insert raced with eviction. Build an
        // entry directly from scratch and force-insert it.
        let entry = Arc::new(CachedBlob {
            buf: RwLock::new(scratch),
        });
        let mut state = self.state.lock().unwrap();
        state.cache.insert(guid, entry.clone());
        if let Some(pos) = state.lru.iter().position(|g| *g == guid) {
            state.lru.remove(pos);
        }
        state.lru.push_back(guid);
        Ok(entry)
    }

    /// Durably write the cached image of `guid` to the inner backend.
    ///
    /// Used by mutation paths after they've finished editing a
    /// pinned buffer: pin → write-guard → mutate → drop guard →
    /// `commit`. Acquires a shared read-guard on the cache entry,
    /// so multiple commits on different blobs run concurrently and
    /// in-flight readers on the same blob are not blocked.
    ///
    /// If `guid` is **not** in cache the call is a no-op — there
    /// is nothing dirty to commit (the inner backend already has
    /// the canonical bytes). This matches the natural use case of
    /// `Tree::checkpoint` running on a freshly-opened tree before
    /// any mutation has loaded the root into cache.
    pub fn commit(&self, guid: BlobGuid) -> Result<()> {
        if let Some(entry) = self.get_cached(guid) {
            let buf = entry.read();
            self.backend.write_blob(guid, &buf)?;
        }
        Ok(())
    }
}

impl Backend for BufferManager {
    fn read_blob(&self, guid: BlobGuid, dst: &mut AlignedBlobBuf) -> Result<()> {
        // Cache hit?
        if let Some(entry) = self.get_cached(guid) {
            let buf = entry.read();
            dst.as_mut_slice().copy_from_slice(buf.as_slice());
            return Ok(());
        }
        // Cache miss — load from inner backend and cache.
        self.backend.read_blob(guid, dst)?;
        self.insert_into_cache(guid, dst);
        Ok(())
    }

    fn write_blob(&self, guid: BlobGuid, src: &AlignedBlobBuf) -> Result<()> {
        // Transparent write-through: if cached, refresh the
        // cached image; either way, always write to the inner
        // backend in the same call so durability is unchanged.
        if let Some(entry) = self.get_cached(guid) {
            let mut buf = entry.write();
            buf.as_mut_slice().copy_from_slice(src.as_slice());
        }
        self.backend.write_blob(guid, src)
    }

    fn delete_blob(&self, guid: BlobGuid) -> Result<()> {
        self.evict_from_cache(guid);
        self.backend.delete_blob(guid)
    }

    fn list_blobs(&self) -> Result<Vec<BlobGuid>> {
        self.backend.list_blobs()
    }

    fn flush(&self) -> Result<()> {
        // Write-through mode: nothing pending in cache.
        self.backend.flush()
    }

    fn has_blob(&self, guid: BlobGuid) -> Result<bool> {
        // Fast path: check cache without locking the inner backend.
        {
            let state = self.state.lock().unwrap();
            if state.cache.contains_key(&guid) {
                return Ok(true);
            }
        }
        self.backend.has_blob(guid)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::backend::MemoryBackend;

    fn make_buf(byte_at_100: u8) -> AlignedBlobBuf {
        let mut b = AlignedBlobBuf::zeroed();
        b.as_mut_slice()[100] = byte_at_100;
        b
    }

    #[test]
    fn read_caches_after_first_load() {
        let inner: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        inner.write_blob([0xAB; 16], &make_buf(7)).unwrap();

        let bm = BufferManager::new(inner.clone(), 4);
        assert_eq!(bm.cached_count(), 0);

        // First read: miss + populate.
        let mut dst = AlignedBlobBuf::zeroed();
        bm.read_blob([0xAB; 16], &mut dst).unwrap();
        assert_eq!(dst.as_slice()[100], 7);
        assert_eq!(bm.cached_count(), 1);

        // Second read: hit, no growth in cache size.
        bm.read_blob([0xAB; 16], &mut dst).unwrap();
        assert_eq!(bm.cached_count(), 1);
    }

    #[test]
    fn lru_eviction_at_capacity() {
        let inner: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        for i in 0..10u8 {
            let mut g = [0u8; 16];
            g[0] = i;
            inner.write_blob(g, &make_buf(i)).unwrap();
        }

        let bm = BufferManager::new(inner, 4);
        for i in 0..10u8 {
            let mut g = [0u8; 16];
            g[0] = i;
            let mut dst = AlignedBlobBuf::zeroed();
            bm.read_blob(g, &mut dst).unwrap();
        }
        assert_eq!(
            bm.cached_count(),
            4,
            "cache must shrink to capacity after over-fill",
        );

        // The most-recently-loaded GUIDs should be the survivors.
        let state = bm.state.lock().unwrap();
        let mut g_last = [0u8; 16];
        g_last[0] = 9;
        let mut g_first = [0u8; 16];
        g_first[0] = 0;
        assert!(state.cache.contains_key(&g_last));
        assert!(!state.cache.contains_key(&g_first));
    }

    #[test]
    fn write_through_propagates_to_inner_backend() {
        let inner: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let bm = BufferManager::new(inner.clone(), 4);

        bm.write_blob([0xCD; 16], &make_buf(0x42)).unwrap();

        // Inner sees the blob immediately (write-through).
        assert!(inner.has_blob([0xCD; 16]).unwrap());
        let mut dst = AlignedBlobBuf::zeroed();
        inner.read_blob([0xCD; 16], &mut dst).unwrap();
        assert_eq!(dst.as_slice()[100], 0x42);
    }

    #[test]
    fn write_through_updates_cache_if_present() {
        let inner: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        inner.write_blob([0xEF; 16], &make_buf(1)).unwrap();
        let bm = BufferManager::new(inner.clone(), 4);

        // Prime the cache.
        let mut dst = AlignedBlobBuf::zeroed();
        bm.read_blob([0xEF; 16], &mut dst).unwrap();
        assert_eq!(dst.as_slice()[100], 1);

        // Overwrite via the BM.
        bm.write_blob([0xEF; 16], &make_buf(99)).unwrap();

        // Subsequent read through the BM sees the updated value
        // (came from the refreshed cache, not the inner backend).
        bm.read_blob([0xEF; 16], &mut dst).unwrap();
        assert_eq!(dst.as_slice()[100], 99);
    }

    #[test]
    fn delete_evicts_from_cache_and_inner() {
        let inner: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        inner.write_blob([0x33; 16], &make_buf(5)).unwrap();
        let bm = BufferManager::new(inner.clone(), 4);

        // Prime cache.
        let mut dst = AlignedBlobBuf::zeroed();
        bm.read_blob([0x33; 16], &mut dst).unwrap();
        assert_eq!(bm.cached_count(), 1);

        bm.delete_blob([0x33; 16]).unwrap();
        assert_eq!(bm.cached_count(), 0);
        assert!(!inner.has_blob([0x33; 16]).unwrap());
        assert!(!bm.has_blob([0x33; 16]).unwrap());
    }

    #[test]
    fn has_blob_fast_path_avoids_inner_when_cached() {
        let inner: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        inner.write_blob([0x77; 16], &make_buf(11)).unwrap();
        let bm = BufferManager::new(inner.clone(), 4);

        // Prime cache.
        let mut dst = AlignedBlobBuf::zeroed();
        bm.read_blob([0x77; 16], &mut dst).unwrap();

        assert!(bm.has_blob([0x77; 16]).unwrap());
        // Sanity: uncached GUID still works (inner check).
        assert!(!bm.has_blob([0x88; 16]).unwrap());
    }

    #[test]
    fn concurrent_reads_on_different_blobs_progress() {
        use std::thread;

        let inner: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        for i in 0..16u8 {
            let mut g = [0u8; 16];
            g[0] = i;
            inner.write_blob(g, &make_buf(i)).unwrap();
        }

        let bm = Arc::new(BufferManager::new(inner, 16));
        let handles: Vec<_> = (0..8u8)
            .map(|t| {
                let bm = bm.clone();
                thread::spawn(move || {
                    for _ in 0..50 {
                        let mut g = [0u8; 16];
                        g[0] = t * 2; // each thread targets its own blob
                        let mut dst = AlignedBlobBuf::zeroed();
                        bm.read_blob(g, &mut dst).unwrap();
                        assert_eq!(dst.as_slice()[100], t * 2);
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        // All 8 thread targets cached.
        assert_eq!(bm.cached_count(), 8);
    }
}
