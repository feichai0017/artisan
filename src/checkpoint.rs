//! Background checkpointer — drains the [`BufferManager`]'s dirty
//! set on a round-driven schedule and (when safe) truncates the
//! WAL.
//!
//! ## Where this fits
//!
//! v0.1 made checkpoint a **synchronous** call: callers had to
//! invoke [`crate::Tree::checkpoint`] to push the cached root
//! image to disk and reset the WAL. With write-heavy workloads
//! that means either calling it on every op (1 fdatasync / op,
//! kills throughput) or hardly ever (WAL grows unbounded,
//! recovery takes minutes).
//!
//! v0.2 adds a single background thread that runs the same
//! checkpoint sequence on its own schedule:
//!
//! 1. Snapshot the dirty set via
//!    [`BufferManager::snapshot_dirty`].
//! 2. `flush` the WAL writer's buffered records so anything we're
//!    about to drop from the dirty map is durable on the log.
//! 3. [`BufferManager::commit`] each snapshotted blob to the
//!    inner backend.
//! 4. `fdatasync` the backend.
//! 5. If the round drained every blob it observed and no racing
//!    writer has re-added entries, take the WAL lock and atomically
//!    [`WalWriter::truncate`] the log — every record up to that
//!    moment is reflected in a durable blob commit.
//!
//! ## Industrial references
//!
//! - **sled `Flusher`** — same lifecycle (spawn on DB open, stop
//!   on drop), `Arc<AtomicBool>` shutdown flag, `thread::park`
//!   for waker semantics.
//! - **fjall `FlushManager`** — the round/journal-coordination
//!   model: never trim the journal until the corresponding flush
//!   succeeds.
//! - **LeanStore checkpointer** — round-driven dirty set + cache
//!   draining (we follow the same pattern, just with one thread
//!   instead of three because we're embedded).
//! - **fractalbit ancestor** — direct lineage; the
//!   `dirty_blobs_set` + `next_dirty_blobs_set` + `min_txn_id`
//!   per blob shape is theirs, simplified for the single-tenant
//!   case.
//!
//! ## Failure handling
//!
//! - **`commit` fails for a blob**: [`BufferManager::commit`]
//!   restores the drained dirty entry, so the next round retries.
//!   The round does NOT truncate the WAL in this case (failed
//!   blobs still need their records in the log for recovery).
//! - **`backend.flush` fails**: we don't know which writes
//!   landed and which didn't, so we skip truncate and let the
//!   next round retry the whole sequence (idempotent — the same
//!   bytes get rewritten).
//! - **`wal.flush` fails**: skip the whole round; dirty stays
//!   intact for next round.
//!
//! Errors are logged via `eprintln!` for now (a `tracing`
//! integration lands in a follow-up). The thread never panics on
//! a backend error — that would leak the JoinHandle.
//!
//! ## Threading model
//!
//! One thread, parked between rounds via [`thread::park_timeout`].
//! Wake sources:
//!
//! - **Timer** — at most `idle_interval` between rounds even with
//!   no writes.
//! - **Threshold** — writers call [`Checkpointer::wake`] when
//!   `dirty_count` crosses `dirty_blob_threshold`. (Wiring lives
//!   in `Tree::put` / `delete` / `rename`; this module only
//!   exposes the wake primitive.)
//! - **Shutdown** — `Drop` sets the stop flag + unparks the
//!   thread.
//!
//! ## Configuration
//!
//! [`CheckpointConfig::default`] disables the background thread
//! (`enabled: false`) — v0.2 leaves it opt-in until we've shaken
//! out edge cases on real workloads. Enable via
//! [`crate::TreeBuilder::checkpoint`].

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crate::api::errors::Result;
use crate::journal::writer::WalWriter;
use crate::store::backend::Backend;
use crate::store::BufferManager;

/// Background checkpointer policy + cadence.
///
/// Round-driven: each round drains the dirty set, flushes the
/// affected blobs to backend, and atomically truncates the WAL
/// when no in-flight write was raced. See module docs for the
/// full sequence.
#[derive(Debug, Clone)]
pub struct CheckpointConfig {
    /// Master switch. `false` (the default) leaves checkpointing
    /// fully synchronous — callers drive it via
    /// [`crate::Tree::checkpoint`]. `true` spawns one background
    /// thread on tree open and stops it on tree drop.
    pub enabled: bool,
    /// Maximum interval between rounds. The thread parks for up
    /// to this duration; an internal wake-up source can
    /// short-circuit the wait when the dirty set grows past
    /// `dirty_blob_threshold`.
    ///
    /// Smaller values = lower checkpoint latency, more wake-ups
    /// per second. Default 200 ms.
    pub idle_interval: Duration,
    /// Trigger an early round when the BufferManager's dirty
    /// blob count reaches this. A threshold heuristic for
    /// "the dirty set is large enough that the next round is
    /// worth running before `idle_interval` elapses".
    ///
    /// Default 16 — chosen for "many child-blob trees but not
    /// pathological". For a single-root workload, the dirty
    /// count is usually ≤ 1, so the timer dominates.
    pub dirty_blob_threshold: usize,
}

impl Default for CheckpointConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            idle_interval: Duration::from_millis(200),
            dirty_blob_threshold: 16,
        }
    }
}

impl CheckpointConfig {
    /// Convenience constructor: enabled with default cadence.
    #[must_use]
    pub fn enabled() -> Self {
        Self {
            enabled: true,
            ..Self::default()
        }
    }
}

// ---------- thread-local state ----------

struct Shared {
    stop: AtomicBool,
    bm: Arc<BufferManager>,
    wal: Option<Arc<Mutex<WalWriter>>>,
    cfg: CheckpointConfig,
    // Telemetry — read by `Checkpointer` accessors, written only
    // by the thread.
    rounds_attempted: AtomicU64,
    rounds_succeeded: AtomicU64,
    blobs_flushed: AtomicU64,
    truncates: AtomicU64,
    last_dirty_count: AtomicUsize,
}

/// Handle to the background checkpoint thread. Dropping the
/// handle signals shutdown and joins.
pub(crate) struct Checkpointer {
    handle: Option<JoinHandle<()>>,
    shared: Arc<Shared>,
}

impl Checkpointer {
    /// Spawn a checkpoint thread bound to `bm` + optional `wal`.
    /// Returns `None` if `cfg.enabled == false` — the caller
    /// should fall back to synchronous checkpointing in that case.
    #[must_use]
    pub(crate) fn spawn(
        bm: Arc<BufferManager>,
        wal: Option<Arc<Mutex<WalWriter>>>,
        cfg: CheckpointConfig,
    ) -> Option<Self> {
        if !cfg.enabled {
            return None;
        }
        let shared = Arc::new(Shared {
            stop: AtomicBool::new(false),
            bm,
            wal,
            cfg,
            rounds_attempted: AtomicU64::new(0),
            rounds_succeeded: AtomicU64::new(0),
            blobs_flushed: AtomicU64::new(0),
            truncates: AtomicU64::new(0),
            last_dirty_count: AtomicUsize::new(0),
        });
        let thread_shared = Arc::clone(&shared);
        let handle = thread::Builder::new()
            .name("holt-checkpointer".to_owned())
            .spawn(move || run(&thread_shared))
            .expect("OS rejected thread spawn for holt-checkpointer");
        Some(Self {
            handle: Some(handle),
            shared,
        })
    }

    /// Unpark the thread so it runs a round at the next park
    /// boundary (without waiting out the remainder of
    /// `idle_interval`). Safe to call from any thread; no-op if
    /// the thread is already running. Currently exposed for
    /// tests + planned threshold-crossing wake-up from writers.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn wake(&self) {
        if let Some(h) = &self.handle {
            h.thread().unpark();
        }
    }

    // Observability accessors — fed into `Tree::stats` in a
    // follow-up commit. Kept live with `allow(dead_code)` so the
    // metric fields stay populated even before the consumer
    // lands; removing them later if unused is a one-line change.

    /// Number of rounds the thread has attempted (succeeded +
    /// failed).
    #[allow(dead_code)]
    #[must_use]
    pub(crate) fn rounds_attempted(&self) -> u64 {
        self.shared.rounds_attempted.load(Ordering::Relaxed)
    }

    /// Number of rounds that completed without error.
    #[allow(dead_code)]
    #[must_use]
    pub(crate) fn rounds_succeeded(&self) -> u64 {
        self.shared.rounds_succeeded.load(Ordering::Relaxed)
    }

    /// Total blobs flushed across all rounds.
    #[allow(dead_code)]
    #[must_use]
    pub(crate) fn blobs_flushed(&self) -> u64 {
        self.shared.blobs_flushed.load(Ordering::Relaxed)
    }

    /// Number of WAL truncates performed across all rounds.
    #[allow(dead_code)]
    #[must_use]
    pub(crate) fn truncates(&self) -> u64 {
        self.shared.truncates.load(Ordering::Relaxed)
    }
}

impl Drop for Checkpointer {
    fn drop(&mut self) {
        // 1. Signal stop + unpark, then join the background
        //    thread so we know no other thread is touching
        //    `shared` after this point.
        self.shared.stop.store(true, Ordering::SeqCst);
        if let Some(handle) = self.handle.take() {
            handle.thread().unpark();
            // We intentionally ignore the join `Result` — a panic
            // inside the thread is already game over for this
            // tree handle; surfacing it during `Drop` would
            // double-panic on unwind.
            let _ = handle.join();
        }

        // 2. Run a final synchronous round on the calling
        //    thread. This closes the window between the bg
        //    thread's last completed round and now: writes that
        //    landed after that round are in cache + WAL pending
        //    but not in backend, and would be lost when the
        //    BM/WAL Arcs drop. The bg thread is already joined
        //    so we own `shared` exclusively here — no need to
        //    worry about contention with the thread itself, and
        //    `Checkpointer` is the last writer because it lives
        //    in `Tree` and `Tree`'s clones must all have dropped
        //    for the inner `Arc<Checkpointer>` to reach refcount
        //    zero. Errors are logged but not surfaced — `Drop`
        //    can't return them.
        if let Err(e) = run_round(&self.shared) {
            eprintln!("holt: final checkpoint round during shutdown failed: {e}");
        }
    }
}

// ---------- thread main loop ----------

fn run(shared: &Arc<Shared>) {
    loop {
        if shared.stop.load(Ordering::Acquire) {
            break;
        }
        thread::park_timeout(shared.cfg.idle_interval);
        if shared.stop.load(Ordering::Acquire) {
            break;
        }
        if let Err(e) = run_round(shared) {
            // Don't crash the thread on a transient backend
            // error — the dirty entries are restored, the next
            // round retries. We log so it doesn't fail silently.
            eprintln!("holt: checkpoint round failed: {e}");
        }
    }
}

fn run_round(shared: &Arc<Shared>) -> Result<()> {
    shared.rounds_attempted.fetch_add(1, Ordering::Relaxed);

    // 1. Snapshot the dirty set. Concurrent writers' new
    //    `mark_dirty` calls land in a fresh empty map.
    let snap = shared.bm.snapshot_dirty();
    let snap_count = snap.len();
    shared.last_dirty_count.store(snap_count, Ordering::Relaxed);

    if snap.is_empty() {
        // Even with nothing to flush, count as a successful
        // round — the metric tracks "thread is alive and
        // making progress" not "actual work done".
        shared.rounds_succeeded.fetch_add(1, Ordering::Relaxed);
        return Ok(());
    }

    // 2. Flush WAL so every record that mirrors a snap entry
    //    is durable. If this fails, restore the snap and bail —
    //    we can't truncate without a durable log behind us.
    if let Some(wal) = &shared.wal {
        if let Err(e) = wal.lock().unwrap().flush() {
            shared.bm.restore_dirty(snap);
            return Err(e);
        }
    }

    // 3. Per-blob commit. `BufferManager::commit` drains the
    //    dirty entry on success and restores it on failure, so
    //    we only need to track whether ANY commit failed (to
    //    decide whether to truncate at the end).
    let mut any_failed = false;
    for (guid, txn_id) in &snap {
        if let Err(e) = shared.bm.commit(*guid) {
            // BufferManager::commit already put the entry back;
            // we just record the failure for the truncate gate.
            eprintln!(
                "holt: checkpoint commit failed for blob (min_txn={txn_id}): {e}"
            );
            any_failed = true;
        } else {
            shared.blobs_flushed.fetch_add(1, Ordering::Relaxed);
        }
    }

    // 4. fdatasync the backend so every commit() above is on
    //    stable storage before we drop WAL records.
    if let Err(e) = Backend::flush(shared.bm.as_ref()) {
        eprintln!("holt: checkpoint backend flush failed: {e}");
        return Err(e);
    }

    // 5. Truncate the WAL atomically iff every snapshotted commit
    //    landed AND no racing writer has re-dirtied anything
    //    since the round started. The WAL-lock-held dirty_count
    //    check is what makes this safe: a writer that called
    //    `mark_dirty` will appear in `dirty_count`, and a writer
    //    still inside its write guard hasn't called `mark_dirty`
    //    yet but also hasn't appended to the WAL — so truncating
    //    can't lose their record (there is none yet).
    if !any_failed {
        if let Some(wal) = &shared.wal {
            let mut w = wal.lock().unwrap();
            if shared.bm.dirty_count() == 0 {
                w.truncate()?;
                shared.truncates.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    shared.rounds_succeeded.fetch_add(1, Ordering::Relaxed);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::backend::MemoryBackend;
    use std::sync::Arc;
    use std::time::Instant;

    fn make_bm() -> Arc<BufferManager> {
        Arc::new(BufferManager::new(Arc::new(MemoryBackend::new()), 8))
    }

    #[test]
    fn disabled_config_spawns_nothing() {
        let bm = make_bm();
        let cfg = CheckpointConfig::default();
        assert!(!cfg.enabled);
        let ck = Checkpointer::spawn(bm, None, cfg);
        assert!(ck.is_none());
    }

    #[test]
    fn spawn_and_drop_is_leak_free() {
        let bm = make_bm();
        let cfg = CheckpointConfig::enabled();
        let ck = Checkpointer::spawn(bm, None, cfg).expect("spawn");
        // Give the thread a tick to wake at least once.
        thread::sleep(Duration::from_millis(50));
        drop(ck);
        // If shutdown deadlocked, this test would hang. The test
        // harness has a per-test timeout; if we reach here we're
        // clean. The previous-round counters are sticky on Shared
        // but that lives inside the Drop'd Checkpointer.
    }

    #[test]
    fn round_drains_dirty_set() {
        let bm = make_bm();

        // Seed a dirty blob.
        bm.mark_dirty([0x42; 16], 10);
        assert_eq!(bm.dirty_count(), 1);

        // Run a single round synchronously (no thread).
        let shared = Arc::new(Shared {
            stop: AtomicBool::new(false),
            bm: Arc::clone(&bm),
            wal: None,
            cfg: CheckpointConfig::enabled(),
            rounds_attempted: AtomicU64::new(0),
            rounds_succeeded: AtomicU64::new(0),
            blobs_flushed: AtomicU64::new(0),
            truncates: AtomicU64::new(0),
            last_dirty_count: AtomicUsize::new(0),
        });
        run_round(&shared).unwrap();

        assert_eq!(bm.dirty_count(), 0, "round should drain dirty set");
        assert_eq!(shared.rounds_succeeded.load(Ordering::Relaxed), 1);
        // No cached blob existed for the GUID — `commit` was a
        // no-op but still cleared the dirty entry. Snapshot drains
        // it regardless of cache presence.
    }

    #[test]
    fn empty_round_is_noop_but_counts() {
        let bm = make_bm();
        let shared = Arc::new(Shared {
            stop: AtomicBool::new(false),
            bm,
            wal: None,
            cfg: CheckpointConfig::enabled(),
            rounds_attempted: AtomicU64::new(0),
            rounds_succeeded: AtomicU64::new(0),
            blobs_flushed: AtomicU64::new(0),
            truncates: AtomicU64::new(0),
            last_dirty_count: AtomicUsize::new(0),
        });
        run_round(&shared).unwrap();
        assert_eq!(shared.rounds_succeeded.load(Ordering::Relaxed), 1);
        assert_eq!(shared.blobs_flushed.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn wake_short_circuits_idle_wait() {
        let bm = make_bm();
        let mut cfg = CheckpointConfig::enabled();
        // Long idle so we know the wake — not the timer —
        // produced the round.
        cfg.idle_interval = Duration::from_secs(10);
        let ck = Checkpointer::spawn(bm.clone(), None, cfg).expect("spawn");

        // Mark dirty + wake; expect a round to drain it well under
        // the configured idle.
        bm.mark_dirty([0x01; 16], 1);
        ck.wake();

        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            if ck.rounds_succeeded() >= 1 && bm.dirty_count() == 0 {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "checkpointer never drained dirty set after wake"
            );
            thread::sleep(Duration::from_millis(5));
        }
    }
}
