//! One checkpoint round — the planner's main work unit, also
//! invoked synchronously by `Checkpointer::Drop` to drain in-flight
//! dirty state before the Tree handle disappears.
//!
//! ## Sequence
//!
//! 0. **Merge pass** (optional, controlled by
//!    `CheckpointConfig::auto_merge`) — walks every reachable blob
//!    and folds any mergeable child back into its parent. Inline
//!    `bm.commit` per merge so the manifest deletion + parent's
//!    new bytes both reach the backend before the round's `Sync`
//!    at step 5.
//! 1. **Snapshot dirty** — atomically drain the BM dirty map.
//!    Concurrent writers' new `mark_dirty` lands in a fresh empty
//!    map and gets picked up by the next round.
//! 2. **Flush WAL** — `sync_data` the writer so every record that
//!    mirrors a snapshotted seq is durable before we drop it.
//! 3. **Submit `Flush` tasks** — snapshot bytes per dirty blob via
//!    `bm.snapshot_bytes` (memcpy under a brief shared read guard),
//!    move the bytes into an `IoTask::Flush`, and push the task to
//!    the I/O thread.
//! 4. **Collect completions** — wait for each task's one-shot
//!    completion. On any failure, restore the corresponding dirty
//!    entry via `bm.restore_dirty` so the next round retries.
//! 5. **Submit `Sync`** — one `IoTask::Sync` after every `Flush`
//!    landed. `fdatasync` of the inner backend, including the
//!    PersistentBackend's manifest persist.
//! 6. **Truncate WAL** — only when (a) no `Flush` failed AND (b)
//!    `bm.dirty_count() == 0` checked **under the WAL lock**. The
//!    interlock with the writer-side `mark_dirty → wal.lock`
//!    ordering ensures we never drop a record whose effect isn't
//!    already in backend.
//!
//! This function is called from two places:
//!
//! - The `checkpoint_thread` main loop in [`super::mod`]
//!   (background path).
//! - `Checkpointer::Drop` (synchronous final round on the calling
//!   thread, after the planner has joined and writers are
//!   guaranteed to be gone).

use crossbeam_channel::bounded;
use std::collections::HashMap;
use std::sync::Arc;

use crate::api::errors::{Error, Result};
use crate::engine;
use crate::layout::BlobGuid;
use crate::store::backend::Backend;
use crate::store::BlobFrame;

use super::io::IoTask;
use super::Shared;

// The round is intentionally a single linear function so the 6
// phases stay readable as one story. Splitting it into helpers
// would hide the interlock between WAL flush / per-blob commit /
// dirty restore / truncate gate.
#[allow(clippy::too_many_lines)]
pub(super) fn run_round(shared: &Arc<Shared>) -> Result<()> {
    use std::sync::atomic::Ordering;

    shared.rounds_attempted.fetch_add(1, Ordering::Relaxed);

    // 0. Optional tree-wide merge pass.
    let merged = if shared.cfg.auto_merge {
        match run_merge_pass(shared) {
            Ok(n) => n,
            Err(e) => {
                eprintln!("holt: checkpoint merge pass failed: {e}");
                0
            }
        }
    } else {
        0
    };
    shared.merges_total.fetch_add(merged, Ordering::Relaxed);

    // 1. Snapshot dirty.
    let snap = shared.bm.snapshot_dirty();
    let snap_count = snap.len();
    shared.last_dirty_count.store(snap_count, Ordering::Relaxed);

    if snap.is_empty() && merged == 0 {
        shared.rounds_succeeded.fetch_add(1, Ordering::Relaxed);
        #[cfg(feature = "tracing")]
        tracing::trace!(target: "holt::checkpoint", "round skipped — nothing dirty");
        return Ok(());
    }

    #[cfg(feature = "tracing")]
    let round_start = std::time::Instant::now();

    // 2. WAL flush.
    if let Some(wal) = &shared.wal {
        if let Err(e) = wal.lock().unwrap().flush() {
            shared.bm.restore_dirty(snap);
            return Err(e);
        }
    }

    // 3. Snapshot bytes + submit Flush tasks.
    let mut completions: Vec<(BlobGuid, u64, crossbeam_channel::Receiver<Result<()>>)> =
        Vec::with_capacity(snap.len());
    let mut failed: HashMap<BlobGuid, u64> = HashMap::new();

    for (guid, txn_id) in &snap {
        // If the blob isn't in cache (eviction raced us, or it was
        // never loaded), skip — `mark_dirty` should never have
        // fired on an uncached blob, but be defensive.
        let Some(bytes) = shared.bm.snapshot_bytes(*guid) else {
            continue;
        };
        let (tx, rx) = bounded(1);
        let task = IoTask::Flush {
            guid: *guid,
            bytes,
            on_done: tx,
        };
        if shared.io_tx.send(task).is_err() {
            // I/O thread is gone (Drop is mid-sequence on another
            // path) — fall back to restoring everything for the
            // next round.
            for (g, t) in &snap {
                failed.entry(*g).or_insert(*t);
            }
            shared.bm.restore_dirty(failed);
            return Err(Error::NotYetImplemented(
                "checkpoint: I/O worker channel closed mid-round",
            ));
        }
        completions.push((*guid, *txn_id, rx));
    }

    // 4. Collect completions.
    for (guid, txn_id, rx) in completions {
        match rx.recv() {
            Ok(Ok(())) => {
                shared.blobs_flushed.fetch_add(1, Ordering::Relaxed);
            }
            Ok(Err(e)) => {
                eprintln!(
                    "holt: checkpoint flush failed for blob {:02x?} (min_txn={txn_id}): {e}",
                    &guid[..4]
                );
                failed.insert(guid, txn_id);
            }
            Err(_) => {
                // Sender dropped before sending — I/O thread died.
                failed.insert(guid, txn_id);
            }
        }
    }

    if !failed.is_empty() {
        shared.bm.restore_dirty(failed.clone());
    }

    // 5. Sync the backend so every Flush above is on stable
    //    storage before we drop WAL records.
    let (sync_tx, sync_rx) = bounded(1);
    if shared
        .io_tx
        .send(IoTask::Sync { on_done: sync_tx })
        .is_err()
    {
        return Err(Error::NotYetImplemented(
            "checkpoint: I/O worker channel closed before Sync",
        ));
    }
    match sync_rx.recv() {
        Ok(Ok(())) => {}
        Ok(Err(e)) => {
            eprintln!("holt: checkpoint backend Sync failed: {e}");
            return Err(e);
        }
        Err(_) => {
            return Err(Error::NotYetImplemented(
                "checkpoint: I/O worker dropped Sync completion",
            ));
        }
    }

    // 6. Truncate WAL atomically iff every snapshot landed AND no
    //    racing writer has re-dirtied (under WAL-lock check).
    if failed.is_empty() {
        if let Some(wal) = &shared.wal {
            let mut w = wal.lock().unwrap();
            if shared.bm.dirty_count() == 0 {
                w.truncate()?;
                shared.truncates.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    shared.rounds_succeeded.fetch_add(1, Ordering::Relaxed);

    #[cfg(feature = "tracing")]
    {
        let elapsed = round_start.elapsed();
        let truncated = failed.is_empty() && shared.wal.is_some() && shared.bm.dirty_count() == 0;
        tracing::info!(
            target: "holt::checkpoint",
            dirty_snapshot = snap_count,
            blobs_flushed = snap_count - failed.len(),
            blobs_failed = failed.len(),
            merged = merged,
            truncated_wal = truncated,
            elapsed_us = elapsed.as_micros() as u64,
            "round complete",
        );
    }

    Ok(())
}

/// Tree-wide merge pass — fold every mergeable `BlobNode` child
/// back into its parent and synchronously commit the parent.
///
/// Returns the cumulative count of children folded.
///
/// Per-blob `bm.commit` runs inline (rather than going through the
/// I/O queue) because:
/// - Merges are rare relative to user writes; the throughput cost
///   is amortised.
/// - `merge_blob` calls `bm.delete_blob` for the child, which
///   updates the manifest in-memory. Persisting that update needs
///   to happen before the round's `Sync` — keeping it inline lets
///   the existing `bm.commit → backend.write_blob` path drive both
///   the parent's new bytes AND the deferred manifest write at
///   step 5.
fn run_merge_pass(shared: &Arc<Shared>) -> Result<u64> {
    let parents = engine::collect_blob_guids(shared.bm.as_ref(), shared.root_guid)?;
    let mut merged_total = 0u64;
    for guid in parents {
        if !shared.bm.has_blob(guid)? {
            continue;
        }
        let pin = shared.bm.pin(guid)?;
        let stats = {
            let mut guard = pin.write();
            let mut frame = BlobFrame::wrap(guard.as_mut_slice());
            engine::try_merge_children(shared.bm.as_ref(), &mut frame)?
        };
        drop(pin);
        if stats.merged > 0 {
            shared.bm.commit(guid)?;
            merged_total += u64::from(stats.merged);
        }
    }
    Ok(merged_total)
}
