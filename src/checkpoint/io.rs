//! I/O worker thread — drains the bounded queue and runs
//! `store.write_blobs` / `store.flush` on behalf of the
//! checkpoint planner.
//!
//! ## Why a separate thread
//!
//! Decouples I/O execution from planning so the planner can:
//! 1. Snapshot bytes under a brief shared read guard, then move on.
//! 2. Submit one batch flush task without serialising on each I/O.
//! 3. Keep checkpoint data writes and the pre-delete store
//!    flush on the same executor turn, so the planner pays one
//!    completion handoff on the common path.
//!
//! For the current local-`pread`/`pwrite` store the parallelism
//! gain is modest (single thread, single FD). On Linux with the
//! `io-uring` feature, the I/O thread owns the SQ submit / CQ
//! drain path and feeds the ring with whole checkpoint batches.
//!
//! ## Shutdown
//!
//! The thread terminates on receiving [`IoTask::Stop`]. The
//! `Checkpointer` orchestrator sends one at the end of its `Drop`
//! sequence, after the final synchronous round has drained
//! everything through this same queue.

use crossbeam_channel::{Receiver, Sender};
use std::sync::Arc;

use crate::api::errors::Result;
use crate::store::WriteThroughEntry;

use super::Shared;

/// One-shot completion channel — sized `bounded(1)` so a `send`
/// never blocks. The I/O worker sends `Ok(())` on success and
/// `Err(_)` on failure; the orchestrator receives once.
pub(crate) type Completion = Sender<Result<()>>;

/// Completion payload for the common checkpoint path: write dirty
/// blob images, then run the pre-delete store flush on the same
/// I/O worker turn.
///
/// The two results stay separate because recovery differs:
/// write failure restores the dirty snapshot; sync failure leaves
/// already-retired writes retired but keeps pending deletes and
/// WAL truncation blocked for the next round.
pub(crate) struct FlushBatchAndSyncReport {
    pub(crate) write_result: Result<()>,
    pub(crate) sync_result: Result<()>,
}

pub(crate) type FlushBatchAndSyncCompletion = Sender<FlushBatchAndSyncReport>;

/// Work item handed to the I/O thread via the bounded queue.
pub(crate) enum IoTask {
    /// Common checkpoint fast path: push dirty blob bytes and
    /// immediately run the pre-delete store flush without a
    /// second channel round trip.
    ///
    /// Each entry carries the dirty-map value observed when the
    /// planner drained the snapshot. The I/O worker retires those
    /// values only after the whole store batch succeeds, guarding
    /// against racing writers and arbitrary-prefix partial store
    /// failures.
    FlushBatchAndSync {
        entries: Vec<WriteThroughEntry>,
        on_done: FlushBatchAndSyncCompletion,
    },
    /// `fdatasync` (via `BlobStore::flush`). Used when a round has
    /// no dirty blob batch to combine with, and after pending
    /// deletes mutate the manifest.
    Sync { on_done: Completion },
    /// Graceful stop signal. Sent once during `Checkpointer::Drop`
    /// after the planner has joined and the final round has run.
    Stop,
}

/// Main loop for the I/O thread.
pub(crate) fn run(shared: &Arc<Shared>, rx: Receiver<IoTask>) {
    while let Ok(task) = rx.recv() {
        match task {
            IoTask::FlushBatchAndSync { entries, on_done } => {
                let write_result = shared.bm.write_through_batch(&entries);
                let sync_result = shared.bm.flush_inner();
                let _ = on_done.send(FlushBatchAndSyncReport {
                    write_result,
                    sync_result,
                });
            }
            IoTask::Sync { on_done } => {
                let result = shared.bm.flush_inner();
                let _ = on_done.send(result);
            }
            IoTask::Stop => break,
        }
    }
}
