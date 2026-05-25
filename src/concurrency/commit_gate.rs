//! Commit-publish admission gate.
//!
//! Foreground persistent writers enter the shared side while they
//! mutate cached blobs, publish dirty state, and submit the WAL
//! record to the journal worker. Checkpoint enters the exclusive
//! side while it drains dirty state, flushes the journal, and
//! snapshots bytes. This preserves the W2D boundary without
//! serialising writers against each other.

use super::gate::{Gate, GateReadGuard, GateWriteGuard};

#[derive(Debug)]
pub(crate) struct CommitGate {
    gate: Gate,
}

impl Default for CommitGate {
    fn default() -> Self {
        Self::new()
    }
}

impl CommitGate {
    #[must_use]
    pub(crate) const fn new() -> Self {
        Self { gate: Gate::new() }
    }

    /// Admit one foreground writer into the publish section.
    ///
    /// Multiple writers can hold this guard concurrently; the
    /// per-blob `HybridLatch` still serialises conflicting blob
    /// mutations.
    pub(crate) fn enter_writer(&self) -> CommitWriteGuard<'_> {
        CommitWriteGuard {
            _inner: self.gate.enter_shared(),
        }
    }

    /// Block new writers and wait for admitted writers to leave.
    ///
    /// Used only around checkpoint's dirty-drain + journal-flush +
    /// byte-snapshot boundary and the final WAL truncate gate.
    pub(crate) fn enter_checkpoint(&self) -> CommitCheckpointGuard<'_> {
        CommitCheckpointGuard {
            _inner: self.gate.enter_exclusive(),
        }
    }
}

#[derive(Debug)]
pub(crate) struct CommitWriteGuard<'a> {
    _inner: GateReadGuard<'a>,
}

#[derive(Debug)]
pub(crate) struct CommitCheckpointGuard<'a> {
    _inner: GateWriteGuard<'a>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc::sync_channel;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn writers_can_enter_concurrently() {
        let gate = CommitGate::new();
        let _a = gate.enter_writer();
        let _b = gate.enter_writer();
    }

    #[test]
    fn checkpoint_waits_for_admitted_writer_and_blocks_new_writers() {
        let gate = Arc::new(CommitGate::new());
        let writer = gate.enter_writer();

        let ck_gate = Arc::clone(&gate);
        let (ck_started_tx, ck_started_rx) = sync_channel(0);
        let (ck_done_tx, ck_done_rx) = sync_channel(0);
        let ck = thread::spawn(move || {
            ck_started_tx.send(()).unwrap();
            let _ck = ck_gate.enter_checkpoint();
            ck_done_tx.send(()).unwrap();
        });
        ck_started_rx.recv().unwrap();
        assert!(ck_done_rx.recv_timeout(Duration::from_millis(50)).is_err());

        let writer_gate = Arc::clone(&gate);
        let (writer_done_tx, writer_done_rx) = sync_channel(0);
        let blocked_writer = thread::spawn(move || {
            let _w = writer_gate.enter_writer();
            writer_done_tx.send(()).unwrap();
        });
        assert!(
            writer_done_rx
                .recv_timeout(Duration::from_millis(50))
                .is_err(),
            "new writer must wait behind a pending checkpoint"
        );

        drop(writer);
        ck_done_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        ck.join().unwrap();
        writer_done_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        blocked_writer.join().unwrap();
    }
}
