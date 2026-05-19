//! `TxnBatch` — buffer multiple ops for a single-record WAL commit.
//!
//! Companion to [`super::tree::Tree::txn`]. The batch is a
//! write-only accumulator: each `put` / `delete` / `rename` call
//! copies its inputs into the pending list. Nothing touches the
//! tree until the closure passed to `Tree::txn` returns; then
//! `Tree::apply_batch` drains the pending list under the
//! rename-lock and emits one `TxnOp::Batch` WAL record.
//!
//! Atomicity contract is documented on `Tree::txn` — short
//! version: crash-atomic (replay sees all-or-nothing), not
//! runtime-isolated (other writers can still interleave on
//! disjoint blobs).

/// Builder for a batched transaction. See [`super::tree::Tree::txn`].
#[derive(Debug, Default)]
pub struct TxnBatch {
    pub(crate) pending: Vec<BatchOp>,
}

#[derive(Debug)]
pub(crate) enum BatchOp {
    Put {
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Delete {
        key: Vec<u8>,
    },
    Rename {
        src: Vec<u8>,
        dst: Vec<u8>,
        force: bool,
    },
}

impl TxnBatch {
    /// Buffer a `put(key, value)` to apply when the txn commits.
    pub fn put(&mut self, key: &[u8], value: &[u8]) {
        self.pending.push(BatchOp::Put {
            key: key.to_vec(),
            value: value.to_vec(),
        });
    }

    /// Buffer a `delete(key)` to apply when the txn commits.
    pub fn delete(&mut self, key: &[u8]) {
        self.pending.push(BatchOp::Delete { key: key.to_vec() });
    }

    /// Buffer a `rename(src, dst, force)` to apply when the txn
    /// commits. The semantics match
    /// [`super::tree::Tree::rename`] — missing `src` errors,
    /// `dst` collision errors unless `force` is `true`.
    pub fn rename(&mut self, src: &[u8], dst: &[u8], force: bool) {
        self.pending.push(BatchOp::Rename {
            src: src.to_vec(),
            dst: dst.to_vec(),
            force,
        });
    }

    /// Number of ops queued so far.
    pub fn len(&self) -> usize {
        self.pending.len()
    }

    /// `true` if nothing has been queued. A closure that leaves
    /// the batch empty makes [`super::tree::Tree::txn`] return
    /// without taking the rename lock or emitting a WAL record.
    pub fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }
}
