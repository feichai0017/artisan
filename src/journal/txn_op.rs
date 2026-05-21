//! TxnOp variants — durable logical redo records.
//!
//! Each variant carries the minimal info needed to replay the
//! operation deterministically during WAL recovery.

/// Transaction-op variants emitted by the public tree API.
///
/// Variant tags are stable on-disk constants — see the `TY_*`
/// block in [`super::codec`]. Never renumber; only append.
// `seq` fields are populated on decode (from the record header) and
// verified via codec round-trip tests, but production replay consumes
// the per-record `seq` via the callback's separate parameter rather
// than re-reading it off the variant. Allow dead_code so the lint
// doesn't fire on those fields in non-test builds.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub enum TxnOp {
    /// Single-key insert / update.
    ///
    /// Replay only redoes from `(key, value)`; there is no
    /// `prev_value` field because replay never undoes (it's an
    /// idempotent forward redo) and holt does not provide a
    /// journal-scan audit surface.
    Insert {
        /// Owning tree root identifier.
        tree_id: u64,
        /// MVCC seq this op was committed at.
        seq: u64,
        /// Key bytes.
        key: Vec<u8>,
        /// New value bytes.
        value: Vec<u8>,
    },
    /// Single-key erase.
    ///
    /// Carries only the key — replay redoes the erase from `key`
    /// alone. The prior value is not retained on disk: the blind
    /// `Tree::delete` walker never reads it, and the returning
    /// `Tree::remove` walker hands it straight to the caller
    /// without round-tripping through the WAL.
    Erase {
        /// Owning tree root identifier.
        tree_id: u64,
        /// MVCC seq this op was committed at.
        seq: u64,
        /// Key bytes.
        key: Vec<u8>,
    },
    /// Atomic in-tree rename.
    RenameObject {
        /// Owning tree root identifier.
        tree_id: u64,
        /// MVCC seq.
        seq: u64,
        /// Source key.
        src_key: Vec<u8>,
        /// Destination key.
        dst_key: Vec<u8>,
        /// Overwrite if dst exists.
        force: bool,
    },
    /// Batch — one WAL record carrying multiple primitive ops so a
    /// crash either replays all of them or none.
    ///
    /// Emitted by [`crate::Tree::txn`]. Inner ops are primitive
    /// variants only (`Insert` / `Erase` / `RenameObject` today);
    /// nested `Batch`es are rejected at encode + decode. Each
    /// inner op carries `seq = outer_seq + index`; the outer
    /// record's header `SEQ` is the base, and the WAL allocator
    /// reserves a contiguous range of `ops.len()` seqs per batch.
    Batch {
        /// Owning tree root identifier.
        tree_id: u64,
        /// Inner ops, applied in order.
        ops: Vec<TxnOp>,
    },
}
