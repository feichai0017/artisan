//! Multi-tree database handle.
//!
//! `DB` owns one buffer manager, one WAL, one checkpoint frontier,
//! and any number of named ART roots. A named tree is still a normal
//! [`crate::Tree`] handle; the difference is that all trees opened
//! from the same `DB` share durability and maintenance gates, so a
//! DB-level atomic batch can commit mutations across trees in one
//! WAL record.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use super::atomic::{BatchOp, RecordVersion};
use super::config::TreeConfig;
use super::errors::Result;
use super::stats::OpenStats;
use super::tree::{ensure_root_blob, replay_wal, Tree, TreeRuntime};
use super::view::View;
use crate::concurrency::{CommitGate, Gate};
use crate::journal::codec::BatchEncoder;
use crate::journal::group_commit::Journal;
use crate::layout::BlobGuid;
use crate::store::BufferManager;

const DB_ROOT_TAG: u8 = 0xDB;
const DB_TREE_HASH_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const DB_TREE_HASH_PRIME: u64 = 0x0000_0100_0000_01b3;

#[derive(Clone)]
struct OpenTree {
    root_guid: BlobGuid,
    runtime: TreeRuntime,
}

/// A storage instance containing multiple named [`Tree`] roots.
///
/// Use `Tree` directly when one ART namespace is enough. Use `DB`
/// when a system needs independent logical indexes that still share
/// one WAL and one checkpoint boundary, for example `default`,
/// `lock`, and `write` trees in an MVCC metadata layer.
#[derive(Clone)]
pub struct DB {
    cfg: TreeConfig,
    store: Arc<BufferManager>,
    maintenance_gate: Arc<Gate>,
    next_seq: Arc<AtomicU64>,
    commit_gate: Arc<CommitGate>,
    journal: Option<Arc<Journal>>,
    checkpointer: Option<Arc<crate::checkpoint::Checkpointer>>,
    open_stats: OpenStats,
    trees: Arc<Mutex<HashMap<u64, OpenTree>>>,
}

impl std::fmt::Debug for DB {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DB")
            .field("storage", &self.cfg.storage)
            .finish_non_exhaustive()
    }
}

impl DB {
    /// Open a multi-tree database using the supplied configuration.
    pub fn open(cfg: TreeConfig) -> Result<Self> {
        let bm = Tree::open_buffer_manager(&cfg)?;
        let mut open_stats = OpenStats::default();
        let (journal, next_seq) = match cfg.wal_path() {
            Some(path) => {
                let next_seq = if path.exists() {
                    let start = std::time::Instant::now();
                    let (next_seq, replay_stats) =
                        replay_wal(&path, &bm, |tree_id| Ok(root_guid_for_tree_id(tree_id)))?;
                    open_stats.wal_replay_micros = start.elapsed().as_micros() as u64;
                    open_stats.wal_replay_records = replay_stats.records_seen;
                    open_stats.wal_torn_tail = replay_stats.torn_tail_at.is_some();
                    if let Ok(meta) = std::fs::metadata(&path) {
                        open_stats.wal_replay_bytes = meta.len();
                    }
                    next_seq
                } else {
                    1
                };
                let journal = Journal::open_or_create(&path, 0)?;
                (Some(Arc::new(journal)), next_seq)
            }
            None => (None, 1),
        };

        let maintenance_gate = Arc::new(Gate::new());
        let commit_gate = Arc::new(CommitGate::new());
        let checkpointer = crate::checkpoint::Checkpointer::spawn(
            Arc::clone(&bm),
            journal.clone(),
            Arc::clone(&maintenance_gate),
            Arc::clone(&commit_gate),
            cfg.checkpoint.clone(),
        )
        .map(Arc::new);

        Ok(Self {
            cfg,
            store: bm,
            maintenance_gate,
            next_seq: Arc::new(AtomicU64::new(next_seq)),
            commit_gate,
            journal,
            checkpointer,
            open_stats,
            trees: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    /// Open a named tree inside this DB.
    ///
    /// The name is stable across reopen: opening the same byte string
    /// maps to the same ART root. Tree creation is lazy; the empty
    /// root blob is written through before the handle is returned.
    pub fn open_tree(&self, name: &str) -> Result<Tree> {
        let tree_id = tree_id_for_name(name.as_bytes());
        let open = self.open_tree_state(tree_id)?;
        self.tree_from_state(tree_id, open)
    }

    /// Apply mutations across named trees under one WAL record.
    ///
    /// The closure buffers operations in a [`DBAtomicBatch`]. Holt
    /// validates all guards for every touched tree before applying
    /// any mutation; if a guard fails, the method returns `Ok(false)`
    /// and emits no WAL record.
    pub fn atomic<F>(&self, build: F) -> Result<bool>
    where
        F: FnOnce(&mut DBAtomicBatch),
    {
        let mut batch = DBAtomicBatch::default();
        build(&mut batch);
        if batch.pending.is_empty() {
            return Ok(true);
        }
        self.apply_atomic(batch.pending)
    }

    /// Run a read-only transaction over explicit tree/prefix scopes.
    ///
    /// Holt captures every listed scope under one DB-wide
    /// maintenance gate, releases the live DB, then invokes `read`
    /// with an immutable [`DBView`]. Writes committed after the
    /// capture are invisible to every captured tree view.
    ///
    /// Scopes are explicit to keep the semantic boundary honest:
    /// `DB` has no catalog enumeration yet, so it cannot infer
    /// "all trees" without depending on which handles happened to
    /// be opened in this process.
    pub fn view<F, R>(&self, scopes: &[(&str, &[u8])], read: F) -> Result<R>
    where
        F: FnOnce(&DBView) -> Result<R>,
    {
        let view = {
            let _maintenance = self.maintenance_gate.enter_exclusive();
            let mut trees = HashMap::with_capacity(scopes.len());
            for (name, prefix) in scopes {
                let tree_id = tree_id_for_name(name.as_bytes());
                let open = self.open_tree_state(tree_id)?;
                let tree = self.tree_from_state(tree_id, open)?;
                trees.insert(tree_id, tree.capture_view_unlocked(prefix)?);
            }
            DBView { trees }
        };
        read(&view)
    }

    fn open_tree_state(&self, tree_id: u64) -> Result<OpenTree> {
        let mut trees = self.trees.lock().unwrap();
        if let Some(open) = trees.get(&tree_id) {
            return Ok(open.clone());
        }
        let root_guid = root_guid_for_tree_id(tree_id);
        ensure_root_blob(&self.store, root_guid)?;
        let open = OpenTree {
            root_guid,
            runtime: TreeRuntime::new(),
        };
        trees.insert(tree_id, open.clone());
        Ok(open)
    }

    fn tree_from_state(&self, tree_id: u64, open: OpenTree) -> Result<Tree> {
        Tree::from_shared(
            self.cfg.clone(),
            open.root_guid,
            tree_id,
            Arc::clone(&self.store),
            open.runtime,
            Arc::clone(&self.maintenance_gate),
            Arc::clone(&self.next_seq),
            Arc::clone(&self.commit_gate),
            self.journal.clone(),
            self.checkpointer.clone(),
            self.open_stats,
        )
    }

    fn apply_atomic(&self, pending: Vec<DBBatchOp>) -> Result<bool> {
        let _maintenance = self.maintenance_gate.enter_exclusive();
        let mut groups = Vec::<DBBatchGroup>::new();
        for item in pending {
            let open = self.open_tree_state(item.tree_id)?;
            match groups
                .iter_mut()
                .find(|group| group.tree_id == item.tree_id)
            {
                Some(group) => group.ops.push(item.op),
                None => groups.push(DBBatchGroup {
                    tree_id: item.tree_id,
                    tree: Tree::from_shared(
                        self.cfg.clone(),
                        open.root_guid,
                        item.tree_id,
                        Arc::clone(&self.store),
                        open.runtime,
                        Arc::clone(&self.maintenance_gate),
                        Arc::clone(&self.next_seq),
                        Arc::clone(&self.commit_gate),
                        self.journal.clone(),
                        self.checkpointer.clone(),
                        self.open_stats,
                    )?,
                    ops: vec![item.op],
                }),
            }
        }

        let count: u64 = groups
            .iter()
            .flat_map(|group| group.ops.iter())
            .filter(|op| op.emits_wal())
            .count()
            .try_into()
            .expect("batch op count fits in u64");
        let base_seq = self.next_seq.fetch_add(count, Ordering::Relaxed);
        let mut group_base = base_seq;
        for group in &groups {
            if !group.tree.preflight_batch(&group.ops, group_base)? {
                return Ok(false);
            }
            group_base += group.ops.iter().filter(|op| op.emits_wal()).count() as u64;
        }
        if count == 0 {
            return Ok(true);
        }

        if let Some(journal) = &self.journal {
            let ack = {
                let _commit = self.commit_gate.enter_writer();
                let mut record = journal.record_buffer(encoded_db_batch_record_len(&groups));
                let mut enc = BatchEncoder::begin(&mut record, base_seq, 0);
                let mut group_base = base_seq;
                for group in &groups {
                    group
                        .tree
                        .apply_batch_walker_inline(&group.ops, group_base, Some(&mut enc))?;
                    group_base += group.ops.iter().filter(|op| op.emits_wal()).count() as u64;
                }
                let _n = enc.finish();
                journal.submit(record, self.cfg.wal_sync)?
            };
            if let Some(ack) = ack {
                ack.wait()?;
            }
        } else {
            let mut group_base = base_seq;
            for group in &groups {
                group
                    .tree
                    .apply_batch_walker_inline(&group.ops, group_base, None)?;
                group_base += group.ops.iter().filter(|op| op.emits_wal()).count() as u64;
            }
            if self.cfg.memory_flush_on_write {
                if let Some(group) = groups.first() {
                    group.tree.flush_dirty_inline()?;
                    group.tree.flush_pending_deletes_inline()?;
                }
            }
        }
        Ok(true)
    }
}

/// Immutable read transaction over one or more named tree scopes.
///
/// Created by [`DB::view`]. Each captured tree is exposed as a
/// normal [`View`], so point lookup and range/list APIs stay the
/// same as single-tree snapshots.
#[derive(Clone)]
pub struct DBView {
    trees: HashMap<u64, View>,
}

impl DBView {
    /// Return the captured view for `name`, if the caller listed it
    /// in [`DB::view`]'s scope array.
    #[must_use]
    pub fn tree(&self, name: &str) -> Option<&View> {
        self.trees.get(&tree_id_for_name(name.as_bytes()))
    }

    /// Number of captured named tree views.
    #[must_use]
    pub fn len(&self) -> usize {
        self.trees.len()
    }

    /// `true` if no tree scopes were captured.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.trees.is_empty()
    }
}

struct DBBatchGroup {
    tree_id: u64,
    tree: Tree,
    ops: Vec<BatchOp>,
}

#[derive(Debug)]
struct DBBatchOp {
    tree_id: u64,
    op: BatchOp,
}

/// Builder for [`DB::atomic`].
#[derive(Debug, Default)]
pub struct DBAtomicBatch {
    pending: Vec<DBBatchOp>,
}

impl DBAtomicBatch {
    /// Buffer a put in `tree`.
    pub fn put(&mut self, tree: &str, key: &[u8], value: &[u8]) {
        self.push(
            tree,
            BatchOp::Put {
                key: key.to_vec(),
                value: value.to_vec(),
            },
        );
    }

    /// Buffer a create-only put in `tree`.
    pub fn put_if_absent(&mut self, tree: &str, key: &[u8], value: &[u8]) {
        self.push(
            tree,
            BatchOp::PutIfAbsent {
                key: key.to_vec(),
                value: value.to_vec(),
            },
        );
    }

    /// Buffer a version-guarded update in `tree`.
    pub fn compare_and_put(
        &mut self,
        tree: &str,
        key: &[u8],
        expected: RecordVersion,
        value: &[u8],
    ) {
        self.push(
            tree,
            BatchOp::CompareAndPut {
                key: key.to_vec(),
                expected,
                value: value.to_vec(),
            },
        );
    }

    /// Buffer a delete in `tree`.
    pub fn delete(&mut self, tree: &str, key: &[u8]) {
        self.push(tree, BatchOp::Delete { key: key.to_vec() });
    }

    /// Buffer a version-guarded delete in `tree`.
    pub fn delete_if_version(&mut self, tree: &str, key: &[u8], expected: RecordVersion) {
        self.push(
            tree,
            BatchOp::DeleteIfVersion {
                key: key.to_vec(),
                expected,
            },
        );
    }

    /// Require that `key` has `expected` in `tree`.
    pub fn assert_version(&mut self, tree: &str, key: &[u8], expected: RecordVersion) {
        self.push(
            tree,
            BatchOp::AssertVersion {
                key: key.to_vec(),
                expected,
            },
        );
    }

    /// Require that no live key starts with `prefix` in `tree`.
    pub fn assert_prefix_empty(&mut self, tree: &str, prefix: &[u8]) {
        self.push(
            tree,
            BatchOp::AssertPrefixEmpty {
                prefix: prefix.to_vec(),
            },
        );
    }

    /// Buffer a rename inside one named tree.
    pub fn rename(&mut self, tree: &str, src: &[u8], dst: &[u8], force: bool) {
        self.push(
            tree,
            BatchOp::Rename {
                src: src.to_vec(),
                dst: dst.to_vec(),
                force,
            },
        );
    }

    /// Number of buffered operations.
    pub fn len(&self) -> usize {
        self.pending.len()
    }

    /// `true` when no operations have been buffered.
    pub fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    fn push(&mut self, tree: &str, op: BatchOp) {
        self.pending.push(DBBatchOp {
            tree_id: tree_id_for_name(tree.as_bytes()),
            op,
        });
    }
}

fn encoded_db_batch_record_len(groups: &[DBBatchGroup]) -> usize {
    let mut len = crate::journal::codec::RECORD_HEADER_SIZE + 8 + 4;
    for group in groups {
        for op in &group.ops {
            len += match op {
                BatchOp::Put { key, value }
                | BatchOp::PutIfAbsent { key, value }
                | BatchOp::CompareAndPut { key, value, .. } => {
                    1 + 8 + 4 + key.len() + 4 + value.len()
                }
                BatchOp::Delete { key } | BatchOp::DeleteIfVersion { key, .. } => {
                    1 + 8 + 4 + key.len()
                }
                BatchOp::Rename { src, dst, .. } => 1 + 8 + 4 + src.len() + 4 + dst.len() + 1,
                BatchOp::AssertVersion { .. } | BatchOp::AssertPrefixEmpty { .. } => 0,
            };
        }
    }
    len + crate::journal::codec::RECORD_FOOTER_SIZE
}

fn tree_id_for_name(name: &[u8]) -> u64 {
    let mut h = DB_TREE_HASH_OFFSET;
    for byte in name {
        h ^= u64::from(*byte);
        h = h.wrapping_mul(DB_TREE_HASH_PRIME);
    }
    if h == 0 {
        1
    } else {
        h
    }
}

fn root_guid_for_tree_id(tree_id: u64) -> BlobGuid {
    let mut guid = [0u8; 16];
    guid[0..8].copy_from_slice(&tree_id.to_le_bytes());
    guid[8..15].copy_from_slice(b"holt-db");
    guid[15] = DB_ROOT_TAG;
    guid
}
