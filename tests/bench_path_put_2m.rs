//! Holt-only path-shaped put probe for the 2M-key regime.
//!
//! Criterion's `_scale_put` comparator answers "do we still beat
//! RocksDB/SQLite?". This probe answers the next engineering
//! question: where does holt spend its own path-put budget at the
//! large-tree tier? It prints update latency plus tree-shape
//! counters before/after the update phase.
//!
//! Run explicitly:
//!
//! ```bash
//! cargo test --release --test bench_path_put_2m -- --ignored --nocapture
//! ```
//!
//! Short smoke:
//!
//! ```bash
//! HOLT_PATH_PUT_KEYS=20000 \
//! HOLT_PATH_PUT_UPDATES=5000 \
//! cargo test --release --test bench_path_put_2m -- --ignored --nocapture
//! ```

use std::env;
use std::hint::black_box;
use std::time::{Duration, Instant};

use hdrhistogram::Histogram;
use holt::{Tree, TreeConfig, TreeStats};
use rand::{rngs::StdRng, RngCore, SeedableRng};

const DEFAULT_KEYS: usize = 2_000_000;
const DEFAULT_UPDATES: usize = 100_000;
const HIST_MAX_NS: u64 = 60_000_000_000;
const SEED: u64 = 0xA11C_E551_2BAD_F00D;

#[test]
#[ignore = "2M path-put probe; use `cargo test --release --test bench_path_put_2m -- --ignored --nocapture`"]
fn path_put_large_tree_probe() {
    let keys = env_usize("HOLT_PATH_PUT_KEYS", DEFAULT_KEYS);
    let updates = env_usize("HOLT_PATH_PUT_UPDATES", DEFAULT_UPDATES);

    println!("\n=== Holt path-put large-tree probe (keys={keys}, updates={updates}) ===\n");
    println!(
        "{:<10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>7} {:>8} {:>8} {:>8} {:>8}",
        "workload",
        "preload",
        "put_p50",
        "put_p95",
        "put_p99",
        "put_max",
        "blobs",
        "avg_hop",
        "max_hop",
        "xdepth",
        "spill",
    );
    println!("{}", "-".repeat(120));

    run_workload("objstore", keys, updates, objstore_key, objstore_value());
    run_workload("fs", keys, updates, fs_key, fs_value());
}

fn run_workload(
    label: &'static str,
    keys: usize,
    updates: usize,
    key_fn: fn(usize) -> Vec<u8>,
    value: Vec<u8>,
) {
    let mut cfg = TreeConfig::memory();
    cfg.memory_flush_on_write = false;
    let tree = Tree::open(cfg).unwrap();
    let keyset: Vec<_> = (0..keys).map(key_fn).collect();

    let preload_start = Instant::now();
    for key in &keyset {
        tree.put(key, &value).unwrap();
    }
    let preload = preload_start.elapsed();
    let before = tree.stats().unwrap();

    let mut rng = StdRng::seed_from_u64(SEED);
    let mut hist = new_hist();
    for _ in 0..updates {
        let idx = (rng.next_u32() as usize) % keyset.len();
        record_elapsed(&mut hist, || {
            tree.put(black_box(&keyset[idx]), black_box(&value))
                .unwrap();
        });
    }
    let after = tree.stats().unwrap();
    let delta = ShapeDelta::new(&before, &after);

    println!(
        "{:<10} {:>10.2?} {:>9.0}ns {:>9.0}ns {:>9.0}ns {:>9.0}ns {:>7} {:>8.2} {:>8} {:>8} {:>8}",
        label,
        preload,
        hist_ns(&hist, 50.0),
        hist_ns(&hist, 95.0),
        hist_ns(&hist, 99.0),
        hist.max(),
        after.blob_count,
        delta.avg_hops(),
        delta.max_blob_hops,
        delta.max_cross_blob_depth,
        after.bm_spillovers,
    );
}

struct ShapeDelta {
    walker_ops: u64,
    walker_blob_hops: u64,
    max_blob_hops: u64,
    max_cross_blob_depth: u64,
}

impl ShapeDelta {
    fn new(before: &TreeStats, after: &TreeStats) -> Self {
        Self {
            walker_ops: after.bm_walker_ops.saturating_sub(before.bm_walker_ops),
            walker_blob_hops: after
                .bm_walker_blob_hops
                .saturating_sub(before.bm_walker_blob_hops),
            max_blob_hops: after.bm_max_blob_hops,
            max_cross_blob_depth: after.bm_max_cross_blob_depth,
        }
    }

    #[allow(clippy::cast_precision_loss)]
    fn avg_hops(&self) -> f64 {
        if self.walker_ops == 0 {
            0.0
        } else {
            self.walker_blob_hops as f64 / self.walker_ops as f64
        }
    }
}

fn objstore_key(i: usize) -> Vec<u8> {
    format!("bucket-{:02}/path/sub/file-{:06}.bin", i % 32, i / 32,).into_bytes()
}

fn fs_key(i: usize) -> Vec<u8> {
    format!("/usr/local/share/category-{}/file-{:06}", i % 16, i / 16,).into_bytes()
}

fn objstore_value() -> Vec<u8> {
    b"{\"size\":00000000,\"etag\":\"00000000\",\"class\":\"STD\"}".to_vec()
}

fn fs_value() -> Vec<u8> {
    let mut value = Vec::with_capacity(32);
    value.extend_from_slice(&0u64.to_le_bytes());
    value.extend_from_slice(&1_700_000_000u64.to_le_bytes());
    value.extend_from_slice(&0o644u32.to_le_bytes());
    value.extend_from_slice(&1000u32.to_le_bytes());
    value.extend_from_slice(&1000u32.to_le_bytes());
    value.extend_from_slice(&1u32.to_le_bytes());
    value
}

fn new_hist() -> Histogram<u64> {
    Histogram::new_with_bounds(1, HIST_MAX_NS, 3).unwrap()
}

fn record_elapsed<T>(hist: &mut Histogram<u64>, f: impl FnOnce() -> T) -> Duration {
    let start = Instant::now();
    let _ = f();
    let elapsed = start.elapsed();
    let nanos = elapsed.as_nanos().min(u128::from(HIST_MAX_NS)) as u64;
    let _ = hist.record(nanos.max(1));
    elapsed
}

fn hist_ns(hist: &Histogram<u64>, percentile: f64) -> u64 {
    hist.value_at_percentile(percentile)
}

fn env_usize(name: &str, default: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(default)
}
