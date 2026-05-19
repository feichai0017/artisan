//! Parameter sweep over `CheckpointConfig::idle_interval` — the one
//! tunable that visibly moves writer overhead, peak WAL size, and
//! reopen latency in the same direction or the other.
//!
//! Run with:
//!
//!     cargo test --release --test bench_checkpoint_sweep -- --nocapture
//!
//! The other two cadence knobs (`io_queue_capacity`,
//! `eviction_idle_ticks`) are deliberately *not* swept here:
//!
//! - `io_queue_capacity` only matters once multiple blobs are
//!   dirty in the same round, which v0.2 trees rarely reach
//!   (single-root usage). Default 16 leaves head-room for the
//!   io_uring batched-flush mode in v0.3.
//! - `eviction_idle_ticks` defaults to 1024 — meaningful only
//!   once `cfg.buffer_pool_size` is sized in the hundreds. For
//!   the default 64-blob pool, eviction is mostly a no-op.
//!
//! Acceptance criterion for the chosen default is "smallest
//! interval that doesn't measurably worsen burst writer
//! throughput while keeping paced peak WAL bounded".

use std::fs;
use std::path::Path;
use std::time::{Duration, Instant};

use holt::{CheckpointConfig, Tree, TreeConfig};
use tempfile::TempDir;

const KEYS: u32 = 5_000;

fn wal_size(dir: &Path) -> u64 {
    fs::metadata(dir.join("journal.wal"))
        .map(|m| m.len())
        .unwrap_or(0)
}

fn pretty(b: u64) -> String {
    let kib = b as f64 / 1024.0;
    if kib < 1024.0 {
        format!("{kib:.1} KiB")
    } else {
        format!("{:.2} MiB", kib / 1024.0)
    }
}

struct Row {
    label: String,
    burst_write: Duration,
    burst_reopen: Duration,
    paced_write: Duration,
    paced_peak_wal: u64,
    paced_final_wal: u64,
    paced_reopen: Duration,
}

fn run_burst(cfg_factory: impl Fn(&Path) -> TreeConfig) -> (Duration, Duration) {
    let dir = TempDir::new().unwrap();
    let cfg = cfg_factory(dir.path());

    let tree = Tree::open(cfg.clone()).unwrap();
    let t0 = Instant::now();
    for i in 0..KEYS {
        let k = format!("burst/{i:06}");
        let v = vec![0xAB_u8; 64];
        tree.put(k.as_bytes(), &v).unwrap();
    }
    let write_total = t0.elapsed();
    drop(tree);

    let t1 = Instant::now();
    let _tree = Tree::open(cfg).unwrap();
    let reopen = t1.elapsed();
    (write_total, reopen)
}

fn run_paced(cfg_factory: impl Fn(&Path) -> TreeConfig) -> (Duration, u64, u64, Duration) {
    let dir = TempDir::new().unwrap();
    let cfg = cfg_factory(dir.path());

    let tree = Tree::open(cfg.clone()).unwrap();
    let mut peak_wal = 0u64;
    let pause_every = 500;
    let t0 = Instant::now();
    for i in 0..KEYS {
        let k = format!("paced/{i:06}");
        let v = vec![0xAB_u8; 64];
        tree.put(k.as_bytes(), &v).unwrap();
        if i > 0 && i % pause_every == 0 {
            peak_wal = peak_wal.max(wal_size(dir.path()));
            std::thread::sleep(Duration::from_millis(100));
            peak_wal = peak_wal.max(wal_size(dir.path()));
        }
    }
    let write_total = t0.elapsed();
    peak_wal = peak_wal.max(wal_size(dir.path()));

    // Let any in-flight bg round settle before sampling final.
    std::thread::sleep(Duration::from_millis(300));
    let final_wal = wal_size(dir.path());
    drop(tree);

    let t1 = Instant::now();
    let _tree = Tree::open(cfg).unwrap();
    let reopen = t1.elapsed();

    (write_total, peak_wal, final_wal, reopen)
}

#[test]
fn idle_interval_sweep() {
    let mut rows = Vec::<Row>::new();

    // ---- baseline: bg disabled ----
    {
        let (bw, br) = run_burst(|p| TreeConfig::new(p));
        let (pw, peak, fwal, pr) = run_paced(|p| TreeConfig::new(p));
        rows.push(Row {
            label: "bg disabled".into(),
            burst_write: bw,
            burst_reopen: br,
            paced_write: pw,
            paced_peak_wal: peak,
            paced_final_wal: fwal,
            paced_reopen: pr,
        });
    }

    // ---- sweep idle_interval ----
    for interval_ms in [50u64, 100, 200, 500, 1000] {
        let mk_cfg = move |p: &Path| {
            let mut c = TreeConfig::new(p);
            c.checkpoint = CheckpointConfig {
                idle_interval: Duration::from_millis(interval_ms),
                ..CheckpointConfig::enabled()
            };
            c
        };
        let (bw, br) = run_burst(mk_cfg);
        let (pw, peak, fwal, pr) = run_paced(mk_cfg);
        rows.push(Row {
            label: format!("bg @ {interval_ms} ms"),
            burst_write: bw,
            burst_reopen: br,
            paced_write: pw,
            paced_peak_wal: peak,
            paced_final_wal: fwal,
            paced_reopen: pr,
        });
    }

    println!("\n=== idle_interval sweep ({KEYS} keys × 64 B) ===\n");
    println!(
        "{:<18}  {:>11}  {:>11}  {:>11}  {:>11}  {:>11}  {:>11}",
        "config",
        "burst_write",
        "burst_reopen",
        "paced_write",
        "paced_peak",
        "paced_final",
        "paced_reopen",
    );
    println!("{}", "-".repeat(102));
    for r in &rows {
        println!(
            "{:<18}  {:>10.1?}  {:>10.1?}  {:>10.1?}  {:>11}  {:>11}  {:>10.1?}",
            r.label,
            r.burst_write,
            r.burst_reopen,
            r.paced_write,
            pretty(r.paced_peak_wal),
            pretty(r.paced_final_wal),
            r.paced_reopen,
        );
    }
    println!();

    // Sanity: every bg row must shrink final_wal to ≈ 0.
    for r in &rows[1..] {
        assert!(
            r.paced_final_wal <= 1024,
            "{}: final_wal should be drop-time truncated, got {}",
            r.label,
            pretty(r.paced_final_wal)
        );
        assert!(
            r.paced_reopen < rows[0].paced_reopen,
            "{}: reopen should beat the disabled baseline ({:?} vs {:?})",
            r.label,
            r.paced_reopen,
            rows[0].paced_reopen,
        );
    }
}
