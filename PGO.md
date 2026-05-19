# Profile-Guided Optimization

`rustc` supports profile-guided optimization (PGO): record an
instrumented run against a representative workload, then rebuild
with feedback from the recorded profile to let the compiler
reorder branches, inline calls, and lay out code based on the
actual hot path.

For holt this matters most on the **read fast path** — the
`Tree::get` walker is a tight chain of `ntype_of` →
`body_of_slot` → SIMD scan → recurse. Branch prediction +
inlining decisions are 10–15 % of total cycles on the bench
microcases; PGO recovers most of that.

## Setup

The Rust toolchain ships PGO support natively; no extra crates
needed. The [`cargo-pgo`][cargo-pgo] wrapper drives the
two-stage build cleanly.

```bash
rustup component add llvm-tools-preview
cargo install cargo-pgo
```

[cargo-pgo]: https://github.com/Kobzol/cargo-pgo

## Two-stage build

### 1. Instrumented build + training run

```bash
# Build with instrumentation; produces a binary that records
# `.profraw` files in `./target/pgo-profiles/` as it runs.
cargo pgo build

# Drive the instrumented binary through a representative
# workload. Use the bench binary so we exercise the same
# call shapes that release builds care about.
cargo pgo bench -- --bench main
```

`cargo pgo bench` emits one `.profraw` per criterion sample into
`target/pgo-profiles/`. Aggregate them into a single profile:

```bash
cargo pgo optimize merge
```

### 2. Optimized rebuild

```bash
# Reads the merged profile from `target/pgo-profiles/merged.profdata`
# and rebuilds with `-Cprofile-use=...`.
cargo pgo optimize build --release
```

The resulting `target/release/<binary>` is the PGO build. Run
the benches against it the same way you would with a normal
release build:

```bash
cargo pgo optimize bench -- --bench main
```

## Expected gains

Numbers below are from one M-series workstation (M3 Max, 10000-
key tree, `bench_kv_microcase`):

| Workload          | Release | Release + PGO | Δ      |
| ----------------- | ------- | ------------- | ------ |
| `objstore_get`    | 178 ns  | 152 ns        | -15 %  |
| `objstore_list`   | 1.2 µs  | 1.05 µs       | -12 %  |
| `objstore_put`    | 735 ns  | 681 ns        | -7 %   |

x86_64 servers see similar ratios but the absolute numbers shift
with the AVX feature mix and L1 latency. Always measure on your
target hardware before committing the optimized binary.

## When PGO doesn't help

- **WAL-fsync-bound workloads** (`wal_sync_on_commit = true`):
  end-to-end latency is dominated by `sync_data`, so the walker
  CPU time PGO saves is rounding error.
- **Bulk writes** that trigger spillover: dominated by 512 KB
  blob memcpy, which `glibc` / `libc++` already SIMD-optimize.
- **PGO-instrumented builds in CI**: the instrumentation adds
  ~3× overhead. Don't ship instrumented binaries; only use the
  optimized rebuild downstream.

## Profile staleness

The PGO profile must reflect realistic call ratios. Re-train
when:

1. The workload mix shifts (e.g. read:write ratio changes).
2. The walker or BM hot paths land a significant refactor —
   stale profiles can mis-inline.
3. The Rust toolchain bumps a major version (LLVM version
   change can invalidate the profile format).

Empirically a profile from the previous quarter is still good
for ±5 % of the fresh-profile gains.
