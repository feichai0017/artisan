# Holt Soak Harness

`holt-soak` is an explicit durability and lifecycle validation tool.
It is not part of the published crate and is intentionally kept out of
the parent workspace.

## Modes

- `normal`: multi-threaded point read/write/delete, key-only prefix
  scan, atomic batch, checkpoint, reopen, and oracle verification.
- `crash`: parent process repeatedly starts a child writer, kills it
  with `SIGKILL`, reopens the tree, and verifies every operation the
  child acknowledged in `soak-ack.log`.
- `child`: internal mode used by `crash`.

## Quick Smoke

```sh
cargo run --manifest-path tools/soak/Cargo.toml --locked -- \
  --mode normal \
  --dir target/holt-soak \
  --reset \
  --duration-secs 60 \
  --keys 100000 \
  --ops 1000000 \
  --threads 4 \
  --buffer-pool 64 \
  --wal-sync false
```

## Crash Campaign

Crash mode requires `--wal-sync true`: the verifier treats the ack log
as the source of acknowledged mutations, so each acknowledged Holt write
must have crossed the WAL durability boundary.

```sh
cargo run --manifest-path tools/soak/Cargo.toml --locked -- \
  --mode crash \
  --dir target/holt-soak-crash \
  --reset \
  --duration-secs 21600 \
  --keys 100000 \
  --ops 1000000 \
  --buffer-pool 64 \
  --wal-sync true \
  --kill-min-ms 100 \
  --kill-max-ms 5000
```

The tool emits JSON lines with cache, WAL, checkpoint, route-cache, and
reopen-replay counters. CI runs only a short `normal` smoke; longer
normal/crash campaigns belong in nightly or release-gate runs.
