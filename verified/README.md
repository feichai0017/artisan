# Holt Verified Model

This directory contains Verus models for small, correctness-critical pieces of
Holt's persistent ART design.

It is intentionally separate from the production crate:

- normal `cargo test` and release builds do not depend on Verus;
- the model uses ghost state and specs instead of duplicating unsafe layout code;
- every spec here should correspond to a real invariant in `src/`.

Run it with a local Verus binary:

```sh
VERUS=/path/to/verus ./verified/verify.sh
```

The normal CI path does not install Verus. To check this model in
GitHub Actions, run the `Nightly Validation` workflow manually with
`run_verus=true` and either provide `verus_url` or use a runner where
`verus` is already in `PATH`.

The current model covers:

- ART inner-node capacity and live-child invariants;
- sorted child lookup for Node4/Node16-style children;
- absent-key child insertion preserving key/child arity;
- Node4/Node16/Node48/Node256 grow and hysteresis-aware shrink shape;
- compact/filter survivor packing into Empty, unary Prefix, or the smallest fitting inner node;
- leaf split structure: optional Prefix plus a valid two-child Node4 branch;
- delimiter rollup bounds for S3-style `CommonPrefix` emission;
- Holt's virtual `0x00` user-key terminator;
- the 8-byte leaf extent alignment rule.
