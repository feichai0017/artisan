//! Concurrency primitives.
//!
//! `HybridLatch` is a 3-mode latch held per blob frame. The
//! contract follows LeanStore (Leis et al., ICDE 2018).
//! `CommitGate` is the writer-shared / checkpoint-exclusive
//! publish barrier for persistent trees. `Gate` is the small
//! shared/exclusive admission primitive used for tree-wide
//! structural maintenance.

mod commit_gate;
mod endpoint_locks;
mod gate;
mod hybrid_latch;

pub(crate) use commit_gate::CommitGate;
pub(crate) use endpoint_locks::EndpointLocks;
pub(crate) use gate::Gate;
pub use hybrid_latch::{Guard, HybridLatch};
