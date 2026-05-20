//! Concurrency primitives.
//!
//! `HybridLatch` is a 3-mode latch held per blob frame. The
//! contract follows LeanStore (Leis et al., ICDE 2018).

mod hybrid_latch;
mod maintenance_gate;

pub use hybrid_latch::{Guard, HybridLatch};
pub(crate) use maintenance_gate::MaintenanceGate;
