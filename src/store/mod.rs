//! Storage layer.
//!
//! - [`BlobFrame`] — typed view over one 512 KB blob, with bump
//!   allocator + per-NodeType free list.
//! - [`backend`] — pluggable storage backend trait
//!   (memory / persistent / future io_uring).
//! - [`BufferManager`] — LRU-bounded cache wrapping any `Backend`,
//!   itself implementing `Backend` so it's transparent.

mod blob_frame;
mod buffer_manager;
pub mod backend;

pub use blob_frame::{
    AllocError, AllocOutcome, BlobFrame, BlobFrameRef, ExtentAllocOutcome, FreeError,
};
pub use buffer_manager::{BufferManager, CachedBlob};
