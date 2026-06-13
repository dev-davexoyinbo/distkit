//! Distributed reader-writer lock: `DistRwLock` and its read / write guards.
//!
//! Mirrors the surface of [`tokio::sync::RwLock`] over Redis. Empty in Stage 0 —
//! the ZSET reader model backend lands in Stage 4, the public type + guards in Stage 5.
