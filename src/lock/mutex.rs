//! Distributed mutual-exclusion lock: `DistMutex` and its RAII release guard.
//!
//! Mirrors the surface of [`tokio::sync::Mutex`] over Redis. Empty in Stage 0 —
//! `DistMutex` + `DistMutexGuard` land in Stage 2, auto-refresh in Stage 3.
