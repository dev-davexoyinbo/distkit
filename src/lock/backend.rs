//! Low-level Redis lock operations: shared Lua scripts and the atomic
//! acquire / refresh / release round-trips backing `DistMutex` and `DistRwLock`.
//!
//! Empty in Stage 0 — populated in Stage 1 (mutex backend) and Stage 4 (rwlock backend).
