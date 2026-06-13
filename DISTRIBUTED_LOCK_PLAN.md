# Distributed Lock Plan — `distkit`

Library-friendly distributed locks for `distkit`: **`DistMutex`** and **`DistRwLock`**, with a
surface that mirrors `tokio::sync::Mutex` / `tokio::sync::RwLock` as closely as a network lock allows.

> Status: **plan only — no implementation in this version.**

---

## Context

celeris-realtime already ships a working distributed lock (`app/src/distributed_lock/`):
Redis `SET NX PX` + Lua refresh/release, owner-UUID, background refresh, a `run_with_lock(closure)`
API routed through a `RedisServiceCommand` channel. It works, but it is:

- **Mutex-only** — no shared/read locking,
- **closure-style** — `run_with_lock(|| ...)` rather than an RAII guard,
- **glued to the app** — Redis access goes through the realtime node's command bus, so it is not
  reusable as a library.

This plan rebuilds the primitive as a clean, reusable module **inside `distkit`** (the published
Redis-primitives crate). celeris-realtime would adopt it later — that migration is **out of scope**
here.

### Decisions

- **Home:** `distkit`. celeris-realtime migration is a future follow-up.
- **Acquire semantics:** three forms per lock —
  - waiting: `lock` / `read` / `write`,
  - non-blocking: `try_lock` / `try_read` / `try_write`,
  - bounded: `try_lock_for` / `try_read_for` / `try_write_for(timeout, retry_interval)`.

  A single internal core `acquire(mode, timeout, retry_interval)` backs all three. `try_lock` is
  literally `acquire(mode, ZERO, ZERO)` (one shot, no sleep); the waiting forms pass the options
  defaults or explicit values.

- **Consistency:** **Strict only.** Every acquire/refresh/release is an atomic Redis Lua
  round-trip. No Lax/buffered variant — buffering lock ownership is unsafe.

### Divergences from tokio (documented, intentional)

- `DistMutex` / `DistRwLock` guard **no inner data** — they are pure mutual exclusion, like
  `tokio::Mutex<()>`. Guards are release tokens; they do **not** `Deref` to a `T`.
- One lock object = one resource. The key + owner are bound at construction (`LockOptions`),
  matching `tokio::Mutex::new(x)`.
- Acquire is fallible (`Result<_, DistkitError>`) and async over the network. Release is
  best-effort on `Drop`, plus an explicit awaitable `release()` for callers who want to observe
  errors.

---

## Module layout

Mirrors `src/counter/`:

```
src/lock/
  mod.rs        # LockOptions, LockMode, re-exports, module docs
  error.rs      # LockError enum
  backend.rs    # shared Lua consts + low-level Redis ops (acquire/refresh/release)
  mutex.rs      # DistMutex + DistMutexGuard
  rwlock.rs     # DistRwLock + DistRwLockReadGuard + DistRwLockWriteGuard
  tests/
    common.rs   # make_mutex / make_rwlock + unique-prefix key helper
    mutex.rs
    rwlock.rs
```

Feature flag `lock` (optional; added to `full`). Gated in `src/lib.rs` exactly like `counter`:

```rust
#[cfg(feature = "lock")]
pub mod lock;
```

---

## Public API

```rust
pub struct LockOptions {
    pub key: DistkitRedisKey,
    pub connection_manager: ConnectionManager,
    pub ttl: Duration,                 // lease length (default 30s)
    pub owner_id: Option<String>,      // default: UUID v4 (uuid crate, already a dep)
    pub max_wait: Option<Duration>,    // bound for lock()/read()/write(); None => wait until acquired
    pub retry_interval: Duration,      // poll gap for the waiting forms (default 50ms)
    pub auto_refresh: bool,            // default true; background lease renewal every ttl/3
}

impl LockOptions {
    // defaults: ttl=30s, owner_id=UUIDv4, max_wait=None, retry_interval=50ms, auto_refresh=true
    pub fn new(key: DistkitRedisKey, connection_manager: ConnectionManager) -> Self;
}
```

### Mutex — looks like `tokio::sync::Mutex`

```rust
let m = DistMutex::new(options);                 // -> Arc<DistMutex>

let g = m.lock().await?;                          // wait up to max_wait (or until acquired)
let g = m.try_lock().await?;                      // one attempt; Err(LockError::WouldBlock) if held
let g = m.try_lock_for(timeout, retry).await?;    // bounded wait

// g: DistMutexGuard
//   - drop releases (best-effort, fire-and-forget)
//   - g.release().await? to release and observe errors
```

### RwLock — looks like `tokio::sync::RwLock`

```rust
let rw = DistRwLock::new(options);               // -> Arc<DistRwLock>

let r = rw.read().await?;
let r = rw.try_read().await?;
let r = rw.try_read_for(timeout, retry).await?;

let w = rw.write().await?;
let w = rw.try_write().await?;
let w = rw.try_write_for(timeout, retry).await?;
```

### Internal core (the only place the retry loop lives)

```rust
async fn acquire(&self, mode: LockMode, timeout: Duration, retry_interval: Duration)
    -> Result<RawLease, DistkitError>;

// try_lock      => acquire(mode, ZERO, ZERO)                     (single shot, no sleep)
// try_lock_for  => acquire(mode, timeout, retry_interval)
// lock          => acquire(mode, max_wait_or_forever, options.retry_interval)
```

Loop: run the acquire Lua once → on success build the guard (and spawn refresh) and return `Ok` →
else if the deadline is exceeded return `LockError::WouldBlock` (timeout == 0) or
`LockError::Timeout` → else `sleep(retry_interval)` and retry. `retry_interval == 0` with
`timeout > 0` is a tight spin (allowed, documented).

---

## Redis data model

All timestamps come from `redis.call('TIME')` **inside Lua** (server clock — avoids cross-node
skew). Keys are namespaced under the existing `{prefix}:` convention (see `RedisKeyGenerator`).

### Mutex (same proven pattern as celeris-realtime today)

- Key `{prefix}:lock:{key}` holds `owner_id`.
- **acquire:** `SET key owner NX PX ttl`.
- **refresh:** Lua — `GET == owner ? PEXPIRE : 0`.
- **release:** Lua — `GET == owner ? DEL : 0`.

### RwLock (read-preferring, v1)

- Writer key `{prefix}:rwlock:{key}:w` holds the writer `owner_id` (PX ttl).
- Readers `{prefix}:rwlock:{key}:r` = a ZSET of `reader_owner_id` scored by expiry (`now + ttl`).

| Operation     | Lua (atomic)                                                                                                               |
| ------------- | -------------------------------------------------------------------------------------------------------------------------- |
| acquire read  | purge expired readers (`ZREMRANGEBYSCORE 0 now`); if writer key exists → fail; else `ZADD` self `now+ttl`, set key TTL, ok |
| acquire write | purge expired readers; if any reader remains **or** writer key exists → fail; else `SET w owner PX ttl`, ok                |
| refresh read  | re-`ZADD` own score                                                                                                        |
| refresh write | `PEXPIRE w`                                                                                                                |
| release read  | `ZREM` self                                                                                                                |
| release write | `GET == owner ? DEL`                                                                                                       |

Crashed holders self-heal: readers via score expiry + lazy purge; writer via PX.

**Known limitation:** read-preferring → possible writer starvation under constant readers.
A writer-preference "pending writers" key is **future work**, not v1.

---

## Guards & auto-refresh

- `DistMutexGuard` / `DistRwLockReadGuard` / `DistRwLockWriteGuard` each hold: the owner, the
  key(s), a cloned `ConnectionManager`, and the refresh task `JoinHandle`.
- **Drop:** abort the refresh task; `tokio::spawn` a fire-and-forget release (Drop is sync, release
  is async) — same approach as the current `DistributedLockGuard`. An explicit
  `async fn release(self) -> Result<(), DistkitError>` lets callers await and see errors (tokio has
  no such method; useful addition here).
- **auto_refresh:** on a successful acquire, if enabled, spawn a task that renews the lease every
  `ttl/3`. On a failed refresh (lease lost) it stops and flips an `AtomicBool`, so a later
  `release()` reports `LockError::LockLost`.

---

## Error type

Extend `DistkitError` (`src/error.rs`) with a feature-gated variant mirroring `CounterError`:

```rust
#[cfg(feature = "lock")]
#[error("Lock Error: {0}")]
LockError(#[from] LockError),
```

New `src/lock/error.rs`:

```rust
#[derive(Debug, thiserror::Error, PartialEq)]
pub enum LockError {
    #[error("lock is held (would block)")]       WouldBlock,
    #[error("timed out after {waited:?}")]        Timeout { waited: Duration },
    #[error("lock lease lost (refresh failed)")]  LockLost,
    #[error("not the lock owner")]                NotOwner,
}
```

Redis failures continue to surface as `DistkitError::RedisError`.

---

## Stages

Each stage compiles and is green via `make test` before the next begins.

- **Stage 0 — Scaffolding.** Add `lock` feature in `Cargo.toml` (+ to `full`); `pub mod lock` gate
  in `lib.rs`; `LockError` + the `DistkitError` variant; `LockOptions`, `LockMode`, empty module
  skeleton. Compiles, no behavior.
- **Stage 1 — Mutex backend.** Lua consts (acquire/refresh/release) + low-level fns in `backend.rs`
  via `execute_pipeline_with_script_retry`. Direct-against-Redis unit tests for owner semantics.
- **Stage 2 — `DistMutex` + guard.** `acquire` core + `lock`/`try_lock`/`try_lock_for`;
  `DistMutexGuard` Drop-release + explicit `release()`. (No refresh yet.) Tests: exclusion,
  try_lock contention, timeout, release frees.
- **Stage 3 — Auto-refresh.** Background renewal task, abort-on-drop, lost-lock handling. Tests:
  lease survives past `ttl`; killed refresh → `LockLost`.
- **Stage 4 — RwLock backend.** ZSET reader model + writer key Lua (read/write acquire/refresh/
  release, server `TIME`, lazy purge). Direct-Redis tests for shared-read / exclusive-write /
  expiry purge.
- **Stage 5 — `DistRwLock` + guards.** `read`/`write` + `try_*` + `try_*_for` over the shared
  `acquire` core; three guard types with Drop + refresh. Tests: N concurrent readers, writer waits
  for readers, reader waits for writer.
- **Stage 6 — Docs, doctests, benches.** Module + type rustdoc (satisfy `#![deny(missing_docs)]`);
  doctests in `docs/lib.md`; criterion bench `benches/lock.rs` mirroring `benches/strict_counter.rs`;
  update `README.md`, `CLAUDE.md`, and the `docs/lib.md` feature table.

**Out of scope (future):** celeris-realtime migration off `app/src/distributed_lock/` onto
`distkit::lock`; RwLock writer-preference mode; lock-acquired metrics/tracing spans.

---

## Critical files

**New**

- `src/lock/{mod,error,backend,mutex,rwlock}.rs`
- `src/lock/tests/{common,mutex,rwlock}.rs`
- `benches/lock.rs`

**Edit**

- `Cargo.toml` — feature `lock`, bench entry
- `src/lib.rs` — module gate
- `src/error.rs` — `LockError` variant
- `docs/lib.md`, `README.md`, `CLAUDE.md`

**Reuse (do not reinvent)**

- `execute_pipeline_with_script_retry` and `mutex_lock` — `src/common/mod.rs`
- `DistkitRedisKey` + `RedisKeyGenerator` namespacing — `src/common/mod.rs`
- `redis::Script` + `ConnectionManager` + `uuid` — already dependencies
- `Arc<Self>`-from-`new(options)` ctor idiom — `src/counter/strict_counter.rs`
- unique per-test prefix harness — `src/counter/tests/common.rs`

**Behavior-parity reference (in celeris-realtime)**

- `app/src/distributed_lock/distributed_lock_guard.rs` — Drop release, refresh-every-TTL/N,
  owner-checked Lua
- `app/src/distributed_lock/redis_distributed_lock.rs` — the Lua scripts

---

## Verification

- `make test` — spins Redis via `compose.yaml`, runs the suite (`--show-output`), tears down.
  Use `make test`, **not** raw `cargo test`. Ensure doctests run (`cargo test --all-features --doc`
  if `make test` does not already cover them) for the `docs/lib.md` examples.
- `make bench` — criterion lock benches.
- **Manual sanity:**
  - Two `DistMutex` instances (distinct owners) on one key → the second `try_lock` returns
    `WouldBlock`; `lock()` blocks then succeeds after the first guard drops.
  - `DistRwLock` → multiple concurrent `read()` succeed; `write()` waits for all readers/the writer
    to drop.
