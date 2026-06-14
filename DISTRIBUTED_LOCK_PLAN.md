# Distributed Lock Plan — `distkit`

Library-friendly distributed locks for `distkit`: **`Mutex`** and **`RwLock`**, with a
surface that mirrors `tokio::sync::Mutex` / `tokio::sync::RwLock` as closely as a network lock allows.

> Status: **Stages 0–2 done (scaffolding + mutex backend + `Mutex`/guard); Stages 3–6 unimplemented.**

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

- `Mutex` / `RwLock` guard **no inner data** — they are pure mutual exclusion, like
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
  mutex.rs      # Mutex + MutexGuard
  rwlock.rs     # RwLock + RwLockReadGuard + RwLockWriteGuard
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
    pub namespace: DistkitRedisKey,    // key prefix; default "distkit-locks". Full key = {namespace}:{key}
    pub ttl: Duration,                 // lease length (default 30s)
    pub owner_id: Option<String>,      // default: UUID v4 (uuid crate, already a dep)
    pub max_wait: Option<Duration>,    // bound for lock()/read()/write(); None => wait until acquired
    pub retry_interval: Duration,      // poll gap for the waiting forms (default 50ms)
    pub auto_refresh: bool,            // default true; background lease renewal every ttl/3
}

impl LockOptions {
    // defaults: namespace="distkit-locks", ttl=30s, owner_id=UUIDv4, max_wait=None,
    //           retry_interval=50ms, auto_refresh=true
    pub fn new(key: DistkitRedisKey, connection_manager: ConnectionManager) -> Self;
    // Fluent alternative (sugar for LockOptionsBuilder::new); same defaults.
    pub fn builder(key: DistkitRedisKey, connection_manager: ConnectionManager) -> LockOptionsBuilder;
}

// Chainable builder seeded with the LockOptions::new defaults.
pub struct LockOptionsBuilder { /* ... */ }
impl LockOptionsBuilder {
    pub fn new(key: DistkitRedisKey, connection_manager: ConnectionManager) -> Self;
    pub fn namespace(self, namespace: DistkitRedisKey) -> Self;
    pub fn ttl(self, ttl: Duration) -> Self;
    pub fn owner_id(self, owner_id: impl Into<String>) -> Self;
    pub fn max_wait(self, max_wait: Duration) -> Self;
    pub fn retry_interval(self, retry_interval: Duration) -> Self;
    pub fn auto_refresh(self, auto_refresh: bool) -> Self;
    pub fn build(self) -> LockOptions;
}
```

### Mutex — looks like `tokio::sync::Mutex`

```rust
let mutex = Mutex::new(options);                      // -> Arc<Mutex>

let guard = mutex.lock().await?;                          // wait up to max_wait (or until acquired)
let guard = mutex.try_lock().await?;                      // one attempt; Err(LockError::AcquireFail) if held
let guard = mutex.try_lock_for(timeout, retry).await?;    // bounded wait

// guard: MutexGuard
//   - drop releases (best-effort, fire-and-forget)
//   - guard.release().await? to release and observe errors
```

### RwLock — looks like `tokio::sync::RwLock`

```rust
let rw = RwLock::new(options);               // -> Arc<RwLock>

let r = rw.read().await?;
let r = rw.try_read().await?;
let r = rw.try_read_for(timeout, retry).await?;

let w = rw.write().await?;
let w = rw.try_write().await?;
let w = rw.try_write_for(timeout, retry).await?;
```

### Internal core (the only place the retry loop lives)

```rust
// As implemented (Stage 2, mutex): timeout is Option<Duration> — None = forever,
// Some(ZERO) = single shot, Some(d) = bounded. (The mode arg arrives with RwLock in Stage 5.)
async fn acquire_core(&self, timeout: Option<Duration>, retry_interval: Duration)
    -> Result<MutexGuard, DistkitError>;

// try_lock      => acquire_core(Some(ZERO), ZERO)                (single shot)
// try_lock_for  => acquire_core(Some(timeout), retry_interval)
// lock          => acquire_core(self.max_wait, self.retry_interval)   (None => forever)
```

Loop: run the acquire round-trip once → on success build the guard and return `Ok` → else if
`timeout == Some(ZERO)` return `LockError::AcquireFail` → else if the deadline is exceeded return
`LockError::Timeout { waited }` → else wait one `retry_interval` tick and retry. The poll cadence
uses a `tokio::time::interval` with `MissedTickBehavior::Delay` (first tick fires immediately);
a `retry_interval` of zero is a tight spin (allowed, documented).

---

## Redis data model

All timestamps come from `redis.call('TIME')` **inside Lua** (server clock — avoids cross-node
skew). Keys are namespaced as `{namespace}:{key}`, where `namespace` comes from
`LockOptions.namespace` (default `distkit-locks`, user-overridable).

### Mutex (same proven pattern as celeris-realtime today)

- Key `{namespace}:{key}` holds `owner_id`.
- **acquire:** `SET key owner NX PX ttl`.
- **refresh:** Lua — `GET == owner ? PEXPIRE : 0`.
- **release:** Lua — `GET == owner ? DEL : 0`.

### RwLock (read-preferring, v1)

- Writer key `{namespace}:{key}:w` holds the writer `owner_id` (PX ttl).
- Readers `{namespace}:{key}:r` = a ZSET of `reader_owner_id` scored by expiry (`now + ttl`).

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

- `MutexGuard` / `RwLockReadGuard` / `RwLockWriteGuard` each hold: the owner, the
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
    #[error("failed to acquire lock")]            AcquireFail,
    #[error("timed out after {waited:?}")]        Timeout { waited: Duration },
    #[error("lock lease lost (refresh failed)")]  LockLost,
    #[error("not the lock owner")]                NotOwner,
}
```

Redis failures continue to surface as `DistkitError::RedisError`.

---

## Stages

Each stage compiles and is green via `make test` before the next begins.

- **Stage 0 — Scaffolding. ✅ Done.** Feature wiring, public option/mode types, error plumbing, and
  empty internal skeletons. No behavior — build + the `LockOptions::new` doctest are the gate.
  - `Cargo.toml`: `lock = []` feature; added to `full`. No new deps, no bench entry yet.
  - `src/lib.rs`: `#[cfg(feature = "lock")] pub mod lock;` beside the `counter` gate.
  - `src/error.rs`: feature-gated `use crate::lock::LockError;` + `DistkitError::LockError(#[from] LockError)`.
  - `src/lock/error.rs`: `LockError { WouldBlock, Timeout { waited }, LockLost, NotOwner }`
    (`Debug, thiserror::Error, PartialEq`).
  - `src/lock/mod.rs`: `LockMode { Shared, Exclusive }` (`Copy`); `LockOptions` (`Debug, Clone`) +
    `LockOptions::new(key, conn)` with defaults `ttl 30s`, `owner_id Some(UUIDv4)`, `max_wait None`,
    `retry_interval 50ms`, `auto_refresh true`; live-Redis doctest mirroring `CounterOptions::new`.
  - `src/lock/{backend,mutex,rwlock}.rs`: module-doc-only stubs (satisfy `deny(missing_docs)`).
    `pub use mutex::*` / `pub use rwlock::*` carry `#[allow(unused_imports)]` until Stages 2/5 add
    types. Doc references to `Mutex` / `RwLock` are plain code spans (not intra-doc links)
    until those types exist.
  - `src/lock/tests/{mod,common}.rs`: `make_options(name)` unique-prefix harness (mirrors
    `counter/tests/common.rs`), `#![allow(dead_code)]` until Stage 2+ test modules use it.
  - Verified: `cargo build` (lock off / `--features lock` / `--all-features`), `cargo doc
    --all-features` clean, `make test` green (77 passed incl. the new doctest).
- **Stage 1 — Mutex backend. ✅ Done.** Three crate-internal async ops in `src/lock/backend.rs`,
  each an atomic Redis round-trip keyed on `owner_id`, taking a fully-formed (already namespaced) key:
  - `acquire(conn, key, owner, ttl_ms) -> bool` — plain `SET key owner NX PX ttl_ms` (no Lua).
  - `refresh(conn, key, owner, ttl_ms) -> bool` — owner-checked `GET == owner ? PEXPIRE : 0` Lua.
  - `release(conn, key, owner) -> bool` — owner-checked `GET == owner ? DEL : 0` Lua.
  - Each Lua script is compiled once into a function-local `static OnceLock<Script>` (keeps the SHA
    so the connection's EVALSHA cache stays warm); `Script::invoke_async` handles `NOSCRIPT`
    fallback, so single-op locks need **no** `execute_pipeline_with_script_retry`. Redis failures
    surface as `DistkitError::RedisError` via `?`.
  - Tests: `src/lock/tests/backend.rs` (registered in `tests/mod.rs`) — 5 direct-against-Redis owner
    cases: acquire exclusion, owner-gated refresh (+PTTL bump), owner-gated release, re-acquire after
    release, lease-expiry frees key. Drives the backend on raw keys via `make_options`.
  - Verified: `make test` green (194 unit + 77 doctest, all passing).
- **Stage 2 — `Mutex` + guard. ✅ Done.** User-facing `Mutex` + RAII `MutexGuard` in
  `src/lock/mutex.rs`, key namespacing, and the shared acquire core. No auto-refresh (Stage 3).
  - `src/lock/mod.rs`: added `LockOptions.namespace` (`DistkitRedisKey`, default `distkit-locks`
    via `DEFAULT_LOCK_NAMESPACE`); added `LockOptionsBuilder` (chainable setters seeded by
    `LockOptions::new`) reachable via `LockOptionsBuilder::new` **and** `LockOptions::builder`.
  - `Mutex::new(options) -> Arc<Self>` destructures options (mirrors `StrictCounter::new`),
    precomputes `full_key = {namespace}:{key}`, resolves `owner` (UUIDv4 default) and
    `ttl_ms = ttl.as_millis() as i64`.
  - Shared `acquire_core(timeout: Option<Duration>, retry_interval)` retry loop: `None` = forever,
    `Some(ZERO)` → `AcquireFail`, `Some(d)` bounded → `Timeout { waited }`. Polls via a
    `tokio::time::interval` (`MissedTickBehavior::Delay`, first tick immediate). `lock` /
    `try_lock` / `try_lock_for` are thin wrappers.
  - `MutexGuard` holds conn + `full_key` + `owner` + an `is_released` flag. `release(self)` awaits
    `backend::release` and sets the flag; `Drop` skips when already released, else `tokio::spawn`s a
    fire-and-forget `backend::release` (logs on error). (Refresh `JoinHandle` field lands in
    Stage 3.)
  - `LockError::WouldBlock` was renamed to `LockError::AcquireFail`.
  - Tests: `src/lock/tests/mutex.rs` (exclusion, lock-waits-then-succeeds, `try_lock_for` timeout,
    explicit-release frees, drop frees, lease-expiry frees) + `src/lock/tests/options.rs`
    (`LockOptions` defaults, unique owner per `new`, builder defaults match `new`, builder overrides
    every field, `LockOptions::builder` entry point).
  - Verified: `make test` green (205 unit + 79 doctest, incl. new `Mutex::new` and
    `LockOptions::builder` doctests); `cargo doc --all-features` clean.
- **Stage 3 — Auto-refresh.** Background renewal task, abort-on-drop, lost-lock handling. Tests:
  lease survives past `ttl`; killed refresh → `LockLost`.
- **Stage 4 — RwLock backend.** ZSET reader model + writer key Lua (read/write acquire/refresh/
  release, server `TIME`, lazy purge). Direct-Redis tests for shared-read / exclusive-write /
  expiry purge.
- **Stage 5 — `RwLock` + guards.** `read`/`write` + `try_*` + `try_*_for` over the shared
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
- `src/lock/tests/{common,backend,mutex,options,rwlock}.rs`
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
  - Two `Mutex` instances (distinct owners) on one key → the second `try_lock` returns
    `WouldBlock`; `lock()` blocks then succeeds after the first guard drops.
  - `RwLock` → multiple concurrent `read()` succeed; `write()` waits for all readers/the writer
    to drop.
