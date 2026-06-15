# Distributed Lock Plan — `distkit`

Library-friendly distributed locks for `distkit`: **`Mutex`** and **`RwLock`**, with a
surface that mirrors `tokio::sync::Mutex` / `tokio::sync::RwLock` as closely as a network lock allows.

> Status: **Stages 0–6 done (scaffolding + mutex backend + `Mutex`/guard + auto-refresh + writer-preferring rwlock backend + `RwLock`/guards + docs/doctests/benches), plus Rust-layer input validation (`ttl_ms`/`owner`) on both backends.** `make test`: 249 unit + 82 doctest, green. Only the out-of-scope celeris-realtime migration remains. Test suites follow the "Testing Philosophy" in `AGENTS.md` — they assert expected behavior, and a failure is a bug to fix in the backend.

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
  mod.rs            # LockOptions, LockMode, re-exports, module docs
  error.rs          # LockError enum
  helpers_lua.rs    # shared Lua prelude (now_ms / purge_pending_writers)
  mutex_backend.rs  # low-level mutex Redis ops (acquire/refresh/release)
  rwlock_backend.rs # low-level writer-preferring rwlock Redis ops
  mutex.rs          # Mutex + MutexGuard
  rwlock.rs         # RwLock + RwLockReadGuard + RwLockWriteGuard
  tests/
    common.rs       # make_mutex / make_rwlock + unique-prefix key helper
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
    // NOTE: lease auto-refresh (every ttl/3) is unconditional/by-contract — no opt-out field.
}

impl LockOptions {
    // defaults: namespace="distkit-locks", ttl=30s, owner_id=UUIDv4, max_wait=None,
    //           retry_interval=50ms
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

### RwLock (writer-preferring, v1)

Upholds the invariant **once a writer is waiting, later readers may not jump ahead of it**, and
serves waiting writers in **FIFO arrival order**. Four keys per resource:

- Writer key `{namespace}:{key}:w` holds the writer `owner_id` (PX ttl).
- Readers `{namespace}:{key}:r` = ZSET of `reader_owner_id` scored by **expiry** (`now + ttl`).
- Pending `{namespace}:{key}:pw` = ZSET of waiting-`writer_owner_id` scored by **arrival**
  (immutable via `ZADD NX`; gives FIFO order among writers).
- Pending-heartbeat `{namespace}:{key}:pwh` = ZSET of the same waiting writers scored by an
  **expiry** heartbeat (`now + ttl`), refreshed each acquire attempt so a crashed waiter is purged.

All timestamps come from `redis.call('TIME')` inside Lua (server clock):
`local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)`.

| Operation     | Lua (atomic)                                                                                                                                                                                  |
| ------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| acquire read  | purge expired readers + dead waiting-writers (`:pwh` heartbeat); if writer key exists **or** any writer is waiting (`ZCARD :pw > 0`) → fail; else `ZADD` self `now+ttl`, set `:r` TTL, ok       |
| acquire write | purge as above; if `mark_pending` register/refresh self (`:pw` `ZADD NX` + `:pwh`); if another writer holds **or** any reader remains **or** this owner isn't the `:pw` front → fail; else clear self from pending, `SET w owner PX ttl`, ok |
| refresh read  | purge expired readers; if own slot still present re-`ZADD` own score, else fail (lease lost)                                                                                                   |
| refresh write | `GET == owner ? PEXPIRE` (reuses the mutex `refresh`)                                                                                                                                          |
| release read  | `ZREM` self from `:r`                                                                                                                                                                          |
| release write | `GET == owner ? DEL` (reuses the mutex `release`)                                                                                                                                              |
| clear pending | `ZREM` self from `:pw` + `:pwh` — when a waiting writer gives up (timeout/drop) before acquiring                                                                                               |

One-shot `try_write` passes `mark_pending = false`: it never registers (so a failed one-shot write
does **not** block readers) but still yields to any writer already queued ahead.

Crashed holders self-heal: readers via score expiry + lazy purge; waiting writers via the `:pwh`
heartbeat + lazy purge; writer via PX.

**Known limitation:** writer-preferring → possible **reader starvation** under constant writers
(the inverse of the read-preferring trade-off). FIFO arrival order keeps waiting *writers* fair
among themselves. **Caveat:** the waiting writer's queue slot is kept alive by its acquire-attempt
cadence, so `retry_interval` must stay `< ttl`; otherwise the heartbeat lapses between attempts and
the writer re-registers with a fresh arrival score (losing its place).

---

## Guards & auto-refresh

- `MutexGuard` / `RwLockReadGuard` / `RwLockWriteGuard` each hold: the owner, the
  key(s), a cloned `ConnectionManager`, and the refresh task `JoinHandle`.
- **Drop:** abort the refresh task; `tokio::spawn` a fire-and-forget release (Drop is sync, release
  is async) — same approach as the current `DistributedLockGuard`. An explicit
  `async fn release(self) -> Result<MutexLockState, DistkitError>` lets callers await and observe the
  final lock state (`Released` or `Lost`); tokio has no such method.
- **auto-refresh (always on):** on a successful acquire, spawn a task that renews the lease every
  `ttl/3`. On any failed refresh — lost ownership (`Ok(false)`) or a transport error — it sets a
  shared `Arc<AtomicBool>` (`lost`) but **keeps ticking**; a later successful refresh clears the flag
  (the lease self-heals). The flag is surfaced as `MutexLockState::Lost` via `MutexGuard::get_state`
  and the value returned by `release`. Refresh is unconditional by contract; there is no opt-out.
  (`RwLock` guards in Stage 5 follow the same shape.)

### Lock semantics & limitations (documented for users)

The guard is a **strong signal**, not an ironclad guarantee — the lease lives in Redis, not in this
process. Callers must understand:

- A `MutexGuard` only ever comes from a **confirmed acquisition**: no guard means no lock.
- Each `ttl/3` refresh re-confirms ownership with Redis. If a refresh **can't be confirmed** (Redis
  unreachable, round-trip error, or the key no longer maps to us) the lock is marked `Lost`. The
  refresh task keeps trying, and a later **confirmed** refresh flips it back to `Acquired` — so a
  brief network blip self-heals.
- A **network partition longer than the TTL** is the real hazard: the lease expires in Redis, another
  owner can take the key, and once that happens our refresh has nothing to reclaim, so the lock stays
  `Lost`. This is the classic lease-lock trade-off; we do not (and cannot, over a network) prevent it.
- Because of this, code whose correctness depends on the lock should **re-check
  `MutexGuard::get_state` before the critical section** rather than trusting the guard's existence
  alone. `get_state` reads the latest refresh result (no extra Redis round-trip).

This is surfaced to users in the rustdoc on `Mutex`, `MutexGuard`, `get_state`, `release`, and
`MutexLockState`.

---

## Error type

Extend `DistkitError` (`src/error.rs`) with a feature-gated variant mirroring `CounterError`:

```rust
#[cfg(feature = "lock")]
#[error("Lock Error: {0}")]
LockError(#[from] LockError),
```

`src/lock/error.rs` (current):

```rust
#[derive(Debug, thiserror::Error, PartialEq)]
pub enum LockError {
    #[error("failed to acquire lock (would block)")]  AcquireFail,
    #[error("timed out after {waited:?}")]            Timeout { waited: Duration },
    #[error("not the lock owner")]                    NotOwner,
    #[error("ttl_ms must be positive, got {0}")]      InvalidTtl(i64),
    #[error("owner id must not be empty")]            InvalidOwner,
}
```

`LockLost` (Stage 0 scaffold) was superseded by `MutexLockState::Lost` in Stage 3 and removed.

`InvalidTtl` / `InvalidOwner` (added alongside the Stage 1/4 validation work below) are returned
by two shared, crate-internal validators also in `error.rs`:

```rust
pub(crate) fn validate_ttl(ttl_ms: i64) -> Result<(), DistkitError> {
    if ttl_ms <= 0 {
        return Err(LockError::InvalidTtl(ttl_ms).into());
    }
    Ok(())
}

pub(crate) fn validate_owner(owner: &str) -> Result<(), DistkitError> {
    if owner.is_empty() {
        return Err(LockError::InvalidOwner.into());
    }
    Ok(())
}
```

Both `mutex_backend` and `rwlock_backend` call these at the top of every op that takes a `ttl_ms`
and/or `owner` argument, before issuing any Redis command — see Stage 1 and Stage 4 below.

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
  - **Input validation (added post-Stage-4, mirrored from rwlock_backend):** `acquire` calls
    `validate_owner(owner)?; validate_ttl(ttl_ms)?;`; `refresh` calls `validate_ttl(ttl_ms)?;` —
    both before touching Redis, returning `LockError::InvalidOwner` / `InvalidTtl(ttl_ms)`. Tests:
    `acquire_rejects_nonpositive_ttl`, `refresh_rejects_nonpositive_ttl`,
    `acquire_rejects_empty_owner` in `src/lock/tests/mutex_backend.rs`. `release` is unchanged
    (no ttl/owner-emptiness contract).
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
- **Stage 3 — Auto-refresh. ✅ Done.** Background renewal task, abort-on-drop, lost-lease handling,
  observable lock state.
  - **Removed the `auto_refresh` opt-out** (struct field, builder setter, option tests): renewal is
    unconditional by contract — a `Mutex` that can silently lose its lease mid-hold is unsafe.
  - **`MutexLockState`** (new `pub enum` in `src/lock/mutex.rs`, re-exported via `lock::*`):
    `Acquired` / `Lost` / `Released`. Replaces the earlier scratch `LockState` in `mod.rs` and the
    `LockError::LockLost`-on-release design — a lost lease is now a *state*, not an error.
  - `Mutex` gains a `ttl_duration: Duration` field (alongside `ttl_ms`) so the refresh ticker can use
    `ttl_duration / 3` directly.
  - `Mutex::spawn_refresh(&self, lost: Arc<AtomicBool>) -> JoinHandle<()>`: ticks a
    `tokio::time::interval(ttl_duration / 3)` (`MissedTickBehavior::Delay`, first immediate tick
    skipped — acquire just set the lease). On each tick it calls `backend::refresh`:
    - `Ok(true)` → lease renewed; if `lost` was set, clears it (`swap(false, AcqRel)`) and logs that
      the lease was re-acquired — **the task self-heals rather than stopping**.
    - `Ok(false)` / `Err` → sets `lost` (`store(true, Release)`) and keeps ticking (a later success
      can recover it). The task runs until the guard aborts it on release/drop.
    `acquire_core` spawns it on every successful acquire and stores the `JoinHandle` + shared
    `Arc<AtomicBool>` (`lost`) on the guard.
  - `MutexGuard` fields: `refresh_handle: Option<JoinHandle<()>>` + `lost: Arc<AtomicBool>`
    (no `is_released` flag — `refresh_handle == None` means released).
    - `get_state(&self) -> MutexLockState`: `None` handle → `Released`; `lost` set → `Lost`; else
      `Acquired`.
    - `release(self) -> Result<MutexLockState, DistkitError>`: aborts the handle; if `lost`, returns
      `Ok(Lost)` **without** issuing a DEL (the key is no longer ours); else owner-checked release →
      `Ok(Released)`.
    - `Drop`: takes the handle (absent ⇒ already released, bail), aborts it, then spawns the
      fire-and-forget owner-checked release.
  - `LockError::LockLost` is retained in `error.rs` but no longer produced by the mutex path
    (superseded by `MutexLockState::Lost`).
  - Tests (`src/lock/tests/mutex.rs`): `auto_refresh_keeps_lease_alive` (held lease survives past
    `ttl`), `get_state_reports_acquired_while_held`, and `lost_lease_reports_lost_state` (external
    `DEL` → next refresh tick → `get_state()` is `Lost` and `release()` returns `Ok(Lost)`);
    replaces the now-invalid `lease_expiry_frees_lock`. Harness gains `make_options_with_key` +
    `raw_connection` in `tests/common.rs`.
  - **One-shot acquire / zero-interval fix:** `acquire_core` now builds the poll ticker lazily — a
    zero `retry_interval` yields `None` (no `tokio::time::interval`, which panics on a zero period).
    With no ticker the loop makes a single attempt and returns `LockError::AcquireFail` on failure,
    so `try_lock` (zero interval) is a true one-shot; non-zero intervals tick as before
    (`MissedTickBehavior::Delay`, first tick immediate) and still honor the bounded-`timeout`
    `Timeout { waited }` path.
  - Verified: `make test` green (207 unit + 79 doctest, incl. the 8 mutex tests); `cargo clippy
    --features lock` clean for the lock module.
- **Stage 4 — RwLock backend. ✅ Done.** Writer-preferring shared/exclusive ops in
  `src/lock/rwlock_backend.rs` (mutex ops live alongside in `src/lock/mutex_backend.rs`; the rwlock
  writer key reuses the mutex `refresh`/`release`), each an atomic Redis/Lua round-trip on
  fully-namespaced keys (mirrors the
  Stage 1 mutex ops: function-local `static OnceLock<Script>`, `Ok(n == 1)`, errors via `?`).
  - **Keys:** `:w` (writer str), `:r` (reader ZSET, expiry score), `:pw` (waiting-writer ZSET,
    arrival score → FIFO), `:pwh` (waiting-writer ZSET, expiry heartbeat → crash purge). Server
    `redis.call('TIME')` for every clock.
  - **New ops:** `acquire_read`, `acquire_write(.., mark_pending: bool)`, `refresh_read`,
    `release_read` (plain `ZREM`), `clear_pending_write`. `refresh_write` / `release_write` **reuse**
    the mutex `refresh` / `release` on the `:w` key — no new code.
  - **Writer preference:** `acquire_read` fails while `ZCARD :pw > 0`, so later readers can't jump
    ahead of a waiting writer. `acquire_write` registers in `:pw`/`:pwh` only when `mark_pending`
    (the waiting forms), so a one-shot `try_write` never stalls readers, and only the `:pw` front
    (earliest arrival) may take the lock → FIFO among writers.
  - **Input validation (added post-implementation, per "Testing Philosophy" expected-behavior
    pass):** `acquire_read` and `acquire_write` call `validate_owner(owner)?; validate_ttl(ttl_ms)?;`;
    `refresh_read` and `refresh_write` call `validate_ttl(ttl_ms)?;` — all before touching Redis/the
    Lua script, returning `LockError::InvalidOwner` / `InvalidTtl(ttl_ms)`. `release_read` /
    `release_write` are unchanged. See "Error type" above for the shared `validate_ttl` /
    `validate_owner` helpers.
  - **Tests:** `src/lock/tests/rwlock_backend.rs` (registered in `tests/mod.rs`; harness gains
    `make_options_with_rw_keys` + `RwKeys` in `tests/common.rs`) — 28 direct-against-Redis cases:
    the original 15 happy-path tests (shared read, writer-excludes-readers/writers,
    reentrant write, read-blocked-by-waiting-writer, one-shot write doesn't enqueue/block, FIFO
    writers, reader/pending-writer expiry purge, `refresh_read`/`refresh_write`/`release_write`
    owner-gating), plus a 13-test **expected-behavior** edge-case section added per `AGENTS.md`
    "Testing Philosophy" (a failing case is a bug to fix in the backend, not to relax):
    `write_held_until_all_readers_released` (write withheld until **all** readers release);
    `writer_blocked_by_writer_enqueues_and_keeps_preference` (a writer blocked by another writer
    enqueues into `:pw` and keeps FIFO preference on handoff);
    `same_ms_writer_fifo_is_lexical` (same-millisecond arrival ties broken lexically);
    `purge_uses_caller_ttl_not_per_member` (reader-expiry purge uses the caller's ttl, not a
    per-member stored ttl); `idle_waiting_writer_loses_its_slot` (an idle waiting writer loses its
    FIFO slot once its `:pwh` heartbeat lapses); `writer_key_expires_via_px_only` (`:w` self-heals
    via Redis `PX` alone, no lazy purge); `release_read_nonmember_and_double_release_return_false`
    (`release_read` on a non-member or double-release returns `Ok(false)`); and six input-validation
    cases — `acquire_read_rejects_nonpositive_ttl`, `acquire_write_rejects_nonpositive_ttl`,
    `refresh_read_rejects_nonpositive_ttl`, `refresh_write_rejects_nonpositive_ttl`,
    `acquire_read_rejects_empty_owner`, `acquire_write_rejects_empty_owner`.
  - Verified: `make test` green (238 unit + 79 doctest); `cargo clippy --features lock --tests`
    clean for the lock module. (The ops show `dead_code` in a plain non-test build until Stage 5
    consumes them — same interim as Stage 0's `allow(unused_imports)`.)
- **Stage 5 — `RwLock` + guards. ✅ Done.** User-facing `RwLock` + RAII `RwLockReadGuard` /
  `RwLockWriteGuard` in `src/lock/rwlock.rs`, mirroring `Mutex`/`MutexGuard` and dispatching read vs
  write via the existing `LockMode`. The `:w`/`:r`/`:pw`/`:pwh` keys are derived in `RwLock::new`.
  - Six acquire forms (`read`/`try_read`/`try_read_for` + `write`/`try_write`/`try_write_for`) are
    thin wrappers over `acquire_loop(mode, timeout, retry_interval)` — same retry/timeout/interval
    skeleton as the mutex `acquire_core` (lazy `tokio::time::interval`, zero interval ⇒ one-shot,
    `AcquireFail`/`Timeout { waited }`/forever). On success each `acquire_*` path builds its guard +
    spawns the mode-appropriate refresh.
  - **`mark_pending = !retry_interval.is_zero()`:** the waiting write forms enqueue into `:pw`/`:pwh`
    (FIFO preference); one-shot `try_write` passes `false` so a failed attempt never blocks readers.
  - **Give-up cleanup:** when a `mark_pending` writer exhausts its timeout / returns `AcquireFail`,
    `acquire_loop` calls `release_write` to `ZREM` its `:pw`/`:pwh` slot (no new backend op needed;
    `release_write` already clears pending and no-ops the writer DEL we don't own). A future dropped
    mid-`await` self-heals via the `:pwh` heartbeat lapse.
  - **Guards:** read guard carries `:r` + owner (release/refresh via `release_read`/`refresh_read`);
    write guard carries `:w`/`:pw`/`:pwh` + owner (via `release_write`/`refresh_write`). Both follow
    the `MutexGuard` shape — `refresh_handle: Option<JoinHandle>` + `lost: Arc<AtomicBool>`,
    `get_state`, awaitable `release`, fire-and-forget `Drop`. `spawn_refresh(mode, lost)` dispatches
    the per-mode refresh in one shared task body. Guard state is the shared
    `LockGuardState { Released, Lost, Acquired }` in `src/lock/mod.rs` (the old per-type
    `MutexLockState`/`RwLockState` were unified into it — every guard's `get_state`/`release`
    returns it).
  - `src/lock/mod.rs`: dropped the `#[allow(unused_imports)]` on `pub use rwlock::*`; the rwlock
    backend ops are no longer `dead_code`.
  - Tests (`src/lock/tests/rwlock.rs`, registered in `tests/mod.rs`): `concurrent_reads_share`,
    `writer_excludes_readers`, `writer_waits_for_readers`, `reader_waits_for_writer`,
    `waiting_writer_blocks_new_readers` (writer preference), `write_release_frees`, `read_drop_frees`,
    `auto_refresh_keeps_lease_alive`, `get_state_reports_acquired`, `lost_lease_reports_lost`,
    `try_write_does_not_block_readers`.
  - Verified: `make test` green (249 unit + 80 doctest, incl. the new `RwLock::new` doctest);
    `cargo clippy --features lock --tests` clean for the lock module; `cargo build --features lock` ok.
- **Stage 6 — Docs, doctests, benches. ✅ Done.** Per-item module/type rustdoc was already in place
  from Stages 2–5 (crate-wide `#![deny(missing_docs)]`), so this stage added the discoverability +
  benchmark surface:
  - `docs/lib.md`: intro "four modules", `lock` feature-table row, a new **Distributed locks**
    section with two runnable `Mutex` / `RwLock` doctests (REDIS_URL fallback), a `LockError`
    bullet in the error section, and intra-doc link refs (`Mutex`/`RwLock`/`LockOptions`/etc.).
  - `benches/lock.rs` (new) + `make_mutex`/`make_rwlock`/`make_contended_mutexes` in
    `benches/common.rs` + `[[bench]] name = "lock"` in `Cargo.toml`. Groups `lock_mutex`
    (`acquire_release`, `try_lock_contended`) and `lock_rwlock` (`read_acquire_release`,
    `write_acquire_release`). The contended holder is released via `rt.block_on(release)` because a
    bare `Drop` outside the Tokio context would panic in the guard's spawn-on-drop.
  - `README.md`: intro sentence, **Mutex / RwLock** feature bullet, `lock` feature-flag row, and a
    **Distributed locks** usage section.
  - `AGENTS.md`: corrected the stale "RwLock is still a stub" note — both locks now ship as live
    behavior.
  - Verified: `make test` green (249 unit + 82 doctest, +2 new lock doctests); `cargo doc
    --all-features --no-deps` clean; `cargo bench --bench lock` runs all four groups (no `CLAUDE.md`
    exists — `AGENTS.md` is the equivalent and was updated instead).

**Out of scope (future):** celeris-realtime migration off `app/src/distributed_lock/` onto
`distkit::lock`; lock-acquired metrics/tracing spans.

---

## Critical files

**New**

- `src/lock/{mod,error,helpers_lua,mutex_backend,rwlock_backend,mutex,rwlock}.rs`
- `src/lock/tests/{common,mutex_backend,mutex,options,rwlock_backend,rwlock}.rs`
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
