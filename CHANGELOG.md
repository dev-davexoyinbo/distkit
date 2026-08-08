# Changelog

Notable changes to distkit, newest first. For the full commit log, see the
[GitHub releases](https://github.com/dev-davexoyinbo/distkit/releases).

## 0.7.0 — 2026-08-08

- **Breaking:** upgraded the optional `trypema` re-export from v1 to v2. The
  monolithic `RateLimiter` and its option structs are replaced by independently
  constructed `LocalRateLimiterProvider`, `RedisRateLimiterProvider`, and
  `HybridRateLimiterProvider` values using the shared `RateLimiterBuilder`
  trait.
- Trypema configuration now uses semantic, unit-aware types:
  `WindowSizeSeconds` is now `WindowSize`, `RateGroupSizeMs` is now
  `BucketSize`, `SuppressionFactorCacheMs` is now
  `SuppressionFactorCachePeriod`, and `SyncIntervalMs` is now `SyncInterval`.
- `RateLimit` now uses unit-explicit constructors such as `per_second`, and
  rejected decisions expose `retry_after: Duration` instead of
  `retry_after_ms`.
- The `trypema` feature name and the `distkit::trypema` module path are
  unchanged.

## 0.6.0 — 2026-06-26

- **Breaking:** the guard inspectors `get_state` and `get_on_attempt` are no
  longer `async`. Both only read locally cached state (an `AtomicBool` and a
  stored `usize`) and never touched Redis, so the `async` was unnecessary. Drop
  the `.await` at call sites: `guard.get_state()` and `guard.get_on_attempt()`.
  Affects `MutexGuard`, `RwLockReadGuard`, and `RwLockWriteGuard`.

## 0.5.3 — 2026-06-17

- New bounded lock-acquisition methods that source the poll interval from the
  lock's configured `retry_interval` instead of taking it per call:
  - `try_lock_with_timeout` / `try_read_with_timeout` / `try_write_with_timeout`
    take only a `timeout` and return `LockError::Timeout` if the deadline passes.
  - `try_lock_with_retries` / `try_read_with_retries` / `try_write_with_retries`
    bound by attempt count instead of time: `max_retries` retries after the
    initial attempt, returning the new `LockError::RetriesExhausted { retries }`
    when every attempt fails.
- Deprecated `try_lock_for` / `try_read_for` / `try_write_for` (the
  `(timeout, retry_interval)` forms) in favor of the `*_with_timeout` methods.

## 0.5.2 — 2026-06-17

- Lock guards now expose `get_on_attempt`: the zero-based acquire poll that
  obtained the lock — `0` when the first poll succeeded, higher when the acquire
  waited through contention (a one-shot `try_*` is always `0`). Available on
  `MutexGuard`, `RwLockReadGuard`, and `RwLockWriteGuard`. Useful for contention
  metrics and backoff tuning.

## 0.5.1 — 2026-06-16

- Documentation updates.

## 0.5.0 — 2026-06-15

The **distributed locks** release.

- New `lock` feature: Redis-backed `Mutex` and a writer-preferring `RwLock`,
  mirroring `tokio::sync`.
- RAII guards with background lease renewal (every `ttl/3`) and an awaitable
  `release()`.
- Unified `LockGuardState` (`Acquired` / `Lost` / `Released`) across all guard
  types.
- TTL and owner are validated up front before any Redis operation.
- Added lock benchmarks.

## 0.4.0 — 2026-04-13

- Stability and internal refinements across counters and instance-aware counters.

## 0.3.0 — 2026-04-13

- Continued hardening of the counter and instance-aware counter paths.

## 0.2.3 — 2026-04-08

- Added the `full` feature flag to enable every primitive at once.

## 0.2.0 — 2026-04-03

- Added **instance-aware counters** (`StrictInstanceAwareCounter`,
  `LaxInstanceAwareCounter`) with automatic dead-instance cleanup.
- Added the `trypema` feature: sliding-window rate limiting re-exported under
  `distkit::trypema`.
- `counter` and `instance-aware-counter` are the default features.

## 0.1.0 — 2026-03-29

- Initial release: distributed `StrictCounter` and `LaxCounter`, backed by Redis.
