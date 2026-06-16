# Changelog

Notable changes to distkit, newest first. For the full commit log, see the
[GitHub releases](https://github.com/dev-davexoyinbo/distkit/releases).

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
