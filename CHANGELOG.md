# Changelog

Notable changes to distkit, newest first. For the full commit log, see the
[GitHub releases](https://github.com/dev-davexoyinbo/distkit/releases).

## 0.7.2 - 2026-11-05

- **Fixed:** `LaxInstanceAwareCounter` reads no longer miss this instance's
  own writes while a flush is in flight. Between swapping the buffered delta
  out and applying the Redis reply, `get`, `inc_if` and the other read paths
  returned values short by the in-flight amount for a full Redis round-trip,
  which could make conditional updates such as `inc_if(Gt(0), -1)` skip an
  update that should have applied. Each key now tracks its in-flight amount,
  every read includes it, and a comparator check and its update run in one
  critical section.
- **Fixed:** a flush that failed no longer loses or re-sends deltas. A failed
  flush inside `set`, `del`, `clear` or their variants dropped the swapped
  delta, and a background flush that failed after part of a multi-chunk batch
  was applied sent the applied part again, which double-counted (both lax
  counters). Confirmed chunks now settle at once and unconfirmed amounts
  return to the buffer for the next flush. A flush interrupted by a
  connection failure stays at-least-once: the unconfirmed amount is retried
  and can apply twice.
- **Fixed:** `set_on_instance` and `set_all_on_instance` no longer overshoot
  the instance slice when called while a flush is in flight.
- **Fixed:** a flush reply that arrives after `del` removed a key no longer
  re-creates a stale cache entry for that key.
- **Fixed:** on a quiet instance, a buffered write could wait up to an
  activity epoch (15 s) before flushing. The flush task now drains the buffer
  before it goes idle, and the first write on an idle instance wakes it at
  once. Applies to `LaxCounter` and `LaxInstanceAwareCounter`.
- Counters now delete a record that reaches 0, since a missing record reads
  as 0. The plain counters delete the hash field; the instance-aware
  counters delete an instance's slice field when that slice reaches 0, and
  the shared record (cumulative field and keys-set member) only when no
  registered instance, dead-but-unreaped ones included, holds a field for
  the key — a key held at -1 and +1 keeps its records. `set(key, 0)`
  deletes the storage and keeps the epoch bump, as `del` does.
- New `cleanup_interval: Option<Duration>` on `CounterOptions`,
  `StrictInstanceAwareCounterOptions` and `LaxInstanceAwareCounterOptions`
  (default `None`: no task). When set, a background task sweeps the
  counter's records on that interval: it deletes records at 0 that older
  versions left behind, reaps dead instances on a timer instead of waiting
  for traffic, and deletes shared records that no instance holds. Scans are
  budgeted (about 10 000 fields per tick) and resume across ticks.

## 0.7.1 - 2026-10-10

- **Fixed:** `StrictInstanceAwareCounter` and `LaxInstanceAwareCounter` no
  longer fail with `too many results to unpack` after an instance dies. In
  0.7.0 and earlier versions, all calls except `clear` failed for a counter
  with more than approximately 8,000 keys, and the counter did not recover. The
  cleanup now reads the counts of the dead instance, not all keys of the
  counter. A deployment that fails today recovers on its first call after the
  upgrade. No manual Redis step is necessary.
- Dead-instance cleanup now has a limit for each call. One call removes dead
  instances until it has read approximately 10,000 of their counts. The next
  calls remove the dead instances that stay.
- **Fixed:** `clear_on_instance` no longer fails when the instance holds more
  than approximately 8,000 keys.
- **Fixed:** `RwLock` no longer fails to acquire when more than approximately
  8,000 reader entries or pending-writer entries expire together.
- The heartbeat of the instance-aware counters now logs an error when it
  fails. Before, it ignored the error.

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
