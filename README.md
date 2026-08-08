# distkit

A toolkit of distributed systems primitives for Rust, backed by Redis.

[![Crates.io](https://img.shields.io/crates/v/distkit.svg)](https://crates.io/crates/distkit)
[![Documentation](https://docs.rs/distkit/badge.svg)](https://docs.rs/distkit)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

## What is distkit?

distkit provides building blocks for distributed applications. It ships
distributed counters (strict and lax), instance-aware counters, distributed
locks (`Mutex`, `RwLock`), and rate limiting, all backed by Redis.

Documentation and guides: <https://distkit.davidoyinbo.com>

## Features

- **StrictCounter** -- every operation executes a Redis Lua script atomically.
  Reads always reflect the latest write. Best for billing, inventory, or
  anything where accuracy is critical.
- **LaxCounter** -- buffers increments in memory and flushes to Redis every
  ~20 ms. Sub-microsecond latency on the hot path. Best for analytics and
  high-throughput metrics.
- **Instance-aware counters** -- each running instance owns a named slice of the
  total, with automatic cleanup of contributions from instances that stop
  heartbeating.
- **Mutex / RwLock** (opt-in `lock` feature) -- Redis-backed distributed locks
  mirroring `tokio::sync::Mutex` / `tokio::sync::RwLock`. RAII guards, background
  lease refresh, writer-preferring reader-writer locking.
- **Rate limiting** (opt-in `trypema` feature) -- sliding-window rate limiting
  with local, Redis-backed, and hybrid providers. Supports absolute and
  probabilistic suppression strategies.
- **Safe by default** -- `#![forbid(unsafe_code)]`, no panics in library code.

## Feature flags

| Feature                  | Default | Description                                                              |
| ------------------------ | ------- | ------------------------------------------------------------------------ |
| `counter`                | **yes** | Distributed counters (`StrictCounter`, `LaxCounter`)                     |
| `instance-aware-counter` | no      | Per-instance counters (`StrictInstanceAwareCounter`, `LaxInstanceAwareCounter`) |
| `lock`                   | no      | Distributed locks (`Mutex`, `RwLock`)                                    |
| `trypema`                | no      | Rate limiting via the [trypema](https://docs.rs/trypema) crate           |

## Installation

```sh
cargo add distkit
```

Or add to `Cargo.toml`:

```toml
[dependencies]
distkit = "0.7"
```

To enable instance-aware counters or rate limiting:

```toml
[dependencies]
distkit = { version = "0.7", features = ["instance-aware-counter", "trypema"] }
```

Counters and locks require Redis 5.0+. Trypema's local provider does not contact
Redis; its Redis and hybrid providers require Redis 7.2+.

## Quick start

```rust
use distkit::{DistkitRedisKey, counter::{StrictCounter, LaxCounter, CounterOptions, CounterTrait}};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = redis::Client::open("redis://127.0.0.1/")?;
    let conn = client.get_connection_manager().await?;

    let prefix = DistkitRedisKey::try_from("my_app".to_string())?;
    let options = CounterOptions::new(prefix, conn);

    let key = DistkitRedisKey::try_from("page_views".to_string())?;

    // Strict: immediate consistency
    let strict = StrictCounter::new(options.clone());
    strict.inc(&key, 1).await?;
    let total = strict.get(&key).await?;
    println!("strict: {total}");

    // Lax: eventual consistency, much faster
    let lax = LaxCounter::new(options);
    lax.inc(&key, 1).await?;
    let approx = lax.get(&key).await?;
    println!("lax: {approx}");

    Ok(())
}
```

## Counter types

### StrictCounter

Every call is a single Redis round-trip executing an atomic Lua script. The
counter value is always authoritative.

```rust
let key = DistkitRedisKey::try_from("orders".to_string())?;
strict.inc(&key, 1).await?;   // HINCRBY via Lua
strict.set(&key, 100).await?; // HSET via Lua
strict.del(&key).await?;      // HDEL, returns old value
strict.clear().await?;        // DEL on the hash
```

Conditional writes use `CounterComparator` and return `(new, old)`. When the
comparison fails, `new == old`.

```rust
use distkit::CounterComparator;

strict.set(&key, 10).await?;
assert_eq!(
    strict.inc_if(&key, CounterComparator::Eq(10), 5).await?,
    (15, 10)
);
assert_eq!(
    strict.set_if(&key, CounterComparator::Gt(20), 99).await?,
    (15, 15)
);
```

Batch increments follow the same rules and preserve input order.

```rust
let results = strict
    .inc_all_if(&[
        (&key, CounterComparator::Eq(15), 2),
        (&key, CounterComparator::Nil, 3),
    ])
    .await?;
assert_eq!(results, vec![(&key, 17, 15), (&key, 20, 17)]);
```

### LaxCounter

Writes are buffered in a local `DashMap` and flushed to Redis in batched
pipelines every `allowed_lag` (default 20 ms). Reads return the local view
(`remote_total + pending_delta`), which is always consistent within the same
process.

```rust
let key = DistkitRedisKey::try_from("impressions".to_string())?;
lax.inc(&key, 1).await?;         // local atomic add, sub-microsecond
let val = lax.get(&key).await?;  // reads local state, no Redis hit
```

A background Tokio task handles flushing. It holds a `Weak` reference to the
counter, so it stops automatically when the counter is dropped.

### Choosing a counter

|                           | `StrictCounter`                        | `LaxCounter`                       | `StrictInstanceAwareCounter`          | `LaxInstanceAwareCounter`                  |
| ------------------------- | -------------------------------------- | ---------------------------------- | ------------------------------------- | ------------------------------------------ |
| **Consistency**           | Immediate                              | Eventual (default: ~20 ms lag)     | Immediate                             | Eventual (`flush_interval` lag)            |
| **`inc` latency**         | Redis round-trip                       | Sub-microsecond (warm path)        | Redis round-trip                      | Sub-microsecond (warm path)                |
| **Redis I/O**             | Every operation                        | Batched on interval                | Every `inc`                           | Batched on interval                        |
| **`set` / `del`**         | Immediate                              | Immediate                          | Immediate (bumps epoch)               | Flushes pending delta, then immediate      |
| **Per-instance tracking** | No                                     | No                                 | Yes                                   | Yes                                        |
| **Dead-instance cleanup** | No                                     | No                                 | Yes                                   | Yes                                        |
| **Feature flag**          | `counter` (default)                    | `counter` (default)                | `instance-aware-counter`              | `instance-aware-counter`                   |
| **Use case**              | Billing, inventory, exact global count | Analytics, high-throughput metrics | Connection counts, exact live metrics | High-frequency per-node throughput metrics |

## Instance-aware counters

Enable the `instance-aware-counter` feature:

```toml
[dependencies]
distkit = { version = "0.7", features = ["instance-aware-counter"] }
```

Instance-aware counters track each running instance's contribution separately.
The cumulative total is the sum of all **live** instances. When an instance stops
heartbeating for longer than `dead_instance_threshold_ms` (default 30 s), its
contribution is automatically subtracted from the cumulative on the next
operation by any surviving instance.

This makes them well-suited for:

- **Connection pool sizing** -- each server reports its active connection count;
  the cumulative is the cluster-wide total.
- **Live session counting** -- contributions disappear naturally when a node
  restarts or crashes.
- **Per-node metrics** -- see both the global total and each instance's slice.

Conditional instance-aware writes follow the same rule set:

- `inc_if` and `set_if` compare against the cumulative total.
- `set_on_instance_if` compares against the calling instance's slice.
- Failed comparisons return the current `(cumulative, instance_count)` unchanged.

### StrictInstanceAwareCounter

Every call is immediately consistent with Redis. `set` and `del` bump a
per-key **epoch** that causes stale instances to reset their stored count on
their next operation, preventing double-counting.

```rust
use distkit::icounter::{
    InstanceAwareCounterTrait,
    StrictInstanceAwareCounter, StrictInstanceAwareCounterOptions,
};
use distkit::DistkitRedisKey;

let client = redis::Client::open("redis://127.0.0.1/")?;
let conn = client.get_connection_manager().await?;
let prefix = DistkitRedisKey::try_from("my_app".to_string())?;
let counter = StrictInstanceAwareCounter::new(
    StrictInstanceAwareCounterOptions::new(prefix, conn),
);

let key = DistkitRedisKey::try_from("connections".to_string())?;

// Increment this instance's contribution; returns (cumulative, instance_count).
let (total, mine) = counter.inc(&key, 5).await?;

// Decrement this instance's contribution.
let (total, mine) = counter.dec(&key, 2).await?;

// Read without modifying.
let (total, mine) = counter.get(&key).await?;

// Set this instance's slice to an exact value without bumping the epoch.
let (total, mine) = counter.set_on_instance(&key, 10).await?;

// Set the global total to an exact value and bump the epoch.
let (total, mine) = counter.set(&key, 100).await?;

// Remove only this instance's contribution.
let (total, removed) = counter.del_on_instance(&key).await?;

// Delete the key globally and bump the epoch.
let (old_total, _) = counter.del(&key).await?;
```

#### Dead-instance cleanup

Each instance sends a heartbeat on every operation. If a process silently dies,
surviving instances automatically remove its contribution the next time any of
them touches the same key.

```rust
use distkit::icounter::{
    InstanceAwareCounterTrait,
    StrictInstanceAwareCounter, StrictInstanceAwareCounterOptions,
};
use distkit::DistkitRedisKey;

let client = redis::Client::open("redis://127.0.0.1/")?;
let conn1 = client.get_connection_manager().await?;
let conn2 = client.get_connection_manager().await?;
let prefix = DistkitRedisKey::try_from("my_app".to_string())?;
let key = DistkitRedisKey::try_from("connections".to_string())?;

let opts = |conn| StrictInstanceAwareCounterOptions {
    prefix: prefix.clone(),
    connection_manager: conn,
    dead_instance_threshold_ms: 30_000, // 30 s
};
let server_a = StrictInstanceAwareCounter::new(opts(conn1));
let server_b = StrictInstanceAwareCounter::new(opts(conn2));

server_a.inc(&key, 10).await?; // cumulative = 10
server_b.inc(&key,  5).await?; // cumulative = 15

// server_a goes offline. After 30 s, server_b's next call removes its
// contribution automatically.
let (total, _) = server_b.get(&key).await?; // total = 5 once cleaned up
```

### LaxInstanceAwareCounter

A buffered wrapper around `StrictInstanceAwareCounter`. `inc` calls accumulate
locally and are flushed to the strict counter in bulk every `flush_interval`
(default 20 ms). Global operations (`set`, `del`, `clear`) flush any pending
delta first, then delegate immediately.

Use this when you have many `inc`/`dec` calls per second and can tolerate a
small consistency lag.

```rust
use distkit::icounter::{
    InstanceAwareCounterTrait,
    LaxInstanceAwareCounter, LaxInstanceAwareCounterOptions,
};
use distkit::DistkitRedisKey;
use std::time::Duration;

let client = redis::Client::open("redis://127.0.0.1/")?;
let conn = client.get_connection_manager().await?;
let prefix = DistkitRedisKey::try_from("my_app".to_string())?;
let counter = LaxInstanceAwareCounter::new(LaxInstanceAwareCounterOptions {
    prefix,
    connection_manager: conn,
    dead_instance_threshold_ms: 30_000,
    flush_interval: Duration::from_millis(20),
    allowed_lag:    Duration::from_millis(20),
});

let key = DistkitRedisKey::try_from("connections".to_string())?;

// Returns the local estimate immediately — no Redis round-trip on warm path.
let (local_total, mine) = counter.inc(&key, 1).await?;

// Decrement also stays local until flushed.
let (local_total, mine) = counter.dec(&key, 1).await?;

// get() also returns the local estimate (cumulative + pending delta).
let (total, mine) = counter.get(&key).await?;
```

## Distributed locks

Enable the `lock` feature for Redis-backed `Mutex` and `RwLock`:

```toml
[dependencies]
distkit = { version = "0.7", features = ["lock"] }
```

Both mirror the surface of `tokio::sync::Mutex` / `tokio::sync::RwLock`. The
guards hold no inner data — they are pure access tokens. A held lock renews its
lease in the background (every `ttl/3`) and releases on drop, with an explicit
awaitable `release()` for callers who want to observe the final state. Each guard
also reports `get_on_attempt` — the zero-based acquire poll that won the lock
(`0` on the first try, higher under contention). `RwLock` is writer-preferring (a
waiting writer blocks new readers).

```rust,no_run
use distkit::{DistkitRedisKey, lock::{Mutex, RwLock, LockOptions}};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = redis::Client::open("redis://127.0.0.1/")?;
    let conn = client.get_connection_manager().await?;

    // Mutex (mutual exclusion).
    let key = DistkitRedisKey::try_from("invoice_42".to_string())?;
    let mutex = Mutex::new(LockOptions::new(key, conn.clone()));
    let guard = mutex.lock().await?;            // waits until acquired
    let _attempts = guard.get_on_attempt().await; // 0 on first poll, higher under contention
    // ... critical section ...
    guard.release().await?;

    // RwLock (reader-writer): many readers OR one writer.
    let key = DistkitRedisKey::try_from("config_blob".to_string())?;
    let rw = RwLock::new(LockOptions::new(key, conn));
    let r = rw.read().await?;                   // shared
    r.release().await?;
    let w = rw.write().await?;                  // exclusive
    w.release().await?;

    Ok(())
}
```

Acquire forms per lock: waiting (`lock` / `read` / `write`), non-blocking
(`try_lock` / `try_read` / `try_write`), time-bounded
(`try_lock_with_timeout` / `try_read_with_timeout` / `try_write_with_timeout`), and
retry-bounded (`try_lock_with_retries` / `try_read_with_retries` /
`try_write_with_retries`). The bounded forms poll at the lock's configured
`retry_interval`; the retry-bounded forms give up with `LockError::RetriesExhausted`
after `max_retries` retries. (The older `try_*_for(timeout, retry_interval)` forms
are deprecated.) Tune `ttl`, `max_wait`, `retry_interval`, `owner_id`, and
`namespace` via `LockOptions` or `LockOptions::builder`.

## Rate limiting (trypema)

Enable the `trypema` feature to access sliding-window rate limiting.

Trypema documentation website: <https://trypema.davidoyinbo.com>

```toml
[dependencies]
distkit = { version = "0.7", features = ["trypema"] }
```

All public types from [`trypema` 2](https://docs.rs/trypema) are re-exported
under `distkit::trypema`. Trypema 2 constructs each provider independently:

- **Sliding-window rate limiting** with configurable window size and rate.
- **Three providers** -- local (in-process), Redis-backed (distributed), and
  hybrid (local fast-path with periodic Redis sync).
- **Two strategies** -- absolute (binary allow/reject) and suppressed
  (probabilistic degradation that smoothly ramps rejection probability).

### Local rate limiting

```rust
use distkit::trypema::{
    BucketSize, RateLimit, RateLimitDecision, RateLimiterBuilder, WindowSize,
    local::LocalRateLimiterProvider,
};

let provider = LocalRateLimiterProvider::builder()
    .window_size(WindowSize::minutes_or_panic(1))
    .bucket_size(BucketSize::milliseconds_or_panic(100))
    .build()
    .unwrap();
let rate = RateLimit::per_second_or_panic(10.0);

match provider.absolute().inc("user_123", &rate, 1) {
    RateLimitDecision::Allowed => { /* process request */ }
    RateLimitDecision::Rejected { retry_after, .. } => {
        eprintln!("Rate limited, retry in {retry_after:?}");
    }
    RateLimitDecision::Suppressed { .. } => unreachable!(),
}
```

`build()` returns an `Arc` and starts stale-state cleanup. Use
`.disable_cleanup()` while building to opt out, or the provider's idempotent
`start_cleanup_loop()` and `stop_cleanup_loop()` methods after construction.

### Redis-backed and hybrid rate limiting

For distributed enforcement across multiple processes or servers, construct a
Redis or hybrid provider with a connection manager. Redis-backed providers
require Redis 7.2 or newer.

```rust
use distkit::trypema::{
    BucketSize, RateLimit, RateLimiterBuilder, WindowSize,
    hybrid::{HybridRateLimiterProvider, SyncInterval},
    redis::{RedisKey, RedisRateLimiterProvider},
};

let client = redis::Client::open("redis://127.0.0.1/")?;
let conn = client.get_connection_manager().await?;
let window = WindowSize::minutes(1)?;
let bucket = BucketSize::milliseconds(100)?;

let redis_provider = RedisRateLimiterProvider::builder(conn.clone())
    .window_size(window)
    .bucket_size(bucket)
    .build()?;
let hybrid_provider = HybridRateLimiterProvider::builder(conn)
    .window_size(window)
    .bucket_size(bucket)
    .sync_interval(SyncInterval::milliseconds(10)?)
    .build()?;

let key = RedisKey::try_from("user_123")?;
let rate = RateLimit::per_second(50.0)?;

// Distributed absolute enforcement
let decision = redis_provider.absolute().inc(&key, &rate, 1).await?;

// Local fast-path with periodic Redis synchronization
let decision = hybrid_provider.absolute().inc(&key, &rate, 1).await?;
```

Trypema 2 removed `RateLimiter`, `RateLimiterOptions`, and the provider option
structs. It also renamed `WindowSizeSeconds` to `WindowSize`, `RateGroupSizeMs`
to `BucketSize`, `SuppressionFactorCacheMs` to
`SuppressionFactorCachePeriod`, and `SyncIntervalMs` to `SyncInterval`.

See the [trypema documentation](https://docs.rs/trypema) for full API details
and advanced configuration.

## Development

### Prerequisites

- Rust (latest stable)
- Docker (for the test Redis instance)

### Commands

```sh
make test       # Start Redis, run tests, tear down
make bench      # Start Redis, run criterion benchmarks, tear down
make redis-up   # Start Redis on port 16379
make redis-down # Stop Redis and remove volumes
```

Tests and benchmarks require the `REDIS_URL` environment variable.
The `make` targets set this automatically.

## License

MIT
