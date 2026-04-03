# distkit

A toolkit of distributed systems primitives for Rust, backed by Redis.

[![Crates.io](https://img.shields.io/crates/v/distkit.svg)](https://crates.io/crates/distkit)
[![Documentation](https://docs.rs/distkit/badge.svg)](https://docs.rs/distkit)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

## What is distkit?

distkit provides building blocks for distributed applications. It ships
distributed counters (strict and lax), instance-aware counters, and rate
limiting, all backed by Redis.

## Features

- **StrictCounter** -- every operation executes a Redis Lua script atomically.
  Reads always reflect the latest write. Best for billing, inventory, or
  anything where accuracy is critical.
- **LaxCounter** -- buffers increments in memory and flushes to Redis every
  ~20 ms. Sub-microsecond latency on the hot path. Best for analytics and
  high-throughput metrics.
- **Instance-aware counters** -- each running process owns a named slice of the
  total, with automatic cleanup of contributions from processes that stop
  heartbeating.
- **Rate limiting** (opt-in `trypema` feature) -- sliding-window rate limiting
  with local, Redis-backed, and hybrid providers. Supports absolute and
  probabilistic suppression strategies.
- **Safe by default** -- `#![forbid(unsafe_code)]`, no panics in library code.

## Feature flags

| Feature                  | Default | Description                                                              |
| ------------------------ | ------- | ------------------------------------------------------------------------ |
| `counter`                | **yes** | Distributed counters (`StrictCounter`, `LaxCounter`)                     |
| `instance-aware-counter` | no      | Per-instance counters (`StrictInstanceAwareCounter`, `LaxInstanceAwareCounter`) |
| `trypema`                | no      | Rate limiting via the [trypema](https://docs.rs/trypema) crate           |

## Installation

```sh
cargo add distkit
```

Or add to `Cargo.toml`:

```toml
[dependencies]
distkit = "0.1"
```

To enable instance-aware counters or rate limiting:

```toml
[dependencies]
distkit = { version = "0.1", features = ["instance-aware-counter", "trypema"] }
```

distkit requires a running Redis instance (5.0+ for Lua script support).

## Quick start

```rust
use distkit::{RedisKey, counter::{StrictCounter, LaxCounter, CounterOptions, CounterTrait}};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = redis::Client::open("redis://127.0.0.1/")?;
    let conn = client.get_connection_manager().await?;

    let prefix = RedisKey::try_from("my_app".to_string())?;
    let options = CounterOptions::new(prefix, conn);

    let key = RedisKey::try_from("page_views".to_string())?;

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
let key = RedisKey::try_from("orders".to_string())?;
strict.inc(&key, 1).await?;   // HINCRBY via Lua
strict.set(&key, 100).await?; // HSET via Lua
strict.del(&key).await?;      // HDEL, returns old value
strict.clear().await?;        // DEL on the hash
```

### LaxCounter

Writes are buffered in a local `DashMap` and flushed to Redis in batched
pipelines every `allowed_lag` (default 20 ms). Reads return the local view
(`remote_total + pending_delta`), which is always consistent within the same
process.

```rust
let key = RedisKey::try_from("impressions".to_string())?;
lax.inc(&key, 1).await?;         // local atomic add, sub-microsecond
let val = lax.get(&key).await?;  // reads local state, no Redis hit
```

A background Tokio task handles flushing. It holds a `Weak` reference to the
counter, so it stops automatically when the counter is dropped.

### Choosing a counter

|                           | `StrictCounter`                        | `LaxCounter`                       | `StrictInstanceAwareCounter`                | `LaxInstanceAwareCounter`                      |
| ------------------------- | -------------------------------------- | ---------------------------------- | ------------------------------------------- | ---------------------------------------------- |
| **Consistency**           | Immediate                              | Eventual (~20 ms lag)              | Immediate                                   | Eventual (`flush_interval` lag)                |
| **`inc` latency**         | Redis round-trip                       | Sub-microsecond (warm path)        | Redis round-trip                            | Sub-microsecond (warm path)                    |
| **Redis I/O**             | Every operation                        | Batched on interval                | Every `inc`                                 | Batched on interval via `inc_batch`            |
| **`set` / `del`**         | Immediate                              | Immediate                          | Immediate (bumps epoch)                     | Flushes pending delta, then immediate          |
| **Per-instance tracking** | No                                     | No                                 | Yes                                         | Yes                                            |
| **Dead-instance cleanup** | No                                     | No                                 | Yes                                         | Yes                                            |
| **Feature flag**          | `counter` (default)                    | `counter` (default)                | `instance-aware-counter`                    | `instance-aware-counter`                       |
| **Use case**              | Billing, inventory, exact global count | Analytics, high-throughput metrics | Connection counts, exact live metrics       | High-frequency per-node throughput metrics     |

## Instance-aware counters

Enable the `instance-aware-counter` feature:

```toml
[dependencies]
distkit = { version = "0.1", features = ["instance-aware-counter"] }
```

Instance-aware counters track each running process's contribution separately.
The cumulative total is the sum of all **live** instances. When a process stops
heartbeating for longer than `dead_instance_threshold_ms` (default 30 s), its
contribution is automatically subtracted from the cumulative on the next
operation by any surviving instance.

This makes them well-suited for:

- **Connection pool sizing** -- each server reports its active connection count;
  the cumulative is the cluster-wide total.
- **Live session counting** -- contributions disappear naturally when a node
  restarts or crashes.
- **Per-node metrics** -- see both the global total and each instance's slice.

### StrictInstanceAwareCounter

Every call is immediately consistent with Redis. `set` and `del` bump a
per-key **epoch** that causes stale instances to reset their stored count on
their next operation, preventing double-counting.

```rust
use distkit::icounter::{
    InstanceAwareCounterTrait,
    StrictInstanceAwareCounter, StrictInstanceAwareCounterOptions,
};
use distkit::RedisKey;

let client = redis::Client::open("redis://127.0.0.1/")?;
let conn = client.get_connection_manager().await?;
let prefix = RedisKey::try_from("my_app".to_string())?;
let counter = StrictInstanceAwareCounter::new(
    StrictInstanceAwareCounterOptions::new(prefix, conn),
);

let key = RedisKey::try_from("connections".to_string())?;

// Increment this instance's contribution; returns (cumulative, instance_count).
let (total, mine) = counter.inc(&key, 5).await?;

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
use distkit::RedisKey;

let client = redis::Client::open("redis://127.0.0.1/")?;
let conn1 = client.get_connection_manager().await?;
let conn2 = client.get_connection_manager().await?;
let prefix = RedisKey::try_from("my_app".to_string())?;
let key = RedisKey::try_from("connections".to_string())?;

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
use distkit::RedisKey;
use std::time::Duration;

let client = redis::Client::open("redis://127.0.0.1/")?;
let conn = client.get_connection_manager().await?;
let prefix = RedisKey::try_from("my_app".to_string())?;
let counter = LaxInstanceAwareCounter::new(LaxInstanceAwareCounterOptions {
    prefix,
    connection_manager: conn,
    dead_instance_threshold_ms: 30_000,
    flush_interval: Duration::from_millis(20),
    allowed_lag:    Duration::from_millis(20),
});

let key = RedisKey::try_from("connections".to_string())?;

// Returns the local estimate immediately — no Redis round-trip on warm path.
let (local_total, mine) = counter.inc(&key, 1).await?;

// get() also returns the local estimate (cumulative + pending delta).
let (total, mine) = counter.get(&key).await?;
```

## Rate limiting (trypema)

Enable the `trypema` feature to access sliding-window rate limiting.

Trypema documentation website: <https://trypema.davidoyinbo.com>

```toml
[dependencies]
distkit = { version = "0.1", features = ["trypema"] }
```

All public types from the [`trypema`](https://docs.rs/trypema) crate are
re-exported under `distkit::trypema`. The module provides:

- **Sliding-window rate limiting** with configurable window size and rate.
- **Three providers** -- local (in-process), Redis-backed (distributed), and
  hybrid (local fast-path with periodic Redis sync).
- **Two strategies** -- absolute (binary allow/reject) and suppressed
  (probabilistic degradation that smoothly ramps rejection probability).

### Local rate limiting (absolute)

```rust
use std::sync::Arc;
use distkit::trypema::{
    HardLimitFactor, RateGroupSizeMs, RateLimit, RateLimitDecision,
    RateLimiter, RateLimiterOptions, SuppressionFactorCacheMs, WindowSizeSeconds,
    local::LocalRateLimiterOptions,
};

let rl = Arc::new(RateLimiter::new(RateLimiterOptions {
    local: LocalRateLimiterOptions {
        window_size_seconds: WindowSizeSeconds::try_from(60).unwrap(),
        rate_group_size_ms: RateGroupSizeMs::try_from(100).unwrap(),
        hard_limit_factor: HardLimitFactor::default(),
        suppression_factor_cache_ms: SuppressionFactorCacheMs::default(),
    },
}));

rl.run_cleanup_loop();

let rate = RateLimit::try_from(10.0).unwrap(); // 10 requests per second

match rl.local().absolute().inc("user_123", &rate, 1) {
    RateLimitDecision::Allowed => { /* process request */ }
    RateLimitDecision::Rejected { retry_after_ms, .. } => {
        eprintln!("Rate limited, retry in {retry_after_ms} ms");
    }
    _ => {}
}
```

### Redis-backed rate limiting

For distributed enforcement across multiple processes or servers:

```rust
use std::sync::Arc;
use distkit::trypema::{
    HardLimitFactor, RateGroupSizeMs, RateLimit, RateLimitDecision,
    RateLimiter, RateLimiterOptions, SuppressionFactorCacheMs, WindowSizeSeconds,
    local::LocalRateLimiterOptions,
    redis::{RedisKey, RedisRateLimiterOptions},
    hybrid::SyncIntervalMs,
};

let client = redis::Client::open("redis://127.0.0.1/")?;
let conn = client.get_connection_manager().await?;
let window = WindowSizeSeconds::try_from(60)?;
let bucket = RateGroupSizeMs::try_from(100)?;

let rl = Arc::new(RateLimiter::new(RateLimiterOptions {
    local: LocalRateLimiterOptions {
        window_size_seconds: window,
        rate_group_size_ms: bucket,
        hard_limit_factor: HardLimitFactor::default(),
        suppression_factor_cache_ms: SuppressionFactorCacheMs::default(),
    },
    redis: RedisRateLimiterOptions {
        connection_manager: conn,
        prefix: None,
        window_size_seconds: window,
        rate_group_size_ms: bucket,
        hard_limit_factor: HardLimitFactor::default(),
        suppression_factor_cache_ms: SuppressionFactorCacheMs::default(),
        sync_interval_ms: SyncIntervalMs::default(),
    },
}));

rl.run_cleanup_loop();

let key = RedisKey::try_from("user_123".to_string())?;
let rate = RateLimit::try_from(50.0)?;

// Distributed absolute enforcement
let decision = rl.redis().absolute().inc(&key, &rate, 1).await?;

// Or use the hybrid provider for local fast-path with Redis sync
let decision = rl.hybrid().absolute().inc(&key, &rate, 1).await?;
```

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
