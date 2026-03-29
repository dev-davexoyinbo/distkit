# distkit

A toolkit of distributed systems primitives for Rust, backed by Redis.

[![Crates.io](https://img.shields.io/crates/v/distkit.svg)](https://crates.io/crates/distkit)
[![Documentation](https://docs.rs/distkit/badge.svg)](https://docs.rs/distkit)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

## What is distkit?

distkit provides building blocks for distributed applications. It ships
distributed counters (strict and lax) and rate limiting, all backed by Redis.

## Features

- **StrictCounter** -- every operation executes a Redis Lua script atomically.
  Reads always reflect the latest write. Best for billing, inventory, or
  anything where accuracy is critical.
- **LaxCounter** -- buffers increments in memory and flushes to Redis every
  ~20 ms. Sub-microsecond latency on the hot path. Best for analytics and
  high-throughput metrics.
- **Shared trait** -- both counters implement `CounterTrait`, so generic code
  works with either.
- **Rate limiting** (opt-in `trypema` feature) -- sliding-window rate limiting
  with local, Redis-backed, and hybrid providers. Supports absolute and
  probabilistic suppression strategies.
- **Safe by default** -- `#![forbid(unsafe_code)]`, no panics in library code.

## Feature flags

| Feature | Default | Description |
|---|---|---|
| `counter` | **yes** | Distributed counters (`StrictCounter`, `LaxCounter`) |
| `trypema` | no | Rate limiting via the [trypema](http://trypema.davidoyinbo.com/) crate |

## Installation

```sh
cargo add distkit
```

Or add to `Cargo.toml`:

```toml
[dependencies]
distkit = "0.1"
```

To enable rate limiting:

```toml
[dependencies]
distkit = { version = "0.1", features = ["trypema"] }
```

distkit requires a running Redis instance (5.0+ for Lua script support).

## Quick start

```rust
use distkit::{RedisKey, counter::{Counter, CounterOptions, CounterTrait}};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect to Redis
    let client = redis::Client::open("redis://127.0.0.1/")?;
    let conn = client.get_connection_manager().await?;

    // Create a counter with a namespace prefix
    let prefix = RedisKey::try_from("my_app".to_string())?;
    let options = CounterOptions::new(prefix, conn);
    let counter = Counter::new(options);

    let key = RedisKey::try_from("page_views".to_string())?;

    // Strict: immediate consistency
    counter.strict().inc(&key, 1).await?;
    let total = counter.strict().get(&key).await?;
    println!("strict: {total}");

    // Lax: eventual consistency, much faster
    counter.lax().inc(&key, 1).await?;
    let approx = counter.lax().get(&key).await?;
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
counter.strict().inc(&key, 1).await?;   // HINCRBY via Lua
counter.strict().set(&key, 100).await?; // HSET via Lua
counter.strict().del(&key).await?;      // HDEL, returns old value
counter.strict().clear().await?;        // DEL on the hash
```

### LaxCounter

Writes are buffered in a local `DashMap` and flushed to Redis in batched
pipelines every `allowed_lag` (default 20 ms). Reads return the local view
(`remote_total + pending_delta`), which is always consistent within the same
process.

```rust
let key = RedisKey::try_from("impressions".to_string())?;
counter.lax().inc(&key, 1).await?;  // local atomic add, sub-microsecond
let val = counter.lax().get(&key).await?; // reads local state, no Redis hit
```

A background Tokio task handles flushing. It holds a `Weak` reference to the
counter, so it stops automatically when the counter is dropped.

### Rate limiting (trypema)

Enable the `trypema` feature to access sliding-window rate limiting with three
provider options (local, Redis-backed, hybrid) and two strategies (absolute,
suppressed). All types from the [trypema](http://trypema.davidoyinbo.com/)
crate are re-exported under `distkit::trypema`.

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

let rate = RateLimit::try_from(10.0).unwrap(); // 10 req/s

match rl.local().absolute().inc("user_123", &rate, 1) {
    RateLimitDecision::Allowed => { /* process request */ }
    RateLimitDecision::Rejected { retry_after_ms, .. } => {
        eprintln!("Rate limited, retry in {retry_after_ms} ms");
    }
    _ => {}
}
```

For distributed enforcement, enable the Redis provider and use
`rl.redis()` or `rl.hybrid()`. See the
[trypema documentation](http://trypema.davidoyinbo.com/) for full details.

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
