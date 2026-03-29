A toolkit of distributed systems primitives for Rust, backed by Redis.

`distkit` provides building blocks for distributed applications. The crate
currently offers two modules:

- **Counters** (`counter` feature, enabled by default) -- two counter
  implementations that share a common async trait, letting you choose between
  immediate consistency and high-throughput eventual consistency.
- **Rate limiting** (`trypema` feature, opt-in) -- re-exports the
  [`trypema`](https://docs.rs/trypema) crate, providing sliding-window rate
  limiting with local, Redis-backed, and hybrid providers.

# Feature flags

| Feature   | Default | Description                                                      |
| --------- | ------- | ---------------------------------------------------------------- |
| `counter` | **yes** | Distributed counters ([`StrictCounter`], [`LaxCounter`])         |
| `trypema` | no      | Rate limiting via the [`trypema`](https://docs.rs/trypema) crate |

# Quick start

```rust
# use distkit::{RedisKey, counter::{Counter, CounterOptions, CounterTrait}};
# async fn example() -> Result<(), Box<dyn std::error::Error>> {
# let client = redis::Client::open("redis://127.0.0.1/")?;
# let conn = client.get_connection_manager().await?;
let prefix = RedisKey::try_from("my_app".to_string())?;
let options = CounterOptions::new(prefix, conn);
let counter = Counter::new(options);

// Strict: every call hits Redis immediately
let key = RedisKey::try_from("page_views".to_string())?;
counter.strict().inc(&key, 1).await?;
let total = counter.strict().get(&key).await?;

// Lax: buffered in memory, flushed to Redis every ~20 ms
counter.lax().inc(&key, 1).await?;
let approx = counter.lax().get(&key).await?;
# Ok(())
# }
```

# Choosing a counter

|                 | [`StrictCounter`]                                       | [`LaxCounter`]                                    |
| --------------- | ------------------------------------------------------- | ------------------------------------------------- |
| **Consistency** | Immediate                                               | Eventual (~20 ms lag)                             |
| **Latency**     | Redis round-trip per call                               | Sub-microsecond (warm path)                       |
| **Redis I/O**   | Every operation                                         | Batched on interval                               |
| **Use case**    | Billing, inventory, anything where accuracy is critical | Analytics, rate limiting, high-throughput metrics |

Both types implement [`CounterTrait`], so you can write generic code that
works with either:

```rust
# use distkit::{DistkitError, RedisKey, counter::CounterTrait};
async fn bump<C: CounterTrait>(counter: &C, key: &RedisKey) -> Result<i64, DistkitError> {
    counter.inc(key, 1).await
}
```

# Key types

- [`RedisKey`] -- A validated key string (non-empty, 255 chars max, no colons).
  Constructed via `TryFrom<String>`.
- [`CounterOptions`] -- Configuration bundle for counter construction. Carries
  a prefix, Redis connection, and the `allowed_lag` duration (default 20 ms).
- [`Counter`] -- Facade that holds both a [`StrictCounter`] and a
  [`LaxCounter`], created from a single [`CounterOptions`].
- [`CounterTrait`] -- The async trait that both counter types implement:
  `inc`, `dec`, `get`, `set`, `del`, `clear`.

# Error handling

All fallible operations return [`DistkitError`]:

- **`InvalidRedisKey`** -- Returned by `RedisKey::try_from` when the input is
  empty, longer than 255 characters, or contains a colon.
- **`RedisError`** -- A Redis operation failed (connection lost, script error,
  etc.). Wraps [`redis::RedisError`].
- **`CounterError`** -- A counter operation failed (e.g., a batch flush could
  not commit state to Redis).
- **`MutexPoisoned`** -- An internal lock was poisoned. This indicates a
  previous panic inside a critical section.
- **`CustomError`** -- Catch-all for internal errors.
- **`TrypemaError`** -- A rate-limiting operation failed (only present with the
  `trypema` feature).

# Redis storage model

Counters use Redis hashes for storage. The hash key is derived from the
prefix and counter type; each counter member is a field within that hash.

```text
HSET  {prefix}:strict_counter  page_views  42
HSET  {prefix}:lax_counter     page_views  40
```

[`StrictCounter`] operates directly on the hash via Lua scripts.
[`LaxCounter`] maintains a local in-memory buffer and periodically flushes
deltas to the same hash structure using `HINCRBY` in batched pipelines.

Both counter types sharing the same prefix occupy **separate** hash keys
(different type suffixes), so they never interfere with each other.

# Rate limiting (trypema)

Enable the `trypema` feature to access distributed rate limiting.

Trypema documentation website: <https://trypema.davidoyinbo.com>

```toml
[dependencies]
distkit = { version = "0.1", features = ["trypema"] }
```

All public types from the [`trypema`](https://docs.rs/trypema) crate are
re-exported under `distkit::trypema`. The module provides:

- **Sliding-window rate limiting** with configurable window size and rate.
- **Three providers** -- local (in-process via `DashMap`), Redis-backed
  (distributed enforcement via Lua scripts), and hybrid (local fast-path
  with periodic Redis sync).
- **Two strategies** -- absolute (binary allow/reject) and suppressed
  (probabilistic degradation that smoothly ramps rejection probability).

## Local rate limiting (absolute)

Use the local provider for in-process rate limiting with sub-microsecond
latency. The absolute strategy gives a deterministic allow/reject decision.

```rust,no_run
# use std::sync::Arc;
# use distkit::trypema::{
#     HardLimitFactor, RateGroupSizeMs, RateLimit, RateLimitDecision,
#     RateLimiter, RateLimiterOptions, SuppressionFactorCacheMs, WindowSizeSeconds,
#     local::LocalRateLimiterOptions,
# };
# fn example() {
let rl = Arc::new(RateLimiter::new(RateLimiterOptions {
    local: LocalRateLimiterOptions {
        window_size_seconds: WindowSizeSeconds::try_from(60).unwrap(),
        rate_group_size_ms: RateGroupSizeMs::try_from(100).unwrap(),
        hard_limit_factor: HardLimitFactor::default(),
        suppression_factor_cache_ms: SuppressionFactorCacheMs::default(),
    },
}));

// Optional: start background cleanup for stale keys
rl.run_cleanup_loop();

let rate = RateLimit::try_from(10.0).unwrap(); // 10 requests per second

match rl.local().absolute().inc("user_123", &rate, 1) {
    RateLimitDecision::Allowed => {
        // Process the request
    }
    RateLimitDecision::Rejected { retry_after_ms, .. } => {
        // Back off and retry later
        eprintln!("Rate limited, retry in {retry_after_ms} ms");
    }
    _ => {}
}
# }
```

## Local rate limiting (suppressed)

The suppressed strategy smoothly ramps rejection probability as load
approaches the limit, instead of a hard cutoff.

```rust,no_run
# use std::sync::Arc;
# use distkit::trypema::{
#     HardLimitFactor, RateGroupSizeMs, RateLimit, RateLimitDecision,
#     RateLimiter, RateLimiterOptions, SuppressionFactorCacheMs, WindowSizeSeconds,
#     local::LocalRateLimiterOptions,
# };
# fn example() {
# let rl = Arc::new(RateLimiter::new(RateLimiterOptions {
#     local: LocalRateLimiterOptions {
#         window_size_seconds: WindowSizeSeconds::try_from(60).unwrap(),
#         rate_group_size_ms: RateGroupSizeMs::try_from(100).unwrap(),
#         hard_limit_factor: HardLimitFactor::try_from(1.5).unwrap(),
#         suppression_factor_cache_ms: SuppressionFactorCacheMs::default(),
#     },
# }));
let rate = RateLimit::try_from(100.0).unwrap();

match rl.local().suppressed().inc("api_endpoint", &rate, 1) {
    RateLimitDecision::Allowed => {
        // Well under the limit
    }
    RateLimitDecision::Suppressed { is_allowed, suppression_factor } => {
        if is_allowed {
            // Approaching the limit but still admitted
            tracing::info!("Suppression factor: {suppression_factor:.2}");
        } else {
            // Probabilistically rejected
        }
    }
    RateLimitDecision::Rejected { .. } => {
        // Over the hard limit (rate * hard_limit_factor)
    }
}
# }
```

## Redis-backed rate limiting

For distributed enforcement across multiple processes or servers, use the
Redis provider. Each call executes an atomic Lua script.

```rust,no_run
# use std::sync::Arc;
# use distkit::trypema::{
#     HardLimitFactor, RateGroupSizeMs, RateLimit, RateLimitDecision,
#     RateLimiter, RateLimiterOptions, SuppressionFactorCacheMs, WindowSizeSeconds,
#     local::LocalRateLimiterOptions,
#     redis::{RedisKey, RedisRateLimiterOptions},
#     hybrid::SyncIntervalMs,
# };
# async fn example() -> Result<(), Box<dyn std::error::Error>> {
# let client = redis::Client::open("redis://127.0.0.1/")?;
# let conn = client.get_connection_manager().await?;
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
# Ok(())
# }
```

See the [`trypema` documentation](https://docs.rs/trypema) for full API
details and advanced configuration.

[`StrictCounter`]: counter::StrictCounter
[`LaxCounter`]: counter::LaxCounter
[`CounterTrait`]: counter::CounterTrait
[`CounterOptions`]: counter::CounterOptions
[`Counter`]: counter::Counter
