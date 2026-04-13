A toolkit of distributed systems primitives for Rust, backed by Redis.

`distkit` (DISTributed system KIT) provides building blocks for distributed applications. The crate
currently offers three modules and they all run on the tokio runtime:

- **Counters** (`counter` feature, enabled by default) -- two counter (strict and lax)
  implementations that share a common async trait, letting you choose between
  immediate consistency and high-throughput eventual consistency.
- **Instance-aware counters** (`instance-aware-counter` feature) -- counters
  where each running process owns a named slice of the total, with automatic
  cleanup of contributions from processes that stop heartbeating. For example if you have
  a cluster of servers where each server reports its active connection count, the cumulative
  is the cluster-wide total, when a server dies, the contribution is automatically subtracted.
  If the server was temporarily offline, the contribution is automatically added back when it comes back online.
- **Rate limiting** (`trypema` feature, opt-in) -- re-exports the
  [`trypema`](https://docs.rs/trypema) crate, providing sliding-window rate
  limiting with local, Redis-backed, and hybrid providers.

# Feature flags

| Feature                  | Default | Description                                                                                       |
| ------------------------ | ------- | ------------------------------------------------------------------------------------------------- |
| `counter`                | **yes** | Distributed counters ([`StrictCounter`], [`LaxCounter`])                                          |
| `instance-aware-counter` | no      | Instance aware distributed counters ([`StrictInstanceAwareCounter`], [`LaxInstanceAwareCounter`]) |
| `trypema`                | no      | Rate limiting via the [`trypema`](https://docs.rs/trypema) crate                                  |

# Quick start

```rust
# use distkit::{DistkitRedisKey, counter::{StrictCounter, LaxCounter, CounterOptions, CounterTrait}};
# async fn example() -> Result<(), Box<dyn std::error::Error>> {
# let client = redis::Client::open("redis://127.0.0.1/")?;
# let conn = client.get_connection_manager().await?;
// Servers sharing the same prefix coordinate through the same Redis keys.
let prefix = DistkitRedisKey::try_from("my_app".to_string())?;
let options = CounterOptions::new(prefix, conn);

// Strict: every call hits Redis immediately
let strict = StrictCounter::new(options.clone());
let key = DistkitRedisKey::try_from("page_views".to_string())?;
strict.inc(&key, 1).await?;
let total = strict.get(&key).await?;

// Lax: buffered in memory, flushed to Redis every ~20 ms
let lax = LaxCounter::new(options);
lax.inc(&key, 1).await?;
let approx = lax.get(&key).await?;
# Ok(())
# }
```

# Choosing a counter

|                           | [`StrictCounter`]                      | [`LaxCounter`]                     | [`StrictInstanceAwareCounter`]        | [`LaxInstanceAwareCounter`]                |
| ------------------------- | -------------------------------------- | ---------------------------------- | ------------------------------------- | ------------------------------------------ |
| **Consistency**           | Immediate                              | Eventual (default: ~20 ms lag)     | Immediate                             | Eventual (`flush_interval` lag)            |
| **`inc` latency**         | Redis round-trip                       | Sub-microsecond (warm path)        | Redis round-trip                      | Sub-microsecond (warm path)                |
| **Redis I/O**             | Every operation                        | Batched on interval                | Every `inc`                           | Batched on interval                        |
| **`set` / `del`**         | Immediate                              | Immediate                          | Immediate (bumps epoch)               | Flushes pending delta, then immediate      |
| **Per-instance tracking** | No                                     | No                                 | Yes                                   | Yes                                        |
| **Dead-instance cleanup** | No                                     | No                                 | Yes                                   | Yes                                        |
| **Feature flag**          | `counter` (default)                    | `counter` (default)                | `instance-aware-counter`              | `instance-aware-counter`                   |
| **Use case**              | Billing, inventory, exact global count | Analytics, high-throughput metrics | Connection counts, exact live metrics | High-frequency per-node throughput metrics |

[`StrictCounter`] and [`LaxCounter`] implement [`CounterTrait`]. Both instance-aware
types implement [`InstanceAwareCounterTrait`]. Write generic code against either trait:

```rust
# use distkit::{DistkitError, DistkitRedisKey, counter::CounterTrait};
// Example: bumping a counter by 1 (strict or lax)
async fn bump<C: CounterTrait>(counter: &C, key: &DistkitRedisKey) -> Result<i64, DistkitError> {
    counter.inc(key, 1).await
}
```

```rust,no_run
# use distkit::{DistkitError, DistkitRedisKey, icounter::InstanceAwareCounterTrait};
async fn report_connection<C: InstanceAwareCounterTrait>(
    counter: &C,
    key: &DistkitRedisKey,
    delta: i64,
) -> Result<i64, DistkitError> {
    let (total, _mine) = counter.inc(key, delta).await?;
    Ok(total)
}
```

# Key types

- [`DistkitRedisKey`] -- A validated key string (non-empty, 255 chars max, no
  colons), with helpers like `new`, `new_or_panic`, and `try_sanitize`.
- [`CounterComparator`] -- The comparison operator used by conditional writes:
  [`Eq`](crate::CounterComparator::Eq), [`Lt`](crate::CounterComparator::Lt),
  [`Gt`](crate::CounterComparator::Gt), [`Ne`](crate::CounterComparator::Ne),
  or [`Nil`](crate::CounterComparator::Nil).
- [`CounterOptions`] -- Configuration bundle for counter construction. Carries
  a prefix, Redis connection, and the `allowed_lag` duration (default 20 ms).
  Implements `Clone`, so the same options can be passed to both counter types.
- [`CounterTrait`] -- The async trait that both counter types implement:
  `inc`, `inc_if`, `dec`, `get`, `set`, `set_if`, `del`, `clear`, and
  multi-key helpers including `inc_all_if` and `set_all_if`.

# Conditional writes

Use [`CounterComparator`] with the `*_if` methods to apply a write only when
the current value matches a condition. Failed comparisons return the current
value unchanged.

```rust,no_run
# use distkit::{CounterComparator, DistkitRedisKey, counter::{CounterOptions, CounterTrait, StrictCounter}};
# async fn example() -> Result<(), Box<dyn std::error::Error>> {
# let client = redis::Client::open("redis://127.0.0.1/")?;
# let conn = client.get_connection_manager().await?;
# let prefix = DistkitRedisKey::try_from("my_app".to_string())?;
# let counter = StrictCounter::new(CounterOptions::new(prefix, conn));
let key = DistkitRedisKey::try_from("orders".to_string())?;
counter.set(&key, 10).await?;

assert_eq!(
    counter.inc_if(&key, CounterComparator::Eq(10), 5).await?,
    15
);
assert_eq!(
    counter.set_if(&key, CounterComparator::Gt(20), 99).await?,
    15
);
assert_eq!(
    counter
        .inc_all_if(&[
            (&key, CounterComparator::Eq(15), 2),
            (&key, CounterComparator::Nil, 3),
        ])
        .await?,
    vec![(&key, 17), (&key, 20)]
);
# Ok(())
# }
```

# Error handling

All fallible operations return [`DistkitError`]:

- **`InvalidRedisKey`** -- Returned by `DistkitRedisKey::try_from` when the input is
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

# Instance-aware counters

Enable the `instance-aware-counter` feature to access per-instance distributed
counters.

```toml
[dependencies]
distkit = { version = "0.2", features = ["instance-aware-counter"] }
```

Instance-aware counters track each running instance's contribution separately.
The cumulative total is the sum of all **live** instances. When a instance stops
heartbeating for longer than `dead_instance_threshold_ms` (default 30 s), its
contribution is automatically subtracted from the cumulative on the next
operation by any surviving instance.

This makes them well-suited for:

- **Connection pool sizing** -- each server reports its active connection count;
  the cumulative is the cluster-wide total.
- **Live session counting** -- contributions disappear naturally when a node
  restarts or crashes.
- **Per-node metrics** -- see both the global total and each instance's slice.

Conditional instance-aware writes follow the same pattern:

- `inc_if` and `set_if` compare against the cumulative total.
- `set_on_instance_if` and `set_all_on_instance_if` compare against this
  instance's current slice.

## StrictInstanceAwareCounter

Every call is immediately consistent with Redis. `set` and `del` bump a
per-key **epoch** that causes stale instances to reset their stored count on
their next operation, preventing double-counting.

```rust,no_run
# use distkit::icounter::{
#     InstanceAwareCounterTrait,
#     StrictInstanceAwareCounter, StrictInstanceAwareCounterOptions,
# };
# use distkit::DistkitRedisKey;
# async fn example() -> Result<(), Box<dyn std::error::Error>> {
# let client = redis::Client::open("redis://127.0.0.1/")?;
# let conn = client.get_connection_manager().await?;
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
// Other instances are not affected.
let (total, mine) = counter.set_on_instance(&key, 10).await?;

// Set the global total to an exact value and bump the epoch, making all
// other instances' stored counts stale.
let (total, mine) = counter.set(&key, 100).await?;

// Remove only this instance's contribution (no epoch bump).
let (total, removed) = counter.del_on_instance(&key).await?;

// Delete the key globally and bump the epoch.
let (old_total, _) = counter.del(&key).await?;
# Ok(())
# }
```

### Dead-instance cleanup

Each instance sends a heartbeat on every operation. If a process silently dies,
surviving instances automatically remove its contribution the next time any of
them touches the same key.

```rust,no_run
# use distkit::icounter::{
#     InstanceAwareCounterTrait,
#     StrictInstanceAwareCounter, StrictInstanceAwareCounterOptions,
# };
# use distkit::DistkitRedisKey;
# async fn example() -> Result<(), Box<dyn std::error::Error>> {
# let client = redis::Client::open("redis://127.0.0.1/")?;
# let conn1 = client.get_connection_manager().await?;
# let conn2 = client.get_connection_manager().await?;
let prefix = DistkitRedisKey::try_from("my_app".to_string())?;
let key = DistkitRedisKey::try_from("connections".to_string())?;

// Two independent instances sharing the same prefix.
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
# Ok(())
# }
```

## LaxInstanceAwareCounter

A buffered wrapper around [`StrictInstanceAwareCounter`]. `inc` calls
accumulate locally and are flushed to the strict counter in bulk every
`flush_interval` (default 20 ms). Global operations (`set`, `del`, `clear`)
flush any pending delta first, then delegate immediately.

Use this when you have many `inc`/`dec` calls per second and can tolerate a
small consistency lag.

```rust,no_run
# use distkit::icounter::{
#     InstanceAwareCounterTrait,
#     LaxInstanceAwareCounter, LaxInstanceAwareCounterOptions,
# };
# use distkit::DistkitRedisKey;
# use std::time::Duration;
# async fn example() -> Result<(), Box<dyn std::error::Error>> {
# let client = redis::Client::open("redis://127.0.0.1/")?;
# let conn = client.get_connection_manager().await?;
let prefix = DistkitRedisKey::try_from("my_app".to_string())?;

// options: LaxInstanceAwareCounterOptions::new(prefix, conn) would give the same result.
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
// A fresh instance with no local state falls back to the strict counter.
let (total, mine) = counter.get(&key).await?;
# Ok(())
# }
```

# Rate limiting (trypema)

Enable the `trypema` feature to access distributed rate limiting.

Trypema documentation website: <https://trypema.davidoyinbo.com>

```toml
[dependencies]
distkit = { version = "0.2", features = ["trypema"] }
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
#     redis::RedisRateLimiterOptions,
#     hybrid::SyncIntervalMs,
# };
# fn example() {
# let connection_manager: redis::aio::ConnectionManager = todo!();
let rl = Arc::new(RateLimiter::new(RateLimiterOptions {
    local: LocalRateLimiterOptions {
        window_size_seconds: WindowSizeSeconds::try_from(60).unwrap(),
        rate_group_size_ms: RateGroupSizeMs::try_from(100).unwrap(),
        hard_limit_factor: HardLimitFactor::default(),
        suppression_factor_cache_ms: SuppressionFactorCacheMs::default(),
    },
#        redis: RedisRateLimiterOptions {
#        connection_manager,
#        prefix: None,
#        window_size_seconds: WindowSizeSeconds::try_from(60).unwrap(),
#        rate_group_size_ms: RateGroupSizeMs::try_from(100).unwrap(),
#        hard_limit_factor: HardLimitFactor::default(),
#        suppression_factor_cache_ms: SuppressionFactorCacheMs::default(),
#        sync_interval_ms: SyncIntervalMs::default(),
#    },
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
#     redis::RedisRateLimiterOptions,
#     hybrid::SyncIntervalMs,
# };
# fn example() {
# let connection_manager: redis::aio::ConnectionManager = todo!();
# let rl = Arc::new(RateLimiter::new(RateLimiterOptions {
#     local: LocalRateLimiterOptions {
#         window_size_seconds: WindowSizeSeconds::try_from(60).unwrap(),
#         rate_group_size_ms: RateGroupSizeMs::try_from(100).unwrap(),
#         hard_limit_factor: HardLimitFactor::try_from(1.5).unwrap(),
#         suppression_factor_cache_ms: SuppressionFactorCacheMs::default(),
#     },
#     redis: RedisRateLimiterOptions {
#         connection_manager,
#         prefix: None,
#         window_size_seconds: WindowSizeSeconds::try_from(60).unwrap(),
#         rate_group_size_ms: RateGroupSizeMs::try_from(100).unwrap(),
#         hard_limit_factor: HardLimitFactor::try_from(1.5).unwrap(),
#         suppression_factor_cache_ms: SuppressionFactorCacheMs::default(),
#         sync_interval_ms: SyncIntervalMs::default(),
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
[`StrictInstanceAwareCounter`]: icounter::StrictInstanceAwareCounter
[`LaxInstanceAwareCounter`]: icounter::LaxInstanceAwareCounter
[`InstanceAwareCounterTrait`]: icounter::InstanceAwareCounterTrait
