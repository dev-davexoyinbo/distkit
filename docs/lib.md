A toolkit of distributed systems primitives for Rust, backed by Redis.

`distkit` provides battle-tested building blocks for distributed applications.
At its core are two counter implementations that let you choose between
immediate consistency and high-throughput eventual consistency.

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

| | [`StrictCounter`] | [`LaxCounter`] |
|---|---|---|
| **Consistency** | Immediate | Eventual (~20 ms lag) |
| **Latency** | Redis round-trip per call | Sub-microsecond (warm path) |
| **Redis I/O** | Every operation | Batched on interval |
| **Use case** | Billing, inventory, anything where accuracy is critical | Analytics, rate limiting, high-throughput metrics |

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
- **`CounterError`** -- Reserved for future counter-specific error variants.
- **`MutexPoisoned`** -- An internal lock was poisoned. This indicates a
  previous panic inside a critical section.
- **`CustomError`** -- Catch-all for internal errors (e.g., batch flush failures
  in [`LaxCounter`]).

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

[`StrictCounter`]: counter::StrictCounter
[`LaxCounter`]: counter::LaxCounter
[`CounterTrait`]: counter::CounterTrait
[`CounterOptions`]: counter::CounterOptions
[`Counter`]: counter::Counter
