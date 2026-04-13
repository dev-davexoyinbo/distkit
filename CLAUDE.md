# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
make test          # Start Redis, run full test suite, tear down
make bench         # Start Redis, run criterion benchmarks, tear down
make redis-up      # Start Redis only (docker-compose, port REDIS_PORT=16379)
make redis-down    # Stop Redis and remove volumes
```

To run tests manually (Redis must be running):
```bash
REDIS_URL="redis://127.0.0.1:16379/" cargo test --all-features
REDIS_URL="redis://127.0.0.1:16379/" cargo test --all-features <test_name>  # single test
cargo test --doc --all-features                                              # doctests only
```

**Always use `make test`, not bare `cargo test`** — tests require `REDIS_URL` to be set and a live Redis instance.

## Architecture

**distkit** is an async Rust library (Tokio + Redis) providing distributed counting primitives with two consistency modes and two counter families.

### Counter families

| Family | Trait | Strict impl | Lax impl |
|--------|-------|-------------|----------|
| Simple | `CounterTrait` | `StrictCounter` | `LaxCounter` |
| Instance-aware | `InstanceAwareCounterTrait` | `StrictInstanceAwareCounter` | `LaxInstanceAwareCounter` |

**Strict** counters: every operation is an atomic Lua script round-trip — fully consistent.

**Lax** counters: `inc`/`dec`/`get`/`set_on_instance` are served from an in-memory `DashMap`; a background Tokio task flushes accumulated deltas to Redis every `flush_interval` (default 20 ms). Epoch-bumping operations (`set`, `del`, `clear`) flush first, then delegate to the strict backend. The background task holds a `Weak` reference and stops when the counter is dropped.

### Instance-aware counters

Each counter instance gets a UUID. Operations return `(cumulative, instance_count)`. Redis stores:
- `cumulative_key` hash: key → global total
- `instance_count_key` hash: per-instance contributions
- `instances_key` sorted set: instance_id → last-heartbeat timestamp (ms)
- `epoch_key` hash: per-key epoch counter (bumped by `set`/`del` to invalidate stale slices)

Dead instances (no heartbeat for `dead_instance_threshold_ms`, default 30 s) are cleaned up by the next live instance that touches an affected key.

### Lua scripts

All Redis logic is embedded as inline Lua strings inside each `*_counter.rs` file — no external `.lua` files. `HELPERS_LUA` (in `strict_instance_aware_counter.rs`) defines shared helpers (`now_ms`, `delete_dead_instances`, `check_and_zadd`) that are prepended to every icounter script via string concatenation.

Scripts echo keys back in their return values so callers can build `HashMap<String, T>` results instead of relying on positional ordering. **Never use `.zip()` to align pipeline results with input keys** — use the HashMap keyed on the returned key string.

### `execute_pipeline_with_script_retry`

`src/common/mod.rs` exports this generic helper used by every batch pipeline operation:

```rust
execute_pipeline_with_script_retry(conn, script, items, |item| {
    let mut inv = script.key(...);
    inv.key(...).arg(...);
    inv  // return owned ScriptInvocation
})
```

On `NOSCRIPT` error it prepends `load_script` and retries the entire pipeline. Callers pass a closure returning one `ScriptInvocation<'s>` per item; the function owns all pipeline mechanics.

### `RedisKey`

Newtype wrapping `String`, validated on construction (`TryFrom<String>`): non-empty, ≤255 chars, no colons. Used as the public API key type throughout. `RedisKeyGenerator` prepends the counter-type prefix when building actual Redis keys.

### `ActivityTracker`

Drives the lax flush task's sleep/wake cycle. `signal()` sets `is_active = true` atomically and sends on a `watch` channel. The flush task parks at `is_active_watch.changed()` when idle; `run_is_active_task` sets `is_active = false` after `epoch_interval / 2` (7.5 s) of inactivity. The epoch advances every `EPOCH_CHANGE_INTERVAL` (15 s); `signal()` is a no-op within the same epoch to avoid redundant sends.

### Feature flags

- `counter` (default): `StrictCounter`, `LaxCounter`
- `instance-aware-counter` (default): `StrictInstanceAwareCounter`, `LaxInstanceAwareCounter`
- `trypema`: re-exports the `trypema` rate-limiting crate

### Module layout

```
src/
  lib.rs                        # feature-gated re-exports
  error.rs                      # DistkitError
  common/
    mod.rs                      # RedisKey, RedisKeyGenerator, execute_pipeline_with_script_retry,
                                #   ActivityTracker, EPOCH_CHANGE_INTERVAL
  counter/
    counter_trait.rs            # CounterTrait (async_trait)
    strict_counter.rs           # StrictCounter + embedded Lua
    lax_counter.rs              # LaxCounter + embedded Lua
    tests/                      # unit tests per impl
  icounter/
    mod.rs                      # InstanceAwareCounterTrait (async_trait)
    strict_instance_aware_counter.rs  # StrictInstanceAwareCounter + all Lua scripts
    lax_instance_aware_counter.rs     # LaxInstanceAwareCounter (wraps strict)
    tests/
  __doctest_helpers.rs          # Counter factory helpers for inline doc examples
```
