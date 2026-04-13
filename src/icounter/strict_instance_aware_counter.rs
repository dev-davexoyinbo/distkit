//! Instance-aware distributed counter backed by Redis.
//!
//! Each [`InstanceAwareCounter`] represents one process/instance. Multiple
//! instances sharing the same Redis prefix each maintain their own count per
//! key, contributing to a shared cumulative total. When an instance stops
//! sending heartbeats, its contribution is automatically removed.

use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc,
        atomic::{AtomicI64, AtomicU64, Ordering},
    },
};

use dashmap::DashMap;
use redis::{Script, aio::ConnectionManager};

use crate::{
    ActivityTracker, CounterComparator, DistkitRedisKey, EPOCH_CHANGE_INTERVAL, RedisKeyGenerator,
    RedisKeyGeneratorTypeKey,
    error::DistkitError,
    execute_pipeline_with_script_retry,
    icounter::{InstanceAwareCounterTrait, generate_instance_id},
};

// ---------------------------------------------------------------------------
// Per-key in-memory state
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct SingleStore {
    /// Last-seen Redis epoch for this key.
    epoch: AtomicU64,
    /// Last-seen cumulative total for this key.
    cumulative: AtomicI64,
    /// This instance's contribution to the counter for this key.
    local_count: AtomicI64,
}

impl SingleStore {
    fn new(epoch: u64, cumulative: i64, local_count: i64) -> Self {
        Self {
            epoch: AtomicU64::new(epoch),
            cumulative: AtomicI64::new(cumulative),
            local_count: AtomicI64::new(local_count),
        }
    }
}

const MAX_BATCH_SIZE: usize = 100;

// ---------------------------------------------------------------------------
// Lua helpers — prepended to all scripts except `clear`
// ---------------------------------------------------------------------------

const HELPERS_LUA: &str = r#"
local function now_ms()
    local time_array = redis.call("TIME")
    return tonumber(time_array[1]) * 1000 + math.floor(tonumber(time_array[2]) / 1000)
end

local function delete_dead_instances(prefix, instances_key, cumulative_key, keys_key, dead_threshold_ms, timestamp_ms)
    local cutoff = timestamp_ms - dead_threshold_ms
    local to_remove = redis.call("ZRANGE", instances_key, "-inf", cutoff, "BYSCORE")

    for _, inst_id in ipairs(to_remove) do
        local inst_count_key = prefix .. ':count:' .. inst_id
        local all_keys = redis.call('SMEMBERS', keys_key)

        if #all_keys > 0 then
            local values = redis.call('HMGET', inst_count_key, unpack(all_keys))
            for i = 1, #values do
                local c = tonumber(values[i] or 0) or 0
                if c ~= 0 then
                    redis.call('HINCRBY', cumulative_key, all_keys[i], -c)
                end
            end
        end

        redis.call('DEL', inst_count_key)
        redis.call('ZREM', instances_key, inst_id)
    end
end

-- Returns 1 if the instance was not previously in the ZSET (newly created or
-- was cleaned up as dead), 0 if it was already live.
local function check_and_zadd(instances_key, instance_id, ts)
    local prev = redis.call('ZSCORE', instances_key, instance_id)
    local created = (prev == false or prev == nil) and 1 or 0
    redis.call('ZADD', instances_key, ts, instance_id)
    return created
end

local function compare_values(current, comparator, expected)
    if comparator == 'nil' then
        return true
    elseif comparator == 'eq' then
        return current == expected
    elseif comparator == 'lt' then
        return current < expected
    elseif comparator == 'gt' then
        return current > expected
    elseif comparator == 'ne' then
        return current ~= expected
    end

    return false
end
"#;

// ---------------------------------------------------------------------------
// Lua script bodies
// ---------------------------------------------------------------------------

const INC_LUA: &str = r#"
local epoch_key      = KEYS[1]
local instances_key  = KEYS[2]
local cumulative_key = KEYS[3]
local keys_key       = KEYS[4]
local inst_count_key = KEYS[5]

local counter_key    = ARGV[1]
local comparator     = ARGV[2]
local compare_against = tonumber(ARGV[3])
local delta          = tonumber(ARGV[4])
local local_epoch    = tonumber(ARGV[5])
local local_count    = tonumber(ARGV[6]) or 0
local dead_threshold = tonumber(ARGV[7])
local prefix         = ARGV[8]
local instance_id    = ARGV[9]

local ts = now_ms()
local instance_created = check_and_zadd(instances_key, instance_id, ts)
delete_dead_instances(prefix, instances_key, cumulative_key, keys_key, dead_threshold, ts)

local redis_epoch = tonumber(redis.call('HGET', epoch_key, counter_key) or 0) or 0
local is_stale    = (local_epoch ~= redis_epoch)
local cumulative  = tonumber(redis.call('HGET', cumulative_key, counter_key) or 0) or 0
local inst_count  = tonumber(redis.call('HGET', inst_count_key, counter_key) or 0) or 0

if instance_created ~= 0 and not is_stale and local_count > 0 then
    redis.call('HSET', inst_count_key, counter_key, local_count)
    cumulative = tonumber(redis.call('HINCRBY', cumulative_key, counter_key, local_count))
    inst_count = local_count
    redis.call('SADD', keys_key, counter_key)
end

local old_cumulative = cumulative
local old_inst_count = inst_count

if not compare_values(cumulative, comparator, compare_against) then
    return {counter_key, old_cumulative, old_inst_count, old_cumulative, old_inst_count, redis_epoch, instance_created, 0}
end

local new_inst_count
if is_stale then
    redis.call('HSET', inst_count_key, counter_key, delta)
    new_inst_count = delta
else
    new_inst_count = tonumber(redis.call('HINCRBY', inst_count_key, counter_key, delta))
end

local new_cumulative = tonumber(redis.call('HINCRBY', cumulative_key, counter_key, delta))
redis.call('SADD', keys_key, counter_key)

return {counter_key, new_cumulative, new_inst_count, old_cumulative, old_inst_count, redis_epoch, instance_created, 1}
"#;

const SET_LUA: &str = r#"
local epoch_key      = KEYS[1]
local instances_key  = KEYS[2]
local cumulative_key = KEYS[3]
local keys_key       = KEYS[4]
local inst_count_key = KEYS[5]

local counter_key    = ARGV[1]
local comparator     = ARGV[2]
local compare_against = tonumber(ARGV[3])
local count          = tonumber(ARGV[4])
local local_epoch    = tonumber(ARGV[5])
local local_count    = tonumber(ARGV[6]) or 0
local dead_threshold = tonumber(ARGV[7])
local prefix         = ARGV[8]
local instance_id    = ARGV[9]
local max_epoch      = tonumber(ARGV[10])

local ts = now_ms()
local instance_created = check_and_zadd(instances_key, instance_id, ts)
delete_dead_instances(prefix, instances_key, cumulative_key, keys_key, dead_threshold, ts)

local redis_epoch = tonumber(redis.call('HGET', epoch_key, counter_key) or 0) or 0
local cumulative  = tonumber(redis.call('HGET', cumulative_key, counter_key) or 0) or 0
local inst_count  = tonumber(redis.call('HGET', inst_count_key, counter_key) or 0) or 0
local is_stale    = (local_epoch ~= redis_epoch)

if instance_created ~= 0 and not is_stale and local_count > 0 then
    redis.call('HSET', inst_count_key, counter_key, local_count)
    cumulative = tonumber(redis.call('HINCRBY', cumulative_key, counter_key, local_count))
    inst_count = local_count
    redis.call('SADD', keys_key, counter_key)
end

local old_cumulative = cumulative
local old_inst_count = inst_count

if not compare_values(cumulative, comparator, compare_against) then
    return {counter_key, old_cumulative, old_inst_count, old_cumulative, old_inst_count, redis_epoch, instance_created, 0}
end

local new_epoch = redis_epoch + 1
if new_epoch > max_epoch then
    new_epoch = 0
end

redis.call('HSET', epoch_key,      counter_key, new_epoch)
redis.call('HSET', cumulative_key, counter_key, count)
redis.call('HSET', inst_count_key, counter_key, count)
redis.call('SADD', keys_key,       counter_key)

return {counter_key, count, count, old_cumulative, old_inst_count, new_epoch, instance_created, 1}
"#;

const SET_ON_INSTANCE_LUA: &str = r#"
local epoch_key      = KEYS[1]
local instances_key  = KEYS[2]
local cumulative_key = KEYS[3]
local keys_key       = KEYS[4]
local inst_count_key = KEYS[5]

local counter_key    = ARGV[1]
local comparator     = ARGV[2]
local compare_against = tonumber(ARGV[3])
local count          = tonumber(ARGV[4])
local local_epoch    = tonumber(ARGV[5])
local local_count    = tonumber(ARGV[6]) or 0
local dead_threshold = tonumber(ARGV[7])
local prefix         = ARGV[8]
local instance_id    = ARGV[9]

local ts = now_ms()
local instance_created = check_and_zadd(instances_key, instance_id, ts)
delete_dead_instances(prefix, instances_key, cumulative_key, keys_key, dead_threshold, ts)

local redis_epoch = tonumber(redis.call('HGET', epoch_key, counter_key) or 0) or 0
local inst_count  = tonumber(redis.call('HGET', inst_count_key, counter_key) or 0) or 0
local cumulative  = tonumber(redis.call('HGET', cumulative_key, counter_key) or 0) or 0
local is_stale    = (local_epoch ~= redis_epoch)

if instance_created ~= 0 and not is_stale and local_count > 0 then
    redis.call('HSET', inst_count_key, counter_key, local_count)
    cumulative = tonumber(redis.call('HINCRBY', cumulative_key, counter_key, local_count))
    inst_count = local_count
    redis.call('SADD', keys_key, counter_key)
end

local current_inst_count = is_stale and 0 or inst_count
local old_cumulative = cumulative
local old_inst_count = current_inst_count
if not compare_values(current_inst_count, comparator, compare_against) then
    return {counter_key, old_cumulative, old_inst_count, old_cumulative, old_inst_count, redis_epoch, instance_created, 0}
end

local delta = count - current_inst_count
redis.call('HSET', inst_count_key, counter_key, count)
local new_cumulative = tonumber(redis.call('HINCRBY', cumulative_key, counter_key, delta))
redis.call('SADD', keys_key, counter_key)

return {counter_key, new_cumulative, count, old_cumulative, old_inst_count, redis_epoch, instance_created, 1}
"#;

const GET_LUA: &str = r#"
local epoch_key      = KEYS[1]
local instances_key  = KEYS[2]
local cumulative_key = KEYS[3]
local keys_key       = KEYS[4]
local inst_count_key = KEYS[5]

local counter_key    = ARGV[1]
local local_epoch    = tonumber(ARGV[2])
local dead_threshold = tonumber(ARGV[3])
local prefix         = ARGV[4]
local instance_id    = ARGV[5]

local ts = now_ms()
local instance_created = check_and_zadd(instances_key, instance_id, ts)
delete_dead_instances(prefix, instances_key, cumulative_key, keys_key, dead_threshold, ts)

local redis_epoch = tonumber(redis.call('HGET', epoch_key, counter_key) or 0) or 0
local cumulative  = tonumber(redis.call('HGET', cumulative_key, counter_key) or 0) or 0
local inst_count  = tonumber(redis.call('HGET', inst_count_key, counter_key) or 0) or 0

return {counter_key, cumulative, inst_count, redis_epoch, instance_created}
"#;

const DEL_LUA: &str = r#"
local epoch_key      = KEYS[1]
local instances_key  = KEYS[2]
local cumulative_key = KEYS[3]
local keys_key       = KEYS[4]
local inst_count_key = KEYS[5]

local counter_key    = ARGV[1]
local local_epoch    = tonumber(ARGV[2])
local dead_threshold = tonumber(ARGV[3])
local prefix         = ARGV[4]
local instance_id    = ARGV[5]
local max_epoch      = tonumber(ARGV[6])

local ts = now_ms()
local instance_created = check_and_zadd(instances_key, instance_id, ts)
delete_dead_instances(prefix, instances_key, cumulative_key, keys_key, dead_threshold, ts)

local old_cumulative = tonumber(redis.call('HGET', cumulative_key, counter_key) or 0) or 0
local old_epoch = tonumber(redis.call('HGET', epoch_key, counter_key) or 0) or 0
local new_epoch = old_epoch + 1
if new_epoch > max_epoch then
    new_epoch = 0
end

redis.call('HSET', epoch_key,      counter_key, new_epoch)
redis.call('HDEL', cumulative_key, counter_key)
redis.call('SREM', keys_key,       counter_key)
redis.call('HDEL', inst_count_key, counter_key)

return {old_cumulative, new_epoch, instance_created}
"#;

const DEL_ON_INSTANCE_LUA: &str = r#"
local epoch_key      = KEYS[1]
local instances_key  = KEYS[2]
local cumulative_key = KEYS[3]
local keys_key       = KEYS[4]
local inst_count_key = KEYS[5]

local counter_key    = ARGV[1]
local local_epoch    = tonumber(ARGV[2])
local dead_threshold = tonumber(ARGV[3])
local prefix         = ARGV[4]
local instance_id    = ARGV[5]

local ts = now_ms()
local instance_created = check_and_zadd(instances_key, instance_id, ts)
delete_dead_instances(prefix, instances_key, cumulative_key, keys_key, dead_threshold, ts)

local redis_epoch = tonumber(redis.call('HGET', epoch_key, counter_key) or 0) or 0
local inst_count  = tonumber(redis.call('HGET', inst_count_key, counter_key) or 0) or 0
local is_stale    = (local_epoch ~= redis_epoch)

redis.call('HDEL', inst_count_key, counter_key)

local new_cumulative
if is_stale then
    new_cumulative = tonumber(redis.call('HGET', cumulative_key, counter_key) or 0) or 0
else
    new_cumulative = tonumber(redis.call('HINCRBY', cumulative_key, counter_key, -inst_count))
end

return {new_cumulative, inst_count, redis_epoch, instance_created}
"#;

/// Nuclear clear — no helpers prepended; iterates instances ZSET to clean up all count keys.
const CLEAR_LUA: &str = r#"
local epoch_key      = KEYS[1]
local instances_key  = KEYS[2]
local cumulative_key = KEYS[3]
local keys_key       = KEYS[4]
local prefix         = ARGV[1]

local all_instances = redis.call('ZRANGE', instances_key, 0, -1)
for _, inst_id in ipairs(all_instances) do
    redis.call('DEL', prefix .. ':count:' .. inst_id)
end
redis.call('DEL', epoch_key, instances_key, cumulative_key, keys_key)
"#;

const CLEAR_ON_INSTANCE_LUA: &str = r#"
local epoch_key      = KEYS[1]
local instances_key  = KEYS[2]
local cumulative_key = KEYS[3]
local keys_key       = KEYS[4]
local inst_count_key = KEYS[5]

local dead_threshold = tonumber(ARGV[1])
local prefix         = ARGV[2]
local instance_id    = ARGV[3]

local ts = now_ms()
check_and_zadd(instances_key, instance_id, ts)
delete_dead_instances(prefix, instances_key, cumulative_key, keys_key, dead_threshold, ts)

local all_keys = redis.call('HKEYS', inst_count_key)
if #all_keys > 0 then
    local values = redis.call('HMGET', inst_count_key, unpack(all_keys))
    for i = 1, #values do
        local c = tonumber(values[i] or 0) or 0
        if c ~= 0 then
            redis.call('HINCRBY', cumulative_key, all_keys[i], -c)
        end
    end
end
redis.call('DEL', inst_count_key)
"#;

const MARK_ALIVE_LUA: &str = r#"
local instances_key  = KEYS[1]
local epoch_key      = KEYS[2]
local cumulative_key = KEYS[3]
local keys_key       = KEYS[4]

local dead_threshold = tonumber(ARGV[1])
local prefix         = ARGV[2]
local instance_id    = ARGV[3]

local ts = now_ms()
local instance_created = check_and_zadd(instances_key, instance_id, ts)
delete_dead_instances(prefix, instances_key, cumulative_key, keys_key, dead_threshold, ts)

return tostring(instance_created)
"#;

const INC_IF_EPOCH_MATCHES_LUA: &str = r#"
local epoch_key      = KEYS[1]
local instances_key  = KEYS[2]
local cumulative_key = KEYS[3]
local keys_key       = KEYS[4]
local inst_count_key = KEYS[5]

local counter_key    = ARGV[1]
local recovery_count = tonumber(ARGV[2])
local local_epoch    = tonumber(ARGV[3])
local dead_threshold = tonumber(ARGV[4])
local prefix         = ARGV[5]
local instance_id    = ARGV[6]

local ts = now_ms()
check_and_zadd(instances_key, instance_id, ts)
delete_dead_instances(prefix, instances_key, cumulative_key, keys_key, dead_threshold, ts)

local redis_epoch = tonumber(redis.call('HGET', epoch_key, counter_key) or 0) or 0

if redis_epoch ~= local_epoch then
    -- Epoch moved while offline; contribution is stale — do not recover.
    local cumulative = tonumber(redis.call('HGET', cumulative_key, counter_key) or 0) or 0
    local inst_count = tonumber(redis.call('HGET', inst_count_key, counter_key) or 0) or 0
    return {counter_key, cumulative, inst_count, redis_epoch}
end

-- Epoch still matches — safe to restore the contribution.
local new_inst_count = tonumber(redis.call('HINCRBY', inst_count_key, counter_key, recovery_count))
local new_cumulative = tonumber(redis.call('HINCRBY', cumulative_key,  counter_key, recovery_count))
redis.call('SADD', keys_key, counter_key)

return {counter_key, new_cumulative, new_inst_count, redis_epoch}
"#;

// ---------------------------------------------------------------------------
// Options
// ---------------------------------------------------------------------------

/// Options for constructing a [`StrictInstanceAwareCounter`].
#[derive(Debug, Clone)]
pub struct StrictInstanceAwareCounterOptions {
    /// Redis key prefix used to namespace all counter keys.
    pub prefix: DistkitRedisKey,
    /// Redis connection manager.
    pub connection_manager: ConnectionManager,
    /// Milliseconds without a heartbeat before an instance is considered dead.
    /// Default: 30 000.
    pub dead_instance_threshold_ms: u64,
}

impl StrictInstanceAwareCounterOptions {
    /// Creates options with `dead_instance_threshold_ms` defaulting to 30 000 ms.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use distkit::{DistkitRedisKey, icounter::StrictInstanceAwareCounterOptions};
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let redis_url = std::env::var("REDIS_URL")
    ///     .unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());
    /// let client = redis::Client::open(redis_url)?;
    /// let conn = client.get_connection_manager().await?;
    /// let prefix = DistkitRedisKey::try_from("my_app".to_string())?;
    /// let opts = StrictInstanceAwareCounterOptions::new(prefix, conn);
    /// assert_eq!(opts.dead_instance_threshold_ms, 30_000);
    /// # Ok(())
    /// # }
    /// ```
    pub fn new(prefix: DistkitRedisKey, connection_manager: ConnectionManager) -> Self {
        Self {
            prefix,
            connection_manager,
            dead_instance_threshold_ms: 30_000,
        }
    }
}

// ---------------------------------------------------------------------------
// Counter struct
// ---------------------------------------------------------------------------

/// Immediately-consistent instance-aware distributed counter backed by Redis.
///
/// Each instance maintains its own per-key contribution; the cumulative total
/// is the sum of all live instances. When an instance stops heartbeating for
/// longer than `dead_instance_threshold_ms`, its contribution is automatically
/// removed by the next surviving instance that touches the same key.
///
/// Construct via [`StrictInstanceAwareCounter::new`], which returns an `Arc<Self>`.
#[derive(Debug)]
pub struct StrictInstanceAwareCounter {
    connection_manager: ConnectionManager,
    key_generator: RedisKeyGenerator,
    instance_id: String,
    dead_instance_threshold_ms: u64,
    /// Per-key in-memory state: epoch, last-seen cumulative, and this instance's count.
    local_store: DashMap<DistkitRedisKey, SingleStore>,
    /// Maximum epoch value before wrapping. Set to `u64::MAX / 2`.
    max_epoch: u64,
    inc_script: Script,
    set_script: Script,
    set_on_instance_script: Script,
    get_script: Script,
    del_script: Script,
    del_on_instance_script: Script,
    clear_script: Script,
    clear_on_instance_script: Script,
    mark_alive_script: Script,
    inc_if_epoch_matches_script: Script,
    activity: Arc<ActivityTracker>,
}

impl StrictInstanceAwareCounter {
    /// Shared construction body — builds the counter Arc and spawns the
    /// heartbeat task. The key type (and therefore Redis namespace) is
    /// determined by the caller via `key_generator`.
    fn build(
        key_generator: RedisKeyGenerator,
        connection_manager: ConnectionManager,
        dead_instance_threshold_ms: u64,
    ) -> Arc<Self> {
        let instance_id = generate_instance_id();
        let counter = Arc::new(Self {
            connection_manager,
            key_generator,
            instance_id,
            dead_instance_threshold_ms,
            local_store: DashMap::default(),
            max_epoch: u64::MAX / 2,
            inc_script: Script::new(&format!("{HELPERS_LUA}\n{INC_LUA}")),
            set_script: Script::new(&format!("{HELPERS_LUA}\n{SET_LUA}")),
            set_on_instance_script: Script::new(&format!("{HELPERS_LUA}\n{SET_ON_INSTANCE_LUA}")),
            get_script: Script::new(&format!("{HELPERS_LUA}\n{GET_LUA}")),
            del_script: Script::new(&format!("{HELPERS_LUA}\n{DEL_LUA}")),
            del_on_instance_script: Script::new(&format!("{HELPERS_LUA}\n{DEL_ON_INSTANCE_LUA}")),
            clear_script: Script::new(CLEAR_LUA),
            clear_on_instance_script: Script::new(&format!(
                "{HELPERS_LUA}\n{CLEAR_ON_INSTANCE_LUA}"
            )),
            mark_alive_script: Script::new(&format!("{HELPERS_LUA}\n{MARK_ALIVE_LUA}")),
            inc_if_epoch_matches_script: Script::new(&format!(
                "{HELPERS_LUA}\n{INC_IF_EPOCH_MATCHES_LUA}"
            )),
            activity: ActivityTracker::new(EPOCH_CHANGE_INTERVAL),
        });
        counter.run_heartbeat_task();
        counter
    }

    /// Creates a new instance-aware counter and spawns its background heartbeat task.
    ///
    /// Each call returns a distinct `Arc<Self>` with a unique `instance_id`. The
    /// heartbeat task holds a [`Weak`](std::sync::Weak) reference and stops
    /// automatically when the counter is dropped.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use distkit::{DistkitRedisKey, icounter::{StrictInstanceAwareCounter, StrictInstanceAwareCounterOptions}};
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let redis_url = std::env::var("REDIS_URL")
    ///     .unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());
    /// let client = redis::Client::open(redis_url)?;
    /// let conn = client.get_connection_manager().await?;
    /// let prefix = DistkitRedisKey::try_from("my_app".to_string())?;
    /// let counter = StrictInstanceAwareCounter::new(StrictInstanceAwareCounterOptions::new(prefix, conn));
    /// assert!(!counter.instance_id().is_empty());
    /// # Ok(())
    /// # }
    /// ```
    pub fn new(options: StrictInstanceAwareCounterOptions) -> Arc<Self> {
        let StrictInstanceAwareCounterOptions {
            prefix,
            connection_manager,
            dead_instance_threshold_ms,
        } = options;
        let key_generator = RedisKeyGenerator::new(prefix, RedisKeyGeneratorTypeKey::InstanceAware);
        Self::build(
            key_generator,
            connection_manager,
            dead_instance_threshold_ms,
        )
    }

    /// Creates a `StrictInstanceAwareCounter` intended to back a
    /// [`LaxInstanceAwareCounter`]. Uses the `lax_instance_aware_counter`
    /// key-type prefix so it does not collide with a standalone
    /// `StrictInstanceAwareCounter` sharing the same logical prefix.
    pub(crate) fn new_as_lax_backend(options: StrictInstanceAwareCounterOptions) -> Arc<Self> {
        let StrictInstanceAwareCounterOptions {
            prefix,
            connection_manager,
            dead_instance_threshold_ms,
        } = options;
        let key_generator =
            RedisKeyGenerator::new(prefix, RedisKeyGeneratorTypeKey::LaxInstanceAware);
        Self::build(
            key_generator,
            connection_manager,
            dead_instance_threshold_ms,
        )
    }

    // -----------------------------------------------------------------------
    // Key helpers
    // -----------------------------------------------------------------------

    fn epoch_key(&self) -> String {
        format!("{}:epoch", self.key_generator.container_key())
    }

    fn instances_key(&self) -> String {
        format!("{}:instances", self.key_generator.container_key())
    }

    fn cumulative_key(&self) -> String {
        format!("{}:cumulative", self.key_generator.container_key())
    }

    fn keys_key(&self) -> String {
        format!("{}:keys", self.key_generator.container_key())
    }

    fn inst_count_key(&self) -> String {
        format!(
            "{}:count:{}",
            self.key_generator.container_key(),
            self.instance_id
        )
    }

    fn prefix_str(&self) -> String {
        self.key_generator.container_key()
    }

    // -----------------------------------------------------------------------
    // local_store helpers
    // -----------------------------------------------------------------------

    fn get_local_epoch(&self, key: &DistkitRedisKey) -> u64 {
        self.local_store
            .get(key)
            .map(|s| s.epoch.load(Ordering::Acquire))
            .unwrap_or(0)
    }

    fn get_local_count(&self, key: &DistkitRedisKey) -> i64 {
        self.local_store
            .get(key)
            .map(|s| s.local_count.load(Ordering::Acquire))
            .unwrap_or(0)
    }

    fn update_local_store(
        &self,
        key: &DistkitRedisKey,
        epoch: u64,
        cumulative: i64,
        local_count: i64,
    ) {
        match self.local_store.get(key) {
            Some(s) => {
                s.epoch.store(epoch, Ordering::Release);
                s.cumulative.store(cumulative, Ordering::Release);
                s.local_count.store(local_count, Ordering::Release);
            }
            None => {
                self.local_store
                    .entry(key.clone())
                    .and_modify(|s| {
                        s.epoch.store(epoch, Ordering::Relaxed);
                        s.cumulative.store(cumulative, Ordering::Relaxed);
                        s.local_count.store(local_count, Ordering::Relaxed);
                    })
                    .or_insert_with(|| SingleStore::new(epoch, cumulative, local_count));
            }
        }
    }

    // -----------------------------------------------------------------------
    // Heartbeat
    // -----------------------------------------------------------------------

    fn run_heartbeat_task(self: &Arc<Self>) {
        let weak = Arc::downgrade(self);
        let mut activity_watch = self.activity.subscribe();

        tokio::spawn(async move {
            let mut tick = tokio::time::interval(EPOCH_CHANGE_INTERVAL);
            tick.tick().await; // skip first immediate tick

            loop {
                tokio::select! {
                    changed = activity_watch.changed() => {
                        if changed.is_err() { break; }
                        let Some(c) = weak.upgrade() else { break; };
                        if !c.activity.get_is_active() {
                            let _ = c.mark_alive().await;
                        }
                    }
                    _ = tick.tick() => {
                        let Some(c) = weak.upgrade() else { break; };
                        if !c.activity.get_is_active() {
                            let _ = c.mark_alive().await;
                        }
                    }
                }
            }
        });
    }

    /// Sends recovery increments for all keys in `recoveries` using pipelined
    /// `INC_IF_EPOCH_MATCHES_LUA` calls, chunked to avoid oversized pipelines.
    /// After each chunk the returned `(key, cumulative, inst_count, redis_epoch)`
    /// tuples are used to update `local_store` before the next chunk begins.
    async fn recover_contributions_batched(
        &self,
        recoveries: Vec<(DistkitRedisKey, i64, u64)>,
        chunk_size: usize,
    ) -> Result<(), DistkitError> {
        if recoveries.is_empty() {
            return Ok(());
        }

        let mut conn = self.connection_manager.clone();
        let mut processed = 0;

        while processed < recoveries.len() {
            let end = (processed + chunk_size).min(recoveries.len());
            let chunk = &recoveries[processed..end];
            let script = &self.inc_if_epoch_matches_script;

            let results: Vec<(String, i64, i64, i64)> =
                execute_pipeline_with_script_retry(&mut conn, script, chunk, |item| {
                    let (key, count, local_epoch) = item;
                    let mut inv = script.key(self.epoch_key());
                    inv.key(self.instances_key());
                    inv.key(self.cumulative_key());
                    inv.key(self.keys_key());
                    inv.key(self.inst_count_key());
                    inv.arg(key.as_str());
                    inv.arg(*count);
                    inv.arg(*local_epoch);
                    inv.arg(self.dead_instance_threshold_ms);
                    inv.arg(self.prefix_str());
                    inv.arg(&self.instance_id);
                    inv
                })
                .await?;

            // Each result carries its own key — no zip required.
            for (key_str, cumulative, inst_count, redis_epoch) in results {
                if let Ok(key) = DistkitRedisKey::try_from(key_str) {
                    self.update_local_store(&key, redis_epoch as u64, cumulative, inst_count);
                }
            }

            processed = end;
        }

        Ok(())
    }

    pub(crate) async fn inc_batch(
        &self,
        increments: &mut Vec<(DistkitRedisKey, i64)>,
        max_batch_size: usize,
    ) -> Result<Vec<(String, i64, i64)>, DistkitError> {
        if increments.is_empty() {
            return Ok(vec![]);
        }

        let mut processed = 0;
        let mut output: Vec<(String, i64, i64)> = Vec::with_capacity(increments.len());

        while processed < increments.len() {
            let end = (processed + max_batch_size).min(increments.len());
            let chunk = &increments[processed..end];
            let conditional_chunk: Vec<(&DistkitRedisKey, CounterComparator, i64)> = chunk
                .iter()
                .map(|(key, delta)| (key, CounterComparator::Nil, *delta))
                .collect();
            let chunk_results = self.inc_if_batch(&conditional_chunk).await?;

            output.extend(
                chunk_results
                    .into_iter()
                    .map(|(key, cumulative, inst_count, _, _)| {
                        (key.to_string(), cumulative, inst_count)
                    }),
            );

            processed = end;
        }

        // All chunks succeeded — drain entire input.
        increments.drain(..processed);

        Ok(output)
    }

    pub(crate) async fn inc_if_batch<'k>(
        &self,
        updates: &[(&'k DistkitRedisKey, CounterComparator, i64)],
    ) -> Result<Vec<(&'k DistkitRedisKey, i64, i64, i64, i64)>, DistkitError> {
        if updates.is_empty() {
            return Ok(vec![]);
        }

        self.activity.signal();

        let mut conn = self.connection_manager.clone();
        let mut processed = 0;
        let mut output = Vec::with_capacity(updates.len());

        while processed < updates.len() {
            let mut seen = HashSet::new();
            let mut end = processed;
            while end < updates.len() && seen.insert(updates[end].0.as_str()) {
                end += 1;
            }

            let chunk = &updates[processed..end];
            let script = &self.inc_script;
            let local_epochs: Vec<u64> = chunk
                .iter()
                .map(|(key, _, _)| self.get_local_epoch(key))
                .collect();

            let chunk_results: Vec<(String, i64, i64, i64, i64, u64, i64, i64)> =
                execute_pipeline_with_script_retry(&mut conn, script, chunk, |update| {
                    let (key, comparator, delta) = update;
                    let (lua_comparator, compare_against) = comparator.as_lua_parts();
                    let mut inv = script.key(self.epoch_key());
                    inv.key(self.instances_key());
                    inv.key(self.cumulative_key());
                    inv.key(self.keys_key());
                    inv.key(self.inst_count_key());
                    inv.arg(key.as_str());
                    inv.arg(lua_comparator);
                    inv.arg(compare_against);
                    inv.arg(*delta);
                    inv.arg(self.get_local_epoch(key));
                    inv.arg(self.get_local_count(key));
                    inv.arg(self.dead_instance_threshold_ms);
                    inv.arg(self.prefix_str());
                    inv.arg(&self.instance_id);
                    inv
                })
                .await?;

            for (
                ((key, _, _), local_epoch),
                (
                    _,
                    cumulative,
                    inst_count,
                    old_cumulative,
                    old_inst_count,
                    redis_epoch,
                    _,
                    matched_raw,
                ),
            ) in chunk
                .iter()
                .zip(local_epochs.iter())
                .zip(chunk_results.into_iter())
            {
                if matched_raw != 0 || *local_epoch == redis_epoch {
                    self.update_local_store(key, redis_epoch, cumulative, inst_count);
                }

                output.push((*key, cumulative, inst_count, old_cumulative, old_inst_count));
            }

            processed = end;
        }

        Ok(output)
    }

    pub(crate) async fn get_batch<'k>(
        &self,
        keys: &[&'k DistkitRedisKey],
    ) -> Result<Vec<(&'k DistkitRedisKey, i64, i64)>, DistkitError> {
        if keys.is_empty() {
            return Ok(vec![]);
        }

        self.activity.signal();

        let mut conn = self.connection_manager.clone();
        let mut map: HashMap<String, (i64, i64)> = HashMap::with_capacity(keys.len());
        let mut recovery_keys: Vec<(DistkitRedisKey, i64)> = Vec::new();

        let mut processed = 0;
        while processed < keys.len() {
            let end = (processed + MAX_BATCH_SIZE).min(keys.len());
            let chunk = &keys[processed..end];
            let script = &self.get_script;

            let chunk_results: Vec<(String, i64, i64, u64, i64)> =
                execute_pipeline_with_script_retry(&mut conn, script, chunk, |key| {
                    let local_epoch = self.get_local_epoch(key);
                    let mut inv = script.key(self.epoch_key());
                    inv.key(self.instances_key());
                    inv.key(self.cumulative_key());
                    inv.key(self.keys_key());
                    inv.key(self.inst_count_key());
                    inv.arg(key.as_str());
                    inv.arg(local_epoch);
                    inv.arg(self.dead_instance_threshold_ms);
                    inv.arg(self.prefix_str());
                    inv.arg(&self.instance_id);
                    inv
                })
                .await?;

            for (key_str, cumulative, inst_count, redis_epoch, instance_created_raw) in
                chunk_results
            {
                if let Ok(key) = DistkitRedisKey::try_from(key_str.clone()) {
                    let instance_created = instance_created_raw != 0;
                    let local_epoch = self.get_local_epoch(&key);
                    let old_local_count = self.get_local_count(&key);
                    self.update_local_store(&key, redis_epoch, cumulative, inst_count);
                    if instance_created && local_epoch == redis_epoch && old_local_count > 0 {
                        recovery_keys.push((key.clone(), old_local_count));
                    }
                    map.insert(key_str, (cumulative, inst_count));
                }
            }

            processed = end;
        }

        // Sequential recovery fallback (rare: instance was cleaned up as dead).
        for (key, old_count) in recovery_keys {
            let (cumulative, inst_count) = self.inc(&key, old_count).await?;
            map.insert(key.to_string(), (cumulative, inst_count));
        }

        Ok(keys
            .iter()
            .map(|k| {
                let (cum, inst) = map.get(k.as_str()).copied().unwrap_or((0, 0));
                (*k, cum, inst)
            })
            .collect())
    }

    pub(crate) async fn set_batch<'k>(
        &self,
        updates: &[(&'k DistkitRedisKey, i64)],
    ) -> Result<Vec<(&'k DistkitRedisKey, i64, i64)>, DistkitError> {
        let conditional_updates: Vec<(&DistkitRedisKey, CounterComparator, i64)> = updates
            .iter()
            .map(|(key, count)| (*key, CounterComparator::Nil, *count))
            .collect();

        Ok(self
            .set_if_batch(&conditional_updates)
            .await?
            .into_iter()
            .map(|(key, cumulative, inst_count, _, _)| (key, cumulative, inst_count))
            .collect())
    }

    pub(crate) async fn set_if_batch<'k>(
        &self,
        updates: &[(&'k DistkitRedisKey, CounterComparator, i64)],
    ) -> Result<Vec<(&'k DistkitRedisKey, i64, i64, i64, i64)>, DistkitError> {
        if updates.is_empty() {
            return Ok(vec![]);
        }

        self.activity.signal();

        let mut conn = self.connection_manager.clone();
        let mut map: HashMap<DistkitRedisKey, (i64, i64, i64, i64)> =
            HashMap::with_capacity(updates.len());
        let mut processed = 0;

        while processed < updates.len() {
            let end = (processed + MAX_BATCH_SIZE).min(updates.len());
            let chunk = &updates[processed..end];
            let script = &self.set_script;
            let local_epochs: HashMap<DistkitRedisKey, u64> = chunk
                .iter()
                .map(|(key, _, _)| ((*key).clone(), self.get_local_epoch(key)))
                .collect();

            let chunk_results: Vec<(String, i64, i64, i64, i64, u64, i64, i64)> =
                execute_pipeline_with_script_retry(&mut conn, script, chunk, |update| {
                    let (key, comparator, count) = update;
                    let (lua_comparator, compare_against) = comparator.as_lua_parts();
                    let mut inv = script.key(self.epoch_key());
                    inv.key(self.instances_key());
                    inv.key(self.cumulative_key());
                    inv.key(self.keys_key());
                    inv.key(self.inst_count_key());
                    inv.arg(key.as_str());
                    inv.arg(lua_comparator);
                    inv.arg(compare_against);
                    inv.arg(*count);
                    inv.arg(self.get_local_epoch(key));
                    inv.arg(self.get_local_count(key));
                    inv.arg(self.dead_instance_threshold_ms);
                    inv.arg(self.prefix_str());
                    inv.arg(&self.instance_id);
                    inv.arg(self.max_epoch);
                    inv
                })
                .await?;

            for (
                key,
                cumulative,
                inst_count,
                old_cumulative,
                old_inst_count,
                redis_epoch,
                _,
                matched_raw,
            ) in chunk_results
            {
                let Ok(key) = DistkitRedisKey::try_from(key.clone()) else {
                    continue;
                };

                let local_epoch = local_epochs.get(&key).copied().unwrap_or(0);
                if matched_raw != 0 || local_epoch == redis_epoch {
                    self.update_local_store(&key, redis_epoch, cumulative, inst_count);
                }

                map.insert(
                    key,
                    (cumulative, inst_count, old_cumulative, old_inst_count),
                );
            }

            processed = end;
        }

        Ok(updates
            .iter()
            .map(|(k, _, _)| {
                let (cum, inst, old_cum, old_inst) = map.get(k).copied().unwrap_or((0, 0, 0, 0));
                (*k, cum, inst, old_cum, old_inst)
            })
            .collect())
    }

    async fn set_on_instance_batch<'k>(
        &self,
        updates: &[(&'k DistkitRedisKey, i64)],
    ) -> Result<Vec<(&'k DistkitRedisKey, i64, i64)>, DistkitError> {
        let conditional_updates: Vec<(&DistkitRedisKey, CounterComparator, i64)> = updates
            .iter()
            .map(|(key, count)| (*key, CounterComparator::Nil, *count))
            .collect();

        Ok(self
            .set_on_instance_if_batch(&conditional_updates)
            .await?
            .into_iter()
            .map(|(key, cumulative, inst_count, _, _)| (key, cumulative, inst_count))
            .collect())
    }

    async fn set_on_instance_if_batch<'k>(
        &self,
        updates: &[(&'k DistkitRedisKey, CounterComparator, i64)],
    ) -> Result<Vec<(&'k DistkitRedisKey, i64, i64, i64, i64)>, DistkitError> {
        if updates.is_empty() {
            return Ok(vec![]);
        }

        self.activity.signal();

        let mut conn = self.connection_manager.clone();
        let mut map: HashMap<DistkitRedisKey, (i64, i64, i64, i64)> =
            HashMap::with_capacity(updates.len());
        let mut processed = 0;

        while processed < updates.len() {
            let end = (processed + MAX_BATCH_SIZE).min(updates.len());
            let chunk = &updates[processed..end];
            let script = &self.set_on_instance_script;
            let local_epochs: HashMap<DistkitRedisKey, u64> = chunk
                .iter()
                .map(|(key, _, _)| ((*key).clone(), self.get_local_epoch(key)))
                .collect();

            let chunk_results: Vec<(String, i64, i64, i64, i64, u64, i64, i64)> =
                execute_pipeline_with_script_retry(&mut conn, script, chunk, |update| {
                    let (key, comparator, count) = update;
                    let (lua_comparator, compare_against) = comparator.as_lua_parts();
                    let mut inv = script.key(self.epoch_key());
                    inv.key(self.instances_key());
                    inv.key(self.cumulative_key());
                    inv.key(self.keys_key());
                    inv.key(self.inst_count_key());
                    inv.arg(key.as_str());
                    inv.arg(lua_comparator);
                    inv.arg(compare_against);
                    inv.arg(*count);
                    inv.arg(self.get_local_epoch(key));
                    inv.arg(self.get_local_count(key));
                    inv.arg(self.dead_instance_threshold_ms);
                    inv.arg(self.prefix_str());
                    inv.arg(&self.instance_id);
                    inv
                })
                .await?;

            for (
                key,
                cumulative,
                inst_count,
                old_cumulative,
                old_inst_count,
                redis_epoch,
                _,
                matched_raw,
            ) in chunk_results
            {
                let Ok(key) = DistkitRedisKey::try_from(key.clone()) else {
                    continue;
                };

                let local_epoch = local_epochs.get(&key).copied().unwrap_or(0);
                if matched_raw != 0 || local_epoch == redis_epoch {
                    self.update_local_store(&key, redis_epoch, cumulative, inst_count);
                }

                map.insert(
                    key,
                    (cumulative, inst_count, old_cumulative, old_inst_count),
                );
            }
            processed = end;
        }

        Ok(updates
            .iter()
            .map(|(k, _, _)| {
                let (cum, inst, old_cum, old_inst) = map.get(k).copied().unwrap_or((0, 0, 0, 0));
                (*k, cum, inst, old_cum, old_inst)
            })
            .collect())
    }

    #[cfg(test)]
    pub(crate) async fn trigger_mark_alive(&self) -> Result<(), DistkitError> {
        self.mark_alive().await
    }

    async fn mark_alive(&self) -> Result<(), DistkitError> {
        let mut conn = self.connection_manager.clone();

        let instance_created: i8 = self
            .mark_alive_script
            .key(self.instances_key())
            .key(self.epoch_key())
            .key(self.cumulative_key())
            .key(self.keys_key())
            .arg(self.dead_instance_threshold_ms)
            .arg(self.prefix_str())
            .arg(&self.instance_id)
            .invoke_async(&mut conn)
            .await?;

        if instance_created != 0i8 {
            // The instance was cleaned up while offline. Recover contributions
            // for all keys that still have a positive local count, but only
            // when the per-key epoch in Redis still matches — epoch-safe recovery.
            let recoveries: Vec<(DistkitRedisKey, i64, u64)> = self
                .local_store
                .iter()
                .filter_map(|e| {
                    let count = e.local_count.load(Ordering::Acquire);
                    let epoch = e.epoch.load(Ordering::Acquire);
                    if count > 0 {
                        Some((e.key().clone(), count, epoch))
                    } else {
                        None
                    }
                })
                .collect();

            let _ = self.recover_contributions_batched(recoveries, 50).await;
        }

        Ok(())
    }

    // -----------------------------------------------------------------------
    // Public API
    // -----------------------------------------------------------------------

    /// This instance's unique identifier.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use distkit::DistkitRedisKey;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let counter = distkit::__doctest_helpers::strict_icounter().await?;
    /// assert!(!counter.instance_id().is_empty());
    /// # Ok(())
    /// # }
    /// ```
    pub fn instance_id(&self) -> &str {
        &self.instance_id
    }

    /// Adds `count` to this instance's contribution for `key`.
    ///
    /// If the local epoch is stale, the stored count is reset to `count` before
    /// incrementing. Returns `(cumulative, instance_count)`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use distkit::DistkitRedisKey;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let (server_a, server_b) = distkit::__doctest_helpers::two_strict_icounters().await?;
    /// let key = DistkitRedisKey::try_from("connections".to_string())?;
    /// let (cumulative_a, slice_a) = server_a.inc(&key, 3).await?;
    /// assert_eq!(cumulative_a, 3);
    /// assert_eq!(slice_a, 3);
    /// let (cumulative_b, slice_b) = server_b.inc(&key, 5).await?;
    /// assert_eq!(cumulative_b, 8); // both contributions
    /// assert_eq!(slice_b, 5);      // only server_b's slice
    /// # Ok(())
    /// # }
    /// ```
    pub async fn inc(&self, key: &DistkitRedisKey, count: i64) -> Result<(i64, i64), DistkitError> {
        Ok(self.inc_if(key, CounterComparator::Nil, count).await?.0)
    }

    /// Conditionally adds `count` to this instance's contribution for `key`
    /// when the cumulative total satisfies `comparator`.
    ///
    /// Returns `(new_state, old_state)`, where each state is
    /// `(cumulative, instance_count)`.
    pub async fn inc_if(
        &self,
        key: &DistkitRedisKey,
        comparator: CounterComparator,
        count: i64,
    ) -> Result<((i64, i64), (i64, i64)), DistkitError> {
        self.activity.signal();

        let mut conn = self.connection_manager.clone();
        let local_epoch = self.get_local_epoch(key);
        let (lua_comparator, compare_against) = comparator.as_lua_parts();

        let (
            _,
            cumulative,
            inst_count,
            old_cumulative,
            old_inst_count,
            redis_epoch,
            _,
            matched_raw,
        ): (String, i64, i64, i64, i64, u64, i64, i64) = self
            .inc_script
            .key(self.epoch_key())
            .key(self.instances_key())
            .key(self.cumulative_key())
            .key(self.keys_key())
            .key(self.inst_count_key())
            .arg(key.as_str())
            .arg(lua_comparator)
            .arg(compare_against)
            .arg(count)
            .arg(local_epoch)
            .arg(self.get_local_count(key))
            .arg(self.dead_instance_threshold_ms)
            .arg(self.prefix_str())
            .arg(&self.instance_id)
            .invoke_async(&mut conn)
            .await?;

        if matched_raw != 0 || local_epoch == redis_epoch {
            self.update_local_store(key, redis_epoch, cumulative, inst_count);
        }

        Ok(((cumulative, inst_count), (old_cumulative, old_inst_count)))
    }

    /// Sets the global cumulative for `key` to `count` and bumps the epoch.
    ///
    /// All other instances see their stored count as stale on their next
    /// operation. The calling instance becomes sole owner of the entire count.
    /// Returns `(cumulative, instance_count)`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use distkit::DistkitRedisKey;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let (server_a, server_b) = distkit::__doctest_helpers::two_strict_icounters().await?;
    /// let key = DistkitRedisKey::try_from("connections".to_string())?;
    /// server_a.inc(&key, 10).await?;
    /// server_b.inc(&key, 5).await?;
    /// // Epoch bumps; all previous per-instance contributions are cleared.
    /// let (cumulative, slice) = server_a.set(&key, 100).await?;
    /// assert_eq!(cumulative, 100);
    /// assert_eq!(slice, 100); // server_a owns the entire count
    /// # Ok(())
    /// # }
    /// ```
    pub async fn set(&self, key: &DistkitRedisKey, count: i64) -> Result<(i64, i64), DistkitError> {
        Ok(self.set_if(key, CounterComparator::Nil, count).await?.0)
    }

    /// Conditionally sets the cumulative total for `key` to `count` when the
    /// current cumulative total satisfies `comparator`.
    ///
    /// Returns `(new_state, old_state)`, where each state is
    /// `(cumulative, instance_count)`.
    pub async fn set_if(
        &self,
        key: &DistkitRedisKey,
        comparator: CounterComparator,
        count: i64,
    ) -> Result<((i64, i64), (i64, i64)), DistkitError> {
        self.activity.signal();

        let mut conn = self.connection_manager.clone();
        let local_epoch = self.get_local_epoch(key);
        let (lua_comparator, compare_against) = comparator.as_lua_parts();

        let (
            _,
            cumulative,
            inst_count,
            old_cumulative,
            old_inst_count,
            redis_epoch,
            _,
            matched_raw,
        ): (String, i64, i64, i64, i64, u64, i64, i64) = self
            .set_script
            .key(self.epoch_key())
            .key(self.instances_key())
            .key(self.cumulative_key())
            .key(self.keys_key())
            .key(self.inst_count_key())
            .arg(key.as_str())
            .arg(lua_comparator)
            .arg(compare_against)
            .arg(count)
            .arg(local_epoch)
            .arg(self.get_local_count(key))
            .arg(self.dead_instance_threshold_ms)
            .arg(self.prefix_str())
            .arg(&self.instance_id)
            .arg(self.max_epoch)
            .invoke_async(&mut conn)
            .await?;

        if matched_raw != 0 || local_epoch == redis_epoch {
            self.update_local_store(key, redis_epoch, cumulative, inst_count);
        }

        Ok(((cumulative, inst_count), (old_cumulative, old_inst_count)))
    }

    /// Sets this instance's contribution for `key` to `count` without bumping the epoch.
    ///
    /// On epoch mismatch the previous contribution is treated as 0. Other
    /// instances' slices are preserved. Returns `(cumulative, instance_count)`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use distkit::DistkitRedisKey;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let (server_a, server_b) = distkit::__doctest_helpers::two_strict_icounters().await?;
    /// let key = DistkitRedisKey::try_from("connections".to_string())?;
    /// server_a.inc(&key, 10).await?;
    /// server_b.inc(&key, 5).await?;
    /// // No epoch bump: server_b's slice is not evicted.
    /// let (cumulative, slice) = server_a.set_on_instance(&key, 7).await?;
    /// assert_eq!(slice, 7);
    /// assert_eq!(cumulative, 12); // server_a: 7 + server_b: 5
    /// # Ok(())
    /// # }
    /// ```
    pub async fn set_on_instance(
        &self,
        key: &DistkitRedisKey,
        count: i64,
    ) -> Result<(i64, i64), DistkitError> {
        Ok(self
            .set_on_instance_if(key, CounterComparator::Nil, count)
            .await?
            .0)
    }

    /// Conditionally sets this instance's contribution for `key` to `count`
    /// when the current instance slice satisfies `comparator`.
    ///
    /// Returns `(new_state, old_state)`, where each state is
    /// `(cumulative, instance_count)`.
    pub async fn set_on_instance_if(
        &self,
        key: &DistkitRedisKey,
        comparator: CounterComparator,
        count: i64,
    ) -> Result<((i64, i64), (i64, i64)), DistkitError> {
        self.activity.signal();

        let mut conn = self.connection_manager.clone();
        let local_epoch = self.get_local_epoch(key);
        let (lua_comparator, compare_against) = comparator.as_lua_parts();

        let (
            _,
            cumulative,
            inst_count,
            old_cumulative,
            old_inst_count,
            redis_epoch,
            _,
            matched_raw,
        ): (String, i64, i64, i64, i64, u64, i64, i64) = self
            .set_on_instance_script
            .key(self.epoch_key())
            .key(self.instances_key())
            .key(self.cumulative_key())
            .key(self.keys_key())
            .key(self.inst_count_key())
            .arg(key.as_str())
            .arg(lua_comparator)
            .arg(compare_against)
            .arg(count)
            .arg(local_epoch)
            .arg(self.get_local_count(key))
            .arg(self.dead_instance_threshold_ms)
            .arg(self.prefix_str())
            .arg(&self.instance_id)
            .invoke_async(&mut conn)
            .await?;

        if matched_raw != 0 || local_epoch == redis_epoch {
            self.update_local_store(key, redis_epoch, cumulative, inst_count);
        }

        Ok(((cumulative, inst_count), (old_cumulative, old_inst_count)))
    }

    /// Returns `(cumulative, instance_count)` for `key`, triggering dead-instance cleanup.
    ///
    /// A missing key returns `(0, 0)`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use distkit::DistkitRedisKey;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let counter = distkit::__doctest_helpers::strict_icounter().await?;
    /// let key = DistkitRedisKey::try_from("connections".to_string())?;
    /// // A missing key returns (0, 0).
    /// assert_eq!(counter.get(&key).await?, (0, 0));
    /// counter.inc(&key, 5).await?;
    /// assert_eq!(counter.get(&key).await?, (5, 5));
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get(&self, key: &DistkitRedisKey) -> Result<(i64, i64), DistkitError> {
        self.activity.signal();

        let mut conn = self.connection_manager.clone();
        let local_epoch = self.get_local_epoch(key);

        let (_, cumulative, inst_count, redis_epoch, instance_created_raw): (
            String,
            i64,
            i64,
            u64,
            i64,
        ) = self
            .get_script
            .key(self.epoch_key())
            .key(self.instances_key())
            .key(self.cumulative_key())
            .key(self.keys_key())
            .key(self.inst_count_key())
            .arg(key.as_str())
            .arg(local_epoch)
            .arg(self.dead_instance_threshold_ms)
            .arg(self.prefix_str())
            .arg(&self.instance_id)
            .invoke_async(&mut conn)
            .await?;

        let instance_created = instance_created_raw != 0;
        let should_recover = instance_created && local_epoch == redis_epoch;

        let old_local_count = self.get_local_count(key);
        self.update_local_store(key, redis_epoch, cumulative, inst_count);

        if should_recover && old_local_count > 0 {
            return self.inc(key, old_local_count).await;
        }

        Ok((cumulative, inst_count))
    }

    /// Deletes `key` globally and bumps the epoch. Returns `(old_cumulative, old_instance_count)`.
    ///
    /// All instances start fresh from `0` on their next operation. A non-existent key
    /// returns `(0, 0)`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use distkit::DistkitRedisKey;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let (server_a, server_b) = distkit::__doctest_helpers::two_strict_icounters().await?;
    /// let key = DistkitRedisKey::try_from("connections".to_string())?;
    /// server_a.inc(&key, 3).await?;
    /// server_b.inc(&key, 7).await?;
    /// let (old_cumulative, _) = server_a.del(&key).await?;
    /// assert_eq!(old_cumulative, 10);
    /// // After deletion both instances start fresh from 0.
    /// assert_eq!(server_b.inc(&key, 1).await?, (1, 1));
    /// # Ok(())
    /// # }
    /// ```
    pub async fn del(&self, key: &DistkitRedisKey) -> Result<(i64, i64), DistkitError> {
        self.activity.signal();

        let mut conn = self.connection_manager.clone();
        let local_epoch = self.get_local_epoch(key);

        let (old_cumulative, _, _): (i64, i64, i64) = self
            .del_script
            .key(self.epoch_key())
            .key(self.instances_key())
            .key(self.cumulative_key())
            .key(self.keys_key())
            .key(self.inst_count_key())
            .arg(key.as_str())
            .arg(local_epoch)
            .arg(self.dead_instance_threshold_ms)
            .arg(self.prefix_str())
            .arg(&self.instance_id)
            .arg(self.max_epoch)
            .invoke_async(&mut conn)
            .await?;

        let old_inst_count = self.get_local_count(key);

        // Key deleted globally; remove from local store entirely.
        // No recovery: epoch always bumps.
        self.local_store.remove(key);

        Ok((old_cumulative, old_inst_count))
    }

    /// Removes this instance's contribution for `key` without bumping the epoch.
    ///
    /// Other instances' slices are preserved. Returns `(new_cumulative, removed_count)`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use distkit::DistkitRedisKey;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let (server_a, server_b) = distkit::__doctest_helpers::two_strict_icounters().await?;
    /// let key = DistkitRedisKey::try_from("connections".to_string())?;
    /// server_a.inc(&key, 3).await?;
    /// server_b.inc(&key, 7).await?;
    /// // Only server_a's slice is removed; server_b is unaffected.
    /// let (new_cumulative, removed) = server_a.del_on_instance(&key).await?;
    /// assert_eq!(removed, 3);
    /// assert_eq!(new_cumulative, 7); // server_b's slice remains
    /// # Ok(())
    /// # }
    /// ```
    pub async fn del_on_instance(&self, key: &DistkitRedisKey) -> Result<(i64, i64), DistkitError> {
        self.activity.signal();

        let mut conn = self.connection_manager.clone();
        let local_epoch = self.get_local_epoch(key);

        let (new_cumulative, removed_count, redis_epoch, _): (i64, i64, u64, i64) = self
            .del_on_instance_script
            .key(self.epoch_key())
            .key(self.instances_key())
            .key(self.cumulative_key())
            .key(self.keys_key())
            .key(self.inst_count_key())
            .arg(key.as_str())
            .arg(local_epoch)
            .arg(self.dead_instance_threshold_ms)
            .arg(self.prefix_str())
            .arg(&self.instance_id)
            .invoke_async(&mut conn)
            .await?;

        // No recovery: explicit removal intent; local_count becomes 0.
        self.update_local_store(key, redis_epoch, new_cumulative, 0);

        Ok((new_cumulative, removed_count))
    }

    /// Removes all keys and all instance state from Redis.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use distkit::DistkitRedisKey;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let counter = distkit::__doctest_helpers::strict_icounter().await?;
    /// let k1 = DistkitRedisKey::try_from("a".to_string())?;
    /// let k2 = DistkitRedisKey::try_from("b".to_string())?;
    /// counter.inc(&k1, 10).await?;
    /// counter.inc(&k2, 20).await?;
    /// counter.clear().await?;
    /// assert_eq!(counter.get(&k1).await?, (0, 0));
    /// assert_eq!(counter.get(&k2).await?, (0, 0));
    /// # Ok(())
    /// # }
    /// ```
    pub async fn clear(&self) -> Result<(), DistkitError> {
        self.activity.signal();

        let mut conn = self.connection_manager.clone();

        let _: () = self
            .clear_script
            .key(self.epoch_key())
            .key(self.instances_key())
            .key(self.cumulative_key())
            .key(self.keys_key())
            .arg(self.prefix_str())
            .invoke_async(&mut conn)
            .await?;

        self.local_store.clear();

        Ok(())
    }

    /// Removes this instance's contribution from every key without affecting other instances.
    pub async fn clear_on_instance(&self) -> Result<(), DistkitError> {
        self.activity.signal();

        let mut conn = self.connection_manager.clone();

        let _: () = self
            .clear_on_instance_script
            .key(self.epoch_key())
            .key(self.instances_key())
            .key(self.cumulative_key())
            .key(self.keys_key())
            .key(self.inst_count_key())
            .arg(self.dead_instance_threshold_ms)
            .arg(self.prefix_str())
            .arg(&self.instance_id)
            .invoke_async(&mut conn)
            .await?;

        // Zero out local_count for every tracked key; this instance no longer contributes.
        for entry in self.local_store.iter() {
            entry.local_count.store(0, Ordering::Release);
        }

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Trait impl
// ---------------------------------------------------------------------------

#[async_trait::async_trait]
impl InstanceAwareCounterTrait for StrictInstanceAwareCounter {
    fn instance_id(&self) -> &str {
        self.instance_id()
    }

    async fn inc(&self, key: &DistkitRedisKey, count: i64) -> Result<(i64, i64), DistkitError> {
        self.inc(key, count).await
    }

    async fn inc_if(
        &self,
        key: &DistkitRedisKey,
        comparator: CounterComparator,
        count: i64,
    ) -> Result<((i64, i64), (i64, i64)), DistkitError> {
        self.inc_if(key, comparator, count).await
    }

    async fn dec(&self, key: &DistkitRedisKey, count: i64) -> Result<(i64, i64), DistkitError> {
        self.inc(key, -count).await
    }

    async fn set(&self, key: &DistkitRedisKey, count: i64) -> Result<(i64, i64), DistkitError> {
        self.set(key, count).await
    }

    async fn set_if(
        &self,
        key: &DistkitRedisKey,
        comparator: CounterComparator,
        count: i64,
    ) -> Result<((i64, i64), (i64, i64)), DistkitError> {
        self.set_if(key, comparator, count).await
    }

    async fn set_on_instance(
        &self,
        key: &DistkitRedisKey,
        count: i64,
    ) -> Result<(i64, i64), DistkitError> {
        self.set_on_instance(key, count).await
    }

    async fn set_on_instance_if(
        &self,
        key: &DistkitRedisKey,
        comparator: CounterComparator,
        count: i64,
    ) -> Result<((i64, i64), (i64, i64)), DistkitError> {
        self.set_on_instance_if(key, comparator, count).await
    }

    async fn get(&self, key: &DistkitRedisKey) -> Result<(i64, i64), DistkitError> {
        self.get(key).await
    }

    async fn del(&self, key: &DistkitRedisKey) -> Result<(i64, i64), DistkitError> {
        self.del(key).await
    }

    async fn del_on_instance(&self, key: &DistkitRedisKey) -> Result<(i64, i64), DistkitError> {
        self.del_on_instance(key).await
    }

    async fn clear(&self) -> Result<(), DistkitError> {
        self.clear().await
    }

    async fn clear_on_instance(&self) -> Result<(), DistkitError> {
        self.clear_on_instance().await
    }

    async fn get_all<'k>(
        &self,
        keys: &[&'k DistkitRedisKey],
    ) -> Result<Vec<(&'k DistkitRedisKey, i64, i64)>, DistkitError> {
        self.get_batch(keys).await
    }

    async fn get_all_on_instance<'k>(
        &self,
        keys: &[&'k DistkitRedisKey],
    ) -> Result<Vec<(&'k DistkitRedisKey, i64)>, DistkitError> {
        let pairs = self.get_batch(keys).await?;
        Ok(pairs.into_iter().map(|(k, _, inst)| (k, inst)).collect())
    }

    async fn inc_all<'k>(
        &self,
        updates: &[(&'k DistkitRedisKey, i64)],
    ) -> Result<Vec<(&'k DistkitRedisKey, i64, i64)>, DistkitError> {
        let conditional_updates: Vec<(&DistkitRedisKey, CounterComparator, i64)> = updates
            .iter()
            .map(|(key, count)| (*key, CounterComparator::Nil, *count))
            .collect();

        Ok(self
            .inc_all_if(&conditional_updates)
            .await?
            .into_iter()
            .map(|(key, new_state, _)| (key, new_state.0, new_state.1))
            .collect())
    }

    async fn inc_all_if<'k>(
        &self,
        updates: &[(&'k DistkitRedisKey, CounterComparator, i64)],
    ) -> Result<Vec<(&'k DistkitRedisKey, (i64, i64), (i64, i64))>, DistkitError> {
        Ok(self
            .inc_if_batch(updates)
            .await?
            .into_iter()
            .map(
                |(key, cumulative, inst_count, old_cumulative, old_inst_count)| {
                    (
                        key,
                        (cumulative, inst_count),
                        (old_cumulative, old_inst_count),
                    )
                },
            )
            .collect())
    }

    async fn set_all<'k>(
        &self,
        updates: &[(&'k DistkitRedisKey, i64)],
    ) -> Result<Vec<(&'k DistkitRedisKey, i64, i64)>, DistkitError> {
        self.set_batch(updates).await
    }

    async fn set_all_if<'k>(
        &self,
        updates: &[(&'k DistkitRedisKey, CounterComparator, i64)],
    ) -> Result<Vec<(&'k DistkitRedisKey, (i64, i64), (i64, i64))>, DistkitError> {
        Ok(self
            .set_if_batch(updates)
            .await?
            .into_iter()
            .map(
                |(key, cumulative, inst_count, old_cumulative, old_inst_count)| {
                    (
                        key,
                        (cumulative, inst_count),
                        (old_cumulative, old_inst_count),
                    )
                },
            )
            .collect())
    }

    async fn set_all_on_instance<'k>(
        &self,
        updates: &[(&'k DistkitRedisKey, i64)],
    ) -> Result<Vec<(&'k DistkitRedisKey, i64, i64)>, DistkitError> {
        self.set_on_instance_batch(updates).await
    }

    async fn set_all_on_instance_if<'k>(
        &self,
        updates: &[(&'k DistkitRedisKey, CounterComparator, i64)],
    ) -> Result<Vec<(&'k DistkitRedisKey, (i64, i64), (i64, i64))>, DistkitError> {
        Ok(self
            .set_on_instance_if_batch(updates)
            .await?
            .into_iter()
            .map(
                |(key, cumulative, inst_count, old_cumulative, old_inst_count)| {
                    (
                        key,
                        (cumulative, inst_count),
                        (old_cumulative, old_inst_count),
                    )
                },
            )
            .collect())
    }
}
