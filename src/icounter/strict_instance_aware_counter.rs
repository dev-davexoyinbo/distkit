//! Instance-aware distributed counter backed by Redis.
//!
//! Each [`InstanceAwareCounter`] represents one process/instance. Multiple
//! instances sharing the same Redis prefix each maintain their own count per
//! key, contributing to a shared cumulative total. When an instance stops
//! sending heartbeats, its contribution is automatically removed.

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use dashmap::DashMap;
use redis::{Script, aio::ConnectionManager};

use crate::{
    ActivityTracker, EPOCH_CHANGE_INTERVAL, RedisKey, RedisKeyGenerator, RedisKeyGeneratorTypeKey,
    error::DistkitError,
    icounter::{InstanceAwareCounterTrait, generate_instance_id},
};

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
local delta          = tonumber(ARGV[2])
local local_epoch    = tonumber(ARGV[3])
local dead_threshold = tonumber(ARGV[4])
local prefix         = ARGV[5]
local instance_id    = ARGV[6]

local ts = now_ms()

redis.call('ZADD', instances_key, ts, instance_id)
delete_dead_instances(prefix, instances_key, cumulative_key, keys_key, dead_threshold, ts, instance_id)

local redis_epoch = tonumber(redis.call('HGET', epoch_key, counter_key) or 0) or 0
local is_stale    = (local_epoch ~= redis_epoch)

if is_stale then
    local old_count = tonumber(redis.call('HGET', inst_count_key, counter_key) or 0) or 0
    redis.call('HSET', inst_count_key, counter_key, delta)
else
    redis.call('HINCRBY', inst_count_key, counter_key, delta)
end

local new_cumulative = tonumber(redis.call('HINCRBY', cumulative_key, counter_key, delta))
redis.call('SADD', keys_key, counter_key)

return {new_cumulative, redis_epoch}
"#;

const SET_LUA: &str = r#"
local epoch_key      = KEYS[1]
local instances_key  = KEYS[2]
local cumulative_key = KEYS[3]
local keys_key       = KEYS[4]
local inst_count_key = KEYS[5]

local counter_key    = ARGV[1]
local count          = tonumber(ARGV[2])
local local_epoch    = tonumber(ARGV[3])
local dead_threshold = tonumber(ARGV[4])
local prefix         = ARGV[5]
local instance_id    = ARGV[6]

local ts = now_ms()
redis.call('ZADD', instances_key, ts, instance_id)
delete_dead_instances(prefix, instances_key, cumulative_key, keys_key, dead_threshold, ts, instance_id)

local old_epoch = tonumber(redis.call('HGET', epoch_key, counter_key) or 0) or 0
local new_epoch = old_epoch + 1

redis.call('HSET', epoch_key,      counter_key, new_epoch)
redis.call('HSET', cumulative_key, counter_key, count)
redis.call('HSET', inst_count_key, counter_key, count)
redis.call('SADD', keys_key,       counter_key)

return {count, new_epoch}
"#;

const SET_ON_INSTANCE_LUA: &str = r#"
local epoch_key      = KEYS[1]
local instances_key  = KEYS[2]
local cumulative_key = KEYS[3]
local keys_key       = KEYS[4]
local inst_count_key = KEYS[5]

local counter_key    = ARGV[1]
local count          = tonumber(ARGV[2])
local local_epoch    = tonumber(ARGV[3])
local dead_threshold = tonumber(ARGV[4])
local prefix         = ARGV[5]
local instance_id    = ARGV[6]

local ts = now_ms()
redis.call('ZADD', instances_key, ts, instance_id)
delete_dead_instances(prefix, instances_key, cumulative_key, keys_key, dead_threshold, ts, instance_id)

local redis_epoch = tonumber(redis.call('HGET', epoch_key, counter_key) or 0) or 0
local inst_count  = tonumber(redis.call('HGET', inst_count_key, counter_key) or 0) or 0
local is_stale    = (local_epoch ~= redis_epoch)

local effective_old = is_stale and 0 or inst_count
local delta = count - effective_old

redis.call('HSET', inst_count_key, counter_key, count)
local new_cumulative = tonumber(redis.call('HINCRBY', cumulative_key, counter_key, delta))
redis.call('SADD', keys_key, counter_key)

return {new_cumulative, count, redis_epoch}
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
redis.call('ZADD', instances_key, ts, instance_id)
delete_dead_instances(prefix, instances_key, cumulative_key, keys_key, dead_threshold, ts, instance_id)

local redis_epoch = tonumber(redis.call('HGET', epoch_key, counter_key) or 0) or 0
local cumulative  = tonumber(redis.call('HGET', cumulative_key, counter_key) or 0) or 0
local inst_count  = tonumber(redis.call('HGET', inst_count_key, counter_key) or 0) or 0

return {cumulative, inst_count, redis_epoch}
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

local ts = now_ms()
redis.call('ZADD', instances_key, ts, instance_id)
delete_dead_instances(prefix, instances_key, cumulative_key, keys_key, dead_threshold, ts, instance_id)

local old_cumulative = tonumber(redis.call('HGET', cumulative_key, counter_key) or 0) or 0
local old_epoch = tonumber(redis.call('HGET', epoch_key, counter_key) or 0) or 0
local new_epoch = old_epoch + 1

redis.call('HSET', epoch_key,      counter_key, new_epoch)
redis.call('HDEL', cumulative_key, counter_key)
redis.call('SREM', keys_key,       counter_key)
redis.call('HDEL', inst_count_key, counter_key)

return {old_cumulative, new_epoch}
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
redis.call('ZADD', instances_key, ts, instance_id)
delete_dead_instances(prefix, instances_key, cumulative_key, keys_key, dead_threshold, ts, instance_id)

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

return {new_cumulative, inst_count, redis_epoch}
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
redis.call('ZADD', instances_key, ts, instance_id)
delete_dead_instances(prefix, instances_key, cumulative_key, keys_key, dead_threshold, ts, instance_id)

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
redis.call('ZADD', instances_key, ts, instance_id)
delete_dead_instances(prefix, instances_key, cumulative_key, keys_key, dead_threshold, ts, instance_id)
"#;

// ---------------------------------------------------------------------------
// Options
// ---------------------------------------------------------------------------

/// Options for constructing an [`InstanceAwareCounter`].
#[derive(Debug, Clone)]
pub struct StrictInstanceAwareCounterOptions {
    /// Redis key prefix used to namespace all counter keys.
    pub prefix: RedisKey,
    /// Redis connection manager.
    pub connection_manager: ConnectionManager,
    /// Milliseconds without a heartbeat before an instance is considered dead.
    /// Default: 30 000.
    pub dead_instance_threshold_ms: u64,
}

impl StrictInstanceAwareCounterOptions {
    /// Creates options with a default `dead_instance_threshold_ms` of 30 000 ms.
    pub fn new(prefix: RedisKey, connection_manager: ConnectionManager) -> Self {
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

/// Instance-aware distributed counter backed by Redis.
///
/// Each `InstanceAwareCounter` represents one running instance. Multiple
/// instances sharing the same Redis prefix each maintain their own per-key
/// count; the cumulative total is the sum of all live instances' counts.
///
/// When an instance stops heartbeating (no call for longer than
/// `dead_instance_threshold_ms`), its contribution is automatically subtracted
/// from the cumulative on the next operation by any surviving instance.
///
/// Construct via [`InstanceAwareCounter::new`], which returns an `Arc<Self>`.
#[derive(Debug)]
pub struct StrictInstanceAwareCounter {
    connection_manager: ConnectionManager,
    key_generator: RedisKeyGenerator,
    instance_id: String,
    dead_instance_threshold_ms: u64,
    /// Per-key epoch last seen in Redis. Sent as ARGV to Lua scripts.
    local_epochs: DashMap<RedisKey, AtomicU64>,
    inc_script: Script,
    set_script: Script,
    set_on_instance_script: Script,
    get_script: Script,
    del_script: Script,
    del_on_instance_script: Script,
    clear_script: Script,
    clear_on_instance_script: Script,
    mark_alive_script: Script,
    activity: Arc<ActivityTracker>,
}

impl StrictInstanceAwareCounter {
    /// Creates a new instance-aware counter and spawns its background heartbeat task.
    pub fn new(options: StrictInstanceAwareCounterOptions) -> Arc<Self> {
        let StrictInstanceAwareCounterOptions {
            prefix,
            connection_manager,
            dead_instance_threshold_ms,
        } = options;

        let key_generator =
            RedisKeyGenerator::new(prefix, RedisKeyGeneratorTypeKey::InstanceAwareCounter);
        let instance_id = generate_instance_id();

        let counter = Arc::new(Self {
            connection_manager,
            key_generator,
            instance_id,
            dead_instance_threshold_ms,
            local_epochs: DashMap::default(),
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
            activity: ActivityTracker::new(EPOCH_CHANGE_INTERVAL),
        });

        counter.run_heartbeat_task();

        counter
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
    // Epoch helpers
    // -----------------------------------------------------------------------

    fn get_local_epoch(&self, key: &RedisKey) -> u64 {
        self.local_epochs
            .get(key)
            .map(|e| e.load(Ordering::Acquire))
            .unwrap_or(0)
    }

    fn set_local_epoch(&self, key: &RedisKey, epoch: u64) {
        match self.local_epochs.get(key) {
            Some(e) => e.store(epoch, Ordering::Release),
            None => {
                self.local_epochs
                    .entry(key.clone())
                    .and_modify(|e| e.store(epoch, Ordering::Relaxed))
                    .or_insert_with(|| AtomicU64::new(epoch));
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

    async fn mark_alive(&self) -> Result<(), DistkitError> {
        let mut conn = self.connection_manager.clone();

        let _: () = self
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

        Ok(())
    }

    // -----------------------------------------------------------------------
    // Public API
    // -----------------------------------------------------------------------

    /// Returns this instance's unique identifier.
    pub fn instance_id(&self) -> &str {
        &self.instance_id
    }

    /// Increments (or decrements if negative) the counter for `key` by `count`.
    ///
    /// Stale-aware: if the local epoch is behind Redis, this instance's stored
    /// count is reset to `count` before incrementing the cumulative.
    ///
    /// Returns the new cumulative total.
    pub async fn inc(&self, key: &RedisKey, count: i64) -> Result<i64, DistkitError> {
        self.activity.signal();

        let mut conn = self.connection_manager.clone();
        let local_epoch = self.get_local_epoch(key);

        let result: Vec<i64> = self
            .inc_script
            .key(self.epoch_key())
            .key(self.instances_key())
            .key(self.cumulative_key())
            .key(self.keys_key())
            .key(self.inst_count_key())
            .arg(key.as_str())
            .arg(count)
            .arg(local_epoch)
            .arg(self.dead_instance_threshold_ms)
            .arg(self.prefix_str())
            .arg(&self.instance_id)
            .invoke_async(&mut conn)
            .await?;

        let cumulative = result[0];
        let redis_epoch = result[1] as u64;
        self.set_local_epoch(key, redis_epoch);

        Ok(cumulative)
    }

    /// Sets the cumulative total for `key` to `count`, bumping the epoch.
    ///
    /// This is a global operation: all other instances will detect the epoch
    /// change on their next operation and treat their stored counts as stale.
    ///
    /// Returns the new cumulative total (equal to `count`).
    pub async fn set(&self, key: &RedisKey, count: i64) -> Result<i64, DistkitError> {
        self.activity.signal();

        let mut conn = self.connection_manager.clone();
        let local_epoch = self.get_local_epoch(key);

        let result: Vec<i64> = self
            .set_script
            .key(self.epoch_key())
            .key(self.instances_key())
            .key(self.cumulative_key())
            .key(self.keys_key())
            .key(self.inst_count_key())
            .arg(key.as_str())
            .arg(count)
            .arg(local_epoch)
            .arg(self.dead_instance_threshold_ms)
            .arg(self.prefix_str())
            .arg(&self.instance_id)
            .invoke_async(&mut conn)
            .await?;

        let cumulative = result[0];
        let new_epoch = result[1] as u64;
        self.set_local_epoch(key, new_epoch);

        Ok(cumulative)
    }

    /// Sets only this instance's contribution for `key` to `count`, without bumping the epoch.
    ///
    /// If this instance's stored count is stale (epoch mismatch), treats the
    /// old contribution as 0 when computing the delta applied to the cumulative.
    ///
    /// Returns `(new_cumulative, instance_count)`.
    pub async fn set_on_instance(
        &self,
        key: &RedisKey,
        count: i64,
    ) -> Result<(i64, i64), DistkitError> {
        self.activity.signal();

        let mut conn = self.connection_manager.clone();
        let local_epoch = self.get_local_epoch(key);

        let result: Vec<i64> = self
            .set_on_instance_script
            .key(self.epoch_key())
            .key(self.instances_key())
            .key(self.cumulative_key())
            .key(self.keys_key())
            .key(self.inst_count_key())
            .arg(key.as_str())
            .arg(count)
            .arg(local_epoch)
            .arg(self.dead_instance_threshold_ms)
            .arg(self.prefix_str())
            .arg(&self.instance_id)
            .invoke_async(&mut conn)
            .await?;

        let cumulative = result[0];
        let inst_count = result[1];
        let redis_epoch = result[2] as u64;
        self.set_local_epoch(key, redis_epoch);

        Ok((cumulative, inst_count))
    }

    /// Returns `(cumulative, instance_count)` for `key`.
    ///
    /// Also triggers dead-instance cleanup and updates the liveness ZSET for
    /// this instance.
    pub async fn get(&self, key: &RedisKey) -> Result<(i64, i64), DistkitError> {
        self.activity.signal();

        let mut conn = self.connection_manager.clone();
        let local_epoch = self.get_local_epoch(key);

        let result: Vec<i64> = self
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

        let cumulative = result[0];
        let inst_count = result[1];
        let redis_epoch = result[2] as u64;
        self.set_local_epoch(key, redis_epoch);

        Ok((cumulative, inst_count))
    }

    /// Deletes `key` globally, bumping the epoch to invalidate all instances.
    ///
    /// Returns the cumulative total before deletion.
    pub async fn del(&self, key: &RedisKey) -> Result<i64, DistkitError> {
        self.activity.signal();

        let mut conn = self.connection_manager.clone();
        let local_epoch = self.get_local_epoch(key);

        let result: Vec<i64> = self
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
            .invoke_async(&mut conn)
            .await?;

        let old_cumulative = result[0];
        let new_epoch = result[1] as u64;
        self.set_local_epoch(key, new_epoch);

        Ok(old_cumulative)
    }

    /// Removes only this instance's contribution for `key`, without bumping the epoch.
    ///
    /// Returns `(new_cumulative, removed_count)`.
    pub async fn del_on_instance(&self, key: &RedisKey) -> Result<(i64, i64), DistkitError> {
        self.activity.signal();

        let mut conn = self.connection_manager.clone();
        let local_epoch = self.get_local_epoch(key);

        let result: Vec<i64> = self
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

        let new_cumulative = result[0];
        let removed_count = result[1];
        let redis_epoch = result[2] as u64;
        self.set_local_epoch(key, redis_epoch);

        Ok((new_cumulative, removed_count))
    }

    /// Clears all keys and all instance state from Redis, then clears the
    /// local epoch map.
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

        self.local_epochs.clear();

        Ok(())
    }

    /// Removes only this instance's contributions for all keys from the
    /// cumulative totals, without affecting other instances.
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

    async fn inc(&self, key: &RedisKey, count: i64) -> Result<i64, DistkitError> {
        self.inc(key, count).await
    }

    async fn set(&self, key: &RedisKey, count: i64) -> Result<i64, DistkitError> {
        self.set(key, count).await
    }

    async fn set_on_instance(
        &self,
        key: &RedisKey,
        count: i64,
    ) -> Result<(i64, i64), DistkitError> {
        self.set_on_instance(key, count).await
    }

    async fn get(&self, key: &RedisKey) -> Result<(i64, i64), DistkitError> {
        self.get(key).await
    }

    async fn del(&self, key: &RedisKey) -> Result<i64, DistkitError> {
        self.del(key).await
    }

    async fn del_on_instance(&self, key: &RedisKey) -> Result<(i64, i64), DistkitError> {
        self.del_on_instance(key).await
    }

    async fn clear(&self) -> Result<(), DistkitError> {
        self.clear().await
    }

    async fn clear_on_instance(&self) -> Result<(), DistkitError> {
        self.clear_on_instance().await
    }
}
