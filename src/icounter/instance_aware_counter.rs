use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use dashmap::DashMap;
use redis::{Script, aio::ConnectionManager};

use crate::{
    ActivityTracker, EPOCH_CHANGE_INTERVAL, RedisKey, RedisKeyGenerator, RedisKeyGeneratorTypeKey,
    error::DistkitError, icounter::generate_instance_id,
};
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
#[derive(Debug, Clone)]
pub struct InstanceAwareCounterOptions {
    /// Redis key prefix used to namespace all counter keys.
    pub prefix: RedisKey,
    /// Redis connection manager.
    pub connection_manager: ConnectionManager,
    /// Milliseconds without a heartbeat before an instance is considered dead.
    /// Default: 30 000.
    pub dead_instance_threshold_ms: u64,
}

impl InstanceAwareCounterOptions {
    /// Creates options with a default `dead_instance_threshold_ms` of 30 000 ms.
    pub fn new(prefix: RedisKey, connection_manager: ConnectionManager) -> Self {
        Self {
            prefix,
            connection_manager,
            dead_instance_threshold_ms: 30_000,
        }
    }
}
#[derive(Debug)]
pub struct InstanceAwareCounter {
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
}

impl InstanceAwareCounter {
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
    pub fn instance_id(&self) -> &str {
        &self.instance_id
    }
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

}
