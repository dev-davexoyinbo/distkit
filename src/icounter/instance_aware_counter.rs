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
