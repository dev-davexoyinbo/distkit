use std::{collections::HashMap, sync::Arc};

use redis::{Script, aio::ConnectionManager};

use crate::{
    CounterComparator, DistkitError, DistkitRedisKey, RedisKeyGenerator, RedisKeyGeneratorTypeKey,
    counter::{CounterOptions, CounterTrait},
    execute_pipeline_with_script_retry,
};

const HELPER_LUA: &str = r#"
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

const INC_LUA: &str = r#"
    local container_key = KEYS[1]
    local key = KEYS[2]
    local comparator = ARGV[1]
    local compare_against = tonumber(ARGV[2]) or 0
    local count = tonumber(ARGV[3]) or 0

    local current = tonumber(redis.call('HGET', container_key, key)) or 0
    if not compare_values(current, comparator, compare_against) then
        return {current, current}
    end

    return {tonumber(redis.call('HINCRBY', container_key, key, count)), current}
"#;

const SET_LUA: &str = r#"
    local container_key = KEYS[1]
    local key = KEYS[2]
    local comparator = ARGV[1]
    local compare_against = tonumber(ARGV[2]) or 0
    local count = tonumber(ARGV[3]) or 0

    local current = tonumber(redis.call('HGET', container_key, key)) or 0
    if not compare_values(current, comparator, compare_against) then
        return {key, current, current}
    end

    redis.call('HSET', container_key, key, count)
    return {key, count, current}
"#;

const GET_LUA: &str = r#"
    local container_key = KEYS[1]
    local key = KEYS[2]

    return {key, tonumber(redis.call('HGET', container_key, key)) or 0}
"#;

const DEL_LUA: &str = r#"
    local container_key = KEYS[1]
    local key = KEYS[2]

    local total = redis.call('HGET', container_key, key) or 0

    redis.call('HDEL', container_key, key)

    return total
"#;

const CLEAR_LUA: &str = r#"
    local container_key = KEYS[1]
    redis.call('DEL', container_key)
"#;

/// Immediately consistent counter backed by Redis Lua scripts.
///
/// Every operation executes a single atomic Lua script against a Redis hash,
/// guaranteeing that reads always reflect the latest write. Latency is
/// dominated by the network round-trip to Redis.
#[derive(Debug)]
pub struct StrictCounter {
    connection_manager: ConnectionManager,
    key_generator: RedisKeyGenerator,
    inc_script: Script,
    get_script: Script,
    set_script: Script,
    del_script: Script,
    clear_script: Script,
}

impl StrictCounter {
    /// Creates a new strict counter from the given options.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use distkit::{DistkitRedisKey, counter::{StrictCounter, CounterOptions}};
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let redis_url = std::env::var("REDIS_URL")
    ///     .unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());
    /// let client = redis::Client::open(redis_url)?;
    /// let conn = client.get_connection_manager().await?;
    /// let prefix = DistkitRedisKey::try_from("my_app".to_string())?;
    /// let counter = StrictCounter::new(CounterOptions::new(prefix, conn));
    /// # Ok(())
    /// # }
    /// ```
    pub fn new(options: CounterOptions) -> Arc<Self> {
        let CounterOptions {
            prefix,
            connection_manager,
            ..
        } = options;

        let key_generator = RedisKeyGenerator::new(prefix, RedisKeyGeneratorTypeKey::Strict);
        let inc_script = Script::new(&format!("{HELPER_LUA}\n{INC_LUA}"));
        let get_script = Script::new(GET_LUA);
        let set_script = Script::new(&format!("{HELPER_LUA}\n{SET_LUA}"));
        let del_script = Script::new(DEL_LUA);
        let clear_script = Script::new(CLEAR_LUA);

        Arc::new(Self {
            connection_manager,
            key_generator,
            inc_script,
            get_script,
            set_script,
            del_script,
            clear_script,
        })
    }
}

#[async_trait::async_trait]
impl CounterTrait for StrictCounter {
    async fn inc(&self, key: &DistkitRedisKey, count: i64) -> Result<i64, DistkitError> {
        Ok(self.inc_if(key, CounterComparator::Nil, count).await?.0)
    }

    async fn inc_if(
        &self,
        key: &DistkitRedisKey,
        comparator: CounterComparator,
        count: i64,
    ) -> Result<(i64, i64), DistkitError> {
        let mut conn = self.connection_manager.clone();
        let (lua_comparator, compare_against) = comparator.as_lua_parts();

        let totals: (i64, i64) = self
            .inc_script
            .key(self.key_generator.container_key())
            .key(key.as_str())
            .arg(lua_comparator)
            .arg(compare_against)
            .arg(count)
            .invoke_async(&mut conn)
            .await?;

        Ok(totals)
    }

    async fn dec(&self, key: &DistkitRedisKey, count: i64) -> Result<i64, DistkitError> {
        self.inc(key, -count).await
    }

    async fn get(&self, key: &DistkitRedisKey) -> Result<i64, DistkitError> {
        let mut conn = self.connection_manager.clone();

        let (_, total): (String, i64) = self
            .get_script
            .key(self.key_generator.container_key())
            .key(key.as_str())
            .invoke_async(&mut conn)
            .await?;

        Ok(total)
    } // end function get

    async fn set(&self, key: &DistkitRedisKey, count: i64) -> Result<i64, DistkitError> {
        Ok(self.set_if(key, CounterComparator::Nil, count).await?.0)
    }

    async fn set_if(
        &self,
        key: &DistkitRedisKey,
        comparator: CounterComparator,
        count: i64,
    ) -> Result<(i64, i64), DistkitError> {
        let mut conn = self.connection_manager.clone();
        let (lua_comparator, compare_against) = comparator.as_lua_parts();

        let (_, total, old): (String, i64, i64) = self
            .set_script
            .key(self.key_generator.container_key())
            .key(key.as_str())
            .arg(lua_comparator)
            .arg(compare_against)
            .arg(count)
            .invoke_async(&mut conn)
            .await?;

        Ok((total, old))
    }

    async fn del(&self, key: &DistkitRedisKey) -> Result<i64, DistkitError> {
        let mut conn = self.connection_manager.clone();

        let total: i64 = self
            .del_script
            .key(self.key_generator.container_key())
            .key(key.as_str())
            .invoke_async(&mut conn)
            .await?;

        Ok(total)
    } // end function del

    async fn clear(&self) -> Result<(), DistkitError> {
        let mut conn = self.connection_manager.clone();

        let _: () = self
            .clear_script
            .key(self.key_generator.container_key())
            .invoke_async(&mut conn)
            .await?;

        Ok(())
    } // end function clear

    async fn get_all<'k>(
        &self,
        keys: &[&'k DistkitRedisKey],
    ) -> Result<Vec<(&'k DistkitRedisKey, i64)>, DistkitError> {
        if keys.is_empty() {
            return Ok(vec![]);
        }

        let mut conn = self.connection_manager.clone();
        let script = &self.get_script;

        let raw: Vec<(String, i64)> =
            execute_pipeline_with_script_retry(&mut conn, script, keys, |key| {
                let mut inv = script.key(self.key_generator.container_key());
                inv.key(key.as_str());
                inv
            })
            .await?;

        let map: HashMap<String, i64> = raw.into_iter().collect();

        Ok(keys
            .iter()
            .map(|k| (*k, map.get(k.as_str()).copied().unwrap_or(0)))
            .collect())
    } // end function get_all

    async fn inc_all<'k>(
        &self,
        updates: &[(&'k DistkitRedisKey, i64)],
    ) -> Result<Vec<(&'k DistkitRedisKey, i64)>, DistkitError> {
        let conditional_updates: Vec<(&DistkitRedisKey, CounterComparator, i64)> = updates
            .iter()
            .map(|(key, count)| (*key, CounterComparator::Nil, *count))
            .collect();

        Ok(self
            .inc_all_if(&conditional_updates)
            .await?
            .into_iter()
            .map(|(key, new, _)| (key, new))
            .collect())
    }

    async fn inc_all_if<'k>(
        &self,
        updates: &[(&'k DistkitRedisKey, CounterComparator, i64)],
    ) -> Result<Vec<(&'k DistkitRedisKey, i64, i64)>, DistkitError> {
        if updates.is_empty() {
            return Ok(vec![]);
        }

        let mut conn = self.connection_manager.clone();
        let script = &self.inc_script;

        let raw: Vec<(i64, i64)> =
            execute_pipeline_with_script_retry(&mut conn, script, updates, |update| {
                let (key, comparator, count) = update;
                let (lua_comparator, compare_against) = comparator.as_lua_parts();
                let mut inv = script.key(self.key_generator.container_key());
                inv.key(key.as_str());
                inv.arg(lua_comparator);
                inv.arg(compare_against);
                inv.arg(*count);
                inv
            })
            .await?;

        Ok(updates
            .iter()
            .zip(raw)
            .map(|((key, _, _), (new, old))| (*key, new, old))
            .collect())
    }

    async fn set_all<'k>(
        &self,
        updates: &[(&'k DistkitRedisKey, i64)],
    ) -> Result<Vec<(&'k DistkitRedisKey, i64)>, DistkitError> {
        let conditional_updates: Vec<(&DistkitRedisKey, CounterComparator, i64)> = updates
            .iter()
            .map(|(key, count)| (*key, CounterComparator::Nil, *count))
            .collect();

        Ok(self
            .set_all_if(&conditional_updates)
            .await?
            .into_iter()
            .map(|(key, new, _)| (key, new))
            .collect())
    }

    async fn set_all_if<'k>(
        &self,
        updates: &[(&'k DistkitRedisKey, CounterComparator, i64)],
    ) -> Result<Vec<(&'k DistkitRedisKey, i64, i64)>, DistkitError> {
        if updates.is_empty() {
            return Ok(vec![]);
        }

        let mut conn = self.connection_manager.clone();
        let script = &self.set_script;

        let raw: Vec<(String, i64, i64)> =
            execute_pipeline_with_script_retry(&mut conn, script, updates, |update| {
                let (key, comparator, count) = update;
                let (lua_comparator, compare_against) = comparator.as_lua_parts();
                let mut inv = script.key(self.key_generator.container_key());
                inv.key(key.as_str());
                inv.arg(lua_comparator);
                inv.arg(compare_against);
                inv.arg(*count);
                inv
            })
            .await?;

        let map: HashMap<String, (i64, i64)> = raw
            .into_iter()
            .map(|(key, new, old)| (key, (new, old)))
            .collect();

        Ok(updates
            .iter()
            .map(|(k, _, _)| {
                let (new, old) = map.get(k.as_str()).copied().unwrap_or((0, 0));
                (*k, new, old)
            })
            .collect())
    }
} // end impl CounterTrait for StrictCounter
