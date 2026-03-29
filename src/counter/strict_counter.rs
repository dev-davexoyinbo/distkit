use redis::{Script, aio::ConnectionManager};

use crate::{
    DistkitError, RedisKey, RedisKeyGenerator, RedisKeyGeneratorTypeKey,
    counter::{CounterOptions, CounterTrait},
};

const INC_LUA: &str = r#"
    local container_key = KEYS[1]
    local key = KEYS[2]
    local count = tonumber(ARGV[1]) or 0

    return redis.call('HINCRBY', container_key, key, count)
"#;

const SET_LUA: &str = r#"
    local container_key = KEYS[1]
    local key = KEYS[2]
    local count = tonumber(ARGV[1]) or 0

    redis.call('HSET', container_key, key, count)

    return count
"#;

const GET_LUA: &str = r#"
    local container_key = KEYS[1]
    local key = KEYS[2]

    return redis.call('HGET', container_key, key) or 0
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
    pub fn new(options: CounterOptions) -> Self {
        let CounterOptions {
            prefix,
            connection_manager,
            ..
        } = options;

        let key_generator = RedisKeyGenerator::new(prefix, RedisKeyGeneratorTypeKey::StrictCounter);
        let inc_script = Script::new(INC_LUA);
        let get_script = Script::new(GET_LUA);
        let set_script = Script::new(SET_LUA);
        let del_script = Script::new(DEL_LUA);
        let clear_script = Script::new(CLEAR_LUA);

        Self {
            connection_manager,
            key_generator,
            inc_script,
            get_script,
            set_script,
            del_script,
            clear_script,
        }
    }
}

#[async_trait::async_trait]
impl CounterTrait for StrictCounter {
    async fn inc(&self, key: &RedisKey, count: i64) -> Result<i64, DistkitError> {
        let mut conn = self.connection_manager.clone();

        let total: i64 = self
            .inc_script
            .key(self.key_generator.container_key())
            .key(key.to_string())
            .arg(count)
            .invoke_async(&mut conn)
            .await?;

        Ok(total)
    } // end function inc

    async fn dec(&self, key: &RedisKey, count: i64) -> Result<i64, DistkitError> {
        self.inc(key, -count).await
    } // end function dec

    async fn get(&self, key: &RedisKey) -> Result<i64, DistkitError> {
        let mut conn = self.connection_manager.clone();

        let total: i64 = self
            .get_script
            .key(self.key_generator.container_key())
            .key(key.to_string())
            .invoke_async(&mut conn)
            .await?;

        Ok(total)
    } // end function get

    async fn set(&self, key: &RedisKey, count: i64) -> Result<i64, DistkitError> {
        let mut conn = self.connection_manager.clone();

        let total: i64 = self
            .set_script
            .key(self.key_generator.container_key())
            .key(key.to_string())
            .arg(count)
            .invoke_async(&mut conn)
            .await?;

        Ok(total)
    } // end function set

    async fn del(&self, key: &RedisKey) -> Result<i64, DistkitError> {
        let mut conn = self.connection_manager.clone();

        let total: i64 = self
            .del_script
            .key(self.key_generator.container_key())
            .key(key.to_string())
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
} // end impl CounterTrait for StrictCounter
