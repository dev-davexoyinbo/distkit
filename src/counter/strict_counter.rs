use redis::{Script, aio::ConnectionManager};

use crate::{
    DistkitError, RedisKey, RedisKeyGenerator, RedisKeyGeneratorTypeKey, counter::CounterTrait,
};

const INC_LUA: &str = r#"
    local key = KEYS[1]
    local count = tonumber(ARGV[1]) or 0
    local total = 0

    if count == 0 then
        total = redis.call('GET', key) or 0
    elseif count > 0 then
        total = redis.call('INCRBY', key, count)
    else
        total = redis.call('DECRBY', key, count * -1)
    end

    return total
"#;

const SET_LUA: &str = r#"
    local key = KEYS[1]
    local count = tonumber(ARGV[1]) or 0
    redis.call('SET', key, count)

    return count
"#;

const GET_LUA: &str = r#"
    local key = KEYS[1]
    return redis.call('GET', key) or 0
"#;

const DEL_LUA: &str = r#"
    local key = KEYS[1]
    local total = redis.call('GET', key) or 0
    redis.call('DEL', key)

    return total
"#;

const CLEAR_LUA: &str = r#"
    local key = KEYS[1]
    redis.call('DEL', key)
"#;

#[derive(Debug, Clone)]
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
    pub fn new(prefix: RedisKey, connection_manager: ConnectionManager) -> Self {
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
            .key(self.key_generator.member_key(key))
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
            .key(self.key_generator.member_key(key))
            .invoke_async(&mut conn)
            .await?;

        Ok(total)
    } // end function get

    async fn set(&self, key: &RedisKey, count: i64) -> Result<i64, DistkitError> {
        let mut conn = self.connection_manager.clone();

        let total: i64 = self
            .set_script
            .key(self.key_generator.member_key(key))
            .arg(count)
            .invoke_async(&mut conn)
            .await?;

        Ok(total)
    } // end function set

    async fn del(&self, key: &RedisKey) -> Result<i64, DistkitError> {
        let mut conn = self.connection_manager.clone();

        let total: i64 = self
            .del_script
            .key(self.key_generator.member_key(key))
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
