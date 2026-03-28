use redis::{Script, aio::ConnectionManager};

use crate::{
    RedisKey, RedisKeyGenerator, RedisKeyGeneratorTypeKey,
    counter::{CounterError, CounterTrait},
};

const INC_LUA: &str = r#"
    local key = KEYS[1]
    local count = ARGV[1]

    if redis.call('exists', key) == 0 then
        redis.call('set', key, count)
    else
        redis.call('incrby', key, count)
    end
"#;

#[derive(Debug, Clone)]
pub struct StrictCounter {
    prefix: RedisKey,
    connection_manager: ConnectionManager,
    key_generator: RedisKeyGenerator,
    inc_script: Script,
}

impl StrictCounter {
    pub fn new(prefix: RedisKey, connection_manager: ConnectionManager) -> Self {
        let key_generator =
            RedisKeyGenerator::new(prefix.clone(), RedisKeyGeneratorTypeKey::StrictCounter);
        let inc_script = Script::new(INC_LUA);

        Self {
            prefix,
            connection_manager,
            key_generator,
            inc_script,
        }
    }
}

#[async_trait::async_trait]
impl CounterTrait for StrictCounter {
    async fn inc(&self, key: &RedisKey, count: i64) -> Result<(), CounterError> {
        todo!()
    } // end function inc

    async fn dec(&self, key: &RedisKey, count: i64) -> Result<(), CounterError> {
        todo!()
    } // end function dec

    async fn get(&self, key: &RedisKey) -> Result<i64, CounterError> {
        todo!()
    } // end function get
}
