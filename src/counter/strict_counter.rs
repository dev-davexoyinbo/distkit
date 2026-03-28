use redis::aio::ConnectionManager;

use crate::{
    RedisKey, RedisKeyGenerator, RedisKeyGeneratorTypeKey,
    counter::{CounterError, CounterTrait},
};

#[derive(Debug, Clone)]
pub struct StrictCounter {
    prefix: RedisKey,
    connection_manager: ConnectionManager,
    key_generator: RedisKeyGenerator,
}

impl StrictCounter {
    pub fn new(prefix: RedisKey, connection_manager: ConnectionManager) -> Self {
        let key_generator =
            RedisKeyGenerator::new(prefix.clone(), RedisKeyGeneratorTypeKey::StrictCounter);
        Self {
            prefix,
            connection_manager,
            key_generator,
        }
    }
}

#[async_trait::async_trait]
impl CounterTrait for StrictCounter {
    async fn inc(&self, key: String, count: i64) -> Result<(), CounterError> {
        todo!()
    } // end function inc

    async fn dec(&self, key: String, count: i64) -> Result<(), CounterError> {
        todo!()
    } // end function dec

    async fn get(&self, key: String) -> Result<i64, CounterError> {
        todo!()
    } // end function get
}
