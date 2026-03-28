use redis::aio::ConnectionManager;

use crate::{
    DistkitError, RedisKey, RedisKeyGenerator, RedisKeyGeneratorTypeKey,
    counter::{CounterError, CounterTrait},
};

#[derive(Debug, Clone)]
pub struct LaxCounter {
    prefix: RedisKey,
    connection_manager: ConnectionManager,
    key_generator: RedisKeyGenerator,
}

impl LaxCounter {
    pub fn new(prefix: RedisKey, connection_manager: ConnectionManager) -> Self {
        let key_generator =
            RedisKeyGenerator::new(prefix.clone(), RedisKeyGeneratorTypeKey::LaxCounter);

        Self {
            prefix,
            connection_manager,
            key_generator,
        }
    }
}

#[async_trait::async_trait]
impl CounterTrait for LaxCounter {
    async fn inc(&self, key: &RedisKey, count: i64) -> Result<i64, DistkitError> {
        todo!()
    } // end function inc

    async fn dec(&self, key: &RedisKey, count: i64) -> Result<i64, DistkitError> {
        todo!()
    } // end function dec

    async fn get(&self, key: &RedisKey) -> Result<i64, DistkitError> {
        todo!()
    } // end function get

    async fn set(&self, key: &RedisKey, count: i64) -> Result<i64, DistkitError> {
        todo!()
    } // end function set

    async fn del(&self, key: &RedisKey) -> Result<i64, DistkitError> {
        todo!()
    } // end function delete

    async fn clear(&self) -> Result<(), DistkitError> {
        todo!()
    }
} // end impl CounterTrait for LaxCounter
