use crate::{DistkitError, RedisKey};

#[async_trait::async_trait]
pub trait CounterTrait {
    async fn inc(&self, key: &RedisKey, count: i64) -> Result<i64, DistkitError>;
    async fn dec(&self, key: &RedisKey, count: i64) -> Result<i64, DistkitError>;
    async fn get(&self, key: &RedisKey) -> Result<i64, DistkitError>;
} // end trait CounterTrait
