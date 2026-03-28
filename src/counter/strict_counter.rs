use crate::RedisKey;

#[derive(Debug, Clone)]
pub struct StrictCounter {
    prefix: RedisKey,
}

impl StrictCounter {
    pub fn new(prefix: RedisKey) -> Self {
        Self { prefix }
    }
}
