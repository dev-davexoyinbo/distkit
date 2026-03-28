use crate::RedisKey;

#[derive(Debug, Clone)]
pub struct LaxCounter {
    prefix: RedisKey,
}

impl LaxCounter {
    pub fn new(prefix: RedisKey) -> Self {
        Self { prefix }
    }
}
