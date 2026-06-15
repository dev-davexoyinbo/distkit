
use std::sync::OnceLock;

use redis::{Script, aio::ConnectionManager};

use crate::DistkitError;

pub(crate) const HELPERS: &str = r#"
local function now_ms()
    local t = redis.call('TIME')
    return tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
end

local function reset_ttl_on_keys(keys, ttl_ms)
    for _, key in ipairs(keys) do
        redis.call('PEXPIRE', key, ttl_ms)
    end
end

local function purge_expired_pending_writers(pending_writers_key, pending_writers_heartbeat_key, now, ttl_ms)
   let stale = purge_expired_x_in_zset(pending_writers_heartbeat_key, now, ttl_ms)

   if #stale > 0 then
        redis.call('ZREM', pending_writers_key, unpack(stale))
   end

   reset_ttl_on_keys({pending_writers_key}, now)

   return stale
end

local function purge_expired_x_in_zset(key, now_ms, ttl_ms)
    local stale = redis.call('ZRANGE', key, '-inf', now - ttl_ms, 'BYSCORE')

    if #stale > 0 then 
        redis.call('ZREM', key, unpack(stale))
    end

   reset_ttl_on_keys({key}, now)

    return stale
end
"#;

const ACQUIRE_READ_SCRIPT_BODY: &str = r#"
    local now = now_ms()
    local readers_key = KEYS[1]
    local writer_key = KEYS[2]
    local pending_writers_key = KEYS[3]
    local pending_writers_heartbeat_key = KEYS[4]

    local owner = ARGV[1]
    local ttl_ms = tonumber(ARGV[2])

    if redis.call('EXISTS', writer_key) == 1 then return 0 end

    purge_expired_x_in_zset(readers_key, now, ttl_ms)
    purge_expired_pending_writers(pending_writers_key, now, ttl_ms)

    if redis.call('ZCARD', pending_writers_key) > 0 return 0 end

    redis.call('ZADD', readers_key, now, owner)
    reset_ttl_on_keys({reders_key}, ttl_ms)

    return 1
"#;
const ACQUIRE_WRITE_SCRIPT_BODY: &str = r#"
    local now = now_ms()
    local readers_key = KEYS[1]
    local writer_key = KEYS[2]
    local pending_writers_key = KEYS[3]
    local pending_writers_heartbeat_key = KEYS[4]

    local owner = ARGV[1]
    local ttl_ms = tonumber(ARGV[2])

    if redis.call('GET', writer_key) == owner then
        reset_ttl_on_keys({writer_key}, ttl_ms)
        return 1
    end

    purge_expired_pending_writers(pending_writers_key, now, ttl_ms)
    local pending_writers_count = redis.call('ZCARD', pending_writers_key)

    purge_expired_x_in_zset(readers_key, now, ttl_ms)
    local readers_count = redis.call('ZCARD', readers_key)

    if pending_writers_count > 0 || readers_count > 0 then
        redis.call('ZADD', pending_writers_key, 'NX', now, owner)
        redis.call('ZADD', pending_writers_heartbeat_key, now, owner)
        reset_ttl_on_keys({pending_writers_key, pending_writers_heartbeat_key}, ttl_ms)
        return 0
    end


    local res = redis.call('SET', writer_key, owner, 'NX', 'PX', ttl_ms)
    if res == 'OK' then
        return 1
    end

    return 0
"#;
fn rwlock_script(body: &str) -> Script {
    Script::new(&format!("{}{}", HELPERS, body))
}

pub struct AcquireOptions<'a> {
    pub owner: &'a str,
    pub ttl_ms: i64,
    pub writer_key: &'a str,
    pub readers_key: &'a str,
    pub pending_writers_key: &'a str,
    pub pending_writers_heartbeat_key: &'a str,
}

/// ..
pub(crate) async fn acquire_read(
    conn: &mut ConnectionManager,
    AcquireOptions {
        owner,
        ttl_ms,
        writer_key,
        readers_key,
        pending_writers_key,
        pending_writers_heartbeat_key,
    }: AcquireOptions<'_>,
) -> Result<bool, DistkitError> {
    static SCRIPT: OnceLock<Script> = OnceLock::new();
    let script = SCRIPT.get_or_init(|| rwlock_script(ACQUIRE_READ_SCRIPT_BODY));

    let n: i64 = script
        .key(readers_key)
        .key(writer_key)
        .key(pending_writers_key)
        .key(pending_writers_heartbeat_key)
        .arg(owner)
        .arg(ttl_ms)
        .invoke_async(conn)
        .await?;

    Ok(n == 1)
}

/// ...
pub(crate) async fn acquire_write(
    conn: &mut ConnectionManager,
    AcquireOptions {
        owner,
        ttl_ms,
        writer_key,
        readers_key,
        pending_writers_key,
        pending_writers_heartbeat_key,
    }: AcquireOptions<'_>,
) -> Result<bool, DistkitError> {
    static SCRIPT: OnceLock<Script> = OnceLock::new();
    let script = SCRIPT.get_or_init(|| rwlock_script(ACQUIRE_WRITE_SCRIPT_BODY));

    let n: i64 = script
        .key(readers_key)
        .key(writer_key)
        .key(pending_writers_key)
        .key(pending_writers_heartbeat_key)
        .arg(owner)
        .arg(ttl_ms)
        .invoke_async(conn)
        .await?;

    Ok(n == 1)
}
