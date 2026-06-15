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


local function purge_expired_x_in_zset(key, now_ms, ttl_ms)
    local stale = redis.call('ZRANGE', key, '-inf', now_ms - ttl_ms, 'BYSCORE')

    if #stale > 0 then 
        redis.call('ZREM', key, unpack(stale))
    end

   reset_ttl_on_keys({key}, ttl_ms)

    return stale
end

local function purge_expired_pending_writers(pending_writers_key, pending_writers_heartbeat_key, now, ttl_ms)
   local stale = purge_expired_x_in_zset(pending_writers_heartbeat_key, now, ttl_ms)

   if #stale > 0 then
        redis.call('ZREM', pending_writers_key, unpack(stale))
   end

   reset_ttl_on_keys({pending_writers_key}, ttl_ms)

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
    purge_expired_pending_writers(pending_writers_key, pending_writers_heartbeat_key, now, ttl_ms)

    if redis.call('ZCARD', pending_writers_key) > 0 then return 0 end

    redis.call('ZADD', readers_key, now, owner)
    reset_ttl_on_keys({readers_key}, ttl_ms)

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
    local should_mark_pending = tonumber(ARGV[3])

    local current_writer = redis.call('GET', writer_key)

    if current_writer == owner then
        reset_ttl_on_keys({writer_key}, ttl_ms)
        return 1
    end

    purge_expired_pending_writers(pending_writers_key, pending_writers_heartbeat_key, now, ttl_ms)
    local oldest_pending_writer = redis.call('ZRANGE', pending_writers_key, '-inf', '+inf', 'BYSCORE', 'LIMIT', 0, 1)[1]

    purge_expired_x_in_zset(readers_key, now, ttl_ms)
    local readers_count = redis.call('ZCARD', readers_key)

    if current_writer or readers_count > 0 or (oldest_pending_writer and oldest_pending_writer ~= owner) then
        if should_mark_pending == 1 then
            redis.call('ZADD', pending_writers_key, 'NX', now, owner)
            redis.call('ZADD', pending_writers_heartbeat_key, now, owner)
            reset_ttl_on_keys({pending_writers_key, pending_writers_heartbeat_key}, ttl_ms)
        end

        return 0
    end

    redis.call('SET', writer_key, owner, 'PX', ttl_ms)
    redis.call('ZREM', pending_writers_key, owner)
    redis.call('ZREM', pending_writers_heartbeat_key, owner)

    return 1
"#;

const REFRESH_READ_SCRIPT_BODY: &str = r#"
    local now = now_ms()
    local readers_key = KEYS[1]
    local owner = ARGV[1]
    local ttl_ms = tonumber(ARGV[2])

    purge_expired_x_in_zset(readers_key, now, ttl_ms)

    if not redis.call('ZSCORE', readers_key, owner) then
        return 0
    end

    redis.call('ZADD', readers_key, 'XX', now, owner)
    return 1
"#;

/// ...
const RELEASE_WRITE_LUA: &str = r#"
    local writer_key = KEYS[1]
    local pending_writers_key = KEYS[2]
    local pending_writers_heartbeat_key = KEYS[3]

    local owner = ARGV[1]


    redis.call('ZREM', pending_writers_key, owner)
    redis.call('ZREM', pending_writers_heartbeat_key, owner)

    if redis.call('GET', writer_key) == owner then
        redis.call('DEL', writer_key)
        return 1
    else
        return 0
    end
"#;

const REFRESH_WRITE_SCRIPT_BODY: &str = r#"
    local now = now_ms()
    local writer_key = KEYS[1]
    local owner = ARGV[1]
    local ttl_ms = tonumber(ARGV[2])

    if redis.call('GET', writer_key) == owner then
        return redis.call('PEXPIRE', writer_key, ttl_ms)
    else
        return 0
    end
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
    should_mark_pending: bool,
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
        .arg(if should_mark_pending { 1 } else { 0 })
        .invoke_async(conn)
        .await?;

    Ok(n == 1)
}

/// ..
pub(crate) async fn refresh_read(
    conn: &mut ConnectionManager,
    readers_key: &str,
    owner: &str,
    ttl_ms: i64,
) -> Result<bool, DistkitError> {
    static SCRIPT: OnceLock<Script> = OnceLock::new();
    let script = SCRIPT.get_or_init(|| rwlock_script(REFRESH_READ_SCRIPT_BODY));

    let n: i64 = script
        .key(readers_key)
        .arg(owner)
        .arg(ttl_ms)
        .invoke_async(conn)
        .await?;

    Ok(n == 1)
}

///...
pub(crate) async fn release_read(
    conn: &mut ConnectionManager,
    readers_key: &str,
    owner: &str,
) -> Result<bool, DistkitError> {
    let n: i64 = redis::cmd("ZREM")
        .arg(readers_key)
        .arg(owner)
        .query_async(conn)
        .await?;

    Ok(n == 1)
}

/// ..
pub(crate) async fn release_write(
    conn: &mut ConnectionManager,
    writer_key: &str,
    pending_writers_key: &str,
    pending_writers_heartbeat_key: &str,
    owner: &str,
) -> Result<bool, DistkitError> {
    static SCRIPT: OnceLock<Script> = OnceLock::new();
    let script = SCRIPT.get_or_init(|| Script::new(RELEASE_WRITE_LUA));

    let n: i64 = script
        .key(writer_key)
        .key(pending_writers_key)
        .key(pending_writers_heartbeat_key)
        .arg(owner)
        .invoke_async(conn)
        .await?;

    Ok(n == 1)
}

/// ..
pub(crate) async fn refresh_write(
    conn: &mut ConnectionManager,
    writer_key: &str,
    owner: &str,
    ttl_ms: i64,
) -> Result<bool, DistkitError> {
    static SCRIPT: OnceLock<Script> = OnceLock::new();
    let script = SCRIPT.get_or_init(|| rwlock_script(REFRESH_WRITE_SCRIPT_BODY));

    let n: i64 = script
        .key(writer_key)
        .arg(owner)
        .arg(ttl_ms)
        .invoke_async(conn)
        .await?;

    Ok(n == 1)
}
