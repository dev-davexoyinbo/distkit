//! Low-level Redis lock operations: shared Lua scripts and the atomic
//! acquire / refresh / release round-trips backing `Mutex` and `RwLock`.
//!
//! Stage 1 populates the mutex backend; the rwlock backend lands in Stage 4.
//! Every op is a single atomic Redis round-trip keyed on the caller's
//! `owner_id`. Callers pass a fully-formed (already namespaced) Redis key.

use std::sync::OnceLock;

use redis::{Script, aio::ConnectionManager};

use crate::DistkitError;

/// Owner-checked PEXPIRE: extend the lease only if we still own it.
const REFRESH_LUA: &str = r#"
    if redis.call('GET', KEYS[1]) == ARGV[1] then
        return redis.call('PEXPIRE', KEYS[1], ARGV[2])
    else
        return 0
    end
"#;

/// Owner-checked DEL: release the lock only if we still own it.
const RELEASE_LUA: &str = r#"
    if redis.call('GET', KEYS[1]) == ARGV[1] then
        return redis.call('DEL', KEYS[1])
    else
        return 0
    end
"#;

/// `SET key owner NX PX ttl_ms`. `Ok(true)` if the lock was taken.
pub(crate) async fn acquire(
    conn: &mut ConnectionManager,
    key: &str,
    owner: &str,
    ttl_ms: i64,
) -> Result<bool, DistkitError> {
    let res: Option<String> = redis::cmd("SET")
        .arg(key)
        .arg(owner)
        .arg("NX")
        .arg("PX")
        .arg(ttl_ms)
        .query_async(conn)
        .await?;

    Ok(res.is_some())
}

/// Owner-checked PEXPIRE. `Ok(true)` if we still own the lease and extended it.
pub(crate) async fn refresh(
    conn: &mut ConnectionManager,
    key: &str,
    owner: &str,
    ttl_ms: i64,
) -> Result<bool, DistkitError> {
    static REFRESH_SCRIPT: OnceLock<Script> = OnceLock::new();
    let script = REFRESH_SCRIPT.get_or_init(|| Script::new(REFRESH_LUA));

    let n: i64 = script
        .key(key)
        .arg(owner)
        .arg(ttl_ms)
        .invoke_async(conn)
        .await?;

    Ok(n == 1)
}

/// Owner-checked DEL. `Ok(true)` if we held the lock and released it.
pub(crate) async fn release(
    conn: &mut ConnectionManager,
    key: &str,
    owner: &str,
) -> Result<bool, DistkitError> {
    static RELEASE_SCRIPT: OnceLock<Script> = OnceLock::new();
    let script = RELEASE_SCRIPT.get_or_init(|| Script::new(RELEASE_LUA));

    let n: i64 = script.key(key).arg(owner).invoke_async(conn).await?;

    Ok(n == 1)
}
