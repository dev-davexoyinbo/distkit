//! Live-Redis tests for the [`Mutex`](crate::lock::Mutex) and its
//! [`MutexGuard`](crate::lock::MutexGuard): exclusion, bounded waiting,
//! release-frees semantics, and Stage 3 auto-refresh (lease kept alive past
//! `ttl`; a lost lease surfaces as [`LockGuardState::Lost`]).

use std::time::Duration;

use crate::DistkitError;
use crate::lock::tests::common::{
    make_fast_options, make_options, make_options_with_key, raw_connection,
};
use crate::lock::{LockError, Mutex, LockGuardState};

/// Two mutexes (distinct owners) on the same key: the second `try_lock` is
/// excluded while the first guard is held.
#[tokio::test]
async fn try_lock_excludes_second_owner() {
    let mutex_a = Mutex::new(make_options("try_lock_excludes_second_owner").await);
    let mutex_b = Mutex::new(make_options("try_lock_excludes_second_owner").await);

    let first_guard = mutex_a
        .try_lock()
        .await
        .expect("first try_lock should take the lock");

    match mutex_b.try_lock().await {
        Err(DistkitError::LockError(LockError::AcquireFail)) => {}
        other => panic!("expected WouldBlock while held, got {other:?}"),
    }

    drop(first_guard);
}

/// `lock`/`try_lock_until` blocks while the key is held, then acquires once the
/// holder releases.
#[tokio::test]
async fn lock_waits_then_succeeds() {
    let mutex_a = Mutex::new(make_options("lock_waits_then_succeeds").await);
    let mutex_b = Mutex::new(make_fast_options("lock_waits_then_succeeds").await);

    let first_guard = mutex_a
        .try_lock()
        .await
        .expect("first try_lock should take the lock");

    let waiter = tokio::spawn(async move { mutex_b.try_lock_until(Duration::from_secs(2)).await });

    tokio::time::sleep(Duration::from_millis(200)).await;
    first_guard
        .release()
        .await
        .expect("explicit release should succeed");

    waiter
        .await
        .expect("waiter task should not panic")
        .expect("waiter should acquire the lock after release");
}

/// A bounded `try_lock_until` on a held key fails with `Timeout`.
#[tokio::test]
async fn try_lock_until_times_out() {
    let mutex_a = Mutex::new(make_options("try_lock_until_times_out").await);
    let mutex_b = Mutex::new(make_fast_options("try_lock_until_times_out").await);

    let _first_guard = mutex_a
        .try_lock()
        .await
        .expect("first try_lock should take the lock");

    match mutex_b.try_lock_until(Duration::from_millis(100)).await {
        Err(DistkitError::LockError(LockError::Timeout { .. })) => {}
        other => panic!("expected Timeout, got {other:?}"),
    }
}

/// The deprecated `try_lock_for` still works: a bounded attempt on a held key
/// fails with `Timeout`. Retained for coverage of the legacy API.
#[allow(deprecated)]
#[tokio::test]
async fn try_lock_for_still_works() {
    let mutex_a = Mutex::new(make_options("try_lock_for_still_works").await);
    let mutex_b = Mutex::new(make_options("try_lock_for_still_works").await);

    let _first_guard = mutex_a
        .try_lock()
        .await
        .expect("first try_lock should take the lock");

    match mutex_b
        .try_lock_for(Duration::from_millis(100), Duration::from_millis(20))
        .await
    {
        Err(DistkitError::LockError(LockError::Timeout { .. })) => {}
        other => panic!("expected Timeout, got {other:?}"),
    }
}

/// Explicit `release` frees the key for another owner.
#[tokio::test]
async fn release_frees_lock() {
    let mutex_a = Mutex::new(make_options("release_frees_lock").await);
    let mutex_b = Mutex::new(make_options("release_frees_lock").await);

    let guard = mutex_a
        .try_lock()
        .await
        .expect("first try_lock should take the lock");
    guard
        .release()
        .await
        .expect("explicit release should succeed");

    mutex_b
        .try_lock()
        .await
        .expect("lock should be acquirable after release");
}

/// Dropping the guard frees the key (best-effort, fire-and-forget release).
#[tokio::test]
async fn drop_frees_lock() {
    let mutex_a = Mutex::new(make_options("drop_frees_lock").await);
    let mutex_b = Mutex::new(make_options("drop_frees_lock").await);

    {
        let _guard = mutex_a
            .try_lock()
            .await
            .expect("first try_lock should take the lock");
    }

    // Drop release is spawned/async — give it a moment to land.
    tokio::time::sleep(Duration::from_millis(150)).await;

    mutex_b
        .try_lock()
        .await
        .expect("lock should be acquirable after the guard drops");
}

/// Background auto-refresh keeps a held lease alive well past its `ttl`: a second
/// owner is still excluded long after the lease would otherwise have expired.
#[tokio::test]
async fn auto_refresh_keeps_lease_alive() {
    let mut options = make_options("auto_refresh_keeps_lease_alive").await;
    options.ttl = Duration::from_millis(150);
    let mutex_a = Mutex::new(options);
    let mutex_b = Mutex::new(make_options("auto_refresh_keeps_lease_alive").await);

    let _guard = mutex_a
        .try_lock()
        .await
        .expect("first try_lock should take the lock");

    // Well past ttl (150 ms) — without refresh the lease would have expired.
    tokio::time::sleep(Duration::from_millis(400)).await;

    match mutex_b.try_lock().await {
        Err(DistkitError::LockError(LockError::AcquireFail)) => {}
        other => panic!("expected AcquireFail — refresh should keep lease alive, got {other:?}"),
    }
}

/// A healthy held lock reports [`LockGuardState::Acquired`].
#[tokio::test]
async fn get_state_reports_acquired_while_held() {
    let mutex = Mutex::new(make_options("get_state_reports_acquired_while_held").await);

    let guard = mutex
        .try_lock()
        .await
        .expect("try_lock should take the lock");

    assert_eq!(guard.get_state().await, LockGuardState::Acquired);
}

/// An uncontended acquire wins on the first poll: `get_on_attempt` is `0`.
#[tokio::test]
async fn get_on_attempt_zero_when_uncontended() {
    let mutex = Mutex::new(make_options("get_on_attempt_zero_when_uncontended").await);

    let guard = mutex
        .try_lock()
        .await
        .expect("try_lock should take the lock");

    assert_eq!(guard.get_on_attempt().await, 0);
}

/// A waiter that blocks while the key is held only wins after retries:
/// `get_on_attempt` is non-zero.
#[tokio::test]
async fn get_on_attempt_counts_retries_under_contention() {
    let mutex_a = Mutex::new(make_options("get_on_attempt_counts_retries").await);
    let mutex_b = Mutex::new(make_fast_options("get_on_attempt_counts_retries").await);

    let first_guard = mutex_a
        .try_lock()
        .await
        .expect("first try_lock should take the lock");

    let waiter = tokio::spawn(async move { mutex_b.try_lock_until(Duration::from_secs(2)).await });

    tokio::time::sleep(Duration::from_millis(200)).await;
    first_guard
        .release()
        .await
        .expect("explicit release should succeed");

    let guard = waiter
        .await
        .expect("waiter task should not panic")
        .expect("waiter should acquire the lock after release");

    assert!(
        guard.get_on_attempt().await >= 1,
        "a contended acquire should take more than one attempt"
    );
}

/// If the lease is lost while held (key deleted out from under us), the refresh
/// task marks it [`LockGuardState::Lost`]: `get_state` reports it and `release`
/// returns `Ok(LockGuardState::Lost)` without issuing a DEL we no longer own.
#[tokio::test]
async fn lost_lease_reports_lost_state() {
    let (mut options, full_key) = make_options_with_key("lost_lease_reports_lost_state").await;
    options.ttl = Duration::from_millis(150);
    let mutex = Mutex::new(options);

    let guard = mutex
        .try_lock()
        .await
        .expect("try_lock should take the lock");

    // Delete the key out from under the holder; the next refresh tick (~50 ms)
    // sees GET != owner -> Ok(false) -> lost.
    let mut conn = raw_connection().await;
    let _: () = redis::cmd("DEL")
        .arg(&full_key)
        .query_async(&mut conn)
        .await
        .expect("DEL should succeed");

    // Past at least one ttl/3 (~50 ms) refresh tick.
    tokio::time::sleep(Duration::from_millis(200)).await;

    assert_eq!(
        guard.get_state().await,
        LockGuardState::Lost,
        "lease should be marked lost after the key was deleted"
    );

    match guard.release().await {
        Ok(LockGuardState::Lost) => {}
        other => {
            panic!("expected Ok(LockGuardState::Lost) after the lease was deleted, got {other:?}")
        }
    }
}
