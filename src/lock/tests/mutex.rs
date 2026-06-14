//! Live-Redis tests for the Stage 2 [`Mutex`](crate::lock::Mutex) and its
//! [`MutexGuard`](crate::lock::MutexGuard): exclusion, bounded waiting, and
//! release-frees semantics. No auto-refresh yet (Stage 3).

use std::time::Duration;

use crate::DistkitError;
use crate::lock::tests::common::make_options;
use crate::lock::{LockError, Mutex};

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

/// `lock`/`try_lock_for` blocks while the key is held, then acquires once the
/// holder releases.
#[tokio::test]
async fn lock_waits_then_succeeds() {
    let mutex_a = Mutex::new(make_options("lock_waits_then_succeeds").await);
    let mutex_b = Mutex::new(make_options("lock_waits_then_succeeds").await);

    let first_guard = mutex_a
        .try_lock()
        .await
        .expect("first try_lock should take the lock");

    let waiter = tokio::spawn(async move {
        mutex_b
            .try_lock_for(Duration::from_secs(2), Duration::from_millis(20))
            .await
    });

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

/// A bounded `try_lock_for` on a held key fails with `Timeout`.
#[tokio::test]
async fn try_lock_for_times_out() {
    let mutex_a = Mutex::new(make_options("try_lock_for_times_out").await);
    let mutex_b = Mutex::new(make_options("try_lock_for_times_out").await);

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

/// With auto-refresh not yet implemented, an unreleased lease simply expires and
/// the key becomes acquirable again.
#[tokio::test]
async fn lease_expiry_frees_lock() {
    let mut options = make_options("lease_expiry_frees_lock").await;
    options.ttl = Duration::from_millis(100);
    let mutex_a = Mutex::new(options);
    let mutex_b = Mutex::new(make_options("lease_expiry_frees_lock").await);

    let _guard = mutex_a
        .try_lock()
        .await
        .expect("first try_lock should take the lock");

    tokio::time::sleep(Duration::from_millis(250)).await;

    mutex_b
        .try_lock()
        .await
        .expect("lock should be acquirable once the lease expires");
}
