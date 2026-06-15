// Criterion benchmarks for the distributed `Mutex` and `RwLock`.
//
// Every acquire / release is an atomic Redis round-trip (SET NX PX, owner-checked
// Lua, or ZSET ops for the rwlock), so latency is dominated by the network
// round-trip to Redis. The locks are benched through their public API as
// acquire -> release cycles, so no key stays held across measured iterations.
//
// Each bench uses a distinct, timestamped key prefix (via the `common::make_*`
// helpers) so there is no cross-bench state interference.

mod common;

use std::time::Duration;

use criterion::{Criterion, criterion_group, criterion_main};
use tokio::runtime::Runtime;

fn bench_mutex(c: &mut Criterion) {
    let rt = Runtime::new().expect("failed to build Tokio runtime");

    let mut group = c.benchmark_group("lock_mutex");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(10));

    // acquire + release cycle (one Redis round-trip each way).
    let mutex = rt.block_on(common::make_mutex("mutex_acquire_release"));
    group.bench_function("acquire_release", |b| {
        b.to_async(&rt).iter(|| async {
            let guard = mutex.try_lock().await.expect("uncontended try_lock");
            guard.release().await.expect("release");
        });
    });

    // contended try_lock: a second mutex holds the key, so every measured
    // try_lock takes the fast `AcquireFail` path.
    let (holder, contender) = rt.block_on(common::make_contended_mutexes("mutex_contended"));
    let held = rt.block_on(holder.lock()).expect("holder acquires the key");
    group.bench_function("try_lock_contended", |b| {
        b.to_async(&rt).iter(|| async {
            let _ = contender.try_lock().await; // Err(AcquireFail) — the lock is held
        });
    });
    // Release inside the runtime — the guard's `Drop` spawns a release task and
    // would panic if dropped outside a Tokio context.
    rt.block_on(async { held.release().await.expect("release holder") });

    group.finish();
}

fn bench_rwlock(c: &mut Criterion) {
    let rt = Runtime::new().expect("failed to build Tokio runtime");

    let mut group = c.benchmark_group("lock_rwlock");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(10));

    // shared read acquire + release.
    let rw = rt.block_on(common::make_rwlock("rwlock_read"));
    group.bench_function("read_acquire_release", |b| {
        b.to_async(&rt).iter(|| async {
            let guard = rw.try_read().await.expect("uncontended try_read");
            guard.release().await.expect("release");
        });
    });

    // exclusive write acquire + release.
    let rw = rt.block_on(common::make_rwlock("rwlock_write"));
    group.bench_function("write_acquire_release", |b| {
        b.to_async(&rt).iter(|| async {
            let guard = rw.try_write().await.expect("uncontended try_write");
            guard.release().await.expect("release");
        });
    });

    group.finish();
}

criterion_group!(benches, bench_mutex, bench_rwlock);
criterion_main!(benches);
