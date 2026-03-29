// Criterion benchmarks for `LaxCounter`.
//
// Warm-path benchmarks (inc_warm, dec_warm, get_warm, set_warm) measure the
// in-memory fast path: the DashMap store entry already exists and was last
// updated within `allowed_lag` (20 ms), so no Redis fetch is triggered.
// Each key is seeded with a single `block_on(inc)` before the measurement
// loop begins. Criterion iterations complete in nanoseconds (pure atomics +
// async overhead), so all samples stay comfortably within the 20 ms window.
//
// Destructive operations (del, clear) use `iter_batched` to re-seed the store
// entry before every measured call:
//
//   del  — without seeding, the first iteration removes the store entry and
//           every subsequent call hits the short-circuit `None` branch
//           (immediate Ok(0), no Redis call), making the bench meaningless.
//   clear — same issue: clears all entries and the batch; subsequent calls
//           would find nothing to do.

mod common;

use std::time::Duration;

use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use distkit::counter::CounterTrait;
use tokio::runtime::Runtime;

fn bench_lax_counter(c: &mut Criterion) {
    let rt = Runtime::new().expect("failed to build Tokio runtime");
    let counter = rt.block_on(common::make_lax_counter("lax"));

    let mut group = c.benchmark_group("lax_counter");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(10));

    // inc_warm
    let k = common::key("inc");
    rt.block_on(counter.inc(&k, 1)).expect("seed inc failed");
    group.bench_function("inc_warm", |b| {
        b.to_async(&rt).iter(|| counter.inc(&k, 1));
    });

    // dec_warm
    let k = common::key("dec");
    rt.block_on(counter.inc(&k, 1)).expect("seed dec failed");
    group.bench_function("dec_warm", |b| {
        b.to_async(&rt).iter(|| counter.dec(&k, 1));
    });

    // get_warm
    let k = common::key("get");
    rt.block_on(counter.inc(&k, 1)).expect("seed get failed");
    group.bench_function("get_warm", |b| {
        b.to_async(&rt).iter(|| counter.get(&k));
    });

    // set_warm
    let k = common::key("set");
    rt.block_on(counter.inc(&k, 1)).expect("seed set failed");
    group.bench_function("set_warm", |b| {
        b.to_async(&rt).iter(|| counter.set(&k, 42));
    });

    // del — re-seed the store entry before each measured call
    let k = common::key("del");
    group.bench_function("del", |b| {
        b.to_async(&rt).iter_batched(
            || counter.set(&k, 1),
            |_| counter.del(&k),
            BatchSize::SmallInput,
        );
    });

    // clear — re-seed a key before each measured call so clear always has
    // something to flush from the store and the batch
    let k = common::key("clear");
    group.bench_function("clear", |b| {
        b.to_async(&rt).iter_batched(
            || counter.set(&k, 1),
            |_| counter.clear(),
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

criterion_group!(benches, bench_lax_counter);
criterion_main!(benches);
