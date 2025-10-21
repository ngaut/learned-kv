use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use learned_kv::{LearnedKvStore, VerifiedKvStore};
use std::collections::{BTreeMap, HashMap};

fn create_test_data(size: usize, key_len: usize) -> HashMap<String, String> {
    let mut data = HashMap::new();
    let padding = "x".repeat(key_len.saturating_sub(15));
    for i in 0..size {
        let key = format!("{}key_{:08}", padding, i);
        let value = format!("value_{}", i);
        data.insert(key, value);
    }
    data
}

fn bench_lookup_performance(c: &mut Criterion) {
    let mut group = c.benchmark_group("lookup_performance");

    // Test with different dataset sizes
    for size in [100, 1000, 10000].iter() {
        let test_data = create_test_data(*size, 64);
        let all_keys: Vec<String> = test_data.keys().cloned().collect();

        let learned: LearnedKvStore<String, String> =
            LearnedKvStore::new(test_data.clone()).unwrap();
        let verified: VerifiedKvStore<String, String> =
            VerifiedKvStore::new(test_data.clone()).unwrap();

        // Single key lookup - LearnedKvStore
        group.bench_with_input(BenchmarkId::new("learned_single", size), size, |b, _| {
            b.iter(|| black_box(learned.get(black_box(&all_keys[0])).unwrap()))
        });

        // Single key lookup - VerifiedKvStore
        group.bench_with_input(BenchmarkId::new("verified_single", size), size, |b, _| {
            b.iter(|| black_box(verified.get(black_box(&all_keys[0])).unwrap()))
        });

        // Random key lookup - LearnedKvStore
        group.bench_with_input(BenchmarkId::new("learned_random", size), size, |b, _| {
            let mut key_idx: usize = 0;
            b.iter(|| {
                let key = &all_keys[key_idx % all_keys.len()];
                key_idx = key_idx.wrapping_add(1);
                black_box(learned.get(black_box(key)).unwrap())
            })
        });

        // Random key lookup - VerifiedKvStore
        group.bench_with_input(BenchmarkId::new("verified_random", size), size, |b, _| {
            let mut key_idx: usize = 0;
            b.iter(|| {
                let key = &all_keys[key_idx % all_keys.len()];
                key_idx = key_idx.wrapping_add(1);
                black_box(verified.get(black_box(key)).unwrap())
            })
        });

        // HashMap baseline
        group.bench_with_input(BenchmarkId::new("hashmap_random", size), size, |b, _| {
            let mut key_idx: usize = 0;
            b.iter(|| {
                let key = &all_keys[key_idx % all_keys.len()];
                key_idx = key_idx.wrapping_add(1);
                black_box(test_data.get(black_box(key)).unwrap())
            })
        });

        // BTreeMap baseline
        let btree_data: BTreeMap<String, String> = test_data
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        group.bench_with_input(BenchmarkId::new("btreemap_random", size), size, |b, _| {
            let mut key_idx: usize = 0;
            b.iter(|| {
                let key = &all_keys[key_idx % all_keys.len()];
                key_idx = key_idx.wrapping_add(1);
                black_box(btree_data.get(black_box(key)).unwrap())
            })
        });
    }

    group.finish();
}

fn bench_construction_performance(c: &mut Criterion) {
    let mut group = c.benchmark_group("construction_performance");

    for (size, key_len, test_name) in [
        (100, 32, "small_keys_small_dataset"),
        (1000, 64, "medium_keys_medium_dataset"),
        (10000, 128, "large_keys_large_dataset"),
    ]
    .iter()
    {
        let test_data = create_test_data(*size, *key_len);

        group.bench_with_input(
            BenchmarkId::new("learned", test_name),
            &test_data,
            |b, data| {
                b.iter(|| {
                    let store: LearnedKvStore<String, String> =
                        LearnedKvStore::new(data.clone()).unwrap();
                    black_box(store);
                })
            },
        );

        group.bench_with_input(
            BenchmarkId::new("verified", test_name),
            &test_data,
            |b, data| {
                b.iter(|| {
                    let store: VerifiedKvStore<String, String> =
                        VerifiedKvStore::new(data.clone()).unwrap();
                    black_box(store);
                })
            },
        );
    }

    group.finish();
}

fn bench_key_length_impact(c: &mut Criterion) {
    let mut group = c.benchmark_group("key_length_impact");

    let size = 1000;
    for key_len in [8, 32, 64, 128, 512].iter() {
        let test_data = create_test_data(size, *key_len);
        let all_keys: Vec<String> = test_data.keys().cloned().collect();

        let learned: LearnedKvStore<String, String> =
            LearnedKvStore::new(test_data.clone()).unwrap();

        group.bench_with_input(BenchmarkId::new("learned", key_len), key_len, |b, _| {
            let mut key_idx: usize = 0;
            b.iter(|| {
                let key = &all_keys[key_idx % all_keys.len()];
                key_idx = key_idx.wrapping_add(1);
                black_box(learned.get(black_box(key)).unwrap())
            })
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_lookup_performance,
    bench_construction_performance,
    bench_key_length_impact
);
criterion_main!(benches);
