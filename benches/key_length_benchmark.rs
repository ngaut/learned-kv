use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use learned_kv::{LearnedKvStore, VerifiedKvStore};
use std::collections::{BTreeMap, HashMap};

fn create_test_data(size: usize, key_len: usize) -> HashMap<String, String> {
    let mut data = HashMap::new();

    // Generate keys of exact length
    for i in 0..size {
        let key = if key_len <= 10 {
            // For very short keys, just use index
            format!("{:0width$}", i, width = key_len.saturating_sub(0))
        } else {
            // For longer keys, pad with 'x'
            let padding = "x".repeat(key_len.saturating_sub(10));
            format!("{}key_{:05}", padding, i)
        };

        // Ensure key is exactly key_len (truncate or pad)
        let key = if key.len() > key_len {
            key[..key_len].to_string()
        } else if key.len() < key_len {
            format!("{:width$}", key, width = key_len)
        } else {
            key
        };

        let value = format!("value_{}", i);
        data.insert(key, value);
    }
    data
}

fn bench_key_length_learned(c: &mut Criterion) {
    let mut group = c.benchmark_group("key_length_learned");

    // Test different key lengths: 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048
    let key_lengths = [4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048];
    let dataset_size = 1000;

    for key_len in key_lengths.iter() {
        let test_data = create_test_data(dataset_size, *key_len);
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

fn bench_key_length_verified(c: &mut Criterion) {
    let mut group = c.benchmark_group("key_length_verified");

    let key_lengths = [4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048];
    let dataset_size = 1000;

    for key_len in key_lengths.iter() {
        let test_data = create_test_data(dataset_size, *key_len);
        let all_keys: Vec<String> = test_data.keys().cloned().collect();

        let verified: VerifiedKvStore<String, String> =
            VerifiedKvStore::new(test_data.clone()).unwrap();

        group.bench_with_input(BenchmarkId::new("verified", key_len), key_len, |b, _| {
            let mut key_idx: usize = 0;
            b.iter(|| {
                let key = &all_keys[key_idx % all_keys.len()];
                key_idx = key_idx.wrapping_add(1);
                black_box(verified.get(black_box(key)).unwrap())
            })
        });
    }

    group.finish();
}

fn bench_key_length_hashmap(c: &mut Criterion) {
    let mut group = c.benchmark_group("key_length_hashmap");

    let key_lengths = [4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048];
    let dataset_size = 1000;

    for key_len in key_lengths.iter() {
        let test_data = create_test_data(dataset_size, *key_len);
        let all_keys: Vec<String> = test_data.keys().cloned().collect();

        group.bench_with_input(BenchmarkId::new("hashmap", key_len), key_len, |b, _| {
            let mut key_idx: usize = 0;
            b.iter(|| {
                let key = &all_keys[key_idx % all_keys.len()];
                key_idx = key_idx.wrapping_add(1);
                black_box(test_data.get(black_box(key)).unwrap())
            })
        });
    }

    group.finish();
}

fn bench_key_length_btreemap(c: &mut Criterion) {
    let mut group = c.benchmark_group("key_length_btreemap");

    let key_lengths = [4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048];
    let dataset_size = 1000;

    for key_len in key_lengths.iter() {
        let test_data = create_test_data(dataset_size, *key_len);
        let all_keys: Vec<String> = test_data.keys().cloned().collect();

        let btree_data: BTreeMap<String, String> = test_data
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        group.bench_with_input(BenchmarkId::new("btreemap", key_len), key_len, |b, _| {
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

fn bench_key_length_comparison(c: &mut Criterion) {
    let mut group = c.benchmark_group("key_length_comparison");

    // Comprehensive comparison at specific key lengths
    let key_lengths = [8, 32, 128, 512, 2048];
    let dataset_size = 1000;

    for key_len in key_lengths.iter() {
        let test_data = create_test_data(dataset_size, *key_len);
        let all_keys: Vec<String> = test_data.keys().cloned().collect();

        let learned: LearnedKvStore<String, String> =
            LearnedKvStore::new(test_data.clone()).unwrap();
        let verified: VerifiedKvStore<String, String> =
            VerifiedKvStore::new(test_data.clone()).unwrap();
        let btree_data: BTreeMap<String, String> = test_data
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        // LearnedKvStore
        group.bench_with_input(BenchmarkId::new("learned", key_len), key_len, |b, _| {
            let mut key_idx: usize = 0;
            b.iter(|| {
                let key = &all_keys[key_idx % all_keys.len()];
                key_idx = key_idx.wrapping_add(1);
                black_box(learned.get(black_box(key)).unwrap())
            })
        });

        // VerifiedKvStore
        group.bench_with_input(BenchmarkId::new("verified", key_len), key_len, |b, _| {
            let mut key_idx: usize = 0;
            b.iter(|| {
                let key = &all_keys[key_idx % all_keys.len()];
                key_idx = key_idx.wrapping_add(1);
                black_box(verified.get(black_box(key)).unwrap())
            })
        });

        // HashMap
        group.bench_with_input(BenchmarkId::new("hashmap", key_len), key_len, |b, _| {
            let mut key_idx: usize = 0;
            b.iter(|| {
                let key = &all_keys[key_idx % all_keys.len()];
                key_idx = key_idx.wrapping_add(1);
                black_box(test_data.get(black_box(key)).unwrap())
            })
        });

        // BTreeMap
        group.bench_with_input(BenchmarkId::new("btreemap", key_len), key_len, |b, _| {
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

criterion_group!(
    benches,
    bench_key_length_learned,
    bench_key_length_verified,
    bench_key_length_hashmap,
    bench_key_length_btreemap,
    bench_key_length_comparison
);
criterion_main!(benches);
