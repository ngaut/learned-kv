use learned_kv::VerifiedKvStore;
use std::collections::HashMap;
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("CACHE ANALYSIS - What gets cached and where?");
    println!("============================================");

    let key_len = 4096;
    let num_keys = 1000;

    // Create test data
    let mut data = HashMap::new();
    let base_key = "a".repeat(key_len - 10);

    for i in 0..num_keys {
        let key = format!("{}{:010}", base_key, i);
        let value = format!("value_{}", i);
        data.insert(key, value);
    }

    let store = VerifiedKvStore::new_string(data)?;
    let all_keys: Vec<String> = store.keys().cloned().collect();

    println!(
        "Dataset: {} keys of {} bytes each",
        all_keys.len(),
        key_len
    );

    // Test 1: Same key repeated lookups (should show caching effects)
    println!("\nTest 1: Same Key Repeated Lookups (Cache Test)");
    let test_key = &all_keys[0];
    let iterations = 100_000;

    let start = Instant::now();
    for _ in 0..iterations {
        let _result = store.get(test_key)?;
    }
    let same_key_time = start.elapsed();
    let same_key_avg = same_key_time.as_nanos() / iterations as u128;
    println!(
        "   Same key {}k times: {} ns/lookup",
        iterations / 1000,
        same_key_avg
    );

    // Test 2: Different keys (no cache benefits)
    println!("\nTest 2: Different Keys (No Cache Test)");
    let mut key_idx = 0;

    let start = Instant::now();
    for _ in 0..iterations {
        let key = &all_keys[key_idx];
        let _result = store.get(key)?;
        key_idx = (key_idx + 1) % all_keys.len();
    }
    let different_keys_time = start.elapsed();
    let different_keys_avg = different_keys_time.as_nanos() / iterations as u128;
    println!(
        "   Different keys {}k times: {} ns/lookup",
        iterations / 1000,
        different_keys_avg
    );

    // Test 3: Hash computation isolation
    println!("\nTest 3: Hash Computation Isolation");
    let hash_avg = {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let start = Instant::now();
        for _ in 0..iterations {
            let mut hasher = DefaultHasher::new();
            test_key.hash(&mut hasher);
            let _hash = hasher.finish();
        }
        let hash_time = start.elapsed();
        let hash_avg = hash_time.as_nanos() / iterations as u128;
        println!("   Pure hash computation: {} ns/operation", hash_avg);
        hash_avg
    };

    // Test 4: Memory access patterns
    println!("\nTest 4: Memory Access Pattern Analysis");

    // Sequential access (cache friendly)
    let start = Instant::now();
    for i in 0..iterations.min(all_keys.len() * 100) {
        let key = &all_keys[i % all_keys.len()];
        let _result = store.get(key)?;
    }
    let sequential_time = start.elapsed();
    let sequential_avg = sequential_time.as_nanos() / iterations.min(all_keys.len() * 100) as u128;
    println!("   Sequential access: {} ns/lookup", sequential_avg);

    // Random access (cache unfriendly)
    let random_indices: Vec<usize> = (0..iterations.min(all_keys.len() * 100))
        .map(|i: usize| (i.wrapping_mul(314159) % all_keys.len()))
        .collect();

    let start = Instant::now();
    for &idx in &random_indices {
        let key = &all_keys[idx];
        let _result = store.get(key)?;
    }
    let random_time = start.elapsed();
    let random_avg = random_time.as_nanos() / random_indices.len() as u128;
    println!("   Random access: {} ns/lookup", random_avg);

    // Analysis
    println!("\nCACHE ANALYSIS RESULTS");
    println!("======================");

    let cache_benefit = if same_key_avg < different_keys_avg {
        ((different_keys_avg - same_key_avg) as f64 / different_keys_avg as f64) * 100.0
    } else {
        0.0
    };

    println!("Cache Performance Comparison:");
    println!("  Same key repeated:    {} ns", same_key_avg);
    println!("  Different keys:       {} ns", different_keys_avg);
    println!("  Pure hash computation: {} ns", hash_avg);
    println!("  Sequential access:    {} ns", sequential_avg);
    println!("  Random access:        {} ns", random_avg);

    if cache_benefit > 0.0 {
        println!("  Cache benefit:        {:.1}%", cache_benefit);
    } else {
        println!("  Cache benefit:        None detected");
    }

    println!("\nCACHE TYPE ANALYSIS");
    println!("===================");

    if same_key_avg < different_keys_avg {
        println!("[DETECTED] CPU cache effects:");
        println!("   - Key data stays in L1/L2/L3 cache");
        println!("   - Hash computation may be optimized out");
        println!("   - Memory prefetching benefits");
    } else {
        println!("[NOT DETECTED] No significant CPU cache benefits");
    }

    if sequential_avg < random_avg {
        println!("[DETECTED] Memory locality effects:");
        println!("   - Sequential access is cache-friendly");
        println!("   - Random access causes cache misses");
        let locality_benefit = ((random_avg - sequential_avg) as f64 / random_avg as f64) * 100.0;
        println!("   - Locality benefit: {:.1}%", locality_benefit);
    }

    // Hash vs lookup analysis
    let hash_to_lookup_ratio = hash_avg as f64 / different_keys_avg as f64;
    println!("\nHASH vs LOOKUP RATIO: {:.2}x", hash_to_lookup_ratio);

    if hash_to_lookup_ratio > 1.0 {
        println!("[ANALYSIS] Hash computation takes MORE time than full lookup!");
        println!("   This indicates internal optimizations:");
        println!("   - Hash result caching in MPHF");
        println!("   - Compiler optimizations eliminating redundant work");
        println!("   - MPHF using pre-computed hash values");
    } else {
        println!("[ANALYSIS] Hash computation is properly integrated");
    }

    Ok(())
}
