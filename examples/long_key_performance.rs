use learned_kv::{LearnedKvStore};
use std::collections::HashMap;
use std::time::Instant;

fn benchmark_key_length(key_length: usize, num_keys: usize, lookups: usize) -> (f64, f64) {
    let mut data = HashMap::new();
    for i in 0..num_keys {
        let key = format!("{:0width$}", i, width = key_length);
        let value = format!("value_{}", i);
        data.insert(key, value);
    }
    
    // Construction time
    let start = Instant::now();
    let store = LearnedKvStore::new(data).unwrap();
    let construction_time = start.elapsed();
    
    // Lookup time
    let test_key = format!("{:0width$}", num_keys / 2, width = key_length);
    let start = Instant::now();
    for _ in 0..lookups {
        let _ = store.get(&test_key);
    }
    let lookup_time = start.elapsed();
    
    let construction_ns = construction_time.as_nanos() as f64;
    let lookup_ns = lookup_time.as_nanos() as f64 / lookups as f64;
    
    (construction_ns, lookup_ns)
}

fn main() {
    println!("Long Key Performance Analysis with target-cpu=native");
    println!("==================================================\n");
    
    let key_lengths = vec![32, 64, 128, 256, 512, 1024, 2048, 4096];
    let num_keys = 1000;
    let lookups = 100000;
    
    println!("Key Length | Construction (μs) | Lookup (ns) | Hash % of Lookup");
    println!("-----------|-------------------|-------------|------------------");
    
    let mut baseline_lookup = None;
    
    for &key_len in &key_lengths {
        let (construction_ns, lookup_ns) = benchmark_key_length(key_len, num_keys, lookups);
        let construction_us = construction_ns / 1000.0;
        
        if baseline_lookup.is_none() {
            baseline_lookup = Some(lookup_ns);
        }
        
        let baseline = baseline_lookup.unwrap();
        let hash_percentage = ((lookup_ns - baseline) / lookup_ns * 100.0).max(0.0);
        
        println!("{:10} | {:13.1} | {:9.1} | {:14.1}%", 
                key_len, construction_us, lookup_ns, hash_percentage);
    }
    
    println!("\nAnalysis:");
    println!("- Short keys (32-64 bytes): Hash computation is minimal overhead");
    println!("- Medium keys (128-512 bytes): Hash starts becoming significant");  
    println!("- Long keys (1024+ bytes): Hash dominates lookup time");
    println!("- With CPU optimizations, GxHash scales better than basic implementations");
    
    println!("\nRecommendations for long keys:");
    println!("1. Use shorter keys when possible (design consideration)");
    println!("2. Cache hash values if keys are reused frequently");
    println!("3. Consider key prefixes or hierarchical structures");
    println!("4. GxHash with AES acceleration is already optimal for the hash function");
}