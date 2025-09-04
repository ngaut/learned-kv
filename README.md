# LearnedKvStore

A high-performance key-value store implementation in Rust using Minimal Perfect Hash Functions (MPHF) based on the [PtrHash algorithm](https://github.com/RagnarGrootKoerkamp/ptrhash).

## Features

- **O(1) lookup time** with minimal perfect hash functions
- **Memory efficient** - optimized storage with minimal overhead
- **Type-safe** - generic over any key and value types that implement required traits
- **Serializable** - save and load stores to/from disk using bincode
- **Performance optimized** - zero-allocation lookups for hot paths
- **Bug-fixed** - patched PtrHash implementation fixes mathematical instability for small datasets

## Performance Characteristics

Based on comprehensive benchmarking with optimized release builds and CPU-specific optimizations:

| Key Size | Lookup Time | Hash Overhead | Notes |
|----------|-------------|---------------|-------|
| ≤64 bytes | ~3-6ns | ~42% | Excellent for small keys |
| 128 bytes | ~10ns | ~69% | Very good performance |
| 256 bytes | ~22ns | ~85% | Good for medium keys |
| 512 bytes | ~57ns | ~94% | Hash becomes dominant |
| 1KB | ~146ns | ~98% | Consider key design |
| 4KB | ~639ns | ~99% | Hash computation dominates |

**Performance Analysis:**
- Hash computation: 95%+ of lookup time for large keys (4KB+)
- String comparison: ~1-3% of lookup time
- MPHF index calculation: <1% of lookup time
- **Optimized builds**: 26-83% faster than debug builds
- **CPU-specific optimizations**: Additional 20-25% improvement over generic builds

## Quick Start

```rust
use learned_kv::{LearnedKvStore, KvStoreBuilder};
use std::collections::HashMap;

// Build from HashMap
let mut data = HashMap::new();
data.insert("key1".to_string(), "value1".to_string());
let store = LearnedKvStore::new(data)?;

// Or use builder pattern
let store = KvStoreBuilder::new()
    .insert("key1".to_string(), "value1".to_string())
    .insert("key2".to_string(), "value2".to_string())
    .build()?;

// Fast lookup (recommended for hot paths)
match store.get(&"key1".to_string()) {
    Ok(value) => println!("Found: {}", value),
    Err(_) => println!("Not found"),
}

// Detailed lookup with error info (for debugging)
match store.get_detailed(&"key1".to_string()) {
    Ok(value) => println!("Found: {}", value),
    Err(e) => println!("Error details: {}", e),
}
```

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
learned-kv = { path = "path/to/learned-kv" }
```

## Examples

Run the examples to see the library in action:

```bash
# Basic usage demonstration
cargo run --example basic_usage --release

# Performance and cache analysis
cargo run --example cache_analysis --release
```

## Build Optimizations

This project is configured with CPU-specific optimizations for maximum performance:

```toml
# .cargo/config.toml
[build]
rustflags = ["-C", "target-cpu=native"]

# Cargo.toml
[profile.release]
lto = "thin"
codegen-units = 1
panic = "abort"
```

These optimizations provide:
- **26-83% performance improvement** over debug builds
- **Additional 20-25% improvement** over generic release builds  
- Better instruction selection for AES operations (GxHash acceleration)
- Improved vectorization and memory access patterns
- Smaller binary size with `panic = "abort"`

## Benchmarks

Run comprehensive benchmarks using Criterion:

```bash
# All benchmarks (automatically uses CPU optimizations)
cargo bench

# Specific benchmark groups
cargo bench key_length_impact
cargo bench lookup_performance
cargo bench construction_performance
```

## API Methods

### Core Operations
- `new(data: HashMap<K, V>)` - Create from HashMap
- `get(&key)` - Fast lookup (zero allocation on miss)
- `get_detailed(&key)` - Lookup with detailed error messages
- `contains_key(&key)` - Check if key exists
- `len()` - Number of key-value pairs
- `is_empty()` - Check if store is empty

### Iteration
- `iter()` - Iterate over all key-value pairs
- `keys()` - Iterate over keys
- `values()` - Iterate over values

### Persistence
- `save_to_file(path)` - Serialize to disk
- `load_from_file(path)` - Deserialize from disk

### Analysis
- `memory_usage_bytes()` - Get memory consumption

## Implementation Details

This library uses a patched version of the PtrHash algorithm that fixes mathematical instability issues for small datasets (<10,000 keys). The implementation provides:

1. **Consistent O(1) lookups** regardless of dataset size
2. **Optimized error handling** with zero-allocation variants for hot paths
3. **Comprehensive testing** with various key sizes and patterns
4. **Fixed partitioning** strategy for small datasets to ensure consistent performance

## Optimization Recommendations

1. **Use shorter keys when possible** - performance scales linearly with key length
2. **Use `get()` instead of `get_detailed()`** for hot paths to avoid string allocation
3. **Consider numeric or hash-based keys** for best performance
4. **Build with release profile** to enable all optimizations
5. **Use CPU-specific builds** for production deployments (automatically configured)
6. **For very large keys (>1KB)**, consider storing hashes as keys instead

## Project Structure

```
learned-kv/
├── src/
│   ├── lib.rs              # Main library interface
│   ├── kv_store.rs         # Core implementation
│   └── error.rs            # Error types
├── benches/
│   └── kv_store_bench.rs   # Criterion benchmarks
├── examples/
│   ├── basic_usage.rs      # Usage examples
│   └── cache_analysis.rs   # Performance analysis tool
└── ptr_hash_patched/       # Patched PtrHash dependency
```

## Key Improvements Over Original

1. **Mathematical Overflow Fix**: Patched ptr_hash to handle edge cases in MPHF construction
2. **Performance Optimizations**: 
   - Zero-allocation error paths for hot lookups
   - CPU-specific build optimizations (26-83% faster)
   - Hardware-accelerated hashing with GxHash AES instructions
3. **Comprehensive Benchmarking**: Criterion-based benchmarks with statistical analysis
4. **Better Small Dataset Handling**: Forced single-part construction for <10K keys
5. **Optimized Build System**: Automatic CPU-specific optimizations and LTO

## Use Cases

Ideal for:
- **Static lookup tables** - configuration data, dictionaries
- **Read-heavy workloads** - optimized for queries, not updates
- **Memory-constrained systems** - minimal overhead per key
- **Performance-critical paths** - consistent nanosecond lookups

## Limitations

- **Immutable** - cannot modify after construction
- **Construction overhead** - building MPHF takes time for large datasets
- **Memory requirement** - all data must fit in memory during construction

## Contributing

Contributions are welcome! Please ensure:
- All tests pass: `cargo test`
- Code follows Rust conventions: `cargo clippy`
- Benchmarks show no regression: `cargo bench`

## License

This project is MIT licensed. The original PtrHash library is by Ragnar Groot Koerkamp.

## Acknowledgments

- Based on the [PtrHash algorithm](https://github.com/RagnarGrootKoerkamp/ptrhash) by Ragnar Groot Koerkamp
- Performance analysis conducted using Criterion.rs benchmarking framework
- Mathematical stability fixes applied to handle edge cases