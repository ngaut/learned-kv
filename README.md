# VerifiedKvStore

A high-performance, immutable key-value store in Rust using Minimal Perfect Hash Functions (MPHF) based on the [PtrHash algorithm](https://github.com/RagnarGrootKoerkamp/ptrhash).

## Quick Start

```rust
use learned_kv::VerifiedKvStore;
use std::collections::HashMap;

// Build from HashMap
let mut data = HashMap::new();
data.insert("key1".to_string(), "value1".to_string());
data.insert("key2".to_string(), "value2".to_string());
let store = VerifiedKvStore::new(data)?;

// Safe lookup with verification
match store.get(&"key1".to_string()) {
    Ok(value) => println!("Found: {}", value),
    Err(_) => println!("Not found"),
}

// Iterate and serialize
for (key, value) in store.iter() {
    println!("{}: {}", key, value);
}
store.save_to_file("data.bin")?;
```

## Features

- **O(1) lookups** - Constant-time queries using minimal perfect hash functions
- **Safe verification** - Returns errors for non-existent keys, never wrong values
- **Type-safe** - Generic over key and value types
- **Serializable** - Save/load to disk with bincode
- **Zero-allocation** - Hot path lookups have no allocations
- **Full API** - iter(), keys(), values(), contains_key()

## Performance

Based on benchmarking with optimized release builds:

| Key Size | Lookup Time | Notes |
|----------|-------------|-------|
| 64 bytes | **5.3 ns** | Excellent for small keys |
| 128 bytes | **10.0 ns** | Very good performance |
| 256 bytes | **21.4 ns** | Good for medium keys |
| 512 bytes | **52.1 ns** | Hash computation dominant |
| 1KB | **133.0 ns** | Hash computation dominant |
| 2KB | **317.7 ns** | Consider shorter keys |

**Bottleneck Analysis** (see [PERFORMANCE_ANALYSIS.md](PERFORMANCE_ANALYSIS.md)):
- Hash computation: 95-100% of lookup time
- MPHF index calculation: <5%
- Key comparison: <1ns (negligible)
- Array access: <1ns (negligible)

**Optimizations enabled:**
- CPU-specific builds: 26-83% faster than debug
- LTO and single codegen-unit
- Hardware-accelerated hashing (AES-NI)

## Installation

```toml
[dependencies]
learned-kv = { path = "path/to/learned-kv" }
```

## Important Limitations

### 1. MPHF Construction Can Fail

Construction may panic for certain key patterns:
- ❌ Sequential patterns: `"key_0001"`, `"key_0002"`, ... or `0, 1, 2, ...`
- ❌ Large datasets (>10K keys) with poorly distributed hashes
- ✅ UUID-style strings: `"key-a3f2-8b1c"`, ...
- ✅ Well-distributed hash-based keys
- ✅ Integer keys with good distribution

**Recommendation:** Use UUID-style or hash-based keys for reliability.

### 2. Slow Load Times

MPHF is rebuilt on every load (no serialization):
- 1K keys: ~1-5ms
- 100K keys: ~50-100ms
- 1M keys: ~500ms-1s
- 10M keys: ~5-10s

**Not suitable** for applications requiring fast startup or frequent reloads.

### 3. Immutable Data

Cannot modify after construction - requires full rebuild for updates.

## When to Use

### Use VerifiedKvStore When:
✅ Static datasets loaded once at startup
✅ Read-heavy workloads with infrequent updates
✅ Memory-constrained environments (3 bits/key overhead)
✅ Need accurate key verification (no false positives)
✅ UUID-style or well-distributed keys

### Don't Use When:
❌ Need mutable/updateable data (use HashMap or BTreeMap)
❌ Have sequential key patterns (MPHF construction fails)
❌ Frequently reload data (slow MPHF rebuild)
❌ Need incremental updates (requires full rebuild)

## API Reference

### Core Operations
```rust
// Construction
VerifiedKvStore::new(data: HashMap<K, V>) -> Result<Self, KvError>

// Lookups
get(&key) -> Result<&V, KvError>              // Fast, zero-allocation
get_detailed(&key) -> Result<&V, KvError>     // With detailed error messages
contains_key(&key) -> bool                     // Accurate, no false positives
len() -> usize
is_empty() -> bool
```

### Iteration
```rust
iter() -> impl Iterator<Item = (&K, &V)>
keys() -> impl Iterator<Item = &K>
values() -> impl Iterator<Item = &V>
```

### Persistence
```rust
save_to_file(path) -> Result<(), KvError>
load_from_file(path) -> Result<Self, KvError>
```

### Analysis
```rust
memory_usage_bytes() -> usize
```

## Examples

```bash
# Basic usage demonstration
cargo run --example basic_usage --release

# Performance and cache analysis
cargo run --example cache_analysis --release

# Component-level profiling
cargo run --example component_analysis --release
```

## Benchmarks

```bash
# Run all benchmarks
cargo bench --bench verified_store_bench

# View HTML reports
open target/criterion/report/index.html
```

Benchmark groups:
- `verified_store_lookups` - Lookup performance with 1K keys
- `key_length_impact` - Performance across key sizes (64B to 2KB)
- `construction` - MPHF construction time by dataset size

## Optimization Tips

1. **Use shorter keys** - Performance scales linearly with key length (0.08-0.15 ns/byte)
2. **Use `get()` not `get_detailed()`** - Avoids string allocation in hot paths
3. **Consider numeric keys** - Faster hashing than strings
4. **Build with `--release`** - Enables all optimizations (26-83% faster)
5. **For keys >1KB** - Consider storing hashes as keys instead

For advanced optimization strategies, see [PERFORMANCE_ANALYSIS.md](PERFORMANCE_ANALYSIS.md).

## Implementation Details

This library uses a patched version of PtrHash that fixes:
- Mathematical overflow in partitioning logic
- Instability for small datasets (<10K keys)
- Performance anomalies with forced single-part construction

Key improvements:
- Zero-allocation error paths
- CPU-specific optimizations (AES-NI for hashing)
- Better small dataset handling
- LTO and codegen optimizations

## Project Structure

```
learned-kv/
├── src/
│   ├── lib.rs                 # Main library interface
│   ├── verified_kv_store.rs   # Core implementation
│   ├── persistence.rs         # Serialization layer
│   ├── error.rs               # Error types
│   └── main.rs                # Demo binary
├── examples/                   # Usage examples and profiling tools
├── benches/                    # Criterion benchmarks
└── ptr_hash_patched/          # Patched PtrHash dependency
```

## Contributing

Contributions welcome! Please ensure:
```bash
cargo test          # All tests pass
cargo clippy        # No warnings
cargo fmt           # Formatted code
```

## License

MIT License. Original PtrHash by [Ragnar Groot Koerkamp](https://github.com/RagnarGrootKoerkamp/ptrhash).

## Acknowledgments

Based on the PtrHash algorithm with mathematical stability fixes and performance optimizations applied.
