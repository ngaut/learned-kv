//! # LearnedKvStore
//!
//! A high-performance key-value store implementation using Minimal Perfect Hash Functions (MPHF).
//!
//! ## Performance Characteristics
//!
//! Based on comprehensive benchmarking with optimized release builds:
//! - **Small keys (≤64 bytes)**: ~3-6ns lookups
//! - **Medium keys (128-512 bytes)**: ~10-57ns lookups
//! - **Large keys (1KB-4KB)**: ~146-639ns lookups (hash computation dominates)
//!
//! **Performance bottlenecks:**
//! - Hash computation: 95% of lookup time for large keys
//! - String comparison: ~1-3% of lookup time
//! - MPHF index calculation: <1% of lookup time
//!
//! ## Optimization Recommendations
//!
//! 1. **Use shorter keys** when possible - performance scales linearly with key length
//! 2. **Use `get()` instead of `get_detailed()`** for hot paths (avoids string allocation)
//! 3. **Consider key design** - hash-based or numeric keys perform better than long strings
//!
//! ## Example Usage
//!
//! **Recommended**: Use `VerifiedKvStore` for safe key verification:
//!
//! ```rust
//! use learned_kv::VerifiedKvStore;
//! use std::collections::HashMap;
//!
//! // Build from HashMap (safe variant recommended)
//! let mut data = HashMap::new();
//! data.insert("key1".to_string(), "value1".to_string());
//! data.insert("key2".to_string(), "value2".to_string());
//! let store = VerifiedKvStore::new(data).unwrap();
//!
//! // Safe lookup with key verification
//! match store.get(&"key1".to_string()) {
//!     Ok(value) => println!("Found: {}", value),
//!     Err(_) => println!("Not found"),
//! }
//! ```
//!
//! **For expert users only**: `LearnedKvStore` offers maximum performance but may
//! return wrong values for non-existent keys. See documentation for warnings.

pub mod error;
pub mod kv_store;
pub mod verified_kv_store;

// Persistence is internal implementation detail
mod persistence;

pub use error::KvError;
pub use kv_store::{KvStoreBuilder, LearnedKvStore};
pub use verified_kv_store::{VerifiedKvStore, VerifiedKvStoreBuilder};

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_basic_operations() {
        let mut data = HashMap::new();
        data.insert("key1".to_string(), "value1".to_string());
        data.insert("key2".to_string(), "value2".to_string());
        data.insert("key3".to_string(), "value3".to_string());

        let store: LearnedKvStore<String, String> = LearnedKvStore::new(data).unwrap();

        assert_eq!(store.len(), 3);
        assert!(!store.is_empty());

        assert_eq!(store.get(&"key1".to_string()).unwrap(), "value1");
        assert_eq!(store.get(&"key2".to_string()).unwrap(), "value2");
        assert_eq!(store.get(&"key3".to_string()).unwrap(), "value3");

        // Note: contains_key may have false positives in optimized mode
        assert!(store.contains_key(&"key1".to_string()));
    }

    #[test]
    fn test_empty_store() {
        let empty_data: HashMap<String, String> = HashMap::new();
        let result: Result<LearnedKvStore<String, String>, _> = LearnedKvStore::new(empty_data);
        assert!(matches!(result, Err(KvError::EmptyKeySet)));
    }

    #[test]
    fn test_builder_pattern() {
        let store: LearnedKvStore<String, String> = KvStoreBuilder::new()
            .insert("hello".to_string(), "world".to_string())
            .insert("foo".to_string(), "bar".to_string())
            .build()
            .unwrap();

        assert_eq!(store.len(), 2);
        assert_eq!(store.get(&"hello".to_string()).unwrap(), "world");
        assert_eq!(store.get(&"foo".to_string()).unwrap(), "bar");
    }

    #[test]
    fn test_values_iterator() {
        let store: LearnedKvStore<i32, String> = KvStoreBuilder::new()
            .insert(1, "one".to_string())
            .insert(2, "two".to_string())
            .insert(3, "three".to_string())
            .build()
            .unwrap();

        // Only values() available in optimized mode
        let values: Vec<_> = store.values().cloned().collect();

        assert_eq!(values.len(), 3);
        assert!(values.contains(&"one".to_string()));
        assert!(values.contains(&"two".to_string()));
        assert!(values.contains(&"three".to_string()));
    }

    #[test]
    fn test_serialization_disabled() {
        let store: LearnedKvStore<String, String> = KvStoreBuilder::new()
            .insert("test".to_string(), "data".to_string())
            .insert("more".to_string(), "info".to_string())
            .build()
            .unwrap();

        // Serialization is disabled in optimized mode
        assert!(store.save_to_file("test_serialization.bin").is_err());
        assert!(
            LearnedKvStore::<String, String>::load_from_file("test_serialization.bin").is_err()
        );
    }

    #[test]
    fn test_large_dataset() {
        let mut data = HashMap::new();
        for i in 0..100 {
            data.insert(i, format!("value_{}", i));
        }

        let store: LearnedKvStore<i32, String> = LearnedKvStore::new(data).unwrap();
        assert_eq!(store.len(), 100);

        // Verify all keys we inserted work correctly
        for i in 0..100 {
            assert_eq!(store.get(&i).unwrap(), &format!("value_{}", i));
        }
    }

    #[test]
    fn test_memory_usage() {
        let store: LearnedKvStore<String, String> = KvStoreBuilder::new()
            .insert("test".to_string(), "data".to_string())
            .build()
            .unwrap();

        let usage = store.memory_usage_bytes();
        assert!(usage > 0);
    }
}
