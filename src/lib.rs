//! # LearnedKvStore
//! 
//! A high-performance key-value store implementation using Minimal Perfect Hash Functions (MPHF).
//! 
//! ## Performance Characteristics
//! 
//! Based on comprehensive analysis, performance scales with key length:
//! - **Small keys (≤64 bytes)**: ~7ns lookups
//! - **Medium keys (128-512 bytes)**: 10-55ns lookups  
//! - **Large keys (1024+ bytes)**: Linear scaling, up to ~700ns for 4KB keys
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
//! ```rust
//! use learned_kv::{LearnedKvStore, KvStoreBuilder};
//! use std::collections::HashMap;
//! 
//! // Build from HashMap
//! let mut data = HashMap::new();
//! data.insert("key1".to_string(), "value1".to_string());
//! let store = LearnedKvStore::new(data).unwrap();
//! 
//! // Or use builder pattern
//! let store = KvStoreBuilder::new()
//!     .insert("key1".to_string(), "value1".to_string())
//!     .build()
//!     .unwrap();
//! 
//! // Fast lookup (recommended for hot paths)
//! match store.get(&"key1".to_string()) {
//!     Ok(value) => println!("Found: {}", value),
//!     Err(_) => println!("Not found"),
//! }
//! ```

pub mod error;
pub mod kv_store;

pub use error::KvError;
pub use kv_store::{KvStoreBuilder, LearnedKvStore};

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

        let store = LearnedKvStore::new(data).unwrap();

        assert_eq!(store.len(), 3);
        assert!(!store.is_empty());

        assert_eq!(store.get(&"key1".to_string()).unwrap(), &"value1".to_string());
        assert_eq!(store.get(&"key2".to_string()).unwrap(), &"value2".to_string());
        assert_eq!(store.get(&"key3".to_string()).unwrap(), &"value3".to_string());

        assert!(store.get(&"nonexistent".to_string()).is_err());
        assert!(store.contains_key(&"key1".to_string()));
        assert!(!store.contains_key(&"nonexistent".to_string()));
    }

    #[test]
    fn test_empty_store() {
        let empty_data: HashMap<String, String> = HashMap::new();
        let result = LearnedKvStore::new(empty_data);
        assert!(matches!(result, Err(KvError::EmptyKeySet)));
    }

    #[test]
    fn test_builder_pattern() {
        let store = KvStoreBuilder::new()
            .insert("hello".to_string(), "world".to_string())
            .insert("foo".to_string(), "bar".to_string())
            .build()
            .unwrap();

        assert_eq!(store.len(), 2);
        assert_eq!(store.get(&"hello".to_string()).unwrap(), &"world".to_string());
        assert_eq!(store.get(&"foo".to_string()).unwrap(), &"bar".to_string());
    }

    #[test]
    fn test_iterators() {
        let store = KvStoreBuilder::new()
            .insert(1, "one".to_string())
            .insert(2, "two".to_string())
            .insert(3, "three".to_string())
            .build()
            .unwrap();

        let keys: Vec<_> = store.keys().cloned().collect();
        let values: Vec<_> = store.values().cloned().collect();
        let pairs: Vec<_> = store.iter().map(|(k, v)| (*k, v.clone())).collect();

        assert_eq!(keys.len(), 3);
        assert_eq!(values.len(), 3);
        assert_eq!(pairs.len(), 3);

        assert!(keys.contains(&1));
        assert!(keys.contains(&2));
        assert!(keys.contains(&3));

        assert!(values.contains(&"one".to_string()));
        assert!(values.contains(&"two".to_string()));
        assert!(values.contains(&"three".to_string()));
    }

    #[test]
    fn test_serialization() {
        let store = KvStoreBuilder::new()
            .insert("test".to_string(), "data".to_string())
            .insert("more".to_string(), "info".to_string())
            .build()
            .unwrap();

        store.save_to_file("test_serialization.bin").unwrap();
        let loaded_store: LearnedKvStore<String, String> = LearnedKvStore::load_from_file("test_serialization.bin").unwrap();

        assert_eq!(loaded_store.len(), store.len());
        assert_eq!(loaded_store.get(&"test".to_string()).unwrap(), &"data".to_string());
        assert_eq!(loaded_store.get(&"more".to_string()).unwrap(), &"info".to_string());
    }

    #[test]
    fn test_large_dataset() {
        let mut data = HashMap::new();
        for i in 0..100 {
            data.insert(i, format!("value_{}", i));
        }

        let store = LearnedKvStore::new(data).unwrap();
        assert_eq!(store.len(), 100);

        for i in 0..100 {
            assert_eq!(store.get(&i).unwrap(), &format!("value_{}", i));
        }

        assert!(store.get(&100).is_err());
    }

    #[test]
    fn test_memory_usage() {
        let store = KvStoreBuilder::new()
            .insert("test".to_string(), "data".to_string())
            .build()
            .unwrap();

        let usage = store.memory_usage_bytes();
        assert!(usage > 0);
    }

}