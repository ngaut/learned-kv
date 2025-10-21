//! # VerifiedKvStore
//!
//! A high-performance **String key-value store** using Minimal Perfect Hash Functions (MPHF).
//!
//! ## Features
//!
//! - **String keys only** - Simplified, no type confusion
//! - **O(1) lookups** - 5-300ns depending on key size
//! - **Safe verification** - Returns errors for missing keys, never wrong values
//! - **GxHash** - AES-NI accelerated, handles all string patterns
//! - **Immutable** - Built once, read many times
//! - **Serializable** - Save/load to disk
//!
//! ## Performance
//!
//! Based on optimized release builds:
//! - **64 bytes**: ~5ns lookups
//! - **128-512 bytes**: ~10-52ns lookups
//! - **1KB-2KB**: ~133-318ns lookups (hash-bound)
//!
//! ## Quick Start
//!
//! ```rust
//! use learned_kv::VerifiedKvStore;
//! use std::collections::HashMap;
//!
//! // Build from HashMap
//! let mut data = HashMap::new();
//! data.insert("key1".to_string(), "value1".to_string());
//! data.insert("key2".to_string(), "value2".to_string());
//! let store = VerifiedKvStore::new(data).unwrap();
//!
//! // Query
//! assert_eq!(store.get(&"key1".to_string()).unwrap(), "value1");
//!
//! // Iterate
//! for (key, value) in store.iter() {
//!     println!("{}: {}", key, value);
//! }
//!
//! // Serialize
//! store.save_to_file("data.bin").unwrap();
//! let loaded: VerifiedKvStore<String> = VerifiedKvStore::load_from_file("data.bin").unwrap();
//! # std::fs::remove_file("data.bin").ok();
//! ```

pub mod error;
pub mod verified_kv_store;

// Persistence is internal implementation detail
mod persistence;

pub use error::KvError;
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

        let store = VerifiedKvStore::new(data).unwrap();

        assert_eq!(store.len(), 3);
        assert!(!store.is_empty());

        assert_eq!(store.get(&"key1".to_string()).unwrap(), "value1");
        assert_eq!(store.get(&"key2".to_string()).unwrap(), "value2");
        assert_eq!(store.get(&"key3".to_string()).unwrap(), "value3");

        assert!(store.contains_key(&"key1".to_string()));
        assert!(!store.contains_key(&"nonexistent".to_string()));
    }

    #[test]
    fn test_empty_store() {
        let empty_data: HashMap<String, String> = HashMap::new();
        let result = VerifiedKvStore::new(empty_data);
        assert!(matches!(result, Err(KvError::EmptyKeySet)));
    }

    #[test]
    fn test_builder_pattern() {
        let store: VerifiedKvStore<String> = VerifiedKvStoreBuilder::new()
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
        let store: VerifiedKvStore<String> = VerifiedKvStoreBuilder::new()
            .insert("1".to_string(), "one".to_string())
            .insert("2".to_string(), "two".to_string())
            .insert("3".to_string(), "three".to_string())
            .build()
            .unwrap();

        let values: Vec<_> = store.values().cloned().collect();

        assert_eq!(values.len(), 3);
        assert!(values.contains(&"one".to_string()));
        assert!(values.contains(&"two".to_string()));
        assert!(values.contains(&"three".to_string()));
    }

    #[test]
    fn test_serialization() {
        let store: VerifiedKvStore<String> = VerifiedKvStoreBuilder::new()
            .insert("test".to_string(), "data".to_string())
            .insert("more".to_string(), "info".to_string())
            .build()
            .unwrap();

        let test_file = "/tmp/test_verified_serialization.bin";

        // Save should succeed
        assert!(store.save_to_file(test_file).is_ok());

        // Load should succeed
        let loaded: VerifiedKvStore<String> =
            VerifiedKvStore::load_from_file(test_file).unwrap();

        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded.get(&"test".to_string()).unwrap(), "data");

        // Cleanup
        std::fs::remove_file(test_file).ok();
    }

    #[test]
    fn test_large_dataset() {
        let mut data = HashMap::new();
        for i in 0..100 {
            data.insert(format!("key_{}", i), format!("value_{}", i));
        }

        // Using new() for String keys
        let store = VerifiedKvStore::new(data).unwrap();
        assert_eq!(store.len(), 100);

        // Verify all keys we inserted work correctly
        for i in 0..100 {
            assert_eq!(store.get(&format!("key_{}", i)).unwrap(), &format!("value_{}", i));
        }
    }

    #[test]
    fn test_memory_usage() {
        let store: VerifiedKvStore<String> = VerifiedKvStoreBuilder::new()
            .insert("test".to_string(), "data".to_string())
            .build()
            .unwrap();

        let usage = store.memory_usage_bytes();
        assert!(usage > 0);
    }
}
