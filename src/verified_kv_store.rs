//! VerifiedKvStore: Safe variant with key verification
//!
//! This variant keeps keys in memory and verifies every lookup, trading memory for safety.
//! Use this when you need strong guarantees that lookups won't return incorrect values.

use crate::error::KvError;
use ptr_hash::bucket_fn::Linear;
use ptr_hash::hash::StringHash;
use ptr_hash::{PtrHash, PtrHashParams};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;

/// Safe key-value store that verifies every lookup.
///
/// Designed for String → Value mappings with:
/// - O(1) lookups using Minimal Perfect Hash Functions (MPHF)
/// - Key verification (returns errors for non-existent keys, never wrong values)
/// - Full serialization and persistence support
/// - Complete API: iter(), keys(), values(), contains_key()
///
/// Uses GxHash (AES-NI accelerated) for optimal string key distribution.
///
/// Generic Parameters:
/// - `V`: Value type (must be cloneable and serializable)
#[derive(Clone)]
pub struct VerifiedKvStore<V>
where
    V: Clone,
{
    mphf: PtrHash<String, Linear, Vec<u32>, StringHash, Vec<u8>>,
    values: Vec<V>,
    keys: Vec<String>, // Keep keys for verification
    len: usize,
}

// Main implementation
impl<V> VerifiedKvStore<V>
where
    V: Clone,
{
    /// Create a new VerifiedKvStore from a HashMap with String keys.
    ///
    /// Uses GxHash (AES-NI accelerated) which provides excellent hash distribution
    /// for all string patterns - sequential, random, UUID-style, etc.
    ///
    /// # Example
    /// ```
    /// use learned_kv::VerifiedKvStore;
    /// use std::collections::HashMap;
    ///
    /// let mut data = HashMap::new();
    /// data.insert("key1".to_string(), "value1".to_string());
    /// data.insert("key2".to_string(), "value2".to_string());
    /// let store = VerifiedKvStore::new(data).unwrap();
    ///
    /// assert_eq!(store.get(&"key1".to_string()).unwrap(), "value1");
    /// ```
    pub fn new(data: HashMap<String, V>) -> Result<Self, KvError> {
        if data.is_empty() {
            return Err(KvError::EmptyKeySet);
        }

        let keys: Vec<String> = data.keys().cloned().collect();
        let n = keys.len();

        let mphf = PtrHash::new(&keys, PtrHashParams::default());

        // Allocate values vector
        let mut values: Vec<V> = Vec::with_capacity(n);
        // SAFETY: We're about to initialize all n elements via ptr::write
        // Clippy false positive: we DO initialize all elements via ptr::write below
        #[allow(clippy::uninit_vec)]
        unsafe {
            values.set_len(n);
        }

        // Allocate keys vector for verification
        let mut key_array: Vec<String> = Vec::with_capacity(n);
        // SAFETY: We're about to initialize all n elements via ptr::write
        // Clippy false positive: we DO initialize all elements via ptr::write below
        #[allow(clippy::uninit_vec)]
        unsafe {
            key_array.set_len(n);
        }

        // Track which indices are written (for debug verification)
        #[cfg(debug_assertions)]
        let mut written = vec![false; n];

        // Fill both arrays
        for (key, value) in data {
            let index = mphf.index(&key);

            // Verify MPHF guarantees
            debug_assert!(index < n, "MPHF returned index {} >= n ({})", index, n);

            #[cfg(debug_assertions)]
            {
                debug_assert!(
                    !written[index],
                    "MPHF collision: index {} written twice",
                    index
                );
                written[index] = true;
            }

            // SAFETY:
            // 1. index < n (verified by debug_assert, guaranteed by MPHF for release)
            // 2. We allocated exactly n slots via set_len
            // 3. MPHF guarantees each index is used exactly once (minimal perfect hash)
            unsafe {
                std::ptr::write(values.as_mut_ptr().add(index), value);
                std::ptr::write(key_array.as_mut_ptr().add(index), key);
            }
        }

        // Verify all slots were initialized
        #[cfg(debug_assertions)]
        debug_assert!(
            written.iter().all(|&w| w),
            "MPHF bug: not all indices were written. Missing: {:?}",
            written
                .iter()
                .enumerate()
                .filter(|(_, &w)| !w)
                .map(|(i, _)| i)
                .collect::<Vec<_>>()
        );

        Ok(Self {
            mphf,
            values,
            keys: key_array,
            len: n,
        })
    }

    /// Fast lookup with key verification.
    ///
    /// This method:
    /// - Verifies the key matches (safe, no wrong values)
    /// - Returns errors for non-existent keys
    /// - Uses MPHF for O(1) lookup time
    #[inline(always)]
    pub fn get(&self, key: &String) -> Result<&V, KvError> {
        let index = self.mphf.index(key);

        // Must use safe indexing because we don't know if this is the right key
        // until AFTER we check. For non-existent keys, MPHF returns *some* index
        // which might be < len but point to a different key.
        if index < self.len && self.keys[index] == *key {
            Ok(&self.values[index])
        } else {
            Err(KvError::KeyNotFoundFast)
        }
    }

    /// Lookup with detailed error messages.
    pub fn get_detailed(&self, key: &String) -> Result<&V, KvError> {
        let index = self.mphf.index(key);

        if index < self.len && self.keys[index] == *key {
            Ok(&self.values[index])
        } else {
            Err(KvError::KeyNotFound {
                key: format!("{:?}", key),
            })
        }
    }

    /// Check if a key is in the store (accurate, no false positives).
    #[inline(always)]
    pub fn contains_key(&self, key: &String) -> bool {
        let index = self.mphf.index(key);
        index < self.len && self.keys[index] == *key
    }

    /// Returns the number of key-value pairs in the store.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Check if the store is empty.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Returns an iterator over all keys in the store.
    pub fn keys(&self) -> impl Iterator<Item = &String> {
        self.keys.iter()
    }

    /// Returns an iterator over all values in the store.
    pub fn values(&self) -> impl Iterator<Item = &V> {
        self.values.iter()
    }

    /// Returns an iterator over all key-value pairs.
    pub fn iter(&self) -> impl Iterator<Item = (&String, &V)> {
        self.keys.iter().zip(self.values.iter())
    }

    /// Returns the approximate **stack-allocated** memory usage in bytes.
    ///
    /// ⚠️ **IMPORTANT - INCOMPLETE MEASUREMENT:**
    /// - Only measures stack-allocated memory (Vec capacities × size_of types)
    /// - For `String`, `Vec`, etc: Reports struct size (24 bytes), NOT heap data
    /// - Does NOT include MPHF internal structures (~2-4 bits per key)
    ///
    /// **Use for:**
    /// - Relative memory comparisons between datasets
    /// - Understanding storage overhead
    ///
    /// **For accurate total memory:** Use external profiler or add heap data separately
    pub fn memory_usage_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.values.capacity() * std::mem::size_of::<V>()
            + self.keys.capacity() * std::mem::size_of::<String>()
        // Note: MPHF memory not included (requires mem_dbg feature)
    }
}

// Serialization support
impl<V> VerifiedKvStore<V>
where
    V: Clone + Serialize + for<'de> Deserialize<'de>,
{
    /// Save the store to a file with integrity protection.
    ///
    /// # ⚠️ PERFORMANCE WARNING ⚠️
    ///
    /// **MPHF is NOT saved** - it will be rebuilt on every load:
    /// - 1K keys: ~1-5ms rebuild time
    /// - 100K keys: ~50-100ms rebuild time
    /// - 1M keys: ~500ms-1s rebuild time
    /// - 10M keys: ~5-10s rebuild time
    ///
    /// For applications requiring fast startup, consider:
    /// - Keeping the store in memory (never reload)
    /// - Using a different data structure (e.g., HashMap)
    /// - Pre-building and caching in a long-running process
    ///
    /// # Features
    ///
    /// - Format versioning for evolution
    /// - CRC32 checksum validation
    /// - Atomic writes (no corruption on crash)
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use learned_kv::VerifiedKvStore;
    /// # use std::collections::HashMap;
    /// # let mut data = HashMap::new();
    /// # data.insert("key".to_string(), 42);
    /// # let store = VerifiedKvStore::new(data).unwrap();
    /// store.save_to_file("data.bin")?;
    /// # Ok::<(), learned_kv::KvError>(())
    /// ```
    pub fn save_to_file<P: AsRef<Path>>(&self, path: P) -> Result<(), KvError> {
        use crate::persistence::{write_with_integrity, PersistedData, PersistenceStrategy};

        let data = PersistedData {
            keys: self.keys.clone(),
            values: self.values.clone(),
            mphf_data: None,
        };

        write_with_integrity(path, &data, PersistenceStrategy::RebuildOnLoad)
    }

    /// Load the store from a file with integrity validation.
    ///
    /// # ⚠️ PERFORMANCE WARNING ⚠️
    ///
    /// **MPHF is rebuilt from scratch on every load:**
    /// - This operation is CPU-intensive and can take seconds for large datasets
    /// - See `save_to_file()` documentation for rebuild time estimates
    ///
    /// # Validation
    ///
    /// - Magic number verification
    /// - Format version compatibility
    /// - CRC32 checksum validation
    /// - Key count verification
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// - File format is invalid or corrupted
    /// - Version is incompatible
    /// - Checksum doesn't match
    /// - Data is structurally invalid
    /// - **MPHF construction fails** (can happen with certain key patterns)
    pub fn load_from_file<P: AsRef<Path>>(path: P) -> Result<Self, KvError> {
        use crate::persistence::read_with_validation;

        let (data, _strategy) = read_with_validation(path)?;

        // Calculate length before moving keys
        let n = data.keys.len();

        // Reconstruct MPHF from keys
        // NOTE: MPHF serialization not implemented - always rebuild on load
        let mphf = PtrHash::new(&data.keys, PtrHashParams::default());

        // CRITICAL: New MPHF assigns different indices! Must reorder values to match.
        // Build mapping: key → old_value, then use new MPHF to place values correctly
        let mut reordered_values: Vec<V> = Vec::with_capacity(n);
        // SAFETY: We're about to initialize all n elements
        #[allow(clippy::uninit_vec)]
        unsafe {
            reordered_values.set_len(n);
        }

        let mut reordered_keys: Vec<String> = Vec::with_capacity(n);
        #[allow(clippy::uninit_vec)]
        unsafe {
            reordered_keys.set_len(n);
        }

        // Reorder both keys and values according to new MPHF indices
        for (old_key, old_value) in data.keys.into_iter().zip(data.values.into_iter()) {
            let new_index = mphf.index(&old_key);

            // SAFETY: new_index guaranteed < n by MPHF, each index used exactly once
            unsafe {
                std::ptr::write(reordered_values.as_mut_ptr().add(new_index), old_value);
                std::ptr::write(reordered_keys.as_mut_ptr().add(new_index), old_key);
            }
        }

        Ok(Self {
            mphf,
            values: reordered_values,
            keys: reordered_keys,
            len: n,
        })
    }
}

/// Builder for constructing VerifiedKvStore instances.
pub struct VerifiedKvStoreBuilder<V> {
    data: HashMap<String, V>,
}

impl<V> VerifiedKvStoreBuilder<V>
where
    V: Clone,
{
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
        }
    }

    pub fn insert(mut self, key: String, value: V) -> Self {
        self.data.insert(key, value);
        self
    }

    pub fn extend<I>(mut self, iter: I) -> Self
    where
        I: IntoIterator<Item = (String, V)>,
    {
        self.data.extend(iter);
        self
    }

    pub fn with_entries<I>(iter: I) -> Self
    where
        I: IntoIterator<Item = (String, V)>,
    {
        Self {
            data: HashMap::from_iter(iter),
        }
    }

    pub fn build(self) -> Result<VerifiedKvStore<V>, KvError> {
        VerifiedKvStore::new(self.data)
    }
}

impl<V> Default for VerifiedKvStoreBuilder<V>
where
    V: Clone,
{
    fn default() -> Self {
        Self::new()
    }
}
