use crate::error::KvError;
use ptr_hash::{PtrHash, PtrHashParams};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::path::Path;

#[derive(Clone)]
pub struct LearnedKvStore<K, V> 
where
    K: Clone + std::hash::Hash + Eq + std::fmt::Debug + Send + Sync,
    V: Clone,
{
    mphf: PtrHash<K>,
    values: Vec<Option<V>>,
    keys: Vec<K>,
}

impl<K, V> LearnedKvStore<K, V>
where
    K: Clone + std::hash::Hash + Eq + std::fmt::Debug + Send + Sync,
    V: Clone,
{
    pub fn new(data: HashMap<K, V>) -> Result<Self, KvError> {
        if data.is_empty() {
            return Err(KvError::EmptyKeySet);
        }

        let keys: Vec<K> = data.keys().cloned().collect();
        let n = keys.len();
        
        let mphf = PtrHash::new(&keys, PtrHashParams::default());
        
        let mut values = vec![None; n];
        let mut key_array = vec![None; n];
        
        for (key, value) in data {
            let index = mphf.index(&key) as usize;
            values[index] = Some(value);
            key_array[index] = Some(key);
        }
        
        let final_keys: Vec<K> = key_array.into_iter().map(|k| k.unwrap()).collect();

        Ok(Self {
            mphf,
            values,
            keys: final_keys,
        })
    }

    /// Fast lookup with zero-allocation error handling.
    /// Use this method for high-performance scenarios where error details aren't needed.
    pub fn get(&self, key: &K) -> Result<&V, KvError> {
        let index = self.mphf.index(key) as usize;
        
        if index < self.keys.len() && self.keys[index] == *key {
            match &self.values[index] {
                Some(value) => Ok(value),
                None => Err(KvError::KeyNotFoundFast), // Zero-allocation error for performance
            }
        } else {
            Err(KvError::KeyNotFoundFast) // Zero-allocation error for performance
        }
    }

    /// Lookup with detailed error messages (slower due to string formatting).
    /// Use this method when you need detailed error information for debugging.
    pub fn get_detailed(&self, key: &K) -> Result<&V, KvError> {
        let index = self.mphf.index(key) as usize;
        
        if index < self.keys.len() && self.keys[index] == *key {
            match &self.values[index] {
                Some(value) => Ok(value),
                None => Err(KvError::KeyNotFound {
                    key: format!("{:?}", key),
                }),
            }
        } else {
            Err(KvError::KeyNotFound {
                key: format!("{:?}", key),
            })
        }
    }

    pub fn contains_key(&self, key: &K) -> bool {
        let index = self.mphf.index(key) as usize;
        index < self.keys.len() && self.keys[index] == *key && self.values[index].is_some()
    }

    pub fn len(&self) -> usize {
        self.values.iter().filter(|v| v.is_some()).count()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn keys(&self) -> impl Iterator<Item = &K> {
        self.keys
            .iter()
            .enumerate()
            .filter(|(i, _)| self.values[*i].is_some())
            .map(|(_, key)| key)
    }

    pub fn values(&self) -> impl Iterator<Item = &V> {
        self.values.iter().filter_map(|v| v.as_ref())
    }

    pub fn iter(&self) -> impl Iterator<Item = (&K, &V)> {
        self.keys
            .iter()
            .enumerate()
            .filter_map(|(i, key)| {
                self.values[i].as_ref().map(|value| (key, value))
            })
    }

    pub fn memory_usage_bytes(&self) -> usize {
        std::mem::size_of::<Self>() +
        self.values.capacity() * std::mem::size_of::<Option<V>>() +
        self.keys.capacity() * std::mem::size_of::<K>()
    }
}

impl<K, V> LearnedKvStore<K, V>
where
    K: Clone + std::hash::Hash + Eq + std::fmt::Debug + Send + Sync + Serialize + for<'de> Deserialize<'de>,
    V: Clone + Serialize + for<'de> Deserialize<'de>,
{
    pub fn save_to_file<P: AsRef<Path>>(&self, path: P) -> Result<(), KvError> {
        let file = File::create(path)?;
        let writer = BufWriter::new(file);
        
        let serializable_data = (
            &self.keys,
            &self.values,
        );
        
        bincode::serialize_into(writer, &serializable_data)?;
        Ok(())
    }

    pub fn load_from_file<P: AsRef<Path>>(path: P) -> Result<Self, KvError> {
        let file = File::open(path)?;
        let reader = BufReader::new(file);
        
        let (keys, values): (Vec<K>, Vec<Option<V>>) = bincode::deserialize_from(reader)?;
        
        let mphf = PtrHash::new(&keys, PtrHashParams::default());
        
        Ok(Self {
            mphf,
            values,
            keys,
        })
    }
}

pub struct KvStoreBuilder<K, V> {
    data: HashMap<K, V>,
}

impl<K, V> KvStoreBuilder<K, V>
where
    K: Clone + std::hash::Hash + Eq + std::fmt::Debug + Send + Sync,
    V: Clone,
{
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
        }
    }

    pub fn insert(mut self, key: K, value: V) -> Self {
        self.data.insert(key, value);
        self
    }

    pub fn extend<I>(mut self, iter: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
    {
        self.data.extend(iter);
        self
    }

    pub fn from_iter<I>(iter: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
    {
        Self {
            data: HashMap::from_iter(iter),
        }
    }

    pub fn build(self) -> Result<LearnedKvStore<K, V>, KvError> {
        LearnedKvStore::new(self.data)
    }
}

impl<K, V> Default for KvStoreBuilder<K, V>
where
    K: Clone + std::hash::Hash + Eq + std::fmt::Debug + Send + Sync,
    V: Clone,
{
    fn default() -> Self {
        Self::new()
    }
}

