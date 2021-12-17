use lru::LruCache;
use std::hash::Hash;
use std::sync::Mutex;

/// A wrapper around `LruCache`. This struct is thread safe, doesn't return any references to any
/// elements inside.
pub struct SyncLruCache<K, V> {
    inner: Mutex<LruCache<K, V>>,
}

impl<K, V> SyncLruCache<K, V>
where
    K: Hash + Eq,
    V: Clone,
{
    /// Creats a new `LRU` cache that holds at most `cap` items.
    pub fn new(cap: usize) -> Self {
        Self { inner: Mutex::new(LruCache::<K, V>::new(cap)) }
    }

    /// Return the value of they key in the cache otherwise computes the value and inserts it into
    /// the cache. If the key is already in the cache, they gets gets moved to the head of
    /// the LRU list.
    pub fn get_or_put(&self, key: K, f: impl FnOnce(&K) -> V) -> V {
        self.get(&key).unwrap_or_else(|| {
            let val = f(&key);
            self.inner.lock().unwrap().put(key, val.clone());
            val
        })
    }

    /// Puts a key-value pair into cache. If the key already exists in the cache,
    /// then it updates the key's value.
    pub fn put(&self, key: K, value: V) {
        self.inner.lock().unwrap().put(key, value);
    }

    /// Returns the value of the key in the cache or None if it is not present in the cache.
    /// Moves the key to the head of the LRU list if it exists.
    pub fn get(&self, key: &K) -> Option<V> {
        self.inner.lock().unwrap().get(key).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache() {
        let cache = SyncLruCache::<u64, Vec<u64>>::new(100);

        assert_eq!(cache.get(&0u64), None);
        assert_eq!(cache.get_or_put(123u64, |key| vec![*key, 123]), vec![123u64, 123]);
        assert_eq!(cache.get(&123u64), Some(vec![123u64, 123]));
        assert_eq!(cache.get(&0u64), None);
    }
}
