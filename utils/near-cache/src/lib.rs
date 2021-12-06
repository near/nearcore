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
    K: Hash + Eq + PartialEq + Clone,
    V: Clone,
{
    /// Creates new cache with `capacity`.
    pub fn new(capacity: usize) -> Self {
        Self { inner: Mutex::new(LruCache::<K, V>::new(capacity)) }
    }

    /// Gets value for `key`
    pub fn get_or_insert(&self, key: K, f: impl FnOnce(&K) -> V) -> V
    where
        V: Clone,
    {
        if let Some(result) = self.get(&key) {
            return result;
        }
        let val = f(&key);
        let val_clone = val.clone();
        self.inner.lock().unwrap().put(key, val_clone);
        val
    }

    /// Inserts `value` into map at `key`. If the value exists updated the value.
    pub fn insert(&self, key: K, value: V) {
        self.inner.lock().unwrap().put(key, value);
    }

    /// Gets value for given `key`.
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
        assert_eq!(cache.get_or_insert(123u64, |key| vec![*key, 123]), vec![123u64, 123]);
        assert_eq!(cache.get(&123u64), Some(vec![123u64, 123]));
        assert_eq!(cache.get(&0u64), None);
    }
}
