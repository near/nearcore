use lru::LruCache;
use std::collections::HashMap;
use std::hash::Hash;

/// `cache_to_hashmap` - converts SizedCache<K, V> to HashMap<K, V>
pub fn cache_to_hashmap<K: Hash + Eq + Clone, V: Clone>(cache: &LruCache<K, V>) -> HashMap<K, V> {
    cache.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
}
