use cached::SizedCache;
use std::collections::HashMap;
use std::hash::Hash;

/// `cache_to_hashmap` - converts SizedCache<K, V> to HashMap<K, V>
pub fn cache_to_hashmap<K: Hash + Eq + Clone, V: Clone>(cache: &SizedCache<K, V>) -> HashMap<K, V> {
    cache.key_order().cloned().zip(cache.value_order().cloned()).collect()
}
