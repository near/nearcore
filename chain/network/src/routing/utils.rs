use std::collections::HashMap;
use std::hash::Hash;

use cached::SizedCache;

/// `cache_to_hashmap` - converts SizedCache<K, V> to HashMap<K, V>
pub fn cache_to_hashmap<K: Hash + Eq + Clone, V: Clone>(cache: &SizedCache<K, V>) -> HashMap<K, V> {
    let keys: Vec<_> = cache.key_order().cloned().collect();
    keys.into_iter().zip(cache.value_order().cloned()).collect()
}
