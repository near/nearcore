use parking_lot::RwLock;
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;

pub struct AppendOnlyMap<K, V> {
    map: RwLock<HashMap<K, Arc<V>>>,
}

impl<K, V> AppendOnlyMap<K, V>
where
    K: Eq + Hash + Clone,
{
    pub fn new() -> Self {
        Self { map: RwLock::new(HashMap::new()) }
    }

    pub fn get_or_insert<F: FnOnce() -> V>(&self, key: &K, value: F) -> Arc<V> {
        let mut map = self.map.write();
        if !map.contains_key(key) {
            map.insert(key.clone(), Arc::new(value()));
        }
        map.get(key).unwrap().clone()
    }
}
