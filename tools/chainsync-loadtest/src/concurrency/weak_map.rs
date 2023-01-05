use std::cmp::Eq;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::hash::Hash;
use std::ops::Deref;
use std::sync::{Arc, Mutex, Weak};

// WeakMap is a collection of weak pointers.
// Once the last reference to an element of the map is dropped,
// the weak pointer is removed from the map.
pub struct WeakMap<K: Hash + Eq + Clone, V> {
    inner: Mutex<HashMap<K, Weak<Ref<K, V>>>>,
}

// Ref is a wrapper of V, which provides a custom drop()
// to clean up the weak map entry once all references to the
// map element are dropped.
pub struct Ref<K: Hash + Eq + Clone, V> {
    key: K,
    map: Arc<WeakMap<K, V>>,
    value: V,
}

impl<K: Hash + Eq + Clone, V> Deref for Ref<K, V> {
    type Target = V;
    fn deref(&self) -> &V {
        return &self.value;
    }
}

impl<K: Hash + Eq + Clone, V> Drop for Ref<K, V> {
    fn drop(&mut self) {
        // Be careful to not to drop an Arc here,
        // because that might trigger this function
        // recursively and cause a deadlock.
        let mut m = self.map.inner.lock().unwrap();
        if let Entry::Occupied(e) = m.entry(self.key.clone()) {
            if e.get().strong_count() == 0 {
                e.remove_entry();
            }
        }
    }
}

impl<K: Hash + Eq + Clone, V> WeakMap<K, V> {
    pub fn new() -> Arc<Self> {
        return Arc::new(Self { inner: Mutex::new(HashMap::new()) });
    }

    // get() returns a reference to map[key], or None if not present.
    pub fn get(self: &Arc<Self>, key: &K) -> Option<Arc<Ref<K, V>>> {
        let m = self.inner.lock().unwrap();
        return m.get(key).map(|w| w.upgrade()).flatten();
    }

    // get() returns a reference to map[key].
    // Uses new_value to initialize the map entry if missing.
    pub fn get_or_insert(self: &Arc<Self>, key: &K, new_value: impl Fn() -> V) -> Arc<Ref<K, V>> {
        let mut m = self.inner.lock().unwrap();
        if let Some(w) = m.get(key) {
            if let Some(v) = w.upgrade() {
                return v;
            }
        }
        let p = Arc::new(Ref { key: key.clone(), map: self.clone(), value: new_value() });
        m.insert(key.clone(), Arc::downgrade(&p));
        return p;
    }
}
