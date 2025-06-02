use std::collections::{HashMap, VecDeque};
use std::hash::Hash;

pub struct FifoCache<K, V> {
    capacity: usize,
    queue: VecDeque<K>,
    map: HashMap<K, V>,
}

impl<K: Eq + Hash + PartialOrd + Clone, V> FifoCache<K, V> {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            queue: VecDeque::with_capacity(capacity),
            map: HashMap::with_capacity(capacity),
        }
    }

    pub fn insert(&mut self, key: K, value: V) {
        if self.map.contains_key(&key) {
            self.map.insert(key, value);
            return;
        }

        if self.queue.len() >= self.capacity {
            if let Some(oldest) = self.queue.pop_front() {
                self.map.remove(&oldest);
            }
        }

        self.queue.push_back(key.clone());
        self.map.insert(key, value);
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        self.map.get(key)
    }

    pub fn remove(&mut self, key: &K) -> Option<V> {
        if let Some(value) = self.map.remove(key) {
            self.queue.retain(|k| k != key);
            return Some(value);
        }
        None
    }

    pub fn remove_all(&mut self) {
        self.queue.clear();
        self.map.clear();
    }

    pub fn remove_range(&mut self, from: &K, to: &K) -> Vec<V> {
        let mut removed_values = Vec::new();
        self.queue.retain(|k| {
            if k >= from && k <= to {
                if let Some(value) = self.map.remove(k) {
                    removed_values.push(value);
                }
                false
            } else {
                true
            }
        });
        removed_values
    }
}
