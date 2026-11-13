use std::num::NonZeroUsize;

/// LRU cache with weight-based eviction policy.
pub struct LruWeightedCache<K, V> {
    current_weight: u64,
    max_weight: u64,
    cache: lru::LruCache<K, LruWeightedCacheEntry<V>>,
}

type LruWeightedCacheEntry<V> = (u64, V);

impl<K: std::hash::Hash + Eq, V> LruWeightedCache<K, V> {
    /// Largest accepted `max_weight`. Capped one below `u64::MAX / 2` so the
    /// transient `current_weight + weight` sum in `insert` can never overflow.
    pub const MAX_WEIGHT: u64 = u64::MAX / 2 - 1;

    pub fn new(item_capacity: NonZeroUsize, max_weight: u64) -> Self {
        Self::with_lru(max_weight, lru::LruCache::new(item_capacity))
    }

    /// Like [`Self::new`], but with no item-count cap — eviction is driven
    /// purely by `max_weight`.
    pub fn without_item_cap(max_weight: u64) -> Self {
        Self::with_lru(max_weight, lru::LruCache::unbounded())
    }

    fn with_lru(max_weight: u64, cache: lru::LruCache<K, LruWeightedCacheEntry<V>>) -> Self {
        assert!(
            max_weight <= Self::MAX_WEIGHT,
            "max_weight must be at most {} to avoid overflows",
            Self::MAX_WEIGHT
        );
        Self { current_weight: 0, max_weight, cache }
    }

    /// Like [`Self::get`], but yields a mutable reference to the stored value.
    /// Also promotes `key` to most-recently-used.
    pub fn get_mut(&mut self, key: &K) -> Option<&mut (u64, V)> {
        self.cache.get_mut(key)
    }

    pub fn get(&mut self, key: &K) -> Option<&(u64, V)> {
        self.cache.get(key)
    }

    /// Insert `key` with `weight` and `value` as MRU. Returns the entries
    /// that were evicted as a result, in eviction order.
    pub fn insert(&mut self, key: K, weight: u64, value: V) -> Vec<(K, V)> {
        // Too big to ever fit: don't store it. Also evict any existing entry
        // for this key so an oversized replacement can't leave a stale one.
        if self.max_weight < weight {
            if let Some((old_weight, _)) = self.cache.pop(&key) {
                self.current_weight -= old_weight;
            }
            return vec![(key, value)];
        }

        let mut evicted = Vec::new();
        // `push` (unlike `put`) returns any evicted entry, whether it was a
        // same-key replacement or the LRU entry dropped due to item_capacity.
        if let Some((evicted_key, (evicted_weight, evicted_value))) =
            self.cache.push(key, (weight, value))
        {
            self.current_weight -= evicted_weight;
            evicted.push((evicted_key, evicted_value));
        }
        self.current_weight += weight;

        while self.max_weight < self.current_weight {
            let (evicted_key, (evicted_weight, evicted_value)) = self
                .cache
                .pop_lru()
                .expect("current_weight >= max_weight implies cache is not empty");
            self.current_weight -= evicted_weight;
            evicted.push((evicted_key, evicted_value));
        }

        evicted
    }

    pub fn put(&mut self, key: K, weight: u64, value: V) {
        let _ = self.insert(key, weight, value);
    }

    /// Removes `key`, returning its weight and value, if it was present.
    pub fn remove(&mut self, key: &K) -> Option<(u64, V)> {
        let (weight, value) = self.cache.pop(key)?;
        self.current_weight -= weight;
        Some((weight, value))
    }

    pub fn len(&self) -> usize {
        self.cache.len()
    }

    pub fn current_weight(&self) -> u64 {
        self.current_weight
    }

    pub fn clear(&mut self) {
        self.current_weight = 0;
        self.cache.clear();
    }

    pub fn contains(&self, key: &K) -> bool {
        self.cache.contains(key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn item_capacity_eviction_tracks_weight() {
        // Regression test: when lru::LruCache silently evicts an entry due to
        // item_capacity, the evicted entry's weight must be subtracted from
        // current_weight. Otherwise the tracked weight drifts upward and
        // triggers spurious weight-based evictions.
        let item_capacity = NonZeroUsize::new(2).unwrap();
        let max_weight = 25;
        let mut cache = LruWeightedCache::<&str, u32>::new(item_capacity, max_weight);

        cache.put("a", 10, 1);
        cache.put("b", 10, 2);
        // At this point: items={a,b}, current_weight should be 20.

        // Inserting "c" causes lru::LruCache to silently evict "a" (the LRU).
        // Total actual weight is now 20 (b=10 + c=10) which fits in max_weight=25.
        cache.put("c", 10, 3);

        // "b" should still be in the cache because total weight (20) <= max_weight (25).
        assert!(
            cache.contains(&"b"),
            "item 'b' was spuriously evicted due to inflated current_weight"
        );
        assert!(cache.contains(&"c"), "item 'c' should be in cache");
        assert!(!cache.contains(&"a"), "item 'a' should have been evicted by item_capacity");
    }

    #[test]
    fn same_key_replace_tracks_weight() {
        let item_capacity = NonZeroUsize::new(2).unwrap();
        let max_weight = 25;
        let mut cache = LruWeightedCache::<&str, u32>::new(item_capacity, max_weight);

        cache.put("a", 20, 1); // current_weight = 20
        cache.put("a", 5, 2); // current_weight = 5 (old weight 20 subtracted)
        cache.put("b", 20, 3); // current_weight = 25, fits in max_weight

        assert!(cache.contains(&"a"), "item 'a' should still be in cache");
        assert!(cache.contains(&"b"), "item 'b' should still be in cache");
    }

    #[test]
    fn insert_returns_weight_evictions_in_order() {
        // Item cap is generous; eviction must be weight-driven.
        let item_capacity = NonZeroUsize::new(10).unwrap();
        let max_weight = 10;
        let mut cache = LruWeightedCache::<&str, u32>::new(item_capacity, max_weight);

        assert!(cache.insert("a", 4, 1).is_empty());
        assert!(cache.insert("b", 4, 2).is_empty()); // current_weight = 8
        // Promote "a" so "b" is LRU.
        cache.get_mut(&"a");

        // Inserting "c" (weight 4) pushes current_weight to 12, must evict
        // exactly the LRU entry "b".
        let evicted = cache.insert("c", 4, 3);
        assert_eq!(evicted, vec![("b", 2)]);
        assert!(cache.contains(&"a"));
        assert!(!cache.contains(&"b"));
        assert!(cache.contains(&"c"));
    }

    #[test]
    fn insert_oversized_returns_self() {
        let item_capacity = NonZeroUsize::new(10).unwrap();
        let max_weight = 5;
        let mut cache = LruWeightedCache::<&str, u32>::new(item_capacity, max_weight);

        // weight > max_weight: entry must not be stored, and must be returned
        // so the caller can clean up any side effect.
        let evicted = cache.insert("too_big", 100, 42);
        assert_eq!(evicted, vec![("too_big", 42)]);
        assert!(!cache.contains(&"too_big"));
        assert_eq!(cache.current_weight(), 0);
    }

    #[test]
    fn insert_oversized_drops_existing_entry() {
        let item_capacity = NonZeroUsize::new(10).unwrap();
        let max_weight = 10;
        let mut cache = LruWeightedCache::<&str, u32>::new(item_capacity, max_weight);

        assert!(cache.insert("a", 5, 1).is_empty());
        assert!(cache.contains(&"a"));

        // An oversized replacement for an existing key must drop the old entry
        // (and reclaim its weight), not leave it tracked.
        let evicted = cache.insert("a", 100, 2);
        assert_eq!(evicted, vec![("a", 2)]);
        assert!(!cache.contains(&"a"), "old entry must be dropped");
        assert_eq!(cache.current_weight(), 0, "old entry's weight must be reclaimed");
    }

    #[test]
    fn insert_same_key_replace_returns_old() {
        let item_capacity = NonZeroUsize::new(10).unwrap();
        let max_weight = 100;
        let mut cache = LruWeightedCache::<&str, u32>::new(item_capacity, max_weight);

        assert!(cache.insert("a", 10, 1).is_empty());
        // Replacing "a" must report the previous value and weight, and must
        // not cascade into a weight eviction because the new weight fits.
        let evicted = cache.insert("a", 20, 2);
        assert_eq!(evicted, vec![("a", 1)]);
        assert!(cache.contains(&"a"));
        assert_eq!(cache.current_weight(), 20);
    }

    #[test]
    fn insert_item_capacity_eviction_returned() {
        // When `lru::LruCache` drops the LRU entry due to item_capacity (not
        // weight), `insert` must still surface it so callers can clean up.
        let item_capacity = NonZeroUsize::new(2).unwrap();
        let max_weight = 100;
        let mut cache = LruWeightedCache::<&str, u32>::new(item_capacity, max_weight);

        assert!(cache.insert("a", 1, 1).is_empty());
        assert!(cache.insert("b", 1, 2).is_empty());
        let evicted = cache.insert("c", 1, 3);
        assert_eq!(evicted, vec![("a", 1)]);
    }

    #[test]
    fn unbounded_evicts_only_on_weight() {
        // With an unbounded item cap, only weight should trigger eviction.
        let max_weight = 10;
        let mut cache = LruWeightedCache::<&str, u32>::without_item_cap(max_weight);

        // Many small entries fit despite there being no item cap.
        for i in 0..10 {
            let evicted = cache.insert(NAMES[i], 1, i as u32);
            assert!(evicted.is_empty(), "insert {i} should not evict");
        }
        assert_eq!(cache.len(), 10);
        assert_eq!(cache.current_weight(), 10);

        // Promote "n0" so it is MRU; "n1" becomes LRU.
        cache.get_mut(&"n0");

        // Inserting one more entry must evict exactly one LRU.
        let evicted = cache.insert("extra", 1, 99);
        assert_eq!(evicted, vec![("n1", 1)]);
        assert!(cache.contains(&"n0"));
        assert!(!cache.contains(&"n1"));
        assert!(cache.contains(&"extra"));
    }

    #[test]
    fn remove_reclaims_weight() {
        let max_weight = 10;
        let mut cache = LruWeightedCache::<&str, u32>::without_item_cap(max_weight);

        assert!(cache.insert("a", 6, 1).is_empty());
        assert!(cache.insert("b", 4, 2).is_empty());
        assert_eq!(cache.remove(&"a"), Some((6, 1)));
        assert_eq!(cache.remove(&"a"), None);
        assert!(!cache.contains(&"a"));
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.current_weight(), 4);

        // The reclaimed weight is available again: "c" fits without evicting "b".
        assert!(cache.insert("c", 6, 3).is_empty());
        assert!(cache.contains(&"b"));
    }

    const NAMES: [&str; 10] = ["n0", "n1", "n2", "n3", "n4", "n5", "n6", "n7", "n8", "n9"];
}
