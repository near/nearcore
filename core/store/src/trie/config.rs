use crate::StoreConfig;
use near_primitives::shard_layout::ShardUId;
use std::collections::HashMap;
use tracing::log::warn;

/// Default number of cache entries.
/// It was chosen to fit into RAM well. RAM spend on trie cache should not exceed 50_000 * 4 (number of shards) *
/// TRIE_LIMIT_CACHED_VALUE_SIZE * 2 (number of caches - for regular and view client) = 0.4 GB.
/// In our tests on a single shard, it barely occupied 40 MB, which is dominated by state cache size
/// with 512 MB limit. The total RAM usage for a single shard was 1 GB.
const TRIE_DEFAULT_SHARD_CACHE_SIZE: u64 = if cfg!(feature = "no_cache") { 1 } else { 50000 };

/// Default total size of values which may simultaneously exist the cache.
/// It is chosen by the estimation of the largest contract storage size we are aware as of 23/08/2022.
const DEFAULT_SHARD_CACHE_TOTAL_SIZE_LIMIT: u64 =
    if cfg!(feature = "no_cache") { 1 } else { 3_000_000_000 };

/// Capacity for the deletions queue.
/// It is chosen to fit all hashes of deleted nodes for 3 completely full blocks.
const DEFAULT_SHARD_CACHE_DELETIONS_QUEUE_CAPACITY: usize =
    if cfg!(feature = "no_cache") { 1 } else { 100_000 };

/// Values above this size (in bytes) are never cached.
/// Note that most of Trie inner nodes are smaller than this - e.g. branches use around 32 * 16 = 512 bytes.
const TRIE_LIMIT_CACHED_VALUE_SIZE: usize = 1000;

/// Stores necessary configuration for the creation of tries.
#[derive(Default)]
pub struct TrieConfig {
    pub shard_cache_config: ShardCacheConfig,
    pub view_shard_cache_config: ShardCacheConfig,
}

pub struct ShardCacheConfig {
    /// Shard cache capacity in number of trie nodes.
    pub default_max_entries: u64,
    /// Limits the sum of all cached value sizes.
    ///
    /// This is useful to limit total memory consumption. However, crucially this
    /// is not a hard limit. It only limits the sum of all cached values, not
    /// factoring in the overhead for each entry.
    pub default_max_total_bytes: u64,
    /// Overrides `default_max_entries` per shard.
    pub override_max_entries: HashMap<ShardUId, u64>,
    /// Overrides `default_max_total_bytes` per shard.
    pub override_max_total_bytes: HashMap<ShardUId, u64>,
}

impl TrieConfig {
    /// Create a new `TrieConfig` with default values or the values specified in `StoreConfig`.
    pub fn from_store_config(store_config: &StoreConfig) -> Self {
        let mut trie_config = TrieConfig::default();

        if !store_config.trie_cache_capacities.is_empty() {
            warn!(target: "store", "`trie_cache_capacities` is deprecated, use `trie_cache` and `view_trie_cache` instead");
            trie_config
                .shard_cache_config
                .override_max_entries
                .extend(store_config.trie_cache_capacities.iter().cloned());
        } else {
            // old default behavior:
            // Temporary solution to make contracts with heavy trie access
            // patterns on shard 3 more stable. Can be removed after
            // implementing flat storage.
            //  can also be replaced with limit
            trie_config
                .shard_cache_config
                .override_max_entries
                .insert(ShardUId { version: 1, shard_id: 3 }, 45_000_000);
        }

        for (shard, shard_config) in &store_config.trie_cache {
            let shard = (*shard).into();
            if let Some(bytes) = shard_config.max_bytes {
                trie_config.shard_cache_config.override_max_total_bytes.insert(shard, bytes);
            }
        }
        for (shard, shard_config) in &store_config.view_trie_cache {
            let shard = (*shard).into();
            if let Some(bytes) = shard_config.max_bytes {
                trie_config.view_shard_cache_config.override_max_total_bytes.insert(shard, bytes);
            }
        }

        trie_config
    }

    /// Shard cache capacity in number of trie nodes.
    pub fn shard_cache_capacity(&self, shard_uid: ShardUId, is_view: bool) -> u64 {
        // TODO: compute this based on total size limit
        if is_view { &self.view_shard_cache_config } else { &self.shard_cache_config }
            .capacity(shard_uid)
    }

    /// Shard cache capacity in total bytes.
    ///
    /// TODO: remove this and compute `shard_cache_capacity` based on total size limit
    pub fn shard_cache_total_size_limit(&self, shard_uid: ShardUId, is_view: bool) -> u64 {
        if is_view { &self.view_shard_cache_config } else { &self.shard_cache_config }
            .total_size_limit(shard_uid)
    }

    /// Size limit in bytes per single value for caching in shard caches.
    pub fn max_cached_value_size() -> usize {
        TRIE_LIMIT_CACHED_VALUE_SIZE
    }

    /// Capacity for deletion queue in which nodes are after unforced eviction.
    ///
    /// The shard cache uses LRU eviction policy for forced evictions. But when a
    /// trie value is overwritten or deleted, the associated nodes are no longer
    /// useful, with the exception of forks.
    /// Thus, deleted and overwritten values are evicted to the deletion queue which
    /// delays the actual eviction.
    pub fn deletions_queue_capacity(&self) -> usize {
        DEFAULT_SHARD_CACHE_DELETIONS_QUEUE_CAPACITY
    }
}

impl ShardCacheConfig {
    fn capacity(&self, shard_uid: ShardUId) -> u64 {
        self.override_max_entries.get(&shard_uid).cloned().unwrap_or(self.default_max_entries)
    }

    fn total_size_limit(&self, shard_uid: ShardUId) -> u64 {
        self.override_max_total_bytes
            .get(&shard_uid)
            .cloned()
            .unwrap_or(self.default_max_total_bytes)
    }
}

impl Default for ShardCacheConfig {
    fn default() -> Self {
        Self {
            default_max_entries: TRIE_DEFAULT_SHARD_CACHE_SIZE,
            default_max_total_bytes: DEFAULT_SHARD_CACHE_TOTAL_SIZE_LIMIT,
            override_max_entries: HashMap::default(),
            override_max_total_bytes: HashMap::default(),
        }
    }
}
