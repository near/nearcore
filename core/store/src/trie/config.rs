use crate::config::TrieCacheConfig;
use crate::trie::trie_storage::TrieCacheInner;
use crate::StoreConfig;
use near_primitives::types::AccountId;
use std::str::FromStr;
use tracing::{error, warn};

/// Default memory limit, if nothing else is configured.
/// It is chosen to correspond roughly to the old limit, which was
/// 50k entries * TRIE_LIMIT_CACHED_VALUE_SIZE.
pub(crate) const DEFAULT_SHARD_CACHE_TOTAL_SIZE_LIMIT: u64 =
    if cfg!(feature = "no_cache") { 1 } else { 50_000_000 };

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
    pub shard_cache_config: TrieCacheConfig,
    pub view_shard_cache_config: TrieCacheConfig,
    pub enable_receipt_prefetching: bool,

    /// Configured accounts will be prefetched as SWEAT token account, if predecessor is listed as sender.
    pub sweat_prefetch_receivers: Vec<AccountId>,
    /// List of allowed predecessor accounts for SWEAT prefetching.
    pub sweat_prefetch_senders: Vec<AccountId>,
}

impl TrieConfig {
    /// Create a new `TrieConfig` with default values or the values specified in `StoreConfig`.
    pub fn from_store_config(config: &StoreConfig) -> Self {
        let mut this = TrieConfig::default();

        if !config.trie_cache_capacities.is_empty() {
            warn!(target: "store", "`trie_cache_capacities` is deprecated, use `trie_cache` and `view_trie_cache` instead");
            for (shard_uid, capacity) in &config.trie_cache_capacities {
                let bytes_limit = Self::deprecated_num_entry_to_memory_limit(*capacity);
                this.shard_cache_config.per_shard_max_bytes.insert(*shard_uid, bytes_limit);
            }
        }

        this.shard_cache_config = config.trie_cache.clone();
        this.view_shard_cache_config = config.view_trie_cache.clone();

        this.enable_receipt_prefetching = config.enable_receipt_prefetching;
        for account in &config.sweat_prefetch_receivers {
            match AccountId::from_str(account) {
                Ok(account_id) => this.sweat_prefetch_receivers.push(account_id),
                Err(e) => error!(target: "config", "invalid account id {account}: {e}"),
            }
        }
        for account in &config.sweat_prefetch_senders {
            match AccountId::from_str(account) {
                Ok(account_id) => this.sweat_prefetch_senders.push(account_id),
                Err(e) => error!(target: "config", "invalid account id {account}: {e}"),
            }
        }

        this
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

    /// Given a number of max entries in the old config format, calculate how
    /// many bytes the limit should be set to such that AT LEAST THE SAME NUMBER
    /// can fit.
    ///
    /// TODO(#7894): Remove this when `trie_cache_capacities` is removed from config.
    ///
    /// As long as `trie_cache_capacities` is a config option, it should be respected.
    /// We no longer commit to a hard limit on this. But we make sure that the old
    /// worst-case assumption of how much memory would be consumed still works.
    /// Specifically, the old calculation ignored `PER_ENTRY_OVERHEAD` and used
    /// `max_cached_value_size()` only to figure out a good value for how many
    /// nodes we want in the cache at most.
    /// This implicit limit should result in the same min number of nodes and
    /// same max memory consumption as the old config.
    pub(crate) fn deprecated_num_entry_to_memory_limit(max_num_entries: u64) -> u64 {
        max_num_entries
            * (TrieCacheInner::PER_ENTRY_OVERHEAD + TrieConfig::max_cached_value_size() as u64)
    }
}
