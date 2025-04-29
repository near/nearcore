use crate::StoreConfig;
use crate::config::{PrefetchConfig, TrieCacheConfig};
use near_primitives::shard_layout::ShardUId;
use near_primitives::types::AccountId;
use std::str::FromStr;
use tracing::error;

/// Default memory limit, if nothing else is configured.
/// It is chosen to correspond roughly to the old limit, which was
/// 50k entries * TRIE_LIMIT_CACHED_VALUE_SIZE.
pub(crate) const DEFAULT_SHARD_CACHE_TOTAL_SIZE_LIMIT: bytesize::ByteSize =
    bytesize::ByteSize::mb(50);

/// Capacity for the deletions queue.
/// It is chosen to fit all hashes of deleted nodes for 3 completely full blocks.
pub(crate) const DEFAULT_SHARD_CACHE_DELETIONS_QUEUE_CAPACITY: usize = 100_000;

/// Values above this size (in bytes) are never cached.
/// Note that most of Trie inner nodes are smaller than this - e.g. branches use around 32 * 16 = 512 bytes.
const TRIE_LIMIT_CACHED_VALUE_SIZE: usize = 1000;

/// Stores necessary configuration for the creation of tries.
#[derive(Clone, Default)]
pub struct TrieConfig {
    pub shard_cache_config: TrieCacheConfig,
    pub view_shard_cache_config: TrieCacheConfig,
    pub enable_receipt_prefetching: bool,

    /// Configured accounts will be prefetched as SWEAT token account, if predecessor is listed as sender.
    pub sweat_prefetch_receivers: Vec<AccountId>,
    /// List of allowed predecessor accounts for SWEAT prefetching.
    pub sweat_prefetch_senders: Vec<AccountId>,
    pub claim_sweat_prefetch_config: Vec<PrefetchConfig>,
    pub kaiching_prefetch_config: Vec<PrefetchConfig>,

    /// List of shards we will load into memory.
    pub load_memtries_for_shards: Vec<ShardUId>,
    /// Whether mem-trie should be loaded for each tracked shard.
    pub load_memtries_for_tracked_shards: bool,
}

impl TrieConfig {
    /// Create a new `TrieConfig` with default values or the values specified in `StoreConfig`.
    pub fn from_store_config(config: &StoreConfig) -> Self {
        let mut this = TrieConfig::default();

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
        this.claim_sweat_prefetch_config.clone_from(&config.claim_sweat_prefetch_config);
        this.kaiching_prefetch_config.clone_from(&config.kaiching_prefetch_config);
        this.load_memtries_for_shards.clone_from(&config.load_memtries_for_shards);
        this.load_memtries_for_tracked_shards = config.load_memtries_for_tracked_shards;

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
        self.shard_cache_config.shard_cache_deletions_queue_capacity
    }

    /// Checks if any of prefetching related configs was enabled.
    pub fn prefetch_enabled(&self) -> bool {
        self.enable_receipt_prefetching
            || (!self.sweat_prefetch_receivers.is_empty()
                && !self.sweat_prefetch_senders.is_empty())
            || !self.claim_sweat_prefetch_config.is_empty()
            || !self.kaiching_prefetch_config.is_empty()
    }
}
