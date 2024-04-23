use super::TrieNodesCount;
use crate::{metrics, TrieStorage};
use near_o11y::metrics::prometheus;
use near_o11y::metrics::prometheus::core::{GenericCounter, GenericGauge};
use near_primitives::errors::StorageError;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardUId;
use std::sync::Arc;

/// Deterministic cache to store trie nodes that have been accessed so far
/// during the cache's lifetime. It is used for deterministic gas accounting
/// so that previously accessed trie nodes and values are charged at a
/// cheaper gas cost.
///
/// This cache's correctness is critical as it contributes to the gas
/// accounting of storage operations during contract execution. For that
/// reason, a new TrieAccountingCache must be created at the beginning of a
/// chunk's execution, and the db_read_nodes and mem_read_nodes must be taken
/// into account whenever a storage operation is performed to calculate what
/// kind of operation it was.
///
/// Note that we don't have a size limit for values in the accounting cache.
/// There are two reasons:
///   - for nodes, value size is an implementation detail. If we change
///     internal representation of a node (e.g. change `memory_usage` field
///     from `RawTrieNodeWithSize`), this would have to be a protocol upgrade.
///   - total size of all values is limited by the runtime fees. More
///     thoroughly:
///       - number of nodes is limited by receipt gas limit / touching trie
///         node fee ~= 500 Tgas / 16 Ggas = 31_250;
///       - size of trie keys and values is limited by receipt gas limit /
///         lowest per byte fee (`storage_read_value_byte`) ~=
///         (500 * 10**12 / 5611005) / 2**20 ~= 85 MB.
/// All values are given as of 16/03/2022. We may consider more precise limit
/// for the accounting cache as well.
///
/// Note that in general, it is NOT true that all storage access is either a
/// db read or mem read. It can also be a flat storage read, which is not
/// tracked via TrieAccountingCache.
pub struct TrieAccountingCache {
    /// Whether the cache is enabled. By default it is not, but it can be
    /// turned on or off on the fly.
    enable: bool,
    /// Cache of trie node hash -> trie node body, or a leaf value hash ->
    /// leaf value.
    cache: near_primitives_core::cryptohashmap::CryptoHashMap<Arc<[u8]>>,
    /// The number of times a key was accessed by reading from the underlying
    /// storage. (This does not necessarily mean it was accessed from *disk*,
    /// as the underlying storage layer may have a best-effort cache.)
    db_read_nodes: u64,
    /// The number of times a key was accessed when it was deterministically
    /// already cached during the processing of this chunk.
    mem_read_nodes: u64,
    /// Prometheus metrics. It's optional - in testing it can be None.
    metrics: Option<TrieAccountingCacheMetrics>,
}

struct TrieAccountingCacheMetrics {
    accounting_cache_hits: GenericCounter<prometheus::core::AtomicU64>,
    accounting_cache_misses: GenericCounter<prometheus::core::AtomicU64>,
    accounting_cache_size: GenericGauge<prometheus::core::AtomicI64>,
}

impl TrieAccountingCache {
    /// Constructs a new accounting cache. By default it is not enabled.
    /// The optional parameter is passed in if prometheus metrics are desired.
    pub fn new(shard_uid_and_is_view: Option<(ShardUId, bool)>) -> Self {
        let metrics = shard_uid_and_is_view.map(|(shard_uid, is_view)| {
            let mut buffer = itoa::Buffer::new();
            let shard_id = buffer.format(shard_uid.shard_id);

            let metrics_labels: [&str; 2] = [&shard_id, if is_view { "1" } else { "0" }];
            TrieAccountingCacheMetrics {
                accounting_cache_hits: metrics::CHUNK_CACHE_HITS.with_label_values(&metrics_labels),
                accounting_cache_misses: metrics::CHUNK_CACHE_MISSES
                    .with_label_values(&metrics_labels),
                accounting_cache_size: metrics::CHUNK_CACHE_SIZE.with_label_values(&metrics_labels),
            }
        });
        Self {
            enable: false,
            cache: Default::default(),
            db_read_nodes: 0,
            mem_read_nodes: 0,
            metrics,
        }
    }

    pub fn set_enabled(&mut self, enabled: bool) {
        self.enable = enabled;
    }

    /// Retrieve raw bytes from the cache if it exists, otherwise retrieve it
    /// from the given storage, and count it as a db access.
    pub fn retrieve_raw_bytes_with_accounting(
        &mut self,
        hash: &CryptoHash,
        storage: &dyn TrieStorage,
    ) -> Result<Arc<[u8]>, StorageError> {
        if let Some(node) = self.cache.get(hash) {
            self.mem_read_nodes += 1;
            if let Some(metrics) = &self.metrics {
                metrics.accounting_cache_hits.inc();
            }
            Ok(node.clone())
        } else {
            self.db_read_nodes += 1;
            if let Some(metrics) = &self.metrics {
                metrics.accounting_cache_misses.inc();
            }
            let node = storage.retrieve_raw_bytes(hash)?;

            if self.enable {
                self.cache.insert(*hash, node.clone());
                if let Some(metrics) = &self.metrics {
                    metrics.accounting_cache_size.set(self.cache.len() as i64);
                }
            }
            Ok(node)
        }
    }

    /// Used to retroactively account for a node or value that was already accessed
    /// through other means (e.g. flat storage read).
    pub fn retroactively_account(&mut self, hash: CryptoHash, data: Arc<[u8]>) {
        if self.cache.contains_key(&hash) {
            self.mem_read_nodes += 1;
        } else {
            self.db_read_nodes += 1;
        }
        if self.enable {
            self.cache.insert(hash, data);
            if let Some(metrics) = &self.metrics {
                metrics.accounting_cache_size.set(self.cache.len() as i64);
            }
        }
    }

    pub fn get_trie_nodes_count(&self) -> TrieNodesCount {
        TrieNodesCount { db_reads: self.db_read_nodes, mem_reads: self.mem_read_nodes }
    }
}
