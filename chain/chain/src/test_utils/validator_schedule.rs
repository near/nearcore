use near_primitives::types::{AccountId, NumShards};

/// Validator schedule describes how block and chunk producers are selected by
/// the KeyValue runtime.
///
/// In the real runtime, we use complex algorithm based on randomness and stake
/// to select the validators. For for tests though, we just want to select them
/// by fiat.
///
/// Conventional short name for `ValidatorSchedule` is `vs`.
#[derive(Clone)]
pub struct ValidatorSchedule {
    pub(super) block_producers: Vec<Vec<AccountId>>,
    #[cfg(feature = "protocol_feature_chunk_only_producers")]
    pub(super) chunk_only_producers: Vec<Vec<Vec<AccountId>>>,
    pub(super) validator_groups: u64,
    pub(super) num_shards: NumShards,
}

impl ValidatorSchedule {
    pub fn new() -> Self {
        Self {
            block_producers: Vec::new(),
            #[cfg(feature = "protocol_feature_chunk_only_producers")]
            chunk_only_producers: Vec::new(),
            validator_groups: 1,
            num_shards: 1,
        }
    }
    /// Specifies, for each epoch, the set of block produces for this epoch.
    ///
    /// Conceptually, this "loops around" when `epoch_id >= block_producers.len()`
    pub fn block_producers_per_epoch(mut self, block_producers: Vec<Vec<AccountId>>) -> Self {
        self.block_producers = block_producers;
        self
    }

    /// Specifies, for each shard in each epoch, the set of chunk-only produces
    /// for the shard.
    ///
    /// The full set of chunk-producers is composed from chunk_only producers
    /// and some subset of block_producers (this is controlled by
    /// `validator_groups`).
    #[cfg(feature = "protocol_feature_chunk_only_producers")]
    pub fn chunk_only_producers_per_epoch_per_shard(
        mut self,
        chunk_only_producers: Vec<Vec<Vec<AccountId>>>,
    ) -> Self {
        self.chunk_only_producers = chunk_only_producers;
        self
    }

    /// Controls how chunk_producers are selected from the block producers.
    ///
    /// See how `EpochValidatorSet` is constructed for details.
    pub fn validator_groups(mut self, validator_groups: u64) -> Self {
        self.validator_groups = validator_groups;
        self
    }

    pub fn num_shards(mut self, num_shards: NumShards) -> Self {
        self.num_shards = num_shards;
        self
    }

    pub fn all_block_producers(&self) -> impl Iterator<Item = &AccountId> {
        self.block_producers.iter().flatten()
    }
}
