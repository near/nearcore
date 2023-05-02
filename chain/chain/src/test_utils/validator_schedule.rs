use near_primitives::types::{AccountId, NumShards};
use std::collections::HashSet;

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
    pub(super) chunk_only_producers: Vec<Vec<Vec<AccountId>>>,
    pub(super) validator_groups: u64,
    pub(super) num_shards: NumShards,
}

impl ValidatorSchedule {
    pub fn new() -> Self {
        Self::new_with_shards(1)
    }

    pub fn new_with_shards(num_shards: NumShards) -> Self {
        Self {
            block_producers: Vec::new(),
            chunk_only_producers: Vec::new(),
            validator_groups: 1,
            num_shards,
        }
    }

    /// Specifies, for each epoch, the set of block producers for this epoch.
    ///
    /// Conceptually, this "loops around" when `epoch_id >= block_producers.len()`
    pub fn block_producers_per_epoch(mut self, block_producers: Vec<Vec<AccountId>>) -> Self {
        self.block_producers = block_producers;
        self.sanity_check();
        self
    }

    /// Specifies, for each shard in each epoch, the set of chunk-only producers
    /// for the shard.
    ///
    /// The full set of chunk-producers is composed from chunk_only producers
    /// and some subset of block_producers (this is controlled by
    /// `validator_groups`).
    pub fn chunk_only_producers_per_epoch_per_shard(
        mut self,
        chunk_only_producers: Vec<Vec<Vec<AccountId>>>,
    ) -> Self {
        self.chunk_only_producers = chunk_only_producers;
        self.sanity_check();
        self
    }

    /// Controls how chunk_producers are selected from the block producers.
    ///
    /// This parameter splits the validators in each shard into that many
    /// groups, and assigns a separate set of shards to each group. E.g. if
    /// there are 8 validators in a particular epoch, and 4 shards, and 2
    /// validator groups, 4 validators will be assigned to two shards, and 4
    /// remaining validators will be assigned to 2 remaining shards. With 8
    /// validators, 4 shards and 4 validator groups each shard will be assigned
    /// to two validators.
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

    pub fn all_validators(&self) -> impl Iterator<Item = &AccountId> {
        self.all_block_producers().chain(self.chunk_only_producers.iter().flatten().flatten())
    }

    fn sanity_check(&self) {
        let mut block_producers = HashSet::new();
        for bp in self.block_producers.iter().flatten() {
            if !block_producers.insert(bp) {
                panic!("block producer {bp} is specified twice")
            }
        }
        let mut chunk_only_producers = HashSet::new();
        for cop in self.chunk_only_producers.iter().flatten().flatten() {
            if !chunk_only_producers.insert(cop) {
                panic!("chunk only producer {cop} is specified twice")
            }
        }
        if let Some(v) = block_producers.intersection(&chunk_only_producers).next() {
            panic!("{v} is both a block and a chunk only producer")
        }
    }
}
