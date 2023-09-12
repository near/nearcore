use std::collections::HashMap;

use actix::Message;

use near_pool::types::PoolIterator;
use near_pool::{InsertTransactionResult, PoolIteratorWrapper, TransactionPool};
use near_primitives::shard_layout::{account_id_to_shard_uid, ShardLayout, ShardUId};
use near_primitives::{
    epoch_manager::RngSeed,
    sharding::{EncodedShardChunk, PartialEncodedChunk, ShardChunk, ShardChunkHeader},
    transaction::SignedTransaction,
    types::{AccountId, ShardId},
};

#[derive(Message, Debug)]
#[rtype(result = "()")]
pub enum ShardsManagerResponse {
    /// Notifies the client that the ShardsManager has collected a complete chunk.
    /// Note that this does NOT mean that the chunk is fully constructed. If we are
    /// not tracking the shard this chunk is in, then being complete only means that
    /// we have received the parts we own, and the receipt proofs corresponding to
    /// shards that we do track. On the other hand if we are tracking the shard this
    /// chunk is in, then being complete does mean having the full chunk, in which
    /// case the shard_chunk is also provided.
    ChunkCompleted { partial_chunk: PartialEncodedChunk, shard_chunk: Option<ShardChunk> },
    /// Notifies the client that we have collected a full chunk but the chunk cannot
    /// be properly decoded.
    InvalidChunk(EncodedShardChunk),
    /// Notifies the client that the chunk header is ready for inclusion into a new
    /// block, so that if we are a block producer, we may create a block that contains
    /// this chunk now. The producer of this chunk is also provided.
    ChunkHeaderReadyForInclusion { chunk_header: ShardChunkHeader, chunk_producer: AccountId },
}

pub struct ShardedTransactionPool {
    tx_pools: HashMap<ShardUId, TransactionPool>,

    /// Useful to make tests deterministic and reproducible,
    /// while keeping the security of randomization of transactions in pool
    rng_seed: RngSeed,

    /// If set, new transactions that bring the size of the pool over this limit will be rejected.
    /// The size is tracked and enforced separately for each shard.
    pool_size_limit: Option<u64>,
}

impl ShardedTransactionPool {
    pub fn new(rng_seed: RngSeed, pool_size_limit: Option<u64>) -> Self {
        Self { tx_pools: HashMap::new(), rng_seed, pool_size_limit }
    }

    pub fn get_pool_iterator(&mut self, shard_uid: ShardUId) -> Option<PoolIteratorWrapper<'_>> {
        self.tx_pools.get_mut(&shard_uid).map(|pool| pool.pool_iterator())
    }

    /// Tries to insert the transaction into the pool for a given shard.
    pub fn insert_transaction(
        &mut self,
        shard_uid: ShardUId,
        tx: SignedTransaction,
    ) -> InsertTransactionResult {
        self.pool_for_shard(shard_uid).insert_transaction(tx)
    }

    pub fn remove_transactions(&mut self, shard_uid: ShardUId, transactions: &[SignedTransaction]) {
        if let Some(pool) = self.tx_pools.get_mut(&shard_uid) {
            pool.remove_transactions(transactions)
        }
    }

    /// Computes a deterministic random seed for given `shard_id`.
    /// This seed is used to randomize the transaction pool.
    /// For better security we want the seed to different in each shard.
    /// For testing purposes we want it to be the reproducible and derived from the `self.rng_seed` and `shard_id`
    fn random_seed(base_seed: &RngSeed, shard_id: ShardId) -> RngSeed {
        let mut res = *base_seed;
        res[0] = shard_id as u8;
        res[1] = (shard_id / 256) as u8;
        res
    }

    fn pool_for_shard(&mut self, shard_uid: ShardUId) -> &mut TransactionPool {
        self.tx_pools.entry(shard_uid).or_insert_with(|| {
            TransactionPool::new(
                Self::random_seed(&self.rng_seed, shard_uid.shard_id()),
                self.pool_size_limit,
                &shard_uid.to_string(),
            )
        })
    }

    /// Reintroduces transactions back during the chain reorg. Returns the number of transactions
    /// that were added or are already present in the pool.
    pub fn reintroduce_transactions(
        &mut self,
        shard_uid: ShardUId,
        transactions: &[SignedTransaction],
    ) -> usize {
        let mut reintroduced_count = 0;
        let pool = self.pool_for_shard(shard_uid);
        for tx in transactions {
            reintroduced_count += match pool.insert_transaction(tx.clone()) {
                InsertTransactionResult::Success | InsertTransactionResult::Duplicate => 1,
                InsertTransactionResult::NoSpaceLeft => 0,
            }
        }
        reintroduced_count
    }

    /// Migrate all of the transactions in the pool from the old shard layout to
    /// the new shard layout.
    /// It works by emptying the pools for old shard uids and re-inserting the
    /// transactions back to the pool with the new shard uids.
    pub fn reshard(&mut self, old_shard_layout: &ShardLayout, new_shard_layout: &ShardLayout) {
        tracing::debug!(
            target: "client",
            old_shard_layout_version = old_shard_layout.version(),
            new_shard_layout_version = new_shard_layout.version(),
            "resharding the transaction pool"
        );
        debug_assert!(old_shard_layout != new_shard_layout);
        debug_assert!(old_shard_layout.version() + 1 == new_shard_layout.version());

        let mut transactions = vec![];

        let old_shard_uids = old_shard_layout.get_shard_uids();
        for old_shard_uid in old_shard_uids {
            if let Some(mut iter) = self.get_pool_iterator(old_shard_uid) {
                while let Some(group) = iter.next() {
                    while let Some(tx) = group.next() {
                        transactions.push(tx);
                    }
                }
            }
        }

        for tx in transactions {
            let signer_id = &tx.transaction.signer_id;
            let new_shard_uid = account_id_to_shard_uid(&signer_id, new_shard_layout);
            self.insert_transaction(new_shard_uid, tx);
        }
    }
}

#[cfg(test)]
mod tests {
    use near_primitives::epoch_manager::RngSeed;

    use crate::client::ShardedTransactionPool;

    const TEST_SEED: RngSeed = [3; 32];

    #[test]
    fn test_random_seed_with_shard_id() {
        let seed0 = ShardedTransactionPool::random_seed(&TEST_SEED, 0);
        let seed10 = ShardedTransactionPool::random_seed(&TEST_SEED, 10);
        let seed256 = ShardedTransactionPool::random_seed(&TEST_SEED, 256);
        let seed1000 = ShardedTransactionPool::random_seed(&TEST_SEED, 1000);
        let seed1000000 = ShardedTransactionPool::random_seed(&TEST_SEED, 1_000_000);
        assert_ne!(seed0, seed10);
        assert_ne!(seed0, seed256);
        assert_ne!(seed0, seed1000);
        assert_ne!(seed0, seed1000000);
        assert_ne!(seed10, seed256);
        assert_ne!(seed10, seed1000);
        assert_ne!(seed10, seed1000000);
        assert_ne!(seed256, seed1000);
        assert_ne!(seed256, seed1000000);
        assert_ne!(seed1000, seed1000000);
    }
}
