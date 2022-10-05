use std::collections::HashMap;

use actix::Message;
use near_network::types::MsgRecipient;
use near_o11y::{WithSpanContext, WithSpanContextExt};
use near_pool::{PoolIteratorWrapper, TransactionPool};
use near_primitives::{
    epoch_manager::RngSeed,
    sharding::{EncodedShardChunk, PartialEncodedChunk, ShardChunk, ShardChunkHeader},
    transaction::SignedTransaction,
    types::ShardId,
};

pub trait ClientAdapterForShardsManager {
    fn did_complete_chunk(
        &self,
        partial_chunk: PartialEncodedChunk,
        shard_chunk: Option<ShardChunk>,
    );
    fn saw_invalid_chunk(&self, chunk: EncodedShardChunk);
    fn chunk_header_ready_for_inclusion(&self, chunk_header: ShardChunkHeader);
}

#[derive(Message)]
#[rtype(result = "()")]
pub enum ShardsManagerResponse {
    ChunkCompleted { partial_chunk: PartialEncodedChunk, shard_chunk: Option<ShardChunk> },
    InvalidChunk(EncodedShardChunk),
    ChunkHeaderReadyForInclusion(ShardChunkHeader),
}

impl<A: MsgRecipient<WithSpanContext<ShardsManagerResponse>>> ClientAdapterForShardsManager for A {
    fn did_complete_chunk(
        &self,
        partial_chunk: PartialEncodedChunk,
        shard_chunk: Option<ShardChunk>,
    ) {
        self.do_send(
            ShardsManagerResponse::ChunkCompleted { partial_chunk, shard_chunk }
                .with_span_context(),
        );
    }
    fn saw_invalid_chunk(&self, chunk: EncodedShardChunk) {
        self.do_send(ShardsManagerResponse::InvalidChunk(chunk).with_span_context());
    }
    fn chunk_header_ready_for_inclusion(&self, chunk_header: ShardChunkHeader) {
        self.do_send(ShardsManagerResponse::ChunkHeaderReadyForInclusion(chunk_header));
    }
}

pub struct ShardedTransactionPool {
    tx_pools: HashMap<ShardId, TransactionPool>,

    /// Useful to make tests deterministic and reproducible,
    /// while keeping the security of randomization of transactions in pool
    rng_seed: RngSeed,
}

impl ShardedTransactionPool {
    pub fn new(rng_seed: RngSeed) -> Self {
        TransactionPool::init_metrics();
        Self { tx_pools: HashMap::new(), rng_seed }
    }

    pub fn get_pool_iterator(&mut self, shard_id: ShardId) -> Option<PoolIteratorWrapper<'_>> {
        self.tx_pools.get_mut(&shard_id).map(|pool| pool.pool_iterator())
    }

    /// Returns true if transaction is not in the pool before call
    pub fn insert_transaction(&mut self, shard_id: ShardId, tx: SignedTransaction) -> bool {
        self.pool_for_shard(shard_id).insert_transaction(tx)
    }

    pub fn remove_transactions(&mut self, shard_id: ShardId, transactions: &[SignedTransaction]) {
        if let Some(pool) = self.tx_pools.get_mut(&shard_id) {
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

    fn pool_for_shard(&mut self, shard_id: ShardId) -> &mut TransactionPool {
        self.tx_pools
            .entry(shard_id)
            .or_insert_with(|| TransactionPool::new(Self::random_seed(&self.rng_seed, shard_id)))
    }

    pub fn reintroduce_transactions(
        &mut self,
        shard_id: ShardId,
        transactions: &[SignedTransaction],
    ) {
        self.pool_for_shard(shard_id).reintroduce_transactions(transactions.to_vec());
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
