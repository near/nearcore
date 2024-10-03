use std::collections::HashMap;

use actix::Message;
use itertools::Itertools;

use near_pool::types::TransactionGroupIterator;
use near_pool::{InsertTransactionResult, PoolIteratorWrapper, TransactionPool};
use near_primitives::shard_layout::{account_id_to_shard_uid, ShardLayout, ShardUId};
use near_primitives::{
    epoch_info::RngSeed,
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
        res[0] = shard_id.get() as u8;
        res[1] = (shard_id.get() / 256) as u8;
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

    pub fn debug_status(&self) -> String {
        self.tx_pools
            .iter()
            .filter(|(_, pool)| pool.len() > 0)
            .map(|(shard_uid, pool)| format!("Shard {} has {} txs", shard_uid, pool.len()))
            .join("; ")
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
    /// TODO check if this logic works in resharding V3
    pub fn reshard(&mut self, old_shard_layout: &ShardLayout, new_shard_layout: &ShardLayout) {
        tracing::debug!(
            target: "resharding",
            old_shard_layout_version = old_shard_layout.version(),
            new_shard_layout_version = new_shard_layout.version(),
            "resharding the transaction pool"
        );
        debug_assert!(old_shard_layout != new_shard_layout);
        debug_assert!(old_shard_layout.version() + 1 == new_shard_layout.version());

        let mut transactions = vec![];

        for old_shard_uid in old_shard_layout.shard_uids() {
            if let Some(mut iter) = self.get_pool_iterator(old_shard_uid) {
                while let Some(group) = iter.next() {
                    while let Some(tx) = group.next() {
                        transactions.push(tx);
                    }
                }
            }
        }

        for tx in transactions {
            let signer_id = tx.transaction.signer_id();
            let new_shard_uid = account_id_to_shard_uid(&signer_id, new_shard_layout);
            self.insert_transaction(new_shard_uid, tx);
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::client::ShardedTransactionPool;
    use near_crypto::{InMemorySigner, KeyType};
    use near_o11y::testonly::init_test_logger;
    use near_pool::types::TransactionGroupIterator;
    use near_primitives::{
        epoch_info::RngSeed,
        hash::CryptoHash,
        shard_layout::{account_id_to_shard_uid, ShardLayout},
        transaction::SignedTransaction,
        types::{AccountId, ShardId},
    };
    use near_store::ShardUId;
    use rand::{rngs::StdRng, seq::SliceRandom, SeedableRng};
    use std::{collections::HashMap, str::FromStr};

    const TEST_SEED: RngSeed = [3; 32];

    #[test]
    fn test_random_seed_with_shard_id() {
        let seed0 = ShardedTransactionPool::random_seed(&TEST_SEED, 0.into());
        let seed10 = ShardedTransactionPool::random_seed(&TEST_SEED, 10.into());
        let seed256 = ShardedTransactionPool::random_seed(&TEST_SEED, 256.into());
        let seed1000 = ShardedTransactionPool::random_seed(&TEST_SEED, 1000.into());
        let seed1000000 = ShardedTransactionPool::random_seed(&TEST_SEED, 1_000_000.into());
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

    #[test]
    fn test_transaction_pool_resharding() {
        init_test_logger();
        let old_shard_layout = ShardLayout::get_simple_nightshade_layout();
        let new_shard_layout = ShardLayout::get_simple_nightshade_layout_v2();

        let mut pool = ShardedTransactionPool::new(TEST_SEED, None);

        let mut shard_id_to_accounts: HashMap<ShardId, _> = HashMap::new();
        shard_id_to_accounts.insert(0.into(), vec!["aaa", "abcd", "a-a-a-a-a"]);
        shard_id_to_accounts.insert(1.into(), vec!["aurora"]);
        shard_id_to_accounts.insert(2.into(), vec!["aurora-0", "bob", "kkk"]);
        // this shard is split, make sure there are accounts for both shards 3' and 4'
        shard_id_to_accounts.insert(3.into(), vec!["mmm", "rrr", "sweat", "ttt", "www", "zzz"]);

        let deposit = 222;

        let mut rng: StdRng = SeedableRng::seed_from_u64(42);

        // insert some transactions using the old shard layout

        let n = 100;
        tracing::info!("inserting {n} transactions into the pool using the old shard layout");
        for i in 0..n {
            let shard_ids: Vec<_> = old_shard_layout.shard_ids().collect();
            let &signer_shard_id = shard_ids.choose(&mut rng).unwrap();
            let &receiver_shard_id = shard_ids.choose(&mut rng).unwrap();
            let nonce = i as u64;

            let signer_id = *shard_id_to_accounts[&signer_shard_id].choose(&mut rng).unwrap();
            let signer_id = AccountId::from_str(signer_id).unwrap();

            let receiver_id = *shard_id_to_accounts[&receiver_shard_id].choose(&mut rng).unwrap();
            let receiver_id = AccountId::from_str(receiver_id).unwrap();

            let signer = InMemorySigner::from_seed(signer_id.clone(), KeyType::ED25519, "seed");

            let tx = SignedTransaction::send_money(
                nonce,
                signer_id.clone(),
                receiver_id.clone(),
                &signer.into(),
                deposit,
                CryptoHash::default(),
            );

            let shard_uid =
                ShardUId { shard_id: signer_shard_id.into(), version: old_shard_layout.version() };
            pool.insert_transaction(shard_uid, tx);
        }

        // reshard

        tracing::info!("resharding the pool");
        pool.reshard(&old_shard_layout, &new_shard_layout);

        // check the pool is correctly resharded

        tracing::info!("checking the pool after resharding");
        {
            let shard_ids: Vec<_> = new_shard_layout.shard_ids().collect();
            for &shard_id in shard_ids.iter() {
                let shard_id = shard_id.into();
                let shard_uid = ShardUId { shard_id, version: new_shard_layout.version() };
                let pool = pool.pool_for_shard(shard_uid);
                let pool_len = pool.len();
                tracing::debug!("checking shard_uid {shard_uid:?}, the pool len is {pool_len}");
                assert_ne!(pool.len(), 0);
            }

            let mut total = 0;
            for shard_id in shard_ids {
                let shard_id = shard_id.into();
                let shard_uid = ShardUId { shard_id, version: new_shard_layout.version() };
                let mut pool_iter = pool.get_pool_iterator(shard_uid).unwrap();
                while let Some(group) = pool_iter.next() {
                    while let Some(tx) = group.next() {
                        total += 1;
                        let account_id = tx.transaction.signer_id();
                        let tx_shard_uid = account_id_to_shard_uid(account_id, &new_shard_layout);
                        tracing::debug!("checking {account_id:?}:{tx_shard_uid} in {shard_uid}");
                        assert_eq!(shard_uid, tx_shard_uid);
                    }
                }
            }

            assert_eq!(total, n);
        }
        tracing::info!("finished");
    }
}
