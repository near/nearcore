use itertools::Itertools;
use near_o11y::span_wrapped_msg::SpanWrapped;
use near_pool::{InsertTransactionResult, PoolIteratorWrapper, TransactionPool};
use near_primitives::shard_layout::{ShardLayout, ShardUId};
use near_primitives::transaction::{TransactionEnvelope, ValidatedTransactionEnvelope};
use near_primitives::{
    epoch_info::RngSeed,
    sharding::{EncodedShardChunk, PartialEncodedChunk, ShardChunk, ShardChunkHeader},
    types::{AccountId, ShardId},
};
use std::collections::HashMap;

/// The decoded content of a completed chunk.
#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum DecodedChunk {
    /// We don't track this shard; no body needed.
    None,
    /// Decoded and passed validation.
    Valid(ShardChunk),
    /// Decoded but failed deserialization or validation (malicious chunk producer).
    /// The encoded chunk is preserved as evidence for `DBCol::InvalidChunks`.
    Invalid(EncodedShardChunk),
}

#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum ShardsManagerResponse {
    /// Notifies the client that the ShardsManager has collected a complete chunk.
    /// The `partial_chunk` is always persisted to `DBCol::PartialChunks`.
    /// The `decoded_chunk` indicates what we know about the chunk content.
    ChunkCompleted { partial_chunk: PartialEncodedChunk, decoded_chunk: DecodedChunk },
    /// Notifies the client that the chunk header is ready for inclusion into a new
    /// block, so that if we are a block producer, we may create a block that contains
    /// this chunk now. The producer of this chunk is also provided.
    ChunkHeaderReadyForInclusion { chunk_header: ShardChunkHeader, chunk_producer: AccountId },
}

pub(crate) type ShardsManagerResponseSender =
    near_async::messaging::Sender<SpanWrapped<ShardsManagerResponse>>;

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
        validated_tx: impl Into<ValidatedTransactionEnvelope>,
    ) -> InsertTransactionResult {
        self.pool_for_shard(shard_uid).insert_transaction(validated_tx)
    }

    pub fn remove_transactions<'a>(
        &mut self,
        shard_uid: ShardUId,
        txs: impl IntoIterator<Item = &'a TransactionEnvelope>,
    ) {
        if let Some(pool) = self.tx_pools.get_mut(&shard_uid) {
            pool.remove_transactions(txs)
        }
    }

    /// Computes a deterministic random seed for given `shard_id`.
    /// This seed is used to randomize the transaction pool.
    /// For better security we want the seed to different in each shard.
    /// For testing purposes we want it to be the reproducible and derived from the `self.rng_seed` and `shard_id`
    fn random_seed(base_seed: &RngSeed, shard_id: ShardId) -> RngSeed {
        let shard_id: u16 = shard_id.into();
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

    pub fn debug_status(&self) -> String {
        self.tx_pools
            .iter()
            .filter(|(_, pool)| pool.len() > 0)
            .map(|(shard_uid, pool)| format!("Shard {} has {} txs", shard_uid, pool.len()))
            .join("; ")
    }

    /// Reintroduces transactions back during the chain reorg, or after they were taken out of the
    /// pool for a chunk. Returns the number of transactions that were added or are already
    /// present in the pool. See `TransactionPool::reintroduce_transactions`.
    pub fn reintroduce_transactions(
        &mut self,
        shard_uid: ShardUId,
        validated_txs: impl IntoIterator<Item = impl Into<ValidatedTransactionEnvelope>>,
    ) -> usize {
        self.pool_for_shard(shard_uid).reintroduce_transactions(validated_txs)
    }

    /// Migrate all of the transactions in the pool from the old shard layout to
    /// the new shard layout.
    /// It works by emptying the pools for old shard uids and re-inserting the
    /// transactions back to the pool with the new shard uids.
    pub fn reshard(&mut self, old_shard_layout: &ShardLayout, new_shard_layout: &ShardLayout) {
        tracing::debug!(
            target: "resharding",
            old_shard_layout_version = old_shard_layout.version(),
            new_shard_layout_version = new_shard_layout.version(),
            "resharding the transaction pool"
        );
        debug_assert!(old_shard_layout != new_shard_layout);

        let mut validated_txs: Vec<ValidatedTransactionEnvelope> = vec![];

        for old_shard_uid in old_shard_layout.shard_uids() {
            let Some(iter) = self.get_pool_iterator(old_shard_uid) else { continue };
            validated_txs.extend(iter.drain());
        }

        for validated_tx in validated_txs {
            let account_id = validated_tx.routing_account_id();
            let new_shard_uid = new_shard_layout.account_id_to_shard_uid(account_id);
            self.insert_transaction(new_shard_uid, validated_tx);
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::client::ShardedTransactionPool;
    use near_crypto::{InMemorySigner, KeyType};
    use near_o11y::testonly::init_test_logger;
    use near_primitives::{
        epoch_info::RngSeed,
        hash::CryptoHash,
        shard_layout::ShardLayout,
        transaction::{
            EccTransaction, SignedTransaction, ValidatedEcc, ValidatedTransaction,
            ValidatedTransactionEnvelope,
        },
        types::{AccountId, Balance, ShardId},
    };
    use near_store::ShardUId;
    use rand::{SeedableRng, rngs::StdRng, seq::SliceRandom};
    use std::{collections::HashMap, str::FromStr};

    const TEST_SEED: RngSeed = [3; 32];

    /// Takes everything out of the pool of `shard_uid`.
    fn drain(
        pool: &mut ShardedTransactionPool,
        shard_uid: ShardUId,
    ) -> Vec<ValidatedTransactionEnvelope> {
        pool.get_pool_iterator(shard_uid).map(|iter| iter.drain()).unwrap_or_default()
    }

    #[test]
    fn test_random_seed_with_shard_id() {
        let seed0 = ShardedTransactionPool::random_seed(&TEST_SEED, ShardId::new(0));
        let seed10 = ShardedTransactionPool::random_seed(&TEST_SEED, ShardId::new(10));
        let seed256 = ShardedTransactionPool::random_seed(&TEST_SEED, ShardId::new(256));
        let seed1000 = ShardedTransactionPool::random_seed(&TEST_SEED, ShardId::new(1000));
        let seed1000000 = ShardedTransactionPool::random_seed(&TEST_SEED, ShardId::new(1_000_000));
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

        let boundary_accounts = ["aurora", "aurora-0", "kkuuue2akv_1630967379.near"]
            .iter()
            .map(|a| a.parse().unwrap())
            .collect();
        let old_shard_layout = ShardLayout::multi_shard_custom(boundary_accounts, 3);
        let new_shard_layout = ShardLayout::derive_shard_layout(
            &old_shard_layout,
            "tge-lockup.sweat".parse().unwrap(),
        );

        let mut pool = ShardedTransactionPool::new(TEST_SEED, None);

        let mut shard_id_to_accounts: HashMap<ShardId, _> = HashMap::new();
        shard_id_to_accounts.insert(ShardId::new(0), vec!["aaa", "abcd", "a-a-a-a-a"]);
        shard_id_to_accounts.insert(ShardId::new(1), vec!["aurora"]);
        shard_id_to_accounts.insert(ShardId::new(2), vec!["aurora-0", "bob", "kkk"]);
        // this shard is split, make sure there are accounts for both shards 3' and 4'
        shard_id_to_accounts
            .insert(ShardId::new(3), vec!["mmm", "rrr", "sweat", "ttt", "www", "zzz"]);

        let deposit = Balance::from_yoctonear(222);

        let mut rng: StdRng = SeedableRng::seed_from_u64(42);

        // insert some transactions using the old shard layout

        let n = 100;
        tracing::info!(%n, "inserting transactions into the pool using the old shard layout");
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

            let signed_tx = SignedTransaction::send_money(
                nonce,
                signer_id.clone(),
                receiver_id.clone(),
                &signer,
                deposit,
                CryptoHash::default(),
            );
            let validated_tx = ValidatedTransaction::new_for_test(signed_tx);

            let shard_uid = ShardUId::new(old_shard_layout.version(), signer_shard_id);
            pool.insert_transaction(shard_uid, validated_tx);
        }

        // reshard

        tracing::info!("resharding the pool");
        pool.reshard(&old_shard_layout, &new_shard_layout);

        // check the pool is correctly resharded

        tracing::info!("checking the pool after resharding");
        {
            let shard_ids: Vec<_> = new_shard_layout.shard_ids().collect();
            for &shard_id in &shard_ids {
                let shard_uid = ShardUId::new(new_shard_layout.version(), shard_id);
                let pool = pool.pool_for_shard(shard_uid);
                let pool_len = pool.len();
                tracing::debug!(?shard_uid, %pool_len, "checking shard_uid, pool_len");
                assert_ne!(pool.len(), 0);
            }

            let mut total = 0;
            for shard_id in shard_ids {
                let shard_uid = ShardUId::new(new_shard_layout.version(), shard_id);
                for validated_tx in drain(&mut pool, shard_uid) {
                    total += 1;
                    let account_id = validated_tx.routing_account_id();
                    let tx_shard_uid = new_shard_layout.account_id_to_shard_uid(account_id);
                    tracing::debug!(
                        ?account_id,
                        ?tx_shard_uid,
                        ?shard_uid,
                        "checking account_id and tx_shard_uid in shard_uid"
                    );
                    assert_eq!(shard_uid, tx_shard_uid);
                }
            }

            assert_eq!(total, n);
        }
        tracing::info!("finished");
    }

    fn ecc(contract_id: &str, tag: u8) -> ValidatedEcc {
        let contract_id = AccountId::from_str(contract_id).unwrap();
        ValidatedEcc::new_for_test(EccTransaction::new(contract_id, vec![tag]))
    }

    /// Takes everything out of the pool of `shard_uid`. Returns the tags of ECCs per
    /// contract in the order they were taken.
    fn drain_eccs(
        pool: &mut ShardedTransactionPool,
        shard_uid: ShardUId,
    ) -> HashMap<AccountId, Vec<u8>> {
        let mut eccs: HashMap<AccountId, Vec<u8>> = HashMap::new();
        for tx in drain(pool, shard_uid) {
            let ValidatedTransactionEnvelope::Unsigned(ecc) = tx else { panic!("signed tx") };
            eccs.entry(ecc.contract_id().clone()).or_default().push(ecc.payload()[0]);
        }
        eccs
    }

    /// ECCs go to the shard of their contract when the pool is resharded, and keep
    /// their order.
    #[test]
    fn test_transaction_pool_resharding_eccs() {
        let boundary_accounts = ["aurora", "aurora-0", "kkuuue2akv_1630967379.near"]
            .iter()
            .map(|a| a.parse().unwrap())
            .collect();
        let old_shard_layout = ShardLayout::multi_shard_custom(boundary_accounts, 3);
        let new_shard_layout = ShardLayout::derive_shard_layout(
            &old_shard_layout,
            "tge-lockup.sweat".parse().unwrap(),
        );
        let mut pool = ShardedTransactionPool::new(TEST_SEED, None);

        // The split shard has contracts on both sides of the new boundary.
        let contracts = ["aaa", "aurora", "bob", "mmm", "sweat", "ttt", "zzz"];
        for contract_id in contracts {
            let old_shard_uid = old_shard_layout
                .account_id_to_shard_uid(&AccountId::from_str(contract_id).unwrap());
            for tag in 1..=3 {
                pool.insert_transaction(old_shard_uid, ecc(contract_id, tag));
            }
        }

        pool.reshard(&old_shard_layout, &new_shard_layout);

        let mut total = 0;
        for shard_uid in new_shard_layout.shard_uids() {
            for (contract_id, tags) in drain_eccs(&mut pool, shard_uid) {
                assert_eq!(new_shard_layout.account_id_to_shard_uid(&contract_id), shard_uid);
                assert_eq!(tags, vec![1, 2, 3]);
                total += 1;
            }
        }
        assert_eq!(total, contracts.len());
    }

    /// Reintroduced ECCs go ahead of the ECCs that arrived while they were away, in
    /// the order they were passed in.
    #[test]
    fn test_reintroduce_eccs_keeps_arrival_order() {
        let mut pool = ShardedTransactionPool::new(TEST_SEED, None);
        let shard_uid = ShardUId::new(0, ShardId::new(0));
        pool.insert_transaction(shard_uid, ecc("a.near", 3));

        let reintroduced =
            pool.reintroduce_transactions(shard_uid, [ecc("a.near", 1), ecc("a.near", 2)]);

        assert_eq!(reintroduced, 2);
        let eccs = drain_eccs(&mut pool, shard_uid);
        assert_eq!(eccs[&AccountId::from_str("a.near").unwrap()], vec![1, 2, 3]);
    }

    /// The ECCs of several blocks are reintroduced one block at a time, the newest block first,
    /// like a reorg does. They end up in the order of arrival.
    #[test]
    fn test_reintroduce_eccs_of_several_blocks_newest_first() {
        let mut pool = ShardedTransactionPool::new(TEST_SEED, None);
        let shard_uid = ShardUId::new(0, ShardId::new(0));
        pool.insert_transaction(shard_uid, ecc("a.near", 5));

        pool.reintroduce_transactions(shard_uid, [ecc("a.near", 3), ecc("a.near", 4)]);
        pool.reintroduce_transactions(shard_uid, [ecc("a.near", 1), ecc("a.near", 2)]);

        let eccs = drain_eccs(&mut pool, shard_uid);
        assert_eq!(eccs[&AccountId::from_str("a.near").unwrap()], vec![1, 2, 3, 4, 5]);
    }
}
