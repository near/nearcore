use crate::debug::PRODUCTION_TIMES_CACHE_SIZE;
use crate::metrics;
use itertools::Itertools;
use near_async::time::{Clock, Duration, Instant};
use near_chain::types::{
    PrepareTransactionsChunkContext, PreparedTransactions, RuntimeAdapter, RuntimeStorageConfig,
};
use near_chain::{Block, Chain, ChainStore};
use near_chain_configs::MutableConfigValue;
use near_chunks::client::ShardedTransactionPool;
use near_client_primitives::debug::ChunkProduction;
use near_client_primitives::types::Error;
use near_epoch_manager::EpochManagerAdapter;
use near_epoch_manager::shard_assignment::shard_id_to_uid;
use near_primitives::bandwidth_scheduler::BandwidthRequests;
use near_primitives::epoch_info::RngSeed;
use near_primitives::hash::CryptoHash;
use near_primitives::merkle::{MerklePath, merklize};
use near_primitives::receipt::Receipt;
use near_primitives::sharding::{ShardChunkHeader, ShardChunkWithEncoding};
use near_primitives::stateless_validation::ChunkProductionKey;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::chunk_extra::ChunkExtra;
use near_primitives::types::{BlockHeight, EpochId, ShardId};
use near_primitives::validator_signer::ValidatorSigner;
use near_store::ShardUId;
use near_store::adapter::chain_store::ChainStoreAdapter;
use parking_lot::Mutex;
use reed_solomon_erasure::galois_8::ReedSolomon;
use std::num::NonZeroUsize;
use std::sync::Arc;
use time::ext::InstantExt as _;
use tracing::{debug, instrument};

#[cfg(feature = "test_features")]
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub enum AdvProduceChunksMode {
    // Produce chunks as usual.
    Valid,
    // Stop producing chunks.
    StopProduce,
    // Produce chunks but do not include any transactions.
    ProduceWithoutTx,
    // Produce chunks but do not bother checking if included transactions pass validity check.
    ProduceWithoutTxValidityCheck,
}

pub struct ProduceChunkResult {
    pub chunk: ShardChunkWithEncoding,
    pub encoded_chunk_parts_paths: Vec<MerklePath>,
    pub receipts: Vec<Receipt>,
}

/// Handles chunk production.
pub struct ChunkProducer {
    /// Adversarial controls - should be enabled only to test disruptive
    /// behavior on chain.
    #[cfg(feature = "test_features")]
    pub adv_produce_chunks: Option<AdvProduceChunksMode>,
    #[cfg(feature = "test_features")]
    pub produce_invalid_chunks: bool,
    #[cfg(feature = "test_features")]
    pub produce_invalid_tx_in_chunks: bool,

    clock: Clock,
    /// If present, limits adding transactions from the transaction
    /// pool to the chunk by certain time.
    chunk_transactions_time_limit: MutableConfigValue<Option<Duration>>,
    chain: ChainStoreAdapter,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    runtime_adapter: Arc<dyn RuntimeAdapter>,
    // TODO: put mutex on individual shards instead of the complete pool
    pub sharded_tx_pool: Arc<Mutex<ShardedTransactionPool>>,
    /// A ReedSolomon instance to encode shard chunks.
    reed_solomon_encoder: ReedSolomon,
    /// Chunk production timing information. Used only for debug purposes.
    pub chunk_production_info: lru::LruCache<(BlockHeight, ShardId), ChunkProduction>,
}

impl ChunkProducer {
    pub fn new(
        clock: Clock,
        chunk_transactions_time_limit: MutableConfigValue<Option<Duration>>,
        chain_store: &ChainStoreAdapter,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        runtime_adapter: Arc<dyn RuntimeAdapter>,
        rng_seed: RngSeed,
        transaction_pool_size_limit: Option<u64>,
    ) -> Self {
        let data_parts = epoch_manager.num_data_parts();
        let parity_parts = epoch_manager.num_total_parts() - data_parts;

        Self {
            #[cfg(feature = "test_features")]
            adv_produce_chunks: None,
            #[cfg(feature = "test_features")]
            produce_invalid_chunks: false,
            #[cfg(feature = "test_features")]
            produce_invalid_tx_in_chunks: false,
            clock,
            chunk_transactions_time_limit,
            chain: chain_store.clone(),
            epoch_manager,
            runtime_adapter,
            sharded_tx_pool: Arc::new(Mutex::new(ShardedTransactionPool::new(
                rng_seed,
                transaction_pool_size_limit,
            ))),
            reed_solomon_encoder: ReedSolomon::new(data_parts, parity_parts).unwrap(),
            chunk_production_info: lru::LruCache::new(
                NonZeroUsize::new(PRODUCTION_TIMES_CACHE_SIZE).unwrap(),
            ),
        }
    }

    pub fn produce_chunk(
        &mut self,
        prev_block: &Block,
        epoch_id: &EpochId,
        last_header: ShardChunkHeader,
        next_height: BlockHeight,
        shard_id: ShardId,
        signer: &Arc<ValidatorSigner>,
        chain_validate: &dyn Fn(&SignedTransaction) -> bool,
    ) -> Result<Option<ProduceChunkResult>, Error> {
        let chunk_proposer = self
            .epoch_manager
            .get_chunk_producer_info(&ChunkProductionKey {
                epoch_id: *epoch_id,
                height_created: next_height,
                shard_id,
            })
            .unwrap()
            .take_account_id();
        if signer.validator_id() != &chunk_proposer {
            debug!(target: "client",
                me = ?signer.as_ref().validator_id(),
                ?chunk_proposer,
                next_height,
                ?shard_id,
                "Not producing chunk. Not chunk producer for next chunk.");
            return Ok(None);
        }

        self.produce_chunk_internal(
            prev_block,
            epoch_id,
            last_header,
            next_height,
            shard_id,
            signer,
            chain_validate,
        )
    }

    #[cfg(feature = "test_features")]
    fn maybe_insert_invalid_transaction(
        mut txs: PreparedTransactions,
        prev_block_hash: CryptoHash,
        insert: bool,
    ) -> PreparedTransactions {
        if insert {
            let signed_tx = SignedTransaction::new(
                near_crypto::Signature::empty(near_crypto::KeyType::ED25519),
                near_primitives::transaction::Transaction::new_v1(
                    "test".parse().unwrap(),
                    near_crypto::PublicKey::empty(near_crypto::KeyType::SECP256K1),
                    "other".parse().unwrap(),
                    3,
                    prev_block_hash,
                    0,
                ),
            );
            let validated_tx =
                near_primitives::transaction::ValidatedTransaction::new_for_test(signed_tx);
            txs.transactions.push(validated_tx);
        }
        txs
    }

    /// Calculates the root of receipt proofs.
    /// All receipts are grouped by receiver_id and hash is calculated
    /// for each such group. Then we merklize these hashes to calculate
    /// the receipts root.
    ///
    /// Receipts root is used in the following ways:
    /// 1. Someone who cares about shard will download all the receipts
    ///    and checks if those correspond to receipts_root.
    /// 2. Anyone who asks for one's incoming receipts will receive a piece
    ///    of incoming receipts only with merkle receipts proofs which can
    ///    be checked locally.
    fn calculate_receipts_root(
        &self,
        epoch_id: &EpochId,
        receipts: &[Receipt],
    ) -> Result<CryptoHash, Error> {
        let shard_layout = self.epoch_manager.get_shard_layout(epoch_id)?;
        let receipts_hashes = Chain::build_receipts_hashes(&receipts, &shard_layout)?;
        let (receipts_root, _) = merklize(&receipts_hashes);
        Ok(receipts_root)
    }

    #[instrument(target = "client", level = "debug", "produce_chunk", skip_all, fields(
        height = next_height,
        shard_id,
        ?epoch_id,
        chunk_hash = tracing::field::Empty,
    ))]
    fn produce_chunk_internal(
        &mut self,
        prev_block: &Block,
        epoch_id: &EpochId,
        last_header: ShardChunkHeader,
        next_height: BlockHeight,
        shard_id: ShardId,
        validator_signer: &Arc<ValidatorSigner>,
        chain_validate: &dyn Fn(&SignedTransaction) -> bool,
    ) -> Result<Option<ProduceChunkResult>, Error> {
        let span = tracing::Span::current();
        let timer = Instant::now();
        let _timer =
            metrics::PRODUCE_CHUNK_TIME.with_label_values(&[&shard_id.to_string()]).start_timer();
        let prev_block_hash = *prev_block.hash();
        if self.epoch_manager.is_next_block_epoch_start(&prev_block_hash)? {
            let prev_prev_hash = *self.chain.get_block_header(&prev_block_hash)?.prev_hash();
            // If we are to start new epoch, check if the previous block is
            // caught up. If it is not the case, we wouldn't be able to
            // apply block with the new chunk, so we also skip chunk production.
            if !ChainStore::prev_block_is_caught_up(&self.chain, &prev_prev_hash, &prev_block_hash)?
            {
                debug!(target: "client", ?shard_id, next_height, "Produce chunk: prev block is not caught up");
                return Err(Error::ChunkProducer(
                    "State for the epoch is not downloaded yet, skipping chunk production"
                        .to_string(),
                ));
            }
        }

        debug!(target: "client", me = ?validator_signer.validator_id(), next_height, ?shard_id, "Producing chunk");

        let shard_uid = shard_id_to_uid(self.epoch_manager.as_ref(), shard_id, epoch_id)?;
        let chunk_extra = self
            .chain
            .get_chunk_extra(&prev_block_hash, &shard_uid)
            .map_err(|err| Error::ChunkProducer(format!("No chunk extra available: {}", err)))?;

        let prepared_transactions = {
            #[cfg(feature = "test_features")]
            match self.adv_produce_chunks {
                Some(AdvProduceChunksMode::ProduceWithoutTx) => {
                    PreparedTransactions { transactions: Vec::new(), limited_by: None }
                }
                _ => self.prepare_transactions(
                    shard_uid,
                    prev_block,
                    chunk_extra.as_ref(),
                    chain_validate,
                )?,
            }
            #[cfg(not(feature = "test_features"))]
            self.prepare_transactions(shard_uid, prev_block, chunk_extra.as_ref(), chain_validate)?
        };

        #[cfg(feature = "test_features")]
        let prepared_transactions = Self::maybe_insert_invalid_transaction(
            prepared_transactions,
            prev_block_hash,
            self.produce_invalid_tx_in_chunks,
        );
        let num_filtered_transactions = prepared_transactions.transactions.len();
        let (tx_root, _) = merklize(
            &prepared_transactions.transactions.iter().map(|vt| vt.to_signed_tx()).collect_vec(),
        );
        let outgoing_receipts = ChainStore::get_outgoing_receipts_for_shard_from_store(
            &self.chain,
            self.epoch_manager.as_ref(),
            prev_block_hash,
            shard_id,
            last_header.height_included(),
        )?;

        let outgoing_receipts_root = self.calculate_receipts_root(epoch_id, &outgoing_receipts)?;
        let gas_used = chunk_extra.gas_used();
        #[cfg(feature = "test_features")]
        let gas_used = if self.produce_invalid_chunks { gas_used + 1 } else { gas_used };

        let congestion_info = chunk_extra.congestion_info();
        let bandwidth_requests = chunk_extra.bandwidth_requests();
        debug_assert!(
            bandwidth_requests.is_some(),
            "Expected bandwidth_request to be Some after BandwidthScheduler feature enabled"
        );
        let (chunk, merkle_paths) = ShardChunkWithEncoding::new(
            prev_block_hash,
            *chunk_extra.state_root(),
            *chunk_extra.outcome_root(),
            next_height,
            shard_id,
            gas_used,
            chunk_extra.gas_limit(),
            chunk_extra.balance_burnt(),
            chunk_extra.validator_proposals().collect(),
            prepared_transactions.transactions,
            outgoing_receipts.clone(),
            outgoing_receipts_root,
            tx_root,
            congestion_info,
            bandwidth_requests.cloned().unwrap_or_else(BandwidthRequests::empty),
            &*validator_signer,
            &mut self.reed_solomon_encoder,
        );

        let encoded_chunk = chunk.to_encoded_shard_chunk();
        span.record("chunk_hash", tracing::field::debug(encoded_chunk.chunk_hash()));
        debug!(target: "client",
            me = %validator_signer.validator_id(),
            chunk_hash = ?encoded_chunk.chunk_hash(),
            %prev_block_hash,
            num_filtered_transactions,
            num_outgoing_receipts = outgoing_receipts.len(),
            "produced_chunk");

        metrics::CHUNK_PRODUCED_TOTAL.inc();

        metrics::CHUNK_TRANSACTIONS_TOTAL
            .with_label_values(&[&shard_id.to_string()])
            .inc_by(num_filtered_transactions as u64);

        self.chunk_production_info.put(
            (next_height, shard_id),
            ChunkProduction {
                chunk_production_time: Some(self.clock.now_utc()),
                chunk_production_duration_millis: Some(
                    (self.clock.now().signed_duration_since(timer)).whole_milliseconds().max(0)
                        as u64,
                ),
            },
        );
        if let Some(limit) = prepared_transactions.limited_by {
            // When some transactions from the pool didn't fit into the chunk due to a limit, it's reported in a metric.
            metrics::PRODUCED_CHUNKS_SOME_POOL_TRANSACTIONS_DID_NOT_FIT
                .with_label_values(&[&shard_id.to_string(), limit.as_ref()])
                .inc();
        }

        Ok(Some(ProduceChunkResult {
            chunk,
            encoded_chunk_parts_paths: merkle_paths,
            receipts: outgoing_receipts,
        }))
    }

    /// Prepares an ordered list of valid transactions from the pool up the limits.
    fn prepare_transactions(
        &self,
        shard_uid: ShardUId,
        prev_block: &Block,
        chunk_extra: &ChunkExtra,
        chain_validate: &dyn Fn(&SignedTransaction) -> bool,
    ) -> Result<PreparedTransactions, Error> {
        let shard_id = shard_uid.shard_id();
        let mut pool_guard = self.sharded_tx_pool.lock();
        let prepared_transactions = if let Some(mut iter) = pool_guard.get_pool_iterator(shard_uid)
        {
            let storage_config = RuntimeStorageConfig {
                state_root: *chunk_extra.state_root(),
                use_flat_storage: true,
                source: near_chain::types::StorageDataSource::Db,
                state_patch: Default::default(),
            };
            self.runtime_adapter.prepare_transactions(
                storage_config,
                PrepareTransactionsChunkContext { shard_id, gas_limit: chunk_extra.gas_limit() },
                prev_block.into(),
                &mut iter,
                chain_validate,
                self.chunk_transactions_time_limit.get(),
            )?
        } else {
            PreparedTransactions { transactions: Vec::new(), limited_by: None }
        };
        // Reintroduce valid transactions back to the pool. They will be removed when the chunk is
        // included into the block.
        let reintroduced_count = pool_guard
            .reintroduce_transactions(shard_uid, prepared_transactions.transactions.clone());

        if reintroduced_count < prepared_transactions.transactions.len() {
            debug!(target: "client", reintroduced_count, num_tx = prepared_transactions.transactions.len(), "Reintroduced transactions");
        }
        Ok(prepared_transactions)
    }
}
