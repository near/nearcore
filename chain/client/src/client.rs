//! Client is responsible for tracking the chain, chunks, and producing them when needed.
//! This client works completely synchronously and must be operated by some async actor outside.

use std::collections::{HashMap, HashSet};
use std::iter;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use cached::{Cached, SizedCache};
use chrono::Utc;
use log::{debug, error, info, warn};

use near_chain::chain::TX_ROUTING_HEIGHT_HORIZON;
use near_chain::test_utils::format_hash;
use near_chain::types::{AcceptedBlock, LatestKnown};
use near_chain::{
    BlockStatus, Chain, ChainGenesis, ChainStoreAccess, Doomslug, DoomslugThresholdMode, ErrorKind,
    Provenance, RuntimeAdapter,
};
use near_chain_configs::ClientConfig;
use near_chunks::{ProcessPartialEncodedChunkResult, ShardsManager};
use near_network::types::PartialEncodedChunkResponseMsg;
use near_network::{
    FullPeerInfo, NetworkAdapter, NetworkClientResponses, NetworkRequests,
    EPOCH_SYNC_PEER_TIMEOUT_MS, EPOCH_SYNC_REQUEST_TIMEOUT_MS,
};
use near_primitives::block::{Approval, ApprovalInner, ApprovalMessage, Block, BlockHeader, Tip};
use near_primitives::challenge::{Challenge, ChallengeBody};
use near_primitives::hash::CryptoHash;
use near_primitives::merkle::{merklize, MerklePath};
use near_primitives::receipt::Receipt;
use near_primitives::sharding::{
    EncodedShardChunk, PartialEncodedChunk, PartialEncodedChunkV2, ReedSolomonWrapper,
    ShardChunkHeader,
};
use near_primitives::syncing::ReceiptResponse;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::chunk_extra::ChunkExtra;
#[cfg(feature = "protocol_feature_block_header_v3")]
use near_primitives::types::NumBlocks;
use near_primitives::types::{AccountId, ApprovalStake, BlockHeight, EpochId, ShardId};
use near_primitives::unwrap_or_return;
use near_primitives::utils::{to_timestamp, MaybeValidated};
use near_primitives::validator_signer::ValidatorSigner;

use crate::metrics;
use crate::sync::{BlockSync, EpochSync, HeaderSync, StateSync, StateSyncResult};
use crate::SyncStatus;
use near_client_primitives::types::{Error, ShardSyncDownload};
use near_primitives::block_header::ApprovalType;
use near_primitives::version::{ProtocolVersion, PROTOCOL_VERSION};

use near_network::types::PartialEncodedChunkForwardMsg;

const NUM_REBROADCAST_BLOCKS: usize = 30;

pub struct Client {
    /// Adversarial controls
    #[cfg(feature = "adversarial")]
    pub adv_produce_blocks: bool,
    #[cfg(feature = "adversarial")]
    pub adv_produce_blocks_only_valid: bool,

    pub config: ClientConfig,
    pub sync_status: SyncStatus,
    pub chain: Chain,
    pub doomslug: Doomslug,
    pub runtime_adapter: Arc<dyn RuntimeAdapter>,
    pub shards_mgr: ShardsManager,
    /// Network adapter.
    network_adapter: Arc<dyn NetworkAdapter>,
    /// Signer for block producer (if present).
    pub validator_signer: Option<Arc<dyn ValidatorSigner>>,
    /// Approvals for which we do not have the block yet
    pending_approvals: SizedCache<ApprovalInner, HashMap<AccountId, (Approval, ApprovalType)>>,
    /// A mapping from a block for which a state sync is underway for the next epoch, and the object
    /// storing the current status of the state sync
    pub catchup_state_syncs: HashMap<CryptoHash, (StateSync, HashMap<u64, ShardSyncDownload>)>,
    /// Keeps track of information needed to perform the initial Epoch Sync
    pub epoch_sync: EpochSync,
    /// Keeps track of syncing headers.
    pub header_sync: HeaderSync,
    /// Keeps track of syncing block.
    pub block_sync: BlockSync,
    /// Keeps track of syncing state.
    pub state_sync: StateSync,
    /// List of currently accumulated challenges.
    pub challenges: HashMap<CryptoHash, Challenge>,
    /// A ReedSolomon instance to reconstruct shard.
    rs: ReedSolomonWrapper,
    /// Blocks that have been re-broadcast recently. They should not be broadcast again.
    rebroadcasted_blocks: SizedCache<CryptoHash, ()>,
    /// Last time the head was updated, or our head was rebroadcasted. Used to re-broadcast the head
    /// again to prevent network from stalling if a large percentage of the network missed a block
    last_time_head_progress_made: Instant,
}

impl Client {
    pub fn new(
        config: ClientConfig,
        chain_genesis: ChainGenesis,
        runtime_adapter: Arc<dyn RuntimeAdapter>,
        network_adapter: Arc<dyn NetworkAdapter>,
        validator_signer: Option<Arc<dyn ValidatorSigner>>,
        enable_doomslug: bool,
    ) -> Result<Self, Error> {
        let doomslug_threshold_mode = if enable_doomslug {
            DoomslugThresholdMode::TwoThirds
        } else {
            DoomslugThresholdMode::NoApprovals
        };
        let chain = Chain::new(runtime_adapter.clone(), &chain_genesis, doomslug_threshold_mode)?;
        let shards_mgr = ShardsManager::new(
            validator_signer.as_ref().map(|x| x.validator_id().clone()),
            runtime_adapter.clone(),
            network_adapter.clone(),
        );
        let sync_status = SyncStatus::AwaitingPeers;
        let genesis_block = chain.genesis_block();
        let epoch_sync = EpochSync::new(
            network_adapter.clone(),
            genesis_block.header().epoch_id().clone(),
            genesis_block.header().next_epoch_id().clone(),
            runtime_adapter
                .get_epoch_block_producers_ordered(
                    &genesis_block.header().epoch_id(),
                    &genesis_block.hash(),
                )?
                .iter()
                .map(|x| x.0.clone().into())
                .collect(),
            Duration::from_millis(EPOCH_SYNC_REQUEST_TIMEOUT_MS),
            Duration::from_millis(EPOCH_SYNC_PEER_TIMEOUT_MS),
        );
        let header_sync = HeaderSync::new(
            network_adapter.clone(),
            config.header_sync_initial_timeout,
            config.header_sync_progress_timeout,
            config.header_sync_stall_ban_timeout,
            config.header_sync_expected_height_per_second,
        );
        let block_sync =
            BlockSync::new(network_adapter.clone(), config.block_fetch_horizon, config.archive);
        let state_sync = StateSync::new(network_adapter.clone(), config.state_sync_timeout);
        let num_block_producer_seats = config.num_block_producer_seats as usize;
        let data_parts = runtime_adapter.num_data_parts();
        let parity_parts = runtime_adapter.num_total_parts() - data_parts;

        let doomslug = Doomslug::new(
            chain.store().largest_target_height()?,
            config.min_block_production_delay,
            config.max_block_production_delay,
            config.max_block_production_delay / 10,
            config.max_block_wait_delay,
            validator_signer.clone(),
            doomslug_threshold_mode,
        );

        Ok(Self {
            #[cfg(feature = "adversarial")]
            adv_produce_blocks: false,
            #[cfg(feature = "adversarial")]
            adv_produce_blocks_only_valid: false,
            config,
            sync_status,
            chain,
            doomslug,
            runtime_adapter,
            shards_mgr,
            network_adapter,
            validator_signer,
            pending_approvals: SizedCache::with_size(num_block_producer_seats),
            catchup_state_syncs: HashMap::new(),
            epoch_sync,
            header_sync,
            block_sync,
            state_sync,
            challenges: Default::default(),
            rs: ReedSolomonWrapper::new(data_parts, parity_parts),
            rebroadcasted_blocks: SizedCache::with_size(NUM_REBROADCAST_BLOCKS),
            last_time_head_progress_made: Instant::now(),
        })
    }

    // Checks if it's been at least `stall_timeout` since the last time the head was updated, or
    // this method was called. If yes, rebroadcasts the current head.
    pub fn check_head_progress_stalled(&mut self, stall_timeout: Duration) -> Result<(), Error> {
        if Instant::now() > self.last_time_head_progress_made + stall_timeout
            && !self.sync_status.is_syncing()
        {
            let block = self.chain.get_block(&self.chain.head()?.last_block_hash)?;
            self.network_adapter.do_send(NetworkRequests::Block { block: block.clone() });
            self.last_time_head_progress_made = Instant::now();
        }
        Ok(())
    }

    pub fn remove_transactions_for_block(&mut self, me: AccountId, block: &Block) {
        for (shard_id, chunk_header) in block.chunks().iter().enumerate() {
            let shard_id = shard_id as ShardId;
            if block.header().height() == chunk_header.height_included() {
                if self.shards_mgr.cares_about_shard_this_or_next_epoch(
                    Some(&me),
                    &block.header().prev_hash(),
                    shard_id,
                    true,
                ) {
                    self.shards_mgr.remove_transactions(
                        shard_id,
                        // By now the chunk must be in store, otherwise the block would have been orphaned
                        self.chain.get_chunk(&chunk_header.chunk_hash()).unwrap().transactions(),
                    );
                }
            }
        }
        for challenge in block.challenges().iter() {
            self.challenges.remove(&challenge.hash);
        }
    }

    pub fn reintroduce_transactions_for_block(&mut self, me: AccountId, block: &Block) {
        for (shard_id, chunk_header) in block.chunks().iter().enumerate() {
            let shard_id = shard_id as ShardId;
            if block.header().height() == chunk_header.height_included() {
                if self.shards_mgr.cares_about_shard_this_or_next_epoch(
                    Some(&me),
                    &block.header().prev_hash(),
                    shard_id,
                    false,
                ) {
                    self.shards_mgr.reintroduce_transactions(
                        shard_id,
                        // By now the chunk must be in store, otherwise the block would have been orphaned
                        self.chain.get_chunk(&chunk_header.chunk_hash()).unwrap().transactions(),
                    );
                }
            }
        }
        for challenge in block.challenges().iter() {
            self.challenges.insert(challenge.hash, challenge.clone());
        }
    }

    /// Check that this block height is not known yet.
    fn known_block_height(&self, next_height: BlockHeight, known_height: BlockHeight) -> bool {
        #[cfg(feature = "adversarial")]
        {
            if self.adv_produce_blocks {
                return false;
            }
        }

        next_height <= known_height
    }

    /// Check that we are next block producer.
    fn is_me_block_producer(
        &self,
        account_id: &AccountId,
        next_block_proposer: &AccountId,
    ) -> bool {
        #[cfg(feature = "adversarial")]
        {
            if self.adv_produce_blocks_only_valid {
                return account_id == next_block_proposer;
            }
            if self.adv_produce_blocks {
                return true;
            }
        }

        account_id == next_block_proposer
    }

    fn should_reschedule_block(
        &self,
        head: &Tip,
        prev_hash: &CryptoHash,
        prev_prev_hash: &CryptoHash,
        next_height: BlockHeight,
        known_height: BlockHeight,
        account_id: &AccountId,
        next_block_proposer: &AccountId,
    ) -> Result<bool, Error> {
        if self.known_block_height(next_height, known_height) {
            return Ok(true);
        }

        if !self.is_me_block_producer(account_id, &next_block_proposer) {
            info!(target: "client", "Produce block: chain at {}, not block producer for next block.", next_height);
            return Ok(true);
        }

        #[cfg(feature = "adversarial")]
        {
            if self.adv_produce_blocks {
                return Ok(false);
            }
        }

        if self.runtime_adapter.is_next_block_epoch_start(&head.last_block_hash)? {
            if !self.chain.prev_block_is_caught_up(&prev_prev_hash, &prev_hash)? {
                // Currently state for the chunks we are interested in this epoch
                // are not yet caught up (e.g. still state syncing).
                // We reschedule block production.
                // Alex's comment:
                // The previous block is not caught up for the next epoch relative to the previous
                // block, which is the current epoch for this block, so this block cannot be applied
                // at all yet, block production must to be rescheduled
                debug!(target: "client", "Produce block: prev block is not caught up");
                return Ok(true);
            }
        }

        Ok(false)
    }

    /// Produce block if we are block producer for given `next_height` block height.
    /// Either returns produced block (not applied) or error.
    pub fn produce_block(&mut self, next_height: BlockHeight) -> Result<Option<Block>, Error> {
        let known_height = self.chain.mut_store().get_latest_known()?.height;

        let validator_signer = self
            .validator_signer
            .as_ref()
            .ok_or_else(|| Error::BlockProducer("Called without block producer info.".to_string()))?
            .clone();
        let head = self.chain.head()?;
        assert_eq!(
            head.epoch_id,
            self.runtime_adapter.get_epoch_id_from_prev_block(&head.prev_block_hash).unwrap()
        );

        // Check that we are were called at the block that we are producer for.
        let epoch_id =
            self.runtime_adapter.get_epoch_id_from_prev_block(&head.last_block_hash).unwrap();
        let next_block_proposer =
            self.runtime_adapter.get_block_producer(&epoch_id, next_height)?;

        let prev = self.chain.get_block_header(&head.last_block_hash)?.clone();
        let prev_hash = head.last_block_hash;
        let prev_height = head.height;
        let prev_prev_hash = *prev.prev_hash();
        let prev_epoch_id = prev.epoch_id().clone();
        let prev_next_bp_hash = *prev.next_bp_hash();

        // Check and update the doomslug tip here. This guarantees that our endorsement will be in the
        // doomslug witness. Have to do it before checking the ability to produce a block.
        let _ = self.check_and_update_doomslug_tip()?;

        if self.should_reschedule_block(
            &head,
            &prev_hash,
            &prev_prev_hash,
            next_height,
            known_height,
            &validator_signer.validator_id(),
            &next_block_proposer,
        )? {
            return Ok(None);
        }
        let (validator_stake, _) = self.runtime_adapter.get_validator_by_account_id(
            &epoch_id,
            &head.last_block_hash,
            &next_block_proposer,
        )?;

        let validator_pk = validator_stake.take_public_key();
        if validator_pk != validator_signer.public_key() {
            debug!(target: "client", "Local validator key {} does not match expected validator key {}, skipping block production", validator_signer.public_key(), validator_pk);
            #[cfg(not(feature = "adversarial"))]
            return Ok(None);
            #[cfg(feature = "adversarial")]
            if !self.adv_produce_blocks || self.adv_produce_blocks_only_valid {
                return Ok(None);
            }
        }

        debug!(target: "client", "{:?} Producing block at height {}, parent {} @ {}", validator_signer.validator_id(), next_height, prev.height(), format_hash(head.last_block_hash));

        let new_chunks = self.shards_mgr.prepare_chunks(&prev_hash);
        // If we are producing empty blocks and there are no transactions.
        if !self.config.produce_empty_blocks && new_chunks.is_empty() {
            debug!(target: "client", "Empty blocks, skipping block production");
            return Ok(None);
        }

        let mut approvals_map = self.doomslug.remove_witness(&prev_hash, prev_height, next_height);

        // At this point, the previous epoch hash must be available
        let epoch_id = self
            .runtime_adapter
            .get_epoch_id_from_prev_block(&head.last_block_hash)
            .expect("Epoch hash should exist at this point");

        let approvals = self
            .runtime_adapter
            .get_epoch_block_approvers_ordered(&prev_hash)?
            .into_iter()
            .map(|(ApprovalStake { account_id, .. }, is_slashed)| {
                if is_slashed {
                    None
                } else {
                    approvals_map.remove(&account_id).map(|x| x.signature)
                }
            })
            .collect();

        debug_assert_eq!(approvals_map.len(), 0);

        let next_epoch_id = self
            .runtime_adapter
            .get_next_epoch_id_from_prev_block(&head.last_block_hash)
            .expect("Epoch hash should exist at this point");

        let protocol_version = self.runtime_adapter.get_epoch_protocol_version(&epoch_id)?;
        let gas_price_adjustment_rate =
            self.chain.block_economics_config.gas_price_adjustment_rate(protocol_version);
        let min_gas_price = self.chain.block_economics_config.min_gas_price(protocol_version);
        let max_gas_price = self.chain.block_economics_config.max_gas_price(protocol_version);

        let next_bp_hash = if prev_epoch_id != epoch_id {
            Chain::compute_bp_hash(&*self.runtime_adapter, next_epoch_id.clone(), &prev_hash)?
        } else {
            prev_next_bp_hash
        };

        // Get block extra from previous block.
        let mut block_merkle_tree =
            self.chain.mut_store().get_block_merkle_tree(&prev_hash)?.clone();
        block_merkle_tree.insert(prev_hash);
        let block_merkle_root = block_merkle_tree.root();
        // The number of leaves in Block Merkle Tree is the amount of Blocks on the Canonical Chain by construction.
        // The ordinal of the next Block will be equal to this amount plus one.
        #[cfg(feature = "protocol_feature_block_header_v3")]
        let block_ordinal: NumBlocks = block_merkle_tree.size() + 1;
        let prev_block_extra = self.chain.get_block_extra(&prev_hash)?.clone();
        let prev_block = self.chain.get_block(&prev_hash)?;
        let mut chunks: Vec<_> = prev_block.chunks().iter().cloned().collect();

        // Collect new chunks.
        for (shard_id, mut chunk_header) in new_chunks {
            *chunk_header.height_included_mut() = next_height;
            chunks[shard_id as usize] = chunk_header;
        }

        let prev_header = &prev_block.header();

        let next_epoch_id =
            self.runtime_adapter.get_next_epoch_id_from_prev_block(&head.last_block_hash)?;

        let minted_amount =
            if self.runtime_adapter.is_next_block_epoch_start(&head.last_block_hash)? {
                Some(self.runtime_adapter.get_epoch_minted_amount(&next_epoch_id)?)
            } else {
                None
            };

        #[cfg(feature = "protocol_feature_block_header_v3")]
        let epoch_sync_data_hash =
            if self.runtime_adapter.is_next_block_epoch_start(&head.last_block_hash)? {
                Some(self.runtime_adapter.get_epoch_sync_data_hash(
                    prev_block.hash(),
                    &epoch_id,
                    &next_epoch_id,
                )?)
            } else {
                None
            };

        // Get all the current challenges.
        // TODO(2445): Enable challenges when they are working correctly.
        // let challenges = self.challenges.drain().map(|(_, challenge)| challenge).collect();
        let protocol_version = self.runtime_adapter.get_epoch_protocol_version(&next_epoch_id)?;

        let block = Block::produce(
            protocol_version,
            &prev_header,
            next_height,
            #[cfg(feature = "protocol_feature_block_header_v3")]
            block_ordinal,
            chunks,
            epoch_id,
            next_epoch_id,
            #[cfg(feature = "protocol_feature_block_header_v3")]
            epoch_sync_data_hash,
            approvals,
            gas_price_adjustment_rate,
            min_gas_price,
            max_gas_price,
            minted_amount,
            prev_block_extra.challenges_result,
            vec![],
            &*validator_signer,
            next_bp_hash,
            block_merkle_root,
        );

        // Update latest known even before returning block out, to prevent race conditions.
        self.chain.mut_store().save_latest_known(LatestKnown {
            height: next_height,
            seen: to_timestamp(Utc::now()),
        })?;

        near_metrics::inc_counter(&metrics::BLOCK_PRODUCED_TOTAL);

        Ok(Some(block))
    }

    pub fn produce_chunk(
        &mut self,
        prev_block_hash: CryptoHash,
        epoch_id: &EpochId,
        last_header: ShardChunkHeader,
        next_height: BlockHeight,
        shard_id: ShardId,
    ) -> Result<Option<(EncodedShardChunk, Vec<MerklePath>, Vec<Receipt>)>, Error> {
        let validator_signer = self
            .validator_signer
            .as_ref()
            .ok_or_else(|| Error::ChunkProducer("Called without block producer info.".to_string()))?
            .clone();

        let chunk_proposer =
            self.runtime_adapter.get_chunk_producer(epoch_id, next_height, shard_id).unwrap();
        if validator_signer.validator_id() != &chunk_proposer {
            debug!(target: "client", "Not producing chunk for shard {}: chain at {}, not block producer for next block. Me: {}, proposer: {}", shard_id, next_height, validator_signer.validator_id(), chunk_proposer);
            return Ok(None);
        }

        if self.runtime_adapter.is_next_block_epoch_start(&prev_block_hash)? {
            let prev_prev_hash = *self.chain.get_block_header(&prev_block_hash)?.prev_hash();
            if !self.chain.prev_block_is_caught_up(&prev_prev_hash, &prev_block_hash)? {
                // See comment in similar snipped in `produce_block`
                debug!(target: "client", "Produce chunk: prev block is not caught up");
                return Err(Error::ChunkProducer(
                    "State for the epoch is not downloaded yet, skipping chunk production"
                        .to_string(),
                ));
            }
        }

        debug!(
            target: "client",
            "Producing chunk at height {} for shard {}, I'm {}",
            next_height,
            shard_id,
            validator_signer.validator_id()
        );

        let chunk_extra = self
            .chain
            .get_chunk_extra(&prev_block_hash, shard_id)
            .map_err(|err| Error::ChunkProducer(format!("No chunk extra available: {}", err)))?
            .clone();

        let prev_block_header = self.chain.get_block_header(&prev_block_hash)?.clone();
        let transactions = self.prepare_transactions(shard_id, &chunk_extra, &prev_block_header)?;
        let num_filtered_transactions = transactions.len();
        let (tx_root, _) = merklize(&transactions);
        let ReceiptResponse(_, outgoing_receipts) = self.chain.get_outgoing_receipts_for_shard(
            prev_block_hash,
            shard_id,
            last_header.height_included(),
        )?;

        // Receipts proofs root is calculating here
        //
        // For each subset of incoming_receipts_into_shard_i_from_the_current_one
        // we calculate hash here and save it
        // and then hash all of them into a single receipts root
        //
        // We check validity in two ways:
        // 1. someone who cares about shard will download all the receipts
        // and checks that receipts_root equals to all receipts hashed
        // 2. anyone who just asks for one's incoming receipts
        // will receive a piece of incoming receipts only
        // with merkle receipts proofs which can be checked locally
        let outgoing_receipts_hashes =
            self.runtime_adapter.build_receipts_hashes(&outgoing_receipts);
        let (outgoing_receipts_root, _) = merklize(&outgoing_receipts_hashes);

        let protocol_version = self.runtime_adapter.get_epoch_protocol_version(epoch_id)?;
        let (encoded_chunk, merkle_paths) = self.shards_mgr.create_encoded_shard_chunk(
            prev_block_hash,
            *chunk_extra.state_root(),
            *chunk_extra.outcome_root(),
            next_height,
            shard_id,
            chunk_extra.gas_used(),
            chunk_extra.gas_limit(),
            chunk_extra.balance_burnt(),
            chunk_extra.validator_proposals().collect(),
            transactions,
            &outgoing_receipts,
            outgoing_receipts_root,
            tx_root,
            &*validator_signer,
            &mut self.rs,
            protocol_version,
        )?;

        debug!(
            target: "client",
            "Produced chunk at height {} for shard {} with {} txs and {} receipts, I'm {}, chunk_hash: {}",
            next_height,
            shard_id,
            num_filtered_transactions,
            outgoing_receipts.len(),
            validator_signer.validator_id(),
            encoded_chunk.chunk_hash().0,
        );

        near_metrics::inc_counter(&metrics::CHUNK_PRODUCED_TOTAL);
        Ok(Some((encoded_chunk, merkle_paths, outgoing_receipts)))
    }

    /// Prepares an ordered list of valid transactions from the pool up the limits.
    fn prepare_transactions(
        &mut self,
        shard_id: ShardId,
        chunk_extra: &ChunkExtra,
        prev_block_header: &BlockHeader,
    ) -> Result<Vec<SignedTransaction>, Error> {
        let Self { chain, shards_mgr, runtime_adapter, .. } = self;

        let next_epoch_id =
            runtime_adapter.get_epoch_id_from_prev_block(&prev_block_header.hash())?;
        let protocol_version = runtime_adapter.get_epoch_protocol_version(&next_epoch_id)?;

        let transactions = if let Some(mut iter) = shards_mgr.get_pool_iterator(shard_id) {
            let transaction_validity_period = chain.transaction_validity_period;
            runtime_adapter.prepare_transactions(
                prev_block_header.gas_price(),
                chunk_extra.gas_limit(),
                shard_id,
                *chunk_extra.state_root(),
                // while the height of the next block that includes the chunk might not be prev_height + 1,
                // passing it will result in a more conservative check and will not accidentally allow
                // invalid transactions to be included.
                prev_block_header.height() + 1,
                &mut iter,
                &mut |tx: &SignedTransaction| -> bool {
                    chain
                        .mut_store()
                        .check_transaction_validity_period(
                            &prev_block_header,
                            &tx.transaction.block_hash,
                            transaction_validity_period,
                        )
                        .is_ok()
                },
                protocol_version,
            )?
        } else {
            vec![]
        };
        // Reintroduce valid transactions back to the pool. They will be removed when the chunk is
        // included into the block.
        shards_mgr.reintroduce_transactions(shard_id, &transactions);
        Ok(transactions)
    }

    pub fn send_challenges(&mut self, challenges: Arc<RwLock<Vec<ChallengeBody>>>) {
        if let Some(validator_signer) = self.validator_signer.as_ref() {
            for body in challenges.write().unwrap().drain(..) {
                let challenge = Challenge::produce(body, &**validator_signer);
                self.challenges.insert(challenge.hash, challenge.clone());
                self.network_adapter.do_send(NetworkRequests::Challenge(challenge));
            }
        }
    }

    pub fn process_block(
        &mut self,
        block: Block,
        provenance: Provenance,
    ) -> (Vec<AcceptedBlock>, Result<Option<Tip>, near_chain::Error>) {
        let is_requested = match provenance {
            Provenance::PRODUCED | Provenance::SYNC => true,
            Provenance::NONE => false,
        };
        // drop the block if a) it is not requested, b) we already processed this height, c) it is not building on top of current head
        if !is_requested
            && block.header().prev_hash()
                != &self
                    .chain
                    .head()
                    .map_or_else(|_| CryptoHash::default(), |tip| tip.last_block_hash)
        {
            match self.chain.mut_store().is_height_processed(block.header().height()) {
                Ok(true) => return (vec![], Ok(None)),
                Ok(false) => {}
                Err(e) => return (vec![], Err(e)),
            }
        }
        // TODO: replace to channels or cross beams here? we don't have multi-threading here so it's mostly to get around borrow checker.
        let accepted_blocks = Arc::new(RwLock::new(vec![]));
        let blocks_missing_chunks = Arc::new(RwLock::new(vec![]));
        let challenges = Arc::new(RwLock::new(vec![]));
        let block_prev_hash = *block.header().prev_hash();
        let block_protocol_version = block.header().latest_protocol_version();

        let result = {
            let me = self
                .validator_signer
                .as_ref()
                .map(|validator_signer| validator_signer.validator_id().clone());
            self.chain.process_block(
                &me,
                block,
                provenance,
                |accepted_block| {
                    accepted_blocks.write().unwrap().push(accepted_block);
                },
                |missing_chunks| blocks_missing_chunks.write().unwrap().push(missing_chunks),
                |challenge| challenges.write().unwrap().push(challenge),
            )
        };

        // Send out challenges that accumulated via on_challenge.
        self.send_challenges(challenges);

        // Send out challenge if the block was found to be invalid.
        if let Some(validator_signer) = self.validator_signer.as_ref() {
            match &result {
                Err(e) => match e.kind() {
                    near_chain::ErrorKind::InvalidChunkProofs(chunk_proofs) => {
                        self.network_adapter.do_send(NetworkRequests::Challenge(
                            Challenge::produce(
                                ChallengeBody::ChunkProofs(*chunk_proofs),
                                &**validator_signer,
                            ),
                        ));
                    }
                    near_chain::ErrorKind::InvalidChunkState(chunk_state) => {
                        self.network_adapter.do_send(NetworkRequests::Challenge(
                            Challenge::produce(
                                ChallengeBody::ChunkState(*chunk_state),
                                &**validator_signer,
                            ),
                        ));
                    }
                    _ => {}
                },
                _ => {}
            }
        }

        if let Ok(Some(_)) = result {
            self.last_time_head_progress_made = Instant::now();
        }

        let protocol_version = self
            .runtime_adapter
            .get_epoch_id_from_prev_block(&block_prev_hash)
            .and_then(|epoch| self.runtime_adapter.get_epoch_protocol_version(&epoch))
            .unwrap_or(block_protocol_version);
        // Request any missing chunks
        self.shards_mgr.request_chunks(
            blocks_missing_chunks.write().unwrap().drain(..).flatten(),
            &self
                .chain
                .header_head()
                .expect("header_head must be available when processing a block"),
            protocol_version,
        );

        let unwrapped_accepted_blocks = accepted_blocks.write().unwrap().drain(..).collect();
        (unwrapped_accepted_blocks, result)
    }

    pub fn rebroadcast_block(&mut self, block: Block) {
        if self.rebroadcasted_blocks.cache_get(&block.hash()).is_none() {
            self.network_adapter.do_send(NetworkRequests::Block { block: block.clone() });
            self.rebroadcasted_blocks.cache_set(*block.hash(), ());
        }
    }

    pub fn process_partial_encoded_chunk_response(
        &mut self,
        response: PartialEncodedChunkResponseMsg,
    ) -> Result<Vec<AcceptedBlock>, Error> {
        let header = self.shards_mgr.get_partial_encoded_chunk_header(&response.chunk_hash)?;
        let partial_chunk = PartialEncodedChunk::new(header, response.parts, response.receipts);
        // We already know the header signature is valid because we read it from the
        // shard manager.
        self.process_partial_encoded_chunk(MaybeValidated::Validated(partial_chunk))
    }

    pub fn process_partial_encoded_chunk_forward(
        &mut self,
        forward: PartialEncodedChunkForwardMsg,
    ) -> Result<Vec<AcceptedBlock>, Error> {
        let maybe_header = self
            .shards_mgr
            .validate_partial_encoded_chunk_forward(&forward)
            .and_then(|_| self.shards_mgr.get_partial_encoded_chunk_header(&forward.chunk_hash));

        let header = match maybe_header {
            Ok(header) => Ok(header),
            Err(near_chunks::Error::UnknownChunk) => {
                // We don't know this chunk yet; cache the forwarded part
                // to be used after we get the header.
                self.shards_mgr.insert_forwarded_chunk(forward);
                return Err(Error::Chunk(near_chunks::Error::UnknownChunk));
            }
            Err(near_chunks::Error::ChainError(chain_error)) => {
                match chain_error.kind() {
                    near_chain::ErrorKind::DBNotFoundErr(_) => {
                        // We can't check if this chunk came from a valid chunk producer because
                        // we don't know `prev_block`, however the signature is checked when
                        // forwarded parts are later processed as partial encoded chunks, so we
                        // can mark it as unknown for now.
                        self.shards_mgr.insert_forwarded_chunk(forward);
                        return Err(Error::Chunk(near_chunks::Error::UnknownChunk));
                    }
                    // Some other error occurred, we don't know how to handle it
                    _ => Err(near_chunks::Error::ChainError(chain_error)),
                }
            }
            Err(err) => Err(err),
        }?;
        let partial_chunk = PartialEncodedChunk::V2(PartialEncodedChunkV2 {
            header,
            parts: forward.parts,
            receipts: Vec::new(),
        });
        // We already know the header signature is valid because we read it from the
        // shard manager.
        self.process_partial_encoded_chunk(MaybeValidated::Validated(partial_chunk))
    }

    pub fn process_partial_encoded_chunk(
        &mut self,
        partial_encoded_chunk: MaybeValidated<PartialEncodedChunk>,
    ) -> Result<Vec<AcceptedBlock>, Error> {
        fn missing_block_handler(
            client: &mut Client,
            pec: PartialEncodedChunkV2,
        ) -> Result<Vec<AcceptedBlock>, Error> {
            client.shards_mgr.store_partial_encoded_chunk(client.chain.head_header()?, pec);
            Ok(vec![])
        }
        let block_hash = partial_encoded_chunk.prev_block();
        match self.runtime_adapter.get_epoch_id_from_prev_block(block_hash) {
            Ok(epoch_id) => {
                let protocol_version =
                    self.runtime_adapter.get_epoch_protocol_version(&epoch_id)?;

                if !partial_encoded_chunk.version_range().contains(protocol_version) {
                    return Err(Error::Other("Invalid chunk version".to_string()));
                };

                let chunk_hash = partial_encoded_chunk.chunk_hash();
                let pec_v2: MaybeValidated<PartialEncodedChunkV2> =
                    partial_encoded_chunk.map(Into::into);
                let process_result = self.shards_mgr.process_partial_encoded_chunk(
                    pec_v2.as_ref(),
                    self.chain.mut_store(),
                    &mut self.rs,
                    protocol_version,
                )?;

                match process_result {
                    ProcessPartialEncodedChunkResult::Known => Ok(vec![]),
                    ProcessPartialEncodedChunkResult::HaveAllPartsAndReceipts(_) => {
                        self.chain.blocks_with_missing_chunks.accept_chunk(&chunk_hash);
                        Ok(self.process_blocks_with_missing_chunks(protocol_version))
                    }
                    ProcessPartialEncodedChunkResult::NeedMorePartsOrReceipts => {
                        let chunk_header = pec_v2.extract().header;
                        self.shards_mgr.request_chunks(
                            iter::once(chunk_header),
                            &self.chain.header_head()?,
                            protocol_version,
                        );
                        Ok(vec![])
                    }
                    ProcessPartialEncodedChunkResult::NeedBlock => {
                        missing_block_handler(self, pec_v2.extract())
                    }
                }
            }

            // If the epoch_id cannot be looked up then we have not processed
            // `partial_encoded_chunk.prev_block()` yet.
            Err(_) => missing_block_handler(self, partial_encoded_chunk.extract().into()),
        }
    }

    pub fn sync_block_headers(
        &mut self,
        headers: Vec<BlockHeader>,
    ) -> Result<(), near_chain::Error> {
        let challenges = Arc::new(RwLock::new(vec![]));
        self.chain
            .sync_block_headers(headers, |challenge| challenges.write().unwrap().push(challenge))?;
        self.send_challenges(challenges);
        Ok(())
    }

    /// Checks if the latest hash known to Doomslug matches the current head, and updates it if not.
    pub fn check_and_update_doomslug_tip(&mut self) -> Result<(), Error> {
        let tip = self.chain.head()?;

        if tip.last_block_hash != self.doomslug.get_tip().0 {
            // We need to update the doomslug tip
            let last_final_hash =
                *self.chain.get_block_header(&tip.last_block_hash)?.last_final_block();
            let last_final_height = if last_final_hash == CryptoHash::default() {
                self.chain.genesis().height()
            } else {
                self.chain.get_block_header(&last_final_hash)?.height()
            };

            self.doomslug.set_tip(
                Instant::now(),
                tip.last_block_hash,
                tip.height,
                last_final_height,
            );
        }

        Ok(())
    }

    pub fn send_approval(
        &mut self,
        parent_hash: &CryptoHash,
        approval: Approval,
    ) -> Result<(), Error> {
        let next_epoch_id = self.runtime_adapter.get_epoch_id_from_prev_block(parent_hash)?;
        let next_block_producer =
            self.runtime_adapter.get_block_producer(&next_epoch_id, approval.target_height)?;
        if Some(&next_block_producer) == self.validator_signer.as_ref().map(|x| x.validator_id()) {
            self.collect_block_approval(&approval, ApprovalType::SelfApproval);
        } else {
            debug!(target: "client", "Sending an approval {:?} from {} to {} for {}", approval.inner, approval.account_id, next_block_producer.clone(), approval.target_height);
            let approval_message = ApprovalMessage::new(approval, next_block_producer);
            self.network_adapter.do_send(NetworkRequests::Approval { approval_message });
        }

        Ok(())
    }

    /// Gets called when block got accepted.
    /// Send updates over network, update tx pool and notify ourselves if it's time to produce next block.
    /// Blocks are passed in no particular order.
    pub fn on_block_accepted(
        &mut self,
        block_hash: CryptoHash,
        status: BlockStatus,
        provenance: Provenance,
    ) {
        let block = match self.chain.get_block(&block_hash) {
            Ok(block) => block.clone(),
            Err(err) => {
                error!(target: "client", "Failed to find block {} that was just accepted: {}", block_hash, err);
                return;
            }
        };

        let _ = self.check_and_update_doomslug_tip();

        // If we produced the block, then it should have already been broadcasted.
        // If received the block from another node then broadcast "header first" to minimize network traffic.
        if provenance == Provenance::NONE {
            let endorsements = self
                .pending_approvals
                .cache_remove(&ApprovalInner::Endorsement(block_hash))
                .unwrap_or_default();
            let skips = self
                .pending_approvals
                .cache_remove(&ApprovalInner::Skip(block.header().height()))
                .unwrap_or_default();

            for (_account_id, (approval, approval_type)) in
                endorsements.into_iter().chain(skips.into_iter())
            {
                self.collect_block_approval(&approval, approval_type);
            }
        }

        if status.is_new_head() {
            self.shards_mgr.update_largest_seen_height(block.header().height());
            let last_final_block = block.header().last_final_block();
            let last_finalized_height = if last_final_block == &CryptoHash::default() {
                self.chain.genesis().height()
            } else {
                self.chain.get_block_header(last_final_block).map_or(0, |header| header.height())
            };
            self.chain.blocks_with_missing_chunks.prune_blocks_below_height(last_finalized_height);
            if !self.config.archive {
                let timer = near_metrics::start_timer(&metrics::GC_TIME);
                if let Err(err) = self
                    .chain
                    .clear_data(self.runtime_adapter.get_tries(), self.config.gc_blocks_limit)
                {
                    error!(target: "client", "Can't clear old data, {:?}", err);
                    debug_assert!(false);
                };
                near_metrics::stop_timer(timer);
            }

            if self.runtime_adapter.is_next_block_epoch_start(block.hash()).unwrap_or(false) {
                let next_epoch_protocol_version = unwrap_or_return!(self
                    .runtime_adapter
                    .get_epoch_protocol_version(block.header().next_epoch_id()));
                if next_epoch_protocol_version > PROTOCOL_VERSION {
                    panic!("The client protocol version is older than the protocol version of the network. Please update nearcore");
                }
            }
        }

        if let Some(validator_signer) = self.validator_signer.clone() {
            // Reconcile the txpool against the new block *after* we have broadcast it too our peers.
            // This may be slow and we do not want to delay block propagation.
            match status {
                BlockStatus::Next => {
                    // If this block immediately follows the current tip, remove transactions
                    //    from the txpool
                    self.remove_transactions_for_block(
                        validator_signer.validator_id().clone(),
                        &block,
                    );
                }
                BlockStatus::Fork => {
                    // If it's a fork, no need to reconcile transactions or produce chunks
                    return;
                }
                BlockStatus::Reorg(prev_head) => {
                    // If a reorg happened, reintroduce transactions from the previous chain and
                    //    remove transactions from the new chain
                    let mut reintroduce_head =
                        self.chain.get_block_header(&prev_head).unwrap().clone();
                    let mut remove_head = block.header().clone();
                    assert_ne!(remove_head.hash(), reintroduce_head.hash());

                    let mut to_remove = vec![];
                    let mut to_reintroduce = vec![];

                    while remove_head.hash() != reintroduce_head.hash() {
                        while remove_head.height() > reintroduce_head.height() {
                            to_remove.push(*remove_head.hash());
                            remove_head = self
                                .chain
                                .get_block_header(remove_head.prev_hash())
                                .unwrap()
                                .clone();
                        }
                        while reintroduce_head.height() > remove_head.height()
                            || reintroduce_head.height() == remove_head.height()
                                && reintroduce_head.hash() != remove_head.hash()
                        {
                            to_reintroduce.push(*reintroduce_head.hash());
                            reintroduce_head = self
                                .chain
                                .get_block_header(reintroduce_head.prev_hash())
                                .unwrap()
                                .clone();
                        }
                    }

                    for to_reintroduce_hash in to_reintroduce {
                        if let Ok(block) = self.chain.get_block(&to_reintroduce_hash) {
                            let block = block.clone();
                            self.reintroduce_transactions_for_block(
                                validator_signer.validator_id().clone(),
                                &block,
                            );
                        }
                    }

                    for to_remove_hash in to_remove {
                        if let Ok(block) = self.chain.get_block(&to_remove_hash) {
                            let block = block.clone();
                            self.remove_transactions_for_block(
                                validator_signer.validator_id().clone(),
                                &block,
                            );
                        }
                    }
                }
            };

            if provenance != Provenance::SYNC && !self.sync_status.is_syncing() {
                // Produce new chunks
                for shard_id in 0..self.runtime_adapter.num_shards() {
                    let epoch_id = self
                        .runtime_adapter
                        .get_epoch_id_from_prev_block(&block.header().hash())
                        .unwrap();
                    let chunk_proposer = self
                        .runtime_adapter
                        .get_chunk_producer(&epoch_id, block.header().height() + 1, shard_id)
                        .unwrap();

                    if chunk_proposer == *validator_signer.validator_id() {
                        match self.produce_chunk(
                            *block.hash(),
                            &epoch_id,
                            block.chunks()[shard_id as usize].clone(),
                            block.header().height() + 1,
                            shard_id,
                        ) {
                            Ok(Some((encoded_chunk, merkle_paths, receipts))) => self
                                .shards_mgr
                                .distribute_encoded_chunk(
                                    encoded_chunk,
                                    merkle_paths,
                                    receipts,
                                    self.chain.mut_store(),
                                )
                                .expect("Failed to process produced chunk"),
                            Ok(None) => {}
                            Err(err) => {
                                error!(target: "client", "Error producing chunk {:?}", err);
                            }
                        }
                    }
                }
            }
        }

        // Process stored partial encoded chunks
        let next_height = block.header().height() + 1;
        let mut partial_encoded_chunks =
            self.shards_mgr.get_stored_partial_encoded_chunks(next_height);
        for (_shard_id, partial_encoded_chunk) in partial_encoded_chunks.drain() {
            let chunk =
                MaybeValidated::NotValidated(PartialEncodedChunk::V2(partial_encoded_chunk));
            if let Ok(accepted_blocks) = self.process_partial_encoded_chunk(chunk) {
                // Executing process_partial_encoded_chunk can unlock some blocks.
                // Any block that is in the blocks_with_missing_chunks which doesn't have any chunks
                // for which we track shards will be unblocked here.
                for accepted_block in accepted_blocks {
                    self.on_block_accepted(
                        accepted_block.hash,
                        accepted_block.status,
                        accepted_block.provenance,
                    );
                }
            }
        }
    }

    /// Check if any block with missing chunks is ready to be processed
    #[must_use]
    pub fn process_blocks_with_missing_chunks(
        &mut self,
        protocol_version: ProtocolVersion,
    ) -> Vec<AcceptedBlock> {
        let accepted_blocks = Arc::new(RwLock::new(vec![]));
        let blocks_missing_chunks = Arc::new(RwLock::new(vec![]));
        let challenges = Arc::new(RwLock::new(vec![]));
        let me =
            self.validator_signer.as_ref().map(|validator_signer| validator_signer.validator_id());
        self.chain.check_blocks_with_missing_chunks(&me.map(|x| x.clone()), |accepted_block| {
            debug!(target: "client", "Block {} was missing chunks but now is ready to be processed", accepted_block.hash);
            accepted_blocks.write().unwrap().push(accepted_block);
        }, |missing_chunks| blocks_missing_chunks.write().unwrap().push(missing_chunks), |challenge| challenges.write().unwrap().push(challenge));
        self.send_challenges(challenges);

        self.shards_mgr.request_chunks(
            blocks_missing_chunks.write().unwrap().drain(..).flatten(),
            &self
                .chain
                .header_head()
                .expect("header_head must be avaiable when processing blocks with missing chunks"),
            protocol_version,
        );

        let unwrapped_accepted_blocks = accepted_blocks.write().unwrap().drain(..).collect();
        unwrapped_accepted_blocks
    }

    pub fn is_validator(&self, epoch_id: &EpochId, block_hash: &CryptoHash) -> bool {
        match self.validator_signer.as_ref() {
            None => false,
            Some(signer) => {
                let account_id = signer.validator_id();
                match self
                    .runtime_adapter
                    .get_validator_by_account_id(epoch_id, block_hash, account_id)
                {
                    Ok((validator_stake, is_slashed)) => {
                        !is_slashed && validator_stake.take_public_key() == signer.public_key()
                    }
                    Err(_) => false,
                }
            }
        }
    }

    fn handle_process_approval_error(
        &mut self,
        approval: &Approval,
        approval_type: ApprovalType,
        check_validator: bool,
        error: near_chain::Error,
    ) {
        let is_validator =
            |epoch_id, block_hash, account_id, runtime_adapter: &Arc<dyn RuntimeAdapter>| {
                match runtime_adapter.get_validator_by_account_id(epoch_id, block_hash, account_id)
                {
                    Ok((_, is_slashed)) => !is_slashed,
                    Err(_) => false,
                }
            };
        if let ErrorKind::DBNotFoundErr(_) = error.kind() {
            if check_validator {
                let head = unwrap_or_return!(self.chain.head());
                if !is_validator(
                    &head.epoch_id,
                    &head.last_block_hash,
                    &approval.account_id,
                    &self.runtime_adapter,
                ) && !is_validator(
                    &head.next_epoch_id,
                    &head.last_block_hash,
                    &approval.account_id,
                    &self.runtime_adapter,
                ) {
                    return;
                }
            }
            let mut entry = self
                .pending_approvals
                .cache_remove(&approval.inner)
                .unwrap_or_else(|| HashMap::new());
            entry.insert(approval.account_id.clone(), (approval.clone(), approval_type));
            self.pending_approvals.cache_set(approval.inner.clone(), entry);
        }
    }

    /// Collects block approvals. Returns false if block approval is invalid.
    ///
    /// We send the approval to doomslug given the epoch of the current tip iff:
    ///  1. We are the block producer for the target height in the tip's epoch;
    ///  2. The signature matches that of the account;
    /// If we are not the block producer, but we also don't know the previous block, we add the
    /// approval to `pending_approvals`, since it could be that the approval is from the next epoch.
    ///
    /// # Arguments
    /// * `approval` - the approval to be collected
    /// * `approval_type`  - whether the approval was just produced by us (in which case skip validation,
    ///                      only check whether we are the next block producer and store in Doomslug)
    pub fn collect_block_approval(&mut self, approval: &Approval, approval_type: ApprovalType) {
        let Approval { inner, account_id, target_height, signature } = approval;

        let parent_hash = match inner {
            ApprovalInner::Endorsement(parent_hash) => parent_hash.clone(),
            ApprovalInner::Skip(parent_height) => {
                match self.chain.get_header_by_height(*parent_height) {
                    Ok(header) => *header.hash(),
                    Err(e) => {
                        self.handle_process_approval_error(approval, approval_type, true, e);
                        return;
                    }
                }
            }
        };

        let next_block_epoch_id =
            match self.runtime_adapter.get_epoch_id_from_prev_block(&parent_hash) {
                Err(e) => {
                    self.handle_process_approval_error(approval, approval_type, true, e);
                    return;
                }
                Ok(next_epoch_id) => next_epoch_id,
            };

        if let ApprovalType::PeerApproval(_) = approval_type {
            // Check signature is correct for given validator.
            // Note that on the epoch boundary the blocks contain approvals from both the current
            // and the next epoch. Here we try to fetch the validator for the epoch of the next block,
            // if we succeed, it must use the key from that epoch, and thus we use the epoch of the
            // next block below when verifying the signature. Otherwise, if the block producer doesn't
            // exist in the epoch of the next block, we use the epoch after next to validate the
            // signature. We don't care here if the block is actually on the epochs boundary yet,
            // `Doomslug::on_approval_message` below will handle it.
            let validator_epoch_id = match self.runtime_adapter.get_validator_by_account_id(
                &next_block_epoch_id,
                &parent_hash,
                account_id,
            ) {
                Ok(_) => next_block_epoch_id.clone(),
                Err(e) if e.kind() == ErrorKind::NotAValidator => {
                    match self.runtime_adapter.get_next_epoch_id_from_prev_block(&parent_hash) {
                        Ok(next_block_next_epoch_id) => next_block_next_epoch_id,
                        Err(_) => return,
                    }
                }
                _ => return,
            };
            match self.runtime_adapter.verify_validator_signature(
                &validator_epoch_id,
                &parent_hash,
                account_id,
                Approval::get_data_for_sig(inner, *target_height).as_ref(),
                signature,
            ) {
                Ok(true) => {}
                _ => return,
            }
        }

        let is_block_producer =
            match self.runtime_adapter.get_block_producer(&next_block_epoch_id, *target_height) {
                Err(_) => false,
                Ok(target_block_producer) => {
                    Some(&target_block_producer)
                        == self.validator_signer.as_ref().map(|x| x.validator_id())
                }
            };

        if !is_block_producer {
            match self.chain.get_block_header(&parent_hash) {
                Ok(_) => {
                    // If we know the header, then either the parent_hash is the tip, and we are
                    // not the block producer for the corresponding height on top of the tip, or
                    // the parent_hash is not the tip, and then we will never build on top of it.
                    // Either way, this approval is of no use for us.
                    return;
                }
                Err(e) => {
                    self.handle_process_approval_error(approval, approval_type, false, e);
                    return;
                }
            };
        }

        let block_producer_stakes =
            match self.runtime_adapter.get_epoch_block_approvers_ordered(&parent_hash) {
                Ok(block_producer_stakes) => block_producer_stakes,
                Err(err) => {
                    error!(target: "client", "Block approval error: {}", err);
                    return;
                }
            };

        self.doomslug.on_approval_message(Instant::now(), &approval, &block_producer_stakes);
    }

    /// Forwards given transaction to upcoming validators.
    fn forward_tx(&self, epoch_id: &EpochId, tx: &SignedTransaction) -> Result<(), Error> {
        let shard_id = self.runtime_adapter.account_id_to_shard_id(&tx.transaction.signer_id);
        let head = self.chain.head()?;
        let maybe_next_epoch_id = self.get_next_epoch_id_if_at_boundary(&head)?;

        let mut validators = HashSet::new();
        for horizon in
            (2..=TX_ROUTING_HEIGHT_HORIZON).chain(vec![TX_ROUTING_HEIGHT_HORIZON * 2].into_iter())
        {
            let validator =
                self.chain.find_chunk_producer_for_forwarding(epoch_id, shard_id, horizon)?;
            validators.insert(validator);
            if let Some(next_epoch_id) = &maybe_next_epoch_id {
                let validator = self.chain.find_chunk_producer_for_forwarding(
                    next_epoch_id,
                    shard_id,
                    horizon,
                )?;
                validators.insert(validator);
            }
        }

        if let Some(account_id) = self.validator_signer.as_ref().map(|bp| bp.validator_id()) {
            validators.remove(account_id);
        }
        for validator in validators {
            debug!(target: "client",
                   "I'm {:?}, routing a transaction {:?} to {}, shard_id = {}",
                   self.validator_signer.as_ref().map(|bp| bp.validator_id()),
                   tx,
                   validator,
                   shard_id
            );

            // Send message to network to actually forward transaction.
            self.network_adapter.do_send(NetworkRequests::ForwardTx(validator, tx.clone()));
        }

        Ok(())
    }

    pub fn process_tx(
        &mut self,
        tx: SignedTransaction,
        is_forwarded: bool,
        check_only: bool,
    ) -> NetworkClientResponses {
        unwrap_or_return!(self.process_tx_internal(&tx, is_forwarded, check_only), {
            let me = self.validator_signer.as_ref().map(|vs| vs.validator_id());
            warn!(target: "client", "I'm: {:?} Dropping tx: {:?}", me, tx);
            NetworkClientResponses::NoResponse
        })
    }

    /// If we are close to epoch boundary, return next epoch id, otherwise return None.
    fn get_next_epoch_id_if_at_boundary(&self, head: &Tip) -> Result<Option<EpochId>, Error> {
        let next_epoch_started =
            self.runtime_adapter.is_next_block_epoch_start(&head.last_block_hash)?;
        if next_epoch_started {
            return Ok(None);
        }
        let next_epoch_estimated_height =
            self.runtime_adapter.get_epoch_start_height(&head.last_block_hash)?
                + self.config.epoch_length;

        let epoch_boundary_possible =
            head.height + TX_ROUTING_HEIGHT_HORIZON >= next_epoch_estimated_height;
        if epoch_boundary_possible {
            Ok(Some(self.runtime_adapter.get_next_epoch_id_from_prev_block(&head.last_block_hash)?))
        } else {
            Ok(None)
        }
    }

    /// If we're a validator in one of the next few chunks, but epoch switch could happen soon,
    /// we forward to a validator from next epoch.
    fn possibly_forward_tx_to_next_epoch(&mut self, tx: &SignedTransaction) -> Result<(), Error> {
        let head = self.chain.head()?;
        if let Some(next_epoch_id) = self.get_next_epoch_id_if_at_boundary(&head)? {
            self.forward_tx(&next_epoch_id, tx)?;
        }
        Ok(())
    }

    /// Process transaction and either add it to the mempool or return to redirect to another validator.
    fn process_tx_internal(
        &mut self,
        tx: &SignedTransaction,
        is_forwarded: bool,
        check_only: bool,
    ) -> Result<NetworkClientResponses, Error> {
        let head = self.chain.head()?;
        let me = self.validator_signer.as_ref().map(|vs| vs.validator_id());
        let shard_id = self.runtime_adapter.account_id_to_shard_id(&tx.transaction.signer_id);
        let cur_block_header = self.chain.head_header()?.clone();
        let transaction_validity_period = self.chain.transaction_validity_period;
        // here it is fine to use `cur_block_header` as it is a best effort estimate. If the transaction
        // were to be included, the block that the chunk points to will have height >= height of
        // `cur_block_header`.
        if let Err(e) = self.chain.mut_store().check_transaction_validity_period(
            &cur_block_header,
            &tx.transaction.block_hash,
            transaction_validity_period,
        ) {
            debug!(target: "client", "Invalid tx: expired or from a different fork -- {:?}", tx);
            return Ok(NetworkClientResponses::InvalidTx(e));
        }
        let gas_price = cur_block_header.gas_price();
        let epoch_id = self.runtime_adapter.get_epoch_id_from_prev_block(&head.last_block_hash)?;

        let protocol_version = self.runtime_adapter.get_epoch_protocol_version(&epoch_id)?;

        if let Some(err) = self
            .runtime_adapter
            .validate_tx(gas_price, None, &tx, true, protocol_version)
            .expect("no storage errors")
        {
            debug!(target: "client", "Invalid tx during basic validation: {:?}", err);
            return Ok(NetworkClientResponses::InvalidTx(err));
        }

        if self.runtime_adapter.cares_about_shard(me, &head.last_block_hash, shard_id, true)
            || self.runtime_adapter.will_care_about_shard(me, &head.last_block_hash, shard_id, true)
        {
            let state_root = match self.chain.get_chunk_extra(&head.last_block_hash, shard_id) {
                Ok(chunk_extra) => *chunk_extra.state_root(),
                Err(_) => {
                    // Not being able to fetch a state root most likely implies that we haven't
                    //     caught up with the next epoch yet.
                    if is_forwarded {
                        return Err(
                            ErrorKind::Other("Node has not caught up yet".to_string()).into()
                        );
                    } else {
                        self.forward_tx(&epoch_id, tx)?;
                        return Ok(NetworkClientResponses::RequestRouted);
                    }
                }
            };
            if let Some(err) = self
                .runtime_adapter
                .validate_tx(gas_price, Some(state_root), &tx, false, protocol_version)
                .expect("no storage errors")
            {
                debug!(target: "client", "Invalid tx: {:?}", err);
                Ok(NetworkClientResponses::InvalidTx(err))
            } else if check_only {
                Ok(NetworkClientResponses::ValidTx)
            } else {
                let active_validator = self.active_validator(shard_id)?;

                // If I'm not an active validator I should forward tx to next validators.
                debug!(
                    target: "client",
                    "Recording a transaction. I'm {:?}, {} is_forwarded: {}",
                    me,
                    shard_id,
                    is_forwarded
                );
                self.shards_mgr.insert_transaction(shard_id, tx.clone());

                // Active validator:
                //   possibly forward to next epoch validators
                // Not active validator:
                //   forward to current epoch validators,
                //   possibly forward to next epoch validators
                if active_validator {
                    if !is_forwarded {
                        self.possibly_forward_tx_to_next_epoch(tx)?;
                    }
                    Ok(NetworkClientResponses::ValidTx)
                } else if !is_forwarded {
                    self.forward_tx(&epoch_id, tx)?;
                    Ok(NetworkClientResponses::RequestRouted)
                } else {
                    Ok(NetworkClientResponses::NoResponse)
                }
            }
        } else if check_only {
            Ok(NetworkClientResponses::DoesNotTrackShard)
        } else {
            if is_forwarded {
                // received forwarded transaction but we are not tracking the shard
                debug!(target: "client", "Received forwarded transaction but no tracking shard {}, I'm {:?}", shard_id, me);
                return Ok(NetworkClientResponses::NoResponse);
            }
            // We are not tracking this shard, so there is no way to validate this tx. Just rerouting.

            self.forward_tx(&epoch_id, tx)?;
            Ok(NetworkClientResponses::RequestRouted)
        }
    }

    /// Determine if I am a validator in next few blocks for specified shard, assuming epoch doesn't change.
    fn active_validator(&self, shard_id: ShardId) -> Result<bool, Error> {
        let head = self.chain.head()?;
        let epoch_id = self.runtime_adapter.get_epoch_id_from_prev_block(&head.last_block_hash)?;

        let account_id = if let Some(vs) = self.validator_signer.as_ref() {
            vs.validator_id()
        } else {
            return Ok(false);
        };

        for i in 1..=TX_ROUTING_HEIGHT_HORIZON {
            let chunk_producer =
                self.runtime_adapter.get_chunk_producer(&epoch_id, head.height + i, shard_id)?;
            if &chunk_producer == account_id {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Walks through all the ongoing state syncs for future epochs and processes them
    pub fn run_catchup(
        &mut self,
        highest_height_peers: &Vec<FullPeerInfo>,
    ) -> Result<Vec<AcceptedBlock>, Error> {
        let me = &self.validator_signer.as_ref().map(|x| x.validator_id().clone());
        for (sync_hash, state_sync_info) in self.chain.store().iterate_state_sync_infos() {
            assert_eq!(sync_hash, state_sync_info.epoch_tail_hash);
            let network_adapter1 = self.network_adapter.clone();

            let state_sync_timeout = self.config.state_sync_timeout;
            let (state_sync, new_shard_sync) =
                self.catchup_state_syncs.entry(sync_hash).or_insert_with(|| {
                    (StateSync::new(network_adapter1, state_sync_timeout), HashMap::new())
                });

            debug!(
                target: "client",
                "Catchup me: {:?}: sync_hash: {:?}, sync_info: {:?}", me, sync_hash, new_shard_sync
            );

            match state_sync.run(
                me,
                sync_hash,
                new_shard_sync,
                &mut self.chain,
                &self.runtime_adapter,
                highest_height_peers,
                state_sync_info.shards.iter().map(|tuple| tuple.0).collect(),
            )? {
                StateSyncResult::Unchanged => {}
                StateSyncResult::Changed(fetch_block) => {
                    assert!(!fetch_block);
                }
                StateSyncResult::Completed => {
                    let accepted_blocks = Arc::new(RwLock::new(vec![]));
                    let blocks_missing_chunks = Arc::new(RwLock::new(vec![]));
                    let challenges = Arc::new(RwLock::new(vec![]));

                    self.chain.catchup_blocks(
                        me,
                        &sync_hash,
                        |accepted_block| {
                            accepted_blocks.write().unwrap().push(accepted_block);
                        },
                        |missing_chunks| {
                            blocks_missing_chunks.write().unwrap().push(missing_chunks)
                        },
                        |challenge| challenges.write().unwrap().push(challenge),
                    )?;

                    self.send_challenges(challenges);

                    self.shards_mgr.request_chunks(
                        blocks_missing_chunks
                            .write()
                            .unwrap()
                            .drain(..)
                            .flat_map(|missing_chunks| missing_chunks.into_iter()),
                        &self.chain.header_head()?,
                        // It is ok to pass the latest protocol version here since we are likely
                        // syncing old blocks, which means the protocol version will not change
                        // the logic. Even in the worst case where we are syncing a recent block,
                        // the only impact is the request will be sent after some delay.
                        PROTOCOL_VERSION,
                    );

                    let unwrapped_accepted_blocks =
                        accepted_blocks.write().unwrap().drain(..).collect();
                    return Ok(unwrapped_accepted_blocks);
                }
            }
        }

        Ok(vec![])
    }

    /// When accepting challenge, we verify that it's valid given signature with current validators.
    pub fn process_challenge(&mut self, _challenge: Challenge) -> Result<(), Error> {
        // TODO(2445): Enable challenges when they are working correctly.
        //        if self.challenges.contains_key(&challenge.hash) {
        //            return Ok(());
        //        }
        //        debug!(target: "client", "Received challenge: {:?}", challenge);
        //        let head = self.chain.head()?;
        //        if self.runtime_adapter.verify_validator_or_fisherman_signature(
        //            &head.epoch_id,
        //            &head.prev_block_hash,
        //            &challenge.account_id,
        //            challenge.hash.as_ref(),
        //            &challenge.signature,
        //        )? {
        //            // If challenge is not double sign, we should process it right away to invalidate the chain.
        //            match challenge.body {
        //                ChallengeBody::BlockDoubleSign(_) => {}
        //                _ => {
        //                    self.chain.process_challenge(&challenge);
        //                }
        //            }
        //            self.challenges.insert(challenge.hash, challenge);
        //        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;
    use std::path::Path;
    use std::sync::Arc;

    use cached::Cached;

    use near_chain::{ChainGenesis, RuntimeAdapter};
    use near_chain_configs::Genesis;
    use near_chunks::test_utils::ChunkForwardingTestFixture;
    use near_chunks::ProcessPartialEncodedChunkResult;
    use near_crypto::KeyType;
    use near_primitives::block::{Approval, ApprovalInner};
    use near_primitives::hash::hash;
    use near_primitives::validator_signer::InMemoryValidatorSigner;
    use near_primitives::version::PROTOCOL_VERSION;
    use near_store::test_utils::create_test_store;
    use neard::config::GenesisExt;

    use crate::test_utils::TestEnv;
    use near_network::test_utils::MockNetworkAdapter;
    use near_network::types::PartialEncodedChunkForwardMsg;
    use near_primitives::block_header::ApprovalType;
    use near_primitives::network::PeerId;
    #[cfg(feature = "protocol_feature_block_header_v3")]
    use near_primitives::sharding::ShardChunkHeaderInner;
    use near_primitives::sharding::{PartialEncodedChunk, ShardChunkHeader};
    use near_primitives::utils::MaybeValidated;

    fn create_runtimes(n: usize) -> Vec<Arc<dyn RuntimeAdapter>> {
        let genesis = Genesis::test(vec!["test0", "test1"], 1);
        (0..n)
            .map(|_| {
                Arc::new(neard::NightshadeRuntime::new(
                    Path::new("."),
                    create_test_store(),
                    &genesis,
                    vec![],
                    vec![],
                    None,
                )) as Arc<dyn RuntimeAdapter>
            })
            .collect()
    }

    #[test]
    fn test_pending_approvals() {
        let runtimes = create_runtimes(1);
        let mut env = TestEnv::new_with_runtime(ChainGenesis::test(), 1, 1, runtimes);
        let signer = InMemoryValidatorSigner::from_seed("test0", KeyType::ED25519, "test0");
        let parent_hash = hash(&[1]);
        let approval = Approval::new(parent_hash, 0, 1, &signer);
        let peer_id = PeerId::random();
        env.clients[0]
            .collect_block_approval(&approval, ApprovalType::PeerApproval(peer_id.clone()));
        let approvals =
            env.clients[0].pending_approvals.cache_remove(&ApprovalInner::Endorsement(parent_hash));
        let expected = vec![("test0".to_string(), (approval, ApprovalType::PeerApproval(peer_id)))]
            .into_iter()
            .collect::<HashMap<_, _>>();
        assert_eq!(approvals, Some(expected));
    }

    #[test]
    fn test_invalid_approvals() {
        let runtimes = create_runtimes(1);
        let network_adapter = Arc::new(MockNetworkAdapter::default());
        let mut env = TestEnv::new_with_runtime_and_network_adapter(
            ChainGenesis::test(),
            1,
            1,
            runtimes,
            vec![network_adapter.clone()],
        );
        let signer = InMemoryValidatorSigner::from_seed("random", KeyType::ED25519, "random");
        let parent_hash = hash(&[1]);
        // Approval not from a validator. Should be dropped
        let approval = Approval::new(parent_hash, 1, 3, &signer);
        let peer_id = PeerId::random();
        env.clients[0]
            .collect_block_approval(&approval, ApprovalType::PeerApproval(peer_id.clone()));
        assert_eq!(env.clients[0].pending_approvals.cache_size(), 0);
        // Approval with invalid signature. Should be dropped
        let signer = InMemoryValidatorSigner::from_seed("test0", KeyType::ED25519, "random");
        let genesis_hash = *env.clients[0].chain.genesis().hash();
        let approval = Approval::new(genesis_hash, 0, 1, &signer);
        env.clients[0].collect_block_approval(&approval, ApprovalType::PeerApproval(peer_id));
        assert_eq!(env.clients[0].pending_approvals.cache_size(), 0);
    }

    #[test]
    fn test_process_partial_encoded_chunk_with_missing_block() {
        let runtimes = create_runtimes(1);
        let mut env = TestEnv::new_with_runtime(ChainGenesis::test(), 1, 1, runtimes);
        let client = &mut env.clients[0];
        let chunk_producer = ChunkForwardingTestFixture::default();
        let mut mock_chunk = chunk_producer.make_partial_encoded_chunk(&[0]);
        // change the prev_block to some unknown block
        match &mut mock_chunk.header {
            ShardChunkHeader::V1(ref mut header) => {
                header.inner.prev_block_hash = hash(b"some_prev_block");
                header.init();
            }
            ShardChunkHeader::V2(ref mut header) => {
                header.inner.prev_block_hash = hash(b"some_prev_block");
                header.init();
            }
            #[cfg(feature = "protocol_feature_block_header_v3")]
            ShardChunkHeader::V3(header) => {
                match &mut header.inner {
                    ShardChunkHeaderInner::V1(inner) => {
                        inner.prev_block_hash = hash(b"some_prev_block")
                    }
                    ShardChunkHeaderInner::V2(inner) => {
                        inner.prev_block_hash = hash(b"some_prev_block")
                    }
                }
                header.init();
            }
        }

        let mock_forward = PartialEncodedChunkForwardMsg::from_header_and_parts(
            &mock_chunk.header,
            mock_chunk.parts.clone(),
        );

        // process_partial_encoded_chunk should return Ok(NeedBlock) if the chunk is
        // based on a missing block.
        let result = client.shards_mgr.process_partial_encoded_chunk(
            MaybeValidated::NotValidated(&mock_chunk),
            client.chain.mut_store(),
            &mut client.rs,
            PROTOCOL_VERSION,
        );
        assert!(matches!(result, Ok(ProcessPartialEncodedChunkResult::NeedBlock)));

        // Client::process_partial_encoded_chunk should not return an error
        // if the chunk is based on a missing block.
        let result = client.process_partial_encoded_chunk(MaybeValidated::NotValidated(
            PartialEncodedChunk::V2(mock_chunk),
        ));
        match result {
            Ok(accepted_blocks) => assert!(accepted_blocks.is_empty()),
            Err(e) => panic!("Client::process_partial_encoded_chunk failed with {:?}", e),
        }

        // process_partial_encoded_chunk_forward should return UnknownChunk if it is based on a
        // a missing block.
        let result = client.process_partial_encoded_chunk_forward(mock_forward);
        assert!(matches!(
            result,
            Err(near_client_primitives::types::Error::Chunk(near_chunks::Error::UnknownChunk))
        ));
    }
}
