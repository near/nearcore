//! Client is responsible for tracking the chain, chunks, and producing them when needed.
//! This client works completely syncronously and must be operated by some async actor outside.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Instant;

use cached::{Cached, SizedCache};
use chrono::Utc;
use log::{debug, error, info, warn};
use reed_solomon_erasure::galois_8::ReedSolomon;

use near_chain::chain::TX_ROUTING_HEIGHT_HORIZON;
use near_chain::test_utils::format_hash;
use near_chain::types::{AcceptedBlock, LatestKnown, ReceiptResponse};
use near_chain::{
    BlockStatus, Chain, ChainGenesis, ChainStoreAccess, Doomslug, DoomslugThresholdMode,
    Provenance, RuntimeAdapter, Tip,
};
use near_chunks::{ProcessPartialEncodedChunkResult, ShardsManager};
use near_network::{FullPeerInfo, NetworkAdapter, NetworkClientResponses, NetworkRequests};
use near_primitives::block::{Approval, ApprovalMessage, Block, BlockHeader};
use near_primitives::challenge::{Challenge, ChallengeBody};
use near_primitives::hash::CryptoHash;
use near_primitives::merkle::{merklize, MerklePath};
use near_primitives::receipt::Receipt;
use near_primitives::sharding::{EncodedShardChunk, PartialEncodedChunk, ShardChunkHeader};
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{AccountId, BlockHeight, ChunkExtra, EpochId, ShardId};
use near_primitives::unwrap_or_return;
use near_primitives::utils::to_timestamp;

use crate::metrics;
use crate::sync::{BlockSync, HeaderSync, StateSync, StateSyncResult};
use crate::types::{Error, ShardSyncDownload};
use crate::{BlockProducer, ClientConfig, SyncStatus};

pub struct Client {
    pub config: ClientConfig,
    pub sync_status: SyncStatus,
    pub chain: Chain,
    pub doomslug: Doomslug,
    pub runtime_adapter: Arc<dyn RuntimeAdapter>,
    pub shards_mgr: ShardsManager,
    /// Network adapter.
    network_adapter: Arc<dyn NetworkAdapter>,
    /// Signer for block producer (if present).
    pub block_producer: Option<BlockProducer>,
    /// Approvals for which we do not have the block yet
    pending_approvals: SizedCache<CryptoHash, HashMap<AccountId, Approval>>,
    /// A mapping from a block for which a state sync is underway for the next epoch, and the object
    /// storing the current status of the state sync
    pub catchup_state_syncs: HashMap<CryptoHash, (StateSync, HashMap<u64, ShardSyncDownload>)>,
    /// Keeps track of syncing headers.
    pub header_sync: HeaderSync,
    /// Keeps track of syncing block.
    pub block_sync: BlockSync,
    /// Keeps track of syncing state.
    pub state_sync: StateSync,
    /// List of currently accumulated challenges.
    pub challenges: HashMap<CryptoHash, Challenge>,
    /// A ReedSolomon instance to reconstruct shard.
    pub rs: ReedSolomon,
}

impl Client {
    pub fn new(
        config: ClientConfig,
        chain_genesis: ChainGenesis,
        runtime_adapter: Arc<dyn RuntimeAdapter>,
        network_adapter: Arc<dyn NetworkAdapter>,
        block_producer: Option<BlockProducer>,
        enable_doomslug: bool,
    ) -> Result<Self, Error> {
        let doomslug_threshold_mode = if enable_doomslug {
            DoomslugThresholdMode::HalfStake
        } else {
            DoomslugThresholdMode::NoApprovals
        };
        let chain = Chain::new(runtime_adapter.clone(), &chain_genesis, doomslug_threshold_mode)?;
        let shards_mgr = ShardsManager::new(
            block_producer.as_ref().map(|x| x.account_id.clone()),
            runtime_adapter.clone(),
            network_adapter.clone(),
        );
        let sync_status = SyncStatus::AwaitingPeers;
        let header_sync = HeaderSync::new(
            network_adapter.clone(),
            config.header_sync_initial_timeout,
            config.header_sync_progress_timeout,
            config.header_sync_stall_ban_timeout,
            config.header_sync_expected_height_per_second,
        );
        let block_sync = BlockSync::new(network_adapter.clone(), config.block_fetch_horizon);
        let state_sync = StateSync::new(network_adapter.clone());
        let num_block_producer_seats = config.num_block_producer_seats as usize;
        let data_parts = runtime_adapter.num_data_parts();
        let parity_parts = runtime_adapter.num_total_parts() - data_parts;

        let doomslug = Doomslug::new(
            block_producer.as_ref().map(|x| x.account_id.clone()),
            chain.store().largest_skipped_height()?,
            chain.store().largest_endorsed_height()?,
            config.min_block_production_delay,
            config.min_block_production_delay / 10,
            config.max_block_production_delay,
            block_producer.as_ref().map(|x| x.signer.clone()),
            doomslug_threshold_mode,
        );

        Ok(Self {
            config,
            sync_status,
            chain,
            doomslug,
            runtime_adapter,
            shards_mgr,
            network_adapter,
            block_producer,
            pending_approvals: SizedCache::with_size(num_block_producer_seats),
            catchup_state_syncs: HashMap::new(),
            header_sync,
            block_sync,
            state_sync,
            challenges: Default::default(),
            rs: ReedSolomon::new(data_parts, parity_parts).unwrap(),
        })
    }

    pub fn remove_transactions_for_block(&mut self, me: AccountId, block: &Block) {
        for (shard_id, chunk_header) in block.chunks.iter().enumerate() {
            let shard_id = shard_id as ShardId;
            if block.header.inner_lite.height == chunk_header.height_included {
                if self.shards_mgr.cares_about_shard_this_or_next_epoch(
                    Some(&me),
                    &block.header.prev_hash,
                    shard_id,
                    true,
                ) {
                    self.shards_mgr.remove_transactions(
                        shard_id,
                        // By now the chunk must be in store, otherwise the block would have been orphaned
                        &self.chain.get_chunk(&chunk_header.chunk_hash()).unwrap().transactions,
                    );
                }
            }
        }
        for challenge in block.challenges.iter() {
            self.challenges.remove(&challenge.hash);
        }
    }

    pub fn reintroduce_transactions_for_block(&mut self, me: AccountId, block: &Block) {
        for (shard_id, chunk_header) in block.chunks.iter().enumerate() {
            let shard_id = shard_id as ShardId;
            if block.header.inner_lite.height == chunk_header.height_included {
                if self.shards_mgr.cares_about_shard_this_or_next_epoch(
                    Some(&me),
                    &block.header.prev_hash,
                    shard_id,
                    false,
                ) {
                    self.shards_mgr.reintroduce_transactions(
                        shard_id,
                        // By now the chunk must be in store, otherwise the block would have been orphaned
                        &self.chain.get_chunk(&chunk_header.chunk_hash()).unwrap().transactions,
                    );
                }
            }
        }
        for challenge in block.challenges.iter() {
            self.challenges.insert(challenge.hash, challenge.clone());
        }
    }

    /// Produce block if we are block producer for given `next_height` block height.
    /// Either returns produced block (not applied) or error.
    pub fn produce_block(&mut self, next_height: BlockHeight) -> Result<Option<Block>, Error> {
        // Check that this block height is not known yet.
        if next_height <= self.chain.mut_store().get_latest_known()?.height {
            return Ok(None);
        }
        let block_producer = self
            .block_producer
            .as_ref()
            .ok_or_else(|| Error::BlockProducer("Called without block producer info.".to_string()))?
            .clone();
        let head = self.chain.head()?;
        assert_eq!(
            head.epoch_id,
            self.runtime_adapter.get_epoch_id_from_prev_block(&head.prev_block_hash).unwrap()
        );

        // Check that we are were called at the block that we are producer for.
        let next_block_proposer = self.runtime_adapter.get_block_producer(
            &self.runtime_adapter.get_epoch_id_from_prev_block(&head.last_block_hash).unwrap(),
            next_height,
        )?;
        if block_producer.account_id != next_block_proposer {
            info!(target: "client", "Produce block: chain at {}, not block producer for next block.", next_height);
            return Ok(None);
        }
        let prev = self.chain.get_block_header(&head.last_block_hash)?.clone();
        let prev_hash = head.last_block_hash;
        let prev_prev_hash = prev.prev_hash;
        let prev_epoch_id = prev.inner_lite.epoch_id.clone();
        let prev_next_bp_hash = prev.inner_lite.next_bp_hash;
        let prev_last_ds_final_block = prev.inner_rest.last_ds_final_block;

        // Check and update the doomslug tip here. This guarantees that our endorsement will be in the
        // doomslug witness. Have to do it before checking the ability to produce a block.
        let _ = self.check_and_update_doomslug_tip()?;

        debug!(target: "client", "{:?} Producing block at height {}, parent {} @ {}", block_producer.account_id, next_height, prev.inner_lite.height, format_hash(head.last_block_hash));

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
                return Ok(None);
            }
        }

        let new_chunks = self.shards_mgr.prepare_chunks(&prev_hash);
        // If we are producing empty blocks and there are no transactions.
        if !self.config.produce_empty_blocks && new_chunks.is_empty() {
            debug!(target: "client", "Empty blocks, skipping block production");
            return Ok(None);
        }

        let approvals = self.doomslug.remove_witness(&prev_hash, next_height);

        // At this point, the previous epoch hash must be available
        let epoch_id = self
            .runtime_adapter
            .get_epoch_id_from_prev_block(&head.last_block_hash)
            .expect("Epoch hash should exist at this point");

        let next_epoch_id = self
            .runtime_adapter
            .get_next_epoch_id_from_prev_block(&head.last_block_hash)
            .expect("Epoch hash should exist at this point");

        let quorums = Chain::compute_quorums(
            prev_hash,
            epoch_id.clone(),
            next_height,
            approvals.clone(),
            &*self.runtime_adapter,
            self.chain.mut_store(),
            true,
        )?
        .clone();

        let account_id_to_stake = self
            .runtime_adapter
            .get_epoch_block_producers_ordered(&prev_epoch_id, &prev_hash)?
            .iter()
            .map(|x| (x.0.account_id.clone(), x.0.stake))
            .collect();
        let is_prev_block_ds_final =
            Doomslug::is_approved_block_ds_final(&approvals, &account_id_to_stake);

        let last_ds_final_block =
            if is_prev_block_ds_final { prev_hash } else { prev_last_ds_final_block };

        let score = if quorums.last_quorum_pre_vote == CryptoHash::default() {
            0.into()
        } else {
            self.chain.get_block_header(&quorums.last_quorum_pre_vote)?.inner_lite.height.into()
        };

        let gas_price_adjustment_rate = self.chain.block_economics_config.gas_price_adjustment_rate;
        let min_gas_price = self.chain.block_economics_config.min_gas_price;

        let next_bp_hash = if prev_epoch_id != epoch_id {
            Chain::compute_bp_hash(&*self.runtime_adapter, next_epoch_id.clone(), &prev_hash)?
        } else {
            prev_next_bp_hash
        };

        // Get block extra from previous block.
        let prev_block_extra = self.chain.get_block_extra(&head.last_block_hash)?.clone();
        let prev_block = self.chain.get_block(&head.last_block_hash)?;
        let mut chunks = prev_block.chunks.clone();

        assert!(score >= prev_block.header.inner_rest.score);

        // Collect new chunks.
        for (shard_id, mut chunk_header) in new_chunks {
            chunk_header.height_included = next_height;
            chunks[shard_id as usize] = chunk_header;
        }

        let prev_header = &prev_block.header;

        let inflation = if self.runtime_adapter.is_next_block_epoch_start(&head.last_block_hash)? {
            let next_epoch_id =
                self.runtime_adapter.get_next_epoch_id_from_prev_block(&head.last_block_hash)?;
            Some(self.runtime_adapter.get_epoch_inflation(&next_epoch_id)?)
        } else {
            None
        };

        // Get all the current challenges.
        let challenges = self.challenges.drain().map(|(_, challenge)| challenge).collect();

        let block = Block::produce(
            &prev_header,
            next_height,
            chunks,
            epoch_id,
            next_epoch_id,
            approvals,
            gas_price_adjustment_rate,
            min_gas_price,
            inflation,
            prev_block_extra.challenges_result,
            challenges,
            &*block_producer.signer,
            score,
            quorums.last_quorum_pre_vote,
            quorums.last_quorum_pre_commit,
            last_ds_final_block,
            next_bp_hash,
        );

        // Update latest known even before returning block out, to prevent race conditions.
        self.chain.mut_store().save_latest_known(LatestKnown {
            height: next_height,
            seen: to_timestamp(Utc::now()),
        })?;

        Ok(Some(block))
    }

    pub fn produce_chunk(
        &mut self,
        prev_block_hash: CryptoHash,
        epoch_id: &EpochId,
        last_header: ShardChunkHeader,
        next_height: BlockHeight,
        prev_block_timestamp: u64,
        shard_id: ShardId,
    ) -> Result<Option<(EncodedShardChunk, Vec<MerklePath>, Vec<Receipt>)>, Error> {
        let block_producer = self
            .block_producer
            .as_ref()
            .ok_or_else(|| Error::ChunkProducer("Called without block producer info.".to_string()))?
            .clone();

        let chunk_proposer =
            self.runtime_adapter.get_chunk_producer(epoch_id, next_height, shard_id).unwrap();
        if block_producer.account_id != chunk_proposer {
            debug!(target: "client", "Not producing chunk for shard {}: chain at {}, not block producer for next block. Me: {}, proposer: {}", shard_id, next_height, block_producer.account_id, chunk_proposer);
            return Ok(None);
        }

        if self.runtime_adapter.is_next_block_epoch_start(&prev_block_hash)? {
            let prev_prev_hash = self.chain.get_block_header(&prev_block_hash)?.prev_hash;
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
            block_producer.account_id
        );

        let chunk_extra = self
            .chain
            .get_latest_chunk_extra(shard_id)
            .map_err(|err| Error::ChunkProducer(format!("No chunk extra available: {}", err)))?
            .clone();

        let prev_block_header = self.chain.get_block_header(&prev_block_hash)?.clone();

        let transactions = self.prepare_transactions(
            next_height,
            prev_block_timestamp,
            shard_id,
            &chunk_extra,
            &prev_block_header,
        );

        let num_filtered_transactions = transactions.len();

        let (tx_root, _) = merklize(&transactions);
        debug!(
            "Creating a chunk with {} filtered transactions for shard {}",
            num_filtered_transactions, shard_id
        );

        let ReceiptResponse(_, outgoing_receipts) = self.chain.get_outgoing_receipts_for_shard(
            prev_block_hash,
            shard_id,
            last_header.height_included,
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

        let (encoded_chunk, merkle_paths) = self.shards_mgr.create_encoded_shard_chunk(
            prev_block_hash,
            chunk_extra.state_root,
            chunk_extra.outcome_root,
            next_height,
            shard_id,
            chunk_extra.gas_used,
            chunk_extra.gas_limit,
            chunk_extra.rent_paid,
            chunk_extra.validator_reward,
            chunk_extra.balance_burnt,
            chunk_extra.validator_proposals.clone(),
            transactions,
            &outgoing_receipts,
            outgoing_receipts_root,
            tx_root,
            &*block_producer.signer,
            &self.rs,
        )?;

        debug!(
            target: "client",
            "Produced chunk at height {} for shard {} with {} txs and {} receipts, I'm {}, chunk_hash: {}",
            next_height,
            shard_id,
            num_filtered_transactions,
            outgoing_receipts.len(),
            block_producer.account_id,
            encoded_chunk.chunk_hash().0,
        );

        near_metrics::inc_counter(&metrics::BLOCK_PRODUCED_TOTAL);
        Ok(Some((encoded_chunk, merkle_paths, outgoing_receipts)))
    }

    /// Prepares an ordered list of valid transactions from the pool up the limits.
    fn prepare_transactions(
        &mut self,
        next_height: BlockHeight,
        prev_block_timestamp: u64,
        shard_id: ShardId,
        chunk_extra: &ChunkExtra,
        prev_block_header: &BlockHeader,
    ) -> Vec<SignedTransaction> {
        let Self { chain, shards_mgr, config, runtime_adapter, .. } = self;
        let transactions = if let Some(mut iter) = shards_mgr.get_pool_iterator(shard_id) {
            let transaction_validity_period = chain.transaction_validity_period;
            runtime_adapter
                .prepare_transactions(
                    next_height,
                    prev_block_timestamp,
                    prev_block_header.inner_rest.gas_price,
                    chunk_extra.gas_limit,
                    chunk_extra.state_root.clone(),
                    config.block_expected_weight as usize,
                    &mut iter,
                    &mut |tx: &SignedTransaction| -> bool {
                        chain
                            .mut_store()
                            .check_blocks_on_same_chain(
                                &prev_block_header,
                                &tx.transaction.block_hash,
                                transaction_validity_period,
                            )
                            .is_ok()
                    },
                )
                .expect("no StorageError please")
        } else {
            vec![]
        };
        // Reintroduce valid transactions back to the pool. They will be removed when the chunk is
        // included into the block.
        shards_mgr.reintroduce_transactions(shard_id, &transactions);
        transactions
    }

    pub fn send_challenges(&mut self, challenges: Arc<RwLock<Vec<ChallengeBody>>>) {
        if let Some(block_producer) = self.block_producer.as_ref() {
            for body in challenges.write().unwrap().drain(..) {
                let challenge = Challenge::produce(
                    body,
                    block_producer.account_id.clone(),
                    &*block_producer.signer,
                );
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
        // TODO: replace to channels or cross beams here? we don't have multi-threading here so it's mostly to get around borrow checker.
        let accepted_blocks = Arc::new(RwLock::new(vec![]));
        let blocks_missing_chunks = Arc::new(RwLock::new(vec![]));
        let challenges = Arc::new(RwLock::new(vec![]));

        let result = {
            let me = self
                .block_producer
                .as_ref()
                .map(|block_producer| block_producer.account_id.clone());
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
        if let Some(block_producer) = self.block_producer.as_ref() {
            match &result {
                Err(e) => match e.kind() {
                    near_chain::ErrorKind::InvalidChunkProofs(chunk_proofs) => {
                        self.network_adapter.do_send(NetworkRequests::Challenge(
                            Challenge::produce(
                                ChallengeBody::ChunkProofs(chunk_proofs),
                                block_producer.account_id.clone(),
                                &*block_producer.signer,
                            ),
                        ));
                    }
                    near_chain::ErrorKind::InvalidChunkState(chunk_state) => {
                        self.network_adapter.do_send(NetworkRequests::Challenge(
                            Challenge::produce(
                                ChallengeBody::ChunkState(chunk_state),
                                block_producer.account_id.clone(),
                                &*block_producer.signer,
                            ),
                        ));
                    }
                    _ => {}
                },
                _ => {}
            }
        }

        for missing_chunks in blocks_missing_chunks.write().unwrap().drain(..) {
            self.shards_mgr.request_chunks(missing_chunks).unwrap();
        }
        let unwrapped_accepted_blocks = accepted_blocks.write().unwrap().drain(..).collect();
        (unwrapped_accepted_blocks, result)
    }

    pub fn process_partial_encoded_chunk(
        &mut self,
        partial_encoded_chunk: PartialEncodedChunk,
    ) -> Result<Vec<AcceptedBlock>, Error> {
        let process_result = self.shards_mgr.process_partial_encoded_chunk(
            partial_encoded_chunk.clone(),
            self.chain.mut_store(),
            &self.rs,
        )?;

        match process_result {
            ProcessPartialEncodedChunkResult::Known => Ok(vec![]),
            ProcessPartialEncodedChunkResult::HaveAllPartsAndReceipts(prev_block_hash) => {
                Ok(self.process_blocks_with_missing_chunks(prev_block_hash))
            }
            ProcessPartialEncodedChunkResult::NeedMorePartsOrReceipts(chunk_header) => {
                self.shards_mgr.request_chunks(vec![chunk_header]).unwrap();
                Ok(vec![])
            }
            ProcessPartialEncodedChunkResult::NeedBlock => {
                self.shards_mgr
                    .store_partial_encoded_chunk(self.chain.head_header()?, partial_encoded_chunk);
                Ok(vec![])
            }
        }
    }

    pub fn process_block_header(&mut self, header: &BlockHeader) -> Result<(), near_chain::Error> {
        let challenges = Arc::new(RwLock::new(vec![]));
        self.chain.process_block_header(header, |challenge| {
            challenges.write().unwrap().push(challenge)
        })?;
        self.send_challenges(challenges);
        Ok(())
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
    /// Returns true if a block header announcement was sent
    pub fn check_and_update_doomslug_tip(&mut self) -> Result<bool, Error> {
        let tip = self.chain.head()?;

        if tip.last_block_hash != self.doomslug.get_tip().0 {
            // We need to update the doomslug tip
            let last_ds_final_hash =
                self.chain.get_block_header(&tip.last_block_hash)?.inner_rest.last_ds_final_block;
            let last_ds_final_height = if last_ds_final_hash == CryptoHash::default() {
                0
            } else {
                self.chain.get_block_header(&last_ds_final_hash)?.inner_lite.height
            };

            let may_be_approval = self.doomslug.set_tip(
                Instant::now(),
                tip.last_block_hash,
                self.chain.get_my_approval_reference_hash(tip.last_block_hash),
                tip.height,
                last_ds_final_height,
            );

            if let Some(approval) = may_be_approval {
                self.chain.process_approval(
                    &self.block_producer.as_ref().map(|x| x.account_id.clone()),
                    &approval,
                )?;

                self.collect_block_approval(&approval, true);

                // Important to update the last endorsed height before sending the approval
                let mut chain_store_update = self.chain.mut_store().store_update();
                chain_store_update
                    .save_largest_endorsed_height(&self.doomslug.get_largest_endorsed_height());
                chain_store_update.commit()?;

                self.send_approval(approval)?;

                return Ok(true);
            }
        }

        Ok(false)
    }

    pub fn send_approval(&mut self, approval: Approval) -> Result<(), Error> {
        let parent_hash = approval.parent_hash;
        let next_epoch_id = self.runtime_adapter.get_epoch_id_from_prev_block(&parent_hash)?;
        let next_block_producer =
            self.runtime_adapter.get_block_producer(&next_epoch_id, approval.target_height)?;
        if Some(&next_block_producer) == self.block_producer.as_ref().map(|x| &x.account_id) {
            self.collect_block_approval(&approval, false);
        } else {
            let approval_message = ApprovalMessage::new(approval, next_block_producer);
            self.network_adapter.do_send(NetworkRequests::BlockHeaderAnnounce {
                header: self.chain.get_block_header(&parent_hash)?.clone(),
                approval_message: Some(approval_message),
            });
        }

        Ok(())
    }

    /// Gets called when block got accepted.
    /// Send updates over network, update tx pool and notify ourselves if it's time to produce next block.
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

        // If we produced the block, then it should have already been broadcasted.
        // If received the block from another node then broadcast "header first" to minimise network traffic.
        if provenance == Provenance::NONE {
            let approvals = self.pending_approvals.cache_remove(&block_hash);
            if let Some(approvals) = approvals {
                for (_account_id, approval) in approvals {
                    self.collect_block_approval(&approval, false);
                }
            }

            if !self.check_and_update_doomslug_tip().unwrap_or(false) {
                self.network_adapter.do_send(NetworkRequests::BlockHeaderAnnounce {
                    header: block.header.clone(),
                    approval_message: None,
                });
            }
        }

        if status.is_new_head() {
            self.shards_mgr.update_largest_seen_height(block.header.inner_lite.height);
            if let Err(err) = self.chain.clear_old_data() {
                error!(target: "client", "Can't clear old data, {:?}", err);
            };
        }

        if let Some(bp) = self.block_producer.clone() {
            // Reconcile the txpool against the new block *after* we have broadcast it too our peers.
            // This may be slow and we do not want to delay block propagation.
            match status {
                BlockStatus::Next => {
                    // If this block immediately follows the current tip, remove transactions
                    //    from the txpool
                    self.remove_transactions_for_block(bp.account_id.clone(), &block);
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
                    let mut remove_head = block.header.clone();
                    assert_ne!(remove_head.hash(), reintroduce_head.hash());

                    let mut to_remove = vec![];
                    let mut to_reintroduce = vec![];

                    while remove_head.hash() != reintroduce_head.hash() {
                        while remove_head.inner_lite.height > reintroduce_head.inner_lite.height {
                            to_remove.push(remove_head.hash());
                            remove_head = self
                                .chain
                                .get_block_header(&remove_head.prev_hash)
                                .unwrap()
                                .clone();
                        }
                        while reintroduce_head.inner_lite.height > remove_head.inner_lite.height
                            || reintroduce_head.inner_lite.height == remove_head.inner_lite.height
                                && reintroduce_head.hash() != remove_head.hash()
                        {
                            to_reintroduce.push(reintroduce_head.hash());
                            reintroduce_head = self
                                .chain
                                .get_block_header(&reintroduce_head.prev_hash)
                                .unwrap()
                                .clone();
                        }
                    }

                    for to_reintroduce_hash in to_reintroduce {
                        if let Ok(block) = self.chain.get_block(&to_reintroduce_hash) {
                            let block = block.clone();
                            self.reintroduce_transactions_for_block(bp.account_id.clone(), &block);
                        }
                    }

                    for to_remove_hash in to_remove {
                        if let Ok(block) = self.chain.get_block(&to_remove_hash) {
                            let block = block.clone();
                            self.remove_transactions_for_block(bp.account_id.clone(), &block);
                        }
                    }
                }
            };

            if provenance != Provenance::SYNC && !self.sync_status.is_syncing() {
                // Produce new chunks
                for shard_id in 0..self.runtime_adapter.num_shards() {
                    let epoch_id = self
                        .runtime_adapter
                        .get_epoch_id_from_prev_block(&block.header.hash())
                        .unwrap();
                    let chunk_proposer = self
                        .runtime_adapter
                        .get_chunk_producer(&epoch_id, block.header.inner_lite.height + 1, shard_id)
                        .unwrap();

                    if chunk_proposer == *bp.account_id {
                        match self.produce_chunk(
                            block.hash(),
                            &epoch_id,
                            block.chunks[shard_id as usize].clone(),
                            block.header.inner_lite.height + 1,
                            block.header.inner_lite.timestamp,
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
        let next_height = block.header.inner_lite.height + 1;
        let mut partial_encoded_chunks =
            self.shards_mgr.get_stored_partial_encoded_chunks(next_height);
        for (_shard_id, partial_encoded_chunk) in partial_encoded_chunks.drain() {
            if let Ok(accepted_blocks) = self.process_partial_encoded_chunk(partial_encoded_chunk) {
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
        last_accepted_block_hash: CryptoHash,
    ) -> Vec<AcceptedBlock> {
        let accepted_blocks = Arc::new(RwLock::new(vec![]));
        let blocks_missing_chunks = Arc::new(RwLock::new(vec![]));
        let challenges = Arc::new(RwLock::new(vec![]));
        let me =
            self.block_producer.as_ref().map(|block_producer| block_producer.account_id.clone());
        self.chain.check_blocks_with_missing_chunks(&me, last_accepted_block_hash, |accepted_block| {
            debug!(target: "client", "Block {} was missing chunks but now is ready to be processed", accepted_block.hash);
            accepted_blocks.write().unwrap().push(accepted_block);
        }, |missing_chunks| blocks_missing_chunks.write().unwrap().push(missing_chunks), |challenge| challenges.write().unwrap().push(challenge));
        self.send_challenges(challenges);
        for missing_chunks in blocks_missing_chunks.write().unwrap().drain(..) {
            self.shards_mgr.request_chunks(missing_chunks).unwrap();
        }
        let unwrapped_accepted_blocks = accepted_blocks.write().unwrap().drain(..).collect();
        unwrapped_accepted_blocks
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
    /// * `is_ours`  - whether the approval was just produced by us (in which case skip validation,
    ///                only check whether we are the next block producer and store in Doomslug)
    pub fn collect_block_approval(&mut self, approval: &Approval, is_ours: bool) {
        let Approval {
            parent_hash,
            reference_hash,
            account_id,
            target_height,
            is_endorsement,
            signature,
        } = approval;

        let head = match self.chain.head() {
            Ok(head) => head,
            Err(_) => return,
        };

        if !is_ours {
            // Check signature is correct for given validator.
            match self.runtime_adapter.verify_validator_signature(
                &head.epoch_id,
                &head.last_block_hash,
                account_id,
                Approval::get_data_for_sig(
                    parent_hash,
                    reference_hash,
                    *target_height,
                    *is_endorsement,
                )
                .as_ref(),
                signature,
            ) {
                Ok(true) => {}
                _ => return,
            }

            if let Err(e) = self.chain.verify_approval_conditions(&approval) {
                debug!(target: "client", "Rejecting approval {:?}: {:?}", approval, e);
                return;
            }
        }

        let is_block_producer =
            match self.runtime_adapter.get_epoch_id_from_prev_block(&approval.parent_hash) {
                Err(_) => false,
                Ok(next_epoch_id) => {
                    match self.runtime_adapter.get_block_producer(&next_epoch_id, *target_height) {
                        Err(_) => false,
                        Ok(target_block_producer) => {
                            Some(&target_block_producer)
                                == self.block_producer.as_ref().map(|x| &x.account_id)
                        }
                    }
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
                    if e.is_bad_data() {
                        return;
                    }
                    let mut entry = self
                        .pending_approvals
                        .cache_remove(parent_hash)
                        .unwrap_or_else(|| HashMap::new());
                    entry.insert(account_id.clone(), approval.clone());
                    self.pending_approvals.cache_set(*parent_hash, entry);
                    return;
                }
            };
        }

        let block_producer_stakes = match self
            .runtime_adapter
            .get_epoch_block_producers_ordered(&head.epoch_id, &parent_hash)
        {
            Ok(block_producer_stakes) => block_producer_stakes,
            Err(err) => {
                error!(target: "client", "Block approval error: {}", err);
                return;
            }
        };

        let block_producer_stakes =
            block_producer_stakes.into_iter().map(|x| x.0).collect::<Vec<_>>();

        self.doomslug.on_approval_message(Instant::now(), &approval, &block_producer_stakes);
    }

    /// Forwards given transaction to upcoming validators.
    fn forward_tx(&self, tx: SignedTransaction) -> NetworkClientResponses {
        let shard_id = self.runtime_adapter.account_id_to_shard_id(&tx.transaction.signer_id);
        let me = self.block_producer.as_ref().map(|bp| &bp.account_id);
        let validator = unwrap_or_return!(self.chain.find_validator_for_forwarding(shard_id), {
            warn!(target: "client", "Me: {:?} Dropping tx: {:?}", me, tx);
            NetworkClientResponses::NoResponse
        });

        debug!(target: "client",
               "I'm {:?}, routing a transaction to {}, shard_id = {}",
               self.block_producer.as_ref().map(|bp| bp.account_id.clone()),
               validator,
               shard_id
        );

        // Send message to network to actually forward transaction.
        self.network_adapter.do_send(NetworkRequests::ForwardTx(validator, tx));

        NetworkClientResponses::RequestRouted
    }

    /// Process transaction and either add it to the mempool or return to redirect to another validator.
    pub fn process_tx(&mut self, tx: SignedTransaction) -> NetworkClientResponses {
        let head = unwrap_or_return!(self.chain.head(), NetworkClientResponses::NoResponse);
        let me = self.block_producer.as_ref().map(|bp| &bp.account_id);
        let shard_id = self.runtime_adapter.account_id_to_shard_id(&tx.transaction.signer_id);
        let cur_block_header = unwrap_or_return!(
            self.chain.get_block_header(&head.last_block_hash),
            NetworkClientResponses::NoResponse
        )
        .clone();
        let transaction_validity_period = self.chain.transaction_validity_period;
        if let Err(e) = self.chain.mut_store().check_blocks_on_same_chain(
            &cur_block_header,
            &tx.transaction.block_hash,
            transaction_validity_period,
        ) {
            debug!(target: "client", "Invalid tx: expired or from a different fork -- {:?}", tx);
            return NetworkClientResponses::InvalidTx(e);
        }

        if self.runtime_adapter.cares_about_shard(me, &head.last_block_hash, shard_id, true)
            || self.runtime_adapter.will_care_about_shard(me, &head.last_block_hash, shard_id, true)
        {
            let gas_price = unwrap_or_return!(
                self.chain.get_block_header(&head.last_block_hash),
                NetworkClientResponses::NoResponse
            )
            .inner_rest
            .gas_price;
            let state_root = match self.chain.get_chunk_extra(&head.last_block_hash, shard_id) {
                Ok(chunk_extra) => chunk_extra.state_root.clone(),
                Err(_) => {
                    // Not being able to fetch a state root most likely implies that we haven't
                    //     caught up with the next epoch yet.
                    return self.forward_tx(tx);
                }
            };
            if let Some(err) = self
                .runtime_adapter
                .validate_tx(
                    head.height + 1,
                    cur_block_header.inner_lite.timestamp,
                    gas_price,
                    state_root,
                    &tx,
                )
                .expect("no storage errors")
            {
                debug!(target: "client", "Invalid tx: {:?}", err);
                NetworkClientResponses::InvalidTx(err)
            } else {
                let active_validator = unwrap_or_return!(self.active_validator(shard_id), {
                    warn!(target: "client", "I'm: {:?} Dropping tx: {:?}", me, tx);
                    NetworkClientResponses::NoResponse
                });

                // If I'm not an active validator I should forward tx to next validators.
                debug!(
                    target: "client",
                    "Recording a transaction. I'm {:?}, {}",
                    me,
                    shard_id
                );
                self.shards_mgr.insert_transaction(shard_id, tx.clone());

                if active_validator {
                    NetworkClientResponses::ValidTx
                } else {
                    self.forward_tx(tx)
                }
            }
        } else {
            // We are not tracking this shard, so there is no way to validate this tx. Just rerouting.
            self.forward_tx(tx)
        }
    }

    /// Determine if I am a validator in next few blocks for specified shard.
    fn active_validator(&self, shard_id: ShardId) -> Result<bool, Error> {
        let head = self.chain.head()?;

        let account_id = if let Some(bp) = self.block_producer.as_ref() {
            &bp.account_id
        } else {
            return Ok(false);
        };

        for i in 1..=TX_ROUTING_HEIGHT_HORIZON {
            let chunk_producer = self.runtime_adapter.get_chunk_producer(
                &head.epoch_id,
                head.height + i,
                shard_id,
            )?;
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
        let me = &self.block_producer.as_ref().map(|x| x.account_id.clone());
        for (sync_hash, state_sync_info) in self.chain.store().iterate_state_sync_infos() {
            assert_eq!(sync_hash, state_sync_info.epoch_tail_hash);
            let network_adapter1 = self.network_adapter.clone();

            let (state_sync, new_shard_sync) = self
                .catchup_state_syncs
                .entry(sync_hash)
                .or_insert_with(|| (StateSync::new(network_adapter1), HashMap::new()));

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

                    for missing_chunks in blocks_missing_chunks.write().unwrap().drain(..) {
                        self.shards_mgr.request_chunks(missing_chunks).unwrap();
                    }
                    let unwrapped_accepted_blocks =
                        accepted_blocks.write().unwrap().drain(..).collect();
                    return Ok(unwrapped_accepted_blocks);
                }
            }
        }

        Ok(vec![])
    }

    /// When accepting challenge, we verify that it's valid given signature with current validators.
    pub fn process_challenge(&mut self, challenge: Challenge) -> Result<(), Error> {
        if self.challenges.contains_key(&challenge.hash) {
            return Ok(());
        }
        debug!(target: "client", "Received challenge: {:?}", challenge);
        let head = self.chain.head()?;
        if self.runtime_adapter.verify_validator_or_fisherman_signature(
            &head.epoch_id,
            &head.prev_block_hash,
            &challenge.account_id,
            challenge.hash.as_ref(),
            &challenge.signature,
        )? {
            // If challenge is not double sign, we should process it right away to invalidate the chain.
            match challenge.body {
                ChallengeBody::BlockDoubleSign(_) => {}
                _ => {
                    self.chain.process_challenge(&challenge);
                }
            }
            self.challenges.insert(challenge.hash, challenge);
        }
        Ok(())
    }
}
