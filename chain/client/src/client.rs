//! Client is responsible for tracking the chain, chunks, and producing them when needed.
//! This client works completely syncronously and must be operated by some async actor outside.

use std::cmp::min;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use cached::{Cached, SizedCache};
use chrono::Utc;
use log::{debug, error, info};

use near_chain::types::{
    AcceptedBlock, LatestKnown, ReceiptResponse, ValidatorSignatureVerificationResult,
};
use near_chain::{
    BlockApproval, BlockStatus, Chain, ChainGenesis, ChainStoreAccess, Provenance, RuntimeAdapter,
    Tip,
};
use near_chunks::{NetworkAdapter, ShardsManager};
use near_crypto::Signature;
use near_network::types::{ChunkPartMsg, PeerId, ReasonForBan};
use near_network::NetworkRequests;
use near_primitives::block::{Block, BlockHeader};
use near_primitives::hash::CryptoHash;
use near_primitives::sharding::{ChunkOnePart, ShardChunkHeader};
use near_primitives::types::{AccountId, BlockIndex, EpochId, ShardId};
use near_primitives::utils::to_timestamp;
use near_store::Store;

use crate::sync::{BlockSync, HeaderSync, StateSync};
use crate::types::{Error, ShardSyncStatus};
use crate::{BlockProducer, ClientConfig, SyncStatus};
use near_primitives::merkle::merklize;
use near_primitives::transaction::check_tx_history;

/// Block economics config taken from genesis config
struct BlockEconomicsConfig {
    gas_price_adjustment_rate: u8,
}

pub struct Client {
    pub config: ClientConfig,
    pub sync_status: SyncStatus,
    pub chain: Chain,
    pub runtime_adapter: Arc<dyn RuntimeAdapter>,
    pub shards_mgr: ShardsManager,
    /// Network adapter.
    network_adapter: Arc<dyn NetworkAdapter>,
    /// Signer for block producer (if present).
    pub block_producer: Option<BlockProducer>,
    /// Set of approvals for the next block.
    pub approvals: HashMap<usize, Signature>,
    /// Approvals for which we do not have the block yet
    pending_approvals: SizedCache<CryptoHash, HashMap<AccountId, (Signature, PeerId)>>,
    /// A mapping from a block for which a state sync is underway for the next epoch, and the object
    /// storing the current status of the state sync
    pub catchup_state_syncs: HashMap<CryptoHash, (StateSync, HashMap<u64, ShardSyncStatus>)>,
    /// Keeps track of syncing headers.
    pub header_sync: HeaderSync,
    /// Keeps track of syncing block.
    pub block_sync: BlockSync,
    /// Keeps track of syncing state.
    pub state_sync: StateSync,
    /// Block economics, relevant to changes when new block must be produced.
    block_economics_config: BlockEconomicsConfig,
}

impl Client {
    pub fn new(
        config: ClientConfig,
        store: Arc<Store>,
        chain_genesis: ChainGenesis,
        runtime_adapter: Arc<dyn RuntimeAdapter>,
        network_adapter: Arc<dyn NetworkAdapter>,
        block_producer: Option<BlockProducer>,
    ) -> Result<Self, Error> {
        let chain = Chain::new(store.clone(), runtime_adapter.clone(), &chain_genesis)?;
        let shards_mgr = ShardsManager::new(
            block_producer.as_ref().map(|x| x.account_id.clone()),
            runtime_adapter.clone(),
            network_adapter.clone(),
            store.clone(),
        );
        let sync_status = SyncStatus::AwaitingPeers;
        let header_sync = HeaderSync::new(network_adapter.clone());
        let block_sync = BlockSync::new(network_adapter.clone(), config.block_fetch_horizon);
        let state_sync = StateSync::new(network_adapter.clone());
        let num_block_producers = config.num_block_producers;
        Ok(Self {
            config,
            sync_status,
            chain,
            runtime_adapter,
            shards_mgr,
            network_adapter,
            block_producer,
            approvals: HashMap::new(),
            pending_approvals: SizedCache::with_size(num_block_producers),
            catchup_state_syncs: HashMap::new(),
            header_sync,
            block_sync,
            state_sync,
            block_economics_config: BlockEconomicsConfig {
                gas_price_adjustment_rate: chain_genesis.gas_price_adjustment_rate,
            },
        })
    }

    pub fn remove_transactions_for_block(&mut self, me: AccountId, block: &Block) {
        for (shard_id, chunk_header) in block.chunks.iter().enumerate() {
            let shard_id = shard_id as ShardId;
            if block.header.inner.height == chunk_header.height_included {
                if self.shards_mgr.cares_about_shard_this_or_next_epoch(
                    Some(&me),
                    &block.header.inner.prev_hash,
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
    }

    pub fn reintroduce_transactions_for_block(&mut self, me: AccountId, block: &Block) {
        for (shard_id, chunk_header) in block.chunks.iter().enumerate() {
            let shard_id = shard_id as ShardId;
            if block.header.inner.height == chunk_header.height_included {
                if self.shards_mgr.cares_about_shard_this_or_next_epoch(
                    Some(&me),
                    &block.header.inner.prev_hash,
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
    }

    /// Produce block if we are block producer for given `next_height` index.
    /// Either returns produced block (not applied) or error.
    pub fn produce_block(
        &mut self,
        next_height: BlockIndex,
        elapsed_since_last_block: Duration,
    ) -> Result<Option<Block>, Error> {
        let block_producer = self.block_producer.as_ref().ok_or_else(|| {
            Error::BlockProducer("Called without block producer info.".to_string())
        })?;
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
        let prev = self.chain.get_block_header(&head.last_block_hash)?;
        let prev_hash = head.last_block_hash;
        let prev_prev_hash = prev.inner.prev_hash;

        debug!(target: "client", "{:?} Producing block at height {}", block_producer.account_id, next_height);

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

        // Wait until we have all approvals or timeouts per max block production delay.
        let validators =
            self.runtime_adapter.get_epoch_block_producers(&head.epoch_id, &prev_hash)?;
        let total_validators = validators.len();
        let prev_same_bp = self.runtime_adapter.get_block_producer(&head.epoch_id, head.height)?
            == block_producer.account_id.clone();
        // If epoch changed, and before there was 2 validators and now there is 1 - prev_same_bp is false, but total validators right now is 1.
        let total_approvals =
            total_validators - min(if prev_same_bp { 1 } else { 2 }, total_validators);
        if head.height > 0
            && self.approvals.len() < total_approvals
            && elapsed_since_last_block < self.config.max_block_production_delay
        {
            // Will retry after a `block_production_tracking_delay`.
            debug!(target: "client", "Produce block: approvals {}, expected: {}", self.approvals.len(), total_approvals);
            return Ok(None);
        }

        // If we are not producing empty blocks, skip this and call handle scheduling for the next block.
        let new_chunks = self.shards_mgr.prepare_chunks(prev_hash);

        // If we are producing empty blocks and there are no transactions.
        if !self.config.produce_empty_blocks && new_chunks.is_empty() {
            debug!(target: "client", "Empty blocks, skipping block production");
            return Ok(None);
        }

        let prev_block = self.chain.get_block(&head.last_block_hash)?;
        let mut chunks = prev_block.chunks.clone();

        // Collect new chunks.
        for (shard_id, mut chunk_header) in new_chunks {
            chunk_header.height_included = next_height;
            chunks[shard_id as usize] = chunk_header;
        }

        let prev_header = self.chain.get_block_header(&head.last_block_hash)?;

        // This is kept here in case we want to revive txs on the block level.
        let transactions = vec![];

        // At this point, the previous epoch hash must be available
        let epoch_id = self
            .runtime_adapter
            .get_epoch_id_from_prev_block(&head.last_block_hash)
            .expect("Epoch hash should exist at this point");

        let inflation = if self.runtime_adapter.is_next_block_epoch_start(&head.last_block_hash)? {
            let next_epoch_id =
                self.runtime_adapter.get_next_epoch_id_from_prev_block(&head.last_block_hash)?;
            Some(self.runtime_adapter.get_epoch_inflation(&next_epoch_id)?)
        } else {
            None
        };

        let block = Block::produce(
            &prev_header,
            next_height,
            chunks,
            epoch_id,
            transactions,
            self.approvals.drain().collect(),
            self.block_economics_config.gas_price_adjustment_rate,
            inflation,
            block_producer.signer.clone(),
        );

        // Update latest known even before returning block out, to prevent race conditions.
        self.chain.mut_store().save_latest_known(LatestKnown {
            height: next_height,
            seen: to_timestamp(Utc::now()),
        })?;

        Ok(Some(block))
    }

    fn produce_chunk(
        &mut self,
        prev_block_hash: CryptoHash,
        epoch_id: &EpochId,
        last_header: ShardChunkHeader,
        next_height: BlockIndex,
        shard_id: ShardId,
    ) -> Result<(), Error> {
        let block_producer = self
            .block_producer
            .as_ref()
            .ok_or_else(|| Error::ChunkProducer("Called without block producer info.".to_string()))?
            .clone();

        let chunk_proposer =
            self.runtime_adapter.get_chunk_producer(epoch_id, next_height, shard_id).unwrap();
        if block_producer.account_id != chunk_proposer {
            debug!(target: "client", "Not producing chunk for shard {}: chain at {}, not block producer for next block. Me: {}, proposer: {}", shard_id, next_height, block_producer.account_id, chunk_proposer);
            return Ok(());
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

        let transaction_validity_period = self.chain.transaction_validity_period;
        let transactions: Vec<_> = self
            .shards_mgr
            .prepare_transactions(shard_id, self.config.block_expected_weight)?
            .into_iter()
            .filter(|t| {
                check_tx_history(
                    self.chain.get_block_header(&t.transaction.block_hash).ok(),
                    next_height,
                    transaction_validity_period,
                )
            })
            .collect();
        let block_header = self.chain.get_block_header(&prev_block_hash)?;
        let transactions_len = transactions.len();
        let filtered_transactions = self.runtime_adapter.filter_transactions(
            next_height,
            block_header.inner.gas_price,
            chunk_extra.state_root,
            transactions,
        );
        let (tx_root, _) = merklize(&filtered_transactions);
        debug!(
            "Creating a chunk with {} filtered transactions from {} total transactions for shard {}",
            filtered_transactions.len(),
            transactions_len,
            shard_id
        );

        let ReceiptResponse(_, receipts) = self.chain.get_outgoing_receipts_for_shard(
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
        let receipts_hashes = self.runtime_adapter.build_receipts_hashes(&receipts)?;
        let (receipts_root, _) = merklize(&receipts_hashes);

        let encoded_chunk = self.shards_mgr.create_encoded_shard_chunk(
            prev_block_hash,
            chunk_extra.state_root,
            next_height,
            shard_id,
            chunk_extra.gas_used,
            chunk_extra.gas_limit,
            chunk_extra.rent_paid,
            chunk_extra.validator_proposals.clone(),
            &filtered_transactions,
            &receipts,
            receipts_root,
            tx_root,
            block_producer.signer.clone(),
        )?;

        debug!(
            target: "client",
            "Produced chunk at height {} for shard {} with {} txs and {} receipts, I'm {}, chunk_hash: {}",
            next_height,
            shard_id,
            filtered_transactions.len(),
            receipts.len(),
            block_producer.account_id,
            encoded_chunk.chunk_hash().0,
        );

        self.shards_mgr.distribute_encoded_chunk(encoded_chunk, receipts);

        Ok(())
    }

    pub fn process_block(
        &mut self,
        block: Block,
        provenance: Provenance,
    ) -> (Vec<AcceptedBlock>, Result<Option<Tip>, near_chain::Error>) {
        // TODO: replace to channels or cross beams here? we don't have multi-threading here so it's mostly to get around borrow checker.
        let accepted_blocks = Arc::new(RwLock::new(vec![]));
        let blocks_missing_chunks = Arc::new(RwLock::new(vec![]));
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
            )
        };
        for missing_chunks in blocks_missing_chunks.write().unwrap().drain(..) {
            self.shards_mgr.request_chunks(missing_chunks).unwrap();
        }
        let unwrapped_accepted_blocks = accepted_blocks.write().unwrap().drain(..).collect();
        (unwrapped_accepted_blocks, result)
    }

    pub fn process_chunk_part(&mut self, part: ChunkPartMsg) -> Result<Vec<AcceptedBlock>, Error> {
        if let Some(block_hash) = self.shards_mgr.process_chunk_part(part)? {
            Ok(self.process_blocks_with_missing_chunks(block_hash))
        } else {
            Ok(vec![])
        }
    }

    pub fn process_chunk_one_part(
        &mut self,
        one_part_msg: ChunkOnePart,
    ) -> Result<Vec<AcceptedBlock>, Error> {
        let prev_block_hash = one_part_msg.header.inner.prev_block_hash;
        let ret = self.shards_mgr.process_chunk_one_part(one_part_msg.clone())?;
        if ret {
            // If the chunk builds on top of the current head, get all the remaining parts
            // TODO: if the bp receives the chunk before they receive the block, they will
            //     not collect the parts currently. It will result in chunk not included
            //     in the next block.
            if self.shards_mgr.cares_about_shard_this_or_next_epoch(
                self.block_producer.as_ref().map(|x| &x.account_id),
                &prev_block_hash,
                one_part_msg.shard_id,
                true,
            ) && self
                .chain
                .head()
                .map(|head| head.last_block_hash == one_part_msg.header.inner.prev_block_hash)
                .unwrap_or(false)
            {
                self.shards_mgr.request_chunks(vec![one_part_msg.header]).unwrap();
            } else {
                // We are getting here either because we don't care about the shard, or
                //    because we see the one part before we see the block.
                // In the latter case we will request parts once the block is received
            }
            return Ok(self.process_blocks_with_missing_chunks(prev_block_hash));
        }
        Ok(vec![])
    }

    pub fn process_block_header(&mut self, header: &BlockHeader) -> Result<(), near_chain::Error> {
        self.chain.process_block_header(header)
    }

    pub fn sync_block_headers(
        &mut self,
        headers: Vec<BlockHeader>,
    ) -> Result<(), near_chain::Error> {
        self.chain.sync_block_headers(headers)
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

        // Process orphaned chunk_one_parts
        if self.shards_mgr.process_orphaned_one_parts(block_hash) {
            self.process_blocks_with_missing_chunks(block_hash);
        }

        if provenance != Provenance::SYNC {
            // If we produced the block, then we want to broadcast it.
            // If received the block from another node then broadcast "header first" to minimise network traffic.
            if provenance == Provenance::PRODUCED {
                let _ = self.network_adapter.send(NetworkRequests::Block { block: block.clone() });
            } else {
                let approval = self.pending_approvals.cache_get(&block_hash).cloned();
                if let Some(approval) = approval {
                    for (account_id, (sig, peer_id)) in approval {
                        if !self.collect_block_approval(&account_id, &block_hash, &sig, &peer_id) {
                            let _ = self.network_adapter.send(NetworkRequests::BanPeer {
                                peer_id,
                                ban_reason: ReasonForBan::BadBlockApproval,
                            });
                        }
                    }
                }
                let approval = self.get_block_approval(&block);
                let _ = self.network_adapter.send(NetworkRequests::BlockHeaderAnnounce {
                    header: block.header.clone(),
                    approval,
                });
            }
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
                        while remove_head.inner.height > reintroduce_head.inner.height {
                            to_remove.push(remove_head.hash());
                            remove_head = self
                                .chain
                                .get_block_header(&remove_head.inner.prev_hash)
                                .unwrap()
                                .clone();
                        }
                        while reintroduce_head.inner.height > remove_head.inner.height
                            || reintroduce_head.inner.height == remove_head.inner.height
                                && reintroduce_head.hash() != remove_head.hash()
                        {
                            to_reintroduce.push(reintroduce_head.hash());
                            reintroduce_head = self
                                .chain
                                .get_block_header(&reintroduce_head.inner.prev_hash)
                                .unwrap()
                                .clone();
                        }
                    }

                    for to_reintroduce_hash in to_reintroduce {
                        let block = self.chain.get_block(&to_reintroduce_hash).unwrap().clone();
                        self.reintroduce_transactions_for_block(bp.account_id.clone(), &block);
                    }

                    for to_remove_hash in to_remove {
                        let block = self.chain.get_block(&to_remove_hash).unwrap().clone();
                        self.remove_transactions_for_block(bp.account_id.clone(), &block);
                    }
                }
            };

            if provenance != Provenance::SYNC {
                // Produce new chunks
                for shard_id in 0..self.runtime_adapter.num_shards() {
                    let epoch_id = self
                        .runtime_adapter
                        .get_epoch_id_from_prev_block(&block.header.hash())
                        .unwrap();
                    let chunk_proposer = self
                        .runtime_adapter
                        .get_chunk_producer(&epoch_id, block.header.inner.height + 1, shard_id)
                        .unwrap();

                    if chunk_proposer == *bp.account_id {
                        if let Err(err) = self.produce_chunk(
                            block.hash(),
                            &epoch_id,
                            block.chunks[shard_id as usize].clone(),
                            block.header.inner.height + 1,
                            shard_id,
                        ) {
                            error!(target: "client", "Error producing chunk {:?}", err);
                        }
                    }
                }
            }
        }
    }

    /// Check if any block with missing chunks is ready to be processed
    pub fn process_blocks_with_missing_chunks(
        &mut self,
        last_accepted_block_hash: CryptoHash,
    ) -> Vec<AcceptedBlock> {
        let accepted_blocks = Arc::new(RwLock::new(vec![]));
        let blocks_missing_chunks = Arc::new(RwLock::new(vec![]));
        let me =
            self.block_producer.as_ref().map(|block_producer| block_producer.account_id.clone());
        self.chain.check_blocks_with_missing_chunks(&me, last_accepted_block_hash, |accepted_block| {
            debug!(target: "client", "Block {} was missing chunks but now is ready to be processed", accepted_block.hash);
            accepted_blocks.write().unwrap().push(accepted_block);
        }, |missing_chunks| blocks_missing_chunks.write().unwrap().push(missing_chunks));
        for missing_chunks in blocks_missing_chunks.write().unwrap().drain(..) {
            self.shards_mgr.request_chunks(missing_chunks).unwrap();
        }
        let unwrapped_accepted_blocks = accepted_blocks.write().unwrap().drain(..).collect();
        unwrapped_accepted_blocks
    }

    /// Create approval for given block or return none if not a block producer.
    fn get_block_approval(&mut self, block: &Block) -> Option<BlockApproval> {
        let mut epoch_hash =
            self.runtime_adapter.get_epoch_id_from_prev_block(&block.hash()).ok()?;
        let next_block_producer_account =
            self.runtime_adapter.get_block_producer(&epoch_hash, block.header.inner.height + 1);
        if let (Some(block_producer), Ok(next_block_producer_account)) =
            (&self.block_producer, &next_block_producer_account)
        {
            if &block_producer.account_id != next_block_producer_account {
                epoch_hash = block.header.inner.epoch_id.clone();
                if let Ok(validators) =
                    self.runtime_adapter.get_epoch_block_producers(&epoch_hash, &block.hash())
                {
                    if let Some((_, is_slashed)) =
                        validators.into_iter().find(|v| v.0 == block_producer.account_id)
                    {
                        if !is_slashed {
                            return Some(BlockApproval::new(
                                block.hash(),
                                &*block_producer.signer,
                                next_block_producer_account.clone(),
                            ));
                        }
                    }
                }
            }
        }
        None
    }

    /// Collects block approvals. Returns false if block approval is invalid.
    pub fn collect_block_approval(
        &mut self,
        account_id: &AccountId,
        hash: &CryptoHash,
        signature: &Signature,
        peer_id: &PeerId,
    ) -> bool {
        // TODO: figure out how to validate better before hitting the disk? For example validator and account cache to validate signature first.
        // TODO: This header is missing, should collect for later? should have better way to verify then.

        let header = match self.chain.get_block_header(&hash) {
            Ok(h) => h.clone(),
            Err(e) => {
                if e.is_bad_data() {
                    return false;
                }
                let mut entry =
                    self.pending_approvals.cache_remove(hash).unwrap_or_else(|| HashMap::new());
                entry.insert(account_id.clone(), (signature.clone(), peer_id.clone()));
                self.pending_approvals.cache_set(*hash, entry);
                return true;
            }
        };

        // TODO: Access runtime adapter only once to find the position and public key.

        // If given account is not current block proposer.
        let position =
            match self.runtime_adapter.get_epoch_block_producers(&header.inner.epoch_id, &hash) {
                Ok(validators) => {
                    let position = validators.iter().position(|x| &(x.0) == account_id);
                    if let Some(idx) = position {
                        if !validators[idx].1 {
                            idx
                        } else {
                            return false;
                        }
                    } else {
                        return false;
                    }
                }
                Err(err) => {
                    error!(target: "client", "Block approval error: {}", err);
                    return false;
                }
            };
        // Check signature is correct for given validator.
        if let ValidatorSignatureVerificationResult::Invalid =
            self.runtime_adapter.verify_validator_signature(
                &header.inner.epoch_id,
                account_id,
                hash.as_ref(),
                signature,
            )
        {
            return false;
        }
        debug!(target: "client", "Received approval for {} from {}", hash, account_id);
        self.approvals.insert(position, signature.clone());
        true
    }
}
