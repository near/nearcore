//! Client is responsible for tracking the chain, chunks, and producing them when needed.
//! This client works completely synchronously and must be operated by some async actor outside.

use std::cmp::max;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use near_client_primitives::debug::BlockProduction;
use near_primitives::time::Clock;
use tracing::{debug, error, info, trace, warn};

use near_chain::chain::{
    ApplyStatePartsRequest, BlockCatchUpRequest, BlockMissingChunks, BlocksCatchUpState,
    OrphanMissingChunks, StateSplitRequest, TX_ROUTING_HEIGHT_HORIZON,
};
use near_chain::test_utils::format_hash;
use near_chain::types::LatestKnown;
use near_chain::{
    BlockProcessingArtifact, BlockStatus, Chain, ChainGenesis, ChainStoreAccess,
    DoneApplyChunkCallback, Doomslug, DoomslugThresholdMode, Provenance, RuntimeAdapter,
};
use near_chain_configs::ClientConfig;
use near_chunks::{ProcessPartialEncodedChunkResult, ShardsManager};
use near_network::types::{
    FullPeerInfo, NetworkClientResponses, NetworkRequests, PeerManagerAdapter,
};
use near_primitives::block::{Approval, ApprovalInner, ApprovalMessage, Block, BlockHeader, Tip};
use near_primitives::challenge::{Challenge, ChallengeBody};
use near_primitives::hash::CryptoHash;
use near_primitives::merkle::{merklize, MerklePath, PartialMerkleTree};
use near_primitives::receipt::Receipt;
use near_primitives::sharding::{
    ChunkHash, EncodedShardChunk, PartialEncodedChunk, PartialEncodedChunkV2, ReedSolomonWrapper,
    ShardChunkHeader, ShardInfo,
};
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::chunk_extra::ChunkExtra;
use near_primitives::types::{AccountId, ApprovalStake, BlockHeight, EpochId, NumBlocks, ShardId};
use near_primitives::unwrap_or_return;
use near_primitives::utils::MaybeValidated;
use near_primitives::validator_signer::ValidatorSigner;

use crate::sync::{BlockSync, EpochSync, HeaderSync, StateSync, StateSyncResult};
use crate::{metrics, SyncStatus};
use near_chain::types::ValidatorInfoIdentifier;
use near_client_primitives::types::{Error, ShardSyncDownload, ShardSyncStatus};
use near_network::types::{AccountKeys, ChainInfo, PeerManagerMessageRequest, SetChainInfo};
use near_network_primitives::types::{
    PartialEncodedChunkForwardMsg, PartialEncodedChunkResponseMsg,
};
use near_o11y::log_assert;
use near_primitives::block_header::ApprovalType;
use near_primitives::epoch_manager::RngSeed;
use near_primitives::version::PROTOCOL_VERSION;

const NUM_REBROADCAST_BLOCKS: usize = 30;

/// The time we wait for the response to a Epoch Sync request before retrying
// TODO #3488 set 30_000
pub const EPOCH_SYNC_REQUEST_TIMEOUT: Duration = Duration::from_millis(1_000);
/// How frequently a Epoch Sync response can be sent to a particular peer
// TODO #3488 set 60_000
pub const EPOCH_SYNC_PEER_TIMEOUT: Duration = Duration::from_millis(10);

/// number of blocks at the epoch start for which we will log more detailed info
pub const EPOCH_START_INFO_BLOCKS: u64 = 500;

/// Number of blocks (and chunks) for which to keep the detailed timing information for debug purposes.
pub const PRODUCTION_TIMES_CACHE_SIZE: usize = 1000;

pub struct Client {
    /// Adversarial controls
    #[cfg(feature = "test_features")]
    pub adv_produce_blocks: bool,
    #[cfg(feature = "test_features")]
    pub adv_produce_blocks_only_valid: bool,

    /// Fast Forward accrued delta height used to calculate fast forwarded timestamps for each block.
    #[cfg(feature = "sandbox")]
    pub(crate) accrued_fastforward_delta: near_primitives::types::BlockHeightDelta,

    pub config: ClientConfig,
    pub sync_status: SyncStatus,
    pub chain: Chain,
    pub doomslug: Doomslug,
    pub runtime_adapter: Arc<dyn RuntimeAdapter>,
    pub shards_mgr: ShardsManager,
    /// Network adapter.
    network_adapter: Arc<dyn PeerManagerAdapter>,
    /// Signer for block producer (if present).
    pub validator_signer: Option<Arc<dyn ValidatorSigner>>,
    /// Approvals for which we do not have the block yet
    pub pending_approvals:
        lru::LruCache<ApprovalInner, HashMap<AccountId, (Approval, ApprovalType)>>,
    /// A mapping from a block for which a state sync is underway for the next epoch, and the object
    /// storing the current status of the state sync and blocks catch up
    pub catchup_state_syncs:
        HashMap<CryptoHash, (StateSync, HashMap<u64, ShardSyncDownload>, BlocksCatchUpState)>,
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
    pub rs: ReedSolomonWrapper,
    /// Blocks that have been re-broadcast recently. They should not be broadcast again.
    rebroadcasted_blocks: lru::LruCache<CryptoHash, ()>,
    /// Last time the head was updated, or our head was rebroadcasted. Used to re-broadcast the head
    /// again to prevent network from stalling if a large percentage of the network missed a block
    last_time_head_progress_made: Instant,

    /// Block and chunk production timing information.
    /// used only for debug purposes.
    pub block_production_times: lru::LruCache<BlockHeight, BlockProduction>,
    pub chunk_production_times: lru::LruCache<(BlockHeight, ShardId), Duration>,

    /// Cached precomputed set of TIER1 accounts.
    /// See send_network_chain_info().
    tier1_accounts_cache: Option<(EpochId, Arc<AccountKeys>)>,
}

// Debug information about the upcoming block.
#[derive(Default)]
pub struct BlockDebugStatus {
    // How long is this block 'in progress' (time since we first saw it).
    pub in_progress_for: Option<Duration>,
    // How long is this block in orphan pool.
    pub in_orphan_for: Option<Duration>,
    // List of chunk hashes that belong to this block.
    pub chunk_hashes: Vec<ChunkHash>,

    // Chunk statuses are below:
    // We first sent the request to fetch the chunk
    // Later we get the response from the peer and we try to reconstruct it.
    // If reconstructions suceeds, the chunk will be marked as complete.
    // If it fails (or fragments are missing) - we're going to re-request the chunk again.

    // Chunks that we reqeusted (sent the request to peers).
    pub chunks_requested: HashSet<ChunkHash>,
    // Chunks for which we've received the response.
    pub chunks_received: HashSet<ChunkHash>,
    // Chunks completed - fully rebuild and present in database.
    pub chunks_completed: HashSet<ChunkHash>,
}

impl Client {
    pub fn new(
        config: ClientConfig,
        chain_genesis: ChainGenesis,
        runtime_adapter: Arc<dyn RuntimeAdapter>,
        network_adapter: Arc<dyn PeerManagerAdapter>,
        validator_signer: Option<Arc<dyn ValidatorSigner>>,
        enable_doomslug: bool,
        rng_seed: RngSeed,
    ) -> Result<Self, Error> {
        let doomslug_threshold_mode = if enable_doomslug {
            DoomslugThresholdMode::TwoThirds
        } else {
            DoomslugThresholdMode::NoApprovals
        };
        let chain = Chain::new(
            runtime_adapter.clone(),
            &chain_genesis,
            doomslug_threshold_mode,
            !config.archive,
        )?;
        let shards_mgr = ShardsManager::new(
            validator_signer.as_ref().map(|x| x.validator_id().clone()),
            runtime_adapter.clone(),
            network_adapter.clone(),
            rng_seed,
        );
        let sync_status = SyncStatus::AwaitingPeers;
        let genesis_block = chain.genesis_block();
        let epoch_sync = EpochSync::new(
            network_adapter.clone(),
            genesis_block.header().epoch_id().clone(),
            genesis_block.header().next_epoch_id().clone(),
            runtime_adapter
                .get_epoch_block_producers_ordered(
                    genesis_block.header().epoch_id(),
                    genesis_block.hash(),
                )?
                .iter()
                .map(|x| x.0.clone())
                .collect(),
            EPOCH_SYNC_REQUEST_TIMEOUT,
            EPOCH_SYNC_PEER_TIMEOUT,
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
            #[cfg(feature = "test_features")]
            adv_produce_blocks: false,
            #[cfg(feature = "test_features")]
            adv_produce_blocks_only_valid: false,
            #[cfg(feature = "sandbox")]
            accrued_fastforward_delta: 0,
            config,
            sync_status,
            chain,
            doomslug,
            runtime_adapter,
            shards_mgr,
            network_adapter,
            validator_signer,
            pending_approvals: lru::LruCache::new(num_block_producer_seats),
            catchup_state_syncs: HashMap::new(),
            epoch_sync,
            header_sync,
            block_sync,
            state_sync,
            challenges: Default::default(),
            rs: ReedSolomonWrapper::new(data_parts, parity_parts),
            rebroadcasted_blocks: lru::LruCache::new(NUM_REBROADCAST_BLOCKS),
            last_time_head_progress_made: Clock::instant(),
            block_production_times: lru::LruCache::new(PRODUCTION_TIMES_CACHE_SIZE),
            chunk_production_times: lru::LruCache::new(PRODUCTION_TIMES_CACHE_SIZE),
            tier1_accounts_cache: None,
        })
    }

    // Checks if it's been at least `stall_timeout` since the last time the head was updated, or
    // this method was called. If yes, rebroadcasts the current head.
    pub fn check_head_progress_stalled(&mut self, stall_timeout: Duration) -> Result<(), Error> {
        if Clock::instant() > self.last_time_head_progress_made + stall_timeout
            && !self.sync_status.is_syncing()
        {
            let block = self.chain.get_block(&self.chain.head()?.last_block_hash)?;
            self.network_adapter.do_send(PeerManagerMessageRequest::NetworkRequests(
                NetworkRequests::Block { block: block },
            ));
            self.last_time_head_progress_made = Clock::instant();
        }
        Ok(())
    }

    pub fn remove_transactions_for_block(&mut self, me: AccountId, block: &Block) {
        for (shard_id, chunk_header) in block.chunks().iter().enumerate() {
            let shard_id = shard_id as ShardId;
            if block.header().height() == chunk_header.height_included() {
                if self.shards_mgr.cares_about_shard_this_or_next_epoch(
                    Some(&me),
                    block.header().prev_hash(),
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
                    block.header().prev_hash(),
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
        #[cfg(feature = "test_features")]
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
        #[cfg(feature = "test_features")]
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

        if !self.is_me_block_producer(account_id, next_block_proposer) {
            info!(target: "client", "Produce block: chain at {}, not block producer for next block.", next_height);
            return Ok(true);
        }

        #[cfg(feature = "test_features")]
        {
            if self.adv_produce_blocks {
                return Ok(false);
            }
        }

        if self.runtime_adapter.is_next_block_epoch_start(&head.last_block_hash)? {
            if !self.chain.prev_block_is_caught_up(prev_prev_hash, prev_hash)? {
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
        let _span = tracing::debug_span!(target: "client", "produce_block", next_height).entered();
        let known_height = self.chain.store().get_latest_known()?.height;

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

        let prev = self.chain.get_block_header(&head.last_block_hash)?;
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
            validator_signer.validator_id(),
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
            #[cfg(not(feature = "test_features"))]
            return Ok(None);
            #[cfg(feature = "test_features")]
            if !self.adv_produce_blocks || self.adv_produce_blocks_only_valid {
                return Ok(None);
            }
        }

        let new_chunks = self.shards_mgr.prepare_chunks(&prev_hash);
        debug!(target: "client", "{:?} Producing block at height {}, parent {} @ {}, {} new chunks", validator_signer.validator_id(),
               next_height, prev.height(), format_hash(head.last_block_hash), new_chunks.len());

        // If we are producing empty blocks and there are no transactions.
        if !self.config.produce_empty_blocks && new_chunks.is_empty() {
            debug!(target: "client", "Empty blocks, skipping block production");
            return Ok(None);
        }

        let mut approvals_map = self.doomslug.get_witness(&prev_hash, prev_height, next_height);

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
                    approvals_map.remove(&account_id).map(|x| x.0.signature)
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
            Chain::compute_bp_hash(
                &*self.runtime_adapter,
                next_epoch_id,
                epoch_id.clone(),
                &prev_hash,
            )?
        } else {
            prev_next_bp_hash
        };

        #[cfg(feature = "sandbox")]
        let timestamp_override = Some(Clock::utc() + self.sandbox_delta_time());
        #[cfg(not(feature = "sandbox"))]
        let timestamp_override = None;

        // Get block extra from previous block.
        let block_merkle_tree = self.chain.store().get_block_merkle_tree(&prev_hash)?;
        let mut block_merkle_tree = PartialMerkleTree::clone(&block_merkle_tree);
        block_merkle_tree.insert(prev_hash);
        let block_merkle_root = block_merkle_tree.root();
        // The number of leaves in Block Merkle Tree is the amount of Blocks on the Canonical Chain by construction.
        // The ordinal of the next Block will be equal to this amount plus one.
        let block_ordinal: NumBlocks = block_merkle_tree.size() + 1;
        let prev_block_extra = self.chain.get_block_extra(&prev_hash)?;
        let prev_block = self.chain.get_block(&prev_hash)?;
        let mut chunks = Chain::get_prev_chunk_headers(&*self.runtime_adapter, &prev_block)?;

        // Add debug information about the block production (and info on when did the chunks arrive).
        self.block_production_times.put(
            next_height,
            BlockProduction {
                block_production_time: Some(chrono::Utc::now()),
                chunks_collection_time: (0..chunks.len() as u64)
                    .map(|shard_id| {
                        new_chunks.get(&shard_id).map(|(_, arrival_time)| arrival_time.clone())
                    })
                    .collect::<Vec<_>>(),
            },
        );

        // Collect new chunks.
        for (shard_id, (mut chunk_header, _)) in new_chunks {
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
        let this_epoch_protocol_version =
            self.runtime_adapter.get_epoch_protocol_version(&epoch_id)?;
        let next_epoch_protocol_version =
            self.runtime_adapter.get_epoch_protocol_version(&next_epoch_id)?;

        let block = Block::produce(
            this_epoch_protocol_version,
            next_epoch_protocol_version,
            prev_header,
            next_height,
            block_ordinal,
            chunks,
            epoch_id,
            next_epoch_id,
            epoch_sync_data_hash,
            approvals,
            gas_price_adjustment_rate,
            min_gas_price,
            max_gas_price,
            minted_amount,
            prev_block_extra.challenges_result.clone(),
            vec![],
            &*validator_signer,
            next_bp_hash,
            block_merkle_root,
            timestamp_override,
        );

        // Update latest known even before returning block out, to prevent race conditions.
        self.chain.mut_store().save_latest_known(LatestKnown {
            height: next_height,
            seen: block.header().raw_timestamp(),
        })?;

        metrics::BLOCK_PRODUCED_TOTAL.inc();

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
        let timer = Instant::now();
        let _timer = metrics::PRODUCE_CHUNK_TIME
            .with_label_values(&[&format!("{}", shard_id)])
            .start_timer();
        let _span = tracing::debug_span!(target: "client", "produce_chunk", next_height, shard_id, ?epoch_id).entered();
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

        let shard_uid = self.runtime_adapter.shard_id_to_uid(shard_id, epoch_id)?;
        let chunk_extra = self
            .chain
            .get_chunk_extra(&prev_block_hash, &shard_uid)
            .map_err(|err| Error::ChunkProducer(format!("No chunk extra available: {}", err)))?;

        let prev_block_header = self.chain.get_block_header(&prev_block_hash)?;
        let transactions = self.prepare_transactions(shard_id, &chunk_extra, &prev_block_header)?;
        let num_filtered_transactions = transactions.len();
        let (tx_root, _) = merklize(&transactions);
        let outgoing_receipts = self.chain.get_outgoing_receipts_for_shard(
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
        let shard_layout = self.runtime_adapter.get_shard_layout(epoch_id)?;
        let outgoing_receipts_hashes =
            Chain::build_receipts_hashes(&outgoing_receipts, &shard_layout);
        let (outgoing_receipts_root, _) = merklize(&outgoing_receipts_hashes);

        let protocol_version = self.runtime_adapter.get_epoch_protocol_version(epoch_id)?;
        let (encoded_chunk, merkle_paths) = ShardsManager::create_encoded_shard_chunk(
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
            height=next_height,
            shard_id,
            me=%validator_signer.validator_id(),
            chunk_hash=%encoded_chunk.chunk_hash().0,
            %prev_block_hash,
            "Produced chunk with {} txs and {} receipts",
            num_filtered_transactions,
            outgoing_receipts.len(),
        );

        metrics::CHUNK_PRODUCED_TOTAL.inc();
        self.chunk_production_times.put((next_height, shard_id), timer.elapsed());
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
            runtime_adapter.get_epoch_id_from_prev_block(prev_block_header.hash())?;
        let protocol_version = runtime_adapter.get_epoch_protocol_version(&next_epoch_id)?;

        let transactions = if let Some(mut iter) = shards_mgr.get_pool_iterator(shard_id) {
            let transaction_validity_period = chain.transaction_validity_period;
            runtime_adapter.prepare_transactions(
                prev_block_header.gas_price(),
                chunk_extra.gas_limit(),
                &next_epoch_id,
                shard_id,
                *chunk_extra.state_root(),
                // while the height of the next block that includes the chunk might not be prev_height + 1,
                // passing it will result in a more conservative check and will not accidentally allow
                // invalid transactions to be included.
                prev_block_header.height() + 1,
                &mut iter,
                &mut |tx: &SignedTransaction| -> bool {
                    chain
                        .store()
                        .check_transaction_validity_period(
                            prev_block_header,
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

    pub fn send_challenges(&mut self, challenges: Vec<ChallengeBody>) {
        if let Some(validator_signer) = &self.validator_signer {
            for body in challenges {
                let challenge = Challenge::produce(body, &**validator_signer);
                self.challenges.insert(challenge.hash, challenge.clone());
                self.network_adapter.do_send(PeerManagerMessageRequest::NetworkRequests(
                    NetworkRequests::Challenge(challenge),
                ));
            }
        }
    }

    /// Start the processing of a block. Note that this function will return before
    /// the full processing is finished because applying chunks is done asynchronously
    /// in the rayon thread pool.
    /// `apply_chunks_done_callback`: a callback that will be called when applying chunks is finished.
    pub fn start_process_block(
        &mut self,
        block: MaybeValidated<Block>,
        provenance: Provenance,
        apply_chunks_done_callback: DoneApplyChunkCallback,
    ) -> Result<(), near_chain::Error> {
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
            if self.chain.store().is_height_processed(block.header().height())? {
                return Ok(());
            }
        }

        let mut block_processing_artifacts = BlockProcessingArtifact::default();

        let result = {
            let me = self
                .validator_signer
                .as_ref()
                .map(|validator_signer| validator_signer.validator_id().clone());
            self.chain.start_process_block_async(
                &me,
                block,
                provenance,
                &mut block_processing_artifacts,
                apply_chunks_done_callback,
            )
        };

        self.process_block_processing_artifact(block_processing_artifacts);

        // Send out challenge if the block was found to be invalid.
        if let Some(validator_signer) = self.validator_signer.as_ref() {
            if let Err(e) = &result {
                match e {
                    near_chain::Error::InvalidChunkProofs(chunk_proofs) => {
                        self.network_adapter.do_send(PeerManagerMessageRequest::NetworkRequests(
                            NetworkRequests::Challenge(Challenge::produce(
                                ChallengeBody::ChunkProofs(*chunk_proofs.clone()),
                                &**validator_signer,
                            )),
                        ));
                    }
                    near_chain::Error::InvalidChunkState(chunk_state) => {
                        self.network_adapter.do_send(PeerManagerMessageRequest::NetworkRequests(
                            NetworkRequests::Challenge(Challenge::produce(
                                ChallengeBody::ChunkState(*chunk_state.clone()),
                                &**validator_signer,
                            )),
                        ));
                    }
                    _ => {}
                }
            }
        }

        result
    }

    /// Check if there are any blocks that has finished applying chunks, run post processing on these
    /// blocks.
    pub fn postprocess_ready_blocks(
        &mut self,
        apply_chunks_done_callback: DoneApplyChunkCallback,
        should_produce_chunk: bool,
    ) -> (Vec<CryptoHash>, HashMap<CryptoHash, near_chain::Error>) {
        let me = self
            .validator_signer
            .as_ref()
            .map(|validator_signer| validator_signer.validator_id().clone());
        let mut block_processing_artifacts = BlockProcessingArtifact::default();
        let (accepted_blocks, errors) = self.chain.postprocess_ready_blocks(
            &me,
            &mut block_processing_artifacts,
            apply_chunks_done_callback.clone(),
        );
        self.process_block_processing_artifact(block_processing_artifacts);
        let accepted_blocks_hashes =
            accepted_blocks.iter().map(|accepted_block| accepted_block.hash.clone()).collect();
        for accepted_block in accepted_blocks {
            self.on_block_accepted_with_optional_chunk_produce(
                accepted_block.hash,
                accepted_block.status,
                accepted_block.provenance,
                !should_produce_chunk,
                apply_chunks_done_callback.clone(),
            );
        }
        self.last_time_head_progress_made =
            max(self.chain.get_last_time_head_updated(), self.last_time_head_progress_made);
        (accepted_blocks_hashes, errors)
    }

    /// Process the result of block processing from chain, finish the steps that can't be done
    /// in chain, including
    ///  - sending challenges
    ///  - requesting missing chunks
    pub(crate) fn process_block_processing_artifact(
        &mut self,
        block_processing_artifacts: BlockProcessingArtifact,
    ) {
        let BlockProcessingArtifact { orphans_missing_chunks, blocks_missing_chunks, challenges } =
            block_processing_artifacts;
        // Send out challenges that accumulated via on_challenge.
        self.send_challenges(challenges);
        // Request any missing chunks
        self.request_missing_chunks(blocks_missing_chunks, orphans_missing_chunks);
    }

    pub fn rebroadcast_block(&mut self, block: &Block) {
        if self.rebroadcasted_blocks.get(block.hash()).is_none() {
            self.network_adapter.do_send(PeerManagerMessageRequest::NetworkRequests(
                NetworkRequests::Block { block: block.clone() },
            ));
            self.rebroadcasted_blocks.put(*block.hash(), ());
        }
    }

    pub fn process_partial_encoded_chunk_response(
        &mut self,
        response: PartialEncodedChunkResponseMsg,
        apply_chunks_done_callback: DoneApplyChunkCallback,
    ) -> Result<(), Error> {
        let header = self.shards_mgr.get_partial_encoded_chunk_header(&response.chunk_hash)?;
        let partial_chunk = PartialEncodedChunk::new(header, response.parts, response.receipts);
        // We already know the header signature is valid because we read it from the
        // shard manager.
        self.process_partial_encoded_chunk(
            MaybeValidated::from_validated(partial_chunk),
            apply_chunks_done_callback,
        )
    }

    pub fn process_partial_encoded_chunk_forward(
        &mut self,
        forward: PartialEncodedChunkForwardMsg,
        apply_chunks_done_callback: DoneApplyChunkCallback,
    ) -> Result<(), Error> {
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
                match chain_error {
                    near_chain::Error::DBNotFoundErr(_) => {
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
        self.process_partial_encoded_chunk(
            MaybeValidated::from_validated(partial_chunk),
            apply_chunks_done_callback,
        )
    }

    /// Try to process chunks in the chunk cache whose previous block hash is `prev_block_hash` and
    /// who are not marked as complete yet
    /// This function is needed because chunks in chunk cache will only be marked as complete after
    /// the previous block is accepted. So we need to check if there are any chunks can be marked as
    /// complete when a new block is accepted.
    pub fn check_incomplete_chunks(
        &mut self,
        prev_block_hash: &CryptoHash,
        apply_chunks_done_callback: DoneApplyChunkCallback,
    ) {
        for chunk_header in self.shards_mgr.get_incomplete_chunks(prev_block_hash) {
            debug!(target:"client", "try to process incomplete chunks {:?}, prev_block: {:?}", chunk_header.chunk_hash(), prev_block_hash);
            let res = self.shards_mgr.try_process_chunk_parts_and_receipts(
                &chunk_header,
                self.chain.mut_store(),
                &mut self.rs,
            );
            match res {
                Ok(res) => self.process_process_partial_encoded_chunk_result(
                    chunk_header,
                    res,
                    apply_chunks_done_callback.clone(),
                ),
                Err(err) => {
                    error!(target:"client", "unexpected error processing orphan chunk {:?}", err)
                }
            }
        }
    }

    pub fn process_partial_encoded_chunk(
        &mut self,
        partial_encoded_chunk: MaybeValidated<PartialEncodedChunk>,
        apply_chunks_done_callback: DoneApplyChunkCallback,
    ) -> Result<(), Error> {
        let chunk_hash = partial_encoded_chunk.chunk_hash();
        let pec_v2: MaybeValidated<PartialEncodedChunkV2> = partial_encoded_chunk.map(Into::into);
        let process_result = self.shards_mgr.process_partial_encoded_chunk(
            pec_v2.as_ref(),
            self.chain.head().ok().as_ref(),
            self.chain.mut_store(),
            &mut self.rs,
        )?;
        debug!(target:"client", "process partial encoded chunk {:?}, result: {:?}", chunk_hash, process_result);

        self.process_process_partial_encoded_chunk_result(
            pec_v2.into_inner().header,
            process_result,
            apply_chunks_done_callback,
        );
        Ok(())
    }

    fn process_process_partial_encoded_chunk_result(
        &mut self,
        header: ShardChunkHeader,
        process_result: ProcessPartialEncodedChunkResult,
        apply_chunks_done_callback: DoneApplyChunkCallback,
    ) {
        match process_result {
            ProcessPartialEncodedChunkResult::HaveAllPartsAndReceipts => {
                self.chain.blocks_delay_tracker.mark_chunk_completed(&header, Clock::instant());
                // We're marking chunk as accepted.
                self.chain.blocks_with_missing_chunks.accept_chunk(&header.chunk_hash());
                // If this was the last chunk that was missing for a block, it will be processed now.
                self.process_blocks_with_missing_chunks(apply_chunks_done_callback)
            }
            _ => {}
        }
    }

    pub fn sync_block_headers(
        &mut self,
        headers: Vec<BlockHeader>,
    ) -> Result<(), near_chain::Error> {
        let mut challenges = vec![];
        self.chain.sync_block_headers(headers, &mut challenges)?;
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
                Clock::instant(),
                tip.last_block_hash,
                tip.height,
                last_final_height,
            );
        }

        Ok(())
    }

    #[cfg(feature = "sandbox")]
    pub fn sandbox_update_tip(&mut self, height: BlockHeight) -> Result<(), Error> {
        let tip = self.chain.head()?;

        let last_final_hash =
            *self.chain.get_block_header(&tip.last_block_hash)?.last_final_block();
        let last_final_height = if last_final_hash == CryptoHash::default() {
            self.chain.genesis().height()
        } else {
            self.chain.get_block_header(&last_final_hash)?.height()
        };
        self.doomslug.set_tip(Clock::instant(), tip.last_block_hash, height, last_final_height);

        Ok(())
    }

    /// Gets the advanced timestamp delta in nanoseconds for sandbox once it has been fast-forwarded
    #[cfg(feature = "sandbox")]
    pub fn sandbox_delta_time(&self) -> chrono::Duration {
        let avg_block_prod_time = (self.config.min_block_production_delay.as_nanos()
            + self.config.max_block_production_delay.as_nanos())
            / 2;
        let ns = (self.accrued_fastforward_delta as u128 * avg_block_prod_time).try_into().expect(
            &format!(
                "Too high of a delta_height {} to convert into u64",
                self.accrued_fastforward_delta
            ),
        );

        chrono::Duration::nanoseconds(ns)
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
            debug!(target: "client", "Sending an approval {:?} from {} to {} for {}", approval.inner, approval.account_id, next_block_producer, approval.target_height);
            let approval_message = ApprovalMessage::new(approval, next_block_producer);
            self.network_adapter.do_send(PeerManagerMessageRequest::NetworkRequests(
                NetworkRequests::Approval { approval_message },
            ));
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
        apply_chunks_done_callback: DoneApplyChunkCallback,
    ) {
        let _span = tracing::debug_span!(
            target: "client",
            "on_block_accepted",
            ?block_hash,
            ?status,
            ?provenance)
        .entered();
        self.on_block_accepted_with_optional_chunk_produce(
            block_hash,
            status,
            provenance,
            false,
            apply_chunks_done_callback,
        );
    }

    /// Gets called when block got accepted.
    /// Only produce chunk if `skip_produce_chunk` is false
    /// Note that this function should only be directly called from tests, production code
    /// should always use `on_block_accepted`
    /// `skip_produce_chunk` is set to true to simulate when there are missing chunks in a block
    pub fn on_block_accepted_with_optional_chunk_produce(
        &mut self,
        block_hash: CryptoHash,
        status: BlockStatus,
        provenance: Provenance,
        skip_produce_chunk: bool,
        apply_chunks_done_callback: DoneApplyChunkCallback,
    ) {
        let block = match self.chain.get_block(&block_hash) {
            Ok(block) => block,
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
                .pop(&ApprovalInner::Endorsement(block_hash))
                .unwrap_or_default();
            let skips = self
                .pending_approvals
                .pop(&ApprovalInner::Skip(block.header().height()))
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

            {
                let _span = tracing::debug_span!(
                    target: "client",
                    "garbage_collection",
                    block_hash = ?block.hash(),
                    height = block.header().height())
                .entered();
                let _gc_timer = metrics::GC_TIME.start_timer();

                let result = if self.config.archive {
                    self.chain.clear_archive_data(self.config.gc.gc_blocks_limit)
                } else {
                    let tries = self.runtime_adapter.get_tries();
                    self.chain.clear_data(tries, &self.config.gc)
                };
                log_assert!(result.is_ok(), "Can't clear old data, {:?}", result);
            }

            if self.runtime_adapter.is_next_block_epoch_start(block.hash()).unwrap_or(false) {
                let next_epoch_protocol_version = unwrap_or_return!(self
                    .runtime_adapter
                    .get_epoch_protocol_version(block.header().next_epoch_id()));
                if next_epoch_protocol_version > PROTOCOL_VERSION {
                    panic!("The client protocol version is older than the protocol version of the network. Please update nearcore");
                }
            }
            // send_network_chain_info should be called whenever the chain head changes.
            // See send_network_chain_info() for more details.
            if let Err(err) = self.send_network_chain_info() {
                error!(target:"client","Failed to update network chain info: {err}");
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
                    let mut reintroduce_head = self.chain.get_block_header(&prev_head).unwrap();
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

            if provenance != Provenance::SYNC
                && !self.sync_status.is_syncing()
                && !skip_produce_chunk
            {
                // Produce new chunks
                let epoch_id = self
                    .runtime_adapter
                    .get_epoch_id_from_prev_block(block.header().hash())
                    .unwrap();
                for shard_id in 0..self.runtime_adapter.num_shards(&epoch_id).unwrap() {
                    let chunk_proposer = self
                        .runtime_adapter
                        .get_chunk_producer(&epoch_id, block.header().height() + 1, shard_id)
                        .unwrap();

                    if chunk_proposer == *validator_signer.validator_id() {
                        let _span = tracing::debug_span!(
                            target: "client",
                            "on_block_accepted_produce_chunk",
                            prev_block_hash = ?*block.hash(),
                            ?shard_id)
                        .entered();
                        let _timer = metrics::PRODUCE_AND_DISTRIBUTE_CHUNK_TIME
                            .with_label_values(&[&format!("{}", shard_id)])
                            .start_timer();
                        match self.produce_chunk(
                            *block.hash(),
                            &epoch_id,
                            Chain::get_prev_chunk_header(&*self.runtime_adapter, &block, shard_id)
                                .unwrap(),
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
                                    shard_id,
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
        self.check_incomplete_chunks(block.hash(), apply_chunks_done_callback);
    }

    pub fn request_missing_chunks(
        &mut self,
        blocks_missing_chunks: Vec<BlockMissingChunks>,
        orphans_missing_chunks: Vec<OrphanMissingChunks>,
    ) {
        let now = Clock::instant();
        for BlockMissingChunks { prev_hash, missing_chunks } in blocks_missing_chunks {
            for chunk in &missing_chunks {
                self.chain.blocks_delay_tracker.mark_chunk_requested(chunk, now);
            }
            self.shards_mgr.request_chunks(
                missing_chunks,
                prev_hash,
                &self
                    .chain
                    .header_head()
                    .expect("header_head must be available when processing a block"),
            );
        }

        for OrphanMissingChunks { missing_chunks, epoch_id, ancestor_hash } in
            orphans_missing_chunks
        {
            for chunk in &missing_chunks {
                self.chain.blocks_delay_tracker.mark_chunk_requested(chunk, now);
            }
            self.shards_mgr.request_chunks_for_orphan(
                missing_chunks,
                &epoch_id,
                ancestor_hash,
                &self
                    .chain
                    .header_head()
                    .expect("header_head must be available when processing a block"),
            );
        }
    }

    /// Check if any block with missing chunks is ready to be processed
    pub fn process_blocks_with_missing_chunks(
        &mut self,
        apply_chunks_done_callback: DoneApplyChunkCallback,
    ) {
        let me =
            self.validator_signer.as_ref().map(|validator_signer| validator_signer.validator_id());
        let mut blocks_processing_artifacts = BlockProcessingArtifact::default();
        self.chain.check_blocks_with_missing_chunks(
            &me.map(|x| x.clone()),
            &mut blocks_processing_artifacts,
            apply_chunks_done_callback,
        );
        self.process_block_processing_artifact(blocks_processing_artifacts);
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
        if let near_chain::Error::DBNotFoundErr(_) = error {
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
            let mut entry =
                self.pending_approvals.pop(&approval.inner).unwrap_or_else(|| HashMap::new());
            entry.insert(approval.account_id.clone(), (approval.clone(), approval_type));
            self.pending_approvals.put(approval.inner.clone(), entry);
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
            ApprovalInner::Endorsement(parent_hash) => *parent_hash,
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
                Err(near_chain::Error::NotAValidator) => {
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
        self.doomslug.on_approval_message(Clock::instant(), approval, &block_producer_stakes);
    }

    /// Forwards given transaction to upcoming validators.
    fn forward_tx(&self, epoch_id: &EpochId, tx: &SignedTransaction) -> Result<(), Error> {
        let shard_id =
            self.runtime_adapter.account_id_to_shard_id(&tx.transaction.signer_id, epoch_id)?;
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
                let next_shard_id = self
                    .runtime_adapter
                    .account_id_to_shard_id(&tx.transaction.signer_id, next_epoch_id)?;
                let validator = self.chain.find_chunk_producer_for_forwarding(
                    next_epoch_id,
                    next_shard_id,
                    horizon,
                )?;
                validators.insert(validator);
            }
        }

        if let Some(account_id) = self.validator_signer.as_ref().map(|bp| bp.validator_id()) {
            validators.remove(account_id);
        }
        for validator in validators {
            trace!(target: "client",
                   "I'm {:?}, routing a transaction {:?} to {}, shard_id = {}",
                   self.validator_signer.as_ref().map(|bp| bp.validator_id()),
                   tx,
                   validator,
                   shard_id
            );

            // Send message to network to actually forward transaction.
            self.network_adapter.do_send(PeerManagerMessageRequest::NetworkRequests(
                NetworkRequests::ForwardTx(validator, tx.clone()),
            ));
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
        } else {
            self.forward_tx(&head.epoch_id, tx)?;
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
        let cur_block_header = self.chain.head_header()?;
        let transaction_validity_period = self.chain.transaction_validity_period;
        // here it is fine to use `cur_block_header` as it is a best effort estimate. If the transaction
        // were to be included, the block that the chunk points to will have height >= height of
        // `cur_block_header`.
        if let Err(e) = self.chain.store().check_transaction_validity_period(
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
            .validate_tx(gas_price, None, tx, true, &epoch_id, protocol_version)
            .expect("no storage errors")
        {
            debug!(target: "client", "Invalid tx during basic validation: {:?}", err);
            return Ok(NetworkClientResponses::InvalidTx(err));
        }

        let shard_id =
            self.runtime_adapter.account_id_to_shard_id(&tx.transaction.signer_id, &epoch_id)?;
        if self.runtime_adapter.cares_about_shard(me, &head.last_block_hash, shard_id, true)
            || self.runtime_adapter.will_care_about_shard(me, &head.last_block_hash, shard_id, true)
        {
            let shard_uid = self.runtime_adapter.shard_id_to_uid(shard_id, &epoch_id)?;
            let state_root = match self.chain.get_chunk_extra(&head.last_block_hash, &shard_uid) {
                Ok(chunk_extra) => *chunk_extra.state_root(),
                Err(_) => {
                    // Not being able to fetch a state root most likely implies that we haven't
                    //     caught up with the next epoch yet.
                    if is_forwarded {
                        return Err(Error::Other("Node has not caught up yet".to_string()));
                    } else {
                        self.forward_tx(&epoch_id, tx)?;
                        return Ok(NetworkClientResponses::RequestRouted);
                    }
                }
            };
            if let Some(err) = self
                .runtime_adapter
                .validate_tx(gas_price, Some(state_root), tx, false, &epoch_id, protocol_version)
                .expect("no storage errors")
            {
                debug!(target: "client", "Invalid tx: {:?}", err);
                Ok(NetworkClientResponses::InvalidTx(err))
            } else if check_only {
                Ok(NetworkClientResponses::ValidTx)
            } else {
                let active_validator = self.active_validator(shard_id)?;

                // TODO #6713: Transactions don't need to be recorded if the node is not a validator
                // for the shard.
                // If I'm not an active validator I should forward tx to next validators.
                self.shards_mgr.insert_transaction(shard_id, tx.clone());
                trace!(target: "client", shard_id, "Recorded a transaction.");

                // Active validator:
                //   possibly forward to next epoch validators
                // Not active validator:
                //   forward to current epoch validators,
                //   possibly forward to next epoch validators
                if active_validator {
                    trace!(target: "client", account = ?me, shard_id, is_forwarded, "Recording a transaction.");
                    metrics::TRANSACTION_RECEIVED_VALIDATOR.inc();

                    if !is_forwarded {
                        self.possibly_forward_tx_to_next_epoch(tx)?;
                    }
                    Ok(NetworkClientResponses::ValidTx)
                } else if !is_forwarded {
                    trace!(target: "client", shard_id, "Forwarding a transaction.");
                    metrics::TRANSACTION_RECEIVED_NON_VALIDATOR.inc();
                    self.forward_tx(&epoch_id, tx)?;
                    Ok(NetworkClientResponses::RequestRouted)
                } else {
                    trace!(target: "client", shard_id, "Non-validator received a forwarded transaction, dropping it.");
                    metrics::TRANSACTION_RECEIVED_NON_VALIDATOR_FORWARDED.inc();
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
        state_parts_task_scheduler: &dyn Fn(ApplyStatePartsRequest),
        block_catch_up_task_scheduler: &dyn Fn(BlockCatchUpRequest),
        state_split_scheduler: &dyn Fn(StateSplitRequest),
        apply_chunks_done_callback: DoneApplyChunkCallback,
    ) -> Result<(), Error> {
        let me = &self.validator_signer.as_ref().map(|x| x.validator_id().clone());
        for (sync_hash, state_sync_info) in self.chain.store().iterate_state_sync_infos()? {
            assert_eq!(sync_hash, state_sync_info.epoch_tail_hash);
            let network_adapter1 = self.network_adapter.clone();

            let new_shard_sync = {
                let prev_hash = *self.chain.get_block(&sync_hash)?.header().prev_hash();
                let need_to_split_states =
                    self.runtime_adapter.will_shard_layout_change_next_epoch(&prev_hash)?;
                if need_to_split_states {
                    // If the client already has the state for this epoch, skip the downloading phase
                    let new_shard_sync = state_sync_info
                        .shards
                        .iter()
                        .filter_map(|ShardInfo(shard_id, _)| {
                            let shard_id = *shard_id;
                            if self.runtime_adapter.cares_about_shard(
                                me.as_ref(),
                                &prev_hash,
                                shard_id,
                                true,
                            ) {
                                Some((
                                    shard_id,
                                    ShardSyncDownload {
                                        downloads: vec![],
                                        status: ShardSyncStatus::StateSplitScheduling,
                                    },
                                ))
                            } else {
                                None
                            }
                        })
                        .collect();
                    debug!(target: "catchup", "need to split states for shards {:?}", new_shard_sync);
                    new_shard_sync
                } else {
                    debug!(target: "catchup", "do not need to split states for shards");
                    HashMap::new()
                }
            };
            let state_sync_timeout = self.config.state_sync_timeout;
            let epoch_id = self.chain.get_block(&sync_hash)?.header().epoch_id().clone();
            let (state_sync, new_shard_sync, blocks_catch_up_state) =
                self.catchup_state_syncs.entry(sync_hash).or_insert_with(|| {
                    (
                        StateSync::new(network_adapter1, state_sync_timeout),
                        new_shard_sync,
                        BlocksCatchUpState::new(sync_hash, epoch_id),
                    )
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
                state_parts_task_scheduler,
                state_split_scheduler,
            )? {
                StateSyncResult::Unchanged => {}
                StateSyncResult::Changed(fetch_block) => {
                    debug!(target:"catchup", "state sync finished but waiting to fetch block");
                    assert!(!fetch_block);
                }
                StateSyncResult::Completed => {
                    debug!(target:"catchup", "state sync completed now catch up blocks");
                    self.chain.catchup_blocks_step(
                        me,
                        &sync_hash,
                        blocks_catch_up_state,
                        block_catch_up_task_scheduler,
                    )?;

                    if blocks_catch_up_state.is_finished() {
                        let mut block_processing_artifacts = BlockProcessingArtifact::default();

                        self.chain.finish_catchup_blocks(
                            me,
                            &sync_hash,
                            &mut block_processing_artifacts,
                            apply_chunks_done_callback.clone(),
                            &blocks_catch_up_state.done_blocks,
                        )?;

                        self.process_block_processing_artifact(block_processing_artifacts);
                    }
                }
            }
        }

        Ok(())
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

impl Client {
    /// Each epoch defines a set of important accounts: block producers, chunk producers,
    /// approvers. Low-latency reliable communication between those accounts is critical,
    /// so that the blocks can be produced on time. This function computes the set of
    /// important accounts (aka TIER1 accounts) so that it can be fed to PeerManager, which
    /// will take care of the traffic prioritization.
    ///
    /// It returns both TIER1 accounts for both current epoch (according to the `tip`)
    /// and the next epoch, so that the PeerManager can establish the priority connections
    /// in advance (before the epoch starts and they are actually needed).
    ///
    /// The result of the last call to get_tier1_accounts() is cached, so that it is not recomputed
    /// if the current epoch didn't change since the last call. In particular SetChainInfo is send
    /// after every block is processed (order of seconds), while the epoch changes way less
    /// frequently (order of hours).
    fn get_tier1_accounts(&mut self, tip: &Tip) -> Result<Arc<AccountKeys>, Error> {
        match &self.tier1_accounts_cache {
            Some(it) if it.0 == tip.epoch_id => return Ok(it.1.clone()),
            _ => {}
        }
        let info = self
            .runtime_adapter
            .get_validator_info(ValidatorInfoIdentifier::BlockHash(tip.last_block_hash))?;
        let mut accounts = HashMap::new();
        accounts.extend(
            info.current_validators
                .into_iter()
                .map(|v| ((tip.epoch_id.clone(), v.account_id), v.public_key)),
        );
        accounts.extend(
            info.next_validators
                .into_iter()
                .map(|v| ((tip.next_epoch_id.clone(), v.account_id), v.public_key)),
        );
        let accounts = Arc::new(accounts);
        self.tier1_accounts_cache = Some((tip.epoch_id.clone(), accounts.clone()));
        Ok(accounts)
    }

    /// send_network_chain_info sends ChainInfo to PeerManagerActor.
    /// ChainInfo contains chain information relevant to p2p networking.
    /// It is expected to be called every time the head of the chain changes (or more often).
    /// Subsequent calls will probably re-send to PeerManagerActor a lot of redundant
    /// information (for example epoch-related data changes way less often than chain head
    /// changes), but that's fine - we avoid recomputing rarely changing data in ChainInfo by caching it.
    /// The condition to call this function is simple - every time chain head changes -
    /// which hopefully will make it hard to forget to call it. And even if there is some
    /// corner case not covered - since blocks are sent frequently (every few seconds),
    /// the POV of Client and PeerManagerActor will be desynchronized only for a short time.
    ///
    /// TODO(gprusak): consider making send_network_chain_info accept chain Tip as an argument
    /// to underline that it is expected to be called whenever Tip changes. Currently
    /// self.chain.head() is fallible for some reason, so calling it at the
    /// send_network_chain_info() call site would be ugly (we just log the error).
    /// In theory we should already have the tip at the call-site, eg from
    /// check_And_update_doomslug_tip, but that would require a bigger refactor.
    fn send_network_chain_info(&mut self) -> Result<(), Error> {
        let tip = self.chain.head()?;
        // convert config tracked shards
        // runtime will track all shards if config tracked shards is not empty
        // https://github.com/near/nearcore/issues/4930
        let tracked_shards = if self.config.tracked_shards.is_empty() {
            vec![]
        } else {
            let num_shards = self.runtime_adapter.num_shards(&tip.epoch_id)?;
            (0..num_shards).collect()
        };
        let tier1_accounts = self.get_tier1_accounts(&tip)?;
        self.network_adapter.do_send(SetChainInfo(ChainInfo {
            height: tip.height,
            tracked_shards,
            tier1_accounts,
        }));
        Ok(())
    }
}
