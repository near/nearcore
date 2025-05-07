//! Client is responsible for tracking the chain, blocks, chunks, and producing
//! them when needed.
//! This client works completely synchronously and must be operated by some async actor outside.

use crate::chunk_distribution_network::{ChunkDistributionClient, ChunkDistributionNetwork};
use crate::chunk_inclusion_tracker::ChunkInclusionTracker;
use crate::chunk_producer::ChunkProducer;
use crate::client_actor::ClientSenderForClient;
use crate::debug::BlockProductionTracker;
use crate::stateless_validation::chunk_endorsement::ChunkEndorsementTracker;
use crate::stateless_validation::chunk_validator::ChunkValidator;
use crate::stateless_validation::partial_witness::partial_witness_actor::PartialWitnessSenderForClient;
use crate::sync::block::BlockSync;
use crate::sync::epoch::EpochSync;
use crate::sync::handler::SyncHandler;
use crate::sync::header::HeaderSync;
use crate::sync::state::chain_requests::ChainSenderForStateSync;
use crate::sync::state::{StateSync, StateSyncResult};
use crate::{ProduceChunkResult, metrics};
use itertools::Itertools;
use near_async::futures::{AsyncComputationSpawner, FutureSpawner};
use near_async::messaging::IntoSender;
use near_async::messaging::{CanSend, Sender};
use near_async::time::{Clock, Duration, Instant};
use near_chain::chain::{
    ApplyChunksDoneMessage, BlockCatchUpRequest, BlockMissingChunks, BlocksCatchUpState,
    VerifyBlockHashAndSignatureResult,
};
use near_chain::orphan::OrphanMissingChunks;
use near_chain::resharding::types::ReshardingSender;
use near_chain::state_snapshot_actor::SnapshotCallbacks;
use near_chain::test_utils::format_hash;
use near_chain::types::{ChainConfig, LatestKnown, RuntimeAdapter};
use near_chain::{
    BlockProcessingArtifact, BlockStatus, Chain, ChainGenesis, ChainStoreAccess, Doomslug,
    DoomslugThresholdMode, Provenance,
};
use near_chain_configs::{ClientConfig, MutableValidatorSigner, UpdatableClientConfig};
use near_chunks::adapter::ShardsManagerRequestFromClient;
use near_chunks::logic::{create_partial_chunk, persist_chunk};
use near_client_primitives::types::{Error, StateSyncStatus, SyncStatus};
use near_epoch_manager::EpochManagerAdapter;
use near_epoch_manager::shard_assignment::shard_id_to_uid;
use near_epoch_manager::shard_tracker::ShardTracker;
use near_network::types::{AccountKeys, ChainInfo, PeerManagerMessageRequest, SetChainInfo};
use near_network::types::{
    HighestHeightPeerInfo, NetworkRequests, PeerManagerAdapter, ReasonForBan,
};
use near_primitives::block::{Approval, ApprovalInner, ApprovalMessage, Block, BlockHeader, Tip};
use near_primitives::block_header::ApprovalType;
use near_primitives::epoch_info::RngSeed;
use near_primitives::errors::EpochError;
use near_primitives::hash::CryptoHash;
use near_primitives::merkle::{MerklePath, PartialMerkleTree};
use near_primitives::network::PeerId;
use near_primitives::optimistic_block::OptimisticBlock;
use near_primitives::receipt::Receipt;
use near_primitives::sharding::{
    EncodedShardChunk, PartialEncodedChunk, ShardChunk, ShardChunkHeader, ShardChunkWithEncoding,
    StateSyncInfo, StateSyncInfoV1,
};
use near_primitives::stateless_validation::ChunkProductionKey;
use near_primitives::transaction::ValidatedTransaction;
use near_primitives::types::{AccountId, ApprovalStake, BlockHeight, EpochId, NumBlocks};
use near_primitives::unwrap_or_return;
use near_primitives::upgrade_schedule::ProtocolUpgradeVotingSchedule;
use near_primitives::utils::MaybeValidated;
use near_primitives::validator_signer::ValidatorSigner;
use near_primitives::version::PROTOCOL_VERSION;
use near_primitives::views::{CatchupStatusView, DroppedReason};
use parking_lot::Mutex;
use std::cmp::max;
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::Arc;
use tracing::{debug, debug_span, error, info, warn};

#[cfg(feature = "test_features")]
use crate::chunk_producer::AdvProduceChunksMode;

const NUM_REBROADCAST_BLOCKS: usize = 30;

/// Drop blocks whose height are beyond head + horizon if it is not in the current epoch.
const BLOCK_HORIZON: u64 = 500;

/// number of blocks at the epoch start for which we will log more detailed info
pub const EPOCH_START_INFO_BLOCKS: u64 = 500;

/// Defines whether in case of adversarial block production invalid blocks can
/// be produced.
#[cfg(feature = "test_features")]
#[derive(PartialEq, Eq)]
pub enum AdvProduceBlocksMode {
    All,
    OnlyValid,
}

/// The state associated with downloading state for a shard this node will track in the
/// future but does not currently.
pub struct CatchupState {
    /// Manages downloading the state.
    pub state_sync: StateSync,
    /// Keeps track of state downloads, and gets passed to `state_sync`.
    pub sync_status: StateSyncStatus,
    /// Manages going back to apply chunks after state has been downloaded.
    pub catchup: BlocksCatchUpState,
}

pub struct Client {
    /// Adversarial controls - should be enabled only to test disruptive
    /// behavior on chain.
    #[cfg(feature = "test_features")]
    pub adv_produce_blocks: Option<AdvProduceBlocksMode>,

    /// Fast Forward accrued delta height used to calculate fast forwarded timestamps for each block.
    #[cfg(feature = "sandbox")]
    pub(crate) accrued_fastforward_delta: near_primitives::types::BlockHeightDelta,

    pub clock: Clock,
    pub config: ClientConfig,
    pub chain: Chain,
    pub doomslug: Doomslug,
    pub epoch_manager: Arc<dyn EpochManagerAdapter>,
    pub shard_tracker: ShardTracker,
    pub runtime_adapter: Arc<dyn RuntimeAdapter>,
    pub shards_manager_adapter: Sender<ShardsManagerRequestFromClient>,
    /// Network adapter.
    pub network_adapter: PeerManagerAdapter,
    /// Signer for block producer (if present). This field is mutable and optional. Use with caution!
    /// Lock the value of mutable validator signer for the duration of a request to ensure consistency.
    /// Please note that the locked value should not be stored anywhere or passed through the thread boundary.
    pub validator_signer: MutableValidatorSigner,
    /// Approvals for which we do not have the block yet
    pub pending_approvals:
        lru::LruCache<ApprovalInner, HashMap<AccountId, (Approval, ApprovalType)>>,
    /// Handles syncing chain to the actual state of the network.
    pub sync_handler: SyncHandler,
    /// A mapping from a block for which a state sync is underway for the next epoch, and the object
    /// storing the current status of the state sync and blocks catch up
    pub catchup_state_syncs: HashMap<CryptoHash, CatchupState>,
    /// Spawns async tasks for catchup state sync.
    state_sync_future_spawner: Arc<dyn FutureSpawner>,
    /// Sender for catchup state sync requests.
    chain_sender_for_state_sync: ChainSenderForStateSync,
    // Sender to be able to send a message to myself.
    pub myself_sender: ClientSenderForClient,
    /// Blocks that have been re-broadcast recently. They should not be broadcast again.
    rebroadcasted_blocks: lru::LruCache<CryptoHash, ()>,
    /// Last time the head was updated, or our head was rebroadcasted. Used to re-broadcast the head
    /// again to prevent network from stalling if a large percentage of the network missed a block
    last_time_head_progress_made: Instant,
    /// Block production timing information. Used only for debug purposes.
    /// Stores approval information and production time of the block
    pub block_production_info: BlockProductionTracker,
    /// Cached precomputed set of TIER1 accounts.
    /// See send_network_chain_info().
    tier1_accounts_cache: Option<(EpochId, Arc<AccountKeys>)>,
    /// Resharding sender.
    pub resharding_sender: ReshardingSender,
    /// Helper module for handling chunk production.
    pub chunk_producer: ChunkProducer,
    /// Helper module for stateless validation functionality like chunk witness production, validation
    /// chunk endorsements tracking etc.
    pub chunk_validator: ChunkValidator,
    /// Tracks current chunks that are ready to be included in block
    /// Also tracks banned chunk producers and filters out chunks produced by them
    pub chunk_inclusion_tracker: ChunkInclusionTracker,
    /// Tracks chunk endorsements received from chunk validators. Used to filter out chunks ready for inclusion
    pub chunk_endorsement_tracker: Arc<Mutex<ChunkEndorsementTracker>>,
    /// Adapter to send request to partial_witness_actor to distribute state witness.
    pub partial_witness_adapter: PartialWitnessSenderForClient,
    // Optional value used for the Chunk Distribution Network Feature.
    chunk_distribution_network: Option<ChunkDistributionNetwork>,
    /// Upgrade schedule which determines when the client starts voting for new protocol versions.
    upgrade_schedule: ProtocolUpgradeVotingSchedule,
    /// Produced optimistic block.
    last_optimistic_block_produced: Option<OptimisticBlock>,
}

impl AsRef<Client> for Client {
    fn as_ref(&self) -> &Client {
        self
    }
}

impl Client {
    pub(crate) fn update_client_config(&self, update_client_config: UpdatableClientConfig) -> bool {
        let mut is_updated = false;
        is_updated |= self.config.expected_shutdown.update(update_client_config.expected_shutdown);
        is_updated |= self.config.resharding_config.update(update_client_config.resharding_config);
        is_updated |= self
            .config
            .produce_chunk_add_transactions_time_limit
            .update(update_client_config.produce_chunk_add_transactions_time_limit);
        is_updated
    }

    /// Updates client's mutable validator signer.
    /// It will update all validator signers that synchronize with it.
    pub(crate) fn update_validator_signer(&self, signer: Option<Arc<ValidatorSigner>>) -> bool {
        self.validator_signer.update(signer)
    }
}

impl Client {
    pub fn new(
        clock: Clock,
        config: ClientConfig,
        chain_genesis: ChainGenesis,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        shard_tracker: ShardTracker,
        runtime_adapter: Arc<dyn RuntimeAdapter>,
        network_adapter: PeerManagerAdapter,
        shards_manager_sender: Sender<ShardsManagerRequestFromClient>,
        validator_signer: MutableValidatorSigner,
        enable_doomslug: bool,
        rng_seed: RngSeed,
        snapshot_callbacks: Option<SnapshotCallbacks>,
        async_computation_spawner: Arc<dyn AsyncComputationSpawner>,
        partial_witness_adapter: PartialWitnessSenderForClient,
        resharding_sender: ReshardingSender,
        state_sync_future_spawner: Arc<dyn FutureSpawner>,
        chain_sender_for_state_sync: ChainSenderForStateSync,
        myself_sender: ClientSenderForClient,
        upgrade_schedule: ProtocolUpgradeVotingSchedule,
    ) -> Result<Self, Error> {
        let doomslug_threshold_mode = if enable_doomslug {
            DoomslugThresholdMode::TwoThirds
        } else {
            DoomslugThresholdMode::NoApprovals
        };
        let chain_config = ChainConfig {
            save_trie_changes: config.save_trie_changes,
            background_migration_threads: config.client_background_migration_threads,
            resharding_config: config.resharding_config.clone(),
        };
        let chain = Chain::new(
            clock.clone(),
            epoch_manager.clone(),
            shard_tracker.clone(),
            runtime_adapter.clone(),
            &chain_genesis,
            doomslug_threshold_mode,
            chain_config,
            snapshot_callbacks,
            async_computation_spawner.clone(),
            validator_signer.clone(),
            resharding_sender.clone(),
        )?;
        chain.init_flat_storage()?;
        let epoch_sync = EpochSync::new(
            clock.clone(),
            network_adapter.clone(),
            chain.genesis().clone(),
            async_computation_spawner.clone(),
            config.epoch_sync.clone(),
            &chain.chain_store.store(),
        );
        let header_sync = HeaderSync::new(
            clock.clone(),
            network_adapter.clone(),
            config.header_sync_initial_timeout,
            config.header_sync_progress_timeout,
            config.header_sync_stall_ban_timeout,
            config.header_sync_expected_height_per_second,
            config.expected_shutdown.clone(),
        );
        let block_sync = BlockSync::new(
            clock.clone(),
            network_adapter.clone(),
            config.block_fetch_horizon,
            config.archive,
            config.state_sync_enabled,
        );

        let state_sync = StateSync::new(
            clock.clone(),
            runtime_adapter.store().clone(),
            epoch_manager.clone(),
            runtime_adapter.clone(),
            network_adapter.clone().into_sender(),
            config.state_sync_external_timeout,
            config.state_sync_p2p_timeout,
            config.state_sync_retry_backoff,
            config.state_sync_external_backoff,
            &config.chain_id,
            &config.state_sync.sync,
            chain_sender_for_state_sync.clone(),
            state_sync_future_spawner.clone(),
            false,
        );
        let num_block_producer_seats = config.num_block_producer_seats as usize;

        let doomslug = Doomslug::new(
            clock.clone(),
            chain.chain_store().largest_target_height()?,
            config.min_block_production_delay,
            config.max_block_production_delay,
            config.max_block_production_delay / 10,
            config.max_block_wait_delay,
            config.chunk_wait_mult,
            doomslug_threshold_mode,
        );
        let chunk_endorsement_tracker = Arc::new(Mutex::new(ChunkEndorsementTracker::new(
            epoch_manager.clone(),
            chain.chain_store().store(),
        )));
        let chunk_producer = ChunkProducer::new(
            clock.clone(),
            config.produce_chunk_add_transactions_time_limit.clone(),
            &chain.chain_store(),
            epoch_manager.clone(),
            runtime_adapter.clone(),
            rng_seed,
            config.transaction_pool_size_limit,
        );
        let chunk_validator = ChunkValidator::new(
            epoch_manager.clone(),
            network_adapter.clone().into_sender(),
            runtime_adapter.clone(),
            config.orphan_state_witness_pool_size,
            async_computation_spawner,
        );
        let chunk_distribution_network = ChunkDistributionNetwork::from_config(&config);
        Ok(Self {
            #[cfg(feature = "test_features")]
            adv_produce_blocks: None,
            #[cfg(feature = "sandbox")]
            accrued_fastforward_delta: 0,
            clock: clock.clone(),
            config: config.clone(),
            chain,
            doomslug,
            epoch_manager,
            shard_tracker,
            runtime_adapter,
            shards_manager_adapter: shards_manager_sender,
            network_adapter,
            validator_signer,
            pending_approvals: lru::LruCache::new(
                NonZeroUsize::new(num_block_producer_seats).unwrap(),
            ),
            sync_handler: SyncHandler::new(
                clock.clone(),
                config,
                epoch_sync,
                header_sync,
                state_sync,
                block_sync,
            ),
            catchup_state_syncs: HashMap::new(),
            state_sync_future_spawner,
            chain_sender_for_state_sync,
            myself_sender,
            rebroadcasted_blocks: lru::LruCache::new(
                NonZeroUsize::new(NUM_REBROADCAST_BLOCKS).unwrap(),
            ),
            last_time_head_progress_made: clock.now(),
            block_production_info: BlockProductionTracker::new(),
            tier1_accounts_cache: None,
            resharding_sender,
            chunk_producer,
            chunk_validator,
            chunk_inclusion_tracker: ChunkInclusionTracker::new(),
            chunk_endorsement_tracker,
            partial_witness_adapter,
            chunk_distribution_network,
            upgrade_schedule,
            last_optimistic_block_produced: None,
        })
    }

    // Checks if it's been at least `stall_timeout` since the last time the head was updated, or
    // this method was called. If yes, rebroadcasts the current head.
    pub fn check_head_progress_stalled(&mut self, stall_timeout: Duration) -> Result<(), Error> {
        if self.clock.now() > self.last_time_head_progress_made + stall_timeout
            && !self.sync_handler.sync_status.is_syncing()
        {
            let block = self.chain.get_block(&self.chain.head()?.last_block_hash)?;
            self.network_adapter.send(PeerManagerMessageRequest::NetworkRequests(
                NetworkRequests::Block { block: block },
            ));
            self.last_time_head_progress_made = self.clock.now();
        }
        Ok(())
    }

    pub fn remove_transactions_for_block(
        &mut self,
        me: &AccountId,
        block: &Block,
    ) -> Result<(), Error> {
        let epoch_id = self.epoch_manager.get_epoch_id(block.hash())?;
        let shard_layout = self.epoch_manager.get_shard_layout(&epoch_id)?;
        for (shard_index, chunk_header) in block.chunks().iter_deprecated().enumerate() {
            let shard_id = shard_layout.get_shard_id(shard_index);
            let shard_id = shard_id.map_err(Into::<EpochError>::into)?;
            let shard_uid = shard_id_to_uid(self.epoch_manager.as_ref(), shard_id, &epoch_id)?;
            if block.header().height() == chunk_header.height_included() {
                if self.shard_tracker.cares_about_shard_this_or_next_epoch(
                    Some(me),
                    block.header().prev_hash(),
                    shard_id,
                    true,
                ) {
                    // By now the chunk must be in store, otherwise the block would have been orphaned
                    let chunk = self.chain.get_chunk(&chunk_header.chunk_hash()).unwrap();
                    let transactions = chunk.to_transactions();
                    let mut pool_guard = self.chunk_producer.sharded_tx_pool.lock();
                    pool_guard.remove_transactions(shard_uid, transactions);
                }
            }
        }
        Ok(())
    }

    pub fn reintroduce_transactions_for_block(
        &mut self,
        me: &AccountId,
        block: &Block,
    ) -> Result<(), Error> {
        let epoch_id = self.epoch_manager.get_epoch_id(block.hash())?;
        let protocol_version = self.epoch_manager.get_epoch_protocol_version(&epoch_id)?;
        let shard_layout =
            self.epoch_manager.get_shard_layout_from_protocol_version(protocol_version);
        let config = self.runtime_adapter.get_runtime_config(protocol_version);

        for (shard_index, chunk_header) in block.chunks().iter_deprecated().enumerate() {
            let shard_id = shard_layout.get_shard_id(shard_index);
            let shard_id = shard_id.map_err(Into::<EpochError>::into)?;
            let shard_uid = shard_id_to_uid(self.epoch_manager.as_ref(), shard_id, &epoch_id)?;

            if block.header().height() == chunk_header.height_included() {
                if self.shard_tracker.cares_about_shard_this_or_next_epoch(
                    Some(me),
                    block.header().prev_hash(),
                    shard_id,
                    false,
                ) {
                    // By now the chunk must be in store, otherwise the block would have been orphaned
                    let chunk = self.chain.get_chunk(&chunk_header.chunk_hash()).unwrap();

                    let validated_txs = chunk
                        .to_transactions()
                        .into_iter()
                        .cloned()
                        .filter_map(|signed_tx| {
                            match ValidatedTransaction::new(&config, signed_tx) {
                                Ok(validated_tx) => Some(validated_tx),
                                Err((err, signed_tx)) => {
                                    debug!(
                                        target: "client",
                                        "Validating signed tx ({:?}) failed with error {:?}",
                                        signed_tx,
                                        err
                                    );
                                    None
                                }
                            }
                        })
                        .collect::<Vec<_>>();

                    let reintroduced_count = {
                        let mut pool_guard = self.chunk_producer.sharded_tx_pool.lock();
                        pool_guard.reintroduce_transactions(shard_uid, validated_txs)
                    };

                    if reintroduced_count < chunk.to_transactions().len() {
                        debug!(target: "client",
                            reintroduced_count,
                            num_tx = chunk.to_transactions().len(),
                            "Reintroduced transactions");
                    }
                }
            }
        }
        Ok(())
    }

    /// Checks couple conditions whether Client can produce new block on height
    /// `height` on top of block with `prev_header`.
    /// Needed to skip several checks in case of adversarial controls enabled.
    /// TODO: consider returning `Result<(), Error>` as `Ok(false)` looks like
    /// faulty logic.
    fn can_produce_block(
        &self,
        prev_header: &BlockHeader,
        height: BlockHeight,
        account_id: &AccountId,
        next_block_proposer: &AccountId,
    ) -> Result<bool, Error> {
        #[cfg(feature = "test_features")]
        {
            if self.adv_produce_blocks == Some(AdvProduceBlocksMode::All) {
                return Ok(true);
            }
        }

        // If we are not block proposer, skip block production.
        if account_id != next_block_proposer {
            info!(target: "client", height, "Skipping block production, not block producer for next block.");
            return Ok(false);
        }

        #[cfg(feature = "test_features")]
        {
            if self.adv_produce_blocks == Some(AdvProduceBlocksMode::OnlyValid) {
                return Ok(true);
            }
        }

        // If height is known already, don't produce new block for this height.
        let known_height = self.chain.chain_store().get_latest_known()?.height;
        if height <= known_height {
            return Ok(false);
        }

        // If we are to start new epoch with this block, check if the previous
        // block is caught up. If it is not the case, we wouldn't be able to
        // apply the following block, so we also skip block production.
        let prev_hash = prev_header.hash();
        if self.epoch_manager.is_next_block_epoch_start(prev_hash)? {
            let prev_prev_hash = prev_header.prev_hash();
            if !self.chain.prev_block_is_caught_up(prev_prev_hash, prev_hash)? {
                debug!(target: "client", height, "Skipping block production, prev block is not caught up");
                return Ok(false);
            }
        }

        Ok(true)
    }

    fn pre_block_production_check(
        &self,
        prev_header: &BlockHeader,
        height: BlockHeight,
        validator_signer: &Arc<ValidatorSigner>,
    ) -> Result<(), Error> {
        // Check that we are were called at the block that we are producer for.
        let epoch_id =
            self.epoch_manager.get_epoch_id_from_prev_block(&prev_header.hash()).unwrap();
        let next_block_proposer = self.epoch_manager.get_block_producer(&epoch_id, height)?;

        let protocol_version = self
            .epoch_manager
            .get_epoch_protocol_version(&epoch_id)
            .expect("Epoch info should be ready at this point");
        if protocol_version > PROTOCOL_VERSION {
            panic!(
                "The client protocol version is older than the protocol version of the network. Please update nearcore. Client protocol version:{}, network protocol version {}",
                PROTOCOL_VERSION, protocol_version
            );
        }

        if !self.can_produce_block(
            &prev_header,
            height,
            validator_signer.validator_id(),
            &next_block_proposer,
        )? {
            debug!(target: "client", me=?validator_signer.validator_id(), ?next_block_proposer, "Should reschedule block");
            return Err(Error::BlockProducer("Should reschedule".to_string()));
        }

        let validator_stake =
            self.epoch_manager.get_validator_by_account_id(&epoch_id, &next_block_proposer)?;

        let validator_pk = validator_stake.take_public_key();
        if validator_pk != validator_signer.public_key() {
            debug!(target: "client",
                local_validator_key = ?validator_signer.public_key(),
                ?validator_pk,
                "Local validator key does not match expected validator key, skipping optimistic block production");
            let err = Error::BlockProducer("Local validator key mismatch".to_string());
            #[cfg(not(feature = "test_features"))]
            return Err(err);
            #[cfg(feature = "test_features")]
            match self.adv_produce_blocks {
                None | Some(AdvProduceBlocksMode::OnlyValid) => return Err(err),
                Some(AdvProduceBlocksMode::All) => {}
            }
        }
        Ok(())
    }

    pub fn is_optimistic_block_done(&self, next_height: BlockHeight) -> bool {
        self.last_optimistic_block_produced
            .as_ref()
            .filter(|ob| ob.inner.block_height == next_height)
            .is_some()
    }

    pub fn save_optimistic_block(&mut self, optimistic_block: &OptimisticBlock) {
        if let Some(old_block) = self.last_optimistic_block_produced.as_ref() {
            if old_block.inner.block_height == optimistic_block.inner.block_height {
                warn!(target: "client",
                    height=old_block.inner.block_height,
                    old_previous_hash=?old_block.inner.prev_block_hash,
                    new_previous_hash=?optimistic_block.inner.prev_block_hash,
                    "Optimistic block already exists, replacing");
            }
        }
        self.last_optimistic_block_produced = Some(optimistic_block.clone());
    }

    /// Produce optimistic block for given `height` on top of chain head.
    /// Either returns optimistic block or error.
    pub fn produce_optimistic_block_on_head(
        &mut self,
        height: BlockHeight,
    ) -> Result<Option<OptimisticBlock>, Error> {
        let _span =
            tracing::debug_span!(target: "client", "produce_optimistic_block_on_head", height)
                .entered();

        let head = self.chain.head()?;
        assert_eq!(
            head.epoch_id,
            self.epoch_manager.get_epoch_id_from_prev_block(&head.prev_block_hash).unwrap()
        );

        let prev_hash = head.last_block_hash;
        let prev_header = self.chain.get_block_header(&prev_hash)?;

        let validator_signer: Arc<ValidatorSigner> =
            self.validator_signer.get().ok_or_else(|| {
                Error::BlockProducer("Called without block producer info.".to_string())
            })?;

        if let Err(err) = self.pre_block_production_check(&prev_header, height, &validator_signer) {
            debug!(target: "client", height, ?err, "Skipping optimistic block production.");
            return Ok(None);
        }

        debug!(
            target: "client",
            validator=?validator_signer.validator_id(),
            height=height,
            prev_height=prev_header.height(),
            prev_hash=format_hash(prev_hash),
            "Producing optimistic block",
        );

        #[cfg(feature = "sandbox")]
        let sandbox_delta_time = Some(self.sandbox_delta_time());
        #[cfg(not(feature = "sandbox"))]
        let sandbox_delta_time = None;

        // TODO(#10584): Add debug information about the block production in self.block_production_info

        let optimistic_block = OptimisticBlock::produce(
            &prev_header,
            height,
            &*validator_signer,
            self.clock.now_utc().unix_timestamp_nanos() as u64,
            sandbox_delta_time,
        );

        metrics::OPTIMISTIC_BLOCK_PRODUCED_TOTAL.inc();

        Ok(Some(optimistic_block))
    }

    /// Produce block if we are block producer for given block `height`.
    /// Either returns produced block (not applied) or error.
    pub fn produce_block(&mut self, height: BlockHeight) -> Result<Option<Block>, Error> {
        self.produce_block_on_head(height, true)
    }

    /// Produce block for given `height` on top of chain head.
    /// Either returns produced block (not applied) or error.
    pub fn produce_block_on_head(
        &mut self,
        height: BlockHeight,
        prepare_chunk_headers: bool,
    ) -> Result<Option<Block>, Error> {
        let _span =
            tracing::debug_span!(target: "client", "produce_block_on_head", height).entered();

        let head = self.chain.head()?;
        assert_eq!(
            head.epoch_id,
            self.epoch_manager.get_epoch_id_from_prev_block(&head.prev_block_hash).unwrap()
        );

        if prepare_chunk_headers {
            let mut tracker = self.chunk_endorsement_tracker.lock();
            self.chunk_inclusion_tracker
                .prepare_chunk_headers_ready_for_inclusion(&head.last_block_hash, &mut tracker)?;
        }

        self.produce_block_on(height, head.last_block_hash)
    }

    /// Produce block for given `height` on top of block `prev_hash`.
    /// Should be called either from `produce_block` or in tests.
    pub fn produce_block_on(
        &mut self,
        height: BlockHeight,
        prev_hash: CryptoHash,
    ) -> Result<Option<Block>, Error> {
        let validator_signer = self.validator_signer.get().ok_or_else(|| {
            Error::BlockProducer("Called without block producer info.".to_string())
        })?;
        let optimistic_block = self
            .last_optimistic_block_produced
            .as_ref()
            .filter(|ob| {
                // Make sure that the optimistic block is produced on the same previous block.
                if ob.inner.prev_block_hash == prev_hash {
                    return true;
                }
                warn!(target: "client",
                    height=height,
                    prev_hash=?prev_hash,
                    optimistic_block_prev_hash=?ob.inner.prev_block_hash,
                    "Optimistic block was constructed on different block, discarding it");
                false
            })
            .cloned();
        // Check that we are were called at the block that we are producer for.
        let epoch_id = self.epoch_manager.get_epoch_id_from_prev_block(&prev_hash).unwrap();

        let prev = self.chain.get_block_header(&prev_hash)?;
        let prev_height = prev.height();
        let prev_epoch_id = *prev.epoch_id();
        let prev_next_bp_hash = *prev.next_bp_hash();

        if let Err(err) = self.pre_block_production_check(&prev, height, &validator_signer) {
            debug!(target: "client", height, ?err, "Skipping block production");
            return Ok(None);
        }

        // Check and update the doomslug tip here. This guarantees that our endorsement will be in the
        // doomslug witness. Have to do it before checking the ability to produce a block.
        let _ = self.check_and_update_doomslug_tip()?;

        let new_chunks = self
            .chunk_inclusion_tracker
            .get_chunk_headers_ready_for_inclusion(&epoch_id, &prev_hash);
        debug!(
            target: "client",
            validator=?validator_signer.validator_id(),
            height=height,
            prev_height=prev.height(),
            prev_hash=format_hash(prev_hash),
            new_chunks_count=new_chunks.len(),
            new_chunks=?new_chunks.values().collect_vec(),
            "Producing block",
        );

        // If we are producing empty blocks and there are no transactions.
        if !self.config.produce_empty_blocks && new_chunks.is_empty() {
            debug!(target: "client", "Empty blocks, skipping block production");
            return Ok(None);
        }

        let mut approvals_map = self.doomslug.get_witness(&prev_hash, prev_height, height);

        // At this point, the previous epoch hash must be available
        let epoch_id = self
            .epoch_manager
            .get_epoch_id_from_prev_block(&prev_hash)
            .expect("Epoch hash should exist at this point");
        let protocol_version = self
            .epoch_manager
            .get_epoch_protocol_version(&epoch_id)
            .expect("Epoch info should be ready at this point");
        if protocol_version > PROTOCOL_VERSION {
            panic!(
                "The client protocol version is older than the protocol version of the network. Please update nearcore. Client protocol version:{}, network protocol version {}",
                PROTOCOL_VERSION, protocol_version
            );
        }

        let approvals = self
            .epoch_manager
            .get_epoch_block_approvers_ordered(&prev_hash)?
            .into_iter()
            .map(|ApprovalStake { account_id, .. }| {
                approvals_map.remove(&account_id).map(|x| x.0.signature.into())
            })
            .collect();

        debug_assert_eq!(approvals_map.len(), 0);

        let next_epoch_id = self
            .epoch_manager
            .get_next_epoch_id_from_prev_block(&prev_hash)
            .expect("Epoch hash should exist at this point");

        let gas_price_adjustment_rate =
            self.chain.block_economics_config.gas_price_adjustment_rate();
        let min_gas_price = self.chain.block_economics_config.min_gas_price();
        let max_gas_price = self.chain.block_economics_config.max_gas_price();

        let next_bp_hash = if prev_epoch_id != epoch_id {
            Chain::compute_bp_hash(self.epoch_manager.as_ref(), next_epoch_id)?
        } else {
            prev_next_bp_hash
        };

        #[cfg(feature = "sandbox")]
        let sandbox_delta_time = Some(self.sandbox_delta_time());
        #[cfg(not(feature = "sandbox"))]
        let sandbox_delta_time = None;

        // Get block extra from previous block.
        let block_merkle_tree = self.chain.chain_store().get_block_merkle_tree(&prev_hash)?;
        let mut block_merkle_tree = PartialMerkleTree::clone(&block_merkle_tree);
        block_merkle_tree.insert(prev_hash);
        let block_merkle_root = block_merkle_tree.root();
        // The number of leaves in Block Merkle Tree is the amount of Blocks on the Canonical Chain by construction.
        // The ordinal of the next Block will be equal to this amount plus one.
        let block_ordinal: NumBlocks = block_merkle_tree.size() + 1;
        let prev_block = self.chain.get_block(&prev_hash)?;
        let mut chunk_headers =
            Chain::get_prev_chunk_headers(self.epoch_manager.as_ref(), &prev_block)?;
        let mut chunk_endorsements = vec![vec![]; chunk_headers.len()];

        // Add debug information about the block production (and info on when did the chunks arrive).
        self.block_production_info.record_block_production(
            height,
            BlockProductionTracker::construct_chunk_collection_info(
                height,
                &epoch_id,
                chunk_headers.len(),
                &new_chunks,
                self.epoch_manager.as_ref(),
                &self.chunk_inclusion_tracker,
            )?,
        );

        // Collect new chunk headers and endorsements.
        let shard_layout = self.epoch_manager.get_shard_layout(&epoch_id)?;
        for (shard_id, chunk_hash) in new_chunks {
            let shard_index =
                shard_layout.get_shard_index(shard_id).map_err(Into::<EpochError>::into)?;
            let (mut chunk_header, chunk_endorsement) =
                self.chunk_inclusion_tracker.get_chunk_header_and_endorsements(&chunk_hash)?;
            *chunk_header.height_included_mut() = height;
            *chunk_headers
                .get_mut(shard_index)
                .ok_or(near_chain_primitives::Error::InvalidShardId(shard_id))? = chunk_header;
            *chunk_endorsements
                .get_mut(shard_index)
                .ok_or(near_chain_primitives::Error::InvalidShardId(shard_id))? = chunk_endorsement;
        }

        let prev_header = &prev_block.header();

        let next_epoch_id = self.epoch_manager.get_next_epoch_id_from_prev_block(&prev_hash)?;

        let minted_amount = if self.epoch_manager.is_next_block_epoch_start(&prev_hash)? {
            Some(self.epoch_manager.get_epoch_info(&next_epoch_id)?.minted_amount())
        } else {
            None
        };

        let epoch_sync_data_hash = if self.epoch_manager.is_next_block_epoch_start(&prev_hash)? {
            let last_block_info = self.epoch_manager.get_block_info(prev_block.hash())?;
            let prev_epoch_id = *last_block_info.epoch_id();
            let prev_epoch_first_block_info =
                self.epoch_manager.get_block_info(last_block_info.epoch_first_block())?;
            let prev_epoch_prev_last_block_info =
                self.epoch_manager.get_block_info(last_block_info.prev_hash())?;
            let prev_epoch_info = self.epoch_manager.get_epoch_info(&prev_epoch_id)?;
            let cur_epoch_info = self.epoch_manager.get_epoch_info(&epoch_id)?;
            let next_epoch_info = self.epoch_manager.get_epoch_info(&next_epoch_id)?;
            Some(CryptoHash::hash_borsh(&(
                prev_epoch_first_block_info,
                prev_epoch_prev_last_block_info,
                last_block_info,
                prev_epoch_info,
                cur_epoch_info,
                next_epoch_info,
            )))
        } else {
            None
        };

        let next_epoch_protocol_version =
            self.epoch_manager.get_epoch_protocol_version(&next_epoch_id)?;

        let block = Block::produce(
            self.upgrade_schedule
                .protocol_version_to_vote_for(self.clock.now_utc(), next_epoch_protocol_version),
            prev_header,
            height,
            block_ordinal,
            chunk_headers,
            chunk_endorsements,
            epoch_id,
            next_epoch_id,
            epoch_sync_data_hash,
            approvals,
            gas_price_adjustment_rate,
            min_gas_price,
            max_gas_price,
            minted_amount,
            &*validator_signer,
            next_bp_hash,
            block_merkle_root,
            self.clock.clone(),
            sandbox_delta_time,
            optimistic_block,
        );

        // Update latest known even before returning block out, to prevent race conditions.
        self.chain
            .mut_chain_store()
            .save_latest_known(LatestKnown { height, seen: block.header().raw_timestamp() })?;

        metrics::BLOCK_PRODUCED_TOTAL.inc();

        Ok(Some(block))
    }

    /// Processes received block. Ban peer if the block header is invalid or the block is ill-formed.
    // This function is just a wrapper for process_block_impl that makes error propagation easier.
    pub fn receive_block(
        &mut self,
        block: Block,
        peer_id: PeerId,
        was_requested: bool,
        apply_chunks_done_sender: Option<Sender<ApplyChunksDoneMessage>>,
        signer: &Option<Arc<ValidatorSigner>>,
    ) {
        let hash = *block.hash();
        let prev_hash = *block.header().prev_hash();
        let _span = tracing::debug_span!(
            target: "client",
            "receive_block",
            me = ?signer.as_ref().map(|vs| vs.validator_id()),
            %prev_hash,
            %hash,
            height = block.header().height(),
            %peer_id,
            was_requested)
        .entered();

        let res = self.receive_block_impl(
            block,
            peer_id,
            was_requested,
            apply_chunks_done_sender,
            signer,
        );
        // Log the errors here. Note that the real error handling logic is already
        // done within process_block_impl, this is just for logging.
        if let Err(err) = res {
            if err.is_bad_data() {
                warn!(target: "client", ?err, "Receive bad block");
            } else if err.is_error() {
                if let near_chain::Error::DBNotFoundErr(msg) = &err {
                    debug_assert!(!msg.starts_with("BLOCK HEIGHT"), "{:?}", err);
                }
                if self.sync_handler.sync_status.is_syncing() {
                    // While syncing, we may receive blocks that are older or from next epochs.
                    // This leads to Old Block or EpochOutOfBounds errors.
                    debug!(target: "client", ?err, sync_status = ?self.sync_handler.sync_status, "Error receiving a block. is syncing");
                } else {
                    error!(target: "client", ?err, "Error on receiving a block. Not syncing");
                }
            } else {
                debug!(target: "client", ?err, "Process block: refused by chain");
            }
            self.chain.blocks_delay_tracker.mark_block_errored(&hash, err.to_string());
        }
    }

    /// Processes received block.
    /// This function first does some pre-check based on block height to avoid processing
    /// blocks multiple times.
    /// Then it process the block header. If the header if valid, broadcast the block to its peers
    /// Then it starts the block processing process to process the full block.
    pub fn receive_block_impl(
        &mut self,
        block: Block,
        peer_id: PeerId,
        was_requested: bool,
        apply_chunks_done_sender: Option<Sender<ApplyChunksDoneMessage>>,
        signer: &Option<Arc<ValidatorSigner>>,
    ) -> Result<(), near_chain::Error> {
        let _span =
            debug_span!(target: "chain", "receive_block_impl", was_requested, ?peer_id).entered();
        self.chain.blocks_delay_tracker.mark_block_received(&block);
        // To protect ourselves from spamming, we do some pre-check on block height before we do any
        // real processing.
        if !self.check_block_height(&block, was_requested)? {
            self.chain
                .blocks_delay_tracker
                .mark_block_dropped(block.hash(), DroppedReason::HeightProcessed);
            return Ok(());
        }

        // Before we proceed with any further processing, we first check that the block
        // hash and signature matches to make sure the block is indeed produced by the assigned
        // block producer. If not, we drop the block immediately and ban the peer
        if self.chain.verify_block_hash_and_signature(&block)?
            == VerifyBlockHashAndSignatureResult::Incorrect
        {
            self.ban_peer(peer_id, ReasonForBan::BadBlockHeader);
            return Err(near_chain::Error::InvalidSignature);
        }

        let prev_hash = *block.header().prev_hash();
        let block = block.into();
        self.verify_and_rebroadcast_block(&block, was_requested, &peer_id)?;
        let provenance =
            if was_requested { near_chain::Provenance::SYNC } else { near_chain::Provenance::NONE };
        let res = self.start_process_block(block, provenance, apply_chunks_done_sender, signer);
        match &res {
            Err(near_chain::Error::Orphan) => {
                debug!(target: "chain", ?prev_hash, "Orphan error");
                if !self.chain.is_orphan(&prev_hash) {
                    debug!(target: "chain", "not orphan");
                    self.request_block(prev_hash, peer_id)
                }
            }
            err => {
                debug!(target: "chain", ?err, "some other error");
            }
        }
        res
    }

    /// Check optimistic block and start processing if is valid.
    pub fn receive_optimistic_block(&mut self, block: OptimisticBlock, peer_id: &PeerId) {
        let _span = debug_span!(target: "client", "receive_optimistic_block").entered();
        debug!(target: "client", ?block, ?peer_id, "Received optimistic block");

        // Pre-validate the optimistic block.
        if let Err(e) = self.chain.pre_check_optimistic_block(&block) {
            near_chain::metrics::NUM_INVALID_OPTIMISTIC_BLOCKS.inc();
            debug!(target: "client", ?e, "Optimistic block is invalid");
            return;
        }

        if let Err(_) = self.chain.get_block_header(&block.prev_block_hash()) {
            // If the previous block is not found, add the block to the orphan
            // pool for now.
            self.chain.save_optimistic_orphan(block);
            return;
        }

        let signer = self.validator_signer.get();
        let me = signer.as_ref().map(|signer| signer.validator_id().clone());
        self.chain.preprocess_optimistic_block(
            block,
            &me,
            Some(self.myself_sender.apply_chunks_done.clone()),
        );
    }

    /// To protect ourselves from spamming, we do some pre-check on block height before we do any
    /// processing. This function returns true if the block height is valid.
    fn check_block_height(
        &self,
        block: &Block,
        was_requested: bool,
    ) -> Result<bool, near_chain::Error> {
        let head = self.chain.head()?;
        let is_syncing = self.sync_handler.sync_status.is_syncing();
        if block.header().height() >= head.height + BLOCK_HORIZON && is_syncing && !was_requested {
            debug!(target: "client", head_height = head.height, "Dropping a block that is too far ahead.");
            return Ok(false);
        }
        let tail = self.chain.tail()?;
        if block.header().height() < tail {
            debug!(target: "client", tail_height = tail, "Dropping a block that is too far behind.");
            return Ok(false);
        }
        // drop the block if a) it is not requested, b) we already processed this height,
        //est-utils/actix-test-utils/src/lib.rs c) it is not building on top of current head
        if !was_requested
            && block.header().prev_hash()
                != &self
                    .chain
                    .head()
                    .map_or_else(|_| CryptoHash::default(), |tip| tip.last_block_hash)
        {
            if self.chain.is_height_processed(block.header().height())? {
                debug!(target: "client", height = block.header().height(), "Dropping a block because we've seen this height before and we didn't request it");
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Verify the block and rebroadcast it if it is valid, ban the peer if it's invalid.
    /// Ignore all other errors because the full block will be processed later.
    /// Note that this happens before the full block processing logic because we want blocks to be
    /// propagated in the network fast.
    fn verify_and_rebroadcast_block(
        &mut self,
        block: &MaybeValidated<Block>,
        was_requested: bool,
        peer_id: &PeerId,
    ) -> Result<(), near_chain::Error> {
        let res = self.chain.process_block_header(block.header());
        let res = res.and_then(|_| self.chain.validate_block(block));
        match res {
            Ok(_) => {
                let head = self.chain.head()?;
                // do not broadcast blocks that are too far back.
                if (head.height < block.header().height()
                    || &head.epoch_id == block.header().epoch_id())
                    && !was_requested
                    && !self.sync_handler.sync_status.is_syncing()
                {
                    self.rebroadcast_block(block.as_ref().into_inner());
                }
                Ok(())
            }
            Err(e) if e.is_bad_data() => {
                // We don't ban a peer if the block timestamp is too much in the future since it's possible
                // that a block is considered valid in one machine and invalid in another machine when their
                // clocks are not synced.
                if !matches!(e, near_chain::Error::InvalidBlockFutureTime(_)) {
                    self.ban_peer(peer_id.clone(), ReasonForBan::BadBlockHeader);
                }
                Err(e)
            }
            Err(_) => {
                // We are ignoring all other errors and proceeding with the
                // block.  If it is an orphan (i.e. we haven’t processed its
                // previous block) than we will get MissingBlock errors.  In
                // those cases we shouldn’t reject the block instead passing
                // it along.  Eventually, it’ll get saved as an orphan.
                Ok(())
            }
        }
    }

    /// Start the processing of a block. Note that this function will return before
    /// the full processing is finished because applying chunks is done asynchronously
    /// in the rayon thread pool.
    /// `apply_chunks_done_sender`: a callback that will be called when applying chunks is finished.
    pub fn start_process_block(
        &mut self,
        block: MaybeValidated<Block>,
        provenance: Provenance,
        apply_chunks_done_sender: Option<Sender<ApplyChunksDoneMessage>>,
        signer: &Option<Arc<ValidatorSigner>>,
    ) -> Result<(), near_chain::Error> {
        let _span = debug_span!(
                target: "chain",
                "start_process_block",
                ?provenance,
                block_height = block.header().height())
        .entered();
        let mut block_processing_artifacts = BlockProcessingArtifact::default();

        let result = {
            let me = signer.as_ref().map(|vs| vs.validator_id().clone());
            self.chain.start_process_block_async(
                &me,
                block,
                provenance,
                &mut block_processing_artifacts,
                apply_chunks_done_sender,
            )
        };

        self.process_block_processing_artifact(block_processing_artifacts);

        result
    }

    /// Check if there are any blocks that has finished applying chunks, run post processing on these
    /// blocks.
    pub fn postprocess_ready_blocks(
        &mut self,
        apply_chunks_done_sender: Option<Sender<ApplyChunksDoneMessage>>,
        should_produce_chunk: bool,
        signer: &Option<Arc<ValidatorSigner>>,
    ) -> (Vec<CryptoHash>, HashMap<CryptoHash, near_chain::Error>) {
        let _span = debug_span!(target: "client", "postprocess_ready_blocks", should_produce_chunk)
            .entered();
        let me = signer.as_ref().map(|signer| signer.validator_id().clone());
        let mut block_processing_artifacts = BlockProcessingArtifact::default();
        let (accepted_blocks, errors) = self.chain.postprocess_ready_blocks(
            &me,
            &mut block_processing_artifacts,
            apply_chunks_done_sender,
        );
        if accepted_blocks.iter().any(|accepted_block| accepted_block.status.is_new_head()) {
            self.shards_manager_adapter.send(ShardsManagerRequestFromClient::UpdateChainHeads {
                head: self.chain.head().unwrap(),
                header_head: self.chain.header_head().unwrap(),
            });
        }
        self.process_block_processing_artifact(block_processing_artifacts);
        let accepted_blocks_hashes =
            accepted_blocks.iter().map(|accepted_block| accepted_block.hash).collect();
        for accepted_block in accepted_blocks {
            self.on_block_accepted_with_optional_chunk_produce(
                accepted_block.hash,
                accepted_block.status,
                accepted_block.provenance,
                !should_produce_chunk,
                signer,
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
        let BlockProcessingArtifact {
            orphans_missing_chunks,
            blocks_missing_chunks,
            invalid_chunks,
        } = block_processing_artifacts;
        // For any missing chunk, let the ShardsManager know of the chunk header so that it may
        // apply forwarded parts. This may end up completing the chunk.
        let missing_chunks = blocks_missing_chunks
            .iter()
            .flat_map(|block| block.missing_chunks.iter())
            .chain(orphans_missing_chunks.iter().flat_map(|block| block.missing_chunks.iter()));
        for chunk in missing_chunks {
            self.shards_manager_adapter
                .send(ShardsManagerRequestFromClient::ProcessChunkHeaderFromBlock(chunk.clone()));
        }
        // Request any missing chunks (which may be completed by the
        // process_chunk_header_from_block call, but that is OK as it would be noop).
        self.request_missing_chunks(blocks_missing_chunks, orphans_missing_chunks);

        for chunk_header in invalid_chunks {
            if let Err(err) = self.ban_chunk_producer_for_producing_invalid_chunk(chunk_header) {
                error!(target: "client", ?err, "Failed to ban chunk producer for producing invalid chunk");
            }
        }
    }

    fn ban_chunk_producer_for_producing_invalid_chunk(
        &mut self,
        chunk_header: ShardChunkHeader,
    ) -> Result<(), Error> {
        let epoch_id =
            self.epoch_manager.get_epoch_id_from_prev_block(chunk_header.prev_block_hash())?;
        let chunk_producer = self
            .epoch_manager
            .get_chunk_producer_info(&ChunkProductionKey {
                epoch_id,
                height_created: chunk_header.height_created(),
                shard_id: chunk_header.shard_id(),
            })?
            .take_account_id();
        error!(
            target: "client",
            ?chunk_producer,
            ?epoch_id,
            chunk_hash = ?chunk_header.chunk_hash(),
            "Banning chunk producer for producing invalid chunk");
        metrics::CHUNK_PRODUCER_BANNED_FOR_EPOCH.inc();
        self.chunk_inclusion_tracker.ban_chunk_producer(epoch_id, chunk_producer);
        Ok(())
    }

    fn rebroadcast_block(&mut self, block: &Block) {
        if self.rebroadcasted_blocks.get(block.hash()).is_none() {
            self.network_adapter.send(PeerManagerMessageRequest::NetworkRequests(
                NetworkRequests::Block { block: block.clone() },
            ));
            self.rebroadcasted_blocks.put(*block.hash(), ());
        }
    }

    /// Called asynchronously when the ShardsManager finishes processing a chunk.
    pub fn on_chunk_completed(
        &mut self,
        partial_chunk: PartialEncodedChunk,
        shard_chunk: Option<ShardChunk>,
        apply_chunks_done_sender: Option<Sender<ApplyChunksDoneMessage>>,
        signer: &Option<Arc<ValidatorSigner>>,
    ) {
        let chunk_header = partial_chunk.cloned_header();
        self.chain.blocks_delay_tracker.mark_chunk_completed(&chunk_header);

        // TODO(#10569) We would like a proper error handling here instead of `expect`.
        let parent_hash = *chunk_header.prev_block_hash();
        let shard_layout = self
            .epoch_manager
            .get_shard_layout_from_prev_block(&parent_hash)
            .expect("Could not obtain shard layout");

        let shard_id = partial_chunk.shard_id();
        let shard_index =
            shard_layout.get_shard_index(shard_id).expect("Could not obtain shard index");
        self.block_production_info
            .record_chunk_collected(partial_chunk.height_created(), shard_index);

        // TODO(#10569) We would like a proper error handling here instead of `expect`.
        persist_chunk(Arc::new(partial_chunk), shard_chunk, self.chain.mut_chain_store())
            .expect("Could not persist chunk");
        // We're marking chunk as accepted.
        self.chain.blocks_with_missing_chunks.accept_chunk(&chunk_header.chunk_hash());
        self.chain.optimistic_block_chunks.add_chunk(&shard_layout, chunk_header);
        // If this was the last chunk that was missing for a block, it will be processed now.
        self.process_blocks_with_missing_chunks(apply_chunks_done_sender, &signer);

        let me = signer.as_ref().map(|signer| signer.validator_id().clone());
        self.chain.maybe_process_optimistic_block(
            &me,
            Some(self.myself_sender.apply_chunks_done.clone()),
        );
    }

    /// Called asynchronously when the ShardsManager finishes processing a chunk but the chunk
    /// is invalid.
    pub fn on_invalid_chunk(&mut self, encoded_chunk: EncodedShardChunk) {
        let mut update = self.chain.mut_chain_store().store_update();
        update.save_invalid_chunk(encoded_chunk);
        if let Err(err) = update.commit() {
            error!(target: "client", ?err, "Error saving invalid chunk");
        }
    }

    pub fn sync_block_headers(
        &mut self,
        headers: Vec<BlockHeader>,
    ) -> Result<(), near_chain::Error> {
        if matches!(self.sync_handler.sync_status, SyncStatus::EpochSync(_)) {
            return Err(near_chain::Error::Other(
                "Cannot sync block headers during an epoch sync".to_owned(),
            ));
        };
        self.chain.sync_block_headers(headers)?;
        self.shards_manager_adapter.send(ShardsManagerRequestFromClient::UpdateChainHeads {
            head: self.chain.head().unwrap(),
            header_head: self.chain.header_head().unwrap(),
        });
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
            self.doomslug.set_tip(tip.last_block_hash, tip.height, last_final_height);
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
        self.doomslug.set_tip(tip.last_block_hash, height, last_final_height);

        Ok(())
    }

    /// Gets the advanced timestamp delta in nanoseconds for sandbox once it has been fast-forwarded
    #[cfg(feature = "sandbox")]
    pub fn sandbox_delta_time(&self) -> Duration {
        let avg_block_prod_time = (self.config.min_block_production_delay.whole_nanoseconds()
            + self.config.max_block_production_delay.whole_nanoseconds())
            / 2;

        let ns = (self.accrued_fastforward_delta as i128 * avg_block_prod_time)
            .try_into()
            .unwrap_or_else(|_| {
                panic!(
                    "Too high of a delta_height {} to convert into i64",
                    self.accrued_fastforward_delta
                )
            });

        Duration::nanoseconds(ns)
    }

    pub fn send_block_approval(
        &mut self,
        parent_hash: &CryptoHash,
        approval: Approval,
        signer: &Option<Arc<ValidatorSigner>>,
    ) -> Result<(), Error> {
        let next_epoch_id = self.epoch_manager.get_epoch_id_from_prev_block(parent_hash)?;
        let next_block_producer =
            self.epoch_manager.get_block_producer(&next_epoch_id, approval.target_height)?;
        let next_block_producer_id = signer.as_ref().map(|x| x.validator_id());
        if Some(&next_block_producer) == next_block_producer_id {
            self.collect_block_approval(&approval, ApprovalType::SelfApproval, signer);
        } else {
            debug!(target: "client",
                approval_inner = ?approval.inner,
                account_id = ?approval.account_id,
                next_bp = ?next_block_producer,
                target_height = approval.target_height,
                approval_type="PeerApproval",
                "send_block_approval");
            let approval_message = ApprovalMessage::new(approval, next_block_producer);
            self.network_adapter.send(PeerManagerMessageRequest::NetworkRequests(
                NetworkRequests::Approval { approval_message },
            ));
        }

        Ok(())
    }

    /// Gets called when block got accepted.
    /// Only produce chunk if `skip_produce_chunk` is false.
    /// `skip_produce_chunk` is set to true to simulate when there are missing chunks in a block
    fn on_block_accepted_with_optional_chunk_produce(
        &mut self,
        block_hash: CryptoHash,
        status: BlockStatus,
        provenance: Provenance,
        skip_produce_chunk: bool,
        signer: &Option<Arc<ValidatorSigner>>,
    ) {
        let _span = tracing::debug_span!(
            target: "client",
            "on_block_accepted_with_optional_chunk_produce",
            ?block_hash,
            ?status,
            ?provenance,
            skip_produce_chunk,
            is_syncing = self.sync_handler.sync_status.is_syncing(),
            sync_status = ?self.sync_handler.sync_status)
        .entered();
        let block = match self.chain.get_block(&block_hash) {
            Ok(block) => block,
            Err(err) => {
                error!(target: "client", ?err, ?block_hash, "Failed to find block that was just accepted");
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
                self.collect_block_approval(&approval, approval_type, signer);
            }
        }

        if status.is_new_head() {
            let last_final_block = block.header().last_final_block();
            let last_finalized_height = if last_final_block == &CryptoHash::default() {
                self.chain.genesis().height()
            } else {
                self.chain.get_block_header(last_final_block).map_or(0, |header| header.height())
            };
            self.chain.blocks_with_missing_chunks.prune_blocks_below_height(last_finalized_height);
            self.chain.blocks_pending_execution.prune_blocks_below_height(last_finalized_height);

            // send_network_chain_info should be called whenever the chain head changes.
            // See send_network_chain_info() for more details.
            if let Err(err) = self.send_network_chain_info() {
                error!(target: "client", ?err, "Failed to update network chain info");
            }

            // If the next block is the first of the next epoch and the shard
            // layout is changing we need to reshard the transaction pool.
            // TODO make sure transactions don't get added for the old shard
            // layout after the pool resharding
            if self.epoch_manager.is_next_block_epoch_start(&block_hash).unwrap_or(false) {
                let new_shard_layout =
                    self.epoch_manager.get_shard_layout_from_prev_block(&block_hash);
                let old_shard_layout =
                    self.epoch_manager.get_shard_layout_from_prev_block(block.header().prev_hash());
                match (old_shard_layout, new_shard_layout) {
                    (Ok(old_shard_layout), Ok(new_shard_layout)) => {
                        if old_shard_layout != new_shard_layout {
                            let mut guarded_pool = self.chunk_producer.sharded_tx_pool.lock();
                            guarded_pool.reshard(&old_shard_layout, &new_shard_layout);
                        }
                    }
                    (old_shard_layout, new_shard_layout) => {
                        tracing::warn!(target: "client", ?old_shard_layout, ?new_shard_layout, "failed to check if shard layout is changing");
                    }
                }
            }
        }

        if let Some(signer) = signer.clone() {
            let validator_id = signer.validator_id().clone();

            if !self.reconcile_transaction_pool(validator_id, status, &block) {
                return;
            }

            let can_produce_with_provenance = provenance != Provenance::SYNC;
            let can_produce_with_sync_status = !self.sync_handler.sync_status.is_syncing();
            if can_produce_with_provenance && can_produce_with_sync_status && !skip_produce_chunk {
                self.produce_chunks(&block, &signer);
            } else {
                info!(target: "client", can_produce_with_provenance, can_produce_with_sync_status, skip_produce_chunk, "not producing a chunk");
            }
        }

        // Run shadow chunk validation on the new block, unless it's coming from sync.
        // Syncing has to be fast to catch up with the rest of the chain,
        // applying the chunks would make the sync unworkably slow.
        if provenance != Provenance::SYNC {
            if let Err(err) = self.shadow_validate_block_chunks(&block) {
                tracing::error!(
                    target: "client",
                    ?err,
                    block_hash = ?block.hash(),
                    "block chunks shadow validation failed"
                );
            }
        }

        self.shards_manager_adapter
            .send(ShardsManagerRequestFromClient::CheckIncompleteChunks(*block.hash()));

        self.process_ready_orphan_witnesses_and_clean_old(&block, signer);
    }

    /// Reconcile the transaction pool after processing a block.
    /// returns true if it's ok to proceed to produce chunks
    /// returns false when handling a fork and there is no need to produce chunks
    fn reconcile_transaction_pool(
        &mut self,
        validator_id: AccountId,
        status: BlockStatus,
        block: &Block,
    ) -> bool {
        match status {
            BlockStatus::Next => {
                // If this block immediately follows the current tip, remove
                // transactions from the tx pool.
                match self.remove_transactions_for_block(&validator_id, block) {
                    Ok(()) => (),
                    Err(err) => {
                        tracing::debug!(
                            target: "client",
                            "validator {}: removing txs for block {:?} failed with {:?}",
                            validator_id,
                            block,
                            err
                        );
                    }
                }
            }
            BlockStatus::Fork => {
                // If it's a fork, no need to reconcile transactions or produce chunks.
                return false;
            }
            BlockStatus::Reorg(prev_head) => {
                // If a reorg happened, reintroduce transactions from the
                // previous chain and remove transactions from the new chain.
                let mut reintroduce_head = self.chain.get_block_header(&prev_head).unwrap();
                let mut remove_head = block.header().clone();
                assert_ne!(remove_head.hash(), reintroduce_head.hash());

                let mut to_remove = vec![];
                let mut to_reintroduce = vec![];

                while remove_head.hash() != reintroduce_head.hash() {
                    while remove_head.height() > reintroduce_head.height() {
                        to_remove.push(*remove_head.hash());
                        remove_head =
                            self.chain.get_block_header(remove_head.prev_hash()).unwrap().clone();
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
                        match self.reintroduce_transactions_for_block(&validator_id, &block) {
                            Ok(()) => (),
                            Err(err) => {
                                tracing::debug!(
                                    target: "client",
                                    "validator {}: reintroducing txs for block {:?} failed with {:?}",
                                    validator_id,
                                    block,
                                    err
                                );
                            }
                        }
                    }
                }

                for to_remove_hash in to_remove {
                    if let Ok(block) = self.chain.get_block(&to_remove_hash) {
                        match self.remove_transactions_for_block(&validator_id, &block) {
                            Ok(()) => (),
                            Err(err) => {
                                tracing::debug!(
                                    target: "client",
                                    "validator {}: removing txs for block {:?} failed with {:?}",
                                    validator_id,
                                    block,
                                    err
                                );
                            }
                        }
                    }
                }
            }
        };
        true
    }

    // Produce new chunks
    fn produce_chunks(&mut self, block: &Block, signer: &Arc<ValidatorSigner>) {
        let validator_id = signer.validator_id().clone();
        let _span = debug_span!(
            target: "client",
            "produce_chunks",
            ?validator_id,
            block_height = block.header().height())
        .entered();

        #[cfg(feature = "test_features")]
        match self.chunk_producer.adv_produce_chunks {
            Some(AdvProduceChunksMode::StopProduce) => {
                tracing::info!(
                    target: "adversary",
                    block_height = block.header().height(),
                    "skipping chunk production due to adversary configuration"
                );
                return;
            }
            _ => {}
        };

        let epoch_id =
            self.epoch_manager.get_epoch_id_from_prev_block(block.header().hash()).unwrap();
        for shard_id in self.epoch_manager.shard_ids(&epoch_id).unwrap() {
            let next_height = block.header().height() + 1;
            let epoch_manager = self.epoch_manager.as_ref();
            let chunk_proposer = epoch_manager
                .get_chunk_producer_info(&ChunkProductionKey {
                    epoch_id,
                    height_created: next_height,
                    shard_id,
                })
                .unwrap()
                .take_account_id();
            if &chunk_proposer != &validator_id {
                continue;
            }

            let _span = debug_span!(
                target: "client",
                "on_block_accepted",
                prev_block_hash = ?*block.hash(),
                ?shard_id)
            .entered();
            let _timer = metrics::PRODUCE_AND_DISTRIBUTE_CHUNK_TIME
                .with_label_values(&[&shard_id.to_string()])
                .start_timer();
            let last_header = Chain::get_prev_chunk_header(epoch_manager, block, shard_id).unwrap();
            let result = self.chunk_producer.produce_chunk(
                block,
                &epoch_id,
                last_header.clone(),
                next_height,
                shard_id,
                signer,
                &|tx| {
                    #[cfg(features = "test_features")]
                    match self.adv_produce_chunks {
                        Some(AdvProduceChunksMode::ProduceWithoutTxValidityCheck) => true,
                        _ => chain.transaction_validity_check(block.header().clone())(tx),
                    }
                    #[cfg(not(features = "test_features"))]
                    self.chain.transaction_validity_check(block.header().clone())(tx)
                },
            );

            let ProduceChunkResult { chunk, encoded_chunk_parts_paths, receipts } = match result {
                Ok(Some(res)) => res,
                Ok(None) => return,
                Err(err) => {
                    error!(target: "client", ?err, "Error producing chunk");
                    return;
                }
            };
            if let Err(err) = self.send_chunk_state_witness_to_chunk_validators(
                &epoch_id,
                block.header(),
                &last_header,
                chunk.to_shard_chunk(),
                &Some(signer.clone()),
            ) {
                tracing::error!(target: "client", ?err, "Failed to send chunk state witness to chunk validators");
            }
            self.persist_and_distribute_encoded_chunk(
                chunk,
                encoded_chunk_parts_paths,
                receipts,
                validator_id.clone(),
            )
            .expect("Failed to process produced chunk");
        }
    }

    pub fn persist_and_distribute_encoded_chunk(
        &mut self,
        chunk: ShardChunkWithEncoding,
        merkle_paths: Vec<MerklePath>,
        receipts: Vec<Receipt>,
        validator_id: AccountId,
    ) -> Result<(), Error> {
        let partial_chunk = create_partial_chunk(
            &chunk,
            merkle_paths.clone(),
            Some(&validator_id),
            self.epoch_manager.as_ref(),
            &self.shard_tracker,
        )?;
        let (shard_chunk, encoded_shard_chunk) = chunk.into_parts();
        let partial_chunk_arc = Arc::new(partial_chunk.clone());
        persist_chunk(
            Arc::clone(&partial_chunk_arc),
            Some(shard_chunk),
            self.chain.mut_chain_store(),
        )?;

        let chunk_header = encoded_shard_chunk.cloned_header();
        if let Some(chunk_distribution) = &self.chunk_distribution_network {
            if chunk_distribution.enabled() {
                let partial_chunk_arc = Arc::clone(&partial_chunk_arc);
                let mut thread_local_client = chunk_distribution.clone();
                near_performance_metrics::actix::spawn("ChunkDistributionNetwork", async move {
                    if let Err(err) = thread_local_client.publish_chunk(&partial_chunk_arc).await {
                        error!(target: "client", ?err, "Failed to distribute chunk via Chunk Distribution Network");
                    }
                });
            }
        }

        self.chunk_inclusion_tracker
            .mark_chunk_header_ready_for_inclusion(chunk_header, validator_id);
        self.shards_manager_adapter.send(ShardsManagerRequestFromClient::DistributeEncodedChunk {
            partial_chunk,
            encoded_chunk: encoded_shard_chunk,
            merkle_paths,
            outgoing_receipts: receipts,
        });
        Ok(())
    }

    pub fn request_missing_chunks(
        &mut self,
        blocks_missing_chunks: Vec<BlockMissingChunks>,
        orphans_missing_chunks: Vec<OrphanMissingChunks>,
    ) {
        let _span = debug_span!(
            target: "client",
            "request_missing_chunks",
            ?blocks_missing_chunks,
            ?orphans_missing_chunks)
        .entered();
        if let Some(chunk_distribution) = &self.chunk_distribution_network {
            if chunk_distribution.enabled() {
                return crate::chunk_distribution_network::request_missing_chunks(
                    blocks_missing_chunks,
                    orphans_missing_chunks,
                    chunk_distribution.clone(),
                    &mut self.chain.blocks_delay_tracker,
                    &self.shards_manager_adapter,
                );
            }
        }
        self.p2p_request_missing_chunks(blocks_missing_chunks, orphans_missing_chunks);
    }

    fn p2p_request_missing_chunks(
        &mut self,
        blocks_missing_chunks: Vec<BlockMissingChunks>,
        orphans_missing_chunks: Vec<OrphanMissingChunks>,
    ) {
        for BlockMissingChunks { prev_hash, missing_chunks } in blocks_missing_chunks {
            for chunk in &missing_chunks {
                self.chain.blocks_delay_tracker.mark_chunk_requested(chunk);
            }
            self.shards_manager_adapter.send(ShardsManagerRequestFromClient::RequestChunks {
                chunks_to_request: missing_chunks,
                prev_hash,
            });
        }

        for OrphanMissingChunks { missing_chunks, epoch_id, ancestor_hash } in
            orphans_missing_chunks
        {
            for chunk in &missing_chunks {
                self.chain.blocks_delay_tracker.mark_chunk_requested(chunk);
            }
            self.shards_manager_adapter.send(
                ShardsManagerRequestFromClient::RequestChunksForOrphan {
                    chunks_to_request: missing_chunks,
                    epoch_id,
                    ancestor_hash,
                },
            );
        }
    }

    /// Check if any block with missing chunks is ready to be processed
    pub fn process_blocks_with_missing_chunks(
        &mut self,
        apply_chunks_done_sender: Option<Sender<ApplyChunksDoneMessage>>,
        signer: &Option<Arc<ValidatorSigner>>,
    ) {
        let _span = debug_span!(target: "client", "process_blocks_with_missing_chunks").entered();
        let me = signer.as_ref().map(|signer| signer.validator_id());
        let mut blocks_processing_artifacts = BlockProcessingArtifact::default();
        self.chain.check_blocks_with_missing_chunks(
            &me.map(|x| x.clone()),
            &mut blocks_processing_artifacts,
            apply_chunks_done_sender,
        );
        self.process_block_processing_artifact(blocks_processing_artifacts);
    }

    pub fn is_validator(&self, epoch_id: &EpochId, signer: &Option<Arc<ValidatorSigner>>) -> bool {
        match signer {
            None => false,
            Some(signer) => {
                let account_id = signer.validator_id();
                match self.epoch_manager.get_validator_by_account_id(epoch_id, account_id) {
                    Ok(validator_stake) => validator_stake.take_public_key() == signer.public_key(),
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
        let is_validator = |epoch_id, account_id, epoch_manager: &dyn EpochManagerAdapter| {
            epoch_manager.get_validator_by_account_id(epoch_id, account_id).is_ok()
        };
        if let near_chain::Error::DBNotFoundErr(_) = error {
            if check_validator {
                let head = unwrap_or_return!(self.chain.head());
                if !is_validator(&head.epoch_id, &approval.account_id, self.epoch_manager.as_ref())
                    && !is_validator(
                        &head.next_epoch_id,
                        &approval.account_id,
                        self.epoch_manager.as_ref(),
                    )
                {
                    return;
                }
            }
            let mut entry =
                self.pending_approvals.pop(&approval.inner).unwrap_or_else(|| HashMap::new());
            entry.insert(approval.account_id.clone(), (approval.clone(), approval_type));
            self.pending_approvals.put(approval.inner.clone(), entry);
        }
    }

    /// Collects block approvals.
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
    pub fn collect_block_approval(
        &mut self,
        approval: &Approval,
        approval_type: ApprovalType,
        signer: &Option<Arc<ValidatorSigner>>,
    ) {
        let Approval { inner, account_id, target_height, signature } = approval;
        debug!(target: "client",
            approval_inner=?inner,
            account_id=?account_id,
            target_height=target_height,
            approval_type=?approval_type,
            "collect_block_approval");
        let parent_hash = match inner {
            ApprovalInner::Endorsement(parent_hash) => *parent_hash,
            ApprovalInner::Skip(parent_height) => {
                match self.chain.chain_store().get_all_block_hashes_by_height(*parent_height) {
                    Ok(hashes) => {
                        // If there is more than one block at the height, all of them will be
                        // eligible to build the next block on, so we just pick one.
                        let hash = hashes.values().flatten().next();
                        match hash {
                            Some(hash) => *hash,
                            None => {
                                self.handle_process_approval_error(
                                    approval,
                                    approval_type,
                                    true,
                                    near_chain::Error::DBNotFoundErr(format!(
                                        "Cannot find any block on height {}",
                                        parent_height
                                    )),
                                );
                                return;
                            }
                        }
                    }
                    Err(e) => {
                        self.handle_process_approval_error(approval, approval_type, true, e);
                        return;
                    }
                }
            }
        };

        let next_block_epoch_id =
            match self.epoch_manager.get_epoch_id_from_prev_block(&parent_hash) {
                Err(e) => {
                    self.handle_process_approval_error(approval, approval_type, true, e.into());
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
            let validator_epoch_id = match self
                .epoch_manager
                .get_validator_by_account_id(&next_block_epoch_id, account_id)
            {
                Ok(_) => next_block_epoch_id,
                Err(EpochError::NotAValidator(_, _)) => {
                    match self.epoch_manager.get_next_epoch_id_from_prev_block(&parent_hash) {
                        Ok(next_block_next_epoch_id) => next_block_next_epoch_id,
                        Err(_) => return,
                    }
                }
                _ => return,
            };
            match self.epoch_manager.verify_validator_signature(
                &validator_epoch_id,
                account_id,
                Approval::get_data_for_sig(inner, *target_height).as_ref(),
                signature,
            ) {
                Ok(true) => {}
                _ => return,
            }
        }

        let is_block_producer =
            match self.epoch_manager.get_block_producer(&next_block_epoch_id, *target_height) {
                Err(_) => false,
                Ok(target_block_producer) => {
                    Some(&target_block_producer) == signer.as_ref().map(|x| x.validator_id())
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
            match self.epoch_manager.get_epoch_block_approvers_ordered(&parent_hash) {
                Ok(block_producer_stakes) => block_producer_stakes,
                Err(err) => {
                    error!(target: "client", ?err, "Block approval error");
                    return;
                }
            };
        self.doomslug.on_approval_message(approval, &block_producer_stakes);
    }

    /// Find the sync hash. Most of the time it will already be set in `state_sync_info`. If not, try to find it,
    /// and set the corresponding field in `state_sync_info`.
    fn get_catchup_sync_hash_v1(
        &mut self,
        state_sync_info: &mut StateSyncInfoV1,
        epoch_first_block: &CryptoHash,
    ) -> Result<Option<CryptoHash>, Error> {
        if state_sync_info.sync_hash.is_some() {
            return Ok(state_sync_info.sync_hash);
        }

        if let Some(sync_hash) = self.chain.get_sync_hash(epoch_first_block)? {
            state_sync_info.sync_hash = Some(sync_hash);
            let mut update = self.chain.mut_chain_store().store_update();
            // note that iterate_state_sync_infos() collects everything into a Vec, so we're not
            // actually writing to the DB while actively iterating this column
            update.add_state_sync_info(StateSyncInfo::V1(state_sync_info.clone()));
            // TODO: would be nice to be able to propagate context up the call stack so we can just log
            // once at the top with all the info. Otherwise this error will look very cryptic
            update.commit()?;
        }
        Ok(state_sync_info.sync_hash)
    }

    /// Find the sync hash. If syncing to the old epoch's state, it's always set. If syncing to
    /// the current epoch's state, it might not yet be known, in which case we try to find it.
    fn get_catchup_sync_hash(
        &mut self,
        state_sync_info: &mut StateSyncInfo,
        epoch_first_block: &CryptoHash,
    ) -> Result<Option<CryptoHash>, Error> {
        match state_sync_info {
            StateSyncInfo::V0(info) => Ok(Some(info.sync_hash)),
            StateSyncInfo::V1(info) => self.get_catchup_sync_hash_v1(info, epoch_first_block),
        }
    }

    /// Walks through all the ongoing state syncs for future epochs and processes them
    pub fn run_catchup(
        &mut self,
        highest_height_peers: &[HighestHeightPeerInfo],
        block_catch_up_task_scheduler: &Sender<BlockCatchUpRequest>,
        apply_chunks_done_sender: Option<Sender<ApplyChunksDoneMessage>>,
        signer: &Option<Arc<ValidatorSigner>>,
    ) -> Result<(), Error> {
        let _span = debug_span!(target: "sync", "run_catchup").entered();
        let me = signer.as_ref().map(|x| x.validator_id().clone());

        for (epoch_first_block, mut state_sync_info) in
            self.chain.chain_store().iterate_state_sync_infos()?
        {
            assert_eq!(&epoch_first_block, state_sync_info.epoch_first_block());

            let block_header = self.chain.get_block(&epoch_first_block)?.header().clone();
            let epoch_id = block_header.epoch_id();

            let sync_hash = self.get_catchup_sync_hash(&mut state_sync_info, &epoch_first_block)?;
            let Some(sync_hash) = sync_hash else { continue };

            let CatchupState { state_sync, sync_status: status, catchup } = self
                .catchup_state_syncs
                .entry(sync_hash)
                .or_insert_with(|| {
                    tracing::debug!(target: "client", ?epoch_first_block, ?sync_hash, "inserting new state sync");
                    CatchupState {
                        state_sync: StateSync::new(
                            self.clock.clone(),
                            self.runtime_adapter.store().clone(),
                            self.epoch_manager.clone(),
                            self.runtime_adapter.clone(),
                            self.network_adapter.clone().into_sender(),
                            self.config.state_sync_external_timeout,
                            self.config.state_sync_p2p_timeout,
                            self.config.state_sync_retry_backoff,
                            self.config.state_sync_external_backoff,
                            &self.config.chain_id,
                            &self.config.state_sync.sync,
                            self.chain_sender_for_state_sync.clone(),
                            self.state_sync_future_spawner.clone(),
                            true,
                        ),
                        sync_status: StateSyncStatus {
                            sync_hash,
                            sync_status: HashMap::new(),
                            download_tasks: Vec::new(),
                            computation_tasks: Vec::new(),
                        },
                        catchup: BlocksCatchUpState::new(sync_hash, *epoch_id),
                    }
                });

            debug!(target: "catchup", ?me, ?sync_hash, progress_per_shard = ?status.sync_status, "Catchup");

            // Initialize the new shard sync to contain the shards to split at
            // first. It will get updated with the shard sync download status
            // for other shards later.
            match state_sync.run(
                sync_hash,
                status,
                highest_height_peers,
                state_sync_info.shards(),
            )? {
                StateSyncResult::InProgress => {}
                StateSyncResult::Completed => {
                    debug!(target: "catchup", "state sync completed now catch up blocks");
                    self.chain.catchup_blocks_step(
                        &me,
                        &sync_hash,
                        catchup,
                        block_catch_up_task_scheduler,
                    )?;

                    if catchup.is_finished() {
                        let mut block_processing_artifacts = BlockProcessingArtifact::default();

                        self.chain.finish_catchup_blocks(
                            &me,
                            &epoch_first_block,
                            &sync_hash,
                            &mut block_processing_artifacts,
                            apply_chunks_done_sender.clone(),
                            &catchup.done_blocks,
                        )?;

                        self.process_block_processing_artifact(block_processing_artifacts);
                    }
                }
            }
        }

        Ok(())
    }
}

/* implements functions used to communicate with network */
impl Client {
    pub fn request_block(&self, hash: CryptoHash, peer_id: PeerId) {
        let _span = debug_span!(target: "client", "request_block", ?hash, ?peer_id).entered();
        match self.chain.block_exists(&hash) {
            Ok(false) => {
                self.network_adapter.send(PeerManagerMessageRequest::NetworkRequests(
                    NetworkRequests::BlockRequest { hash, peer_id },
                ));
            }
            Ok(true) => {
                debug!(target: "client", ?hash, "send_block_request_to_peer: block already known")
            }
            Err(err) => {
                error!(target: "client", ?hash, ?err, "send_block_request_to_peer: failed to check block exists")
            }
        }
    }

    pub fn ban_peer(&self, peer_id: PeerId, ban_reason: ReasonForBan) {
        self.network_adapter.send(PeerManagerMessageRequest::NetworkRequests(
            NetworkRequests::BanPeer { peer_id, ban_reason },
        ));
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
    /// if the current epoch didn't change since the last call. In particular SetChainInfo is being
    /// send after processing each block (order of seconds), while the epoch changes way less
    /// frequently (order of hours).
    fn get_tier1_accounts(&mut self, tip: &Tip) -> Result<Arc<AccountKeys>, Error> {
        match &self.tier1_accounts_cache {
            Some(it) if it.0 == tip.epoch_id => return Ok(it.1.clone()),
            _ => {}
        }

        let _guard =
            tracing::debug_span!(target: "client", "get_tier1_accounts(): recomputing").entered();

        // What we really need are: chunk producers, block producers and block approvers for
        // this epoch and the beginning of the next epoch (so that all required connections are
        // established in advance). Note that block producers and block approvers are not
        // exactly the same - last blocks of this epoch will also need to be signed by the
        // block producers of the next epoch. On the other hand, block approvers
        // of the next epoch will also include block producers of the N+2 epoch (which we
        // definitely don't need to connect to right now). Still, as long as there is no big churn
        // in the set of block producers, it doesn't make much difference.
        //
        // With the current implementation we just fetch chunk producers and block producers
        // of this and the next epoch (which covers what we need, as described above), but may
        // require some tuning in the future. In particular, if we decide that connecting to
        // block & chunk producers of the next epoch is too expensive, we can postpone it
        // till almost the end of this epoch.
        let mut account_keys = AccountKeys::new();
        for epoch_id in [&tip.epoch_id, &tip.next_epoch_id] {
            // We assume here that calls to get_epoch_chunk_producers and get_epoch_block_producers_ordered
            // are cheaper than block processing (and that they will work with both this and
            // the next epoch). The caching on top of that (in tier1_accounts_cache field) is just
            // a defense in depth, based on the previous experience with expensive
            // EpochManagerAdapter::get_validators_info call.
            for cp in self.epoch_manager.get_epoch_chunk_producers(epoch_id)? {
                account_keys
                    .entry(cp.account_id().clone())
                    .or_default()
                    .insert(cp.public_key().clone());
            }
            for bp in self.epoch_manager.get_epoch_block_producers_ordered(epoch_id)? {
                account_keys
                    .entry(bp.account_id().clone())
                    .or_default()
                    .insert(bp.public_key().clone());
            }
        }
        let account_keys = Arc::new(account_keys);
        self.tier1_accounts_cache = Some((tip.epoch_id, account_keys.clone()));
        Ok(account_keys)
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
    pub(crate) fn send_network_chain_info(&mut self) -> Result<(), Error> {
        let tip = self.chain.head()?;
        // convert config tracked shards
        // runtime will track all shards if config tracked shards is not empty
        // https://github.com/near/nearcore/issues/4930
        let tracked_shards = if self.config.tracked_shards_config.tracks_all_shards() {
            self.epoch_manager.shard_ids(&tip.epoch_id)?
        } else {
            // TODO(archival_v2): Revisit this to determine if improvements can be made
            // and if the issue described above has been resolved.
            vec![]
        };
        let tier1_accounts = self.get_tier1_accounts(&tip)?;
        let block = self.chain.get_block(&tip.last_block_hash)?;
        self.network_adapter.send(SetChainInfo(ChainInfo {
            block,
            tracked_shards,
            tier1_accounts,
        }));
        Ok(())
    }
}

impl Client {
    pub fn get_catchup_status(&self) -> Result<Vec<CatchupStatusView>, near_chain::Error> {
        let mut ret = vec![];
        for (sync_hash, CatchupState { sync_status, catchup, .. }) in &self.catchup_state_syncs {
            let sync_block_height = self.chain.get_block_header(sync_hash)?.height();
            let shard_sync_status: HashMap<_, _> = sync_status
                .sync_status
                .iter()
                .map(|(shard_id, state)| (*shard_id, state.to_string()))
                .collect();
            ret.push(CatchupStatusView {
                sync_block_hash: *sync_hash,
                sync_block_height,
                shard_sync_status,
                blocks_to_catchup: self.chain.get_block_catchup_status(catchup),
            });
        }
        Ok(ret)
    }
}
