use crate::block_processing_utils::{
    ApplyChunksDoneTracker, BlockPreprocessInfo, BlockProcessingArtifact, BlocksInProcessing,
};
use crate::blocks_delay_tracker::BlocksDelayTracker;
use crate::chain_update::ChainUpdate;
use crate::crypto_hash_timer::CryptoHashTimer;
use crate::lightclient::get_epoch_block_producers_view;
use crate::migrations::check_if_block_is_first_with_chunk_of_version;
use crate::missing_chunks::MissingChunksPool;
use crate::orphan::{Orphan, OrphanBlockPool};
use crate::rayon_spawner::RayonAsyncComputationSpawner;
use crate::sharding::shuffle_receipt_proofs;
use crate::state_request_tracker::StateRequestTracker;
use crate::state_snapshot_actor::SnapshotCallbacks;
use crate::store::{ChainStore, ChainStoreAccess, ChainStoreUpdate};
use crate::types::{
    AcceptedBlock, ApplyChunkBlockContext, BlockEconomicsConfig, ChainConfig, RuntimeAdapter,
    StorageDataSource,
};
pub use crate::update_shard::{
    apply_new_chunk, apply_old_chunk, NewChunkData, NewChunkResult, OldChunkData, OldChunkResult,
    ShardContext, StorageContext,
};
use crate::update_shard::{
    process_shard_update, ReshardingData, ShardUpdateReason, ShardUpdateResult,
};
use crate::validate::{
    validate_challenge, validate_chunk_proofs, validate_chunk_with_chunk_extra,
    validate_transactions_order,
};
use crate::{
    byzantine_assert, create_light_client_block_view, BlockStatus, ChainGenesis, Doomslug,
    Provenance,
};
use crate::{metrics, DoomslugThresholdMode};
use borsh::BorshDeserialize;
use crossbeam_channel::{unbounded, Receiver, Sender};
use itertools::Itertools;
use lru::LruCache;
use near_async::futures::{AsyncComputationSpawner, AsyncComputationSpawnerExt};
use near_async::time::{Clock, Duration, Instant};
use near_chain_configs::{
    MutableConfigValue, MutableValidatorSigner, ReshardingConfig, ReshardingHandle,
};
#[cfg(feature = "new_epoch_sync")]
use near_chain_primitives::error::epoch_sync::EpochSyncInfoError;
use near_chain_primitives::error::{BlockKnownError, Error, LogTransientStorageError};
use near_epoch_manager::shard_tracker::ShardTracker;
use near_epoch_manager::types::BlockHeaderInfo;
use near_epoch_manager::EpochManagerAdapter;
use near_o11y::log_assert;
use near_primitives::block::{genesis_chunks, Block, BlockValidityError, Tip};
use near_primitives::block_header::BlockHeader;
use near_primitives::challenge::{
    BlockDoubleSign, Challenge, ChallengeBody, ChallengesResult, ChunkProofs, ChunkState,
    MaybeEncodedShardChunk, PartialState, SlashedValidator,
};
use near_primitives::checked_feature;
use near_primitives::congestion_info::CongestionInfo;
#[cfg(feature = "new_epoch_sync")]
use near_primitives::epoch_manager::epoch_sync::EpochSyncInfo;
#[cfg(feature = "new_epoch_sync")]
use near_primitives::errors::epoch_sync::EpochSyncHashType;
use near_primitives::errors::EpochError;
use near_primitives::hash::{hash, CryptoHash};
use near_primitives::merkle::{
    combine_hash, merklize, verify_path, Direction, MerklePath, MerklePathItem, PartialMerkleTree,
};
use near_primitives::receipt::Receipt;
use near_primitives::sandbox::state_patch::SandboxStatePatch;
use near_primitives::shard_layout::{account_id_to_shard_id, ShardLayout, ShardUId};
use near_primitives::sharding::{
    ChunkHash, ChunkHashHeight, EncodedShardChunk, ReceiptList, ReceiptProof, ShardChunk,
    ShardChunkHeader, ShardInfo, ShardProof, StateSyncInfo,
};
use near_primitives::state_part::PartId;
use near_primitives::state_sync::{
    get_num_state_parts, BitArray, CachedParts, ReceiptProofResponse, RootProof,
    ShardStateSyncResponseHeader, ShardStateSyncResponseHeaderV2, StateHeaderKey, StatePartKey,
};
use near_primitives::stateless_validation::{ChunkStateWitness, ChunkStateWitnessSize};
use near_primitives::transaction::{ExecutionOutcomeWithIdAndProof, SignedTransaction};
use near_primitives::types::chunk_extra::ChunkExtra;
use near_primitives::types::{
    AccountId, Balance, BlockExtra, BlockHeight, BlockHeightDelta, EpochId, Gas, MerkleHash,
    NumBlocks, ShardId, StateRoot,
};
use near_primitives::unwrap_or_return;
#[cfg(feature = "new_epoch_sync")]
use near_primitives::utils::index_to_bytes;
use near_primitives::utils::MaybeValidated;
use near_primitives::version::{ProtocolFeature, ProtocolVersion, PROTOCOL_VERSION};
use near_primitives::views::{
    BlockStatusView, DroppedReason, ExecutionOutcomeWithIdView, ExecutionStatusView,
    FinalExecutionOutcomeView, FinalExecutionOutcomeWithReceiptView, FinalExecutionStatus,
    LightClientBlockView, SignedTransactionView,
};
use near_store::config::StateSnapshotType;
use near_store::flat::{store_helper, FlatStorageReadyStatus, FlatStorageStatus};
use near_store::get_genesis_state_roots;
use near_store::DBCol;
use node_runtime::bootstrap_congestion_info;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::{Debug, Formatter};
use std::num::NonZeroUsize;
use std::sync::Arc;
use time::ext::InstantExt as _;
use tracing::{debug, debug_span, error, info, warn, Span};

/// The size of the invalid_blocks in-memory pool
pub const INVALID_CHUNKS_POOL_SIZE: usize = 5000;

/// 5000 years in seconds. Big constant for sandbox to allow time traveling.
#[cfg(feature = "sandbox")]
const ACCEPTABLE_TIME_DIFFERENCE: i64 = 60 * 60 * 24 * 365 * 5000;

// Number of parent blocks traversed to check if the block can be finalized.
const NUM_PARENTS_TO_CHECK_FINALITY: usize = 20;

/// Refuse blocks more than this many block intervals in the future (as in bitcoin).
#[cfg(not(feature = "sandbox"))]
const ACCEPTABLE_TIME_DIFFERENCE: i64 = 12 * 10;

/// Private constant for 1 NEAR (copy from near/config.rs) used for reporting.
const NEAR_BASE: Balance = 1_000_000_000_000_000_000_000_000;

/// apply_chunks may be called in two code paths, through process_block or through catchup_blocks
/// When it is called through process_block, it is possible that the shard state for the next epoch
/// has not been caught up yet, thus the two modes IsCaughtUp and NotCaughtUp.
/// CatchingUp is for when apply_chunks is called through catchup_blocks, this is to catch up the
/// shard states for the next epoch
#[derive(Eq, PartialEq, Copy, Clone, Debug)]
enum ApplyChunksMode {
    IsCaughtUp,
    CatchingUp,
    NotCaughtUp,
}

/// `ApplyChunksDoneMessage` is a message that signals the finishing of applying chunks of a block.
/// Upon receiving this message, ClientActors know that it's time to finish processing the blocks that
/// just finished applying chunks.
#[derive(actix::Message, Debug)]
#[rtype(result = "()")]
pub struct ApplyChunksDoneMessage;

/// Contains information for missing chunks in a block
pub struct BlockMissingChunks {
    /// previous block hash
    pub prev_hash: CryptoHash,
    pub missing_chunks: Vec<ShardChunkHeader>,
}

impl Debug for BlockMissingChunks {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlockMissingChunks")
            .field("prev_hash", &self.prev_hash)
            .field("num_missing_chunks", &self.missing_chunks.len())
            .finish()
    }
}

/// Check if block header is known
/// Returns Err(Error) if any error occurs when checking store
///         Ok(Err(BlockKnownError)) if the block header is known
///         Ok(Ok()) otherwise
pub fn check_header_known(
    chain: &Chain,
    header: &BlockHeader,
) -> Result<Result<(), BlockKnownError>, Error> {
    // TODO: Change the return type to Result<BlockKnownStatusEnum, Error>.
    let header_head = chain.chain_store().header_head()?;
    if header.hash() == &header_head.last_block_hash
        || header.hash() == &header_head.prev_block_hash
    {
        return Ok(Err(BlockKnownError::KnownInHeader));
    }
    check_known_store(chain, header.hash())
}

/// Check if this block is in the store already.
/// Returns Err(Error) if any error occurs when checking store
///         Ok(Err(BlockKnownError)) if the block is in the store
///         Ok(Ok()) otherwise
fn check_known_store(
    chain: &Chain,
    block_hash: &CryptoHash,
) -> Result<Result<(), BlockKnownError>, Error> {
    // TODO: Change the return type to Result<BlockKnownStatusEnum, Error>.
    if chain.chain_store().block_exists(block_hash)? {
        Ok(Err(BlockKnownError::KnownInStore))
    } else {
        // Not yet processed this block, we can proceed.
        Ok(Ok(()))
    }
}

/// Check if block is known: head, orphan, in processing or in store.
/// Returns Err(Error) if any error occurs when checking store
///         Ok(Err(BlockKnownError)) if the block is known
///         Ok(Ok()) otherwise
pub fn check_known(
    chain: &Chain,
    block_hash: &CryptoHash,
) -> Result<Result<(), BlockKnownError>, Error> {
    // TODO: Change the return type to Result<BlockKnownStatusEnum, Error>.
    let head = chain.chain_store().head()?;
    // Quick in-memory check for fast-reject any block handled recently.
    if block_hash == &head.last_block_hash || block_hash == &head.prev_block_hash {
        return Ok(Err(BlockKnownError::KnownInHead));
    }
    if chain.blocks_in_processing.contains(block_hash) {
        return Ok(Err(BlockKnownError::KnownInProcessing));
    }
    // Check if this block is in the set of known orphans.
    if chain.orphans.contains(block_hash) {
        return Ok(Err(BlockKnownError::KnownInOrphan));
    }
    if chain.blocks_with_missing_chunks.contains(block_hash) {
        return Ok(Err(BlockKnownError::KnownInMissingChunks));
    }
    if chain.is_block_invalid(block_hash) {
        return Ok(Err(BlockKnownError::KnownAsInvalid));
    }
    check_known_store(chain, block_hash)
}

type BlockApplyChunksResult = (CryptoHash, Vec<(ShardId, Result<ShardUpdateResult, Error>)>);

/// Facade to the blockchain block processing and storage.
/// Provides current view on the state according to the chain state.
pub struct Chain {
    pub(crate) clock: Clock,
    pub chain_store: ChainStore,
    pub epoch_manager: Arc<dyn EpochManagerAdapter>,
    pub shard_tracker: ShardTracker,
    pub runtime_adapter: Arc<dyn RuntimeAdapter>,
    pub(crate) orphans: OrphanBlockPool,
    pub blocks_with_missing_chunks: MissingChunksPool<Orphan>,
    genesis: Block,
    pub transaction_validity_period: NumBlocks,
    pub epoch_length: BlockHeightDelta,
    /// Block economics, relevant to changes when new block must be produced.
    pub block_economics_config: BlockEconomicsConfig,
    pub doomslug_threshold_mode: DoomslugThresholdMode,
    pub blocks_delay_tracker: BlocksDelayTracker,
    /// Processing a block is done in three stages: preprocess_block, async_apply_chunks and
    /// postprocess_block. The async_apply_chunks is done asynchronously from the ClientActor thread.
    /// `blocks_in_processing` keeps track of all the blocks that have been preprocessed but are
    /// waiting for chunks being applied.
    pub(crate) blocks_in_processing: BlocksInProcessing,
    /// Used by async_apply_chunks to send apply chunks results back to chain
    apply_chunks_sender: Sender<BlockApplyChunksResult>,
    /// Used to receive apply chunks results
    apply_chunks_receiver: Receiver<BlockApplyChunksResult>,
    /// Used to spawn the apply chunks jobs.
    apply_chunks_spawner: Arc<dyn AsyncComputationSpawner>,
    /// Time when head was updated most recently.
    last_time_head_updated: Instant,
    /// Prevents re-application of known-to-be-invalid blocks, so that in case of a
    /// protocol issue we can recover faster by focusing on correct blocks.
    invalid_blocks: LruCache<CryptoHash, ()>,

    /// Support for sandbox's patch_state requests.
    ///
    /// Sandbox needs ability to arbitrary modify the state. Blockchains
    /// naturally prevent state tampering, so we can't *just* modify data in
    /// place in the database. Instead, we will include this "bonus changes" in
    /// the next block we'll be processing, keeping them in this field in the
    /// meantime.
    ///
    /// Note that without `sandbox` feature enabled, `SandboxStatePatch` is
    /// a ZST.  All methods of the type are no-ops which behave as if the object
    /// was empty and could not hold any records (which it cannot).  It’s
    /// impossible to have non-empty state patch on non-sandbox builds.
    pending_state_patch: SandboxStatePatch,

    /// Used to store state parts already requested along with elapsed time
    /// to create the parts. This information is used for debugging
    pub(crate) requested_state_parts: StateRequestTracker,

    /// A callback to initiate state snapshot.
    snapshot_callbacks: Option<SnapshotCallbacks>,

    /// Configuration for resharding.
    pub(crate) resharding_config: MutableConfigValue<near_chain_configs::ReshardingConfig>,

    // A handle that allows the main process to interrupt resharding if needed.
    // This typically happens when the main process is interrupted.
    pub resharding_handle: ReshardingHandle,
}

impl Drop for Chain {
    fn drop(&mut self) {
        let _ = self.blocks_in_processing.wait_for_all_blocks();
    }
}

/// UpdateShardJob is a closure that is responsible for updating a shard for a single block.
/// Execution context (latest blocks/chunks details) are already captured within.
type UpdateShardJob =
    (ShardId, Box<dyn FnOnce(&Span) -> Result<ShardUpdateResult, Error> + Send + Sync + 'static>);

/// PreprocessBlockResult is a tuple where the first element is a vector of jobs
/// to update shards, the second element is BlockPreprocessInfo
type PreprocessBlockResult = (Vec<UpdateShardJob>, BlockPreprocessInfo);

// Used only for verify_block_hash_and_signature. See that method.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum VerifyBlockHashAndSignatureResult {
    Correct,
    Incorrect,
    CannotVerifyBecauseBlockIsOrphan,
}

impl Chain {
    /// Builds genesis block and chunks from the current configuration obtained through the arguments.
    pub fn make_genesis_block(
        epoch_manager: &dyn EpochManagerAdapter,
        runtime_adapter: &dyn RuntimeAdapter,
        chain_genesis: &ChainGenesis,
        state_roots: Vec<CryptoHash>,
    ) -> Result<(Block, Vec<ShardChunk>), Error> {
        let congestion_infos =
            get_genesis_congestion_infos(epoch_manager, runtime_adapter, &state_roots)?;
        let genesis_chunks = genesis_chunks(
            state_roots,
            congestion_infos,
            &epoch_manager.shard_ids(&EpochId::default())?,
            chain_genesis.gas_limit,
            chain_genesis.height,
            chain_genesis.protocol_version,
        );
        let genesis_block = Block::genesis(
            chain_genesis.protocol_version,
            genesis_chunks.iter().map(|chunk| chunk.cloned_header()).collect(),
            chain_genesis.time,
            chain_genesis.height,
            chain_genesis.min_gas_price,
            chain_genesis.total_supply,
            Chain::compute_bp_hash(
                epoch_manager,
                EpochId::default(),
                EpochId::default(),
                &CryptoHash::default(),
            )?,
        );
        Ok((genesis_block, genesis_chunks))
    }

    pub fn new_for_view_client(
        clock: Clock,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        shard_tracker: ShardTracker,
        runtime_adapter: Arc<dyn RuntimeAdapter>,
        chain_genesis: &ChainGenesis,
        doomslug_threshold_mode: DoomslugThresholdMode,
        save_trie_changes: bool,
    ) -> Result<Chain, Error> {
        let store = runtime_adapter.store();
        let chain_store = ChainStore::new(store.clone(), chain_genesis.height, save_trie_changes);
        let state_roots = get_genesis_state_roots(runtime_adapter.store())?
            .expect("genesis should be initialized.");
        let (genesis, _genesis_chunks) = Self::make_genesis_block(
            epoch_manager.as_ref(),
            runtime_adapter.as_ref(),
            chain_genesis,
            state_roots,
        )?;
        let (sc, rc) = unbounded();
        Ok(Chain {
            clock: clock.clone(),
            chain_store,
            epoch_manager,
            shard_tracker,
            runtime_adapter,
            orphans: OrphanBlockPool::new(),
            blocks_with_missing_chunks: MissingChunksPool::new(),
            blocks_in_processing: BlocksInProcessing::new(),
            genesis,
            transaction_validity_period: chain_genesis.transaction_validity_period,
            epoch_length: chain_genesis.epoch_length,
            block_economics_config: BlockEconomicsConfig::from(chain_genesis),
            doomslug_threshold_mode,
            blocks_delay_tracker: BlocksDelayTracker::new(clock.clone()),
            apply_chunks_sender: sc,
            apply_chunks_receiver: rc,
            apply_chunks_spawner: Arc::new(RayonAsyncComputationSpawner),
            last_time_head_updated: clock.now(),
            invalid_blocks: LruCache::new(NonZeroUsize::new(INVALID_CHUNKS_POOL_SIZE).unwrap()),
            pending_state_patch: Default::default(),
            requested_state_parts: StateRequestTracker::new(),
            snapshot_callbacks: None,
            resharding_config: MutableConfigValue::new(
                ReshardingConfig::default(),
                "resharding_config",
            ),
            resharding_handle: ReshardingHandle::new(),
        })
    }

    pub fn new(
        clock: Clock,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        shard_tracker: ShardTracker,
        runtime_adapter: Arc<dyn RuntimeAdapter>,
        chain_genesis: &ChainGenesis,
        doomslug_threshold_mode: DoomslugThresholdMode,
        chain_config: ChainConfig,
        snapshot_callbacks: Option<SnapshotCallbacks>,
        apply_chunks_spawner: Arc<dyn AsyncComputationSpawner>,
        validator: MutableValidatorSigner,
    ) -> Result<Chain, Error> {
        let state_roots = get_genesis_state_roots(runtime_adapter.store())?
            .expect("genesis should be initialized.");
        let (genesis, genesis_chunks) = Self::make_genesis_block(
            epoch_manager.as_ref(),
            runtime_adapter.as_ref(),
            chain_genesis,
            state_roots.clone(),
        )?;

        // Check if we have a head in the store, otherwise pick genesis block.
        let mut chain_store = ChainStore::new(
            runtime_adapter.store().clone(),
            chain_genesis.height,
            chain_config.save_trie_changes,
        );
        let mut store_update = chain_store.store_update();
        let (block_head, header_head) = match store_update.head() {
            Ok(block_head) => {
                // Check that genesis in the store is the same as genesis given in the config.
                let genesis_hash = store_update.get_block_hash_by_height(chain_genesis.height)?;
                if &genesis_hash != genesis.hash() {
                    return Err(Error::Other(format!(
                        "Genesis mismatch between storage and config: {:?} vs {:?}",
                        genesis_hash,
                        genesis.hash()
                    )));
                }

                // Check we have the header corresponding to the header_head.
                let mut header_head = store_update.header_head()?;
                if store_update.get_block_header(&header_head.last_block_hash).is_err() {
                    // Reset header head and "sync" head to be consistent with current block head.
                    store_update.save_header_head_if_not_challenged(&block_head)?;
                    header_head = block_head.clone();
                }

                // TODO: perform validation that latest state in runtime matches the stored chain.

                (block_head, header_head)
            }
            Err(Error::DBNotFoundErr(_)) => {
                for chunk in genesis_chunks {
                    store_update.save_chunk(chunk.clone());
                }
                store_update.merge(epoch_manager.add_validator_proposals(BlockHeaderInfo::new(
                    genesis.header(),
                    // genesis height is considered final
                    chain_genesis.height,
                ))?);
                store_update.save_block_header(genesis.header().clone())?;
                store_update.save_block(genesis.clone());
                store_update
                    .save_block_extra(genesis.hash(), BlockExtra { challenges_result: vec![] });

                for (chunk_header, state_root) in genesis.chunks().iter().zip(state_roots.iter()) {
                    let congestion_info = if ProtocolFeature::CongestionControl
                        .enabled(chain_genesis.protocol_version)
                    {
                        genesis
                            .block_congestion_info()
                            .get(&chunk_header.shard_id())
                            .map(|info| info.congestion_info)
                    } else {
                        None
                    };

                    store_update.save_chunk_extra(
                        genesis.hash(),
                        &epoch_manager
                            .shard_id_to_uid(chunk_header.shard_id(), &EpochId::default())?,
                        Self::create_genesis_chunk_extra(
                            state_root,
                            chain_genesis.gas_limit,
                            chain_genesis.protocol_version,
                            congestion_info,
                        ),
                    );
                }

                let block_head = Tip::from_header(genesis.header());
                let header_head = block_head.clone();
                store_update.save_head(&block_head)?;
                store_update.save_final_head(&header_head)?;

                // Set the root block of flat state to be the genesis block. Later, when we
                // init FlatStorages, we will read the from this column in storage, so it
                // must be set here.
                let flat_storage_manager = runtime_adapter.get_flat_storage_manager();
                let genesis_epoch_id = genesis.header().epoch_id();
                let mut tmp_store_update = store_update.store().store_update();
                for shard_uid in epoch_manager.get_shard_layout(genesis_epoch_id)?.shard_uids() {
                    flat_storage_manager.set_flat_storage_for_genesis(
                        &mut tmp_store_update,
                        shard_uid,
                        genesis.hash(),
                        genesis.header().height(),
                    )
                }
                store_update.merge(tmp_store_update);

                info!(target: "chain", "Init: saved genesis: #{} {} / {:?}", block_head.height, block_head.last_block_hash, state_roots);

                (block_head, header_head)
            }
            Err(err) => return Err(err),
        };
        store_update.commit()?;

        // We must load in-memory tries here, and not inside runtime, because
        // if we were initializing from genesis, the runtime would be
        // initialized when no blocks or flat storage were initialized. We
        // require flat storage in order to load in-memory tries.
        // TODO(#9511): The calculation of shard UIDs is not precise in the case
        // of resharding. We need to revisit this.
        let tip = chain_store.head()?;
        let shard_uids: Vec<_> =
            epoch_manager.get_shard_layout(&tip.epoch_id)?.shard_uids().collect();
        let tracked_shards: Vec<_> = shard_uids
            .iter()
            .filter(|shard_uid| {
                shard_tracker.care_about_shard(
                    validator.get().map(|v| v.validator_id().clone()).as_ref(),
                    &tip.prev_block_hash,
                    shard_uid.shard_id(),
                    true,
                )
            })
            .cloned()
            .collect();
        runtime_adapter.get_tries().load_mem_tries_for_enabled_shards(&tracked_shards, true)?;

        info!(target: "chain", "Init: header head @ #{} {}; block head @ #{} {}",
              header_head.height, header_head.last_block_hash,
              block_head.height, block_head.last_block_hash);
        metrics::BLOCK_HEIGHT_HEAD.set(block_head.height as i64);
        let block_header = chain_store.get_block_header(&block_head.last_block_hash)?;
        metrics::BLOCK_ORDINAL_HEAD.set(block_header.block_ordinal() as i64);
        metrics::HEADER_HEAD_HEIGHT.set(header_head.height as i64);
        metrics::BOOT_TIME_SECONDS.set(clock.now_utc().unix_timestamp());

        metrics::TAIL_HEIGHT.set(chain_store.tail()? as i64);
        metrics::CHUNK_TAIL_HEIGHT.set(chain_store.chunk_tail()? as i64);
        metrics::FORK_TAIL_HEIGHT.set(chain_store.fork_tail()? as i64);

        // Even though the channel is unbounded, the channel size is practically bounded by the size
        // of blocks_in_processing, which is set to 5 now.
        let (sc, rc) = unbounded();
        Ok(Chain {
            clock: clock.clone(),
            chain_store,
            epoch_manager,
            shard_tracker,
            runtime_adapter,
            orphans: OrphanBlockPool::new(),
            blocks_with_missing_chunks: MissingChunksPool::new(),
            blocks_in_processing: BlocksInProcessing::new(),
            invalid_blocks: LruCache::new(NonZeroUsize::new(INVALID_CHUNKS_POOL_SIZE).unwrap()),
            genesis: genesis.clone(),
            transaction_validity_period: chain_genesis.transaction_validity_period,
            epoch_length: chain_genesis.epoch_length,
            block_economics_config: BlockEconomicsConfig::from(chain_genesis),
            doomslug_threshold_mode,
            blocks_delay_tracker: BlocksDelayTracker::new(clock.clone()),
            apply_chunks_sender: sc,
            apply_chunks_receiver: rc,
            apply_chunks_spawner,
            last_time_head_updated: clock.now(),
            pending_state_patch: Default::default(),
            requested_state_parts: StateRequestTracker::new(),
            snapshot_callbacks,
            resharding_config: chain_config.resharding_config,
            resharding_handle: ReshardingHandle::new(),
        })
    }

    #[cfg(feature = "test_features")]
    pub fn adv_disable_doomslug(&mut self) {
        self.doomslug_threshold_mode = DoomslugThresholdMode::NoApprovals
    }

    pub fn compute_bp_hash(
        epoch_manager: &dyn EpochManagerAdapter,
        epoch_id: EpochId,
        prev_epoch_id: EpochId,
        last_known_hash: &CryptoHash,
    ) -> Result<CryptoHash, Error> {
        let bps = epoch_manager.get_epoch_block_producers_ordered(&epoch_id, last_known_hash)?;
        let protocol_version = epoch_manager.get_epoch_protocol_version(&prev_epoch_id)?;
        if checked_feature!("stable", BlockHeaderV3, protocol_version) {
            let validator_stakes = bps.into_iter().map(|(bp, _)| bp);
            Ok(CryptoHash::hash_borsh_iter(validator_stakes))
        } else {
            let validator_stakes = bps.into_iter().map(|(bp, _)| bp.into_v1());
            Ok(CryptoHash::hash_borsh_iter(validator_stakes))
        }
    }

    pub fn get_last_time_head_updated(&self) -> Instant {
        self.last_time_head_updated
    }

    fn create_genesis_chunk_extra(
        state_root: &StateRoot,
        gas_limit: Gas,
        genesis_protocol_version: ProtocolVersion,
        congestion_info: Option<CongestionInfo>,
    ) -> ChunkExtra {
        ChunkExtra::new(
            genesis_protocol_version,
            state_root,
            CryptoHash::default(),
            vec![],
            0,
            gas_limit,
            0,
            congestion_info,
        )
    }

    pub fn genesis_chunk_extra(
        &self,
        shard_id: ShardId,
        genesis_protocol_version: ProtocolVersion,
        congestion_info: Option<CongestionInfo>,
    ) -> Result<ChunkExtra, Error> {
        let shard_index = shard_id as usize;
        let state_root = *get_genesis_state_roots(self.chain_store.store())?
            .ok_or_else(|| Error::Other("genesis state roots do not exist in the db".to_owned()))?
            .get(shard_index)
            .ok_or_else(|| {
                Error::Other(format!("genesis state root does not exist for shard {shard_index}"))
            })?;
        let gas_limit = self
            .genesis
            .chunks()
            .get(shard_index)
            .ok_or_else(|| {
                Error::Other(format!("genesis chunk does not exist for shard {shard_index}"))
            })?
            .gas_limit();
        Ok(Self::create_genesis_chunk_extra(
            &state_root,
            gas_limit,
            genesis_protocol_version,
            congestion_info,
        ))
    }

    /// Creates a light client block for the last final block from perspective of some other block
    ///
    /// # Arguments
    ///  * `header` - the last finalized block seen from `header` (not pushed back) will be used to
    ///               compute the light client block
    pub fn create_light_client_block(
        header: &BlockHeader,
        epoch_manager: &dyn EpochManagerAdapter,
        chain_store: &dyn ChainStoreAccess,
    ) -> Result<LightClientBlockView, Error> {
        let final_block_header = {
            let ret = chain_store.get_block_header(header.last_final_block())?;
            let two_ahead = chain_store.get_block_header_by_height(ret.height() + 2)?;
            if two_ahead.epoch_id() != ret.epoch_id() {
                let one_ahead = chain_store.get_block_header_by_height(ret.height() + 1)?;
                if one_ahead.epoch_id() != ret.epoch_id() {
                    let new_final_hash = *ret.last_final_block();
                    chain_store.get_block_header(&new_final_hash)?
                } else {
                    let new_final_hash = *one_ahead.last_final_block();
                    chain_store.get_block_header(&new_final_hash)?
                }
            } else {
                ret
            }
        };

        let next_block_producers = get_epoch_block_producers_view(
            final_block_header.next_epoch_id(),
            header.prev_hash(),
            epoch_manager,
        )?;

        create_light_client_block_view(&final_block_header, chain_store, Some(next_block_producers))
    }

    pub fn save_block(&mut self, block: MaybeValidated<Block>) -> Result<(), Error> {
        if self.chain_store.get_block(block.hash()).is_ok() {
            return Ok(());
        }
        let mut chain_store_update = ChainStoreUpdate::new(&mut self.chain_store);

        chain_store_update.save_block(block.into_inner());
        // We don't need to increase refcount for `prev_hash` at this point
        // because this is the block before State Sync.

        chain_store_update.commit()?;
        Ok(())
    }

    fn save_block_height_processed(&mut self, block_height: BlockHeight) -> Result<(), Error> {
        let mut chain_store_update = ChainStoreUpdate::new(&mut self.chain_store);
        if !chain_store_update.is_height_processed(block_height)? {
            chain_store_update.save_block_height_processed(block_height);
        }
        chain_store_update.commit()?;
        Ok(())
    }

    fn maybe_mark_block_invalid(&mut self, block_hash: CryptoHash, error: &Error) {
        // We only mark the block as invalid if the block has bad data (not for other errors that would
        // not be the fault of the block itself), except when the block has a bad signature which means
        // the block might not have been what the block producer originally produced. Either way, it's
        // OK if we miss some cases here because this is just an optimization to avoid reprocessing
        // known invalid blocks so the network recovers faster in case of any issues.
        if error.is_bad_data() && !matches!(error, Error::InvalidSignature) {
            metrics::NUM_INVALID_BLOCKS.with_label_values(&[error.prometheus_label_value()]).inc();
            self.invalid_blocks.put(block_hash, ());
        }
    }

    /// Return a StateSyncInfo that includes the information needed for syncing state for shards needed
    /// in the next epoch.
    fn get_state_sync_info(
        &self,
        me: &Option<AccountId>,
        block: &Block,
    ) -> Result<Option<StateSyncInfo>, Error> {
        let prev_hash = *block.header().prev_hash();
        let shards_to_state_sync = Chain::get_shards_to_state_sync(
            self.epoch_manager.as_ref(),
            &self.shard_tracker,
            me,
            &prev_hash,
        )?;
        let prev_block = self.get_block(&prev_hash)?;

        if prev_block.chunks().len() != block.chunks().len() && !shards_to_state_sync.is_empty() {
            // Currently, the state sync algorithm assumes that the number of chunks do not change
            // between the epoch being synced to and the last epoch.
            // For example, if shard layout changes at the beginning of epoch T, validators
            // will not be able to sync states at epoch T for epoch T+1
            // Fortunately, since all validators track all shards for now, this error will not be
            // triggered in live yet
            // Instead of propagating the error, we simply log the error here because the error
            // do not affect processing blocks for this epoch. However, when the next epoch comes,
            // the validator will not have the states ready so it will halt.
            error!(
                "Cannot download states for epoch {:?} because sharding just changed. I'm {:?}",
                block.header().epoch_id(),
                me
            );
            debug_assert!(false);
        }
        if shards_to_state_sync.is_empty() {
            Ok(None)
        } else {
            debug!(target: "chain", "Downloading state for {:?}, I'm {:?}", shards_to_state_sync, me);

            let state_sync_info = StateSyncInfo {
                epoch_tail_hash: *block.header().hash(),
                shards: shards_to_state_sync
                    .iter()
                    .map(|shard_id| {
                        let chunk = &prev_block.chunks()[*shard_id as usize];
                        ShardInfo(*shard_id, chunk.chunk_hash())
                    })
                    .collect(),
            };

            Ok(Some(state_sync_info))
        }
    }

    /// Do basic validation of a block upon receiving it. Check that block is
    /// well-formed (various roots match).
    pub fn validate_block(&self, block: &MaybeValidated<Block>) -> Result<(), Error> {
        block
            .validate_with(|block| {
                Chain::validate_block_impl(self.epoch_manager.as_ref(), self.genesis_block(), block)
                    .map(|_| true)
            })
            .map(|_| ())
    }

    fn validate_block_impl(
        epoch_manager: &dyn EpochManagerAdapter,
        genesis_block: &Block,
        block: &Block,
    ) -> Result<(), Error> {
        for (shard_id, chunk_header) in block.chunks().iter().enumerate() {
            if chunk_header.height_created() == genesis_block.header().height() {
                // Special case: genesis chunks can be in non-genesis blocks and don't have a signature
                // We must verify that content matches and signature is empty.
                // TODO: this code will not work when genesis block has different number of chunks as the current block
                // https://github.com/near/nearcore/issues/4908
                let chunks = genesis_block.chunks();
                let genesis_chunk = chunks.get(shard_id);
                let genesis_chunk = genesis_chunk.ok_or_else(|| {
                    Error::InvalidChunk(format!(
                        "genesis chunk not found for shard {}, genesis block has {} chunks",
                        shard_id,
                        chunks.len(),
                    ))
                })?;

                if genesis_chunk.chunk_hash() != chunk_header.chunk_hash()
                    || genesis_chunk.signature() != chunk_header.signature()
                {
                    return Err(Error::InvalidChunk(format!(
                        "genesis chunk mismatch for shard {}. genesis chunk hash: {:?}, chunk hash: {:?}, genesis signature: {}, chunk signature: {}",
                        shard_id,
                        genesis_chunk.chunk_hash(),
                        chunk_header.chunk_hash(),
                        genesis_chunk.signature(),
                        chunk_header.signature()
                    )));
                }
            } else if chunk_header.height_created() == block.header().height() {
                if chunk_header.shard_id() != shard_id as ShardId {
                    return Err(Error::InvalidShardId(chunk_header.shard_id()));
                }
                if !epoch_manager.verify_chunk_header_signature(
                    &chunk_header.clone(),
                    block.header().epoch_id(),
                    block.header().prev_hash(),
                )? {
                    byzantine_assert!(false);
                    return Err(Error::InvalidChunk(format!(
                        "Invalid chunk header signature for shard {}, chunk hash: {:?}",
                        shard_id,
                        chunk_header.chunk_hash()
                    )));
                }
            }
        }
        block.check_validity().map_err(|e| <BlockValidityError as Into<Error>>::into(e))?;
        Ok(())
    }

    /// Verify header signature when the epoch is known, but not the whole chain.
    /// Same as verify_header_signature except it does not verify that block producer hasn't been slashed
    fn partial_verify_orphan_header_signature(&self, header: &BlockHeader) -> Result<bool, Error> {
        let block_producer =
            self.epoch_manager.get_block_producer(header.epoch_id(), header.height())?;
        // DEVNOTE: we pass head which is not necessarily on block's chain, but it's only used for
        // slashing info which we will ignore
        let head = self.head()?;
        let (block_producer, _slashed) = self.epoch_manager.get_validator_by_account_id(
            header.epoch_id(),
            &head.last_block_hash,
            &block_producer,
        )?;
        Ok(header.signature().verify(header.hash().as_ref(), block_producer.public_key()))
    }

    /// Optimization which checks if block with the given header can be reached from final head, and thus can be
    /// finalized by this node.
    /// If this is the case, returns Ok.
    /// If we discovered that it is not the case, returns `Error::CannotBeFinalized`.
    /// If too many parents were checked, returns Ok to avoid long delays.
    fn check_if_finalizable(&self, header: &BlockHeader) -> Result<(), Error> {
        let mut header = header.clone();
        let final_head = self.final_head()?;
        for _ in 0..NUM_PARENTS_TO_CHECK_FINALITY {
            // If we reached final head, then block can be finalized.
            if header.hash() == &final_head.last_block_hash {
                return Ok(());
            }
            // If we went behind final head, then block cannot be finalized on top of final head.
            if header.height() < final_head.height {
                return Err(Error::CannotBeFinalized);
            }
            // Otherwise go to parent block.
            header = match self.get_previous_header(&header) {
                Ok(header) => header,
                Err(_) => {
                    // We couldn't find previous header. Return Ok because it can be an orphaned block which can be
                    // connected to canonical chain later.
                    return Ok(());
                }
            }
        }

        // If we traversed too many blocks, return Ok to avoid long delays.
        Ok(())
    }

    /// Validate header. Returns error if the header is invalid.
    /// `challenges`: the function will add new challenges generated from validating this header
    ///               to the vector. You can pass an empty vector here, or a vector with existing
    ///               challenges already.
    fn validate_header(
        &self,
        header: &BlockHeader,
        provenance: &Provenance,
        challenges: &mut Vec<ChallengeBody>,
    ) -> Result<(), Error> {
        // Refuse blocks from the too distant future.
        if header.timestamp() > self.clock.now_utc() + Duration::seconds(ACCEPTABLE_TIME_DIFFERENCE)
        {
            return Err(Error::InvalidBlockFutureTime(header.timestamp()));
        }

        // Check the signature.
        if !self.epoch_manager.verify_header_signature(header)? {
            return Err(Error::InvalidSignature);
        }

        // Check we don't know a block with given height already.
        // If we do - send out double sign challenge and keep going as double signed blocks are valid blocks.
        // Check if there is already known block of the same height that has the same epoch id
        if let Some(block_hashes) =
            self.chain_store.get_all_block_hashes_by_height(header.height())?.get(header.epoch_id())
        {
            // This should be guaranteed but it doesn't hurt to check again
            if !block_hashes.contains(header.hash()) {
                let other_header = self.get_block_header(block_hashes.iter().next().unwrap())?;

                challenges.push(ChallengeBody::BlockDoubleSign(BlockDoubleSign {
                    left_block_header: borsh::to_vec(&header).expect("Failed to serialize"),
                    right_block_header: borsh::to_vec(&other_header).expect("Failed to serialize"),
                }));
            }
        }

        #[cfg(feature = "protocol_feature_reject_blocks_with_outdated_protocol_version")]
        if let Ok(epoch_protocol_version) =
            self.epoch_manager.get_epoch_protocol_version(header.epoch_id())
        {
            if checked_feature!(
                "protocol_feature_reject_blocks_with_outdated_protocol_version",
                RejectBlocksWithOutdatedProtocolVersions,
                epoch_protocol_version
            ) {
                if header.latest_protocol_version() < epoch_protocol_version {
                    error!(
                        "header protocol version {} smaller than epoch protocol version {}",
                        header.latest_protocol_version(),
                        epoch_protocol_version
                    );
                    return Err(Error::InvalidProtocolVersion);
                }
            }
        }

        let prev_header = self.get_previous_header(header)?;

        // Check that epoch_id in the header does match epoch given previous header (only if previous header is present).
        let epoch_id_from_prev_block =
            &self.epoch_manager.get_epoch_id_from_prev_block(header.prev_hash())?;
        let epoch_id_from_header = header.epoch_id();
        if epoch_id_from_prev_block != epoch_id_from_header {
            return Err(Error::InvalidEpochHash);
        }

        // Check that epoch_id in the header does match epoch given previous header (only if previous header is present).
        if &self.epoch_manager.get_next_epoch_id_from_prev_block(header.prev_hash())?
            != header.next_epoch_id()
        {
            return Err(Error::InvalidEpochHash);
        }

        if header.epoch_id() == prev_header.epoch_id() {
            if header.next_bp_hash() != prev_header.next_bp_hash() {
                return Err(Error::InvalidNextBPHash);
            }
        } else {
            if header.next_bp_hash()
                != &Chain::compute_bp_hash(
                    self.epoch_manager.as_ref(),
                    *header.next_epoch_id(),
                    *header.epoch_id(),
                    header.prev_hash(),
                )?
            {
                return Err(Error::InvalidNextBPHash);
            }
        }

        if header.chunk_mask().len() != self.epoch_manager.shard_ids(header.epoch_id())?.len() {
            return Err(Error::InvalidChunkMask);
        }

        if !header.verify_chunks_included() {
            return Err(Error::InvalidChunkMask);
        }

        if let Some(prev_height) = header.prev_height() {
            if prev_height != prev_header.height() {
                return Err(Error::Other("Invalid prev_height".to_string()));
            }
        }

        // Prevent time warp attacks and some timestamp manipulations by forcing strict
        // time progression.
        if header.raw_timestamp() <= prev_header.raw_timestamp() {
            return Err(Error::InvalidBlockPastTime(prev_header.timestamp(), header.timestamp()));
        }
        // If this is not the block we produced (hence trust in it) - validates block
        // producer, confirmation signatures and finality info.
        if *provenance != Provenance::PRODUCED {
            // first verify aggregated signature
            if !self.epoch_manager.verify_approval(
                prev_header.hash(),
                prev_header.height(),
                header.height(),
                header.approvals(),
            )? {
                return Err(Error::InvalidApprovals);
            };

            let stakes = self
                .epoch_manager
                .get_epoch_block_approvers_ordered(header.prev_hash())?
                .iter()
                .map(|(x, is_slashed)| (x.stake_this_epoch, x.stake_next_epoch, *is_slashed))
                .collect::<Vec<_>>();
            if !Doomslug::can_approved_block_be_produced(
                self.doomslug_threshold_mode,
                header.approvals(),
                &stakes,
            ) {
                return Err(Error::NotEnoughApprovals);
            }

            let expected_last_ds_final_block = if prev_header.height() + 1 == header.height() {
                prev_header.hash()
            } else {
                prev_header.last_ds_final_block()
            };

            let expected_last_final_block = if prev_header.height() + 1 == header.height()
                && prev_header.last_ds_final_block() == prev_header.prev_hash()
            {
                prev_header.prev_hash()
            } else {
                prev_header.last_final_block()
            };

            if header.last_ds_final_block() != expected_last_ds_final_block
                || header.last_final_block() != expected_last_final_block
            {
                return Err(Error::InvalidFinalityInfo);
            }

            let block_merkle_tree = self.chain_store.get_block_merkle_tree(header.prev_hash())?;
            let mut block_merkle_tree = PartialMerkleTree::clone(&block_merkle_tree);
            block_merkle_tree.insert(*header.prev_hash());
            if &block_merkle_tree.root() != header.block_merkle_root() {
                return Err(Error::InvalidBlockMerkleRoot);
            }

            // Check that challenges root is empty to ensure later that block doesn't contain challenges.
            // TODO (#2445): Enable challenges when they are working correctly.
            if header.challenges_root() != &MerkleHash::default() {
                return Err(Error::InvalidChallengeRoot);
            }
            if !header.challenges_result().is_empty() {
                return Err(Error::InvalidChallenge);
            }
        }

        Ok(())
    }

    /// Process block header as part of "header first" block propagation.
    /// We validate the header but we do not store it or update header head
    /// based on this. We will update these once we get the block back after
    /// requesting it.
    pub fn process_block_header(
        &self,
        header: &BlockHeader,
        challenges: &mut Vec<ChallengeBody>,
    ) -> Result<(), Error> {
        debug!(target: "chain", block_hash=?header.hash(), height=header.height(), "process_block_header");

        check_known(self, header.hash())?.map_err(|e| Error::BlockKnown(e))?;
        self.validate_header(header, &Provenance::NONE, challenges)?;
        Ok(())
    }

    /// Verify that the block signature and block body hash matches. It makes sure that the block
    /// content is not tampered by a middle man.
    /// Returns Correct if the both check succeeds. Returns Incorrect if either check fails.
    /// Returns CannotVerifyBecauseBlockIsOrphan, if we could not verify the signature because
    /// the parent block is not yet available.
    pub fn verify_block_hash_and_signature(
        &self,
        block: &Block,
    ) -> Result<VerifyBlockHashAndSignatureResult, Error> {
        // skip the verification if we are processing the genesis block
        if block.hash() == self.genesis.hash() {
            return Ok(VerifyBlockHashAndSignatureResult::Correct);
        }
        let epoch_id = match self.epoch_manager.get_epoch_id(block.header().prev_hash()) {
            Ok(epoch_id) => epoch_id,
            Err(EpochError::MissingBlock(missing_block))
                if &missing_block == block.header().prev_hash() =>
            {
                return Ok(VerifyBlockHashAndSignatureResult::CannotVerifyBecauseBlockIsOrphan);
            }
            Err(err) => return Err(err.into()),
        };
        let epoch_protocol_version = self.epoch_manager.get_epoch_protocol_version(&epoch_id)?;

        // Check that block body hash matches the block body. This makes sure that the block body
        // content is not tampered
        if checked_feature!("stable", BlockHeaderV4, epoch_protocol_version) {
            let block_body_hash = block.compute_block_body_hash();
            if block_body_hash.is_none() {
                tracing::warn!("Block version too old for block: {:?}", block.hash());
                return Ok(VerifyBlockHashAndSignatureResult::Incorrect);
            }
            if block.header().block_body_hash() != block_body_hash {
                tracing::warn!("Invalid block body hash for block: {:?}", block.hash());
                return Ok(VerifyBlockHashAndSignatureResult::Incorrect);
            }
        }

        // Verify the signature. Since the signature is signed on the hash of block header, this check
        // makes sure the block header content is not tampered
        if !self.epoch_manager.verify_header_signature(block.header())? {
            tracing::error!("wrong signature");
            return Ok(VerifyBlockHashAndSignatureResult::Incorrect);
        }
        Ok(VerifyBlockHashAndSignatureResult::Correct)
    }

    /// Verify that `challenges` are valid
    /// If all challenges are valid, returns ChallengesResult, which comprises of the list of
    /// validators that need to be slashed and the list of blocks that are challenged.
    /// Returns Error if any challenge is invalid.
    /// Note: you might be wondering why the list of challenged blocks is not part of ChallengesResult.
    /// That's because ChallengesResult is part of BlockHeader, to modify that struct requires protocol
    /// upgrade.
    pub fn verify_challenges(
        &self,
        challenges: &[Challenge],
        epoch_id: &EpochId,
        prev_block_hash: &CryptoHash,
    ) -> Result<(ChallengesResult, Vec<CryptoHash>), Error> {
        let _span = tracing::debug_span!(
            target: "chain",
            "verify_challenges",
            ?challenges)
        .entered();
        let mut result = vec![];
        let mut challenged_blocks = vec![];
        for challenge in challenges.iter() {
            match validate_challenge(
                self.epoch_manager.as_ref(),
                self.runtime_adapter.as_ref(),
                epoch_id,
                prev_block_hash,
                challenge,
            ) {
                Ok((hash, account_ids)) => {
                    let is_double_sign = match challenge.body {
                        // If it's double signed block, we don't invalidate blocks just slash.
                        ChallengeBody::BlockDoubleSign(_) => true,
                        _ => {
                            challenged_blocks.push(hash);
                            false
                        }
                    };
                    let slash_validators: Vec<_> = account_ids
                        .into_iter()
                        .map(|id| SlashedValidator::new(id, is_double_sign))
                        .collect();
                    result.extend(slash_validators);
                }
                Err(Error::MaliciousChallenge) => {
                    result.push(SlashedValidator::new(challenge.account_id.clone(), false));
                }
                Err(err) => return Err(err),
            }
        }
        Ok((result, challenged_blocks))
    }

    /// Do basic validation of the information that we can get from the chunk headers in `block`
    fn validate_chunk_headers(&self, block: &Block, prev_block: &Block) -> Result<(), Error> {
        let prev_chunk_headers =
            Chain::get_prev_chunk_headers(self.epoch_manager.as_ref(), prev_block)?;
        for (chunk_header, prev_chunk_header) in
            block.chunks().iter().zip(prev_chunk_headers.iter())
        {
            if chunk_header.height_included() == block.header().height() {
                // new chunk
                if chunk_header.prev_block_hash() != block.header().prev_hash() {
                    return Err(Error::InvalidChunk(format!(
                        "Invalid prev_block_hash, chunk hash {:?}, chunk prev block hash {}, block prev block hash {}",
                        chunk_header.chunk_hash(),
                        chunk_header.prev_block_hash(),
                        block.header().prev_hash()
                    )));
                }
            } else {
                // old chunk
                if prev_chunk_header != chunk_header {
                    return Err(Error::InvalidChunk(format!(
                        "Invalid chunk header, prev chunk hash {:?}, chunk hash {:?}",
                        prev_chunk_header.chunk_hash(),
                        chunk_header.chunk_hash()
                    )));
                }
            }
        }

        // Verify that proposals from chunks match block header proposals.
        let block_height = block.header().height();
        for pair in block
            .chunks()
            .iter()
            .filter(|chunk| chunk.is_new_chunk(block_height))
            .flat_map(|chunk| chunk.prev_validator_proposals())
            .zip_longest(block.header().prev_validator_proposals())
        {
            match pair {
                itertools::EitherOrBoth::Both(cp, hp) => {
                    if hp != cp {
                        // Proposals differed!
                        return Err(Error::InvalidValidatorProposals);
                    }
                }
                _ => {
                    // Can only occur if there were a different number of proposals in the header
                    // and chunks
                    return Err(Error::InvalidValidatorProposals);
                }
            }
        }

        Ok(())
    }

    /// Check if the chain leading to the given block has challenged blocks on it. Returns Ok if the chain
    /// does not have challenged blocks, otherwise error ChallengedBlockOnChain.
    fn check_if_challenged_block_on_chain(&self, block_header: &BlockHeader) -> Result<(), Error> {
        let mut hash = *block_header.hash();
        let mut height = block_header.height();
        let mut prev_hash = *block_header.prev_hash();
        loop {
            match self.get_block_hash_by_height(height) {
                Ok(cur_hash) if cur_hash == hash => {
                    // Found common ancestor.
                    return Ok(());
                }
                _ => {
                    if self.chain_store.is_block_challenged(&hash)? {
                        return Err(Error::ChallengedBlockOnChain);
                    }
                    let prev_header = self.get_block_header(&prev_hash)?;
                    hash = *prev_header.hash();
                    height = prev_header.height();
                    prev_hash = *prev_header.prev_hash();
                }
            };
        }
    }

    pub fn ping_missing_chunks(
        &self,
        me: &Option<AccountId>,
        parent_hash: CryptoHash,
        block: &Block,
    ) -> Result<(), Error> {
        if !self.care_about_any_shard_or_part(me, parent_hash)? {
            return Ok(());
        }
        let mut missing = vec![];
        let block_height = block.header().height();
        for (shard_id, chunk_header) in block.chunks().iter().enumerate() {
            // Check if any chunks are invalid in this block.
            if let Some(encoded_chunk) =
                self.chain_store.is_invalid_chunk(&chunk_header.chunk_hash())?
            {
                let merkle_paths = Block::compute_chunk_headers_root(block.chunks().iter()).1;
                let merkle_proof = merkle_paths
                    .get(shard_id)
                    .ok_or_else(|| Error::InvalidShardId(shard_id as ShardId))?;
                let chunk_proof = ChunkProofs {
                    block_header: borsh::to_vec(&block.header()).expect("Failed to serialize"),
                    merkle_proof: merkle_proof.clone(),
                    chunk: Box::new(MaybeEncodedShardChunk::Encoded(EncodedShardChunk::clone(
                        &encoded_chunk,
                    ))),
                };
                return Err(Error::InvalidChunkProofs(Box::new(chunk_proof)));
            }
            let shard_id = shard_id as ShardId;
            if chunk_header.is_new_chunk(block_height) {
                let chunk_hash = chunk_header.chunk_hash();

                if let Err(_) = self.chain_store.get_partial_chunk(&chunk_header.chunk_hash()) {
                    missing.push(chunk_header.clone());
                } else if self.shard_tracker.care_about_shard(
                    me.as_ref(),
                    &parent_hash,
                    shard_id,
                    true,
                ) || self.shard_tracker.will_care_about_shard(
                    me.as_ref(),
                    &parent_hash,
                    shard_id,
                    true,
                ) {
                    if let Err(_) = self.chain_store.get_chunk(&chunk_hash) {
                        missing.push(chunk_header.clone());
                    }
                }
            }
        }
        if !missing.is_empty() {
            return Err(Error::ChunksMissing(missing));
        }
        Ok(())
    }

    fn care_about_any_shard_or_part(
        &self,
        me: &Option<AccountId>,
        parent_hash: CryptoHash,
    ) -> Result<bool, Error> {
        let epoch_id = self.epoch_manager.get_epoch_id_from_prev_block(&parent_hash)?;
        for shard_id in self.epoch_manager.shard_ids(&epoch_id)? {
            if self.shard_tracker.care_about_shard(me.as_ref(), &parent_hash, shard_id, true)
                || self.shard_tracker.will_care_about_shard(
                    me.as_ref(),
                    &parent_hash,
                    shard_id,
                    true,
                )
            {
                return Ok(true);
            }
        }
        for part_id in 0..self.epoch_manager.num_total_parts() {
            if &Some(self.epoch_manager.get_part_owner(&epoch_id, part_id as u64)?) == me {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Collect all incoming receipts generated in `block`, return a map from target shard id to the
    /// list of receipts that the target shard receives.
    /// The receipts are sorted by the order that they will be processed.
    /// Note that the receipts returned in this function do not equal all receipts that will be
    /// processed as incoming receipts in this block, because that may include incoming receipts
    /// generated in previous blocks too, if some shards in the previous blocks did not produce
    /// new chunks.
    pub fn collect_incoming_receipts_from_block(
        &self,
        me: &Option<AccountId>,
        block: &Block,
    ) -> Result<HashMap<ShardId, Vec<ReceiptProof>>, Error> {
        if !self.care_about_any_shard_or_part(me, *block.header().prev_hash())? {
            return Ok(HashMap::new());
        }
        let block_height = block.header().height();
        let mut receipt_proofs_by_shard_id = HashMap::new();

        for chunk_header in block.chunks().iter() {
            if !chunk_header.is_new_chunk(block_height) {
                continue;
            }
            let partial_encoded_chunk =
                self.chain_store.get_partial_chunk(&chunk_header.chunk_hash()).unwrap();
            for receipt in partial_encoded_chunk.prev_outgoing_receipts().iter() {
                let ReceiptProof(_, shard_proof) = receipt;
                let ShardProof { to_shard_id, .. } = shard_proof;
                receipt_proofs_by_shard_id
                    .entry(*to_shard_id)
                    .or_insert_with(Vec::new)
                    .push(receipt.clone());
            }
        }
        // sort the receipts deterministically so the order that they will be processed is deterministic
        for (_, receipt_proofs) in receipt_proofs_by_shard_id.iter_mut() {
            shuffle_receipt_proofs(receipt_proofs, block.hash());
        }

        Ok(receipt_proofs_by_shard_id)
    }

    /// Start processing a received or produced block. This function will process block asynchronously.
    /// It preprocesses the block by verifying that the block is valid and ready to process, then
    /// schedules the work of applying chunks in rayon thread pool. The function will return before
    /// the block processing is finished.
    /// This function is used in conjunction with the function postprocess_ready_blocks, which checks
    /// if any of the blocks in processing has finished applying chunks to finish postprocessing
    /// these blocks that are ready.
    /// `block_processing_artifacts`: Callers can pass an empty object or an existing BlockProcessingArtifact.
    ///              This function will add the effect from processing this block to there.
    /// `apply_chunks_done_sender`: An ApplyChunksDoneMessage message will be sent via this sender after apply_chunks is finished
    ///              (so it also happens asynchronously in the rayon thread pool). Callers can
    ///              use this sender as a way to receive notifications when apply chunks are done
    ///              so it can call postprocess_ready_blocks.
    pub fn start_process_block_async(
        &mut self,
        me: &Option<AccountId>,
        block: MaybeValidated<Block>,
        provenance: Provenance,
        block_processing_artifacts: &mut BlockProcessingArtifact,
        apply_chunks_done_sender: Option<near_async::messaging::Sender<ApplyChunksDoneMessage>>,
    ) -> Result<(), Error> {
        let block_height = block.header().height();
        let _span =
            debug_span!(target: "chain", "start_process_block_async", ?provenance, height=block_height).entered();
        let block_received_time = self.clock.now();
        metrics::BLOCK_PROCESSING_ATTEMPTS_TOTAL.inc();

        let hash = *block.hash();
        let res = self.start_process_block_impl(
            me,
            block,
            provenance,
            block_processing_artifacts,
            apply_chunks_done_sender,
            block_received_time,
        );

        if matches!(res, Err(Error::TooManyProcessingBlocks)) {
            self.blocks_delay_tracker
                .mark_block_dropped(&hash, DroppedReason::TooManyProcessingBlocks);
        }
        // Save the block as processed even if it failed. This is used to filter out the
        // incoming blocks that are not requested on heights which we already processed.
        // If there is a new incoming block that we didn't request and we already have height
        // processed 'marked as true' - then we'll not even attempt to process it
        if let Err(e) = self.save_block_height_processed(block_height) {
            warn!(target: "chain", "Failed to save processed height {}: {}", block_height, e);
        }

        res
    }

    /// Checks if any block has finished applying chunks and postprocesses these blocks to complete
    /// their processing. Return a list of blocks that have finished processing.
    /// If there are no blocks that are ready to be postprocessed, it returns immediately
    /// with an empty list. Even if there are blocks being processed, it does not wait
    /// for these blocks to be ready.
    pub fn postprocess_ready_blocks(
        &mut self,
        me: &Option<AccountId>,
        block_processing_artifacts: &mut BlockProcessingArtifact,
        apply_chunks_done_sender: Option<near_async::messaging::Sender<ApplyChunksDoneMessage>>,
    ) -> (Vec<AcceptedBlock>, HashMap<CryptoHash, Error>) {
        let _span = debug_span!(target: "chain", "postprocess_ready_blocks_chain").entered();
        let mut accepted_blocks = vec![];
        let mut errors = HashMap::new();
        while let Ok((block_hash, apply_result)) = self.apply_chunks_receiver.try_recv() {
            match self.postprocess_block(
                me,
                block_hash,
                apply_result,
                block_processing_artifacts,
                apply_chunks_done_sender.clone(),
            ) {
                Err(e) => {
                    errors.insert(block_hash, e);
                }
                Ok(accepted_block) => {
                    accepted_blocks.push(accepted_block);
                }
            }
        }
        (accepted_blocks, errors)
    }

    /// Process challenge to invalidate chain. This is done between blocks to unroll the chain as
    /// soon as possible and allow next block producer to skip invalid blocks.
    pub fn process_challenge(&mut self, challenge: &Challenge) {
        let head = unwrap_or_return!(self.head());
        match self.verify_challenges(&[challenge.clone()], &head.epoch_id, &head.last_block_hash) {
            Ok((_, challenged_blocks)) => {
                let mut chain_update = self.chain_update();
                for block_hash in challenged_blocks {
                    match chain_update.mark_block_as_challenged(&block_hash, None) {
                        Ok(()) => {}
                        Err(err) => {
                            warn!(target: "chain", %block_hash, ?err, "Error saving block as challenged");
                        }
                    }
                }
                unwrap_or_return!(chain_update.commit());
            }
            Err(err) => {
                warn!(target: "chain", ?err, "Invalid challenge: {:#?}", challenge);
            }
        }
    }

    /// Processes headers and adds them to store for syncing.
    pub fn sync_block_headers(
        &mut self,
        mut headers: Vec<BlockHeader>,
        challenges: &mut Vec<ChallengeBody>,
    ) -> Result<(), Error> {
        // Sort headers by heights.
        headers.sort_by_key(|left| left.height());

        if let (Some(first_header), Some(last_header)) = (headers.first(), headers.last()) {
            info!(
                target: "chain",
                num_headers = headers.len(),
                first_hash = ?first_header.hash(),
                first_height = first_header.height(),
                last_hash = ?last_header.hash(),
                last_height = ?last_header.height(),
                "Sync block headers");
        } else {
            // No headers.
            return Ok(());
        };

        // Performance optimization to skip looking up every header in the store.
        let all_known = if let Some(last_header) = headers.last() {
            // If the last header is known, then the other headers are known too.
            self.chain_store.get_block_header(last_header.hash()).is_ok()
        } else {
            // Empty set of headers, therefore all received headers are known.
            true
        };

        if all_known {
            return Ok(());
        }

        // Validate header and then add to the chain.
        for header in headers.iter() {
            match check_header_known(self, header)? {
                Ok(_) => {}
                Err(_) => continue,
            }

            self.validate_header(header, &Provenance::SYNC, challenges)?;
            let mut chain_store_update = self.chain_store.store_update();
            chain_store_update.save_block_header(header.clone())?;

            // Add validator proposals for given header.
            let last_finalized_height =
                chain_store_update.get_block_height(header.last_final_block())?;
            let epoch_manager_update = self
                .epoch_manager
                .add_validator_proposals(BlockHeaderInfo::new(header, last_finalized_height))?;
            chain_store_update.merge(epoch_manager_update);
            chain_store_update.commit()?;

            #[cfg(feature = "new_epoch_sync")]
            {
                // At this point BlockInfo for this header should be in DB and in `epoch_manager`s cache because of `add_validator_proposals` call.
                let mut chain_update = self.chain_update();
                chain_update.save_epoch_sync_info_if_finalised(header)?;
                chain_update.commit()?;
            }
        }

        let mut chain_update = self.chain_update();
        if let Some(header) = headers.last() {
            // Update header_head if it's the new tip
            chain_update.update_header_head_if_not_challenged(header)?;
        }
        chain_update.commit()
    }

    /// Returns if given block header is on the current chain.
    ///
    /// This is done by fetching header by height and checking that it’s the
    /// same one as provided.
    fn is_on_current_chain(&self, header: &BlockHeader) -> Result<bool, Error> {
        let chain_header = self.get_block_header_by_height(header.height())?;
        Ok(chain_header.hash() == header.hash())
    }

    /// Finds first of the given hashes that is known on the main chain.
    pub fn find_common_header(&self, hashes: &[CryptoHash]) -> Option<BlockHeader> {
        for hash in hashes {
            if let Ok(header) = self.get_block_header(hash) {
                if let Ok(header_at_height) = self.get_block_header_by_height(header.height()) {
                    if header.hash() == header_at_height.hash() {
                        return Some(header);
                    }
                }
            }
        }
        None
    }

    fn determine_status(&self, head: Option<Tip>, prev_head: Tip) -> BlockStatus {
        let has_head = head.is_some();
        let mut is_next_block = false;

        let old_hash = if let Some(head) = head {
            if head.prev_block_hash == prev_head.last_block_hash {
                is_next_block = true;
                None
            } else {
                Some(prev_head.last_block_hash)
            }
        } else {
            None
        };

        match (has_head, is_next_block) {
            (true, true) => BlockStatus::Next,
            (true, false) => BlockStatus::Reorg(old_hash.unwrap()),
            (false, _) => BlockStatus::Fork,
        }
    }

    /// Set the new head after state sync was completed if it is indeed newer.
    /// Check for potentially unlocked orphans after this update.
    pub fn reset_heads_post_state_sync(
        &mut self,
        me: &Option<AccountId>,
        sync_hash: CryptoHash,
        block_processing_artifacts: &mut BlockProcessingArtifact,
        apply_chunks_done_sender: Option<near_async::messaging::Sender<ApplyChunksDoneMessage>>,
    ) -> Result<(), Error> {
        let _span = tracing::debug_span!(target: "sync", "reset_heads_post_state_sync", ?sync_hash)
            .entered();
        // Get header we were syncing into.
        let header = self.get_block_header(&sync_hash)?;
        let hash = *header.prev_hash();
        let prev_block = self.get_block(&hash)?;
        let new_tail = prev_block.header().height();
        let new_chunk_tail = prev_block.chunks().iter().map(|x| x.height_created()).min().unwrap();
        let tip = Tip::from_header(prev_block.header());
        let final_head = Tip::from_header(self.genesis.header());
        // Update related heads now.
        let mut chain_store_update = self.mut_chain_store().store_update();
        chain_store_update.save_body_head(&tip)?;
        // Reset final head to genesis since at this point we don't have the last final block.
        chain_store_update.save_final_head(&final_head)?;
        // New Tail can not be earlier than `prev_block.header.inner_lite.height`
        chain_store_update.update_tail(new_tail)?;
        // New Chunk Tail can not be earlier than minimum of height_created in Block `prev_block`
        chain_store_update.update_chunk_tail(new_chunk_tail);
        chain_store_update.commit()?;

        // Check if there are any orphans unlocked by this state sync.
        // We can't fail beyond this point because the caller will not process accepted blocks
        //    and the blocks with missing chunks if this method fails
        self.check_orphans(me, hash, block_processing_artifacts, apply_chunks_done_sender);
        Ok(())
    }

    // Unlike start_process_block() this function doesn't update metrics for
    // successful blocks processing.
    fn start_process_block_impl(
        &mut self,
        me: &Option<AccountId>,
        block: MaybeValidated<Block>,
        provenance: Provenance,
        block_processing_artifact: &mut BlockProcessingArtifact,
        apply_chunks_done_sender: Option<near_async::messaging::Sender<ApplyChunksDoneMessage>>,
        block_received_time: Instant,
    ) -> Result<(), Error> {
        let block_height = block.header().height();
        let _span =
            debug_span!(target: "chain", "start_process_block_impl", block_height).entered();
        // 0) Before we proceed with any further processing, we first check that the block
        // hash and signature matches to make sure the block is indeed produced by the assigned
        // block producer. If not, we drop the block immediately
        // Note that it may appear that we call verify_block_hash_signature twice, once in
        // receive_block_impl, once here. The redundancy is because if a block is received as an orphan,
        // the check in receive_block_impl will not be complete and the block will be stored in
        // the orphan pool. When the orphaned block is ready to be processed, we must perform this check.
        // Also note that we purposely separates the check from the rest of the block verification check in
        // preprocess_block.
        if self.verify_block_hash_and_signature(&block)?
            == VerifyBlockHashAndSignatureResult::Incorrect
        {
            return Err(Error::InvalidSignature);
        }

        // 1) preprocess the block where we verify that the block is valid and ready to be processed
        //    No chain updates are applied at this step.
        let state_patch = self.pending_state_patch.take();
        let preprocess_timer = metrics::BLOCK_PREPROCESSING_TIME.start_timer();
        let preprocess_res = self.preprocess_block(
            me,
            &block,
            &provenance,
            &mut block_processing_artifact.challenges,
            &mut block_processing_artifact.invalid_chunks,
            block_received_time,
            state_patch,
        );
        let preprocess_res = match preprocess_res {
            Ok(preprocess_res) => {
                preprocess_timer.observe_duration();
                preprocess_res
            }
            Err(e) => {
                self.maybe_mark_block_invalid(*block.hash(), &e);
                preprocess_timer.stop_and_discard();
                match &e {
                    Error::Orphan => {
                        let tail_height = self.chain_store.tail()?;
                        // we only add blocks that couldn't have been gc'ed to the orphan pool.
                        if block_height >= tail_height {
                            let requested_missing_chunks = if let Some(orphan_missing_chunks) =
                                self.should_request_chunks_for_orphan(me, &block)
                            {
                                block_processing_artifact
                                    .orphans_missing_chunks
                                    .push(orphan_missing_chunks);
                                true
                            } else {
                                false
                            };

                            self.blocks_delay_tracker.mark_block_orphaned(block.hash());
                            self.save_orphan(block, provenance, requested_missing_chunks);
                        }
                    }
                    Error::ChunksMissing(missing_chunks) => {
                        let block_hash = *block.hash();
                        let missing_chunk_hashes: Vec<_> =
                            missing_chunks.iter().map(|header| header.chunk_hash()).collect();
                        block_processing_artifact.blocks_missing_chunks.push(BlockMissingChunks {
                            prev_hash: *block.header().prev_hash(),
                            missing_chunks: missing_chunks.clone(),
                        });
                        self.blocks_delay_tracker.mark_block_has_missing_chunks(block.hash());
                        let orphan = Orphan { block, provenance, added: self.clock.now() };
                        self.blocks_with_missing_chunks
                            .add_block_with_missing_chunks(orphan, missing_chunk_hashes.clone());
                        debug!(
                            target: "chain",
                            ?block_hash,
                            chunk_hashes=missing_chunk_hashes.iter().map(|h| format!("{:?}", h)).join(","),
                            "Process block: missing chunks"
                        );
                    }
                    Error::EpochOutOfBounds(epoch_id) => {
                        // Possibly block arrived before we finished processing all of the blocks for epoch before last.
                        // Or someone is attacking with invalid chain.
                        debug!(target: "chain", "Received block {}/{} ignored, as epoch {:?} is unknown", block_height, block.hash(), epoch_id);
                    }
                    Error::BlockKnown(block_known_error) => {
                        debug!(
                            target: "chain",
                            block_hash=?block.hash(),
                            height=block_height,
                            error=?block_known_error,
                            "Block known at this time");
                    }
                    _ => {}
                }
                return Err(e);
            }
        };
        let (apply_chunk_work, block_preprocess_info) = preprocess_res;

        // 2) Start creating snapshot if needed.
        if let Err(err) = self.process_snapshot() {
            tracing::error!(target: "state_snapshot", ?err, "Failed to make a state snapshot");
        }

        let block = block.into_inner();
        let block_hash = *block.hash();
        let block_height = block.header().height();
        let apply_chunks_done_tracker = block_preprocess_info.apply_chunks_done_tracker.clone();
        self.blocks_in_processing.add(block, block_preprocess_info)?;

        // 3) schedule apply chunks, which will be executed in the rayon thread pool.
        self.schedule_apply_chunks(
            block_hash,
            block_height,
            apply_chunk_work,
            apply_chunks_done_tracker,
            apply_chunks_done_sender,
        );

        Ok(())
    }

    /// Applying chunks async by starting the work at the rayon thread pool
    /// `apply_chunks_done_tracker`: notifies the threads that wait for applying chunks is finished
    /// `apply_chunks_done_sender`: a sender to send a ApplyChunksDoneMessage message once applying chunks is finished
    fn schedule_apply_chunks(
        &self,
        block_hash: CryptoHash,
        block_height: BlockHeight,
        work: Vec<UpdateShardJob>,
        mut apply_chunks_done_tracker: ApplyChunksDoneTracker,
        apply_chunks_done_sender: Option<near_async::messaging::Sender<ApplyChunksDoneMessage>>,
    ) {
        let sc = self.apply_chunks_sender.clone();
        self.apply_chunks_spawner.spawn("apply_chunks", move || {
            // do_apply_chunks runs `work` in parallel, but still waits for all of them to finish
            let res = do_apply_chunks(block_hash, block_height, work);
            // If we encounter error here, that means the receiver is deallocated and the client
            // thread is already shut down. The node is already crashed, so we can unwrap here
            sc.send((block_hash, res)).unwrap();
            if let Err(_) = apply_chunks_done_tracker.set_done() {
                // This should never happen, if it does, it means there is a bug in our code.
                log_assert!(false, "apply chunks are called twice for block {block_hash:?}");
            }
            if let Some(sender) = apply_chunks_done_sender {
                sender.send(ApplyChunksDoneMessage {});
            }
        });
    }

    #[tracing::instrument(level = "debug", target = "chain", "postprocess_block_only", skip_all)]
    fn postprocess_block_only(
        &mut self,
        me: &Option<AccountId>,
        block: &Block,
        block_preprocess_info: BlockPreprocessInfo,
        apply_results: Vec<(ShardId, Result<ShardUpdateResult, Error>)>,
    ) -> Result<Option<Tip>, Error> {
        // Save state transition data to the database only if it might later be needed
        // for generating a state witness. Storage space optimization.
        let should_save_state_transition_data =
            self.should_produce_state_witness_for_this_or_next_epoch(me, block.header())?;
        let mut chain_update = self.chain_update();
        let new_head = chain_update.postprocess_block(
            &block,
            block_preprocess_info,
            apply_results,
            should_save_state_transition_data,
        )?;
        chain_update.commit()?;
        Ok(new_head)
    }

    /// Run postprocessing on this block, which stores the block on chain.
    /// Check that if accepting the block unlocks any orphans in the orphan pool and start
    /// the processing of those blocks.
    fn postprocess_block(
        &mut self,
        me: &Option<AccountId>,
        block_hash: CryptoHash,
        apply_results: Vec<(ShardId, Result<ShardUpdateResult, Error>)>,
        block_processing_artifacts: &mut BlockProcessingArtifact,
        apply_chunks_done_sender: Option<near_async::messaging::Sender<ApplyChunksDoneMessage>>,
    ) -> Result<AcceptedBlock, Error> {
        let timer = metrics::BLOCK_POSTPROCESSING_TIME.start_timer();
        let (block, block_preprocess_info) =
            self.blocks_in_processing.remove(&block_hash).unwrap_or_else(|| {
                panic!(
                    "block {:?} finished applying chunks but not in blocks_in_processing pool",
                    block_hash
                )
            });
        // We want to include block height here, so we didn't put this line at the beginning of the
        // function.
        let _span = tracing::debug_span!(
            target: "chain",
            "postprocess_block",
            height = block.header().height())
        .entered();

        let prev_head = self.chain_store.head()?;
        let is_caught_up = block_preprocess_info.is_caught_up;
        let provenance = block_preprocess_info.provenance.clone();
        let block_start_processing_time = block_preprocess_info.block_start_processing_time;
        // TODO(#8055): this zip relies on the ordering of the apply_results.
        for (shard_id, apply_result) in apply_results.iter() {
            if let Err(err) = apply_result {
                if err.is_bad_data() {
                    let chunk = block.chunks()[*shard_id as usize].clone();
                    block_processing_artifacts.invalid_chunks.push(chunk);
                }
            }
        }
        let new_head =
            match self.postprocess_block_only(me, &block, block_preprocess_info, apply_results) {
                Err(err) => {
                    self.maybe_mark_block_invalid(*block.hash(), &err);
                    self.blocks_delay_tracker.mark_block_errored(&block_hash, err.to_string());
                    return Err(err);
                }
                Ok(new_head) => new_head,
            };

        let epoch_id = block.header().epoch_id();
        let mut shards_cares_this_or_next_epoch = vec![];
        for shard_id in self.epoch_manager.shard_ids(epoch_id)? {
            let care_about_shard = self.shard_tracker.care_about_shard(
                me.as_ref(),
                block.header().prev_hash(),
                shard_id,
                true,
            );
            let will_care_about_shard = self.shard_tracker.will_care_about_shard(
                me.as_ref(),
                block.header().prev_hash(),
                shard_id,
                true,
            );
            let care_about_shard_this_or_next_epoch = care_about_shard || will_care_about_shard;
            if care_about_shard_this_or_next_epoch {
                let shard_uid = self.epoch_manager.shard_id_to_uid(shard_id, &epoch_id).unwrap();
                shards_cares_this_or_next_epoch.push(shard_uid);
            }

            // Update flat storage head to be the last final block. Note that this update happens
            // in a separate db transaction from the update from block processing. This is intentional
            // because flat_storage need to be locked during the update of flat head, otherwise
            // flat_storage is in an inconsistent state that could be accessed by the other
            // apply chunks processes. This means, the flat head is not always the same as
            // the last final block on chain, which is OK, because in the flat storage implementation
            // we don't assume that.
            let need_flat_storage_update = if is_caught_up {
                // If we already caught up this epoch, then flat storage exists for both shards which we already track
                // and shards which will be tracked in next epoch, so we can update them.
                care_about_shard_this_or_next_epoch
            } else {
                // If we didn't catch up, we can update only shards tracked right now. Remaining shards will be updated
                // during catchup of this block.
                care_about_shard
            };
            tracing::debug!(target: "chain", shard_id, need_flat_storage_update, "Updating flat storage");

            if need_flat_storage_update {
                self.update_flat_storage_and_memtrie(&block, shard_id)?;
            }
        }

        if self.epoch_manager.is_next_block_epoch_start(block.header().prev_hash())? {
            // Keep in memory only these tries that we care about this or next epoch.
            self.runtime_adapter.get_tries().retain_mem_tries(&shards_cares_this_or_next_epoch);
        }

        if let Err(err) = self.garbage_collect_state_transition_data(&block) {
            tracing::error!(target: "chain", ?err, "failed to garbage collect state transition data");
        }

        self.pending_state_patch.clear();

        if let Some(tip) = &new_head {
            // TODO: move this logic of tracking validators metrics to EpochManager
            let mut count = 0;
            let mut stake = 0;
            if let Ok(producers) = self.epoch_manager.get_epoch_chunk_producers(&tip.epoch_id) {
                stake += producers.iter().map(|info| info.stake()).sum::<Balance>();
                count += producers.len();
            }

            stake /= NEAR_BASE;
            metrics::VALIDATOR_AMOUNT_STAKED.set(i64::try_from(stake).unwrap_or(i64::MAX));
            metrics::VALIDATOR_ACTIVE_TOTAL.set(i64::try_from(count).unwrap_or(i64::MAX));

            self.last_time_head_updated = self.clock.now();
        };

        metrics::BLOCK_PROCESSED_TOTAL.inc();
        metrics::BLOCK_PROCESSING_TIME.observe(
            (self.clock.now().signed_duration_since(block_start_processing_time))
                .as_seconds_f64()
                .max(0.0),
        );
        self.blocks_delay_tracker.finish_block_processing(&block_hash, new_head.clone());

        timer.observe_duration();
        let _timer = CryptoHashTimer::new_with_start(
            self.clock.clone(),
            *block.hash(),
            block_start_processing_time,
        );

        self.check_orphans(me, *block.hash(), block_processing_artifacts, apply_chunks_done_sender);

        // Determine the block status of this block (whether it is a side fork and updates the chain head)
        // Block status is needed in Client::on_block_accepted_with_optional_chunk_produce to
        // decide to how to update the tx pool.
        let block_status = self.determine_status(new_head, prev_head);
        Ok(AcceptedBlock { hash: *block.hash(), status: block_status, provenance })
    }

    /// Gets new flat storage head candidate for given `shard_id` and newly
    /// processed `block`.
    /// It will be `block.last_final_block().chunk(shard_id).prev_block_hash()`
    /// if all necessary conditions are met.
    /// This is required for `StateSnapshot` to be able to make snapshot of
    /// flat storage at the epoch boundary.
    fn get_new_flat_storage_head(
        &self,
        block: &Block,
        shard_id: ShardId,
    ) -> Result<Option<CryptoHash>, Error> {
        let epoch_id = block.header().epoch_id();
        let last_final_block_hash = *block.header().last_final_block();
        // If final block doesn't exist yet, skip getting candidate.
        if last_final_block_hash == CryptoHash::default() {
            return Ok(None);
        }

        let last_final_block = self.get_block(&last_final_block_hash)?;
        let last_final_block_epoch_id = last_final_block.header().epoch_id();
        // If shard layout was changed, the update is impossible so we skip
        // getting candidate.
        if self.epoch_manager.get_shard_layout(last_final_block_epoch_id)
            != self.epoch_manager.get_shard_layout(epoch_id)
        {
            return Ok(None);
        }

        let last_final_block_chunks = last_final_block.chunks();
        let chunk_header = last_final_block_chunks
            .iter()
            .find(|chunk| chunk.shard_id() == shard_id)
            .ok_or_else(|| Error::InvalidShardId(shard_id))?;
        let new_flat_head = *chunk_header.prev_block_hash();
        if new_flat_head == CryptoHash::default() {
            return Ok(None);
        }
        Ok(Some(new_flat_head))
    }

    /// Update flat storage and memtrie for given `shard_id` and newly
    /// processed `block`.
    fn update_flat_storage_and_memtrie(
        &self,
        block: &Block,
        shard_id: ShardId,
    ) -> Result<(), Error> {
        let epoch_id = block.header().epoch_id();
        let shard_uid = self.epoch_manager.shard_id_to_uid(shard_id, epoch_id)?;

        // Update flat storage.
        let flat_storage_manager = self.runtime_adapter.get_flat_storage_manager();
        if flat_storage_manager.get_flat_storage_for_shard(shard_uid).is_some() {
            if let Some(new_flat_head) = self.get_new_flat_storage_head(block, shard_id)? {
                flat_storage_manager.update_flat_storage_for_shard(shard_uid, new_flat_head)?;
            }
        }

        // Garbage collect memtrie roots.
        let tries = self.runtime_adapter.get_tries();
        let last_final_block = block.header().last_final_block();
        if last_final_block != &CryptoHash::default() {
            let header = self.chain_store.get_block_header(last_final_block).unwrap();
            if let Some(prev_height) = header.prev_height() {
                tries.delete_memtrie_roots_up_to_height(shard_uid, prev_height);
            }
        }
        Ok(())
    }

    /// Preprocess a block before applying chunks, verify that we have the necessary information
    /// to process the block an the block is valid.
    /// Note that this function does NOT introduce any changes to chain state.
    fn preprocess_block(
        &self,
        me: &Option<AccountId>,
        block: &MaybeValidated<Block>,
        provenance: &Provenance,
        challenges: &mut Vec<ChallengeBody>,
        invalid_chunks: &mut Vec<ShardChunkHeader>,
        block_received_time: Instant,
        state_patch: SandboxStatePatch,
    ) -> Result<PreprocessBlockResult, Error> {
        let header = block.header();

        // see if the block is already in processing or if there are too many blocks being processed
        self.blocks_in_processing.add_dry_run(block.hash())?;

        debug!(target: "chain", height=header.height(), num_approvals = header.num_approvals(), "preprocess_block");

        // Check that we know the epoch of the block before we try to get the header
        // (so that a block from unknown epoch doesn't get marked as an orphan)
        if !self.epoch_manager.epoch_exists(header.epoch_id()) {
            return Err(Error::EpochOutOfBounds(*header.epoch_id()));
        }

        if block.chunks().len() != self.epoch_manager.shard_ids(header.epoch_id())?.len() {
            return Err(Error::IncorrectNumberOfChunkHeaders);
        }

        // Check if we have already processed this block previously.
        check_known(self, header.hash())?.map_err(|e| Error::BlockKnown(e))?;

        // Delay hitting the db for current chain head until we know this block is not already known.
        let head = self.head()?;
        let is_next = header.prev_hash() == &head.last_block_hash;

        // Sandbox allows fast-forwarding, so only enable when not within sandbox
        if !cfg!(feature = "sandbox") {
            // A heuristic to prevent block height to jump too fast towards BlockHeight::max and cause
            // overflow-related problems
            let block_height = header.height();
            if block_height > head.height + self.epoch_length * 20 {
                return Err(Error::InvalidBlockHeight(block_height));
            }
        }

        // Block is an orphan if we do not know about the previous full block.
        if !is_next && !self.block_exists(header.prev_hash())? {
            // Before we add the block to the orphan pool, do some checks:
            // 1. Block header is signed by the block producer for height.
            // 2. Chunk headers in block body match block header.
            // 3. Header has enough approvals from epoch block producers.
            // Not checked:
            // - Block producer could be slashed
            // - Chunk header signatures could be wrong
            if !self.partial_verify_orphan_header_signature(header)? {
                return Err(Error::InvalidSignature);
            }
            block.check_validity()?;
            // TODO: enable after #3729 and #3863
            // self.verify_orphan_header_approvals(&header)?;
            return Err(Error::Orphan);
        }

        let epoch_protocol_version =
            self.epoch_manager.get_epoch_protocol_version(header.epoch_id())?;
        if epoch_protocol_version > PROTOCOL_VERSION {
            panic!("The client protocol version is older than the protocol version of the network. Please update nearcore. Client protocol version:{}, network protocol version {}", PROTOCOL_VERSION, epoch_protocol_version);
        }

        // First real I/O expense.
        let prev = self.get_previous_header(header)?;
        let prev_hash = *prev.hash();
        let prev_prev_hash = *prev.prev_hash();
        let gas_price = prev.next_gas_price();
        let prev_random_value = *prev.random_value();
        let prev_height = prev.height();

        // Do not accept old forks
        if prev_height < self.runtime_adapter.get_gc_stop_height(&head.last_block_hash) {
            return Err(Error::InvalidBlockHeight(prev_height));
        }

        let (is_caught_up, state_sync_info) =
            self.get_catchup_and_state_sync_infos(header, prev_hash, prev_prev_hash, me, block)?;

        self.check_if_challenged_block_on_chain(header)?;

        debug!(target: "chain", block_hash = ?header.hash(), me=?me, is_caught_up=is_caught_up, "Process block");

        // Check the header is valid before we proceed with the full block.
        self.validate_header(header, provenance, challenges)?;

        self.epoch_manager.verify_block_vrf(
            header.epoch_id(),
            header.height(),
            &prev_random_value,
            block.vrf_value(),
            block.vrf_proof(),
        )?;

        if header.random_value() != &hash(block.vrf_value().0.as_ref()) {
            return Err(Error::InvalidRandomnessBeaconOutput);
        }

        if let Err(e) = self.validate_block(block) {
            byzantine_assert!(false);
            return Err(e);
        }

        let protocol_version = self.epoch_manager.get_epoch_protocol_version(header.epoch_id())?;
        if !block.verify_gas_price(
            gas_price,
            self.block_economics_config.min_gas_price(protocol_version),
            self.block_economics_config.max_gas_price(protocol_version),
            self.block_economics_config.gas_price_adjustment_rate(protocol_version),
        ) {
            byzantine_assert!(false);
            return Err(Error::InvalidGasPrice);
        }
        let minted_amount = if self.epoch_manager.is_next_block_epoch_start(&prev_hash)? {
            Some(self.epoch_manager.get_epoch_minted_amount(header.next_epoch_id())?)
        } else {
            None
        };

        if !block.verify_total_supply(prev.total_supply(), minted_amount) {
            byzantine_assert!(false);
            return Err(Error::InvalidGasPrice);
        }

        let (challenges_result, challenged_blocks) =
            self.verify_challenges(block.challenges(), header.epoch_id(), header.prev_hash())?;

        let prev_block = self.get_block(&prev_hash)?;

        self.validate_chunk_headers(&block, &prev_block)?;

        if ProtocolFeature::StatelessValidation.enabled(protocol_version) {
            self.validate_chunk_endorsements_in_block(&block)?;
        }

        self.ping_missing_chunks(me, prev_hash, block)?;
        let incoming_receipts = self.collect_incoming_receipts_from_block(me, block)?;

        // Check if block can be finalized and drop it otherwise.
        self.check_if_finalizable(header)?;

        let apply_chunk_work = self.apply_chunks_preprocessing(
            me,
            block,
            &prev_block,
            &incoming_receipts,
            // If we have the state for shards in the next epoch already downloaded, apply the state transition
            // for these states as well
            // otherwise put the block into the permanent storage, waiting for be caught up
            if is_caught_up { ApplyChunksMode::IsCaughtUp } else { ApplyChunksMode::NotCaughtUp },
            state_patch,
            invalid_chunks,
        )?;

        Ok((
            apply_chunk_work,
            BlockPreprocessInfo {
                is_caught_up,
                state_sync_info,
                incoming_receipts,
                challenges_result,
                challenged_blocks,
                provenance: provenance.clone(),
                apply_chunks_done_tracker: ApplyChunksDoneTracker::new(),
                block_start_processing_time: block_received_time,
            },
        ))
    }

    fn get_catchup_and_state_sync_infos(
        &self,
        header: &BlockHeader,
        prev_hash: CryptoHash,
        prev_prev_hash: CryptoHash,
        me: &Option<AccountId>,
        block: &MaybeValidated<Block>,
    ) -> Result<(bool, Option<StateSyncInfo>), Error> {
        if self.epoch_manager.is_next_block_epoch_start(&prev_hash)? {
            debug!(target: "chain", block_hash=?header.hash(), "block is the first block of an epoch");
            if !self.prev_block_is_caught_up(&prev_prev_hash, &prev_hash)? {
                // The previous block is not caught up for the next epoch relative to the previous
                // block, which is the current epoch for this block, so this block cannot be applied
                // at all yet, needs to be orphaned
                return Err(Error::Orphan);
            }

            // For the first block of the epoch we check if we need to start download states for
            // shards that we will care about in the next epoch. If there is no state to be downloaded,
            // we consider that we are caught up, otherwise not
            let state_sync_info = self.get_state_sync_info(me, block)?;
            Ok((state_sync_info.is_none(), state_sync_info))
        } else {
            Ok((self.prev_block_is_caught_up(&prev_prev_hash, &prev_hash)?, None))
        }
    }

    pub fn prev_block_is_caught_up(
        &self,
        prev_prev_hash: &CryptoHash,
        prev_hash: &CryptoHash,
    ) -> Result<bool, Error> {
        // Needs to be used with care: for the first block of each epoch the semantic is slightly
        // different, since the prev_block is in a different epoch. So for all the blocks but the
        // first one in each epoch this method returns true if the block is ready to have state
        // applied for the next epoch, while for the first block in a particular epoch this method
        // returns true if the block is ready to have state applied for the current epoch (and
        // otherwise should be orphaned)
        Ok(!self.chain_store.get_blocks_to_catchup(prev_prev_hash)?.contains(prev_hash))
    }

    /// Return all shards that whose states need to be caught up
    /// That has two cases:
    /// 1) Shard layout will change in the next epoch. In this case, the method returns all shards
    ///    in the current epoch that will be split into a future shard that `me` will track.
    /// 2) Shard layout will be the same. In this case, the method returns all shards that `me` will
    ///    track in the next epoch but not this epoch
    fn get_shards_to_state_sync(
        epoch_manager: &dyn EpochManagerAdapter,
        shard_tracker: &ShardTracker,
        me: &Option<AccountId>,
        parent_hash: &CryptoHash,
    ) -> Result<Vec<ShardId>, Error> {
        let epoch_id = epoch_manager.get_epoch_id_from_prev_block(parent_hash)?;
        Ok((epoch_manager.shard_ids(&epoch_id)?)
            .into_iter()
            .filter(|shard_id| {
                Self::should_catch_up_shard(
                    epoch_manager,
                    shard_tracker,
                    me,
                    parent_hash,
                    *shard_id,
                )
            })
            .collect())
    }

    fn should_catch_up_shard(
        epoch_manager: &dyn EpochManagerAdapter,
        shard_tracker: &ShardTracker,
        me: &Option<AccountId>,
        parent_hash: &CryptoHash,
        shard_id: ShardId,
    ) -> bool {
        let result = epoch_manager.will_shard_layout_change(parent_hash);
        let will_shard_layout_change = match result {
            Ok(will_shard_layout_change) => will_shard_layout_change,
            Err(err) => {
                // TODO(resharding) This is a problem, if this happens the node
                // will not perform resharding and fall behind the network.
                tracing::error!(target: "chain", ?err, "failed to check if shard layout will change");
                false
            }
        };
        // if shard layout will change the next epoch, we should catch up the shard regardless
        // whether we already have the shard's state this epoch, because we need to generate
        // new states for shards split from the current shard for the next epoch
        let will_care_about_shard =
            shard_tracker.will_care_about_shard(me.as_ref(), parent_hash, shard_id, true);
        let does_care_about_shard =
            shard_tracker.care_about_shard(me.as_ref(), parent_hash, shard_id, true);

        tracing::debug!(target: "chain", does_care_about_shard, will_care_about_shard, will_shard_layout_change, "should catch up shard");

        will_care_about_shard && (will_shard_layout_change || !does_care_about_shard)
    }

    /// Check if any block with missing chunk is ready to be processed and start processing these blocks
    pub fn check_blocks_with_missing_chunks(
        &mut self,
        me: &Option<AccountId>,
        block_processing_artifact: &mut BlockProcessingArtifact,
        apply_chunks_done_sender: Option<near_async::messaging::Sender<ApplyChunksDoneMessage>>,
    ) {
        let blocks = self.blocks_with_missing_chunks.ready_blocks();
        if !blocks.is_empty() {
            debug!(target:"chain", "Got {} blocks that were missing chunks but now are ready.", blocks.len());
        }
        for block in blocks {
            let block_hash = *block.block.header().hash();
            let height = block.block.header().height();
            let res = self.start_process_block_async(
                me,
                block.block,
                block.provenance,
                block_processing_artifact,
                apply_chunks_done_sender.clone(),
            );
            match res {
                Ok(_) => {
                    debug!(target: "chain", %block_hash, height, "Accepted block with missing chunks");
                    self.blocks_delay_tracker.mark_block_completed_missing_chunks(&block_hash);
                }
                Err(_) => {
                    debug!(target: "chain", %block_hash, height, "Declined block with missing chunks is declined.");
                }
            }
        }
    }

    pub fn get_outgoing_receipts_for_shard(
        &self,
        prev_block_hash: CryptoHash,
        shard_id: ShardId,
        last_height_included: BlockHeight,
    ) -> Result<Vec<Receipt>, Error> {
        self.chain_store.get_outgoing_receipts_for_shard(
            self.epoch_manager.as_ref(),
            prev_block_hash,
            shard_id,
            last_height_included,
        )
    }

    /// Computes ShardStateSyncResponseHeader.
    pub fn compute_state_response_header(
        &self,
        shard_id: ShardId,
        sync_hash: CryptoHash,
    ) -> Result<ShardStateSyncResponseHeader, Error> {
        // Consistency rules:
        // 1. Everything prefixed with `sync_` indicates new epoch, for which we are syncing.
        // 1a. `sync_prev` means the last of the prev epoch.
        // 2. Empty prefix means the height where chunk was applied last time in the prev epoch.
        //    Let's call it `current`.
        // 2a. `prev_` means we're working with height before current.
        // 3. In inner loops we use all prefixes with no relation to the context described above.
        let sync_block = self
            .get_block(&sync_hash)
            .log_storage_error("block has already been checked for existence")?;
        let sync_block_header = sync_block.header();
        let sync_block_epoch_id = sync_block_header.epoch_id();
        let shard_ids = self.epoch_manager.shard_ids(sync_block_epoch_id)?;
        if !shard_ids.contains(&shard_id) {
            return Err(shard_id_out_of_bounds(shard_id));
        }

        // The chunk was applied at height `chunk_header.height_included`.
        // Getting the `current` state.
        let sync_prev_block = self.get_block(sync_block_header.prev_hash())?;
        if sync_block_epoch_id == sync_prev_block.header().epoch_id() {
            return Err(sync_hash_not_first_hash(sync_hash));
        }
        // Chunk header here is the same chunk header as at the `current` height.
        let sync_prev_hash = sync_prev_block.hash();
        let chunks = sync_prev_block.chunks();
        let chunk_header = chunks
            .get(shard_id as usize)
            .ok_or_else(|| Error::InvalidShardId(shard_id as ShardId))?;
        let (chunk_headers_root, chunk_proofs) = merklize(
            &sync_prev_block
                .chunks()
                .iter()
                .map(|shard_chunk| {
                    ChunkHashHeight(shard_chunk.chunk_hash(), shard_chunk.height_included())
                })
                .collect::<Vec<ChunkHashHeight>>(),
        );
        assert_eq!(&chunk_headers_root, sync_prev_block.header().chunk_headers_root());

        let chunk = self.get_chunk_clone_from_header(chunk_header)?;
        let chunk_proof = chunk_proofs
            .get(shard_id as usize)
            .ok_or_else(|| Error::InvalidShardId(shard_id as ShardId))?
            .clone();
        let block_header =
            self.get_block_header_on_chain_by_height(&sync_hash, chunk_header.height_included())?;

        // Collecting the `prev` state.
        let (prev_chunk_header, prev_chunk_proof, prev_chunk_height_included) = match self
            .get_block(block_header.prev_hash())
        {
            Ok(prev_block) => {
                let prev_chunk_header = prev_block
                    .chunks()
                    .get(shard_id as usize)
                    .ok_or_else(|| Error::InvalidShardId(shard_id as ShardId))?
                    .clone();
                let (prev_chunk_headers_root, prev_chunk_proofs) = merklize(
                    &prev_block
                        .chunks()
                        .iter()
                        .map(|shard_chunk| {
                            ChunkHashHeight(shard_chunk.chunk_hash(), shard_chunk.height_included())
                        })
                        .collect::<Vec<ChunkHashHeight>>(),
                );
                assert_eq!(&prev_chunk_headers_root, prev_block.header().chunk_headers_root());

                let prev_chunk_proof = prev_chunk_proofs
                    .get(shard_id as usize)
                    .ok_or_else(|| Error::InvalidShardId(shard_id as ShardId))?
                    .clone();
                let prev_chunk_height_included = prev_chunk_header.height_included();

                (Some(prev_chunk_header), Some(prev_chunk_proof), prev_chunk_height_included)
            }
            Err(e) => match e {
                Error::DBNotFoundErr(_) => {
                    if block_header.is_genesis() {
                        (None, None, 0)
                    } else {
                        return Err(e);
                    }
                }
                _ => return Err(e),
            },
        };

        // Getting all existing incoming_receipts from prev_chunk height to the
        // new epoch.
        let incoming_receipts_proofs = self.chain_store.get_incoming_receipts_for_shard(
            self.epoch_manager.as_ref(),
            shard_id,
            sync_hash,
            prev_chunk_height_included,
        )?;

        // Collecting proofs for incoming receipts.
        let mut root_proofs = vec![];
        for receipt_response in incoming_receipts_proofs.iter() {
            let ReceiptProofResponse(block_hash, receipt_proofs) = receipt_response;
            let block_header = self.get_block_header(block_hash)?.clone();
            let block = self.get_block(block_hash)?;
            let (block_receipts_root, block_receipts_proofs) = merklize(
                &block
                    .chunks()
                    .iter()
                    .map(|chunk| chunk.prev_outgoing_receipts_root())
                    .collect::<Vec<CryptoHash>>(),
            );

            let mut root_proofs_cur = vec![];
            if receipt_proofs.len() != block_header.chunks_included() as usize {
                // Happens if a node doesn't track all shards and can't provide
                // all incoming receipts to a chunk.
                return Err(Error::Other("Not tracking all shards".to_owned()));
            }
            for receipt_proof in receipt_proofs.iter() {
                let ReceiptProof(receipts, shard_proof) = receipt_proof;
                let ShardProof { from_shard_id, to_shard_id: _, proof } = shard_proof;
                let receipts_hash = CryptoHash::hash_borsh(ReceiptList(shard_id, receipts));
                let from_shard_id = *from_shard_id as usize;

                let root_proof = block.chunks()[from_shard_id].prev_outgoing_receipts_root();
                root_proofs_cur
                    .push(RootProof(root_proof, block_receipts_proofs[from_shard_id].clone()));

                // Make sure we send something reasonable.
                assert_eq!(block_header.prev_chunk_outgoing_receipts_root(), &block_receipts_root);
                assert!(verify_path(root_proof, proof, &receipts_hash));
                assert!(verify_path(
                    block_receipts_root,
                    &block_receipts_proofs[from_shard_id],
                    &root_proof,
                ));
            }
            root_proofs.push(root_proofs_cur);
        }

        let state_root_node = self.runtime_adapter.get_state_root_node(
            shard_id,
            sync_prev_hash,
            &chunk_header.prev_state_root(),
        )?;

        let (chunk, prev_chunk_header) = match chunk {
            ShardChunk::V1(chunk) => {
                let prev_chunk_header =
                    prev_chunk_header.and_then(|prev_header| match prev_header {
                        ShardChunkHeader::V1(header) => Some(ShardChunkHeader::V1(header)),
                        ShardChunkHeader::V2(_) => None,
                        ShardChunkHeader::V3(_) => None,
                    });
                let chunk = ShardChunk::V1(chunk);
                (chunk, prev_chunk_header)
            }
            chunk @ ShardChunk::V2(_) => (chunk, prev_chunk_header),
        };

        let shard_state_header = ShardStateSyncResponseHeaderV2 {
            chunk,
            chunk_proof,
            prev_chunk_header,
            prev_chunk_proof,
            incoming_receipts_proofs,
            root_proofs,
            state_root_node,
        };

        Ok(ShardStateSyncResponseHeader::V2(shard_state_header))
    }

    /// Returns ShardStateSyncResponseHeader for the given epoch and shard.
    /// If the header is already available in the DB, returns the cached version and doesn't recompute it.
    /// If the header was computed then it also gets cached in the DB.
    pub fn get_state_response_header(
        &self,
        shard_id: ShardId,
        sync_hash: CryptoHash,
    ) -> Result<ShardStateSyncResponseHeader, Error> {
        // Check cache
        let key = borsh::to_vec(&StateHeaderKey(shard_id, sync_hash))?;
        if let Ok(Some(header)) = self.chain_store.store().get_ser(DBCol::StateHeaders, &key) {
            return Ok(header);
        }

        let shard_state_header = self.compute_state_response_header(shard_id, sync_hash)?;

        // Saving the header data
        let mut store_update = self.chain_store.store().store_update();
        store_update.set_ser(DBCol::StateHeaders, &key, &shard_state_header)?;
        store_update.commit()?;

        Ok(shard_state_header)
    }

    pub fn get_state_response_part(
        &mut self,
        shard_id: ShardId,
        part_id: u64,
        sync_hash: CryptoHash,
    ) -> Result<Vec<u8>, Error> {
        let _span = tracing::debug_span!(
            target: "sync",
            "get_state_response_part",
            shard_id,
            part_id,
            ?sync_hash)
        .entered();
        // Check cache
        let key = borsh::to_vec(&StatePartKey(sync_hash, shard_id, part_id))?;
        if let Ok(Some(state_part)) = self.chain_store.store().get(DBCol::StateParts, &key) {
            return Ok(state_part.into());
        }

        let block = self
            .get_block(&sync_hash)
            .log_storage_error("block has already been checked for existence")?;
        let header = block.header();
        let epoch_id = block.header().epoch_id();
        let shard_ids = self.epoch_manager.shard_ids(epoch_id)?;
        if !shard_ids.contains(&shard_id) {
            return Err(shard_id_out_of_bounds(shard_id));
        }
        let prev_block = self.get_block(header.prev_hash())?;
        if epoch_id == prev_block.header().epoch_id() {
            return Err(sync_hash_not_first_hash(sync_hash));
        }
        let state_root = prev_block
            .chunks()
            .get(shard_id as usize)
            .ok_or_else(|| Error::InvalidShardId(shard_id as ShardId))?
            .prev_state_root();
        let prev_hash = *prev_block.hash();
        let prev_prev_hash = *prev_block.header().prev_hash();
        let state_root_node = self
            .runtime_adapter
            .get_state_root_node(shard_id, &prev_hash, &state_root)
            .log_storage_error("get_state_root_node fail")?;
        let num_parts = get_num_state_parts(state_root_node.memory_usage);
        if part_id >= num_parts {
            return Err(shard_id_out_of_bounds(shard_id));
        }
        let current_time = Instant::now();
        let state_part = self
            .runtime_adapter
            .obtain_state_part(
                shard_id,
                &prev_prev_hash,
                &state_root,
                PartId::new(part_id, num_parts),
            )
            .log_storage_error("obtain_state_part fail")?;

        let elapsed_ms = (self.clock.now().signed_duration_since(current_time))
            .whole_milliseconds()
            .max(0) as u128;
        self.requested_state_parts
            .save_state_part_elapsed(&sync_hash, &shard_id, &part_id, elapsed_ms);

        // Before saving State Part data, we need to make sure we can calculate and save State Header
        self.get_state_response_header(shard_id, sync_hash)?;

        // Saving the part data
        let mut store_update = self.chain_store.store().store_update();
        store_update.set(DBCol::StateParts, &key, &state_part);
        store_update.commit()?;

        Ok(state_part)
    }

    pub fn set_state_header(
        &mut self,
        shard_id: ShardId,
        sync_hash: CryptoHash,
        shard_state_header: ShardStateSyncResponseHeader,
    ) -> Result<(), Error> {
        let sync_block_header = self.get_block_header(&sync_hash)?;

        let chunk = shard_state_header.cloned_chunk();
        let prev_chunk_header = shard_state_header.cloned_prev_chunk_header();

        // 1-2. Checking chunk validity
        if !validate_chunk_proofs(&chunk, self.epoch_manager.as_ref())? {
            byzantine_assert!(false);
            return Err(Error::Other(
                "set_shard_state failed: chunk header proofs are invalid".into(),
            ));
        }

        // Consider chunk itself is valid.

        // 3. Checking that chunks `chunk` and `prev_chunk` are included in appropriate blocks
        // 3a. Checking that chunk `chunk` is included into block at last height before sync_hash
        // 3aa. Also checking chunk.height_included
        let sync_prev_block_header = self.get_block_header(sync_block_header.prev_hash())?;
        if !verify_path(
            *sync_prev_block_header.chunk_headers_root(),
            shard_state_header.chunk_proof(),
            &ChunkHashHeight(chunk.chunk_hash(), chunk.height_included()),
        ) {
            byzantine_assert!(false);
            return Err(Error::Other(
                "set_shard_state failed: chunk isn't included into block".into(),
            ));
        }

        let block_header =
            self.get_block_header_on_chain_by_height(&sync_hash, chunk.height_included())?;
        // 3b. Checking that chunk `prev_chunk` is included into block at height before chunk.height_included
        // 3ba. Also checking prev_chunk.height_included - it's important for getting correct incoming receipts
        match (&prev_chunk_header, shard_state_header.prev_chunk_proof()) {
            (Some(prev_chunk_header), Some(prev_chunk_proof)) => {
                let prev_block_header =
                    self.get_block_header(block_header.prev_hash())?;
                if !verify_path(
                    *prev_block_header.chunk_headers_root(),
                    prev_chunk_proof,
                    &ChunkHashHeight(prev_chunk_header.chunk_hash(), prev_chunk_header.height_included()),
                ) {
                    byzantine_assert!(false);
                    return Err(Error::Other(
                        "set_shard_state failed: prev_chunk isn't included into block".into(),
                    ));
                }
            }
            (None, None) => {
                if chunk.height_included() != 0 {
                    return Err(Error::Other(
                    "set_shard_state failed: received empty state response for a chunk that is not at height 0".into()
                ));
                }
            }
            _ =>
                return Err(Error::Other("set_shard_state failed: `prev_chunk_header` and `prev_chunk_proof` must either both be present or both absent".into()))
        };

        // 4. Proving incoming receipts validity
        // 4a. Checking len of proofs
        if shard_state_header.root_proofs().len()
            != shard_state_header.incoming_receipts_proofs().len()
        {
            byzantine_assert!(false);
            return Err(Error::Other("set_shard_state failed: invalid proofs".into()));
        }
        let mut hash_to_compare = sync_hash;
        for (i, receipt_response) in
            shard_state_header.incoming_receipts_proofs().iter().enumerate()
        {
            let ReceiptProofResponse(block_hash, receipt_proofs) = receipt_response;

            // 4b. Checking that there is a valid sequence of continuous blocks
            if *block_hash != hash_to_compare {
                byzantine_assert!(false);
                return Err(Error::Other(
                    "set_shard_state failed: invalid incoming receipts".into(),
                ));
            }
            let header = self.get_block_header(&hash_to_compare)?;
            hash_to_compare = *header.prev_hash();

            let block_header = self.get_block_header(block_hash)?;
            // 4c. Checking len of receipt_proofs for current block
            if receipt_proofs.len() != shard_state_header.root_proofs()[i].len()
                || receipt_proofs.len() != block_header.chunks_included() as usize
            {
                byzantine_assert!(false);
                return Err(Error::Other("set_shard_state failed: invalid proofs".into()));
            }
            // We know there were exactly `block_header.chunks_included` chunks included
            // on the height of block `block_hash`.
            // There were no other proofs except for included chunks.
            // According to Pigeonhole principle, it's enough to ensure all receipt_proofs are distinct
            // to prove that all receipts were received and no receipts were hidden.
            let mut visited_shard_ids = HashSet::<ShardId>::new();
            for (j, receipt_proof) in receipt_proofs.iter().enumerate() {
                let ReceiptProof(receipts, shard_proof) = receipt_proof;
                let ShardProof { from_shard_id, to_shard_id: _, proof } = shard_proof;
                // 4d. Checking uniqueness for set of `from_shard_id`
                match visited_shard_ids.get(from_shard_id) {
                    Some(_) => {
                        byzantine_assert!(false);
                        return Err(Error::Other("set_shard_state failed: invalid proofs".into()));
                    }
                    _ => visited_shard_ids.insert(*from_shard_id),
                };
                let RootProof(root, block_proof) = &shard_state_header.root_proofs()[i][j];
                let receipts_hash = CryptoHash::hash_borsh(ReceiptList(shard_id, receipts));
                // 4e. Proving the set of receipts is the subset of outgoing_receipts of shard `shard_id`
                if !verify_path(*root, proof, &receipts_hash) {
                    byzantine_assert!(false);
                    return Err(Error::Other("set_shard_state failed: invalid proofs".into()));
                }
                // 4f. Proving the outgoing_receipts_root matches that in the block
                if !verify_path(
                    *block_header.prev_chunk_outgoing_receipts_root(),
                    block_proof,
                    root,
                ) {
                    byzantine_assert!(false);
                    return Err(Error::Other("set_shard_state failed: invalid proofs".into()));
                }
            }
        }
        // 4g. Checking that there are no more heights to get incoming_receipts
        let header = self.get_block_header(&hash_to_compare)?;
        if header.height() != prev_chunk_header.map_or(0, |h| h.height_included()) {
            byzantine_assert!(false);
            return Err(Error::Other("set_shard_state failed: invalid incoming receipts".into()));
        }

        // 5. Checking that state_root_node is valid
        let chunk_inner = chunk.take_header().take_inner();
        if !self.runtime_adapter.validate_state_root_node(
            shard_state_header.state_root_node(),
            chunk_inner.prev_state_root(),
        ) {
            byzantine_assert!(false);
            return Err(Error::Other("set_shard_state failed: state_root_node is invalid".into()));
        }

        // Saving the header data.
        let mut store_update = self.chain_store.store().store_update();
        let key = borsh::to_vec(&StateHeaderKey(shard_id, sync_hash))?;
        store_update.set_ser(DBCol::StateHeaders, &key, &shard_state_header)?;
        store_update.commit()?;

        Ok(())
    }

    pub fn get_state_header(
        &self,
        shard_id: ShardId,
        sync_hash: CryptoHash,
    ) -> Result<ShardStateSyncResponseHeader, Error> {
        self.chain_store.get_state_header(shard_id, sync_hash)
    }

    pub fn set_state_part(
        &mut self,
        shard_id: ShardId,
        sync_hash: CryptoHash,
        part_id: PartId,
        data: &[u8],
    ) -> Result<(), Error> {
        let shard_state_header = self.get_state_header(shard_id, sync_hash)?;
        let chunk = shard_state_header.take_chunk();
        let state_root = *chunk.take_header().take_inner().prev_state_root();
        if !self.runtime_adapter.validate_state_part(&state_root, part_id, data) {
            byzantine_assert!(false);
            return Err(Error::Other(format!(
                "set_state_part failed: validate_state_part failed. state_root={:?}",
                state_root
            )));
        }

        // Saving the part data.
        let mut store_update = self.chain_store.store().store_update();
        let key = borsh::to_vec(&StatePartKey(sync_hash, shard_id, part_id.idx))?;
        store_update.set(DBCol::StateParts, &key, data);
        store_update.commit()?;
        Ok(())
    }

    pub fn schedule_apply_state_parts(
        &self,
        shard_id: ShardId,
        sync_hash: CryptoHash,
        num_parts: u64,
        state_parts_task_scheduler: &near_async::messaging::Sender<ApplyStatePartsRequest>,
    ) -> Result<(), Error> {
        let epoch_id = *self.get_block_header(&sync_hash)?.epoch_id();
        let shard_uid = self.epoch_manager.shard_id_to_uid(shard_id, &epoch_id)?;

        let shard_state_header = self.get_state_header(shard_id, sync_hash)?;
        let state_root = shard_state_header.chunk_prev_state_root();

        state_parts_task_scheduler.send(ApplyStatePartsRequest {
            runtime_adapter: self.runtime_adapter.clone(),
            shard_uid,
            state_root,
            num_parts,
            epoch_id,
            sync_hash,
        });

        Ok(())
    }

    pub fn schedule_load_memtrie(
        &self,
        shard_uid: ShardUId,
        sync_hash: CryptoHash,
        chunk: &ShardChunk,
        load_memtrie_scheduler: &near_async::messaging::Sender<LoadMemtrieRequest>,
    ) {
        load_memtrie_scheduler.send(LoadMemtrieRequest {
            runtime_adapter: self.runtime_adapter.clone(),
            shard_uid,
            prev_state_root: chunk.prev_state_root(),
            sync_hash,
        });
    }

    pub fn create_flat_storage_for_shard(
        &self,
        shard_uid: ShardUId,
        chunk: &ShardChunk,
    ) -> Result<(), Error> {
        let flat_storage_manager = self.runtime_adapter.get_flat_storage_manager();
        // Flat storage must not exist at this point because leftover keys corrupt its state.
        assert!(flat_storage_manager.get_flat_storage_for_shard(shard_uid).is_none());

        let flat_head_hash = *chunk.prev_block();
        let flat_head_header = self.get_block_header(&flat_head_hash)?;
        let flat_head_prev_hash = *flat_head_header.prev_hash();
        let flat_head_height = flat_head_header.height();

        tracing::debug!(target: "store", ?shard_uid, ?flat_head_hash, flat_head_height, "set_state_finalize - initialized flat storage");

        let mut store_update = self.runtime_adapter.store().store_update();
        store_helper::set_flat_storage_status(
            &mut store_update,
            shard_uid,
            FlatStorageStatus::Ready(FlatStorageReadyStatus {
                flat_head: near_store::flat::BlockInfo {
                    hash: flat_head_hash,
                    prev_hash: flat_head_prev_hash,
                    height: flat_head_height,
                },
            }),
        );
        store_update.commit()?;
        flat_storage_manager.create_flat_storage_for_shard(shard_uid).unwrap();
        Ok(())
    }

    /// This method is called when the state sync is finished for a shard. It
    /// applies the chunks and populates information in the db, most notably for
    /// the chunk, chunk extra and flat storage.
    ///
    /// It starts at the height included of the chunk in the sync hash up until
    /// the height of the sync hash.
    ///
    /// The first chunk, the one at height included, is a new chunk. The
    /// remaining ones are old (missing) chunks.
    pub fn set_state_finalize(
        &mut self,
        shard_id: ShardId,
        sync_hash: CryptoHash,
    ) -> Result<(), Error> {
        let _span = tracing::debug_span!(target: "sync", "set_state_finalize").entered();
        let shard_state_header = self.get_state_header(shard_id, sync_hash)?;
        let mut height = shard_state_header.chunk_height_included();
        let mut chain_update = self.chain_update();
        let shard_uid = chain_update.set_state_finalize(shard_id, sync_hash, shard_state_header)?;
        chain_update.commit()?;

        // We restored the state on height `shard_state_header.chunk.header.height_included`.
        // Now we should build a chain up to height of `sync_hash` block.
        loop {
            height += 1;
            let mut chain_update = self.chain_update();
            // Result of successful execution of set_state_finalize_on_height is bool,
            // should we commit and continue or stop.
            if chain_update.set_state_finalize_on_height(height, shard_id, sync_hash)? {
                chain_update.commit()?;
            } else {
                break;
            }
        }

        let flat_storage_manager = self.runtime_adapter.get_flat_storage_manager();
        if let Some(flat_storage) = flat_storage_manager.get_flat_storage_for_shard(shard_uid) {
            let header = self.get_block_header(&sync_hash)?;
            flat_storage.update_flat_head(header.prev_hash()).unwrap();
        }

        Ok(())
    }

    pub fn clear_downloaded_parts(
        &mut self,
        shard_id: ShardId,
        sync_hash: CryptoHash,
        num_parts: u64,
    ) -> Result<(), Error> {
        let mut chain_store_update = self.mut_chain_store().store_update();
        chain_store_update.gc_col_state_parts(sync_hash, shard_id, num_parts)?;
        chain_store_update.commit()
    }

    pub fn catchup_blocks_step(
        &mut self,
        me: &Option<AccountId>,
        sync_hash: &CryptoHash,
        blocks_catch_up_state: &mut BlocksCatchUpState,
        block_catch_up_scheduler: &near_async::messaging::Sender<BlockCatchUpRequest>,
    ) -> Result<(), Error> {
        tracing::debug!(
            target: "catchup",
            pending_blocks = ?blocks_catch_up_state.pending_blocks,
            processed_blocks = ?blocks_catch_up_state.processed_blocks.keys().collect::<Vec<_>>(),
            scheduled_blocks = ?blocks_catch_up_state.scheduled_blocks,
            done_blocks = blocks_catch_up_state.done_blocks.len(),
            "catch up blocks");
        let mut processed_blocks = HashMap::new();
        for (queued_block, results) in blocks_catch_up_state.processed_blocks.drain() {
            // If this block is parent of some blocks in processing that need to be caught up,
            // we can't mark this block as done yet because these blocks haven't been added to
            // the store as blocks to be caught up yet. If we mark this block as done right now,
            // these blocks will never get caught up. So we add these blocks back to the processed_blocks
            // queue.
            if self.blocks_in_processing.has_blocks_to_catch_up(&queued_block) {
                processed_blocks.insert(queued_block, results);
            } else {
                match self.block_catch_up_postprocess(me, &queued_block, results) {
                    Ok(_) => {
                        let mut saw_one = false;
                        for next_block_hash in
                            self.chain_store.get_blocks_to_catchup(&queued_block)?.clone()
                        {
                            saw_one = true;
                            blocks_catch_up_state.pending_blocks.push(next_block_hash);
                        }
                        if saw_one {
                            assert_eq!(
                                self.epoch_manager.get_epoch_id_from_prev_block(&queued_block)?,
                                blocks_catch_up_state.epoch_id
                            );
                        }
                        blocks_catch_up_state.done_blocks.push(queued_block);
                    }
                    Err(_) => {
                        error!("Error processing block during catch up, retrying");
                        blocks_catch_up_state.pending_blocks.push(queued_block);
                    }
                }
            }
        }
        blocks_catch_up_state.processed_blocks = processed_blocks;

        for pending_block in blocks_catch_up_state.pending_blocks.drain(..) {
            let block = self.chain_store.get_block(&pending_block)?.clone();
            let prev_block = self.chain_store.get_block(block.header().prev_hash())?.clone();

            let receipts_by_shard = self.collect_incoming_receipts_from_block(me, &block)?;
            let work = self.apply_chunks_preprocessing(
                me,
                &block,
                &prev_block,
                &receipts_by_shard,
                ApplyChunksMode::CatchingUp,
                Default::default(),
                &mut Vec::new(),
            )?;
            metrics::SCHEDULED_CATCHUP_BLOCK.set(block.header().height() as i64);
            blocks_catch_up_state.scheduled_blocks.insert(pending_block);
            block_catch_up_scheduler.send(BlockCatchUpRequest {
                sync_hash: *sync_hash,
                block_hash: pending_block,
                block_height: block.header().height(),
                work,
            });
        }

        Ok(())
    }

    /// Validates basic correctness of array of transactions included in chunk.
    /// Doesn't require state.
    fn validate_chunk_transactions(
        &self,
        block: &Block,
        prev_block_header: &BlockHeader,
        chunk: &ShardChunk,
    ) -> Result<(), Error> {
        if !validate_transactions_order(chunk.transactions()) {
            let merkle_paths = Block::compute_chunk_headers_root(block.chunks().iter()).1;
            let chunk_proof = ChunkProofs {
                block_header: borsh::to_vec(&block.header()).expect("Failed to serialize"),
                merkle_proof: merkle_paths[chunk.shard_id() as usize].clone(),
                chunk: MaybeEncodedShardChunk::Decoded(chunk.clone()).into(),
            };
            return Err(Error::InvalidChunkProofs(Box::new(chunk_proof)));
        }

        let protocol_version =
            self.epoch_manager.get_epoch_protocol_version(block.header().epoch_id())?;
        if checked_feature!("stable", AccessKeyNonceRange, protocol_version) {
            let transaction_validity_period = self.transaction_validity_period;
            for transaction in chunk.transactions() {
                self.chain_store()
                    .check_transaction_validity_period(
                        prev_block_header,
                        transaction.transaction.block_hash(),
                        transaction_validity_period,
                    )
                    .map_err(|_| Error::from(Error::InvalidTransactions))?;
            }
        };

        Ok(())
    }

    pub fn transaction_validity_check<'a>(
        &'a self,
        prev_block_header: BlockHeader,
    ) -> impl FnMut(&SignedTransaction) -> bool + 'a {
        move |tx: &SignedTransaction| -> bool {
            self.chain_store()
                .check_transaction_validity_period(
                    &prev_block_header,
                    tx.transaction.block_hash(),
                    self.transaction_validity_period,
                )
                .is_ok()
        }
    }

    /// For a given previous block header and current block, return information
    /// about block necessary for processing shard update.
    pub fn get_apply_chunk_block_context(
        epoch_manager: &dyn EpochManagerAdapter,
        block: &Block,
        prev_block_header: &BlockHeader,
        is_new_chunk: bool,
    ) -> Result<ApplyChunkBlockContext, Error> {
        let block_header = &block.header();
        let epoch_id = block_header.epoch_id();
        let protocol_version = epoch_manager.get_epoch_protocol_version(epoch_id)?;
        // Before `FixApplyChunks` feature, gas price was taken from current
        // block by mistake. Preserve it for backwards compatibility.
        let gas_price = if !is_new_chunk
            && protocol_version < ProtocolFeature::FixApplyChunks.protocol_version()
        {
            block_header.next_gas_price()
        } else {
            prev_block_header.next_gas_price()
        };
        let congestion_info = block.block_congestion_info();

        Ok(ApplyChunkBlockContext::from_header(block_header, gas_price, congestion_info))
    }

    fn block_catch_up_postprocess(
        &mut self,
        me: &Option<AccountId>,
        block_hash: &CryptoHash,
        results: Vec<Result<ShardUpdateResult, Error>>,
    ) -> Result<(), Error> {
        let block = self.chain_store.get_block(block_hash)?;
        // Save state transition data to the database only if it might later be needed
        // for generating a state witness. Storage space optimization.
        let should_save_state_transition_data =
            self.should_produce_state_witness_for_this_or_next_epoch(me, block.header())?;
        let mut chain_update = self.chain_update();
        let results = results.into_iter().collect::<Result<Vec<_>, Error>>()?;
        chain_update.apply_chunk_postprocessing(
            &block,
            results,
            should_save_state_transition_data,
        )?;
        chain_update.commit()?;

        let epoch_id = block.header().epoch_id();
        for shard_id in self.epoch_manager.shard_ids(epoch_id)? {
            // Update flat storage for each shard being caught up. We catch up a shard if it is tracked in the next
            // epoch. If it is tracked in this epoch as well, it was updated during regular block processing.
            if !self.shard_tracker.care_about_shard(
                me.as_ref(),
                block.header().prev_hash(),
                shard_id,
                true,
            ) && self.shard_tracker.will_care_about_shard(
                me.as_ref(),
                block.header().prev_hash(),
                shard_id,
                true,
            ) {
                self.update_flat_storage_and_memtrie(&block, shard_id)?;
            }
        }

        Ok(())
    }

    /// Apply transactions in chunks for the next epoch in blocks that were blocked on the state sync
    pub fn finish_catchup_blocks(
        &mut self,
        me: &Option<AccountId>,
        epoch_first_block: &CryptoHash,
        block_processing_artifacts: &mut BlockProcessingArtifact,
        apply_chunks_done_sender: Option<near_async::messaging::Sender<ApplyChunksDoneMessage>>,
        affected_blocks: &[CryptoHash],
    ) -> Result<(), Error> {
        debug!(
            "Finishing catching up blocks after syncing pre {:?}, me: {:?}",
            epoch_first_block, me
        );

        let first_block = self.chain_store.get_block(epoch_first_block)?;

        let mut chain_store_update = ChainStoreUpdate::new(&mut self.chain_store);

        // `blocks_to_catchup` consists of pairs (`prev_hash`, `hash`). For the block that precedes
        // `epoch_first_block` we should only remove the pair with hash = epoch_first_block, while
        // for all the blocks in the queue we can remove all the pairs that have them as `prev_hash`
        // since we processed all the blocks built on top of them above during the BFS
        chain_store_update
            .remove_block_to_catchup(*first_block.header().prev_hash(), *epoch_first_block);

        for block_hash in affected_blocks {
            debug!(target: "chain", "Catching up: removing prev={:?} from the queue. I'm {:?}", block_hash, me);
            chain_store_update.remove_prev_block_to_catchup(*block_hash);
        }
        chain_store_update.remove_state_sync_info(*epoch_first_block);

        // Remove all stored split state changes for resharding once catchup is completed.
        // We only remove these after the catchup is completed to ensure that restarting the node
        // in the middle of the catchup does not lead to the split state already being deleted from prior run
        chain_store_update.remove_all_state_changes_for_resharding();

        chain_store_update.commit()?;

        for hash in affected_blocks.iter() {
            self.check_orphans(
                me,
                *hash,
                block_processing_artifacts,
                apply_chunks_done_sender.clone(),
            );
        }

        // Nit: it would be more elegant to only call this after resharding, not
        // after every state sync but it doesn't hurt.
        self.process_snapshot_after_resharding()?;

        Ok(())
    }

    pub fn get_transaction_execution_result(
        &self,
        id: &CryptoHash,
    ) -> Result<Vec<ExecutionOutcomeWithIdView>, Error> {
        Ok(self.chain_store.get_outcomes_by_id(id)?.into_iter().map(Into::into).collect())
    }

    /// Returns execution status based on the list of currently existing outcomes
    fn get_execution_status(
        &self,
        outcomes: &[ExecutionOutcomeWithIdView],
        transaction_hash: &CryptoHash,
    ) -> FinalExecutionStatus {
        if outcomes.is_empty() {
            return FinalExecutionStatus::NotStarted;
        }
        let mut looking_for_id = *transaction_hash;
        let num_outcomes = outcomes.len();
        outcomes
            .iter()
            .find_map(|outcome_with_id| {
                if outcome_with_id.id == looking_for_id {
                    match &outcome_with_id.outcome.status {
                        ExecutionStatusView::Unknown if num_outcomes == 1 => {
                            Some(FinalExecutionStatus::NotStarted)
                        }
                        ExecutionStatusView::Unknown => Some(FinalExecutionStatus::Started),
                        ExecutionStatusView::Failure(e) => {
                            Some(FinalExecutionStatus::Failure(e.clone()))
                        }
                        ExecutionStatusView::SuccessValue(v) => {
                            Some(FinalExecutionStatus::SuccessValue(v.clone()))
                        }
                        ExecutionStatusView::SuccessReceiptId(id) => {
                            looking_for_id = *id;
                            None
                        }
                    }
                } else {
                    None
                }
            })
            .unwrap_or_else(|| FinalExecutionStatus::Started)
    }

    /// Collect all the execution outcomes existing at the current moment
    /// Fails if there are non executed receipts, and require_all_outcomes == true
    fn get_recursive_transaction_results(
        &self,
        outcomes: &mut Vec<ExecutionOutcomeWithIdView>,
        id: &CryptoHash,
        require_all_outcomes: bool,
    ) -> Result<(), Error> {
        let outcome = match self.get_execution_outcome(id) {
            Ok(outcome) => outcome,
            Err(err) => return if require_all_outcomes { Err(err) } else { Ok(()) },
        };
        outcomes.push(ExecutionOutcomeWithIdView::from(outcome));
        let outcome_idx = outcomes.len() - 1;
        for idx in 0..outcomes[outcome_idx].outcome.receipt_ids.len() {
            let id = outcomes[outcome_idx].outcome.receipt_ids[idx];
            self.get_recursive_transaction_results(outcomes, &id, require_all_outcomes)?;
        }
        Ok(())
    }

    /// Returns FinalExecutionOutcomeView for the given transaction.
    /// Waits for the end of the execution of all corresponding receipts
    pub fn get_final_transaction_result(
        &self,
        transaction_hash: &CryptoHash,
    ) -> Result<FinalExecutionOutcomeView, Error> {
        let mut outcomes = Vec::new();
        self.get_recursive_transaction_results(&mut outcomes, transaction_hash, true)?;
        let status = self.get_execution_status(&outcomes, transaction_hash);
        let receipts_outcome = outcomes.split_off(1);
        let transaction = self.chain_store.get_transaction(transaction_hash)?.ok_or_else(|| {
            Error::DBNotFoundErr(format!("Transaction {} is not found", transaction_hash))
        })?;
        let transaction: SignedTransactionView = SignedTransaction::clone(&transaction).into();
        let transaction_outcome = outcomes.pop().unwrap();
        Ok(FinalExecutionOutcomeView { status, transaction, transaction_outcome, receipts_outcome })
    }

    /// Returns FinalExecutionOutcomeView for the given transaction.
    /// Does not wait for the end of the execution of all corresponding receipts
    pub fn get_partial_transaction_result(
        &self,
        transaction_hash: &CryptoHash,
    ) -> Result<FinalExecutionOutcomeView, Error> {
        let transaction = self.chain_store.get_transaction(transaction_hash)?.ok_or_else(|| {
            Error::DBNotFoundErr(format!("Transaction {} is not found", transaction_hash))
        })?;
        let transaction: SignedTransactionView = SignedTransaction::clone(&transaction).into();

        let mut outcomes = Vec::new();
        self.get_recursive_transaction_results(&mut outcomes, transaction_hash, false)?;
        if outcomes.is_empty() {
            // It can't be, we would fail with tx not found error earlier in this case
            // But if so, let's return meaningful error instead of panic on split_off
            return Err(Error::DBNotFoundErr(format!(
                "Transaction {} is not found",
                transaction_hash
            )));
        }

        let status = self.get_execution_status(&outcomes, transaction_hash);
        let receipts_outcome = outcomes.split_off(1);
        let transaction_outcome = outcomes.pop().unwrap();
        Ok(FinalExecutionOutcomeView { status, transaction, transaction_outcome, receipts_outcome })
    }

    /// Returns corresponding receipts for provided outcome
    /// The incoming list in receipts_outcome may be partial
    pub fn get_transaction_result_with_receipt(
        &self,
        outcome: FinalExecutionOutcomeView,
    ) -> Result<FinalExecutionOutcomeWithReceiptView, Error> {
        let receipt_id_from_transaction =
            outcome.transaction_outcome.outcome.receipt_ids.get(0).cloned();
        let is_local_receipt = outcome.transaction.signer_id == outcome.transaction.receiver_id;

        let receipts = outcome
            .receipts_outcome
            .iter()
            .filter_map(|outcome| {
                if Some(outcome.id) == receipt_id_from_transaction && is_local_receipt {
                    None
                } else {
                    Some(self.chain_store.get_receipt(&outcome.id).and_then(|r| {
                        r.map(|r| Receipt::clone(&r).into()).ok_or_else(|| {
                            Error::DBNotFoundErr(format!("Receipt {} is not found", outcome.id))
                        })
                    }))
                }
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(FinalExecutionOutcomeWithReceiptView { final_outcome: outcome, receipts })
    }

    pub fn check_blocks_final_and_canonical(
        &self,
        block_headers: &[BlockHeader],
    ) -> Result<(), Error> {
        let last_final_block_hash = *self.head_header()?.last_final_block();
        let last_final_height = self.get_block_header(&last_final_block_hash)?.height();
        for hdr in block_headers {
            if hdr.height() > last_final_height || !self.is_on_current_chain(&hdr)? {
                return Err(Error::Other(format!("{} not on current chain", hdr.hash())));
            }
        }
        Ok(())
    }

    pub fn create_chunk_state_challenge(
        &self,
        prev_block: &Block,
        block: &Block,
        chunk_header: &ShardChunkHeader,
    ) -> Result<ChunkState, Error> {
        let chunk_shard_id = chunk_header.shard_id();
        let prev_merkle_proofs = Block::compute_chunk_headers_root(prev_block.chunks().iter()).1;
        let merkle_proofs = Block::compute_chunk_headers_root(block.chunks().iter()).1;
        let prev_chunk = self
            .get_chunk_clone_from_header(&prev_block.chunks()[chunk_shard_id as usize].clone())
            .unwrap();

        // TODO (#6316): enable storage proof generation
        // let prev_chunk_header = &prev_block.chunks()[chunk_shard_id as usize];
        // let receipt_proof_response: Vec<ReceiptProofResponse> =
        //     self.chain_store_update.get_incoming_receipts_for_shard(
        //         chunk_shard_id,
        //         *prev_block.hash(),
        //         prev_chunk_header.height_included(),
        //     )?;
        // let receipts = collect_receipts_from_response(&receipt_proof_response);
        //
        // let challenges_result = self.verify_challenges(
        //     block.challenges(),
        //     block.header().epoch_id(),
        //     block.header().prev_hash(),
        //     Some(block.hash()),
        // )?;
        // let prev_chunk_inner = prev_chunk.cloned_header().take_inner();
        // let is_first_block_with_chunk_of_version = check_if_block_is_first_with_chunk_of_version(
        //     &mut self.chain_store_update,
        //     self.runtime_adapter.as_ref(),
        //     prev_block.hash(),
        //     chunk_shard_id,
        // )?;
        // let apply_result = self
        //     .runtime_adapter
        //     .apply_transactions_with_optional_storage_proof(
        //         chunk_shard_id,
        //         prev_chunk_inner.prev_state_root(),
        //         prev_chunk.height_included(),
        //         prev_block.header().raw_timestamp(),
        //         prev_chunk_inner.prev_block_hash(),
        //         prev_block.hash(),
        //         &receipts,
        //         prev_chunk.transactions(),
        //         prev_chunk_inner.validator_proposals(),
        //         prev_block.header().gas_price(),
        //         prev_chunk_inner.gas_limit(),
        //         &challenges_result,
        //         *block.header().random_value(),
        //         true,
        //         true,
        //         is_first_block_with_chunk_of_version,
        //         None,
        //     )
        //     .unwrap();
        // let partial_state = apply_result.proof.unwrap().nodes;
        Ok(ChunkState {
            prev_block_header: borsh::to_vec(&prev_block.header())?,
            block_header: borsh::to_vec(&block.header())?,
            prev_merkle_proof: prev_merkle_proofs[chunk_shard_id as usize].clone(),
            merkle_proof: merkle_proofs[chunk_shard_id as usize].clone(),
            prev_chunk,
            chunk_header: chunk_header.clone(),
            partial_state: PartialState::TrieValues(vec![]),
        })
    }

    fn get_resharding_state_roots(
        &self,
        block: &Block,
        shard_id: ShardId,
    ) -> Result<HashMap<ShardUId, StateRoot>, Error> {
        let next_shard_layout =
            self.epoch_manager.get_shard_layout(block.header().next_epoch_id())?;
        let new_shards =
            next_shard_layout.get_children_shards_uids(shard_id).unwrap_or_else(|| {
                panic!(
                    "shard layout must contain maps of all shards to its children shards {} {:?}",
                    shard_id, next_shard_layout,
                );
            });
        new_shards
            .iter()
            .map(|shard_uid| {
                self.get_chunk_extra(block.header().prev_hash(), shard_uid)
                    .map(|chunk_extra| (*shard_uid, *chunk_extra.state_root()))
            })
            .collect()
    }

    /// Returns sequence of blocks in chain from `last_block_hash` (inclusive)
    /// until the block with height `first_block_height` (inclusive if `include_with_height`
    /// is true). For each block hash in resulting `Vec`, next entry contains hash of its
    /// parent on chain.
    /// TODO(logunov): consider uniting with `get_incoming_receipts_for_shard` because it
    /// has the same purpose.
    pub fn get_blocks_until_height(
        &self,
        mut last_block_hash: CryptoHash,
        first_block_height: BlockHeight,
        include_with_height: bool,
    ) -> Result<Vec<CryptoHash>, Error> {
        let mut blocks = vec![];
        loop {
            let header = self.get_block_header(&last_block_hash)?;
            if header.height() < first_block_height {
                return Err(Error::InvalidBlockHeight(first_block_height));
            }

            if header.height() == first_block_height {
                break;
            }

            blocks.push(last_block_hash);
            last_block_hash = *header.prev_hash();
        }
        if include_with_height {
            blocks.push(last_block_hash);
        }
        Ok(blocks)
    }

    /// Checks whether `me` is chunk producer for this or next epoch, given
    /// block header which is not in DB yet. If this is the case, node must
    /// produce necessary data for state witness.
    /// TODO(#9292): Check this for specific shard by extending EpochManager
    /// interface. Consider asserting that node tracks the shard. Consider
    /// returning true only if node produces state witness only for the next
    /// chunk. However, node can't determine this if next validators missed
    /// chunks.
    pub fn should_produce_state_witness_for_this_or_next_epoch(
        &self,
        me: &Option<AccountId>,
        block_header: &BlockHeader,
    ) -> Result<bool, Error> {
        if cfg!(feature = "shadow_chunk_validation") {
            return Ok(true);
        }
        let epoch_id = block_header.epoch_id();
        // Use epoch manager because block is not in DB yet.
        let next_epoch_id =
            self.epoch_manager.get_next_epoch_id_from_prev_block(block_header.prev_hash())?;
        let next_protocol_version =
            self.epoch_manager.get_epoch_protocol_version(&next_epoch_id)?;
        if !checked_feature!("stable", StatelessValidation, next_protocol_version) {
            // Chunk validation not enabled yet.
            return Ok(false);
        }
        let Some(account_id) = me.as_ref() else { return Ok(false) };
        Ok(self.epoch_manager.is_chunk_producer_for_epoch(epoch_id, account_id)?
            || self.epoch_manager.is_chunk_producer_for_epoch(&next_epoch_id, account_id)?)
    }

    /// Creates jobs which will update shards for the given block and incoming
    /// receipts aggregated for it.
    fn apply_chunks_preprocessing(
        &self,
        me: &Option<AccountId>,
        block: &Block,
        prev_block: &Block,
        incoming_receipts: &HashMap<ShardId, Vec<ReceiptProof>>,
        mode: ApplyChunksMode,
        mut state_patch: SandboxStatePatch,
        invalid_chunks: &mut Vec<ShardChunkHeader>,
    ) -> Result<Vec<UpdateShardJob>, Error> {
        let _span = tracing::debug_span!(target: "chain", "apply_chunks_preprocessing").entered();
        let prev_chunk_headers =
            Chain::get_prev_chunk_headers(self.epoch_manager.as_ref(), prev_block)?;

        let mut maybe_jobs = vec![];
        for (shard_id, (chunk_header, prev_chunk_header)) in
            block.chunks().iter().zip(prev_chunk_headers.iter()).enumerate()
        {
            // XXX: This is a bit questionable -- sandbox state patching works
            // only for a single shard. This so far has been enough.
            let state_patch = state_patch.take();

            let storage_context =
                StorageContext { storage_data_source: StorageDataSource::Db, state_patch };
            let stateful_job = self.get_update_shard_job(
                me,
                block,
                prev_block,
                chunk_header,
                prev_chunk_header,
                shard_id as ShardId,
                mode,
                incoming_receipts,
                storage_context,
            );
            maybe_jobs.push((shard_id, stateful_job));
        }

        let mut jobs = vec![];
        for (shard_id, maybe_job) in maybe_jobs {
            match maybe_job {
                Ok(Some(processor)) => jobs.push(processor),
                Ok(None) => {}
                Err(err) => {
                    if err.is_bad_data() {
                        let chunk_header = block
                            .chunks()
                            .get(shard_id)
                            .ok_or_else(|| Error::InvalidShardId(shard_id as ShardId))?
                            .clone();
                        invalid_chunks.push(chunk_header);
                    }
                    return Err(err);
                }
            }
        }

        Ok(jobs)
    }

    fn get_shard_context(
        &self,
        me: &Option<AccountId>,
        block_header: &BlockHeader,
        shard_id: ShardId,
        mode: ApplyChunksMode,
    ) -> Result<ShardContext, Error> {
        let prev_hash = block_header.prev_hash();
        let epoch_id = block_header.epoch_id();
        let cares_about_shard_this_epoch =
            self.shard_tracker.care_about_shard(me.as_ref(), prev_hash, shard_id, true);
        let cares_about_shard_next_epoch =
            self.shard_tracker.will_care_about_shard(me.as_ref(), prev_hash, shard_id, true);
        let will_shard_layout_change = self.epoch_manager.will_shard_layout_change(prev_hash)?;
        let should_apply_chunk = get_should_apply_chunk(
            mode,
            cares_about_shard_this_epoch,
            cares_about_shard_next_epoch,
        );
        let need_to_reshard = will_shard_layout_change && cares_about_shard_next_epoch;
        let shard_uid = self.epoch_manager.shard_id_to_uid(shard_id, epoch_id)?;
        Ok(ShardContext {
            shard_uid,
            cares_about_shard_this_epoch,
            will_shard_layout_change,
            should_apply_chunk,
            need_to_reshard,
        })
    }

    /// This method returns the closure that is responsible for updating a shard.
    fn get_update_shard_job(
        &self,
        me: &Option<AccountId>,
        block: &Block,
        prev_block: &Block,
        chunk_header: &ShardChunkHeader,
        prev_chunk_header: &ShardChunkHeader,
        shard_id: ShardId,
        mode: ApplyChunksMode,
        incoming_receipts: &HashMap<u64, Vec<ReceiptProof>>,
        storage_context: StorageContext,
    ) -> Result<Option<UpdateShardJob>, Error> {
        let _span = tracing::debug_span!(target: "chain", "get_update_shard_job").entered();
        let prev_hash = block.header().prev_hash();
        let shard_context = self.get_shard_context(me, block.header(), shard_id, mode)?;

        // We can only perform resharding when states are ready, i.e., mode != ApplyChunksMode::NotCaughtUp
        // 1) if should_apply_chunk == true && resharding_state_roots.is_some(),
        //     that means children shards are ready.
        //    `apply_resharding_state_changes` will apply updates to the children shards
        // 2) if should_apply_chunk == true && resharding_state_roots.is_none(),
        //     that means children shards are not ready yet.
        //    `apply_resharding_state_changes` will return `state_changes_for_resharding`,
        //     which will be stored to the database in `process_apply_chunks`
        // 3) if should_apply_chunk == false && resharding_state_roots.is_some()
        //    This implies mode == CatchingUp and cares_about_shard_this_epoch == true,
        //    otherwise should_apply_chunk will be true
        //    That means transactions have already been applied last time when apply_chunks are
        //    called with mode NotCaughtUp, therefore `state_changes_for_resharding` have been
        //    stored in the database. Then we can safely read that and apply that to the split
        //    states
        let resharding_state_roots =
            if shard_context.need_to_reshard && mode != ApplyChunksMode::NotCaughtUp {
                Some(self.get_resharding_state_roots(block, shard_id)?)
            } else {
                None
            };

        let is_new_chunk = chunk_header.is_new_chunk(block.header().height());
        let shard_update_reason = if shard_context.should_apply_chunk {
            let block_context = Self::get_apply_chunk_block_context(
                self.epoch_manager.as_ref(),
                &block,
                prev_block.header(),
                is_new_chunk,
            )?;
            if is_new_chunk {
                // Validate new chunk and collect incoming receipts for it.

                let prev_chunk_extra = self.get_chunk_extra(prev_hash, &shard_context.shard_uid)?;
                let chunk = self.get_chunk_clone_from_header(&chunk_header)?;
                let prev_chunk_height_included = prev_chunk_header.height_included();

                // Validate that all next chunk information matches previous chunk extra.
                validate_chunk_with_chunk_extra(
                    // It's safe here to use ChainStore instead of ChainStoreUpdate
                    // because we're asking prev_chunk_header for already committed block
                    self.chain_store(),
                    self.epoch_manager.as_ref(),
                    prev_hash,
                    prev_chunk_extra.as_ref(),
                    prev_chunk_height_included,
                    &chunk_header,
                )
                .map_err(|err| {
                    warn!(
                        target: "chain",
                        ?err,
                        prev_block_hash=?prev_hash,
                        block_hash=?block.header().hash(),
                        shard_id,
                        prev_chunk_height_included,
                        ?prev_chunk_extra,
                        ?chunk_header,
                        "Failed to validate chunk extra"
                    );
                    byzantine_assert!(false);
                    match self.create_chunk_state_challenge(prev_block, block, &chunk_header) {
                        Ok(chunk_state) => Error::InvalidChunkState(Box::new(chunk_state)),
                        Err(err) => err,
                    }
                })?;

                self.validate_chunk_transactions(&block, prev_block.header(), &chunk)?;

                // we can't use hash from the current block here yet because the incoming receipts
                // for this block is not stored yet
                let new_receipts = collect_receipts(incoming_receipts.get(&shard_id).unwrap());
                let old_receipts = &self.chain_store().get_incoming_receipts_for_shard(
                    self.epoch_manager.as_ref(),
                    shard_id,
                    *prev_hash,
                    prev_chunk_height_included,
                )?;
                let old_receipts = collect_receipts_from_response(old_receipts);
                let receipts = [new_receipts, old_receipts].concat();

                // This variable is responsible for checking to which block we can apply receipts previously lost in apply_chunks
                // (see https://github.com/near/nearcore/pull/4248/)
                // We take the first block with existing chunk in the first epoch in which protocol feature
                // RestoreReceiptsAfterFixApplyChunks was enabled, and put the restored receipts there.
                let is_first_block_with_chunk_of_version =
                    check_if_block_is_first_with_chunk_of_version(
                        self.chain_store(),
                        self.epoch_manager.as_ref(),
                        block.header().prev_hash(),
                        shard_id,
                    )?;

                ShardUpdateReason::NewChunk(NewChunkData {
                    chunk_header: chunk_header.clone(),
                    transactions: chunk.transactions().to_vec(),
                    receipts,
                    block: block_context,
                    is_first_block_with_chunk_of_version,
                    resharding_state_roots,
                    storage_context,
                })
            } else {
                ShardUpdateReason::OldChunk(OldChunkData {
                    block: block_context,
                    prev_chunk_extra: ChunkExtra::clone(
                        self.get_chunk_extra(prev_hash, &shard_context.shard_uid)?.as_ref(),
                    ),
                    resharding_state_roots,
                    storage_context,
                })
            }
        } else if let Some(resharding_state_roots) = resharding_state_roots {
            assert!(
                mode == ApplyChunksMode::CatchingUp && shard_context.cares_about_shard_this_epoch
            );
            let state_changes =
                self.chain_store().get_state_changes_for_resharding(block.hash(), shard_id)?;
            ShardUpdateReason::Resharding(ReshardingData {
                block_hash: *block.hash(),
                block_height: block.header().height(),
                state_changes,
                resharding_state_roots,
            })
        } else {
            return Ok(None);
        };

        let runtime = self.runtime_adapter.clone();
        let epoch_manager = self.epoch_manager.clone();
        Ok(Some((
            shard_id,
            Box::new(move |parent_span| -> Result<ShardUpdateResult, Error> {
                Ok(process_shard_update(
                    parent_span,
                    runtime.as_ref(),
                    epoch_manager.as_ref(),
                    shard_update_reason,
                    shard_context,
                )?)
            }),
        )))
    }

    /// Function to create or delete a snapshot if necessary.
    fn process_snapshot(&mut self) -> Result<(), Error> {
        let (make_snapshot, delete_snapshot) = self.should_make_or_delete_snapshot()?;
        if !make_snapshot && !delete_snapshot {
            return Ok(());
        }
        let Some(snapshot_callbacks) = &self.snapshot_callbacks else { return Ok(()) };
        if make_snapshot {
            let head = self.head()?;
            let prev_hash = head.prev_block_hash;
            let epoch_height = self.epoch_manager.get_epoch_height_from_prev_block(&prev_hash)?;
            let shard_layout = &self.epoch_manager.get_shard_layout_from_prev_block(&prev_hash)?;
            let shard_uids = shard_layout.shard_uids().collect();
            let last_block = self.get_block(&head.last_block_hash)?;
            let make_snapshot_callback = &snapshot_callbacks.make_snapshot_callback;
            make_snapshot_callback(prev_hash, epoch_height, shard_uids, last_block);
        } else if delete_snapshot {
            let delete_snapshot_callback = &snapshot_callbacks.delete_snapshot_callback;
            delete_snapshot_callback();
        }
        Ok(())
    }

    // Similar to `process_snapshot` but only called after resharding and
    // catchup is done. This is to speed up the snapshot removal once resharding
    // is finished in order to minimize the storage overhead.
    fn process_snapshot_after_resharding(&mut self) -> Result<(), Error> {
        let Some(snapshot_callbacks) = &self.snapshot_callbacks else { return Ok(()) };

        let tries = self.runtime_adapter.get_tries();
        let snapshot_config = tries.state_snapshot_config();
        let delete_snapshot = match snapshot_config.state_snapshot_type {
            // Do not delete snapshot if the node is configured to snapshot every epoch.
            StateSnapshotType::EveryEpoch => false,
            // Delete the snapshot if it was created only for resharding.
            StateSnapshotType::ForReshardingOnly => true,
        };

        if delete_snapshot {
            tracing::debug!(target: "resharding", "deleting snapshot after resharding");
            let delete_snapshot_callback = &snapshot_callbacks.delete_snapshot_callback;
            delete_snapshot_callback();
        }

        Ok(())
    }

    /// Function to check whether we need to create a new snapshot while processing the current block
    /// Note that this functions is called as a part of block preprocesing, so the head is not updated to current block
    fn should_make_or_delete_snapshot(&mut self) -> Result<(bool, bool), Error> {
        // head value is that of the previous block, i.e. curr_block.prev_hash
        let head = self.head()?;
        if head.prev_block_hash == CryptoHash::default() {
            // genesis block, do not snapshot
            return Ok((false, false));
        }

        let is_epoch_boundary =
            self.epoch_manager.is_next_block_epoch_start(&head.last_block_hash)?;
        let will_shard_layout_change =
            self.epoch_manager.will_shard_layout_change(&head.last_block_hash)?;
        let tries = self.runtime_adapter.get_tries();
        let snapshot_config = tries.state_snapshot_config();
        let make_snapshot = match snapshot_config.state_snapshot_type {
            // For every epoch, we snapshot if the next block would be in a different epoch
            StateSnapshotType::EveryEpoch => is_epoch_boundary,
            // For resharding only, we snapshot if next block would be in a different shard layout
            StateSnapshotType::ForReshardingOnly => is_epoch_boundary && will_shard_layout_change,
        };

        // We need to delete the existing snapshot at the epoch boundary if we are not making a new snapshot
        // This is useful for the next epoch after resharding where make_snapshot is false but it's an epoch boundary
        let delete_snapshot = !make_snapshot && is_epoch_boundary;

        Ok((make_snapshot, delete_snapshot))
    }

    /// Returns a description of state parts cached for the given shard of the given epoch.
    pub fn get_cached_state_parts(
        &self,
        sync_hash: CryptoHash,
        shard_id: ShardId,
        num_parts: u64,
    ) -> Result<CachedParts, Error> {
        let _span = tracing::debug_span!(target: "chain", "get_cached_state_parts").entered();
        // DBCol::StateParts is keyed by StatePartKey: (BlockHash || ShardId || PartId (u64)).
        let lower_bound = StatePartKey(sync_hash, shard_id, 0);
        let lower_bound = borsh::to_vec(&lower_bound)?;
        let upper_bound = StatePartKey(sync_hash, shard_id + 1, 0);
        let upper_bound = borsh::to_vec(&upper_bound)?;
        let mut num_cached_parts = 0;
        let mut bit_array = BitArray::new(num_parts);
        for item in self.chain_store.store().iter_range(
            DBCol::StateParts,
            Some(&lower_bound),
            Some(&upper_bound),
        ) {
            let key = item?.0;
            let key = StatePartKey::try_from_slice(&key)?;
            let part_id = key.2;
            num_cached_parts += 1;
            bit_array.set_bit(part_id);
        }
        Ok(if num_cached_parts == 0 {
            CachedParts::NoParts
        } else if num_cached_parts == num_parts {
            CachedParts::AllParts
        } else {
            CachedParts::BitArray(bit_array)
        })
    }
}

/// This method calculates the congestion info for the genesis chunks. It uses
/// the congestion info bootstrapping logic. This method is just a wrapper
/// around the [`get_genesis_congestion_infos_impl`]. It logs an error if one
/// happens.
pub fn get_genesis_congestion_infos(
    epoch_manager: &dyn EpochManagerAdapter,
    runtime: &dyn RuntimeAdapter,
    state_roots: &Vec<CryptoHash>,
) -> Result<Vec<Option<CongestionInfo>>, Error> {
    get_genesis_congestion_infos_impl(epoch_manager, runtime, state_roots).map_err(|err| {
        tracing::error!(target: "chain", ?err, "Failed to get the genesis congestion infos.");
        err
    })
}

fn get_genesis_congestion_infos_impl(
    epoch_manager: &dyn EpochManagerAdapter,
    runtime: &dyn RuntimeAdapter,
    state_roots: &Vec<CryptoHash>,
) -> Result<Vec<Option<CongestionInfo>>, Error> {
    let genesis_prev_hash = CryptoHash::default();
    let genesis_epoch_id = epoch_manager.get_epoch_id_from_prev_block(&genesis_prev_hash)?;
    let genesis_protocol_version = epoch_manager.get_epoch_protocol_version(&genesis_epoch_id)?;
    // If congestion control is not enabled at the genesis block, we return None (congestion info) for each shard.
    if !ProtocolFeature::CongestionControl.enabled(genesis_protocol_version) {
        return Ok(std::iter::repeat(None).take(state_roots.len()).collect());
    }

    // Since the congestion info is already bootstrapped in statelessnet, skip another bootstrap.
    // TODO: This is temporary mitigation for the failing genesis congestion info due to garbage
    // collected genesis state roots. It can be removed after the statelessnet network is turned down.
    if let Ok(protocol_config) = runtime.get_protocol_config(&genesis_epoch_id) {
        if protocol_config.genesis_config.chain_id == near_primitives::chains::STATELESSNET {
            return Ok(std::iter::repeat(None).take(state_roots.len()).collect());
        }
    }

    // Check we had already computed the congestion infos from the genesis state roots.
    if let Some(saved_infos) = near_store::get_genesis_congestion_infos(runtime.store())? {
        tracing::debug!(target: "chain", "Reading genesis congestion infos from database.");
        return Ok(saved_infos.into_iter().map(Option::Some).collect());
    }

    let mut new_infos = vec![];
    for (shard_id, &state_root) in state_roots.iter().enumerate() {
        let shard_id = shard_id as ShardId;
        let congestion_info = get_genesis_congestion_info(
            runtime,
            genesis_protocol_version,
            &genesis_prev_hash,
            shard_id,
            state_root,
        )?;
        new_infos.push(congestion_info);
    }

    // Store it in DB so that we can read it later, instead of recomputing from genesis state roots.
    // Note that this is necessary because genesis state roots will be garbage-collected and will not
    // be available, for example, when the node restarts later.
    tracing::debug!(target: "chain", "Saving genesis congestion infos to database.");
    let mut store_update = runtime.store().store_update();
    near_store::set_genesis_congestion_infos(&mut store_update, &new_infos);
    store_update.commit()?;

    Ok(new_infos.into_iter().map(Option::Some).collect())
}

fn get_genesis_congestion_info(
    runtime: &dyn RuntimeAdapter,
    protocol_version: ProtocolVersion,
    prev_hash: &CryptoHash,
    shard_id: ShardId,
    state_root: StateRoot,
) -> Result<CongestionInfo, Error> {
    // Get the view trie because it's possible that the chain is ahead of
    // genesis and doesn't have this block in flat state and memtrie.
    let trie = runtime.get_view_trie_for_shard(shard_id, prev_hash, state_root)?;
    let runtime_config = runtime.get_runtime_config(protocol_version)?;
    let congestion_info = bootstrap_congestion_info(&trie, &runtime_config, shard_id)?;
    tracing::debug!(target: "chain", ?shard_id, ?state_root, ?congestion_info, "Computed genesis congestion info.");
    Ok(congestion_info)
}

fn shard_id_out_of_bounds(shard_id: ShardId) -> Error {
    Error::InvalidStateRequest(format!("shard_id {shard_id:?} out of bounds").into())
}

fn sync_hash_not_first_hash(sync_hash: CryptoHash) -> Error {
    Error::InvalidStateRequest(
        format!("sync_hash {sync_hash:?} is not the first hash of the epoch").into(),
    )
}

/// We want to guarantee that transactions are only applied once for each shard,
/// even though apply_chunks may be called twice, once with
/// ApplyChunksMode::NotCaughtUp once with ApplyChunksMode::CatchingUp. Note
/// that it does not guard whether the children shards are ready or not, see the
/// comments before `need_to_reshard`
fn get_should_apply_chunk(
    mode: ApplyChunksMode,
    cares_about_shard_this_epoch: bool,
    cares_about_shard_next_epoch: bool,
) -> bool {
    match mode {
        // next epoch's shard states are not ready, only update this epoch's shards
        ApplyChunksMode::NotCaughtUp => cares_about_shard_this_epoch,
        // update both this epoch and next epoch
        ApplyChunksMode::IsCaughtUp => cares_about_shard_this_epoch || cares_about_shard_next_epoch,
        // catching up next epoch's shard states, do not update this epoch's shard state
        // since it has already been updated through ApplyChunksMode::NotCaughtUp
        ApplyChunksMode::CatchingUp => {
            !cares_about_shard_this_epoch && cares_about_shard_next_epoch
        }
    }
}

/// Implement block merkle proof retrieval.
impl Chain {
    fn combine_maybe_hashes(
        hash1: Option<MerkleHash>,
        hash2: Option<MerkleHash>,
    ) -> Option<MerkleHash> {
        match (hash1, hash2) {
            (Some(h1), Some(h2)) => Some(combine_hash(&h1, &h2)),
            (Some(h1), None) => Some(h1),
            (None, Some(_)) => {
                debug_assert!(false, "Inconsistent state in merkle proof computation: left node is None but right node exists");
                None
            }
            _ => None,
        }
    }

    fn chain_update(&mut self) -> ChainUpdate {
        ChainUpdate::new(
            &mut self.chain_store,
            self.epoch_manager.clone(),
            self.runtime_adapter.clone(),
            self.doomslug_threshold_mode,
            self.transaction_validity_period,
        )
    }

    /// Get node at given position (index, level). If the node does not exist, return `None`.
    fn get_merkle_tree_node(
        &self,
        index: u64,
        level: u64,
        counter: u64,
        tree_size: u64,
        tree_nodes: &mut HashMap<(u64, u64), Option<MerkleHash>>,
    ) -> Result<Option<MerkleHash>, Error> {
        if let Some(hash) = tree_nodes.get(&(index, level)) {
            Ok(*hash)
        } else {
            if level == 0 {
                let maybe_hash = if index >= tree_size {
                    None
                } else {
                    Some(self.chain_store().get_block_hash_from_ordinal(index)?)
                };
                tree_nodes.insert((index, level), maybe_hash);
                Ok(maybe_hash)
            } else {
                let cur_tree_size = (index + 1) * counter;
                let maybe_hash = if cur_tree_size > tree_size {
                    if index * counter <= tree_size {
                        let left_hash = self.get_merkle_tree_node(
                            index * 2,
                            level - 1,
                            counter / 2,
                            tree_size,
                            tree_nodes,
                        )?;
                        let right_hash = self.reconstruct_merkle_tree_node(
                            index * 2 + 1,
                            level - 1,
                            counter / 2,
                            tree_size,
                            tree_nodes,
                        )?;
                        Self::combine_maybe_hashes(left_hash, right_hash)
                    } else {
                        None
                    }
                } else {
                    Some(
                        *self
                            .chain_store()
                            .get_block_merkle_tree_from_ordinal(cur_tree_size)?
                            .get_path()
                            .last()
                            .ok_or_else(|| Error::Other("Merkle tree node missing".to_string()))?,
                    )
                };
                tree_nodes.insert((index, level), maybe_hash);
                Ok(maybe_hash)
            }
        }
    }

    /// Reconstruct node at given position (index, level). If the node does not exist, return `None`.
    fn reconstruct_merkle_tree_node(
        &self,
        index: u64,
        level: u64,
        counter: u64,
        tree_size: u64,
        tree_nodes: &mut HashMap<(u64, u64), Option<MerkleHash>>,
    ) -> Result<Option<MerkleHash>, Error> {
        if let Some(hash) = tree_nodes.get(&(index, level)) {
            Ok(*hash)
        } else {
            if level == 0 {
                let maybe_hash = if index >= tree_size {
                    None
                } else {
                    Some(self.chain_store().get_block_hash_from_ordinal(index)?)
                };
                tree_nodes.insert((index, level), maybe_hash);
                Ok(maybe_hash)
            } else {
                let left_hash = self.get_merkle_tree_node(
                    index * 2,
                    level - 1,
                    counter / 2,
                    tree_size,
                    tree_nodes,
                )?;
                let right_hash = self.reconstruct_merkle_tree_node(
                    index * 2 + 1,
                    level - 1,
                    counter / 2,
                    tree_size,
                    tree_nodes,
                )?;
                let maybe_hash = Self::combine_maybe_hashes(left_hash, right_hash);
                tree_nodes.insert((index, level), maybe_hash);

                Ok(maybe_hash)
            }
        }
    }

    /// Get merkle proof for block with hash `block_hash` in the merkle tree of `head_block_hash`.
    pub fn get_block_proof(
        &self,
        block_hash: &CryptoHash,
        head_block_hash: &CryptoHash,
    ) -> Result<MerklePath, Error> {
        let leaf_index = self.chain_store().get_block_merkle_tree(block_hash)?.size();
        let tree_size = self.chain_store().get_block_merkle_tree(head_block_hash)?.size();
        if leaf_index >= tree_size {
            if block_hash == head_block_hash {
                // special case if the block to prove is the same as head
                return Ok(vec![]);
            }
            return Err(Error::Other(format!(
                "block {} is ahead of head block {}",
                block_hash, head_block_hash
            )));
        }
        let mut level = 0;
        let mut counter = 1;
        let mut cur_index = leaf_index;
        let mut path = vec![];
        let mut tree_nodes = HashMap::new();
        let mut iter = tree_size;
        while iter > 1 {
            if cur_index % 2 == 0 {
                cur_index += 1
            } else {
                cur_index -= 1;
            }
            let direction = if cur_index % 2 == 0 { Direction::Left } else { Direction::Right };
            let maybe_hash = if cur_index % 2 == 1 {
                // node not immediately available. Needs to be reconstructed
                self.reconstruct_merkle_tree_node(
                    cur_index,
                    level,
                    counter,
                    tree_size,
                    &mut tree_nodes,
                )?
            } else {
                self.get_merkle_tree_node(cur_index, level, counter, tree_size, &mut tree_nodes)?
            };
            if let Some(hash) = maybe_hash {
                path.push(MerklePathItem { hash, direction });
            }
            cur_index /= 2;
            iter = (iter + 1) / 2;
            level += 1;
            counter *= 2;
        }
        Ok(path)
    }
}

/// Various chain getters.
impl Chain {
    /// Gets chain head.
    #[inline]
    pub fn head(&self) -> Result<Tip, Error> {
        self.chain_store.head()
    }

    /// Gets chain tail height
    #[inline]
    pub fn tail(&self) -> Result<BlockHeight, Error> {
        self.chain_store.tail()
    }

    /// Gets chain header head.
    #[inline]
    pub fn header_head(&self) -> Result<Tip, Error> {
        self.chain_store.header_head()
    }

    /// Header of the block at the head of the block chain (not the same thing as header_head).
    #[inline]
    pub fn head_header(&self) -> Result<BlockHeader, Error> {
        self.chain_store.head_header()
    }

    /// Get final head of the chain.
    #[inline]
    pub fn final_head(&self) -> Result<Tip, Error> {
        self.chain_store.final_head()
    }

    /// Gets a block by hash.
    #[inline]
    pub fn get_block(&self, hash: &CryptoHash) -> Result<Block, Error> {
        self.chain_store.get_block(hash)
    }

    /// Gets the block at chain head
    pub fn get_head_block(&self) -> Result<Block, Error> {
        let tip = self.head()?;
        self.chain_store.get_block(&tip.last_block_hash)
    }

    /// Gets a chunk from hash.
    #[inline]
    pub fn get_chunk(&self, chunk_hash: &ChunkHash) -> Result<Arc<ShardChunk>, Error> {
        self.chain_store.get_chunk(chunk_hash)
    }

    /// Gets a chunk from header.
    #[inline]
    pub fn get_chunk_clone_from_header(
        &self,
        header: &ShardChunkHeader,
    ) -> Result<ShardChunk, Error> {
        self.chain_store.get_chunk_clone_from_header(header)
    }

    /// Gets a block from the current chain by height.
    #[inline]
    pub fn get_block_by_height(&self, height: BlockHeight) -> Result<Block, Error> {
        let hash = self.chain_store.get_block_hash_by_height(height)?;
        self.chain_store.get_block(&hash)
    }

    /// Gets block hash from the current chain by height.
    #[inline]
    pub fn get_block_hash_by_height(&self, height: BlockHeight) -> Result<CryptoHash, Error> {
        self.chain_store.get_block_hash_by_height(height)
    }

    /// Gets a block header by hash.
    #[inline]
    pub fn get_block_header(&self, hash: &CryptoHash) -> Result<BlockHeader, Error> {
        self.chain_store.get_block_header(hash)
    }

    /// Returns block header from the canonical chain for given height if present.
    #[inline]
    pub fn get_block_header_by_height(&self, height: BlockHeight) -> Result<BlockHeader, Error> {
        self.chain_store.get_block_header_by_height(height)
    }

    /// Returns block header from the current chain defined by `sync_hash` for given height if present.
    #[inline]
    pub fn get_block_header_on_chain_by_height(
        &self,
        sync_hash: &CryptoHash,
        height: BlockHeight,
    ) -> Result<BlockHeader, Error> {
        self.chain_store.get_block_header_on_chain_by_height(sync_hash, height)
    }

    /// Get previous block header.
    #[inline]
    pub fn get_previous_header(&self, header: &BlockHeader) -> Result<BlockHeader, Error> {
        self.chain_store.get_previous_header(header).map_err(|e| match e {
            Error::DBNotFoundErr(_) => Error::Orphan,
            other => other,
        })
    }

    /// Returns hash of the first available block after genesis.
    pub fn get_earliest_block_hash(&self) -> Result<Option<CryptoHash>, Error> {
        self.chain_store.get_earliest_block_hash()
    }

    /// Check if block exists.
    #[inline]
    pub fn block_exists(&self, hash: &CryptoHash) -> Result<bool, Error> {
        self.chain_store.block_exists(hash)
    }

    /// Get block extra that was computer after applying previous block.
    #[inline]
    pub fn get_block_extra(&self, block_hash: &CryptoHash) -> Result<Arc<BlockExtra>, Error> {
        self.chain_store.get_block_extra(block_hash)
    }

    /// Get chunk extra that was computed after applying chunk with given hash.
    #[inline]
    pub fn get_chunk_extra(
        &self,
        block_hash: &CryptoHash,
        shard_uid: &ShardUId,
    ) -> Result<Arc<ChunkExtra>, Error> {
        self.chain_store.get_chunk_extra(block_hash, shard_uid)
    }

    /// Get next block hash for which there is a new chunk for the shard.
    /// If sharding changes before we can find a block with a new chunk for the shard,
    /// find the first block that contains a new chunk for any of the shards that split from the
    /// original shard
    pub fn get_next_block_hash_with_new_chunk(
        &self,
        block_hash: &CryptoHash,
        shard_id: ShardId,
    ) -> Result<Option<(CryptoHash, ShardId)>, Error> {
        let mut block_hash = *block_hash;
        let mut epoch_id = *self.get_block_header(&block_hash)?.epoch_id();
        let mut shard_layout = self.epoch_manager.get_shard_layout(&epoch_id)?;
        // this corrects all the shard where the original shard will split to if sharding changes
        let mut shard_ids = vec![shard_id];

        while let Ok(next_block_hash) = self.chain_store.get_next_block_hash(&block_hash) {
            let next_epoch_id = *self.get_block_header(&next_block_hash)?.epoch_id();
            if next_epoch_id != epoch_id {
                let next_shard_layout = self.epoch_manager.get_shard_layout(&next_epoch_id)?;
                if next_shard_layout != shard_layout {
                    shard_ids = shard_ids
                        .into_iter()
                        .flat_map(|id| {
                            next_shard_layout.get_children_shards_ids(id).unwrap_or_else(|| {
                                panic!("invalid shard layout {:?} because it does not contain children shards for parent shard {}", next_shard_layout, id)
                            })
                        })
                        .collect();

                    shard_layout = next_shard_layout;
                }
                epoch_id = next_epoch_id;
            }
            block_hash = next_block_hash;

            let block = self.get_block(&block_hash)?;
            let chunks = block.chunks();
            for &shard_id in shard_ids.iter() {
                let chunk_header = &chunks
                    .get(shard_id as usize)
                    .ok_or_else(|| Error::InvalidShardId(shard_id as ShardId))?;
                if chunk_header.height_included() == block.header().height() {
                    return Ok(Some((block_hash, shard_id)));
                }
            }
        }

        Ok(None)
    }

    /// Returns underlying ChainStore.
    #[inline]
    pub fn chain_store(&self) -> &ChainStore {
        &self.chain_store
    }

    /// Returns mutable ChainStore.
    #[inline]
    pub fn mut_chain_store(&mut self) -> &mut ChainStore {
        &mut self.chain_store
    }

    /// Returns genesis block.
    #[inline]
    pub fn genesis_block(&self) -> &Block {
        &self.genesis
    }

    /// Returns genesis block header.
    #[inline]
    pub fn genesis(&self) -> &BlockHeader {
        self.genesis.header()
    }

    /// Returns number of orphans currently in the orphan pool.
    #[inline]
    pub fn blocks_with_missing_chunks_len(&self) -> usize {
        self.blocks_with_missing_chunks.len()
    }

    #[inline]
    pub fn blocks_in_processing_len(&self) -> usize {
        self.blocks_in_processing.len()
    }

    /// Check if hash is for a known chunk orphan.
    #[inline]
    pub fn is_chunk_orphan(&self, hash: &CryptoHash) -> bool {
        self.blocks_with_missing_chunks.contains(hash)
    }

    /// Check if hash is for a block that is being processed
    #[inline]
    pub fn is_in_processing(&self, hash: &CryptoHash) -> bool {
        self.blocks_in_processing.contains(hash)
    }

    #[inline]
    pub fn is_height_processed(&self, height: BlockHeight) -> Result<bool, Error> {
        self.chain_store.is_height_processed(height)
    }

    #[inline]
    pub fn is_block_invalid(&self, hash: &CryptoHash) -> bool {
        self.invalid_blocks.contains(hash)
    }

    /// Check that sync_hash is the first block of an epoch.
    pub fn check_sync_hash_validity(&self, sync_hash: &CryptoHash) -> Result<bool, Error> {
        // It's important to check that Block exists because we will sync with it.
        // Do not replace with `get_block_header()`.
        let sync_block = self.get_block(sync_hash)?;
        let prev_hash = *sync_block.header().prev_hash();
        let is_first_block_of_epoch = self.epoch_manager.is_next_block_epoch_start(&prev_hash);
        tracing::debug!(
            target: "chain",
            ?sync_hash,
            ?prev_hash,
            sync_hash_epoch_id = ?sync_block.header().epoch_id(),
            sync_hash_next_epoch_id = ?sync_block.header().next_epoch_id(),
            ?is_first_block_of_epoch,
            "check_sync_hash_validity");

        // If sync_hash is not on the Epoch boundary, it's malicious behavior
        Ok(is_first_block_of_epoch?)
    }

    /// Get transaction result for given hash of transaction or receipt id
    /// Chain may not be canonical yet
    pub fn get_execution_outcome(
        &self,
        id: &CryptoHash,
    ) -> Result<ExecutionOutcomeWithIdAndProof, Error> {
        let outcomes = self.chain_store.get_outcomes_by_id(id)?;
        outcomes
            .into_iter()
            .find(|outcome| match self.get_block_header(&outcome.block_hash) {
                Ok(header) => self.is_on_current_chain(&header).unwrap_or(false),
                Err(_) => false,
            })
            .ok_or_else(|| Error::DBNotFoundErr(format!("EXECUTION OUTCOME: {}", id)))
    }

    /// Retrieve the up to `max_headers_returned` headers on the main chain
    /// `hashes`: a list of block "locators". `hashes` should be ordered from older blocks to
    ///           more recent blocks. This function will find the first block in `hashes`
    ///           that is on the main chain and returns the blocks after this block. If none of the
    ///           blocks in `hashes` are on the main chain, the function returns an empty vector.
    pub fn retrieve_headers(
        &self,
        hashes: Vec<CryptoHash>,
        max_headers_returned: u64,
        max_height: Option<BlockHeight>,
    ) -> Result<Vec<BlockHeader>, Error> {
        let header = match self.find_common_header(&hashes) {
            Some(header) => header,
            None => return Ok(vec![]),
        };

        let mut headers = vec![];
        let header_head_height = self.header_head()?.height;
        let max_height = max_height.unwrap_or(header_head_height);
        // TODO: this may be inefficient if there are a lot of skipped blocks.
        for h in header.height() + 1..=max_height {
            if let Ok(header) = self.get_block_header_by_height(h) {
                headers.push(header.clone());
                if headers.len() >= max_headers_returned as usize {
                    break;
                }
            }
        }
        Ok(headers)
    }

    /// Returns a vector of chunk headers, each of which corresponds to the chunk in the `prev_block`
    /// This function is important when the block after `prev_block` has different number of chunks
    /// from `prev_block` in cases of resharding.
    /// In block production and processing, often we need to get the previous chunks of chunks
    /// in the current block, this function provides a way to do so while handling sharding changes
    /// correctly.
    /// For example, if `prev_block` has two shards 0, 1 and the block after `prev_block` will have
    /// 4 shards 0, 1, 2, 3, 0 and 1 split from shard 0 and 2 and 3 split from shard 1.
    /// `get_prev_chunk_headers(epoch_manager, prev_block)` will return
    /// `[prev_block.chunks()[0], prev_block.chunks()[0], prev_block.chunks()[1], prev_block.chunks()[1]]`
    pub fn get_prev_chunk_headers(
        epoch_manager: &dyn EpochManagerAdapter,
        prev_block: &Block,
    ) -> Result<Vec<ShardChunkHeader>, Error> {
        let epoch_id = epoch_manager.get_epoch_id_from_prev_block(prev_block.hash())?;
        let shard_ids = epoch_manager.shard_ids(&epoch_id)?;
        let prev_shard_ids = epoch_manager.get_prev_shard_ids(prev_block.hash(), shard_ids)?;
        let chunks = prev_block.chunks();
        Ok(prev_shard_ids
            .into_iter()
            .map(|shard_id| chunks.get(shard_id as usize).unwrap().clone())
            .collect())
    }

    pub fn get_prev_chunk_header(
        epoch_manager: &dyn EpochManagerAdapter,
        prev_block: &Block,
        shard_id: ShardId,
    ) -> Result<ShardChunkHeader, Error> {
        let prev_shard_id = epoch_manager.get_prev_shard_id(prev_block.hash(), shard_id)?;
        Ok(prev_block
            .chunks()
            .get(prev_shard_id as usize)
            .ok_or(Error::InvalidShardId(shard_id))?
            .clone())
    }

    pub fn group_receipts_by_shard(
        receipts: Vec<Receipt>,
        shard_layout: &ShardLayout,
    ) -> HashMap<ShardId, Vec<Receipt>> {
        let mut result = HashMap::new();
        for receipt in receipts {
            let shard_id = account_id_to_shard_id(receipt.receiver_id(), shard_layout);
            let entry = result.entry(shard_id).or_insert_with(Vec::new);
            entry.push(receipt)
        }
        result
    }

    pub fn build_receipts_hashes(
        receipts: &[Receipt],
        shard_layout: &ShardLayout,
    ) -> Vec<CryptoHash> {
        // Using a BTreeMap instead of HashMap to enable in order iteration
        // below.
        //
        // Pre-populating because even if there are no receipts for a shard, we
        // need an empty vector for it.
        let mut result: BTreeMap<_, _> =
            shard_layout.shard_ids().map(|shard_id| (shard_id, vec![])).collect();
        let mut cache = HashMap::new();
        for receipt in receipts {
            let &mut shard_id = cache
                .entry(receipt.receiver_id())
                .or_insert_with(|| account_id_to_shard_id(receipt.receiver_id(), shard_layout));
            // This unwrap should be safe as we pre-populated the map with all
            // valid shard ids.
            result.get_mut(&shard_id).unwrap().push(receipt);
        }
        result
            .into_iter()
            .map(|(shard_id, receipts)| {
                let bytes = borsh::to_vec(&(shard_id, receipts)).unwrap();
                hash(&bytes)
            })
            .collect()
    }
}

/// Sandbox node specific operations
impl Chain {
    // NB: `SandboxStatePatch` can only be created in `#[cfg(feature =
    // "sandbox")]`, so we don't need extra cfg-gating here.
    pub fn patch_state(&mut self, patch: SandboxStatePatch) {
        self.pending_state_patch.merge(patch);
    }

    pub fn patch_state_in_progress(&self) -> bool {
        !self.pending_state_patch.is_empty()
    }
}

/// Epoch sync specific functions.
#[cfg(feature = "new_epoch_sync")]
impl Chain {
    /// TODO(posvyatokum): validate `epoch_sync_info` before `store_update` commit.
    pub fn validate_and_record_epoch_sync_info(
        &mut self,
        epoch_sync_info: &EpochSyncInfo,
    ) -> Result<(), EpochSyncInfoError> {
        let store = self.chain_store().store().clone();
        let epoch_manager = self.epoch_manager.clone();
        let mut chain_store_update = self.chain_store.store_update();
        let mut store_update = store.store_update();

        let epoch_id = epoch_sync_info.get_epoch_id()?;
        // save EpochSyncInfo

        store_update.set_ser(DBCol::EpochSyncInfo, epoch_id.as_ref(), epoch_sync_info)?;

        // save EpochInfo's

        store_update.set_ser(DBCol::EpochInfo, epoch_id.as_ref(), &epoch_sync_info.epoch_info)?;
        store_update.set_ser(
            DBCol::EpochInfo,
            epoch_sync_info.get_next_epoch_id()?.as_ref(),
            &epoch_sync_info.next_epoch_info,
        )?;
        store_update.set_ser(
            DBCol::EpochInfo,
            epoch_sync_info.get_next_next_epoch_id()?.as_ref(),
            &epoch_sync_info.next_next_epoch_info,
        )?;

        // construct and save all new BlockMerkleTree's

        let mut cur_block_merkle_tree = (*chain_store_update
            .get_block_merkle_tree(epoch_sync_info.get_epoch_first_header()?.prev_hash())?)
        .clone();
        let mut prev_hash = epoch_sync_info.get_epoch_first_header()?.prev_hash();
        for hash in &epoch_sync_info.all_block_hashes {
            cur_block_merkle_tree.insert(*prev_hash);
            chain_store_update.save_block_merkle_tree(*hash, cur_block_merkle_tree.clone());
            prev_hash = hash;
        }

        // save all block data in headers_to_save

        for hash in &epoch_sync_info.headers_to_save {
            let header = epoch_sync_info.get_header(*hash, EpochSyncHashType::BlockToSave)?;
            // check that block is not known already
            if store.exists(DBCol::BlockHeader, hash.as_ref())? {
                continue;
            }

            store_update.insert_ser(DBCol::BlockHeader, header.hash().as_ref(), header)?;
            store_update.set_ser(
                DBCol::NextBlockHashes,
                header.prev_hash().as_ref(),
                header.hash(),
            )?;
            store_update.set_ser(
                DBCol::BlockHeight,
                &index_to_bytes(header.height()),
                header.hash(),
            )?;
            store_update.set_ser(
                DBCol::BlockOrdinal,
                &index_to_bytes(header.block_ordinal()),
                &header.hash(),
            )?;

            store_update.insert_ser(
                DBCol::BlockInfo,
                hash.as_ref(),
                &epoch_sync_info.get_block_info(hash)?,
            )?;
        }

        // save header head, final head, update epoch_manager aggregator
        chain_store_update
            .force_save_header_head(&Tip::from_header(epoch_sync_info.get_epoch_last_header()?))?;
        chain_store_update.save_final_head(&Tip::from_header(
            epoch_sync_info.get_epoch_last_finalised_header()?,
        ))?;
        epoch_manager
            .force_update_aggregator(epoch_id, epoch_sync_info.get_epoch_last_finalised_hash()?);

        // TODO(posvyatokum): add EpochSyncInfo validation.

        chain_store_update.merge(store_update);
        chain_store_update.commit()?;
        Ok(())
    }
}

pub fn do_apply_chunks(
    block_hash: CryptoHash,
    block_height: BlockHeight,
    work: Vec<UpdateShardJob>,
) -> Vec<(ShardId, Result<ShardUpdateResult, Error>)> {
    let parent_span =
        tracing::debug_span!(target: "chain", "do_apply_chunks", block_height, %block_hash)
            .entered();
    work.into_par_iter()
        .map(|(shard_id, task)| {
            // As chunks can be processed in parallel, make sure they are all tracked as children of
            // a single span.
            (shard_id, task(&parent_span))
        })
        .collect()
}

pub fn collect_receipts<'a, T>(receipt_proofs: T) -> Vec<Receipt>
where
    T: IntoIterator<Item = &'a ReceiptProof>,
{
    receipt_proofs.into_iter().flat_map(|ReceiptProof(receipts, _)| receipts).cloned().collect()
}

pub fn collect_receipts_from_response(
    receipt_proof_response: &[ReceiptProofResponse],
) -> Vec<Receipt> {
    collect_receipts(
        receipt_proof_response.iter().flat_map(|ReceiptProofResponse(_, proofs)| proofs.iter()),
    )
}

#[derive(actix::Message)]
#[rtype(result = "()")]
pub struct ApplyStatePartsRequest {
    pub runtime_adapter: Arc<dyn RuntimeAdapter>,
    pub shard_uid: ShardUId,
    pub state_root: StateRoot,
    pub num_parts: u64,
    pub epoch_id: EpochId,
    pub sync_hash: CryptoHash,
}

// Skip `runtime_adapter`, because it's a complex object that has complex logic
// and many fields.
impl Debug for ApplyStatePartsRequest {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ApplyStatePartsRequest")
            .field("runtime_adapter", &"<not shown>")
            .field("shard_uid", &self.shard_uid)
            .field("state_root", &self.state_root)
            .field("num_parts", &self.num_parts)
            .field("epoch_id", &self.epoch_id)
            .field("sync_hash", &self.sync_hash)
            .finish()
    }
}

#[derive(actix::Message, Debug)]
#[rtype(result = "()")]
pub struct ApplyStatePartsResponse {
    pub apply_result: Result<(), near_chain_primitives::error::Error>,
    pub shard_id: ShardId,
    pub sync_hash: CryptoHash,
}

// This message is handled by `sync_job_actions.rs::handle_load_memtrie_request()`.
// It is a request for `runtime_adapter` to load in-memory trie for `shard_uid`.
#[derive(actix::Message)]
#[rtype(result = "()")]
pub struct LoadMemtrieRequest {
    pub runtime_adapter: Arc<dyn RuntimeAdapter>,
    pub shard_uid: ShardUId,
    // Required to load memtrie.
    pub prev_state_root: StateRoot,
    // Needs to be included in a response to the caller for identification purposes.
    pub sync_hash: CryptoHash,
}

// Skip `runtime_adapter`, because it's a complex object that has complex logic
// and many fields.
impl Debug for LoadMemtrieRequest {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LoadMemtrieRequest")
            .field("runtime_adapter", &"<not shown>")
            .field("shard_uid", &self.shard_uid)
            .field("prev_state_root", &self.prev_state_root)
            .field("sync_hash", &self.sync_hash)
            .finish()
    }
}

// It is message indicating the result of loading in-memory trie for `shard_id`.
// `sync_hash` is passed around to indicate to which block we were catching up.
#[derive(actix::Message, Debug)]
#[rtype(result = "()")]
pub struct LoadMemtrieResponse {
    pub load_result: Result<(), near_chain_primitives::error::Error>,
    pub shard_uid: ShardUId,
    pub sync_hash: CryptoHash,
}

#[derive(actix::Message)]
#[rtype(result = "()")]
pub struct BlockCatchUpRequest {
    pub sync_hash: CryptoHash,
    pub block_hash: CryptoHash,
    pub block_height: BlockHeight,
    pub work: Vec<UpdateShardJob>,
}

// Skip `work`, because displaying functions is not possible.
impl Debug for BlockCatchUpRequest {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlockCatchUpRequest")
            .field("sync_hash", &self.sync_hash)
            .field("block_hash", &self.block_hash)
            .field("block_height", &self.block_height)
            .field("work", &format!("<vector of length {}>", self.work.len()))
            .finish()
    }
}

#[derive(actix::Message, Debug)]
#[rtype(result = "()")]
pub struct BlockCatchUpResponse {
    pub sync_hash: CryptoHash,
    pub block_hash: CryptoHash,
    pub results: Vec<(ShardId, Result<ShardUpdateResult, Error>)>,
}

#[derive(actix::Message, Debug, Clone, PartialEq, Eq)]
#[rtype(result = "()")]
pub struct ChunkStateWitnessMessage {
    pub witness: ChunkStateWitness,
    pub raw_witness_size: ChunkStateWitnessSize,
}

/// Helper to track blocks catch up
/// Lifetime of a block_hash is as follows:
/// 1. It is added to pending blocks, either as first block of an epoch or because we (post)
///     processed previous block
/// 2. Block is preprocessed and scheduled for processing in sync jobs actor. Block hash
///     and state changes from preprocessing goes to scheduled blocks
/// 3. We've got response from sync jobs actor that block was processed. Block hash, state
///     changes from preprocessing and result of processing block are moved to processed blocks
/// 4. Results are postprocessed. If there is any error block goes back to pending to try again.
///     Otherwise results are committed, block is moved to done blocks and any blocks that
///     have this block as previous are added to pending
pub struct BlocksCatchUpState {
    /// Hash of first block of an epoch
    pub first_block_hash: CryptoHash,
    /// Epoch id
    pub epoch_id: EpochId,
    /// Collection of block hashes that are yet to be sent for processed
    pub pending_blocks: Vec<CryptoHash>,
    /// Map from block hashes that are scheduled for processing to saved store updates from their
    /// preprocessing
    pub scheduled_blocks: HashSet<CryptoHash>,
    /// Map from block hashes that were processed to (saved store update, process results)
    pub processed_blocks: HashMap<CryptoHash, Vec<Result<ShardUpdateResult, Error>>>,
    /// Collection of block hashes that are fully processed
    pub done_blocks: Vec<CryptoHash>,
}

impl BlocksCatchUpState {
    pub fn new(first_block_hash: CryptoHash, epoch_id: EpochId) -> Self {
        Self {
            first_block_hash,
            epoch_id,
            pending_blocks: vec![first_block_hash],
            scheduled_blocks: HashSet::new(),
            processed_blocks: HashMap::new(),
            done_blocks: vec![],
        }
    }

    pub fn is_finished(&self) -> bool {
        self.pending_blocks.is_empty()
            && self.scheduled_blocks.is_empty()
            && self.processed_blocks.is_empty()
    }
}

impl Chain {
    // Get status for debug page
    pub fn get_block_catchup_status(
        &self,
        block_catchup_state: &BlocksCatchUpState,
    ) -> Vec<BlockStatusView> {
        block_catchup_state
            .pending_blocks
            .iter()
            .chain(block_catchup_state.scheduled_blocks.iter())
            .chain(block_catchup_state.processed_blocks.keys())
            .map(|block_hash| BlockStatusView {
                height: self
                    .get_block_header(block_hash)
                    .map(|header| header.height())
                    .unwrap_or_default(),
                hash: *block_hash,
            })
            .collect()
    }
}
