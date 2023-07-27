use crate::block_processing_utils::{
    BlockPreprocessInfo, BlockProcessingArtifact, BlocksInProcessing, DoneApplyChunkCallback,
};
use crate::blocks_delay_tracker::BlocksDelayTracker;
use crate::crypto_hash_timer::CryptoHashTimer;
use crate::lightclient::get_epoch_block_producers_view;
use crate::migrations::check_if_block_is_first_with_chunk_of_version;
use crate::missing_chunks::{BlockLike, MissingChunksPool};
use crate::state_request_tracker::StateRequestTracker;
use crate::state_snapshot_actor::MakeSnapshotCallback;
use crate::store::{ChainStore, ChainStoreAccess, ChainStoreUpdate, GCMode};
use crate::types::{
    AcceptedBlock, ApplySplitStateResult, ApplySplitStateResultOrStateChanges,
    ApplyTransactionResult, Block, BlockEconomicsConfig, BlockHeader, BlockStatus, ChainConfig,
    ChainGenesis, Provenance, RuntimeAdapter,
};
use crate::validate::{
    validate_challenge, validate_chunk_proofs, validate_chunk_with_chunk_extra,
    validate_transactions_order,
};
use crate::{byzantine_assert, create_light_client_block_view, Doomslug};
use crate::{metrics, DoomslugThresholdMode};
use borsh::BorshSerialize;
use chrono::Duration;
use crossbeam_channel::{unbounded, Receiver, Sender};
use delay_detector::DelayDetector;
use itertools::Itertools;
use lru::LruCache;
use near_chain_primitives::error::{BlockKnownError, Error, LogTransientStorageError};
use near_epoch_manager::shard_tracker::ShardTracker;
use near_epoch_manager::types::BlockHeaderInfo;
use near_epoch_manager::EpochManagerAdapter;
use near_o11y::log_assert;
use near_primitives::block::{genesis_chunks, BlockValidityError, Tip};
use near_primitives::challenge::{
    BlockDoubleSign, Challenge, ChallengeBody, ChallengesResult, ChunkProofs, ChunkState,
    MaybeEncodedShardChunk, PartialState, SlashedValidator,
};
use near_primitives::checked_feature;
use near_primitives::errors::EpochError;
use near_primitives::hash::{hash, CryptoHash};
use near_primitives::merkle::{
    combine_hash, merklize, verify_path, Direction, MerklePath, MerklePathItem, PartialMerkleTree,
};
use near_primitives::receipt::Receipt;
use near_primitives::sandbox::state_patch::SandboxStatePatch;
use near_primitives::shard_layout::{
    account_id_to_shard_id, account_id_to_shard_uid, ShardLayout, ShardUId,
};
use near_primitives::sharding::{
    ChunkHash, ChunkHashHeight, EncodedShardChunk, ReceiptList, ReceiptProof, ShardChunk,
    ShardChunkHeader, ShardInfo, ShardProof, StateSyncInfo,
};
use near_primitives::state_part::PartId;
use near_primitives::static_clock::StaticClock;
use near_primitives::syncing::{
    get_num_state_parts, ReceiptProofResponse, RootProof, ShardStateSyncResponseHeader,
    ShardStateSyncResponseHeaderV1, ShardStateSyncResponseHeaderV2, StateHeaderKey, StatePartKey,
};
use near_primitives::transaction::{ExecutionOutcomeWithIdAndProof, SignedTransaction};
use near_primitives::types::chunk_extra::ChunkExtra;
use near_primitives::types::{
    AccountId, Balance, BlockExtra, BlockHeight, BlockHeightDelta, EpochId, Gas, MerkleHash,
    NumBlocks, NumShards, ShardId, StateChangesForSplitStates, StateRoot,
};
use near_primitives::unwrap_or_return;
use near_primitives::utils::MaybeValidated;
use near_primitives::version::PROTOCOL_VERSION;
use near_primitives::views::{
    BlockStatusView, DroppedReason, ExecutionOutcomeWithIdView, ExecutionStatusView,
    FinalExecutionOutcomeView, FinalExecutionOutcomeWithReceiptView, FinalExecutionStatus,
    LightClientBlockView, SignedTransactionView,
};
use near_store::flat::{
    store_helper, FlatStateChanges, FlatStateDelta, FlatStateDeltaMetadata, FlatStorageError,
    FlatStorageReadyStatus, FlatStorageStatus,
};
use near_store::{get_genesis_state_roots, StorageError};
use near_store::{DBCol, ShardTries, WrappedTrieChanges};
use once_cell::sync::OnceCell;
use rand::seq::SliceRandom;
use rand::SeedableRng;
use rand_chacha::ChaCha20Rng;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration as TimeDuration, Instant};
use tracing::{debug, error, info, warn, Span};

/// Maximum number of orphans chain can store.
pub const MAX_ORPHAN_SIZE: usize = 1024;

/// Maximum age of orphan to store in the chain.
const MAX_ORPHAN_AGE_SECS: u64 = 300;

// Number of orphan ancestors should be checked to request chunks
// Orphans for which we will request for missing chunks must satisfy,
// its NUM_ORPHAN_ANCESTORS_CHECK'th ancestor has been accepted
pub const NUM_ORPHAN_ANCESTORS_CHECK: u64 = 3;

/// The size of the invalid_blocks in-memory pool
pub const INVALID_CHUNKS_POOL_SIZE: usize = 5000;

// Maximum number of orphans that we can request missing chunks
// Note that if there are no forks, the maximum number of orphans we would
// request missing chunks will not exceed NUM_ORPHAN_ANCESTORS_CHECK,
// this number only adds another restriction when there are multiple forks.
// It should almost never be hit
const MAX_ORPHAN_MISSING_CHUNKS: usize = 5;

/// 10000 years in seconds. Big constant for sandbox to allow time traveling.
#[cfg(feature = "sandbox")]
const ACCEPTABLE_TIME_DIFFERENCE: i64 = 60 * 60 * 24 * 365 * 10000;

// Number of parent blocks traversed to check if the block can be finalized.
const NUM_PARENTS_TO_CHECK_FINALITY: usize = 20;

/// Refuse blocks more than this many block intervals in the future (as in bitcoin).
#[cfg(not(feature = "sandbox"))]
const ACCEPTABLE_TIME_DIFFERENCE: i64 = 12 * 10;

/// Over this block height delta in advance if we are not chunk producer - route tx to upcoming validators.
pub const TX_ROUTING_HEIGHT_HORIZON: BlockHeightDelta = 4;

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

/// Orphan is a block whose previous block is not accepted (in store) yet.
/// Therefore, they are not ready to be processed yet.
/// We save these blocks in an in-memory orphan pool to be processed later
/// after their previous block is accepted.
pub struct Orphan {
    block: MaybeValidated<Block>,
    provenance: Provenance,
    added: Instant,
}

impl BlockLike for Orphan {
    fn hash(&self) -> CryptoHash {
        *self.block.hash()
    }

    fn height(&self) -> u64 {
        self.block.header().height()
    }
}

impl Orphan {
    fn prev_hash(&self) -> &CryptoHash {
        self.block.header().prev_hash()
    }
}

/// OrphanBlockPool stores information of all orphans that are waiting to be processed
/// A block is added to the orphan pool when process_block failed because the block is an orphan
/// A block is removed from the pool if
/// 1) it is ready to be processed
/// or
/// 2) size of the pool exceeds MAX_ORPHAN_SIZE and the orphan was added a long time ago
///    or the height is high
pub struct OrphanBlockPool {
    /// A map from block hash to a orphan block
    orphans: HashMap<CryptoHash, Orphan>,
    /// A set that contains all orphans for which we have requested missing chunks for them
    /// An orphan can be added to this set when it was first added to the pool, or later
    /// when certain requirements are satisfied (see check_orphans)
    /// It can only be removed from this set when the orphan is removed from the pool
    orphans_requested_missing_chunks: HashSet<CryptoHash>,
    /// A map from block heights to orphan blocks at the height
    /// It's used to evict orphans when the pool is saturated
    height_idx: HashMap<BlockHeight, Vec<CryptoHash>>,
    /// A map from block hashes to orphan blocks whose prev block is the block
    /// It's used to check which orphan blocks are ready to be processed when a block is accepted
    prev_hash_idx: HashMap<CryptoHash, Vec<CryptoHash>>,
    /// number of orphans that were evicted
    evicted: usize,
}

impl OrphanBlockPool {
    pub fn new() -> OrphanBlockPool {
        OrphanBlockPool {
            orphans: HashMap::default(),
            orphans_requested_missing_chunks: HashSet::default(),
            height_idx: HashMap::default(),
            prev_hash_idx: HashMap::default(),
            evicted: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.orphans.len()
    }

    fn len_evicted(&self) -> usize {
        self.evicted
    }

    /// Add a block to the orphan pool
    /// `requested_missing_chunks`: whether missing chunks has been requested for the orphan
    fn add(&mut self, orphan: Orphan, requested_missing_chunks: bool) {
        let block_hash = *orphan.block.hash();
        let height_hashes = self.height_idx.entry(orphan.block.header().height()).or_default();
        height_hashes.push(*orphan.block.hash());
        let prev_hash_entries =
            self.prev_hash_idx.entry(*orphan.block.header().prev_hash()).or_default();
        prev_hash_entries.push(block_hash);
        self.orphans.insert(block_hash, orphan);
        if requested_missing_chunks {
            self.orphans_requested_missing_chunks.insert(block_hash);
        }

        if self.orphans.len() > MAX_ORPHAN_SIZE {
            let old_len = self.orphans.len();

            let mut removed_hashes: HashSet<CryptoHash> = HashSet::default();
            self.orphans.retain(|_, ref mut x| {
                let keep = x.added.elapsed() < TimeDuration::from_secs(MAX_ORPHAN_AGE_SECS);
                if !keep {
                    removed_hashes.insert(*x.block.hash());
                }
                keep
            });
            let mut heights = self.height_idx.keys().cloned().collect::<Vec<u64>>();
            heights.sort_unstable();
            for h in heights.iter().rev() {
                if let Some(hash) = self.height_idx.remove(h) {
                    for h in hash {
                        let _ = self.orphans.remove(&h);
                        removed_hashes.insert(h);
                    }
                }
                if self.orphans.len() < MAX_ORPHAN_SIZE {
                    break;
                }
            }
            self.height_idx.retain(|_, ref mut xs| xs.iter().any(|x| !removed_hashes.contains(x)));
            self.prev_hash_idx
                .retain(|_, ref mut xs| xs.iter().any(|x| !removed_hashes.contains(x)));
            self.orphans_requested_missing_chunks.retain(|x| !removed_hashes.contains(x));

            self.evicted += old_len - self.orphans.len();
        }
        metrics::NUM_ORPHANS.set(self.orphans.len() as i64);
    }

    pub fn contains(&self, hash: &CryptoHash) -> bool {
        self.orphans.contains_key(hash)
    }

    pub fn get(&self, hash: &CryptoHash) -> Option<&Orphan> {
        self.orphans.get(hash)
    }

    // Iterates over existing orphans.
    pub fn map(&self, orphan_fn: &mut dyn FnMut(&CryptoHash, &Block, &Instant)) {
        self.orphans
            .iter()
            .map(|it| orphan_fn(it.0, it.1.block.get_inner(), &it.1.added))
            .collect_vec();
    }

    /// Remove all orphans in the pool that can be "adopted" by block `prev_hash`, i.e., children
    /// of `prev_hash` and return the list.
    /// This function is called when `prev_hash` is accepted, thus its children can be removed
    /// from the orphan pool and be processed.
    pub fn remove_by_prev_hash(&mut self, prev_hash: CryptoHash) -> Option<Vec<Orphan>> {
        let mut removed_hashes: HashSet<CryptoHash> = HashSet::default();
        let ret = self.prev_hash_idx.remove(&prev_hash).map(|hs| {
            hs.iter()
                .filter_map(|h| {
                    removed_hashes.insert(*h);
                    self.orphans_requested_missing_chunks.remove(h);
                    self.orphans.remove(h)
                })
                .collect()
        });

        self.height_idx.retain(|_, ref mut xs| xs.iter().any(|x| !removed_hashes.contains(x)));

        metrics::NUM_ORPHANS.set(self.orphans.len() as i64);
        ret
    }

    /// Return a list of orphans that are among the `target_depth` immediate descendants of
    /// the block `parent_hash`
    pub fn get_orphans_within_depth(
        &self,
        parent_hash: CryptoHash,
        target_depth: u64,
    ) -> Vec<CryptoHash> {
        let mut _visited = HashSet::new();

        let mut res = vec![];
        let mut queue = vec![(parent_hash, 0)];
        while let Some((prev_hash, depth)) = queue.pop() {
            if depth == target_depth {
                break;
            }
            if let Some(block_hashes) = self.prev_hash_idx.get(&prev_hash) {
                for hash in block_hashes {
                    queue.push((*hash, depth + 1));
                    res.push(*hash);
                    // there should be no loop
                    debug_assert!(_visited.insert(*hash));
                }
            }

            // probably something serious went wrong here because there shouldn't be so many forks
            assert!(
                res.len() <= 100 * target_depth as usize,
                "found too many orphans {:?}, probably something is wrong with the chain",
                res
            );
        }
        res
    }

    /// Returns true if the block has not been requested yet and the number of orphans
    /// for which we have requested missing chunks have not exceeded MAX_ORPHAN_MISSING_CHUNKS
    fn can_request_missing_chunks_for_orphan(&self, block_hash: &CryptoHash) -> bool {
        self.orphans_requested_missing_chunks.len() < MAX_ORPHAN_MISSING_CHUNKS
            && !self.orphans_requested_missing_chunks.contains(block_hash)
    }

    fn mark_missing_chunks_requested_for_orphan(&mut self, block_hash: CryptoHash) {
        self.orphans_requested_missing_chunks.insert(block_hash);
    }
}

/// Contains information for missing chunks in a block
pub struct BlockMissingChunks {
    /// previous block hash
    pub prev_hash: CryptoHash,
    pub missing_chunks: Vec<ShardChunkHeader>,
}

/// Contains information needed to request chunks for orphans
/// Fields will be used as arguments for `request_chunks_for_orphan`
pub struct OrphanMissingChunks {
    pub missing_chunks: Vec<ShardChunkHeader>,
    /// epoch id for the block that has missing chunks
    pub epoch_id: EpochId,
    /// hash of an ancestor block of the block that has missing chunks
    /// this is used as an argument for `request_chunks_for_orphan`
    /// see comments in `request_chunks_for_orphan` for what `ancestor_hash` is used for
    pub ancestor_hash: CryptoHash,
}

/// Check if block header is known
/// Returns Err(Error) if any error occurs when checking store
///         Ok(Err(BlockKnownError)) if the block header is known
///         Ok(Ok()) otherwise
pub fn check_header_known(
    chain: &Chain,
    header: &BlockHeader,
) -> Result<Result<(), BlockKnownError>, Error> {
    let header_head = chain.store().header_head()?;
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
    if chain.store().block_exists(block_hash)? {
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
    let head = chain.store().head()?;
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

type BlockApplyChunksResult = (CryptoHash, Vec<Result<ApplyChunkResult, Error>>);

/// Facade to the blockchain block processing and storage.
/// Provides current view on the state according to the chain state.
pub struct Chain {
    store: ChainStore,
    pub epoch_manager: Arc<dyn EpochManagerAdapter>,
    pub shard_tracker: ShardTracker,
    pub runtime_adapter: Arc<dyn RuntimeAdapter>,
    orphans: OrphanBlockPool,
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

    /// Lets trigger new state snapshots.
    state_snapshot_helper: Option<StateSnapshotHelper>,
}

/// Lets trigger new state snapshots.
struct StateSnapshotHelper {
    /// A callback to initiate state snapshot.
    make_snapshot_callback: MakeSnapshotCallback,

    /// Test-only. Artificially triggers state snapshots every N blocks.
    /// The value is (countdown, N).
    test_snapshot_countdown_and_frequency: Option<(u64, u64)>,
}

impl Drop for Chain {
    fn drop(&mut self) {
        let _ = self.blocks_in_processing.wait_for_all_blocks();
    }
}

/// PreprocessBlockResult is a tuple where
/// the first element is a vector of jobs to apply chunks
/// the second element is BlockPreprocessInfo
type PreprocessBlockResult = (
    Vec<Box<dyn FnOnce(&Span) -> Result<ApplyChunkResult, Error> + Send + 'static>>,
    BlockPreprocessInfo,
);

// Used only for verify_block_hash_and_signature. See that method.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum VerifyBlockHashAndSignatureResult {
    Correct,
    Incorrect,
    CannotVerifyBecauseBlockIsOrphan,
}

impl Chain {
    pub fn make_genesis_block(
        epoch_manager: &dyn EpochManagerAdapter,
        runtime_adapter: &dyn RuntimeAdapter,
        chain_genesis: &ChainGenesis,
    ) -> Result<Block, Error> {
        let state_roots = get_genesis_state_roots(runtime_adapter.store())?
            .expect("genesis should be initialized.");
        let genesis_chunks = genesis_chunks(
            state_roots,
            epoch_manager.num_shards(&EpochId::default())?,
            chain_genesis.gas_limit,
            chain_genesis.height,
            chain_genesis.protocol_version,
        );
        Ok(Block::genesis(
            chain_genesis.protocol_version,
            genesis_chunks.into_iter().map(|chunk| chunk.take_header()).collect(),
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
        ))
    }

    pub fn new_for_view_client(
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        shard_tracker: ShardTracker,
        runtime_adapter: Arc<dyn RuntimeAdapter>,
        chain_genesis: &ChainGenesis,
        doomslug_threshold_mode: DoomslugThresholdMode,
        save_trie_changes: bool,
    ) -> Result<Chain, Error> {
        let store = runtime_adapter.store();
        let store = ChainStore::new(store.clone(), chain_genesis.height, save_trie_changes);
        let genesis = Self::make_genesis_block(
            epoch_manager.as_ref(),
            runtime_adapter.as_ref(),
            chain_genesis,
        )?;
        let (sc, rc) = unbounded();
        Ok(Chain {
            store,
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
            blocks_delay_tracker: BlocksDelayTracker::default(),
            apply_chunks_sender: sc,
            apply_chunks_receiver: rc,
            last_time_head_updated: StaticClock::instant(),
            invalid_blocks: LruCache::new(INVALID_CHUNKS_POOL_SIZE),
            pending_state_patch: Default::default(),
            requested_state_parts: StateRequestTracker::new(),
            state_snapshot_helper: None,
        })
    }

    pub fn new(
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        shard_tracker: ShardTracker,
        runtime_adapter: Arc<dyn RuntimeAdapter>,
        chain_genesis: &ChainGenesis,
        doomslug_threshold_mode: DoomslugThresholdMode,
        chain_config: ChainConfig,
        make_snapshot_callback: Option<MakeSnapshotCallback>,
    ) -> Result<Chain, Error> {
        // Get runtime initial state and create genesis block out of it.
        let state_roots = get_genesis_state_roots(runtime_adapter.store())?
            .expect("genesis should be initialized.");
        let mut store = ChainStore::new(
            runtime_adapter.store().clone(),
            chain_genesis.height,
            chain_config.save_trie_changes,
        );
        let genesis_chunks = genesis_chunks(
            state_roots.clone(),
            epoch_manager.num_shards(&EpochId::default())?,
            chain_genesis.gas_limit,
            chain_genesis.height,
            chain_genesis.protocol_version,
        );
        let genesis = Block::genesis(
            chain_genesis.protocol_version,
            genesis_chunks.iter().map(|chunk| chunk.cloned_header()).collect(),
            chain_genesis.time,
            chain_genesis.height,
            chain_genesis.min_gas_price,
            chain_genesis.total_supply,
            Chain::compute_bp_hash(
                epoch_manager.as_ref(),
                EpochId::default(),
                EpochId::default(),
                &CryptoHash::default(),
            )?,
        );

        // Check if we have a head in the store, otherwise pick genesis block.
        let mut store_update = store.store_update();
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
                    store_update.save_chunk_extra(
                        genesis.hash(),
                        &epoch_manager
                            .shard_id_to_uid(chunk_header.shard_id(), &EpochId::default())?,
                        ChunkExtra::new(
                            state_root,
                            CryptoHash::default(),
                            vec![],
                            0,
                            chain_genesis.gas_limit,
                            0,
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
                if let Some(flat_storage_manager) = runtime_adapter.get_flat_storage_manager() {
                    let genesis_epoch_id = genesis.header().epoch_id();
                    let mut tmp_store_update = store_update.store().store_update();
                    for shard_uid in
                        epoch_manager.get_shard_layout(genesis_epoch_id)?.get_shard_uids()
                    {
                        flat_storage_manager.set_flat_storage_for_genesis(
                            &mut tmp_store_update,
                            shard_uid,
                            genesis.hash(),
                            genesis.header().height(),
                        )
                    }
                    store_update.merge(tmp_store_update);
                }

                info!(target: "chain", "Init: saved genesis: #{} {} / {:?}", block_head.height, block_head.last_block_hash, state_roots);

                (block_head, header_head)
            }
            Err(err) => return Err(err),
        };
        store_update.commit()?;

        info!(target: "chain", "Init: header head @ #{} {}; block head @ #{} {}",
              header_head.height, header_head.last_block_hash,
              block_head.height, block_head.last_block_hash);
        metrics::BLOCK_HEIGHT_HEAD.set(block_head.height as i64);
        let block_header = store.get_block_header(&block_head.last_block_hash)?;
        metrics::BLOCK_ORDINAL_HEAD.set(block_header.block_ordinal() as i64);
        metrics::HEADER_HEAD_HEIGHT.set(header_head.height as i64);
        metrics::BOOT_TIME_SECONDS.set(StaticClock::utc().timestamp());

        metrics::TAIL_HEIGHT.set(store.tail()? as i64);
        metrics::CHUNK_TAIL_HEIGHT.set(store.chunk_tail()? as i64);
        metrics::FORK_TAIL_HEIGHT.set(store.fork_tail()? as i64);

        // Even though the channel is unbounded, the channel size is practically bounded by the size
        // of blocks_in_processing, which is set to 5 now.
        let (sc, rc) = unbounded();
        Ok(Chain {
            store,
            epoch_manager,
            shard_tracker,
            runtime_adapter,
            orphans: OrphanBlockPool::new(),
            blocks_with_missing_chunks: MissingChunksPool::new(),
            blocks_in_processing: BlocksInProcessing::new(),
            invalid_blocks: LruCache::new(INVALID_CHUNKS_POOL_SIZE),
            genesis: genesis.clone(),
            transaction_validity_period: chain_genesis.transaction_validity_period,
            epoch_length: chain_genesis.epoch_length,
            block_economics_config: BlockEconomicsConfig::from(chain_genesis),
            doomslug_threshold_mode,
            blocks_delay_tracker: BlocksDelayTracker::default(),
            apply_chunks_sender: sc,
            apply_chunks_receiver: rc,
            last_time_head_updated: StaticClock::instant(),
            pending_state_patch: Default::default(),
            requested_state_parts: StateRequestTracker::new(),
            state_snapshot_helper: make_snapshot_callback.map(|callback| StateSnapshotHelper {
                make_snapshot_callback: callback,
                test_snapshot_countdown_and_frequency: chain_config
                    .state_snapshot_every_n_blocks
                    .map(|n| (0, n)),
            }),
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
        if self.store.get_block(block.hash()).is_ok() {
            return Ok(());
        }
        if let Err(e) = self.validate_block(&block) {
            byzantine_assert!(false);
            return Err(e);
        }

        let mut chain_store_update = ChainStoreUpdate::new(&mut self.store);

        chain_store_update.save_block(block.into_inner());
        // We don't need to increase refcount for `prev_hash` at this point
        // because this is the block before State Sync.

        chain_store_update.commit()?;
        Ok(())
    }

    pub fn save_orphan(
        &mut self,
        block: MaybeValidated<Block>,
        requested_missing_chunks: bool,
    ) -> Result<(), Error> {
        if self.orphans.contains(block.hash()) {
            return Ok(());
        }
        if let Err(e) = self.validate_block(&block) {
            byzantine_assert!(false);
            return Err(e);
        }
        self.orphans.add(
            Orphan { block, provenance: Provenance::NONE, added: StaticClock::instant() },
            requested_missing_chunks,
        );
        Ok(())
    }

    fn save_block_height_processed(&mut self, block_height: BlockHeight) -> Result<(), Error> {
        let mut chain_store_update = ChainStoreUpdate::new(&mut self.store);
        if !chain_store_update.is_height_processed(block_height)? {
            chain_store_update.save_block_height_processed(block_height);
        }
        chain_store_update.commit()?;
        Ok(())
    }

    // GC CONTRACT
    // ===
    //
    // Prerequisites, guaranteed by the System:
    // 1. Genesis block is available and should not be removed by GC.
    // 2. No block in storage except Genesis has height lower or equal to `genesis_height`.
    // 3. There is known lowest block height (Tail) came from Genesis or State Sync.
    //    a. Tail is always on the Canonical Chain.
    //    b. Only one Tail exists.
    //    c. Tail's height is higher than or equal to `genesis_height`,
    // 4. There is a known highest block height (Head).
    //    a. Head is always on the Canonical Chain.
    // 5. All blocks in the storage have heights in range [Tail; Head].
    //    a. All forks end up on height of Head or lower.
    // 6. If block A is ancestor of block B, height of A is strictly less then height of B.
    // 7. (Property 1). A block with the lowest height among all the blocks at which the fork has started,
    //    i.e. all the blocks with the outgoing degree 2 or more,
    //    has the least height among all blocks on the fork.
    // 8. (Property 2). The oldest block where the fork happened is never affected
    //    by Canonical Chain Switching and always stays on Canonical Chain.
    //
    // Overall:
    // 1. GC procedure is handled by `clear_data()` function.
    // 2. `clear_data()` runs GC process for all blocks from the Tail to GC Stop Height provided by Epoch Manager.
    // 3. `clear_data()` executes separately:
    //    a. Forks Clearing runs for each height from Tail up to GC Stop Height.
    //    b. Canonical Chain Clearing from (Tail + 1) up to GC Stop Height.
    // 4. Before actual clearing is started, Block Reference Map should be built.
    // 5. `clear_data()` executes every time when block at new height is added.
    // 6. In case of State Sync, State Sync Clearing happens.
    //
    // Forks Clearing:
    // 1. Any fork which ends up on height `height` INCLUSIVELY and earlier will be completely deleted
    //    from the Store with all its ancestors up to the ancestor block where fork is happened
    //    EXCLUDING the ancestor block where fork is happened.
    // 2. The oldest ancestor block always remains on the Canonical Chain by property 2.
    // 3. All forks which end up on height `height + 1` and further are protected from deletion and
    //    no their ancestor will be deleted (even with lowest heights).
    // 4. `clear_forks_data()` handles forks clearing for fixed height `height`.
    //
    // Canonical Chain Clearing:
    // 1. Blocks on the Canonical Chain with the only descendant (if no forks started from them)
    //    are unlocked for Canonical Chain Clearing.
    // 2. If Forks Clearing ended up on the Canonical Chain, the block may be unlocked
    //    for the Canonical Chain Clearing. There is no other reason to unlock the block exists.
    // 3. All the unlocked blocks will be completely deleted
    //    from the Tail up to GC Stop Height EXCLUSIVELY.
    // 4. (Property 3, GC invariant). Tail can be shifted safely to the height of the
    //    earliest existing block. There is always only one Tail (based on property 1)
    //    and it's always on the Canonical Chain (based on property 2).
    //
    // Example:
    //
    // height: 101   102   103   104
    // --------[A]---[B]---[C]---[D]
    //          \     \
    //           \     \---[E]
    //            \
    //             \-[F]---[G]
    //
    // 1. Let's define clearing height = 102. It this case fork A-F-G is protected from deletion
    //    because of G which is on height 103. Nothing will be deleted.
    // 2. Let's define clearing height = 103. It this case Fork Clearing will be executed for A
    //    to delete blocks G and F, then Fork Clearing will be executed for B to delete block E.
    //    Then Canonical Chain Clearing will delete blocks A and B as unlocked.
    //    Block C is the only block of height 103 remains on the Canonical Chain (invariant).
    //
    // State Sync Clearing:
    // 1. Executing State Sync means that no data in the storage is useful for block processing
    //    and should be removed completely.
    // 2. The Tail should be set to the block preceding Sync Block.
    // 3. All the data preceding new Tail is deleted in State Sync Clearing
    //    and the Trie is updated with having only Genesis data.
    // 4. State Sync Clearing happens in `reset_data_pre_state_sync()`.
    //
    pub fn clear_data(
        &mut self,
        tries: ShardTries,
        gc_config: &near_chain_configs::GCConfig,
    ) -> Result<(), Error> {
        let _d = DelayDetector::new(|| "GC".into());

        let head = self.store.head()?;
        let tail = self.store.tail()?;
        let gc_stop_height = self.runtime_adapter.get_gc_stop_height(&head.last_block_hash);
        if gc_stop_height > head.height {
            return Err(Error::GCError("gc_stop_height cannot be larger than head.height".into()));
        }
        let prev_epoch_id = self.get_block_header(&head.prev_block_hash)?.epoch_id().clone();
        let epoch_change = prev_epoch_id != head.epoch_id;
        let mut fork_tail = self.store.fork_tail()?;
        metrics::TAIL_HEIGHT.set(tail as i64);
        metrics::FORK_TAIL_HEIGHT.set(fork_tail as i64);
        metrics::CHUNK_TAIL_HEIGHT.set(self.store.chunk_tail()? as i64);
        metrics::GC_STOP_HEIGHT.set(gc_stop_height as i64);
        if epoch_change && fork_tail < gc_stop_height {
            // if head doesn't change on the epoch boundary, we may update fork tail several times
            // but that is fine since it doesn't affect correctness and also we limit the number of
            // heights that fork cleaning goes through so it doesn't slow down client either.
            let mut chain_store_update = self.store.store_update();
            chain_store_update.update_fork_tail(gc_stop_height);
            chain_store_update.commit()?;
            fork_tail = gc_stop_height;
        }
        let mut gc_blocks_remaining = gc_config.gc_blocks_limit;

        // Forks Cleaning
        let gc_fork_clean_step = gc_config.gc_fork_clean_step;
        let stop_height = tail.max(fork_tail.saturating_sub(gc_fork_clean_step));
        for height in (stop_height..fork_tail).rev() {
            self.clear_forks_data(tries.clone(), height, &mut gc_blocks_remaining)?;
            if gc_blocks_remaining == 0 {
                return Ok(());
            }
            let mut chain_store_update = self.store.store_update();
            chain_store_update.update_fork_tail(height);
            chain_store_update.commit()?;
        }

        // Canonical Chain Clearing
        for height in tail + 1..gc_stop_height {
            if gc_blocks_remaining == 0 {
                return Ok(());
            }
            let blocks_current_height = self
                .store
                .get_all_block_hashes_by_height(height)?
                .values()
                .flatten()
                .cloned()
                .collect::<Vec<_>>();
            let mut chain_store_update = self.store.store_update();
            if let Some(block_hash) = blocks_current_height.first() {
                let prev_hash = *chain_store_update.get_block_header(block_hash)?.prev_hash();
                let prev_block_refcount = chain_store_update.get_block_refcount(&prev_hash)?;
                if prev_block_refcount > 1 {
                    // Block of `prev_hash` starts a Fork, stopping
                    break;
                } else if prev_block_refcount == 1 {
                    debug_assert_eq!(blocks_current_height.len(), 1);
                    chain_store_update.clear_block_data(
                        self.epoch_manager.as_ref(),
                        *block_hash,
                        GCMode::Canonical(tries.clone()),
                    )?;
                    gc_blocks_remaining -= 1;
                } else {
                    return Err(Error::GCError(
                        "block on canonical chain shouldn't have refcount 0".into(),
                    ));
                }
            }
            chain_store_update.update_tail(height)?;
            chain_store_update.commit()?;
        }
        Ok(())
    }

    /// Garbage collect data which archival node doesn’t need to keep.
    ///
    /// Normally, archival nodes keep all the data from the genesis block and
    /// don’t run garbage collection.  On the other hand, for better performance
    /// the storage contains some data duplication, i.e. values in some of the
    /// columns can be recomputed from data in different columns.  To save on
    /// storage, archival nodes do garbage collect that data.
    ///
    /// `gc_height_limit` limits how many heights will the function process.
    pub fn clear_archive_data(&mut self, gc_height_limit: BlockHeightDelta) -> Result<(), Error> {
        let _d = DelayDetector::new(|| "GC".into());

        let head = self.store.head()?;
        let gc_stop_height = self.runtime_adapter.get_gc_stop_height(&head.last_block_hash);
        if gc_stop_height > head.height {
            return Err(Error::GCError("gc_stop_height cannot be larger than head.height".into()));
        }

        let mut chain_store_update = self.store.store_update();
        chain_store_update.clear_redundant_chunk_data(gc_stop_height, gc_height_limit)?;
        metrics::CHUNK_TAIL_HEIGHT.set(chain_store_update.chunk_tail()? as i64);
        metrics::GC_STOP_HEIGHT.set(gc_stop_height as i64);
        chain_store_update.commit()
    }

    pub fn clear_forks_data(
        &mut self,
        tries: ShardTries,
        height: BlockHeight,
        gc_blocks_remaining: &mut NumBlocks,
    ) -> Result<(), Error> {
        let blocks_current_height = self
            .store
            .get_all_block_hashes_by_height(height)?
            .values()
            .flatten()
            .cloned()
            .collect::<Vec<_>>();
        for block_hash in blocks_current_height.iter() {
            let mut current_hash = *block_hash;
            loop {
                if *gc_blocks_remaining == 0 {
                    return Ok(());
                }
                // Block `block_hash` is not on the Canonical Chain
                // because shorter chain cannot be Canonical one
                // and it may be safely deleted
                // and all its ancestors while there are no other sibling blocks rely on it.
                let mut chain_store_update = self.store.store_update();
                if chain_store_update.get_block_refcount(&current_hash)? == 0 {
                    let prev_hash =
                        *chain_store_update.get_block_header(&current_hash)?.prev_hash();

                    // It's safe to call `clear_block_data` for prev data because it clears fork only here
                    chain_store_update.clear_block_data(
                        self.epoch_manager.as_ref(),
                        current_hash,
                        GCMode::Fork(tries.clone()),
                    )?;
                    chain_store_update.commit()?;
                    *gc_blocks_remaining -= 1;

                    current_hash = prev_hash;
                } else {
                    // Block of `current_hash` is an ancestor for some other blocks, stopping
                    break;
                }
            }
        }

        Ok(())
    }

    fn maybe_mark_block_invalid(&mut self, block_hash: CryptoHash, error: &Error) {
        metrics::NUM_INVALID_BLOCKS.inc();
        // We only mark the block as invalid if the block has bad data (not for other errors that would
        // not be the fault of the block itself), except when the block has a bad signature which means
        // the block might not have been what the block producer originally produced. Either way, it's
        // OK if we miss some cases here because this is just an optimization to avoid reprocessing
        // known invalid blocks so the network recovers faster in case of any issues.
        if error.is_bad_data() && !matches!(error, Error::InvalidSignature) {
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
                let genesis_chunk = &genesis_block.chunks()[shard_id];
                if genesis_chunk.chunk_hash() != chunk_header.chunk_hash()
                    || genesis_chunk.signature() != chunk_header.signature()
                {
                    return Err(Error::InvalidChunk);
                }
            } else if chunk_header.height_created() == block.header().height() {
                if !epoch_manager.verify_chunk_header_signature(
                    &chunk_header.clone(),
                    block.header().epoch_id(),
                    block.header().prev_hash(),
                )? {
                    byzantine_assert!(false);
                    return Err(Error::InvalidChunk);
                }
                if chunk_header.shard_id() != shard_id as ShardId {
                    return Err(Error::InvalidShardId(chunk_header.shard_id()));
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
        if header.timestamp() > StaticClock::utc() + Duration::seconds(ACCEPTABLE_TIME_DIFFERENCE) {
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
            self.store.get_all_block_hashes_by_height(header.height())?.get(header.epoch_id())
        {
            // This should be guaranteed but it doesn't hurt to check again
            if !block_hashes.contains(header.hash()) {
                let other_header = self.get_block_header(block_hashes.iter().next().unwrap())?;

                challenges.push(ChallengeBody::BlockDoubleSign(BlockDoubleSign {
                    left_block_header: header.try_to_vec().expect("Failed to serialize"),
                    right_block_header: other_header.try_to_vec().expect("Failed to serialize"),
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
                    header.next_epoch_id().clone(),
                    header.epoch_id().clone(),
                    header.prev_hash(),
                )?
            {
                return Err(Error::InvalidNextBPHash);
            }
        }

        if header.chunk_mask().len() as u64 != self.epoch_manager.num_shards(header.epoch_id())? {
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

            let block_merkle_tree = self.store.get_block_merkle_tree(header.prev_hash())?;
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
        debug!(target: "chain", "Process block header: {} at {}", header.hash(), header.height());

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
        if checked_feature!(
            "protocol_feature_block_header_v4",
            BlockHeaderV4,
            epoch_protocol_version
        ) {
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
                if chunk_header.prev_block_hash() != block.header().prev_hash() {
                    return Err(Error::InvalidChunk);
                }
            } else {
                if prev_chunk_header != chunk_header {
                    return Err(Error::InvalidChunk);
                }
            }
        }

        // Verify that proposals from chunks match block header proposals.
        let block_height = block.header().height();
        for pair in block
            .chunks()
            .iter()
            .filter(|chunk| block_height == chunk.height_included())
            .flat_map(|chunk| chunk.validator_proposals())
            .zip_longest(block.header().validator_proposals())
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
                    if self.store.is_block_challenged(&hash)? {
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
        let height = block.header().height();
        for (shard_id, chunk_header) in block.chunks().iter().enumerate() {
            // Check if any chunks are invalid in this block.
            if let Some(encoded_chunk) = self.store.is_invalid_chunk(&chunk_header.chunk_hash())? {
                let merkle_paths = Block::compute_chunk_headers_root(block.chunks().iter()).1;
                let chunk_proof = ChunkProofs {
                    block_header: block.header().try_to_vec().expect("Failed to serialize"),
                    merkle_proof: merkle_paths[shard_id].clone(),
                    chunk: MaybeEncodedShardChunk::Encoded(EncodedShardChunk::clone(
                        &encoded_chunk,
                    )),
                };
                return Err(Error::InvalidChunkProofs(Box::new(chunk_proof)));
            }
            let shard_id = shard_id as ShardId;
            if chunk_header.height_included() == height {
                let chunk_hash = chunk_header.chunk_hash();

                if let Err(_) = self.store.get_partial_chunk(&chunk_header.chunk_hash()) {
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
                    if let Err(_) = self.store.get_chunk(&chunk_hash) {
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
        for shard_id in 0..self.epoch_manager.num_shards(&epoch_id)? {
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
        let height = block.header().height();
        let mut receipt_proofs_by_shard_id = HashMap::new();

        for chunk_header in block.chunks().iter() {
            if chunk_header.height_included() == height {
                let partial_encoded_chunk =
                    self.store.get_partial_chunk(&chunk_header.chunk_hash()).unwrap();
                for receipt in partial_encoded_chunk.receipts().iter() {
                    let ReceiptProof(_, shard_proof) = receipt;
                    let ShardProof { from_shard_id: _, to_shard_id, proof: _ } = shard_proof;
                    receipt_proofs_by_shard_id
                        .entry(*to_shard_id)
                        .or_insert_with(Vec::new)
                        .push(receipt.clone());
                }
            }
        }
        // sort the receipts deterministically so the order that they will be processed is deterministic
        for (_, receipt_proofs) in receipt_proofs_by_shard_id.iter_mut() {
            Self::shuffle_receipt_proofs(receipt_proofs, block.hash());
        }

        Ok(receipt_proofs_by_shard_id)
    }

    fn shuffle_receipt_proofs<ReceiptProofType>(
        receipt_proofs: &mut Vec<ReceiptProofType>,
        block_hash: &CryptoHash,
    ) {
        let mut slice = [0u8; 32];
        slice.copy_from_slice(block_hash.as_ref());
        let mut rng: ChaCha20Rng = SeedableRng::from_seed(slice);
        receipt_proofs.shuffle(&mut rng);
    }

    #[cfg(test)]
    pub(crate) fn mark_block_as_challenged(
        &mut self,
        block_hash: &CryptoHash,
        challenger_hash: &CryptoHash,
    ) -> Result<(), Error> {
        let mut chain_update = self.chain_update();
        chain_update.mark_block_as_challenged(block_hash, Some(challenger_hash))?;
        chain_update.commit()?;
        Ok(())
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
    /// `apply_chunks_done_callback`: This callback will be called after apply_chunks are finished
    ///              (so it also happens asynchronously in the rayon thread pool). Callers can
    ///              use this callback as a way to receive notifications when apply chunks are done
    ///              so it can call postprocess_ready_blocks.
    pub fn start_process_block_async(
        &mut self,
        me: &Option<AccountId>,
        block: MaybeValidated<Block>,
        provenance: Provenance,
        block_processing_artifacts: &mut BlockProcessingArtifact,
        apply_chunks_done_callback: DoneApplyChunkCallback,
    ) -> Result<(), Error> {
        let block_received_time = StaticClock::instant();
        metrics::BLOCK_PROCESSING_ATTEMPTS_TOTAL.inc();

        let block_height = block.header().height();
        let hash = *block.hash();
        let res = self.start_process_block_impl(
            me,
            block,
            provenance,
            block_processing_artifacts,
            apply_chunks_done_callback,
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
        apply_chunks_done_callback: DoneApplyChunkCallback,
    ) -> (Vec<AcceptedBlock>, HashMap<CryptoHash, Error>) {
        let mut accepted_blocks = vec![];
        let mut errors = HashMap::new();
        while let Ok((block_hash, apply_result)) = self.apply_chunks_receiver.try_recv() {
            match self.postprocess_block(
                me,
                block_hash,
                apply_result,
                block_processing_artifacts,
                apply_chunks_done_callback.clone(),
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
        // Sort headers by heights if they are out of order.
        headers.sort_by_key(|left| left.height());

        if let Some(header) = headers.first() {
            debug!(target: "chain", "Sync block headers: {} headers from {} at {}", headers.len(), header.hash(), header.height());
        } else {
            return Ok(());
        };

        let all_known = if let Some(last_header) = headers.last() {
            self.store.get_block_header(last_header.hash()).is_ok()
        } else {
            false
        };

        if !all_known {
            // Validate header and then add to the chain.
            for header in headers.iter() {
                match check_header_known(self, header)? {
                    Ok(_) => {}
                    Err(_) => continue,
                }

                self.validate_header(header, &Provenance::SYNC, challenges)?;
                let mut chain_update = self.chain_update();
                chain_update.chain_store_update.save_block_header(header.clone())?;

                // Add validator proposals for given header.
                let last_finalized_height =
                    chain_update.chain_store_update.get_block_height(header.last_final_block())?;
                let epoch_manager_update = chain_update
                    .epoch_manager
                    .add_validator_proposals(BlockHeaderInfo::new(header, last_finalized_height))?;
                chain_update.chain_store_update.merge(epoch_manager_update);
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

    pub fn reset_data_pre_state_sync(&mut self, sync_hash: CryptoHash) -> Result<(), Error> {
        let _span = tracing::debug_span!(target: "sync", "reset_data_pre_state_sync").entered();
        let head = self.head()?;
        // Get header we were syncing into.
        let header = self.get_block_header(&sync_hash)?;
        let prev_hash = *header.prev_hash();
        let sync_height = header.height();
        let gc_height = std::cmp::min(head.height + 1, sync_height);

        // GC all the data from current tail up to `gc_height`. In case tail points to a height where
        // there is no block, we need to make sure that the last block before tail is cleaned.
        let tail = self.store.tail()?;
        let mut tail_prev_block_cleaned = false;
        for height in tail..gc_height {
            let blocks_current_height = self
                .store
                .get_all_block_hashes_by_height(height)?
                .values()
                .flatten()
                .cloned()
                .collect::<Vec<_>>();
            for block_hash in blocks_current_height {
                let epoch_manager = self.epoch_manager.clone();
                let mut chain_store_update = self.mut_store().store_update();
                if !tail_prev_block_cleaned {
                    let prev_block_hash =
                        *chain_store_update.get_block_header(&block_hash)?.prev_hash();
                    if chain_store_update.get_block(&prev_block_hash).is_ok() {
                        chain_store_update.clear_block_data(
                            epoch_manager.as_ref(),
                            prev_block_hash,
                            GCMode::StateSync { clear_block_info: true },
                        )?;
                    }
                    tail_prev_block_cleaned = true;
                }
                chain_store_update.clear_block_data(
                    epoch_manager.as_ref(),
                    block_hash,
                    GCMode::StateSync { clear_block_info: block_hash != prev_hash },
                )?;
                chain_store_update.commit()?;
            }
        }

        // Clear Chunks data
        let mut chain_store_update = self.mut_store().store_update();
        // The largest height of chunk we have in storage is head.height + 1
        let chunk_height = std::cmp::min(head.height + 2, sync_height);
        chain_store_update.clear_chunk_data_and_headers(chunk_height)?;
        chain_store_update.commit()?;

        // clear all trie data

        let tries = self.runtime_adapter.get_tries();
        let mut chain_store_update = self.mut_store().store_update();
        let mut store_update = tries.store_update();
        store_update.delete_all(DBCol::State);
        chain_store_update.merge(store_update);

        // The reason to reset tail here is not to allow Tail be greater than Head
        chain_store_update.reset_tail();
        chain_store_update.commit()?;
        Ok(())
    }

    /// Set the new head after state sync was completed if it is indeed newer.
    /// Check for potentially unlocked orphans after this update.
    pub fn reset_heads_post_state_sync(
        &mut self,
        me: &Option<AccountId>,
        sync_hash: CryptoHash,
        block_processing_artifacts: &mut BlockProcessingArtifact,
        apply_chunks_done_callback: DoneApplyChunkCallback,
    ) -> Result<(), Error> {
        let _span = tracing::debug_span!(target: "sync", "reset_heads_post_state_sync").entered();
        // Get header we were syncing into.
        let header = self.get_block_header(&sync_hash)?;
        let hash = *header.prev_hash();
        let prev_block = self.get_block(&hash)?;
        let new_tail = prev_block.header().height();
        let new_chunk_tail = prev_block.chunks().iter().map(|x| x.height_created()).min().unwrap();
        let tip = Tip::from_header(prev_block.header());
        let final_head = Tip::from_header(self.genesis.header());
        // Update related heads now.
        let mut chain_store_update = self.mut_store().store_update();
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
        self.check_orphans(me, hash, block_processing_artifacts, apply_chunks_done_callback);
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
        apply_chunks_done_callback: DoneApplyChunkCallback,
        block_received_time: Instant,
    ) -> Result<(), Error> {
        let block_height = block.header().height();
        let _span = tracing::debug_span!(
            target: "chain",
            "start_process_block_impl",
            height = block_height)
        .entered();
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
                        let tail_height = self.store.tail()?;
                        // we only add blocks that couldn't have been gc'ed to the orphan pool.
                        if block_height >= tail_height {
                            let block_hash = *block.hash();
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

                            let time = StaticClock::instant();
                            self.blocks_delay_tracker.mark_block_orphaned(block.hash(), time);
                            let orphan = Orphan { block, provenance, added: time };
                            self.orphans.add(orphan, requested_missing_chunks);

                            debug!(
                                target: "chain",
                                "Process block: orphan: {:?}, # orphans {}{}",
                                block_hash,
                                self.orphans.len(),
                                if self.orphans.len_evicted() > 0 {
                                    format!(", # evicted {}", self.orphans.len_evicted())
                                } else {
                                    String::new()
                                },
                            );
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
                        let time = StaticClock::instant();
                        self.blocks_delay_tracker.mark_block_has_missing_chunks(block.hash(), time);
                        let orphan = Orphan { block, provenance, added: time };
                        self.blocks_with_missing_chunks
                            .add_block_with_missing_chunks(orphan, missing_chunk_hashes.clone());
                        debug!(
                            target: "chain",
                            "Process block: missing chunks. Block hash: {:?}. Missing chunks: {:?}",
                            block_hash, missing_chunk_hashes,
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
                            "Block {} at {} is known at this time: {:?}",
                            block.hash(),
                            block_height,
                            block_known_error);
                    }
                    _ => {}
                }
                return Err(e);
            }
        };
        let (apply_chunk_work, block_preprocess_info) = preprocess_res;

        let need_state_snapshot = block_preprocess_info.need_state_snapshot
            | self.need_test_state_snapshot(block_preprocess_info.need_state_snapshot);
        if let Err(err) = self.maybe_start_state_snapshot(need_state_snapshot) {
            tracing::error!(target: "state_snapshot", ?err, "Failed to make a state snapshot");
        }

        let block = block.into_inner();
        let block_hash = *block.hash();
        let block_height = block.header().height();
        let apply_chunks_done_marker = block_preprocess_info.apply_chunks_done.clone();
        self.blocks_in_processing.add(block, block_preprocess_info)?;

        // 2) schedule apply chunks, which will be executed in the rayon thread pool.
        self.schedule_apply_chunks(
            block_hash,
            block_height,
            apply_chunk_work,
            apply_chunks_done_marker,
            apply_chunks_done_callback.clone(),
        );

        Ok(())
    }

    /// Applying chunks async by starting the work at the rayon thread pool
    /// `apply_chunks_done_marker`: a marker that will be set to true once applying chunks is finished
    /// `apply_chunks_done_callback`: a callback that will be called once applying chunks is finished
    fn schedule_apply_chunks(
        &self,
        block_hash: CryptoHash,
        block_height: BlockHeight,
        work: Vec<Box<dyn FnOnce(&Span) -> Result<ApplyChunkResult, Error> + Send>>,
        apply_chunks_done_marker: Arc<OnceCell<()>>,
        apply_chunks_done_callback: DoneApplyChunkCallback,
    ) {
        let sc = self.apply_chunks_sender.clone();
        spawn(move || {
            // do_apply_chunks runs `work` parallelly, but still waits for all of them to finish
            let res = do_apply_chunks(block_hash, block_height, work);
            // If we encounter error here, that means the receiver is deallocated and the client
            // thread is already shut down. The node is already crashed, so we can unwrap here
            sc.send((block_hash, res)).unwrap();
            if let Err(_) = apply_chunks_done_marker.set(()) {
                // This should never happen, if it does, it means there is a bug in our code.
                log_assert!(false, "apply chunks are called twice for block {block_hash:?}");
            }
            apply_chunks_done_callback(block_hash);
        });

        /// `rayon::spawn` decorated to propagate `tracing` context across
        /// threads.
        fn spawn(f: impl FnOnce() + Send + 'static) {
            let dispatcher = tracing::dispatcher::get_default(|it| it.clone());
            rayon::spawn(move || tracing::dispatcher::with_default(&dispatcher, f))
        }
    }

    fn postprocess_block_only(
        &mut self,
        me: &Option<AccountId>,
        block: &Block,
        block_preprocess_info: BlockPreprocessInfo,
        apply_results: Vec<Result<ApplyChunkResult, Error>>,
    ) -> Result<Option<Tip>, Error> {
        let mut chain_update = self.chain_update();
        let new_head =
            chain_update.postprocess_block(me, &block, block_preprocess_info, apply_results)?;
        chain_update.commit()?;
        Ok(new_head)
    }

    /// Update flat storage for given processed or caught up block, which includes:
    /// - merge deltas from current flat storage head to new one;
    /// - update flat storage head to the hash of final block visible from given one;
    /// - remove info about unreachable blocks from memory.
    fn update_flat_storage_for_block(
        &mut self,
        block: &Block,
        shard_uid: ShardUId,
    ) -> Result<(), Error> {
        if let Some(flat_storage) = self
            .runtime_adapter
            .get_flat_storage_manager()
            .and_then(|manager| manager.get_flat_storage_for_shard(shard_uid))
        {
            let mut new_flat_head = *block.header().last_final_block();
            if new_flat_head == CryptoHash::default() {
                new_flat_head = *self.genesis.hash();
            }
            // Try to update flat head.
            flat_storage.update_flat_head(&new_flat_head).unwrap_or_else(|err| {
                match &err {
                    FlatStorageError::BlockNotSupported(_) => {
                        // It's possible that new head is not a child of current flat head, e.g. when we have a
                        // fork:
                        //
                        //      (flat head)        /-------> 6
                        // 1 ->      2     -> 3 -> 4
                        //                         \---> 5
                        //
                        // where during postprocessing (5) we call `update_flat_head(3)` and then for (6) we can
                        // call `update_flat_head(2)` because (2) will be last visible final block from it.
                        // In such case, just log an error.
                        debug!(target: "chain", "Cannot update flat head to {:?}: {:?}", new_flat_head, err);
                    }
                    _ => {
                        // All other errors are unexpected, so we panic.
                        panic!("Cannot update flat head to {:?}: {:?}", new_flat_head, err);
                    }
                }
            });
        } else {
            // TODO (#8250): come up with correct assertion. Currently it doesn't work because runtime may be
            // implemented by KeyValueRuntime which doesn't support flat storage, and flat storage background
            // creation may happen.
            // debug_assert!(false, "Flat storage state for shard {shard_id} does not exist and its creation was not initiated");
        }
        Ok(())
    }

    /// Run postprocessing on this block, which stores the block on chain.
    /// Check that if accepting the block unlocks any orphans in the orphan pool and start
    /// the processing of those blocks.
    fn postprocess_block(
        &mut self,
        me: &Option<AccountId>,
        block_hash: CryptoHash,
        apply_results: Vec<Result<ApplyChunkResult, Error>>,
        block_processing_artifacts: &mut BlockProcessingArtifact,
        apply_chunks_done_callback: DoneApplyChunkCallback,
    ) -> Result<AcceptedBlock, Error> {
        let timer = metrics::BLOCK_POSTPROCESSING_TIME.start_timer();
        let (block, block_preprocess_info) =
            self.blocks_in_processing.remove(&block_hash).expect(&format!(
                "block {:?} finished applying chunks but not in blocks_in_processing pool",
                block_hash
            ));
        // We want to include block height here, so we didn't put this line at the beginning of the
        // function.
        let _span = tracing::debug_span!(
            target: "chain",
            "postprocess_block",
            height = block.header().height())
        .entered();

        let prev_head = self.store.head()?;
        let is_caught_up = block_preprocess_info.is_caught_up;
        let provenance = block_preprocess_info.provenance.clone();
        let block_start_processing_time = block_preprocess_info.block_start_processing_time;
        // TODO(#8055): this zip relies on the ordering of the apply_results.
        for (apply_result, chunk) in apply_results.iter().zip(block.chunks().iter()) {
            if let Err(err) = apply_result {
                if err.is_bad_data() {
                    block_processing_artifacts.invalid_chunks.push(chunk.clone());
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

        // Update flat storage head to be the last final block. Note that this update happens
        // in a separate db transaction from the update from block processing. This is intentional
        // because flat_storage need to be locked during the update of flat head, otherwise
        // flat_storage is in an inconsistent state that could be accessed by the other
        // apply chunks processes. This means, the flat head is not always the same as
        // the last final block on chain, which is OK, because in the flat storage implementation
        // we don't assume that.
        let epoch_id = block.header().epoch_id();
        for shard_id in 0..self.epoch_manager.num_shards(epoch_id)? {
            let need_flat_storage_update = if is_caught_up {
                // If we already caught up this epoch, then flat storage exists for both shards which we already track
                // and shards which will be tracked in next epoch, so we can update them.
                self.shard_tracker.care_about_shard(
                    me.as_ref(),
                    block.header().prev_hash(),
                    shard_id,
                    true,
                ) || self.shard_tracker.will_care_about_shard(
                    me.as_ref(),
                    block.header().prev_hash(),
                    shard_id,
                    true,
                )
            } else {
                // If we didn't catch up, we can update only shards tracked right now. Remaining shards will be updated
                // during catchup of this block.
                self.shard_tracker.care_about_shard(
                    me.as_ref(),
                    block.header().prev_hash(),
                    shard_id,
                    true,
                )
            };

            if need_flat_storage_update {
                let shard_uid = self.epoch_manager.shard_id_to_uid(shard_id, epoch_id)?;
                self.update_flat_storage_for_block(&block, shard_uid)?;
            }
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

            self.last_time_head_updated = StaticClock::instant();
        };

        metrics::BLOCK_PROCESSED_TOTAL.inc();
        metrics::BLOCK_PROCESSING_TIME.observe(
            StaticClock::instant()
                .saturating_duration_since(block_start_processing_time)
                .as_secs_f64(),
        );
        self.blocks_delay_tracker.finish_block_processing(&block_hash, new_head.clone());

        timer.observe_duration();
        let _timer = CryptoHashTimer::new_with_start(*block.hash(), block_start_processing_time);

        self.check_orphans(
            me,
            *block.hash(),
            block_processing_artifacts,
            apply_chunks_done_callback,
        );

        // Determine the block status of this block (whether it is a side fork and updates the chain head)
        // Block status is needed in Client::on_block_accepted_with_optional_chunk_produce to
        // decide to how to update the tx pool.
        let block_status = self.determine_status(new_head, prev_head);
        Ok(AcceptedBlock { hash: *block.hash(), status: block_status, provenance })
    }

    /// Preprocess a block before applying chunks, verify that we have the necessary information
    /// to process the block an the block is valid.
    //  Note that this function does NOT introduce any changes to chain state.
    pub(crate) fn preprocess_block(
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

        debug!(target: "chain", num_approvals = header.num_approvals(), "Preprocess block");

        // Check that we know the epoch of the block before we try to get the header
        // (so that a block from unknown epoch doesn't get marked as an orphan)
        if !self.epoch_manager.epoch_exists(header.epoch_id()) {
            return Err(Error::EpochOutOfBounds(header.epoch_id().clone()));
        }

        if block.chunks().len() != self.epoch_manager.num_shards(header.epoch_id())? as usize {
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
        let prev_gas_price = prev.gas_price();
        let prev_random_value = *prev.random_value();
        let prev_height = prev.height();

        // Do not accept old forks
        if prev_height < self.runtime_adapter.get_gc_stop_height(&head.last_block_hash) {
            return Err(Error::InvalidBlockHeight(prev_height));
        }

        let (is_caught_up, state_sync_info, need_state_snapshot) =
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

        let res = block.validate_with(|block| {
            Chain::validate_block_impl(self.epoch_manager.as_ref(), &self.genesis, block)
                .map(|_| true)
        });
        if let Err(e) = res {
            byzantine_assert!(false);
            return Err(e);
        }

        let protocol_version = self.epoch_manager.get_epoch_protocol_version(header.epoch_id())?;
        if !block.verify_gas_price(
            prev_gas_price,
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
                apply_chunks_done: Arc::new(OnceCell::new()),
                block_start_processing_time: block_received_time,
                need_state_snapshot,
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
    ) -> Result<(bool, Option<StateSyncInfo>, bool), Error> {
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
            let is_genesis = prev_prev_hash == CryptoHash::default();
            let need_state_snapshot = !is_genesis;
            Ok((state_sync_info.is_none(), state_sync_info, need_state_snapshot))
        } else {
            Ok((self.prev_block_is_caught_up(&prev_prev_hash, &prev_hash)?, None, false))
        }
    }

    /// Check if we can request chunks for this orphan. Conditions are
    /// 1) Orphans that with outstanding missing chunks request has not exceed `MAX_ORPHAN_MISSING_CHUNKS`
    /// 2) we haven't already requested missing chunks for the orphan
    /// 3) All the `NUM_ORPHAN_ANCESTORS_CHECK` immediate parents of the block are either accepted,
    ///    or orphans or in `blocks_with_missing_chunks`
    /// 4) Among the `NUM_ORPHAN_ANCESTORS_CHECK` immediate parents of the block at least one is
    ///    accepted(is in store), call it `ancestor`
    /// 5) The next block of `ancestor` has the same epoch_id as the orphan block
    ///    (This is because when requesting chunks, we will use `ancestor` hash instead of the
    ///     previous block hash of the orphan to decide epoch id)
    /// 6) The orphan has missing chunks
    pub fn should_request_chunks_for_orphan(
        &mut self,
        me: &Option<AccountId>,
        orphan: &Block,
    ) -> Option<OrphanMissingChunks> {
        // 1) Orphans that with outstanding missing chunks request has not exceed `MAX_ORPHAN_MISSING_CHUNKS`
        // 2) we haven't already requested missing chunks for the orphan
        if !self.orphans.can_request_missing_chunks_for_orphan(orphan.hash()) {
            return None;
        }
        let mut block_hash = *orphan.header().prev_hash();
        for _ in 0..NUM_ORPHAN_ANCESTORS_CHECK {
            // 3) All the `NUM_ORPHAN_ANCESTORS_CHECK` immediate parents of the block are either accepted,
            //    or orphans or in `blocks_with_missing_chunks`
            if let Some(block) = self.blocks_with_missing_chunks.get(&block_hash) {
                block_hash = *block.prev_hash();
                continue;
            }
            if let Some(orphan) = self.orphans.get(&block_hash) {
                block_hash = *orphan.prev_hash();
                continue;
            }
            // 4) Among the `NUM_ORPHAN_ANCESTORS_CHECK` immediate parents of the block at least one is
            //    accepted(is in store), call it `ancestor`
            if self.get_block(&block_hash).is_ok() {
                if let Ok(epoch_id) = self.epoch_manager.get_epoch_id_from_prev_block(&block_hash) {
                    // 5) The next block of `ancestor` has the same epoch_id as the orphan block
                    if &epoch_id == orphan.header().epoch_id() {
                        // 6) The orphan has missing chunks
                        if let Err(e) = self.ping_missing_chunks(me, block_hash, orphan) {
                            return match e {
                                Error::ChunksMissing(missing_chunks) => {
                                    debug!(target:"chain", "Request missing chunks for orphan {:?} {:?}", orphan.hash(), missing_chunks.iter().map(|chunk|{(chunk.shard_id(), chunk.chunk_hash())}).collect::<Vec<_>>());
                                    Some(OrphanMissingChunks {
                                        missing_chunks,
                                        epoch_id,
                                        ancestor_hash: block_hash,
                                    })
                                }
                                _ => None,
                            };
                        }
                    }
                }
                return None;
            }
            return None;
        }
        None
    }

    /// only used for test
    pub fn check_orphan_partial_chunks_requested(&self, block_hash: &CryptoHash) -> bool {
        self.orphans.orphans_requested_missing_chunks.contains(block_hash)
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
        Ok(!self.store.get_blocks_to_catchup(prev_prev_hash)?.contains(prev_hash))
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
        Ok((0..epoch_manager.num_shards(&epoch_id)?)
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
        let will_shard_layout_change =
            epoch_manager.will_shard_layout_change(parent_hash).unwrap_or(false);
        // if shard layout will change the next epoch, we should catch up the shard regardless
        // whether we already have the shard's state this epoch, because we need to generate
        // new states for shards split from the current shard for the next epoch
        let will_care_about_shard =
            shard_tracker.will_care_about_shard(me.as_ref(), parent_hash, shard_id, true);
        let does_care_about_shard =
            shard_tracker.care_about_shard(me.as_ref(), parent_hash, shard_id, true);

        will_care_about_shard && (will_shard_layout_change || !does_care_about_shard)
    }

    /// Check if any block with missing chunk is ready to be processed and start processing these blocks
    pub fn check_blocks_with_missing_chunks(
        &mut self,
        me: &Option<AccountId>,
        block_processing_artifact: &mut BlockProcessingArtifact,
        apply_chunks_done_callback: DoneApplyChunkCallback,
    ) {
        let blocks = self.blocks_with_missing_chunks.ready_blocks();
        if !blocks.is_empty() {
            debug!(target:"chain", "Got {} blocks that were missing chunks but now are ready.", blocks.len());
        }
        for block in blocks {
            let block_hash = *block.block.header().hash();
            let height = block.block.header().height();
            let time = StaticClock::instant();
            let res = self.start_process_block_async(
                me,
                block.block,
                block.provenance,
                block_processing_artifact,
                apply_chunks_done_callback.clone(),
            );
            match res {
                Ok(_) => {
                    debug!(target: "chain", %block_hash, height, "Accepted block with missing chunks");
                    self.blocks_delay_tracker
                        .mark_block_completed_missing_chunks(&block_hash, time);
                }
                Err(_) => {
                    debug!(target: "chain", %block_hash, height, "Declined block with missing chunks is declined.");
                }
            }
        }
    }

    /// Check for orphans that are ready to be processed or request missing chunks, process these blocks.
    /// `prev_hash`: hash of the block that is just accepted
    /// `block_accepted`: callback to be called when an orphan is accepted
    /// `block_misses_chunks`: callback to be called when an orphan is added to the pool of blocks
    ///                        that have missing chunks
    /// `orphan_misses_chunks`: callback to be called when it is ready to request missing chunks for
    ///                         an orphan
    /// `on_challenge`: callback to be called when an orphan should be challenged
    pub fn check_orphans(
        &mut self,
        me: &Option<AccountId>,
        prev_hash: CryptoHash,
        block_processing_artifacts: &mut BlockProcessingArtifact,
        apply_chunks_done_callback: DoneApplyChunkCallback,
    ) {
        // Check if there are orphans we can process.
        debug!(target: "chain", "Check orphans: from {}, # total orphans {}", prev_hash, self.orphans.len());
        // check within the descendents of `prev_hash` to see if there are orphans there that
        // are ready to request missing chunks for
        let orphans_to_check =
            self.orphans.get_orphans_within_depth(prev_hash, NUM_ORPHAN_ANCESTORS_CHECK);
        for orphan_hash in orphans_to_check {
            let orphan = self.orphans.get(&orphan_hash).unwrap().block.clone();
            if let Some(orphan_missing_chunks) = self.should_request_chunks_for_orphan(me, &orphan)
            {
                block_processing_artifacts.orphans_missing_chunks.push(orphan_missing_chunks);
                self.orphans.mark_missing_chunks_requested_for_orphan(orphan_hash);
            }
        }
        if let Some(orphans) = self.orphans.remove_by_prev_hash(prev_hash) {
            debug!(target: "chain", found_orphans = orphans.len(), "Check orphans");
            for orphan in orphans.into_iter() {
                let block_hash = orphan.hash();
                self.blocks_delay_tracker
                    .mark_block_unorphaned(&block_hash, StaticClock::instant());
                let res = self.start_process_block_async(
                    me,
                    orphan.block,
                    orphan.provenance,
                    block_processing_artifacts,
                    apply_chunks_done_callback.clone(),
                );
                if let Err(err) = res {
                    debug!(target: "chain", "Orphan {:?} declined, error: {:?}", block_hash, err);
                }
            }
            debug!(
                target: "chain",
                remaining_orphans=self.orphans.len(),
                "Check orphans",
            );
        }
    }

    pub fn get_outgoing_receipts_for_shard(
        &self,
        prev_block_hash: CryptoHash,
        shard_id: ShardId,
        last_height_included: BlockHeight,
    ) -> Result<Vec<Receipt>, Error> {
        self.store.get_outgoing_receipts_for_shard(
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
        let sync_block_header = sync_block.header().clone();
        let sync_block_epoch_id = sync_block.header().epoch_id().clone();
        if shard_id as usize >= sync_block.chunks().len() {
            return Err(Error::InvalidStateRequest("shard_id out of bounds".into()));
        }

        // The chunk was applied at height `chunk_header.height_included`.
        // Getting the `current` state.
        let sync_prev_block = self.get_block(sync_block_header.prev_hash())?;
        if &sync_block_epoch_id == sync_prev_block.header().epoch_id() {
            return Err(Error::InvalidStateRequest(
                "sync_hash is not the first hash of the epoch".into(),
            ));
        }
        if shard_id as usize >= sync_prev_block.chunks().len() {
            return Err(Error::InvalidStateRequest("shard_id out of bounds".into()));
        }
        // Chunk header here is the same chunk header as at the `current` height.
        let sync_prev_hash = *sync_prev_block.hash();
        let chunk_header = sync_prev_block.chunks()[shard_id as usize].clone();
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

        let chunk = self.get_chunk_clone_from_header(&chunk_header)?;
        let chunk_proof = chunk_proofs[shard_id as usize].clone();
        let block_header =
            self.get_block_header_on_chain_by_height(&sync_hash, chunk_header.height_included())?;

        // Collecting the `prev` state.
        let (prev_chunk_header, prev_chunk_proof, prev_chunk_height_included) = match self
            .get_block(block_header.prev_hash())
        {
            Ok(prev_block) => {
                if shard_id as usize >= prev_block.chunks().len() {
                    return Err(Error::InvalidStateRequest("shard_id out of bounds".into()));
                }
                let prev_chunk_header = prev_block.chunks()[shard_id as usize].clone();
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

                let prev_chunk_proof = prev_chunk_proofs[shard_id as usize].clone();
                let prev_chunk_height_included = prev_chunk_header.height_included();

                (Some(prev_chunk_header), Some(prev_chunk_proof), prev_chunk_height_included)
            }
            Err(e) => match e {
                Error::DBNotFoundErr(_) => {
                    if block_header.prev_hash() == &CryptoHash::default() {
                        (None, None, 0)
                    } else {
                        return Err(e);
                    }
                }
                _ => return Err(e),
            },
        };

        // Getting all existing incoming_receipts from prev_chunk height to the new epoch.
        let incoming_receipts_proofs = self.store.get_incoming_receipts_for_shard(
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
                    .map(|chunk| chunk.outgoing_receipts_root())
                    .collect::<Vec<CryptoHash>>(),
            );

            let mut root_proofs_cur = vec![];
            assert_eq!(receipt_proofs.len(), block_header.chunks_included() as usize);
            for receipt_proof in receipt_proofs.iter() {
                let ReceiptProof(receipts, shard_proof) = receipt_proof;
                let ShardProof { from_shard_id, to_shard_id: _, proof } = shard_proof;
                let receipts_hash = CryptoHash::hash_borsh(ReceiptList(shard_id, receipts));
                let from_shard_id = *from_shard_id as usize;

                let root_proof = block.chunks()[from_shard_id].outgoing_receipts_root();
                root_proofs_cur
                    .push(RootProof(root_proof, block_receipts_proofs[from_shard_id].clone()));

                // Make sure we send something reasonable.
                assert_eq!(block_header.chunk_receipts_root(), &block_receipts_root);
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
            &sync_prev_hash,
            &chunk_header.prev_state_root(),
        )?;

        let shard_state_header = match chunk {
            ShardChunk::V1(chunk) => {
                let prev_chunk_header =
                    prev_chunk_header.and_then(|prev_header| match prev_header {
                        ShardChunkHeader::V1(header) => Some(header),
                        ShardChunkHeader::V2(_) => None,
                        ShardChunkHeader::V3(_) => None,
                    });
                ShardStateSyncResponseHeader::V1(ShardStateSyncResponseHeaderV1 {
                    chunk,
                    chunk_proof,
                    prev_chunk_header,
                    prev_chunk_proof,
                    incoming_receipts_proofs,
                    root_proofs,
                    state_root_node,
                })
            }

            chunk @ ShardChunk::V2(_) => {
                ShardStateSyncResponseHeader::V2(ShardStateSyncResponseHeaderV2 {
                    chunk,
                    chunk_proof,
                    prev_chunk_header,
                    prev_chunk_proof,
                    incoming_receipts_proofs,
                    root_proofs,
                    state_root_node,
                })
            }
        };
        Ok(shard_state_header)
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
        let key = StateHeaderKey(shard_id, sync_hash).try_to_vec()?;
        if let Ok(Some(header)) = self.store.store().get_ser(DBCol::StateHeaders, &key) {
            return Ok(header);
        }

        let shard_state_header = self.compute_state_response_header(shard_id, sync_hash)?;

        // Saving the header data
        let mut store_update = self.store.store().store_update();
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
            %sync_hash)
        .entered();
        // Check cache
        let key = StatePartKey(sync_hash, shard_id, part_id).try_to_vec()?;
        if let Ok(Some(state_part)) = self.store.store().get(DBCol::StateParts, &key) {
            return Ok(state_part.into());
        }

        let sync_block = self
            .get_block(&sync_hash)
            .log_storage_error("block has already been checked for existence")?;
        let sync_block_header = sync_block.header().clone();
        let sync_block_epoch_id = sync_block.header().epoch_id().clone();
        if shard_id as usize >= sync_block.chunks().len() {
            return Err(Error::InvalidStateRequest("shard_id out of bounds".into()));
        }
        let sync_prev_block = self.get_block(sync_block_header.prev_hash())?;
        if &sync_block_epoch_id == sync_prev_block.header().epoch_id() {
            return Err(Error::InvalidStateRequest(
                "sync_hash is not the first hash of the epoch".into(),
            ));
        }
        if shard_id as usize >= sync_prev_block.chunks().len() {
            return Err(Error::InvalidStateRequest("shard_id out of bounds".into()));
        }
        let state_root = sync_prev_block.chunks()[shard_id as usize].prev_state_root();
        let sync_prev_hash = *sync_prev_block.hash();
        let sync_prev_prev_hash = *sync_prev_block.header().prev_hash();
        let state_root_node = self
            .runtime_adapter
            .get_state_root_node(shard_id, &sync_prev_hash, &state_root)
            .log_storage_error("get_state_root_node fail")?;
        let num_parts = get_num_state_parts(state_root_node.memory_usage);

        if part_id >= num_parts {
            return Err(Error::InvalidStateRequest("part_id out of bound".to_string()));
        }
        let current_time = Instant::now();
        let state_part = self
            .runtime_adapter
            .obtain_state_part(
                shard_id,
                &sync_prev_prev_hash,
                &state_root,
                PartId::new(part_id, num_parts),
            )
            .log_storage_error("obtain_state_part fail")?;

        let elapsed_ms = current_time.elapsed().as_millis();
        self.requested_state_parts
            .save_state_part_elapsed(&sync_hash, &shard_id, &part_id, elapsed_ms);

        // Before saving State Part data, we need to make sure we can calculate and save State Header
        self.get_state_response_header(shard_id, sync_hash)?;

        // Saving the part data
        let mut store_update = self.store.store().store_update();
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
                if !verify_path(*block_header.chunk_receipts_root(), block_proof, root) {
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
        let mut store_update = self.store.store().store_update();
        let key = StateHeaderKey(shard_id, sync_hash).try_to_vec()?;
        store_update.set_ser(DBCol::StateHeaders, &key, &shard_state_header)?;
        store_update.commit()?;

        Ok(())
    }

    pub fn get_state_header(
        &self,
        shard_id: ShardId,
        sync_hash: CryptoHash,
    ) -> Result<ShardStateSyncResponseHeader, Error> {
        self.store.get_state_header(shard_id, sync_hash)
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
        let mut store_update = self.store.store().store_update();
        let key = StatePartKey(sync_hash, shard_id, part_id.idx).try_to_vec()?;
        store_update.set(DBCol::StateParts, &key, data);
        store_update.commit()?;
        Ok(())
    }

    pub fn schedule_apply_state_parts(
        &self,
        shard_id: ShardId,
        sync_hash: CryptoHash,
        num_parts: u64,
        state_parts_task_scheduler: &dyn Fn(ApplyStatePartsRequest),
    ) -> Result<(), Error> {
        let epoch_id = self.get_block_header(&sync_hash)?.epoch_id().clone();
        let shard_uid = self.epoch_manager.shard_id_to_uid(shard_id, &epoch_id)?;

        let shard_state_header = self.get_state_header(shard_id, sync_hash)?;
        let state_root = shard_state_header.chunk_prev_state_root();

        state_parts_task_scheduler(ApplyStatePartsRequest {
            runtime_adapter: self.runtime_adapter.clone(),
            shard_uid,
            state_root,
            num_parts,
            epoch_id,
            sync_hash,
        });

        Ok(())
    }

    pub fn set_state_finalize(
        &mut self,
        shard_id: ShardId,
        sync_hash: CryptoHash,
        apply_result: Result<(), near_chain_primitives::Error>,
    ) -> Result<(), Error> {
        let _span = tracing::debug_span!(target: "sync", "set_state_finalize").entered();
        apply_result?;

        let shard_state_header = self.get_state_header(shard_id, sync_hash)?;
        let chunk = shard_state_header.cloned_chunk();

        let block_hash = chunk.prev_block();

        // We synced shard state on top of _previous_ block for chunk in shard state header and applied state parts to
        // flat storage. Now we can set flat head to hash of this block and create flat storage.
        // If block_hash is equal to default - this means that we're all the way back at genesis.
        // So we don't have to add the storage state for shard in such case.
        // TODO(8438) - add additional test scenarios for this case.
        if *block_hash != CryptoHash::default() {
            let block_header = self.get_block_header(block_hash)?;
            let epoch_id = block_header.epoch_id();
            let shard_uid = self.epoch_manager.shard_id_to_uid(shard_id, epoch_id)?;

            if let Some(flat_storage_manager) = self.runtime_adapter.get_flat_storage_manager() {
                // Flat storage must not exist at this point because leftover keys corrupt its state.
                assert!(flat_storage_manager.get_flat_storage_for_shard(shard_uid).is_none());

                let mut store_update = self.runtime_adapter.store().store_update();
                store_helper::set_flat_storage_status(
                    &mut store_update,
                    shard_uid,
                    FlatStorageStatus::Ready(FlatStorageReadyStatus {
                        flat_head: near_store::flat::BlockInfo {
                            hash: *block_hash,
                            prev_hash: *block_header.prev_hash(),
                            height: block_header.height(),
                        },
                    }),
                );
                store_update.commit()?;
                flat_storage_manager.create_flat_storage_for_shard(shard_uid).unwrap();
            }
        }

        let mut height = shard_state_header.chunk_height_included();
        let mut chain_update = self.chain_update();
        chain_update.set_state_finalize(shard_id, sync_hash, shard_state_header)?;
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

        Ok(())
    }

    pub fn clear_downloaded_parts(
        &mut self,
        shard_id: ShardId,
        sync_hash: CryptoHash,
        num_parts: u64,
    ) -> Result<(), Error> {
        let mut chain_store_update = self.mut_store().store_update();
        chain_store_update.gc_col_state_parts(sync_hash, shard_id, num_parts)?;
        Ok(chain_store_update.commit()?)
    }

    pub fn catchup_blocks_step(
        &mut self,
        me: &Option<AccountId>,
        sync_hash: &CryptoHash,
        blocks_catch_up_state: &mut BlocksCatchUpState,
        block_catch_up_scheduler: &dyn Fn(BlockCatchUpRequest),
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
                            self.store.get_blocks_to_catchup(&queued_block)?.clone()
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
            let block = self.store.get_block(&pending_block)?.clone();
            let prev_block = self.store.get_block(block.header().prev_hash())?.clone();

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
            block_catch_up_scheduler(BlockCatchUpRequest {
                sync_hash: *sync_hash,
                block_hash: pending_block,
                block_height: block.header().height(),
                work,
            });
        }

        Ok(())
    }

    fn block_catch_up_postprocess(
        &mut self,
        me: &Option<AccountId>,
        block_hash: &CryptoHash,
        results: Vec<Result<ApplyChunkResult, Error>>,
    ) -> Result<(), Error> {
        let block = self.store.get_block(block_hash)?;
        let prev_block = self.store.get_block(block.header().prev_hash())?;
        let mut chain_update = self.chain_update();
        chain_update.apply_chunk_postprocessing(
            &block,
            &prev_block,
            results.into_iter().collect::<Result<Vec<_>, Error>>()?,
        )?;
        chain_update.commit()?;

        let epoch_id = block.header().epoch_id();
        for shard_id in 0..self.epoch_manager.num_shards(epoch_id)? {
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
                let shard_uid = self.epoch_manager.shard_id_to_uid(shard_id, epoch_id)?;
                self.update_flat_storage_for_block(&block, shard_uid)?;
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
        apply_chunks_done_callback: DoneApplyChunkCallback,
        affected_blocks: &[CryptoHash],
    ) -> Result<(), Error> {
        debug!(
            "Finishing catching up blocks after syncing pre {:?}, me: {:?}",
            epoch_first_block, me
        );

        let first_block = self.store.get_block(epoch_first_block)?;

        let mut chain_store_update = ChainStoreUpdate::new(&mut self.store);

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

        chain_store_update.commit()?;

        for hash in affected_blocks.iter() {
            self.check_orphans(
                me,
                *hash,
                block_processing_artifacts,
                apply_chunks_done_callback.clone(),
            );
        }

        Ok(())
    }

    pub fn get_transaction_execution_result(
        &self,
        id: &CryptoHash,
    ) -> Result<Vec<ExecutionOutcomeWithIdView>, Error> {
        Ok(self.store.get_outcomes_by_id(id)?.into_iter().map(Into::into).collect())
    }

    fn get_recursive_transaction_results(
        &self,
        outcomes: &mut Vec<ExecutionOutcomeWithIdView>,
        id: &CryptoHash,
    ) -> Result<(), Error> {
        outcomes.push(ExecutionOutcomeWithIdView::from(self.get_execution_outcome(id)?));
        let outcome_idx = outcomes.len() - 1;
        for idx in 0..outcomes[outcome_idx].outcome.receipt_ids.len() {
            let id = outcomes[outcome_idx].outcome.receipt_ids[idx];
            self.get_recursive_transaction_results(outcomes, &id)?;
        }
        Ok(())
    }

    pub fn get_final_transaction_result(
        &self,
        transaction_hash: &CryptoHash,
    ) -> Result<FinalExecutionOutcomeView, Error> {
        let mut outcomes = Vec::new();
        self.get_recursive_transaction_results(&mut outcomes, transaction_hash)?;
        let mut looking_for_id = *transaction_hash;
        let num_outcomes = outcomes.len();
        let status = outcomes
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
            .expect("results should resolve to a final outcome");
        let receipts_outcome = outcomes.split_off(1);
        let transaction = self.store.get_transaction(transaction_hash)?.ok_or_else(|| {
            Error::DBNotFoundErr(format!("Transaction {} is not found", transaction_hash))
        })?;
        let transaction: SignedTransactionView = SignedTransaction::clone(&transaction).into();
        let transaction_outcome = outcomes.pop().unwrap();
        Ok(FinalExecutionOutcomeView { status, transaction, transaction_outcome, receipts_outcome })
    }

    pub fn get_final_transaction_result_with_receipt(
        &self,
        final_outcome: FinalExecutionOutcomeView,
    ) -> Result<FinalExecutionOutcomeWithReceiptView, Error> {
        let receipt_id_from_transaction =
            final_outcome.transaction_outcome.outcome.receipt_ids.get(0).cloned();
        let is_local_receipt =
            final_outcome.transaction.signer_id == final_outcome.transaction.receiver_id;

        let receipts = final_outcome
            .receipts_outcome
            .iter()
            .filter_map(|outcome| {
                if Some(outcome.id) == receipt_id_from_transaction && is_local_receipt {
                    None
                } else {
                    Some(self.store.get_receipt(&outcome.id).and_then(|r| {
                        r.map(|r| Receipt::clone(&r).into()).ok_or_else(|| {
                            Error::DBNotFoundErr(format!("Receipt {} is not found", outcome.id))
                        })
                    }))
                }
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(FinalExecutionOutcomeWithReceiptView { final_outcome, receipts })
    }

    /// Find a validator to forward transactions to
    pub fn find_chunk_producer_for_forwarding(
        &self,
        epoch_id: &EpochId,
        shard_id: ShardId,
        horizon: BlockHeight,
    ) -> Result<AccountId, Error> {
        let head = self.head()?;
        let target_height = head.height + horizon - 1;
        Ok(self.epoch_manager.get_chunk_producer(epoch_id, target_height, shard_id)?)
    }

    /// Find a validator that is responsible for a given shard to forward requests to
    pub fn find_validator_for_forwarding(&self, shard_id: ShardId) -> Result<AccountId, Error> {
        let head = self.head()?;
        let epoch_id = self.epoch_manager.get_epoch_id_from_prev_block(&head.last_block_hash)?;
        self.find_chunk_producer_for_forwarding(&epoch_id, shard_id, TX_ROUTING_HEIGHT_HORIZON)
    }

    pub fn check_blocks_final_and_canonical(
        &self,
        block_headers: &[&BlockHeader],
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
            prev_block_header: prev_block.header().try_to_vec()?,
            block_header: block.header().try_to_vec()?,
            prev_merkle_proof: prev_merkle_proofs[chunk_shard_id as usize].clone(),
            merkle_proof: merkle_proofs[chunk_shard_id as usize].clone(),
            prev_chunk,
            chunk_header: chunk_header.clone(),
            partial_state: PartialState::TrieValues(vec![]),
        })
    }

    fn get_split_state_roots(
        &self,
        block: &Block,
        shard_id: ShardId,
    ) -> Result<HashMap<ShardUId, StateRoot>, Error> {
        let next_shard_layout =
            self.epoch_manager.get_shard_layout(block.header().next_epoch_id())?;
        let new_shards = next_shard_layout.get_split_shard_uids(shard_id).unwrap_or_else(|| {
            panic!("shard layout must contain maps of all shards to its split shards {}", shard_id)
        });
        new_shards
            .iter()
            .map(|shard_uid| {
                self.get_chunk_extra(block.header().prev_hash(), shard_uid)
                    .map(|chunk_extra| (*shard_uid, *chunk_extra.state_root()))
            })
            .collect()
    }

    /// Creates jobs that would apply chunks
    fn apply_chunks_preprocessing(
        &self,
        me: &Option<AccountId>,
        block: &Block,
        prev_block: &Block,
        incoming_receipts: &HashMap<ShardId, Vec<ReceiptProof>>,
        mode: ApplyChunksMode,
        mut state_patch: SandboxStatePatch,
        invalid_chunks: &mut Vec<ShardChunkHeader>,
    ) -> Result<
        Vec<Box<dyn FnOnce(&Span) -> Result<ApplyChunkResult, Error> + Send + 'static>>,
        Error,
    > {
        let _span = tracing::debug_span!(target: "chain", "apply_chunks_preprocessing").entered();
        #[cfg(not(feature = "mock_node"))]
        let protocol_version =
            self.epoch_manager.get_epoch_protocol_version(block.header().epoch_id())?;

        let prev_hash = block.header().prev_hash();
        let will_shard_layout_change = self.epoch_manager.will_shard_layout_change(prev_hash)?;
        let prev_chunk_headers =
            Chain::get_prev_chunk_headers(self.epoch_manager.as_ref(), prev_block)?;
        let mut process_one_chunk = |shard_id: usize,
                                     chunk_header: &ShardChunkHeader,
                                     prev_chunk_header: &ShardChunkHeader|
         -> Result<
            Option<Box<dyn FnOnce(&Span) -> Result<ApplyChunkResult, Error> + Send + 'static>>,
            Error,
        > {
            // XXX: This is a bit questionable -- sandbox state patching works
            // only for a single shard. This so far has been enough.
            let state_patch = state_patch.take();

            let shard_id = shard_id as ShardId;
            let cares_about_shard_this_epoch =
                self.shard_tracker.care_about_shard(me.as_ref(), prev_hash, shard_id, true);
            let cares_about_shard_next_epoch =
                self.shard_tracker.will_care_about_shard(me.as_ref(), prev_hash, shard_id, true);
            // We want to guarantee that transactions are only applied once for each shard, even
            // though apply_chunks may be called twice, once with ApplyChunksMode::NotCaughtUp
            // once with ApplyChunksMode::CatchingUp
            // Note that this variable does not guard whether we split states or not, see the comments
            // before `need_to_split_state`
            let should_apply_transactions = match mode {
                // next epoch's shard states are not ready, only update this epoch's shards
                ApplyChunksMode::NotCaughtUp => cares_about_shard_this_epoch,
                // update both this epoch and next epoch
                ApplyChunksMode::IsCaughtUp => {
                    cares_about_shard_this_epoch || cares_about_shard_next_epoch
                }
                // catching up next epoch's shard states, do not update this epoch's shard state
                // since it has already been updated through ApplyChunksMode::NotCaughtUp
                ApplyChunksMode::CatchingUp => {
                    !cares_about_shard_this_epoch && cares_about_shard_next_epoch
                }
            };
            let need_to_split_states = will_shard_layout_change && cares_about_shard_next_epoch;
            // We can only split states when states are ready, i.e., mode != ApplyChunksMode::NotCaughtUp
            // 1) if should_apply_transactions == true && split_state_roots.is_some(),
            //     that means split states are ready.
            //    `apply_split_state_changes` will apply updates to split_states
            // 2) if should_apply_transactions == true && split_state_roots.is_none(),
            //     that means split states are not ready yet.
            //    `apply_split_state_changes` will return `state_changes_for_split_states`,
            //     which will be stored to the database in `process_apply_chunks`
            // 3) if should_apply_transactions == false && split_state_roots.is_some()
            //    This implies mode == CatchingUp and cares_about_shard_this_epoch == true,
            //    otherwise should_apply_transactions will be true
            //    That means transactions have already been applied last time when apply_chunks are
            //    called with mode NotCaughtUp, therefore `state_changes_for_split_states` have been
            //    stored in the database. Then we can safely read that and apply that to the split
            //    states
            let split_state_roots = if need_to_split_states {
                match mode {
                    ApplyChunksMode::IsCaughtUp | ApplyChunksMode::CatchingUp => {
                        Some(self.get_split_state_roots(block, shard_id)?)
                    }
                    ApplyChunksMode::NotCaughtUp => None,
                }
            } else {
                None
            };
            let shard_uid =
                self.epoch_manager.shard_id_to_uid(shard_id, block.header().epoch_id())?;
            let is_new_chunk = chunk_header.height_included() == block.header().height();
            let epoch_manager = self.epoch_manager.clone();
            let runtime = self.runtime_adapter.clone();
            if should_apply_transactions {
                if is_new_chunk {
                    let prev_chunk_height_included = prev_chunk_header.height_included();
                    // Validate state root.
                    let prev_chunk_extra = self.get_chunk_extra(prev_hash, &shard_uid)?;

                    // Validate that all next chunk information matches previous chunk extra.
                    validate_chunk_with_chunk_extra(
                        // It's safe here to use ChainStore instead of ChainStoreUpdate
                        // because we're asking prev_chunk_header for already committed block
                        self.store(),
                        self.epoch_manager.as_ref(),
                        block.header().prev_hash(),
                        &prev_chunk_extra,
                        prev_chunk_height_included,
                        chunk_header,
                    )
                    .map_err(|e| {
                        warn!(target: "chain", "Failed to validate chunk extra: {:?}.\n\
                                                block prev_hash: {}\n\
                                                block hash: {}\n\
                                                shard_id: {}\n\
                                                prev_chunk_height_included: {}\n\
                                                prev_chunk_extra: {:#?}\n\
                                                chunk_header: {:#?}", e,block.header().prev_hash(),block.header().hash(),shard_id,prev_chunk_height_included,prev_chunk_extra,chunk_header);
                        byzantine_assert!(false);
                        match self.create_chunk_state_challenge(prev_block, block, chunk_header) {
                            Ok(chunk_state) => {
                                Error::InvalidChunkState(Box::new(chunk_state))
                            }
                            Err(err) => err,
                        }
                    })?;
                    // we can't use hash from the current block here yet because the incoming receipts
                    // for this block is not stored yet
                    let mut receipts = collect_receipts(incoming_receipts.get(&shard_id).unwrap());
                    receipts.extend(collect_receipts_from_response(
                        &self.store().get_incoming_receipts_for_shard(
                            shard_id,
                            *prev_hash,
                            prev_chunk_height_included,
                        )?,
                    ));
                    let chunk = self.get_chunk_clone_from_header(&chunk_header.clone())?;

                    let transactions = chunk.transactions();
                    if !validate_transactions_order(transactions) {
                        let merkle_paths =
                            Block::compute_chunk_headers_root(block.chunks().iter()).1;
                        let chunk_proof = ChunkProofs {
                            block_header: block.header().try_to_vec().expect("Failed to serialize"),
                            merkle_proof: merkle_paths[shard_id as usize].clone(),
                            chunk: MaybeEncodedShardChunk::Decoded(chunk),
                        };
                        return Err(Error::InvalidChunkProofs(Box::new(chunk_proof)));
                    }

                    // if we are running mock_node, ignore this check because
                    // this check may require old block headers, which may not exist in storage
                    // of the client in the mock network
                    #[cfg(not(feature = "mock_node"))]
                    if checked_feature!("stable", AccessKeyNonceRange, protocol_version) {
                        let transaction_validity_period = self.transaction_validity_period;
                        for transaction in transactions {
                            self.store()
                                .check_transaction_validity_period(
                                    prev_block.header(),
                                    &transaction.transaction.block_hash,
                                    transaction_validity_period,
                                )
                                .map_err(|_| {
                                    tracing::warn!("Invalid Transactions for mock node");
                                    Error::from(Error::InvalidTransactions)
                                })?;
                        }
                    };

                    let chunk_inner = chunk.cloned_header().take_inner();
                    let gas_limit = chunk_inner.gas_limit();

                    // This variable is responsible for checking to which block we can apply receipts previously lost in apply_chunks
                    // (see https://github.com/near/nearcore/pull/4248/)
                    // We take the first block with existing chunk in the first epoch in which protocol feature
                    // RestoreReceiptsAfterFixApplyChunks was enabled, and put the restored receipts there.
                    let is_first_block_with_chunk_of_version =
                        check_if_block_is_first_with_chunk_of_version(
                            self.store(),
                            epoch_manager.as_ref(),
                            prev_block.hash(),
                            shard_id,
                        )?;

                    let block_hash = *block.hash();
                    let challenges_result = block.header().challenges_result().clone();
                    let block_timestamp = block.header().raw_timestamp();
                    let gas_price = prev_block.header().gas_price();
                    let random_seed = *block.header().random_value();
                    let height = chunk_header.height_included();
                    let prev_block_hash = *chunk_header.prev_block_hash();

                    Ok(Some(Box::new(move |parent_span| -> Result<ApplyChunkResult, Error> {
                        let _span = tracing::debug_span!(
                            target: "chain",
                            parent: parent_span,
                            "new_chunk",
                            shard_id)
                        .entered();
                        let _timer = CryptoHashTimer::new(chunk.chunk_hash().0);
                        match runtime.apply_transactions(
                            shard_id,
                            chunk_inner.prev_state_root(),
                            height,
                            block_timestamp,
                            &prev_block_hash,
                            &block_hash,
                            &receipts,
                            chunk.transactions(),
                            chunk_inner.validator_proposals(),
                            gas_price,
                            gas_limit,
                            &challenges_result,
                            random_seed,
                            true,
                            is_first_block_with_chunk_of_version,
                            state_patch,
                            true,
                        ) {
                            Ok(apply_result) => {
                                let apply_split_result_or_state_changes =
                                    if will_shard_layout_change {
                                        Some(ChainUpdate::apply_split_state_changes(
                                            epoch_manager.as_ref(),
                                            runtime.as_ref(),
                                            &block_hash,
                                            &prev_block_hash,
                                            &apply_result,
                                            split_state_roots,
                                        )?)
                                    } else {
                                        None
                                    };
                                Ok(ApplyChunkResult::SameHeight(SameHeightResult {
                                    gas_limit,
                                    shard_uid,
                                    apply_result,
                                    apply_split_result_or_state_changes,
                                }))
                            }
                            Err(err) => Err(err),
                        }
                    })))
                } else {
                    let new_extra = self.get_chunk_extra(prev_block.hash(), &shard_uid)?;

                    let block_hash = *block.hash();
                    let challenges_result = block.header().challenges_result().clone();
                    let block_timestamp = block.header().raw_timestamp();
                    let gas_price = block.header().gas_price();
                    let random_seed = *block.header().random_value();
                    let height = block.header().height();
                    let prev_block_hash = *prev_block.hash();

                    Ok(Some(Box::new(move |parent_span| -> Result<ApplyChunkResult, Error> {
                        let _span = tracing::debug_span!(
                            target: "chain",
                            parent: parent_span,
                            "existing_chunk",
                            shard_id)
                        .entered();
                        match runtime.apply_transactions(
                            shard_id,
                            new_extra.state_root(),
                            height,
                            block_timestamp,
                            &prev_block_hash,
                            &block_hash,
                            &[],
                            &[],
                            new_extra.validator_proposals(),
                            gas_price,
                            new_extra.gas_limit(),
                            &challenges_result,
                            random_seed,
                            false,
                            false,
                            state_patch,
                            true,
                        ) {
                            Ok(apply_result) => {
                                let apply_split_result_or_state_changes =
                                    if will_shard_layout_change {
                                        Some(ChainUpdate::apply_split_state_changes(
                                            epoch_manager.as_ref(),
                                            runtime.as_ref(),
                                            &block_hash,
                                            &prev_block_hash,
                                            &apply_result,
                                            split_state_roots,
                                        )?)
                                    } else {
                                        None
                                    };
                                Ok(ApplyChunkResult::DifferentHeight(DifferentHeightResult {
                                    shard_uid,
                                    apply_result,
                                    apply_split_result_or_state_changes,
                                }))
                            }
                            Err(err) => Err(err),
                        }
                    })))
                }
            } else if let Some(split_state_roots) = split_state_roots {
                // case 3)
                assert!(mode == ApplyChunksMode::CatchingUp && cares_about_shard_this_epoch);
                // Split state are ready. Read the state changes from the database and apply them
                // to the split states.
                let next_epoch_shard_layout =
                    epoch_manager.get_shard_layout(block.header().next_epoch_id())?;
                let state_changes =
                    self.store().get_state_changes_for_split_states(block.hash(), shard_id)?;
                let block_hash = *block.hash();
                Ok(Some(Box::new(move |parent_span| -> Result<ApplyChunkResult, Error> {
                    let _span = tracing::debug_span!(
                        target: "chain",
                        parent: parent_span,
                        "split_state",
                        shard_id,
                        ?shard_uid)
                    .entered();
                    Ok(ApplyChunkResult::SplitState(SplitStateResult {
                        shard_uid,
                        results: runtime.apply_update_to_split_states(
                            &block_hash,
                            split_state_roots,
                            &next_epoch_shard_layout,
                            state_changes,
                        )?,
                    }))
                })))
            } else {
                Ok(None)
            }
        };
        block
            .chunks()
            .iter()
            .zip(prev_chunk_headers.iter())
            .enumerate()
            .filter_map(|(shard_id, (chunk_header, prev_chunk_header))| {
                match process_one_chunk(shard_id, chunk_header, prev_chunk_header) {
                    Ok(Some(processor)) => Some(Ok(processor)),
                    Ok(None) => None,
                    Err(err) => {
                        if err.is_bad_data() {
                            invalid_chunks.push(chunk_header.clone());
                        }
                        Some(Err(err))
                    }
                }
            })
            .collect()
    }

    /// Checks if the time has come to make a state snapshot for testing purposes.
    /// If a state snapshot was requested for another reason, then resets the countdown.
    fn need_test_state_snapshot(&mut self, need_state_snapshot: bool) -> bool {
        let mut res = false;
        if let Some(helper) = &mut self.state_snapshot_helper {
            if let Some((countdown, frequency)) = helper.test_snapshot_countdown_and_frequency {
                helper.test_snapshot_countdown_and_frequency = if need_state_snapshot {
                    Some((frequency, frequency))
                } else if countdown == 0 {
                    res = true;
                    Some((frequency, frequency))
                } else {
                    Some((countdown - 1, frequency))
                };
            }
        }
        res
    }

    /// Makes a state snapshot.
    /// If there was already a state snapshot, deletes that first.
    fn maybe_start_state_snapshot(&self, need_state_snapshot: bool) -> Result<(), Error> {
        if need_state_snapshot {
            if let Some(helper) = &self.state_snapshot_helper {
                let head = self.head()?;
                let epoch_id = self.epoch_manager.get_epoch_id(&head.prev_block_hash)?;
                let shard_layout = self.epoch_manager.get_shard_layout(&epoch_id)?;
                (helper.make_snapshot_callback)(head.prev_block_hash, shard_layout.get_shard_uids())
            }
        }
        Ok(())
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
            &mut self.store,
            self.epoch_manager.clone(),
            self.shard_tracker.clone(),
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
                    Some(self.store().get_block_hash_from_ordinal(index)?)
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
                            .store()
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
                    Some(self.store().get_block_hash_from_ordinal(index)?)
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
        let leaf_index = self.store().get_block_merkle_tree(block_hash)?.size();
        let tree_size = self.store().get_block_merkle_tree(head_block_hash)?.size();
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
        self.store.head()
    }

    /// Gets chain tail height
    #[inline]
    pub fn tail(&self) -> Result<BlockHeight, Error> {
        self.store.tail()
    }

    /// Gets chain header head.
    #[inline]
    pub fn header_head(&self) -> Result<Tip, Error> {
        self.store.header_head()
    }

    /// Header of the block at the head of the block chain (not the same thing as header_head).
    #[inline]
    pub fn head_header(&self) -> Result<BlockHeader, Error> {
        self.store.head_header()
    }

    /// Get final head of the chain.
    #[inline]
    pub fn final_head(&self) -> Result<Tip, Error> {
        self.store.final_head()
    }

    /// Gets a block by hash.
    #[inline]
    pub fn get_block(&self, hash: &CryptoHash) -> Result<Block, Error> {
        self.store.get_block(hash)
    }

    /// Gets the block at chain head
    pub fn get_head_block(&self) -> Result<Block, Error> {
        let tip = self.head()?;
        self.store.get_block(&tip.last_block_hash)
    }

    /// Gets a chunk from hash.
    #[inline]
    pub fn get_chunk(&self, chunk_hash: &ChunkHash) -> Result<Arc<ShardChunk>, Error> {
        self.store.get_chunk(chunk_hash)
    }

    /// Gets a chunk from header.
    #[inline]
    pub fn get_chunk_clone_from_header(
        &self,
        header: &ShardChunkHeader,
    ) -> Result<ShardChunk, Error> {
        self.store.get_chunk_clone_from_header(header)
    }

    /// Gets a block from the current chain by height.
    #[inline]
    pub fn get_block_by_height(&self, height: BlockHeight) -> Result<Block, Error> {
        let hash = self.store.get_block_hash_by_height(height)?;
        self.store.get_block(&hash)
    }

    /// Gets block hash from the current chain by height.
    #[inline]
    pub fn get_block_hash_by_height(&self, height: BlockHeight) -> Result<CryptoHash, Error> {
        self.store.get_block_hash_by_height(height)
    }

    /// Gets a block header by hash.
    #[inline]
    pub fn get_block_header(&self, hash: &CryptoHash) -> Result<BlockHeader, Error> {
        self.store.get_block_header(hash)
    }

    /// Returns block header from the canonical chain for given height if present.
    #[inline]
    pub fn get_block_header_by_height(&self, height: BlockHeight) -> Result<BlockHeader, Error> {
        self.store.get_block_header_by_height(height)
    }

    /// Returns block header from the current chain defined by `sync_hash` for given height if present.
    #[inline]
    pub fn get_block_header_on_chain_by_height(
        &self,
        sync_hash: &CryptoHash,
        height: BlockHeight,
    ) -> Result<BlockHeader, Error> {
        self.store.get_block_header_on_chain_by_height(sync_hash, height)
    }

    /// Get previous block header.
    #[inline]
    pub fn get_previous_header(&self, header: &BlockHeader) -> Result<BlockHeader, Error> {
        self.store.get_previous_header(header).map_err(|e| match e {
            Error::DBNotFoundErr(_) => Error::Orphan,
            other => other,
        })
    }

    /// Returns hash of the first available block after genesis.
    pub fn get_earliest_block_hash(&self) -> Result<Option<CryptoHash>, Error> {
        self.store.get_earliest_block_hash()
    }

    /// Check if block exists.
    #[inline]
    pub fn block_exists(&self, hash: &CryptoHash) -> Result<bool, Error> {
        self.store.block_exists(hash)
    }

    /// Get block extra that was computer after applying previous block.
    #[inline]
    pub fn get_block_extra(&self, block_hash: &CryptoHash) -> Result<Arc<BlockExtra>, Error> {
        self.store.get_block_extra(block_hash)
    }

    /// Get chunk extra that was computed after applying chunk with given hash.
    #[inline]
    pub fn get_chunk_extra(
        &self,
        block_hash: &CryptoHash,
        shard_uid: &ShardUId,
    ) -> Result<Arc<ChunkExtra>, Error> {
        self.store.get_chunk_extra(block_hash, shard_uid)
    }

    /// Get destination shard id for a given receipt id.
    #[inline]
    pub fn get_shard_id_for_receipt_id(&self, receipt_id: &CryptoHash) -> Result<ShardId, Error> {
        self.store.get_shard_id_for_receipt_id(receipt_id)
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
        let mut epoch_id = self.get_block_header(&block_hash)?.epoch_id().clone();
        let mut shard_layout = self.epoch_manager.get_shard_layout(&epoch_id)?;
        // this corrects all the shard where the original shard will split to if sharding changes
        let mut shard_ids = vec![shard_id];

        while let Ok(next_block_hash) = self.store.get_next_block_hash(&block_hash) {
            let next_epoch_id = self.get_block_header(&next_block_hash)?.epoch_id().clone();
            if next_epoch_id != epoch_id {
                let next_shard_layout = self.epoch_manager.get_shard_layout(&next_epoch_id)?;
                if next_shard_layout != shard_layout {
                    shard_ids = shard_ids
                        .into_iter()
                        .flat_map(|id| {
                            next_shard_layout.get_split_shard_ids(id).unwrap_or_else(|| {
                                panic!("invalid shard layout {:?} because it does not contain split shards for parent shard {}", next_shard_layout, id)
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
                if chunks[shard_id as usize].height_included() == block.header().height() {
                    return Ok(Some((block_hash, shard_id)));
                }
            }
        }

        Ok(None)
    }

    /// Returns underlying ChainStore.
    #[inline]
    pub fn store(&self) -> &ChainStore {
        &self.store
    }

    /// Returns mutable ChainStore.
    #[inline]
    pub fn mut_store(&mut self) -> &mut ChainStore {
        &mut self.store
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
    pub fn orphans_len(&self) -> usize {
        self.orphans.len()
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

    /// Returns number of evicted orphans.
    #[inline]
    pub fn orphans_evicted_len(&self) -> usize {
        self.orphans.len_evicted()
    }

    /// Check if hash is for a known orphan.
    #[inline]
    pub fn is_orphan(&self, hash: &CryptoHash) -> bool {
        self.orphans.contains(hash)
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
        self.store.is_height_processed(height)
    }

    #[inline]
    pub fn is_block_invalid(&self, hash: &CryptoHash) -> bool {
        self.invalid_blocks.contains(hash)
    }

    /// Check if can sync with sync_hash
    pub fn check_sync_hash_validity(&self, sync_hash: &CryptoHash) -> Result<bool, Error> {
        let head = self.head()?;
        // It's important to check that Block exists because we will sync with it.
        // Do not replace with `get_block_header`.
        let sync_block = self.get_block(sync_hash)?;
        // The Epoch of sync_hash may be either the current one or the previous one
        if head.epoch_id == *sync_block.header().epoch_id()
            || head.epoch_id == *sync_block.header().next_epoch_id()
        {
            let prev_hash = *sync_block.header().prev_hash();
            // If sync_hash is not on the Epoch boundary, it's malicious behavior
            Ok(self.epoch_manager.is_next_block_epoch_start(&prev_hash)?)
        } else {
            Ok(false) // invalid Epoch of sync_hash, possible malicious behavior
        }
    }

    /// Get transaction result for given hash of transaction or receipt id on the canonical chain
    pub fn get_execution_outcome(
        &self,
        id: &CryptoHash,
    ) -> Result<ExecutionOutcomeWithIdAndProof, Error> {
        let outcomes = self.store.get_outcomes_by_id(id)?;
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

    /// Returns a vector of chunk headers, each of which corresponds to the previous chunk of
    /// a chunk in the block after `prev_block`
    /// This function is important when the block after `prev_block` has different number of chunks
    /// from `prev_block`.
    /// In block production and processing, often we need to get the previous chunks of chunks
    /// in the current block, this function provides a way to do so while handling sharding changes
    /// correctly.
    /// For example, if `prev_block` has two shards 0, 1 and the block after `prev_block` will have
    /// 4 shards 0, 1, 2, 3, 0 and 1 split from shard 0 and 2 and 3 split from shard 1.
    /// `get_prev_chunks(runtime_adapter, prev_block)` will return
    /// `[prev_block.chunks()[0], prev_block.chunks()[0], prev_block.chunks()[1], prev_block.chunks()[1]]`
    pub fn get_prev_chunk_headers(
        epoch_manager: &dyn EpochManagerAdapter,
        prev_block: &Block,
    ) -> Result<Vec<ShardChunkHeader>, Error> {
        let epoch_id = epoch_manager.get_epoch_id_from_prev_block(prev_block.hash())?;
        let num_shards = epoch_manager.num_shards(&epoch_id)?;
        let prev_shard_ids =
            epoch_manager.get_prev_shard_ids(prev_block.hash(), (0..num_shards).collect())?;
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
        let prev_shard_id = epoch_manager.get_prev_shard_ids(prev_block.hash(), vec![shard_id])?[0];
        Ok(prev_block.chunks().get(prev_shard_id as usize).unwrap().clone())
    }

    pub fn group_receipts_by_shard(
        receipts: Vec<Receipt>,
        shard_layout: &ShardLayout,
    ) -> HashMap<ShardId, Vec<Receipt>> {
        let mut result = HashMap::with_capacity(shard_layout.num_shards() as usize);
        for receipt in receipts {
            let shard_id = account_id_to_shard_id(&receipt.receiver_id, shard_layout);
            let entry = result.entry(shard_id).or_insert_with(Vec::new);
            entry.push(receipt)
        }
        result
    }

    pub fn build_receipts_hashes(
        receipts: &[Receipt],
        shard_layout: &ShardLayout,
    ) -> Vec<CryptoHash> {
        if shard_layout.num_shards() == 1 {
            return vec![CryptoHash::hash_borsh(ReceiptList(0, receipts))];
        }
        let mut account_id_to_shard_id_map = HashMap::new();
        let mut shard_receipts: Vec<_> =
            (0..shard_layout.num_shards()).map(|i| (i, Vec::new())).collect();
        for receipt in receipts.iter() {
            let shard_id = match account_id_to_shard_id_map.get(&receipt.receiver_id) {
                Some(id) => *id,
                None => {
                    let id = account_id_to_shard_id(&receipt.receiver_id, shard_layout);
                    account_id_to_shard_id_map.insert(receipt.receiver_id.clone(), id);
                    id
                }
            };
            shard_receipts[shard_id as usize].1.push(receipt);
        }
        shard_receipts
            .into_iter()
            .map(|(i, rs)| {
                let bytes = (i, rs).try_to_vec().unwrap();
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

/// Chain update helper, contains information that is needed to process block
/// and decide to accept it or reject it.
/// If rejected nothing will be updated in underlying storage.
/// Safe to stop process mid way (Ctrl+C or crash).
pub struct ChainUpdate<'a> {
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    shard_tracker: ShardTracker,
    runtime_adapter: Arc<dyn RuntimeAdapter>,
    chain_store_update: ChainStoreUpdate<'a>,
    doomslug_threshold_mode: DoomslugThresholdMode,
    #[allow(unused)]
    transaction_validity_period: BlockHeightDelta,
}

pub struct SameHeightResult {
    shard_uid: ShardUId,
    gas_limit: Gas,
    apply_result: ApplyTransactionResult,
    apply_split_result_or_state_changes: Option<ApplySplitStateResultOrStateChanges>,
}

pub struct DifferentHeightResult {
    shard_uid: ShardUId,
    apply_result: ApplyTransactionResult,
    apply_split_result_or_state_changes: Option<ApplySplitStateResultOrStateChanges>,
}

pub struct SplitStateResult {
    // parent shard of the split states
    shard_uid: ShardUId,
    results: Vec<ApplySplitStateResult>,
}

pub enum ApplyChunkResult {
    SameHeight(SameHeightResult),
    DifferentHeight(DifferentHeightResult),
    SplitState(SplitStateResult),
}

impl<'a> ChainUpdate<'a> {
    pub fn new(
        store: &'a mut ChainStore,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        shard_tracker: ShardTracker,
        runtime_adapter: Arc<dyn RuntimeAdapter>,
        doomslug_threshold_mode: DoomslugThresholdMode,
        transaction_validity_period: BlockHeightDelta,
    ) -> Self {
        let chain_store_update: ChainStoreUpdate<'_> = store.store_update();
        Self::new_impl(
            epoch_manager,
            shard_tracker,
            runtime_adapter,
            doomslug_threshold_mode,
            transaction_validity_period,
            chain_store_update,
        )
    }

    fn new_impl(
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        shard_tracker: ShardTracker,
        runtime_adapter: Arc<dyn RuntimeAdapter>,
        doomslug_threshold_mode: DoomslugThresholdMode,
        transaction_validity_period: BlockHeightDelta,
        chain_store_update: ChainStoreUpdate<'a>,
    ) -> Self {
        ChainUpdate {
            epoch_manager,
            shard_tracker,
            runtime_adapter,
            chain_store_update,
            doomslug_threshold_mode,
            transaction_validity_period,
        }
    }

    /// Commit changes to the chain into the database.
    pub fn commit(self) -> Result<(), Error> {
        self.chain_store_update.commit()
    }

    /// For all the outgoing receipts generated in block `hash` at the shards we are tracking
    /// in this epoch,
    /// save a mapping from receipt ids to the destination shard ids that the receipt will be sent
    /// to in the next block.
    /// Note that this function should be called after `save_block` is called on this block because
    /// it requires that the block info is available in EpochManager, otherwise it will return an
    /// error.
    pub fn save_receipt_id_to_shard_id_for_block(
        &mut self,
        me: &Option<AccountId>,
        hash: &CryptoHash,
        prev_hash: &CryptoHash,
        num_shards: NumShards,
    ) -> Result<(), Error> {
        for shard_id in 0..num_shards {
            if self.shard_tracker.care_about_shard(
                me.as_ref(),
                &prev_hash,
                shard_id as ShardId,
                true,
            ) {
                let receipt_id_to_shard_id: HashMap<_, _> = {
                    // it can be empty if there is no new chunk for this shard
                    if let Ok(outgoing_receipts) =
                        self.chain_store_update.get_outgoing_receipts(hash, shard_id)
                    {
                        let shard_layout =
                            self.epoch_manager.get_shard_layout_from_prev_block(hash)?;
                        outgoing_receipts
                            .iter()
                            .map(|receipt| {
                                (
                                    receipt.receipt_id,
                                    account_id_to_shard_id(&receipt.receiver_id, &shard_layout),
                                )
                            })
                            .collect()
                    } else {
                        HashMap::new()
                    }
                };
                for (receipt_id, shard_id) in receipt_id_to_shard_id {
                    self.chain_store_update.save_receipt_id_to_shard_id(receipt_id, shard_id);
                }
            }
        }

        Ok(())
    }

    fn apply_chunk_postprocessing(
        &mut self,
        block: &Block,
        prev_block: &Block,
        apply_results: Vec<ApplyChunkResult>,
    ) -> Result<(), Error> {
        let _span = tracing::debug_span!(target: "chain", "apply_chunk_postprocessing").entered();
        for result in apply_results {
            self.process_apply_chunk_result(
                result,
                *block.hash(),
                block.header().height(),
                *prev_block.hash(),
            )?
        }
        Ok(())
    }

    /// Process ApplyTransactionResult to apply changes to split states
    /// When shards will change next epoch,
    ///    if `split_state_roots` is not None, that means states for the split shards are ready
    ///    this function updates these states and return apply results for these states
    ///    otherwise, this function returns state changes needed to be applied to split
    ///    states. These state changes will be stored in the database by `process_split_state`
    fn apply_split_state_changes(
        epoch_manager: &dyn EpochManagerAdapter,
        runtime_adapter: &dyn RuntimeAdapter,
        block_hash: &CryptoHash,
        prev_block_hash: &CryptoHash,
        apply_result: &ApplyTransactionResult,
        split_state_roots: Option<HashMap<ShardUId, StateRoot>>,
    ) -> Result<ApplySplitStateResultOrStateChanges, Error> {
        let state_changes = StateChangesForSplitStates::from_raw_state_changes(
            apply_result.trie_changes.state_changes(),
            apply_result.processed_delayed_receipts.clone(),
        );
        let next_epoch_shard_layout = {
            let next_epoch_id = epoch_manager.get_next_epoch_id_from_prev_block(prev_block_hash)?;
            epoch_manager.get_shard_layout(&next_epoch_id)?
        };
        // split states are ready, apply update to them now
        if let Some(state_roots) = split_state_roots {
            let split_state_results = runtime_adapter.apply_update_to_split_states(
                block_hash,
                state_roots,
                &next_epoch_shard_layout,
                state_changes,
            )?;
            Ok(ApplySplitStateResultOrStateChanges::ApplySplitStateResults(split_state_results))
        } else {
            // split states are not ready yet, store state changes in consolidated_state_changes
            Ok(ApplySplitStateResultOrStateChanges::StateChangesForSplitStates(state_changes))
        }
    }

    /// Postprocess split state results or state changes, do the necessary update on chain
    /// for split state results: store the chunk extras and trie changes for the split states
    /// for state changes, store the state changes for splitting states
    fn process_split_state(
        &mut self,
        block_hash: &CryptoHash,
        prev_block_hash: &CryptoHash,
        shard_uid: &ShardUId,
        apply_results_or_state_changes: ApplySplitStateResultOrStateChanges,
    ) -> Result<(), Error> {
        match apply_results_or_state_changes {
            ApplySplitStateResultOrStateChanges::ApplySplitStateResults(results) => {
                // Split validator_proposals, gas_burnt, balance_burnt to each split shard
                // and store the chunk extra for split shards
                // Note that here we do not split outcomes by the new shard layout, we simply store
                // the outcome_root from the parent shard. This is because outcome proofs are
                // generated per shard using the old shard layout and stored in the database.
                // For these proofs to work, we must store the outcome root per shard
                // using the old shard layout instead of the new shard layout
                let chunk_extra = self.chain_store_update.get_chunk_extra(block_hash, shard_uid)?;
                let next_epoch_shard_layout = {
                    let epoch_id =
                        self.epoch_manager.get_next_epoch_id_from_prev_block(prev_block_hash)?;
                    self.epoch_manager.get_shard_layout(&epoch_id)?
                };

                let mut validator_proposals_by_shard: HashMap<_, Vec<_>> = HashMap::new();
                for validator_proposal in chunk_extra.validator_proposals() {
                    let shard_id = account_id_to_shard_uid(
                        validator_proposal.account_id(),
                        &next_epoch_shard_layout,
                    );
                    validator_proposals_by_shard
                        .entry(shard_id)
                        .or_default()
                        .push(validator_proposal);
                }

                let num_split_shards = next_epoch_shard_layout
                    .get_split_shard_uids(shard_uid.shard_id())
                    .unwrap_or_else(|| panic!("invalid shard layout {:?}", next_epoch_shard_layout))
                    .len() as NumShards;
                let total_gas_used = chunk_extra.gas_used();
                let total_balance_burnt = chunk_extra.balance_burnt();
                let gas_res = total_gas_used % num_split_shards;
                let gas_split = total_gas_used / num_split_shards;
                let balance_res = (total_balance_burnt % num_split_shards as u128) as NumShards;
                let balance_split = total_balance_burnt / (num_split_shards as u128);
                let gas_limit = chunk_extra.gas_limit();
                let outcome_root = *chunk_extra.outcome_root();

                let mut sum_gas_used = 0;
                let mut sum_balance_burnt = 0;
                for result in results {
                    let shard_id = result.shard_uid.shard_id();
                    let gas_burnt = gas_split + if shard_id < gas_res { 1 } else { 0 };
                    let balance_burnt = balance_split + if shard_id < balance_res { 1 } else { 0 };
                    let new_chunk_extra = ChunkExtra::new(
                        &result.new_root,
                        outcome_root,
                        validator_proposals_by_shard.remove(&result.shard_uid).unwrap_or_default(),
                        gas_burnt,
                        gas_limit,
                        balance_burnt,
                    );
                    sum_gas_used += gas_burnt;
                    sum_balance_burnt += balance_burnt;

                    self.chain_store_update.save_chunk_extra(
                        block_hash,
                        &result.shard_uid,
                        new_chunk_extra,
                    );
                    self.chain_store_update.save_trie_changes(result.trie_changes);
                }
                assert_eq!(sum_gas_used, total_gas_used);
                assert_eq!(sum_balance_burnt, total_balance_burnt);
            }
            ApplySplitStateResultOrStateChanges::StateChangesForSplitStates(state_changes) => {
                self.chain_store_update.add_state_changes_for_split_states(
                    *block_hash,
                    shard_uid.shard_id(),
                    state_changes,
                );
            }
        }
        Ok(())
    }

    fn save_flat_state_changes(
        &mut self,
        block_hash: CryptoHash,
        prev_hash: CryptoHash,
        height: BlockHeight,
        shard_uid: ShardUId,
        trie_changes: &WrappedTrieChanges,
    ) -> Result<(), Error> {
        let delta = FlatStateDelta {
            changes: FlatStateChanges::from_state_changes(&trie_changes.state_changes()),
            metadata: FlatStateDeltaMetadata {
                block: near_store::flat::BlockInfo { hash: block_hash, height, prev_hash },
            },
        };

        if let Some(chain_flat_storage) = self
            .runtime_adapter
            .get_flat_storage_manager()
            .and_then(|manager| manager.get_flat_storage_for_shard(shard_uid))
        {
            // If flat storage exists, we add a block to it.
            let store_update =
                chain_flat_storage.add_delta(delta).map_err(|e| StorageError::from(e))?;
            self.chain_store_update.merge(store_update);
        } else {
            let shard_id = shard_uid.shard_id();
            // Otherwise, save delta to disk so it will be used for flat storage creation later.
            debug!(target: "chain", %shard_id, "Add delta for flat storage creation");
            let mut store_update = self.chain_store_update.store().store_update();
            store_helper::set_delta(&mut store_update, shard_uid, &delta);
            self.chain_store_update.merge(store_update);
        }

        Ok(())
    }

    /// Processed results of applying chunk
    fn process_apply_chunk_result(
        &mut self,
        result: ApplyChunkResult,
        block_hash: CryptoHash,
        height: BlockHeight,
        prev_block_hash: CryptoHash,
    ) -> Result<(), Error> {
        match result {
            ApplyChunkResult::SameHeight(SameHeightResult {
                gas_limit,
                shard_uid,
                apply_result,
                apply_split_result_or_state_changes,
            }) => {
                let (outcome_root, outcome_paths) =
                    ApplyTransactionResult::compute_outcomes_proof(&apply_result.outcomes);
                let shard_id = shard_uid.shard_id();

                // Save state root after applying transactions.
                self.chain_store_update.save_chunk_extra(
                    &block_hash,
                    &shard_uid,
                    ChunkExtra::new(
                        &apply_result.new_root,
                        outcome_root,
                        apply_result.validator_proposals,
                        apply_result.total_gas_burnt,
                        gas_limit,
                        apply_result.total_balance_burnt,
                    ),
                );
                self.save_flat_state_changes(
                    block_hash,
                    prev_block_hash,
                    height,
                    shard_uid,
                    &apply_result.trie_changes,
                )?;
                self.chain_store_update.save_trie_changes(apply_result.trie_changes);
                self.chain_store_update.save_outgoing_receipt(
                    &block_hash,
                    shard_id,
                    apply_result.outgoing_receipts,
                );
                // Save receipt and transaction results.
                self.chain_store_update.save_outcomes_with_proofs(
                    &block_hash,
                    shard_id,
                    apply_result.outcomes,
                    outcome_paths,
                );
                if let Some(apply_results_or_state_changes) = apply_split_result_or_state_changes {
                    self.process_split_state(
                        &block_hash,
                        &prev_block_hash,
                        &shard_uid,
                        apply_results_or_state_changes,
                    )?;
                }
            }
            ApplyChunkResult::DifferentHeight(DifferentHeightResult {
                shard_uid,
                apply_result,
                apply_split_result_or_state_changes,
            }) => {
                let old_extra =
                    self.chain_store_update.get_chunk_extra(&prev_block_hash, &shard_uid)?;

                let mut new_extra = ChunkExtra::clone(&old_extra);
                *new_extra.state_root_mut() = apply_result.new_root;

                self.save_flat_state_changes(
                    block_hash,
                    prev_block_hash,
                    height,
                    shard_uid,
                    &apply_result.trie_changes,
                )?;
                self.chain_store_update.save_chunk_extra(&block_hash, &shard_uid, new_extra);
                self.chain_store_update.save_trie_changes(apply_result.trie_changes);

                if let Some(apply_results_or_state_changes) = apply_split_result_or_state_changes {
                    self.process_split_state(
                        &block_hash,
                        &prev_block_hash,
                        &shard_uid,
                        apply_results_or_state_changes,
                    )?;
                }
            }
            ApplyChunkResult::SplitState(SplitStateResult { shard_uid, results }) => {
                self.chain_store_update
                    .remove_state_changes_for_split_states(block_hash, shard_uid.shard_id());
                self.process_split_state(
                    &block_hash,
                    &prev_block_hash,
                    &shard_uid,
                    ApplySplitStateResultOrStateChanges::ApplySplitStateResults(results),
                )?;
            }
        };
        Ok(())
    }

    /// This is the last step of process_block_single, where we take the preprocess block info
    /// apply chunk results and store the results on chain.
    fn postprocess_block(
        &mut self,
        me: &Option<AccountId>,
        block: &Block,
        block_preprocess_info: BlockPreprocessInfo,
        apply_chunks_results: Vec<Result<ApplyChunkResult, Error>>,
    ) -> Result<Option<Tip>, Error> {
        let prev_hash = block.header().prev_hash();
        let prev_block = self.chain_store_update.get_block(prev_hash)?;
        let results = apply_chunks_results.into_iter().map(|x| {
            if let Err(err) = &x {
                warn!(target:"chain", hash = %block.hash(), error = %err, "Error in applying chunks for block");
            }
            x
        }).collect::<Result<Vec<_>, Error>>()?;
        self.apply_chunk_postprocessing(block, &prev_block, results)?;

        let BlockPreprocessInfo {
            is_caught_up,
            state_sync_info,
            incoming_receipts,
            challenges_result,
            challenged_blocks,
            ..
        } = block_preprocess_info;

        if !is_caught_up {
            debug!(target: "chain", %prev_hash, hash = %*block.hash(), "Add block to catch up");
            self.chain_store_update.add_block_to_catchup(*prev_hash, *block.hash());
        }

        for (shard_id, receipt_proofs) in incoming_receipts {
            self.chain_store_update.save_incoming_receipt(
                block.hash(),
                shard_id,
                Arc::new(receipt_proofs),
            );
        }
        if let Some(state_sync_info) = state_sync_info {
            self.chain_store_update.add_state_sync_info(state_sync_info);
        }

        self.chain_store_update.save_block_extra(block.hash(), BlockExtra { challenges_result });
        for block_hash in challenged_blocks {
            self.mark_block_as_challenged(&block_hash, Some(block.hash()))?;
        }

        self.chain_store_update.save_block_header(block.header().clone())?;
        self.update_header_head_if_not_challenged(block.header())?;

        // If block checks out, record validator proposals for given block.
        let last_final_block = block.header().last_final_block();
        let last_finalized_height = if last_final_block == &CryptoHash::default() {
            self.chain_store_update.get_genesis_height()
        } else {
            self.chain_store_update.get_block_header(last_final_block)?.height()
        };

        let epoch_manager_update = self
            .epoch_manager
            .add_validator_proposals(BlockHeaderInfo::new(block.header(), last_finalized_height))?;
        self.chain_store_update.merge(epoch_manager_update);

        // Add validated block to the db, even if it's not the canonical fork.
        self.chain_store_update.save_block(block.clone());
        self.chain_store_update.inc_block_refcount(prev_hash)?;

        // Save receipt_id_to_shard_id for all outgoing receipts generated in this block
        self.save_receipt_id_to_shard_id_for_block(
            me,
            block.hash(),
            prev_hash,
            block.chunks().len() as NumShards,
        )?;

        // Update the chain head if it's the new tip
        let res = self.update_head(block.header())?;

        if res.is_some() {
            // On the epoch switch record the epoch light client block
            // Note that we only do it if `res.is_some()`, i.e. if the current block is the head.
            // This is necessary because the computation of the light client block relies on
            // `ColNextBlockHash`-es populated, and they are only populated for the canonical
            // chain. We need to be careful to avoid a situation when the first block of the epoch
            // never becomes a tip of the canonical chain.
            // Presently the epoch boundary is defined by the height, and the fork choice rule
            // is also just height, so the very first block to cross the epoch end is guaranteed
            // to be the head of the chain, and result in the light client block produced.
            let prev = self.chain_store_update.get_previous_header(block.header())?;
            let prev_epoch_id = prev.epoch_id().clone();
            if block.header().epoch_id() != &prev_epoch_id {
                if prev.last_final_block() != &CryptoHash::default() {
                    let light_client_block = self.create_light_client_block(&prev)?;
                    self.chain_store_update
                        .save_epoch_light_client_block(&prev_epoch_id.0, light_client_block);
                }
            }
        }
        Ok(res)
    }

    pub fn create_light_client_block(
        &mut self,
        header: &BlockHeader,
    ) -> Result<LightClientBlockView, Error> {
        // First update the last next_block, since it might not be set yet
        self.chain_store_update.save_next_block_hash(header.prev_hash(), *header.hash());

        Chain::create_light_client_block(
            header,
            self.epoch_manager.as_ref(),
            &mut self.chain_store_update,
        )
    }

    #[allow(dead_code)]
    fn verify_orphan_header_approvals(&mut self, header: &BlockHeader) -> Result<(), Error> {
        let prev_hash = header.prev_hash();
        let prev_height = match header.prev_height() {
            None => {
                // this will accept orphans of V1 and V2
                // TODO: reject header V1 and V2 after a certain height
                return Ok(());
            }
            Some(prev_height) => prev_height,
        };
        let height = header.height();
        let epoch_id = header.epoch_id();
        let approvals = header.approvals();
        self.epoch_manager.verify_approvals_and_threshold_orphan(
            epoch_id,
            &|approvals, stakes| {
                Doomslug::can_approved_block_be_produced(
                    self.doomslug_threshold_mode,
                    approvals,
                    stakes,
                )
            },
            prev_hash,
            prev_height,
            height,
            approvals,
        )
    }

    /// Update the header head if this header has most work.
    fn update_header_head_if_not_challenged(
        &mut self,
        header: &BlockHeader,
    ) -> Result<Option<Tip>, Error> {
        let header_head = self.chain_store_update.header_head()?;
        if header.height() > header_head.height {
            let tip = Tip::from_header(header);
            self.chain_store_update.save_header_head_if_not_challenged(&tip)?;
            debug!(target: "chain", "Header head updated to {} at {}", tip.last_block_hash, tip.height);
            metrics::HEADER_HEAD_HEIGHT.set(tip.height as i64);

            Ok(Some(tip))
        } else {
            Ok(None)
        }
    }

    fn update_final_head_from_block(&mut self, header: &BlockHeader) -> Result<Option<Tip>, Error> {
        let final_head = self.chain_store_update.final_head()?;
        let last_final_block_header =
            match self.chain_store_update.get_block_header(header.last_final_block()) {
                Ok(final_header) => final_header,
                Err(Error::DBNotFoundErr(_)) => return Ok(None),
                Err(err) => return Err(err),
            };
        if last_final_block_header.height() > final_head.height {
            let tip = Tip::from_header(&last_final_block_header);
            self.chain_store_update.save_final_head(&tip)?;
            Ok(Some(tip))
        } else {
            Ok(None)
        }
    }

    /// Directly updates the head if we've just appended a new block to it or handle
    /// the situation where the block has higher height to have a fork
    fn update_head(&mut self, header: &BlockHeader) -> Result<Option<Tip>, Error> {
        // if we made a fork with higher height than the head (which should also be true
        // when extending the head), update it
        self.update_final_head_from_block(header)?;
        let head = self.chain_store_update.head()?;
        if header.height() > head.height {
            let tip = Tip::from_header(header);

            self.chain_store_update.save_body_head(&tip)?;
            metrics::BLOCK_HEIGHT_HEAD.set(tip.height as i64);
            metrics::BLOCK_ORDINAL_HEAD.set(header.block_ordinal() as i64);
            debug!(target: "chain", "Head updated to {} at {}", tip.last_block_hash, tip.height);
            Ok(Some(tip))
        } else {
            Ok(None)
        }
    }

    /// Marks a block as invalid,
    fn mark_block_as_challenged(
        &mut self,
        block_hash: &CryptoHash,
        challenger_hash: Option<&CryptoHash>,
    ) -> Result<(), Error> {
        info!(target: "chain", "Marking {} as challenged block (challenged in {:?}) and updating the chain.", block_hash, challenger_hash);
        let block_header = match self.chain_store_update.get_block_header(block_hash) {
            Ok(block_header) => block_header,
            Err(e) => match e {
                Error::DBNotFoundErr(_) => {
                    // The block wasn't seen yet, still challenge is good.
                    self.chain_store_update.save_challenged_block(*block_hash);
                    return Ok(());
                }
                _ => return Err(e),
            },
        };

        let cur_block_at_same_height =
            match self.chain_store_update.get_block_hash_by_height(block_header.height()) {
                Ok(bh) => Some(bh),
                Err(e) => match e {
                    Error::DBNotFoundErr(_) => None,
                    _ => return Err(e),
                },
            };

        self.chain_store_update.save_challenged_block(*block_hash);

        // If the block being invalidated is on the canonical chain, update head
        if cur_block_at_same_height == Some(*block_hash) {
            // We only consider two candidates for the new head: the challenger and the block
            //   immediately preceding the block being challenged
            // It could be that there is a better chain known. However, it is extremely unlikely,
            //   and even if there's such chain available, the very next block built on it will
            //   bring this node's head to that chain.
            let prev_header = self.chain_store_update.get_block_header(block_header.prev_hash())?;
            let prev_height = prev_header.height();
            let new_head_header = if let Some(hash) = challenger_hash {
                let challenger_header = self.chain_store_update.get_block_header(hash)?;
                if challenger_header.height() > prev_height {
                    challenger_header
                } else {
                    prev_header
                }
            } else {
                prev_header
            };
            let last_final_block = *new_head_header.last_final_block();

            let tip = Tip::from_header(&new_head_header);
            self.chain_store_update.save_head(&tip)?;
            let new_final_header = self.chain_store_update.get_block_header(&last_final_block)?;
            self.chain_store_update.save_final_head(&Tip::from_header(&new_final_header))?;
        }

        Ok(())
    }

    pub fn set_state_finalize(
        &mut self,
        shard_id: ShardId,
        sync_hash: CryptoHash,
        shard_state_header: ShardStateSyncResponseHeader,
    ) -> Result<(), Error> {
        let _span =
            tracing::debug_span!(target: "sync", "chain_update_set_state_finalize").entered();
        let (chunk, incoming_receipts_proofs) = match shard_state_header {
            ShardStateSyncResponseHeader::V1(shard_state_header) => (
                ShardChunk::V1(shard_state_header.chunk),
                shard_state_header.incoming_receipts_proofs,
            ),
            ShardStateSyncResponseHeader::V2(shard_state_header) => {
                (shard_state_header.chunk, shard_state_header.incoming_receipts_proofs)
            }
        };

        let block_header = self
            .chain_store_update
            .get_block_header_on_chain_by_height(&sync_hash, chunk.height_included())?;

        // Getting actual incoming receipts.
        let mut receipt_proof_response: Vec<ReceiptProofResponse> = vec![];
        for incoming_receipt_proof in incoming_receipts_proofs.iter() {
            let ReceiptProofResponse(hash, _) = incoming_receipt_proof;
            let block_header = self.chain_store_update.get_block_header(hash)?;
            if block_header.height() <= chunk.height_included() {
                receipt_proof_response.push(incoming_receipt_proof.clone());
            }
        }
        let receipts = collect_receipts_from_response(&receipt_proof_response);
        // Prev block header should be present during state sync, since headers have been synced at this point.
        let gas_price = if block_header.height() == self.chain_store_update.get_genesis_height() {
            block_header.gas_price()
        } else {
            self.chain_store_update.get_block_header(block_header.prev_hash())?.gas_price()
        };

        let chunk_header = chunk.cloned_header();
        let gas_limit = chunk_header.gas_limit();
        // This is set to false because the value is only relevant
        // during protocol version RestoreReceiptsAfterFixApplyChunks.
        // TODO(nikurt): Determine the value correctly.
        let is_first_block_with_chunk_of_version = false;

        let apply_result = self.runtime_adapter.apply_transactions(
            shard_id,
            &chunk_header.prev_state_root(),
            chunk_header.height_included(),
            block_header.raw_timestamp(),
            &chunk_header.prev_block_hash(),
            block_header.hash(),
            &receipts,
            chunk.transactions(),
            chunk_header.validator_proposals(),
            gas_price,
            gas_limit,
            block_header.challenges_result(),
            *block_header.random_value(),
            true,
            is_first_block_with_chunk_of_version,
            Default::default(),
            true,
        )?;

        let (outcome_root, outcome_proofs) =
            ApplyTransactionResult::compute_outcomes_proof(&apply_result.outcomes);

        self.chain_store_update.save_chunk(chunk);

        let shard_uid = self.epoch_manager.shard_id_to_uid(shard_id, block_header.epoch_id())?;
        self.save_flat_state_changes(
            *block_header.hash(),
            *chunk_header.prev_block_hash(),
            chunk_header.height_included(),
            shard_uid,
            &apply_result.trie_changes,
        )?;
        self.chain_store_update.save_trie_changes(apply_result.trie_changes);
        let chunk_extra = ChunkExtra::new(
            &apply_result.new_root,
            outcome_root,
            apply_result.validator_proposals,
            apply_result.total_gas_burnt,
            gas_limit,
            apply_result.total_balance_burnt,
        );
        self.chain_store_update.save_chunk_extra(block_header.hash(), &shard_uid, chunk_extra);

        self.chain_store_update.save_outgoing_receipt(
            block_header.hash(),
            shard_id,
            apply_result.outgoing_receipts,
        );
        // Saving transaction results.
        self.chain_store_update.save_outcomes_with_proofs(
            block_header.hash(),
            shard_id,
            apply_result.outcomes,
            outcome_proofs,
        );
        // Saving all incoming receipts.
        for receipt_proof_response in incoming_receipts_proofs {
            self.chain_store_update.save_incoming_receipt(
                &receipt_proof_response.0,
                shard_id,
                receipt_proof_response.1,
            );
        }
        Ok(())
    }

    pub fn set_state_finalize_on_height(
        &mut self,
        height: BlockHeight,
        shard_id: ShardId,
        sync_hash: CryptoHash,
    ) -> Result<bool, Error> {
        let _span = tracing::debug_span!(target: "sync", "set_state_finalize_on_height").entered();
        let block_header_result =
            self.chain_store_update.get_block_header_on_chain_by_height(&sync_hash, height);
        if let Err(_) = block_header_result {
            // No such height, go ahead.
            return Ok(true);
        }
        let block_header = block_header_result?;
        if block_header.hash() == &sync_hash {
            // Don't continue
            return Ok(false);
        }
        let prev_block_header =
            self.chain_store_update.get_block_header(block_header.prev_hash())?;

        let shard_uid = self.epoch_manager.shard_id_to_uid(shard_id, block_header.epoch_id())?;
        let chunk_extra =
            self.chain_store_update.get_chunk_extra(prev_block_header.hash(), &shard_uid)?;

        let apply_result = self.runtime_adapter.apply_transactions(
            shard_id,
            chunk_extra.state_root(),
            block_header.height(),
            block_header.raw_timestamp(),
            prev_block_header.hash(),
            block_header.hash(),
            &[],
            &[],
            chunk_extra.validator_proposals(),
            prev_block_header.gas_price(),
            chunk_extra.gas_limit(),
            block_header.challenges_result(),
            *block_header.random_value(),
            false,
            false,
            Default::default(),
            true,
        )?;
        self.save_flat_state_changes(
            *block_header.hash(),
            *prev_block_header.hash(),
            height,
            shard_uid,
            &apply_result.trie_changes,
        )?;
        self.chain_store_update.save_trie_changes(apply_result.trie_changes);

        let mut new_chunk_extra = ChunkExtra::clone(&chunk_extra);
        *new_chunk_extra.state_root_mut() = apply_result.new_root;

        self.chain_store_update.save_chunk_extra(block_header.hash(), &shard_uid, new_chunk_extra);
        Ok(true)
    }
}

pub fn do_apply_chunks(
    block_hash: CryptoHash,
    block_height: BlockHeight,
    work: Vec<Box<dyn FnOnce(&Span) -> Result<ApplyChunkResult, Error> + Send>>,
) -> Vec<Result<ApplyChunkResult, Error>> {
    let parent_span =
        tracing::debug_span!(target: "chain", "do_apply_chunks", block_height, %block_hash)
            .entered();
    work.into_par_iter()
        .map(|task| {
            // As chunks can be processed in parallel, make sure they are all tracked as children of
            // a single span.
            task(&parent_span)
        })
        .collect::<Vec<_>>()
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

#[derive(actix::Message)]
#[rtype(result = "()")]
pub struct ApplyStatePartsResponse {
    pub apply_result: Result<(), near_chain_primitives::error::Error>,
    pub shard_id: ShardId,
    pub sync_hash: CryptoHash,
}

#[derive(actix::Message)]
#[rtype(result = "()")]
pub struct BlockCatchUpRequest {
    pub sync_hash: CryptoHash,
    pub block_hash: CryptoHash,
    pub block_height: BlockHeight,
    pub work: Vec<Box<dyn FnOnce(&Span) -> Result<ApplyChunkResult, Error> + Send>>,
}

#[derive(actix::Message)]
#[rtype(result = "()")]
pub struct BlockCatchUpResponse {
    pub sync_hash: CryptoHash,
    pub block_hash: CryptoHash,
    pub results: Vec<Result<ApplyChunkResult, Error>>,
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
///     Otherwise results are commited, block is moved to done blocks and any blocks that
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
    pub processed_blocks: HashMap<CryptoHash, Vec<Result<ApplyChunkResult, Error>>>,
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

#[cfg(test)]
mod tests {
    use near_primitives::hash::CryptoHash;

    #[test]
    pub fn receipt_randomness_reproducibility() {
        // Sanity check that the receipt shuffling implementation does not change.
        let mut receipt_proofs = vec![0, 1, 2, 3, 4, 5, 6];
        crate::Chain::shuffle_receipt_proofs(
            &mut receipt_proofs,
            &CryptoHash::hash_bytes(&[1, 2, 3, 4, 5]),
        );
        assert_eq!(receipt_proofs, vec![2, 3, 1, 4, 0, 5, 6],);
    }
}
