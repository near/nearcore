//! Structs in this file are used for debug purposes, and might change at any time
//! without backwards compatibility.
use crate::chunk_inclusion_tracker::ChunkInclusionTracker;
use crate::client_actor::ClientActorInner;
use itertools::Itertools;
use near_async::messaging::Handler;
use near_async::time::{Clock, Instant};
use near_chain::crypto_hash_timer::CryptoHashTimer;
use near_chain::{Block, Chain, ChainStoreAccess, near_chain_primitives};
use near_client_primitives::debug::{
    ApprovalAtHeightStatus, BlockProduction, ChunkCollection, DebugBlockStatusData,
    DebugBlockStatusQuery, DebugBlocksStartingMode, DebugStatus, DebugStatusResponse,
    MissedHeightInfo, ProductionAtHeight, ValidatorStatus,
};
use near_client_primitives::types::Error;
use near_client_primitives::{
    debug::{EpochInfoView, TrackedShardsView},
    types::StatusError,
};
use near_epoch_manager::EpochManagerAdapter;
use near_o11y::log_assert;
use near_performance_metrics_macros::perf;
use near_primitives::congestion_info::CongestionControl;
use near_primitives::errors::EpochError;
use near_primitives::state_sync::get_num_state_parts;
use near_primitives::stateless_validation::ChunkProductionKey;
use near_primitives::stateless_validation::chunk_endorsement::ChunkEndorsement;
use near_primitives::types::{
    AccountId, BlockHeight, NumShards, ShardId, ShardIndex, ValidatorInfoIdentifier,
};
use near_primitives::{
    hash::CryptoHash,
    state_sync::{ShardStateSyncResponseHeader, StateHeaderKey},
    types::EpochId,
    views::ValidatorInfo,
};
use near_store::DBCol;
use near_store::adapter::chain_store::ChainStoreAdapter;
use std::cmp::{max, min};
use std::collections::{HashMap, HashSet};
use std::num::NonZeroUsize;
use time::ext::InstantExt as _;

use near_client_primitives::debug::{DebugBlockStatus, DebugChunkStatus};
use near_network::types::{ConnectedPeerInfo, NetworkInfo, PeerType};
use near_primitives::sharding::ChunkHash;
use near_primitives::views::{
    AccountDataView, KnownProducerView, NetworkInfoView, PeerInfoView, Tier1ProxyView,
};

// Maximum number of blocks to search for the first block to display.
const DEBUG_MAX_BLOCKS_TO_SEARCH: u64 = 10000;

// Maximum number of blocks to fetch when displaying block status.
const DEBUG_MAX_BLOCKS_TO_FETCH: u64 = 1000;

// Number of epochs to fetch when displaying epoch info.
const DEBUG_EPOCHS_TO_FETCH: u32 = 5;

// How many old blocks (before HEAD) should be shown in debug page.
const DEBUG_PRODUCTION_OLD_BLOCKS_TO_SHOW: u64 = 50;

// Maximum number of blocks to show.
const DEBUG_MAX_PRODUCTION_BLOCKS_TO_SHOW: u64 = 1000;

/// Number of blocks (and chunks) for which to keep the detailed timing information for debug purposes.
pub const PRODUCTION_TIMES_CACHE_SIZE: usize = 1000;

pub struct BlockProductionTracker(lru::LruCache<BlockHeight, BlockProduction>);

impl BlockProductionTracker {
    pub(crate) fn new() -> Self {
        Self(lru::LruCache::new(NonZeroUsize::new(PRODUCTION_TIMES_CACHE_SIZE).unwrap()))
    }

    pub(crate) fn get(&mut self, height: BlockHeight) -> BlockProduction {
        self.0.get(&height).cloned().unwrap_or_default()
    }

    /// Record approvals received so far for this block. Must be called before block is produced.
    pub(crate) fn record_approvals(
        &mut self,
        height: BlockHeight,
        approvals: ApprovalAtHeightStatus,
    ) {
        // This function will only be called before block production, so it is ok to overwrite
        // the previous value
        if let Some(prev_block_production) = self.0.put(
            height,
            BlockProduction {
                approvals,
                chunks_collection_time: vec![],
                block_production_time: None,
                block_included: false,
            },
        ) {
            log_assert!(
                prev_block_production.block_production_time.is_none(),
                "record approvals called after block {} produced",
                height
            );
        }
    }

    /// Record block production info
    pub(crate) fn record_block_production(
        &mut self,
        height: BlockHeight,
        chunk_collections: Vec<ChunkCollection>,
    ) {
        if let Some(block_production) = self.0.get_mut(&height) {
            block_production.block_production_time = Some(Clock::real().now_utc());
            block_production.chunks_collection_time = chunk_collections;
        }
    }

    /// Record chunk collected after a block is produced if the block didn't include a chunk for the shard.
    /// If called before the block was produced, nothing happens.
    pub(crate) fn record_chunk_collected(&mut self, height: BlockHeight, shard_index: ShardIndex) {
        if let Some(block_production) = self.0.get_mut(&height) {
            let chunk_collections = &mut block_production.chunks_collection_time;
            // Check that chunk_collection is set and we haven't received this chunk yet.
            if let Some(chunk_collection) = chunk_collections.get_mut(shard_index) {
                if chunk_collection.received_time.is_none() {
                    chunk_collection.received_time = Some(Clock::real().now_utc());
                }
            }
            // Otherwise, it means chunk_collections is not set yet, which means the block wasn't produced.
            // And we do nothing in this case.
        }
    }

    pub(crate) fn construct_chunk_collection_info(
        block_height: BlockHeight,
        epoch_id: &EpochId,
        num_shards: usize,
        new_chunks: &HashMap<ShardId, ChunkHash>,
        epoch_manager: &dyn EpochManagerAdapter,
        chunk_inclusion_tracker: &ChunkInclusionTracker,
    ) -> Result<Vec<ChunkCollection>, Error> {
        let mut chunk_collection_info = vec![];
        for shard_index in 0..num_shards {
            let shard_layout = epoch_manager.get_shard_layout(epoch_id)?;
            let shard_id = shard_layout.get_shard_id(shard_index);
            let shard_id = shard_id.map_err(Into::<EpochError>::into)?;

            if let Some(chunk_hash) = new_chunks.get(&shard_id) {
                let (chunk_producer, received_time) =
                    chunk_inclusion_tracker.get_chunk_producer_and_received_time(chunk_hash)?;
                chunk_collection_info.push(ChunkCollection {
                    chunk_producer,
                    received_time: Some(received_time),
                    chunk_included: true,
                });
            } else {
                let chunk_producer = epoch_manager
                    .get_chunk_producer_info(&ChunkProductionKey {
                        epoch_id: *epoch_id,
                        height_created: block_height,
                        shard_id,
                    })?
                    .take_account_id();
                chunk_collection_info.push(ChunkCollection {
                    chunk_producer,
                    received_time: None,
                    chunk_included: false,
                });
            }
        }
        Ok(chunk_collection_info)
    }
}

impl Handler<DebugStatus> for ClientActorInner {
    #[perf]
    fn handle(&mut self, msg: DebugStatus) -> Result<DebugStatusResponse, StatusError> {
        match msg {
            DebugStatus::SyncStatus => Ok(DebugStatusResponse::SyncStatus(
                self.client.sync_handler.sync_status.clone().into(),
            )),
            DebugStatus::TrackedShards => {
                Ok(DebugStatusResponse::TrackedShards(self.get_tracked_shards_view()?))
            }
            DebugStatus::EpochInfo(epoch_id) => {
                Ok(DebugStatusResponse::EpochInfo(self.get_recent_epoch_info(epoch_id)?))
            }
            DebugStatus::BlockStatus(query) => {
                Ok(DebugStatusResponse::BlockStatus(self.get_last_blocks_info(query)?))
            }
            DebugStatus::ValidatorStatus => {
                Ok(DebugStatusResponse::ValidatorStatus(self.get_validator_status()?))
            }
            DebugStatus::CatchupStatus => {
                Ok(DebugStatusResponse::CatchupStatus(self.client.get_catchup_status()?))
            }
            DebugStatus::RequestedStateParts => Ok(DebugStatusResponse::RequestedStateParts(
                self.client.chain.state_sync_adapter.get_requested_state_parts(),
            )),
            DebugStatus::ChainProcessingStatus => Ok(DebugStatusResponse::ChainProcessingStatus(
                self.client.chain.get_chain_processing_info(),
            )),
        }
    }
}

fn get_block_hashes_to_fetch(
    chain_store: &ChainStoreAdapter,
    height_to_fetch: BlockHeight,
    final_height: BlockHeight,
) -> Vec<CryptoHash> {
    if height_to_fetch >= final_height {
        chain_store
            .get_all_header_hashes_by_height(height_to_fetch)
            .unwrap_or_default()
            .into_iter()
            .collect()
    } else {
        chain_store.get_block_hash_by_height(height_to_fetch).ok().into_iter().collect()
    }
}

fn find_first_height_to_fetch(
    chain_store: &ChainStoreAdapter,
    mut height_to_fetch: BlockHeight,
    mode: DebugBlocksStartingMode,
    final_height: BlockHeight,
) -> Result<BlockHeight, near_chain_primitives::Error> {
    if matches!(mode, DebugBlocksStartingMode::All) {
        return Ok(height_to_fetch);
    }

    let min_height_to_search = max(
        height_to_fetch as i64 - DEBUG_MAX_BLOCKS_TO_SEARCH as i64,
        chain_store.genesis_height() as i64,
    ) as u64;
    while height_to_fetch > min_height_to_search {
        let block_hashes = get_block_hashes_to_fetch(chain_store, height_to_fetch, final_height);
        if block_hashes.is_empty() {
            if matches!(mode, DebugBlocksStartingMode::JumpToBlockMiss) {
                break;
            }
            height_to_fetch -= 1;
            continue;
        }
        if matches!(mode, DebugBlocksStartingMode::JumpToBlockProduced) {
            break;
        }

        let mut found_block = false;
        for block_hash in block_hashes {
            let block_header = chain_store.get_block_header(&block_hash)?;
            let all_chunks_included = block_header.chunk_mask().iter().all(|&x| x);
            if all_chunks_included {
                if matches!(mode, DebugBlocksStartingMode::JumpToAllChunksIncluded) {
                    found_block = true;
                    break;
                }
            } else {
                if matches!(mode, DebugBlocksStartingMode::JumpToChunkMiss) {
                    found_block = true;
                    break;
                }
            }
        }
        if found_block {
            break;
        }

        height_to_fetch -= 1;
    }

    Ok(height_to_fetch)
}

fn get_epoch_start_height(
    epoch_manager: &dyn EpochManagerAdapter,
    epoch_identifier: &ValidatorInfoIdentifier,
) -> Result<BlockHeight, EpochError> {
    match epoch_identifier {
        ValidatorInfoIdentifier::EpochId(epoch_id) => {
            epoch_manager.get_epoch_start_from_epoch_id(epoch_id)
        }
        ValidatorInfoIdentifier::BlockHash(block_hash) => {
            epoch_manager.get_epoch_start_height(block_hash)
        }
    }
}

fn get_prev_epoch_identifier(
    chain: &Chain,
    first_block: Option<CryptoHash>,
) -> Option<ValidatorInfoIdentifier> {
    let epoch_start_block_header = chain.get_block_header(first_block.as_ref()?).ok()?;
    if epoch_start_block_header.is_genesis() {
        return None;
    }
    let prev_epoch_last_block_hash = epoch_start_block_header.prev_hash();
    let prev_epoch_last_block_header = chain.get_block_header(prev_epoch_last_block_hash).ok()?;
    if prev_epoch_last_block_header.is_genesis() {
        return None;
    }
    Some(ValidatorInfoIdentifier::EpochId(*prev_epoch_last_block_header.epoch_id()))
}

impl ClientActorInner {
    // Gets a list of block producers, chunk producers and chunk validators for a given epoch.
    fn get_validators_for_epoch(
        &self,
        epoch_id: &EpochId,
    ) -> Result<(Vec<ValidatorInfo>, Vec<String>, Vec<String>), Error> {
        let all_validators = self
            .client
            .epoch_manager
            .get_epoch_all_validators(epoch_id)?
            .into_iter()
            .map(|validator_stake| validator_stake.take_account_id().to_string())
            .collect_vec();
        let block_producers: Vec<ValidatorInfo> = self
            .client
            .epoch_manager
            .get_epoch_block_producers_ordered(epoch_id)?
            .into_iter()
            .map(|validator_stake| ValidatorInfo { account_id: validator_stake.take_account_id() })
            .collect();
        let chunk_producers = self
            .client
            .epoch_manager
            .get_epoch_chunk_producers(&epoch_id)?
            .into_iter()
            .map(|validator_stake| validator_stake.take_account_id().to_string())
            .collect_vec();
        // Note that currently all validators are chunk validators.
        Ok((block_producers, chunk_producers, all_validators))
    }

    /// Gets the information about the epoch that contains a given block.
    fn get_epoch_info_view(
        &self,
        epoch_identifier: &ValidatorInfoIdentifier,
    ) -> Result<EpochInfoView, Error> {
        let epoch_start_height =
            get_epoch_start_height(self.client.epoch_manager.as_ref(), epoch_identifier)?;
        let epoch_start_block_header =
            self.client.chain.get_block_header_by_height(epoch_start_height)?;
        let epoch_id = epoch_start_block_header.epoch_id();
        let shard_layout = self.client.epoch_manager.get_shard_layout(&epoch_id)?;

        let (block_producers, chunk_producers, chunk_validators) =
            self.get_validators_for_epoch(&epoch_id)?;

        let sync_hash =
            self.client.chain.get_sync_hash(epoch_start_block_header.hash()).ok().flatten();
        let hash_to_compute_shard_sizes = match &sync_hash {
            Some(sync_hash) => sync_hash,
            None => epoch_start_block_header.hash(),
        };

        let shards_size_and_parts: Vec<(u64, u64)> = if let Ok(block) =
            self.client.chain.get_block(hash_to_compute_shard_sizes)
        {
            block
                .chunks()
                .iter_raw()
                .enumerate()
                .map(|(shard_index, chunk)| {
                    let shard_id = shard_layout.get_shard_id(shard_index);
                    let Ok(shard_id) = shard_id else {
                        tracing::error!("Failed to get shard id for shard index {}", shard_index);
                        return (0, 0);
                    };

                    let state_root_node = self.client.runtime_adapter.get_state_root_node(
                        shard_id,
                        epoch_start_block_header.hash(),
                        &chunk.prev_state_root(),
                    );
                    if let Ok(state_root_node) = state_root_node {
                        (
                            state_root_node.memory_usage,
                            get_num_state_parts(state_root_node.memory_usage),
                        )
                    } else {
                        (0, 0)
                    }
                })
                .collect()
        } else {
            epoch_start_block_header.chunk_mask().iter().map(|_| (0, 0)).collect()
        };

        let state_header_exists: Vec<bool> = shard_layout
            .shard_ids()
            .map(|shard_id| {
                let key =
                    borsh::to_vec(&StateHeaderKey(shard_id, *epoch_start_block_header.hash()));
                match key {
                    Ok(key) => {
                        matches!(
                            self.client
                                .chain
                                .chain_store()
                                .store()
                                .get_ser::<ShardStateSyncResponseHeader>(DBCol::StateHeaders, &key),
                            Ok(Some(_))
                        )
                    }
                    Err(_) => false,
                }
            })
            .collect();

        let shards_size_and_parts = shards_size_and_parts
            .iter()
            .zip(state_header_exists.iter())
            .map(|((a, b), c)| (*a, *b, *c))
            .collect();

        let validator_info =
            self.client.epoch_manager.get_validator_info(epoch_identifier.clone())?;
        let epoch_height =
            self.client.epoch_manager.get_epoch_info(&epoch_id).map(|info| info.epoch_height())?;
        Ok(EpochInfoView {
            epoch_height,
            epoch_id: epoch_id.0,
            height: epoch_start_block_header.height(),
            first_block: Some((
                *epoch_start_block_header.hash(),
                epoch_start_block_header.timestamp(),
            )),
            block_producers,
            chunk_producers,
            chunk_validators,
            validator_info: Some(validator_info),
            protocol_version: self
                .client
                .epoch_manager
                .get_epoch_protocol_version(epoch_id)
                .unwrap_or(0),
            sync_hash,
            shards_size_and_parts,
        })
    }

    /// Get information about the next epoch, which may not have started yet.
    fn get_next_epoch_view(
        &self,
        epoch_identifier: &ValidatorInfoIdentifier,
    ) -> Result<EpochInfoView, Error> {
        let epoch_start_height =
            get_epoch_start_height(self.client.epoch_manager.as_ref(), epoch_identifier)?;
        let epoch_first_block = self.client.chain.get_block_header_by_height(epoch_start_height)?;
        let next_epoch_id = epoch_first_block.next_epoch_id();
        let (block_producers, chunk_producers, chunk_validators) =
            self.get_validators_for_epoch(next_epoch_id)?;

        Ok(EpochInfoView {
            epoch_height: self
                .client
                .epoch_manager
                .get_epoch_info(next_epoch_id)
                .map(|info| info.epoch_height())?,
            epoch_id: next_epoch_id.0,
            // Expected height of the next epoch.
            height: epoch_start_height + self.client.config.epoch_length,
            first_block: None,
            block_producers,
            chunk_producers,
            chunk_validators,
            validator_info: None,
            protocol_version: self
                .client
                .epoch_manager
                .get_epoch_protocol_version(next_epoch_id)?,
            sync_hash: None,
            shards_size_and_parts: vec![],
        })
    }

    fn get_tracked_shards_view(&self) -> Result<TrackedShardsView, near_chain_primitives::Error> {
        let epoch_id = self.client.chain.header_head()?.epoch_id;
        let fetch_hash = self.client.chain.header_head()?.last_block_hash;
        let me = self.client.validator_signer.get().map(|x| x.validator_id().clone());
        let shard_ids = self.client.epoch_manager.shard_ids(&epoch_id).unwrap();
        let shards_tracked_this_epoch = shard_ids
            .iter()
            .map(|&shard_id| {
                self.client.shard_tracker.cares_about_shard(
                    me.as_ref(),
                    &fetch_hash,
                    shard_id,
                    true,
                )
            })
            .collect();
        let shards_tracked_next_epoch = shard_ids
            .into_iter()
            .map(|shard_id| {
                self.client.shard_tracker.will_care_about_shard(
                    me.as_ref(),
                    &fetch_hash,
                    shard_id,
                    true,
                )
            })
            .collect();
        Ok(TrackedShardsView { shards_tracked_this_epoch, shards_tracked_next_epoch })
    }

    fn get_recent_epoch_info(
        &self,
        epoch_id: Option<EpochId>,
    ) -> Result<Vec<EpochInfoView>, near_chain_primitives::Error> {
        let mut epochs_info: Vec<EpochInfoView> = Vec::new();

        let head = self.client.chain.head()?;
        let epoch_identifier = match epoch_id {
            // Use epoch id if the epoch is already finalized.
            Some(epoch_id) if head.epoch_id != epoch_id => {
                ValidatorInfoIdentifier::EpochId(epoch_id)
            }
            // Otherwise use the last block hash.
            _ => ValidatorInfoIdentifier::BlockHash(self.client.chain.head()?.last_block_hash),
        };

        // Fetch the next epoch info
        if let Ok(next_epoch) = self.get_next_epoch_view(&epoch_identifier) {
            epochs_info.push(next_epoch);
        }

        let mut current_epoch_identifier = epoch_identifier;
        for _ in 0..DEBUG_EPOCHS_TO_FETCH {
            let Ok(epoch_view) = self.get_epoch_info_view(&current_epoch_identifier) else {
                break;
            };
            let first_block = epoch_view.first_block.map(|(hash, _)| hash);
            epochs_info.push(epoch_view);

            let Some(prev_epoch_identifier) =
                get_prev_epoch_identifier(&self.client.chain, first_block)
            else {
                break;
            };
            current_epoch_identifier = prev_epoch_identifier;
        }

        Ok(epochs_info)
    }

    fn get_last_blocks_info(
        &self,
        query: DebugBlockStatusQuery,
    ) -> Result<DebugBlockStatusData, near_chain_primitives::Error> {
        let DebugBlockStatusQuery { starting_height, mode, mut num_blocks } = query;
        num_blocks = min(num_blocks, DEBUG_MAX_BLOCKS_TO_FETCH);

        let head = self.client.chain.head()?;
        let header_head = self.client.chain.header_head()?;
        let final_head = self.client.chain.final_head()?;

        let mut blocks: HashMap<CryptoHash, DebugBlockStatus> = HashMap::new();
        let mut missed_heights: Vec<MissedHeightInfo> = Vec::new();
        let mut last_epoch_id = head.epoch_id;
        let initial_gas_price = self.client.chain.genesis_block().header().next_gas_price();

        let chain_store = self.client.chain.chain_store();
        let mut height_to_fetch = starting_height.unwrap_or(header_head.height);
        height_to_fetch =
            find_first_height_to_fetch(chain_store, height_to_fetch, mode, final_head.height)?;
        let min_height_to_fetch =
            max(height_to_fetch as i64 - num_blocks as i64, chain_store.genesis_height() as i64)
                as u64;

        let mut block_hashes_to_force_fetch = HashSet::new();
        while height_to_fetch > min_height_to_fetch || !block_hashes_to_force_fetch.is_empty() {
            let block_hashes = if height_to_fetch > min_height_to_fetch {
                let block_hashes =
                    get_block_hashes_to_fetch(chain_store, height_to_fetch, final_head.height);
                if block_hashes.is_empty() {
                    missed_heights.push(MissedHeightInfo {
                        block_height: height_to_fetch,
                        block_producer: self
                            .client
                            .epoch_manager
                            .get_block_producer(&last_epoch_id, height_to_fetch)
                            .ok(),
                    });
                }
                height_to_fetch -= 1;
                block_hashes
            } else {
                let block_hashes = block_hashes_to_force_fetch.iter().cloned().collect();
                block_hashes_to_force_fetch.clear();
                block_hashes
            };
            for block_hash in block_hashes {
                if blocks.contains_key(&block_hash) {
                    continue;
                }
                let block_header = if block_hash == CryptoHash::default() {
                    self.client.chain.genesis().clone()
                } else {
                    self.client.chain.get_block_header(&block_hash)?
                };
                let block = if block_hash == CryptoHash::default() {
                    Some(self.client.chain.genesis_block().clone())
                } else {
                    self.client.chain.get_block(&block_hash).ok()
                };
                let is_on_canonical_chain =
                    match self.client.chain.get_block_by_height(block_header.height()) {
                        Ok(block) => block.hash() == &block_hash,
                        Err(_) => false,
                    };

                let block_producer = self
                    .client
                    .epoch_manager
                    .get_block_producer(block_header.epoch_id(), block_header.height())
                    .ok();

                let chunk_endorsements = self.compute_chunk_endorsements_ratio(&block);
                let congestion_control_config = self
                    .client
                    .runtime_adapter
                    .get_protocol_config(block_header.epoch_id())?
                    .runtime_config
                    .congestion_control_config;
                let block_congestion_info =
                    block.as_ref().map(|block| block.block_congestion_info());

                let chunks = match &block {
                    Some(block) => block
                        .chunks()
                        .iter_deprecated()
                        .map(|chunk| {
                            let endorsement_ratio = chunk_endorsements
                                .as_ref()
                                .map(|chunks| chunks.get(&chunk.chunk_hash()))
                                .flatten()
                                .copied();

                            let congestion_level =
                                block_congestion_info.as_ref().and_then(|shards_info| {
                                    shards_info.get(&chunk.shard_id()).map(|ext_info| {
                                        CongestionControl::new(
                                            congestion_control_config,
                                            ext_info.congestion_info,
                                            ext_info.missed_chunks_count,
                                        )
                                        .congestion_level()
                                    })
                                });

                            DebugChunkStatus {
                                shard_id: chunk.shard_id().into(),
                                chunk_hash: chunk.chunk_hash(),
                                chunk_producer: self
                                    .client
                                    .epoch_manager
                                    .get_chunk_producer_info(&ChunkProductionKey {
                                        epoch_id: *block_header.epoch_id(),
                                        height_created: block_header.height(),
                                        shard_id: chunk.shard_id(),
                                    })
                                    .map(|info| info.take_account_id())
                                    .ok(),
                                gas_used: chunk.prev_gas_used(),
                                processing_time_ms: CryptoHashTimer::get_timer_value(
                                    chunk.chunk_hash().0,
                                )
                                .map(|s| s.whole_milliseconds() as u64),
                                congestion_level,
                                congestion_info: Some(chunk.congestion_info()),
                                endorsement_ratio,
                            }
                        })
                        .collect(),
                    None => vec![],
                };

                blocks.insert(
                    block_hash,
                    DebugBlockStatus {
                        block_hash,
                        prev_block_hash: *block_header.prev_hash(),
                        block_height: block_header.height(),
                        block_producer,
                        full_block_missing: block.is_none(),
                        is_on_canonical_chain,
                        chunks,
                        processing_time_ms: CryptoHashTimer::get_timer_value(block_hash)
                            .map(|s| s.whole_milliseconds() as u64),
                        block_timestamp: block_header.raw_timestamp(),
                        gas_price_ratio: block_header.next_gas_price() as f64
                            / initial_gas_price as f64,
                    },
                );
                // TODO(robin): using last epoch id when iterating in reverse height direction is
                // not a good idea for calculating producer of missing heights. Revisit this.
                last_epoch_id = *block_header.epoch_id();
                if let Some(prev_height) = block_header.prev_height() {
                    if block_header.height() != prev_height + 1 {
                        // This block was produced using a Skip approval; make sure to fetch the
                        // previous block even if it's very far back so we can better understand
                        // the skip.
                        // TODO(robin): A better heuristic can be used to determine how far back
                        // to fetch additional blocks.
                        block_hashes_to_force_fetch.insert(*block_header.prev_hash());
                    }
                }
            }
        }

        let mut blocks = blocks.into_values().collect_vec();
        blocks.sort_by_key(|block| block.block_height);
        blocks.reverse();

        Ok(DebugBlockStatusData {
            head: head.last_block_hash,
            header_head: header_head.last_block_hash,
            missed_heights,
            blocks,
        })
    }

    /// Returns debugging information about the validator - including things like which approvals were received, which blocks/chunks will be
    /// produced and some detailed timing information.
    fn get_validator_status(&mut self) -> Result<ValidatorStatus, near_chain_primitives::Error> {
        let head = self.client.chain.head()?;
        let mut productions = vec![];

        if let Some(signer) = &self.client.validator_signer.get() {
            let validator_id = signer.validator_id().to_string();

            // We want to show some older blocks (up to DEBUG_PRODUCTION_OLD_BLOCKS_TO_SHOW in the past)
            // and new blocks (up to the current height for which we've sent approval).

            let estimated_epoch_end = max(
                head.height,
                self.client.epoch_manager.get_epoch_start_height(&head.last_block_hash)?
                    + self.client.chain.epoch_length,
            );
            let max_height = self.client.doomslug.get_largest_approval_height().clamp(
                head.height,
                min(head.height + DEBUG_MAX_PRODUCTION_BLOCKS_TO_SHOW, estimated_epoch_end),
            );

            #[allow(clippy::redundant_clone)]
            let mut epoch_id = head.epoch_id;
            for height in
                head.height.saturating_sub(DEBUG_PRODUCTION_OLD_BLOCKS_TO_SHOW)..=max_height
            {
                let mut has_block_or_chunks_to_produce = false;
                let mut production = ProductionAtHeight::default();

                // The block may be in the last epoch from head, we need to account for that.
                if let Ok(header) = self.client.chain.get_block_header_by_height(height) {
                    epoch_id = *header.epoch_id();
                }

                // And if we are the block (or chunk) producer for this height - collect some timing info.
                let block_producer = self
                    .client
                    .epoch_manager
                    .get_block_producer(&epoch_id, height)
                    .map(|f| f.to_string())
                    .unwrap_or_default();

                let shard_ids = self.client.epoch_manager.shard_ids(&epoch_id)?;

                if block_producer == validator_id {
                    // For each height - we want to collect information about received approvals.
                    let mut block_production = self.client.block_production_info.get(height);
                    block_production.block_included =
                        self.client.chain.get_block_hash_by_height(height).is_ok();
                    production.block_production = Some(block_production);
                    has_block_or_chunks_to_produce = true;
                }

                for shard_id in shard_ids {
                    let chunk_producer = self
                        .client
                        .epoch_manager
                        .get_chunk_producer_info(&ChunkProductionKey {
                            epoch_id,
                            height_created: height,
                            shard_id,
                        })
                        .map(|info| info.take_account_id().to_string())
                        .unwrap_or_default();
                    if chunk_producer == validator_id {
                        production.chunk_production.insert(
                            shard_id,
                            self.client
                                .chunk_producer
                                .chunk_production_info
                                .get(&(height, shard_id))
                                .cloned()
                                .unwrap_or_default(),
                        );
                        has_block_or_chunks_to_produce = true;
                    }
                }
                if has_block_or_chunks_to_produce {
                    productions.push((height, production));
                }
            }
        }
        productions.reverse();

        Ok(ValidatorStatus {
            validator_name: self
                .client
                .validator_signer
                .get()
                .map(|signer| signer.validator_id().clone()),
            // TODO: this might not work correctly when we're at the epoch boundary (as it will
            // just return the validators for the current epoch). We can fix it in the future, if
            // we see that this debug page is useful.
            validators: self
                .client
                .epoch_manager
                .get_epoch_block_approvers_ordered(&head.last_block_hash)
                .map(|validators| {
                    validators
                        .iter()
                        .map(|validator| {
                            (
                                validator.account_id.clone(),
                                (validator.stake_this_epoch / 10u128.pow(24)) as u64,
                            )
                        })
                        .collect::<Vec<(AccountId, u64)>>()
                })
                .ok(),
            head_height: head.height,
            shards: self.client.epoch_manager.shard_ids(&head.epoch_id).unwrap_or_default().len()
                as NumShards,
            approval_history: self.client.doomslug.get_approval_history(),
            production: productions,
            banned_chunk_producers: self
                .client
                .chunk_inclusion_tracker
                .get_banned_chunk_producers(),
        })
    }

    /// Computes the ratio of stake endorsed to all chunks in `block`.
    /// The logic is based on `validate_chunk_endorsements_in_block`.
    fn compute_chunk_endorsements_ratio(
        &self,
        block: &Option<Block>,
    ) -> Option<HashMap<ChunkHash, f64>> {
        let Some(block) = block else {
            return None;
        };
        let mut chunk_endorsements = HashMap::new();
        if block.chunks().len() != block.chunk_endorsements().len() {
            return None;
        }
        // Get the epoch id.
        let Ok(epoch_id) =
            self.client.epoch_manager.get_epoch_id_from_prev_block(block.header().prev_hash())
        else {
            return None;
        };
        // Iterate all shards and compute the endorsed stake from the endorsement signatures.
        for (chunk_header, signatures) in
            block.chunks().iter_deprecated().zip(block.chunk_endorsements())
        {
            // Validation checks.
            if chunk_header.height_included() != block.header().height() {
                chunk_endorsements.insert(chunk_header.chunk_hash(), 0.0);
                continue;
            }
            let Ok(chunk_validator_assignments) =
                self.client.epoch_manager.get_chunk_validator_assignments(
                    &epoch_id,
                    chunk_header.shard_id(),
                    chunk_header.height_created(),
                )
            else {
                chunk_endorsements.insert(chunk_header.chunk_hash(), f64::NAN);
                continue;
            };
            let ordered_chunk_validators = chunk_validator_assignments.ordered_chunk_validators();
            if ordered_chunk_validators.len() != signatures.len() {
                chunk_endorsements.insert(chunk_header.chunk_hash(), f64::NAN);
                continue;
            }
            // Compute total stake and endorsed stake.
            let mut endorsed_chunk_validators = HashMap::new();
            for (account_id, signature) in ordered_chunk_validators.iter().zip(signatures) {
                let Some(signature) = signature else { continue };
                let Ok(validator) =
                    self.client.epoch_manager.get_validator_by_account_id(&epoch_id, account_id)
                else {
                    continue;
                };
                if !ChunkEndorsement::validate_signature(
                    chunk_header.chunk_hash(),
                    signature,
                    validator.public_key(),
                ) {
                    continue;
                }
                endorsed_chunk_validators.insert(account_id, *signature.clone());
            }
            let endorsement_state =
                chunk_validator_assignments.compute_endorsement_state(endorsed_chunk_validators);
            chunk_endorsements.insert(
                chunk_header.chunk_hash(),
                endorsement_state.endorsed_stake as f64 / endorsement_state.total_stake as f64,
            );
        }
        Some(chunk_endorsements)
    }
}

fn new_peer_info_view(chain: &Chain, connected_peer_info: &ConnectedPeerInfo) -> PeerInfoView {
    let full_peer_info = &connected_peer_info.full_peer_info;
    let now = Instant::now();
    PeerInfoView {
        addr: match full_peer_info.peer_info.addr {
            Some(socket_addr) => socket_addr.to_string(),
            None => "N/A".to_string(),
        },
        account_id: full_peer_info.peer_info.account_id.clone(),
        height: full_peer_info.chain_info.last_block.map(|x| x.height),
        block_hash: full_peer_info.chain_info.last_block.map(|x| x.hash),
        is_highest_block_invalid: full_peer_info
            .chain_info
            .last_block
            .is_some_and(|x| chain.is_block_invalid(&x.hash)),
        tracked_shards: full_peer_info.chain_info.tracked_shards.clone(),
        archival: full_peer_info.chain_info.archival,
        peer_id: full_peer_info.peer_info.id.public_key().clone(),
        received_bytes_per_sec: connected_peer_info.received_bytes_per_sec,
        sent_bytes_per_sec: connected_peer_info.sent_bytes_per_sec,
        last_time_peer_requested_millis: now
            .signed_duration_since(connected_peer_info.last_time_peer_requested)
            .whole_milliseconds() as u64,
        last_time_received_message_millis: now
            .signed_duration_since(connected_peer_info.last_time_received_message)
            .whole_milliseconds() as u64,
        connection_established_time_millis: now
            .signed_duration_since(connected_peer_info.connection_established_time)
            .whole_milliseconds() as u64,
        is_outbound_peer: connected_peer_info.peer_type == PeerType::Outbound,
        nonce: connected_peer_info.nonce,
    }
}

pub(crate) fn new_network_info_view(chain: &Chain, network_info: &NetworkInfo) -> NetworkInfoView {
    NetworkInfoView {
        peer_max_count: network_info.peer_max_count,
        num_connected_peers: network_info.num_connected_peers,
        connected_peers: network_info
            .connected_peers
            .iter()
            .map(|full_peer_info| new_peer_info_view(chain, full_peer_info))
            .collect::<Vec<_>>(),
        known_producers: network_info
            .known_producers
            .iter()
            .map(|it| KnownProducerView {
                account_id: it.account_id.clone(),
                peer_id: it.peer_id.public_key().clone(),
                next_hops: it
                    .next_hops
                    .as_ref()
                    .map(|it| it.iter().map(|peer_id| peer_id.public_key().clone()).collect()),
            })
            .collect(),
        tier1_accounts_keys: network_info.tier1_accounts_keys.clone(),
        tier1_accounts_data: network_info
            .tier1_accounts_data
            .iter()
            .map(|d| AccountDataView {
                peer_id: d.peer_id.public_key().clone(),
                proxies: d
                    .proxies
                    .iter()
                    .map(|p| Tier1ProxyView {
                        addr: p.addr,
                        peer_id: p.peer_id.public_key().clone(),
                    })
                    .collect(),
                account_key: d.account_key.clone(),
                timestamp: d.timestamp,
            })
            .collect(),
        tier1_connections: network_info
            .tier1_connections
            .iter()
            .map(|full_peer_info| new_peer_info_view(chain, full_peer_info))
            .collect::<Vec<_>>(),
    }
}
