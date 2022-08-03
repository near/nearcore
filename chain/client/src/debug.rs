//! Structs in this file are used for debug purposes, and might change at any time
//! without backwards compatibility.
use crate::ClientActor;
use actix::{Context, Handler};
use borsh::BorshSerialize;
use chrono::DateTime;
use near_chain::crypto_hash_timer::CryptoHashTimer;
use near_chain::types::ValidatorInfoIdentifier;
use near_chain::{near_chain_primitives, ChainStoreAccess, RuntimeAdapter};
use near_client_primitives::debug::{
    ApprovalAtHeightStatus, BlockProduction, ChunkCollection, DebugStatus, DebugStatusResponse,
    ProductionAtHeight, ValidatorStatus,
};
use near_client_primitives::types::Error;
use near_client_primitives::{
    debug::{EpochInfoView, TrackedShardsView},
    types::StatusError,
};
use near_o11y::log_assert;
use near_performance_metrics_macros::perf;
use near_primitives::syncing::get_num_state_parts;
use near_primitives::types::{AccountId, BlockHeight, ShardId};
use near_primitives::{
    hash::CryptoHash,
    syncing::{ShardStateSyncResponseHeader, StateHeaderKey},
    types::EpochId,
    views::ValidatorInfo,
};
use near_store::DBCol;
use std::cmp::{max, min};
use std::collections::{HashMap, HashSet};

use near_client_primitives::debug::{DebugBlockStatus, DebugChunkStatus};
use near_primitives::sharding::ShardChunkHeader;
use near_primitives::time::Clock;

// Constants for debug requests.
const DEBUG_BLOCKS_TO_FETCH: u32 = 50;
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
        Self(lru::LruCache::new(PRODUCTION_TIMES_CACHE_SIZE))
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
            block_production.block_production_time = Some(Clock::utc());
            block_production.chunks_collection_time = chunk_collections;
        }
    }

    /// Record chunk collected after a block is produced if the block didn't include a chunk for the shard.
    /// If called before the block was produced, nothing happens.
    pub(crate) fn record_chunk_collected(&mut self, height: BlockHeight, shard_id: ShardId) {
        if let Some(block_production) = self.0.get_mut(&height) {
            let chunk_collections = &mut block_production.chunks_collection_time;
            // Check that chunk_collection is set and we haven't received this chunk yet.
            if let Some(chunk_collection) = chunk_collections.get_mut(shard_id as usize) {
                if chunk_collection.received_time.is_none() {
                    chunk_collection.received_time = Some(Clock::utc());
                }
            }
            // Otherwise, it means chunk_collections is not set yet, which means the block wasn't produced.
            // And we do nothing in this case.
        }
    }

    pub(crate) fn construct_chunk_collection_info(
        block_height: BlockHeight,
        epoch_id: &EpochId,
        num_shards: ShardId,
        new_chunks: &HashMap<ShardId, (ShardChunkHeader, chrono::DateTime<chrono::Utc>)>,
        runtime_adapter: &dyn RuntimeAdapter,
    ) -> Result<Vec<ChunkCollection>, Error> {
        let mut chunk_collection_info = vec![];
        for shard_id in 0..num_shards {
            if let Some((new_chunk, chunk_time)) = new_chunks.get(&shard_id) {
                let chunk_producer = runtime_adapter.get_chunk_producer(
                    epoch_id,
                    new_chunk.height_created(),
                    shard_id,
                )?;
                chunk_collection_info.push(ChunkCollection {
                    chunk_producer,
                    received_time: Some(chunk_time.clone()),
                    chunk_included: true,
                });
            } else {
                let chunk_producer =
                    runtime_adapter.get_chunk_producer(epoch_id, block_height, shard_id)?;
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

impl Handler<DebugStatus> for ClientActor {
    type Result = Result<DebugStatusResponse, StatusError>;

    #[perf]
    fn handle(&mut self, msg: DebugStatus, _ctx: &mut Context<Self>) -> Self::Result {
        match msg {
            DebugStatus::SyncStatus => {
                Ok(DebugStatusResponse::SyncStatus(self.client.sync_status.clone()))
            }
            DebugStatus::TrackedShards => {
                Ok(DebugStatusResponse::TrackedShards(self.get_tracked_shards_view()?))
            }
            DebugStatus::EpochInfo => {
                Ok(DebugStatusResponse::EpochInfo(self.get_recent_epoch_info()?))
            }
            DebugStatus::BlockStatus => {
                Ok(DebugStatusResponse::BlockStatus(self.get_last_blocks_info()?))
            }
            DebugStatus::ValidatorStatus => {
                Ok(DebugStatusResponse::ValidatorStatus(self.get_validator_status()?))
            }
        }
    }
}

impl ClientActor {
    // Gets a list of block producers and chunk-only producers for a given epoch.
    fn get_producers_for_epoch(
        &self,
        epoch_id: &EpochId,
        last_known_block_hash: &CryptoHash,
    ) -> Result<(Vec<ValidatorInfo>, Vec<String>), Error> {
        let mut block_producers_set = HashSet::new();
        let block_producers: Vec<ValidatorInfo> = self
            .client
            .runtime_adapter
            .get_epoch_block_producers_ordered(&epoch_id, &last_known_block_hash)?
            .into_iter()
            .map(|(validator_stake, is_slashed)| {
                block_producers_set.insert(validator_stake.account_id().as_str().to_owned());
                ValidatorInfo { account_id: validator_stake.take_account_id(), is_slashed }
            })
            .collect();
        let chunk_only_producers = self
            .client
            .runtime_adapter
            .get_epoch_chunk_producers(&epoch_id)?
            .iter()
            .filter_map(|producer| {
                if block_producers_set.contains(&producer.account_id().to_string()) {
                    None
                } else {
                    Some(producer.account_id().to_string())
                }
            })
            .collect::<Vec<_>>();
        Ok((block_producers, chunk_only_producers))
    }

    /// Gets the information about the epoch that contains a given block.
    /// Also returns the hash of the last block of the previous epoch.
    fn get_epoch_info_view(
        &mut self,
        current_block: CryptoHash,
        is_current_block_head: bool,
    ) -> Result<(EpochInfoView, CryptoHash), Error> {
        let epoch_start_height =
            self.client.runtime_adapter.get_epoch_start_height(&current_block)?;

        let block = self.client.chain.get_block_by_height(epoch_start_height)?.clone();
        let epoch_id = block.header().epoch_id();
        let (validators, chunk_only_producers) =
            self.get_producers_for_epoch(&epoch_id, &current_block)?;

        let shards_size_and_parts: Vec<(u64, u64)> = block
            .chunks()
            .iter()
            .enumerate()
            .map(|(shard_id, chunk)| {
                let state_root_node = self.client.runtime_adapter.get_state_root_node(
                    shard_id as u64,
                    block.hash(),
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
            .collect();

        let state_header_exists: Vec<bool> = (0..block.chunks().len())
            .map(|shard_id| {
                let key = StateHeaderKey(shard_id as u64, *block.hash()).try_to_vec();
                match key {
                    Ok(key) => {
                        if let Ok(Some(_)) =
                            self.client
                                .chain
                                .store()
                                .store()
                                .get_ser::<ShardStateSyncResponseHeader>(DBCol::StateHeaders, &key)
                        {
                            true
                        } else {
                            false
                        }
                    }
                    Err(_) => false,
                }
            })
            .collect();

        let shards_size_and_parts = shards_size_and_parts
            .iter()
            .zip(state_header_exists.iter())
            .map(|((a, b), c)| (a.clone(), b.clone(), c.clone()))
            .collect();

        let validator_info = if is_current_block_head {
            self.client
                .runtime_adapter
                .get_validator_info(ValidatorInfoIdentifier::BlockHash(current_block))?
        } else {
            self.client
                .runtime_adapter
                .get_validator_info(ValidatorInfoIdentifier::EpochId(epoch_id.clone()))?
        };
        return Ok((
            EpochInfoView {
                epoch_id: epoch_id.0,
                height: block.header().height(),
                first_block: Some((block.header().hash().clone(), block.header().timestamp())),
                block_producers: validators.to_vec(),
                chunk_only_producers,
                validator_info: Some(validator_info),
                protocol_version: self
                    .client
                    .runtime_adapter
                    .get_epoch_protocol_version(epoch_id)
                    .unwrap_or(0),
                shards_size_and_parts,
            },
            // Last block of the previous epoch.
            *block.header().prev_hash(),
        ));
    }

    fn get_next_epoch_view(&self) -> Result<EpochInfoView, Error> {
        let head = self.client.chain.head()?;
        let epoch_start_height =
            self.client.runtime_adapter.get_epoch_start_height(&head.last_block_hash)?;
        let (validators, chunk_only_producers) =
            self.get_producers_for_epoch(&&head.next_epoch_id, &head.last_block_hash)?;

        Ok(EpochInfoView {
            epoch_id: head.next_epoch_id.0,
            // Expected height of the next epoch.
            height: epoch_start_height + self.client.config.epoch_length,
            first_block: None,
            block_producers: validators,
            chunk_only_producers,
            validator_info: None,
            protocol_version: self
                .client
                .runtime_adapter
                .get_epoch_protocol_version(&head.next_epoch_id)?,
            shards_size_and_parts: vec![],
        })
    }

    fn get_tracked_shards_view(&self) -> Result<TrackedShardsView, near_chain_primitives::Error> {
        let epoch_id = self.client.chain.header_head()?.epoch_id;
        let fetch_hash = self.client.chain.header_head()?.last_block_hash;
        let me = self.client.validator_signer.as_ref().map(|x| x.validator_id().clone());

        let tracked_shards: Vec<(bool, bool)> =
            (0..self.client.runtime_adapter.num_shards(&epoch_id).unwrap())
                .map(|x| {
                    (
                        self.client.runtime_adapter.cares_about_shard(
                            me.as_ref(),
                            &fetch_hash,
                            x,
                            true,
                        ),
                        self.client.runtime_adapter.will_care_about_shard(
                            me.as_ref(),
                            &fetch_hash,
                            x,
                            true,
                        ),
                    )
                })
                .collect();
        Ok(TrackedShardsView {
            shards_tracked_this_epoch: tracked_shards.iter().map(|x| x.0).collect(),
            shards_tracked_next_epoch: tracked_shards.iter().map(|x| x.1).collect(),
        })
    }

    fn get_recent_epoch_info(
        &mut self,
    ) -> Result<Vec<EpochInfoView>, near_chain_primitives::Error> {
        // Next epoch id
        let mut epochs_info: Vec<EpochInfoView> = Vec::new();

        if let Ok(next_epoch) = self.get_next_epoch_view() {
            epochs_info.push(next_epoch);
        }
        let head = self.client.chain.head()?;
        let mut current_block = head.last_block_hash;
        for i in 0..DEBUG_EPOCHS_TO_FETCH {
            if let Ok((epoch_view, block_previous_epoch)) =
                self.get_epoch_info_view(current_block, i == 0)
            {
                current_block = block_previous_epoch;
                epochs_info.push(epoch_view);
            } else {
                break;
            }
        }
        Ok(epochs_info)
    }

    fn get_last_blocks_info(
        &mut self,
    ) -> Result<Vec<DebugBlockStatus>, near_chain_primitives::Error> {
        let head = self.client.chain.head()?;

        let mut blocks_debug: Vec<DebugBlockStatus> = Vec::new();
        let mut last_block_hash = head.last_block_hash;
        let mut last_block_timestamp: u64 = 0;
        let mut last_block_height = head.height + 1;

        let initial_gas_price = self.client.chain.genesis_block().header().gas_price();

        // Fetch last 50 blocks (we can fetch more blocks in the future if needed)
        for _ in 0..DEBUG_BLOCKS_TO_FETCH {
            let block = match self.client.chain.get_block(&last_block_hash) {
                Ok(block) => block,
                Err(_) => break,
            };
            // If there is a gap - and some blocks were not produced - make sure to report this
            // (and mention who was supposed to be a block producer).
            for height in (block.header().height() + 1..last_block_height).rev() {
                let block_producer = self
                    .client
                    .runtime_adapter
                    .get_block_producer(block.header().epoch_id(), height)
                    .ok();
                blocks_debug.push(DebugBlockStatus {
                    block_hash: CryptoHash::default(),
                    block_height: height,
                    block_producer,
                    chunks: vec![],
                    processing_time_ms: None,
                    timestamp_delta: 0,
                    gas_price_ratio: 1.0,
                });
            }

            let block_producer = self
                .client
                .runtime_adapter
                .get_block_producer(block.header().epoch_id(), block.header().height())
                .ok();

            let chunks = block
                .chunks()
                .iter()
                .map(|chunk| DebugChunkStatus {
                    shard_id: chunk.shard_id(),
                    chunk_hash: chunk.chunk_hash(),
                    chunk_producer: self
                        .client
                        .runtime_adapter
                        .get_chunk_producer(
                            block.header().epoch_id(),
                            block.header().height(),
                            chunk.shard_id(),
                        )
                        .ok(),
                    gas_used: chunk.gas_used(),
                    processing_time_ms: CryptoHashTimer::get_timer_value(chunk.chunk_hash().0)
                        .map(|s| s.as_millis() as u64),
                })
                .collect();

            blocks_debug.push(DebugBlockStatus {
                block_hash: last_block_hash,
                block_height: block.header().height(),
                block_producer: block_producer,
                chunks,
                processing_time_ms: CryptoHashTimer::get_timer_value(last_block_hash)
                    .map(|s| s.as_millis() as u64),
                timestamp_delta: if last_block_timestamp > 0 {
                    last_block_timestamp.saturating_sub(block.header().raw_timestamp())
                } else {
                    0
                },
                gas_price_ratio: block.header().gas_price() as f64 / initial_gas_price as f64,
            });
            last_block_hash = block.header().prev_hash().clone();
            last_block_timestamp = block.header().raw_timestamp();
            last_block_height = block.header().height();
        }
        Ok(blocks_debug)
    }

    /// Returns debugging information about the validator - including things like which approvals were received, which blocks/chunks will be
    /// produced and some detailed timing information.
    fn get_validator_status(&mut self) -> Result<ValidatorStatus, near_chain_primitives::Error> {
        let head = self.client.chain.head()?;
        let mut productions = vec![];

        if let Some(signer) = &self.client.validator_signer {
            let validator_id = signer.validator_id().to_string();

            // We want to show some older blocks (up to DEBUG_PRODUCTION_OLD_BLOCKS_TO_SHOW in the past)
            // and new blocks (up to the current height for which we've sent approval).

            let estimated_epoch_end = max(
                head.height,
                self.client.runtime_adapter.get_epoch_start_height(&head.last_block_hash)?
                    + self.client.chain.epoch_length,
            );
            let max_height = self.client.doomslug.get_largest_target_height().clamp(
                head.height,
                min(head.height + DEBUG_MAX_PRODUCTION_BLOCKS_TO_SHOW, estimated_epoch_end),
            );

            let mut epoch_id = head.epoch_id.clone();
            for height in
                head.height.saturating_sub(DEBUG_PRODUCTION_OLD_BLOCKS_TO_SHOW)..=max_height
            {
                let mut has_block_or_chunks_to_produce = false;
                let mut production = ProductionAtHeight::default();

                // The block may be in the last epoch from head, we need to account for that.
                if let Ok(header) = self.client.chain.get_header_by_height(height) {
                    epoch_id = header.epoch_id().clone();
                }

                // And if we are the block (or chunk) producer for this height - collect some timing info.
                let block_producer = self
                    .client
                    .runtime_adapter
                    .get_block_producer(&epoch_id, height)
                    .map(|f| f.to_string())
                    .unwrap_or_default();

                let num_chunks = self.client.runtime_adapter.num_shards(&epoch_id)?;

                if block_producer == validator_id {
                    // For each height - we want to collect information about received approvals.
                    let mut block_production = self.client.block_production_info.get(height);
                    block_production.block_included =
                        self.client.chain.get_block_hash_by_height(height).is_ok();
                    production.block_production = Some(block_production);
                    has_block_or_chunks_to_produce = true;
                }

                for shard_id in 0..num_chunks {
                    let chunk_producer = self
                        .client
                        .runtime_adapter
                        .get_chunk_producer(&epoch_id, height, shard_id)
                        .map(|f| f.to_string())
                        .unwrap_or_default();
                    if chunk_producer == validator_id {
                        production.chunk_production.insert(
                            shard_id,
                            self.client
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
                .as_ref()
                .map(|signer| signer.validator_id().clone()),
            // TODO: this might not work correctly when we're at the epoch boundary (as it will just return the validators for the current epoch).
            // We can fix it in the future, if we see that this debug page is useful.
            validators: self
                .client
                .runtime_adapter
                .get_epoch_block_approvers_ordered(&head.last_block_hash)
                .map(|validators| {
                    validators
                        .iter()
                        .map(|validator| {
                            (
                                validator.0.account_id.clone(),
                                (validator.0.stake_this_epoch / 10u128.pow(24)) as u64,
                            )
                        })
                        .collect::<Vec<(AccountId, u64)>>()
                })
                .ok(),
            head_height: head.height,
            shards: self.client.runtime_adapter.num_shards(&head.epoch_id).unwrap_or_default(),
            approval_history: self.client.doomslug.get_approval_history(),
            production: productions,
        })
    }
}
