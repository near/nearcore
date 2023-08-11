//! Structs in this file are used for debug purposes, and might change at any time
//! without backwards compatibility.
use crate::ClientActor;
use actix::{Context, Handler};
use borsh::BorshSerialize;
use itertools::Itertools;
use near_chain::crypto_hash_timer::CryptoHashTimer;
use near_chain::{near_chain_primitives, Chain, ChainStoreAccess};
use near_client_primitives::debug::{
    ApprovalAtHeightStatus, BlockProduction, ChunkCollection, DebugBlockStatusData, DebugStatus,
    DebugStatusResponse, MissedHeightInfo, ProductionAtHeight, ValidatorStatus,
};
use near_client_primitives::types::Error;
use near_client_primitives::{
    debug::{EpochInfoView, TrackedShardsView},
    types::StatusError,
};
use near_epoch_manager::EpochManagerAdapter;
use near_o11y::{handler_debug_span, log_assert, OpenTelemetrySpanExt, WithSpanContext};
use near_performance_metrics_macros::perf;
use near_primitives::syncing::get_num_state_parts;
use near_primitives::types::{AccountId, BlockHeight, ShardId, ValidatorInfoIdentifier};
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
use near_network::types::{ConnectedPeerInfo, NetworkInfo, PeerType};
use near_primitives::sharding::ShardChunkHeader;
use near_primitives::static_clock::StaticClock;
use near_primitives::views::{
    AccountDataView, KnownProducerView, NetworkInfoView, PeerInfoView, Tier1ProxyView,
};

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
            block_production.block_production_time = Some(StaticClock::utc());
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
                    chunk_collection.received_time = Some(StaticClock::utc());
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
        new_chunks: &HashMap<ShardId, (ShardChunkHeader, chrono::DateTime<chrono::Utc>, AccountId)>,
        epoch_manager: &dyn EpochManagerAdapter,
    ) -> Result<Vec<ChunkCollection>, Error> {
        let mut chunk_collection_info = vec![];
        for shard_id in 0..num_shards {
            if let Some((_, chunk_time, chunk_producer)) = new_chunks.get(&shard_id) {
                chunk_collection_info.push(ChunkCollection {
                    chunk_producer: chunk_producer.clone(),
                    received_time: Some(*chunk_time),
                    chunk_included: true,
                });
            } else {
                let chunk_producer =
                    epoch_manager.get_chunk_producer(epoch_id, block_height, shard_id)?;
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

impl Handler<WithSpanContext<DebugStatus>> for ClientActor {
    type Result = Result<DebugStatusResponse, StatusError>;

    #[perf]
    fn handle(
        &mut self,
        msg: WithSpanContext<DebugStatus>,
        _ctx: &mut Context<Self>,
    ) -> Self::Result {
        let (_span, msg) = handler_debug_span!(target: "client", msg);
        match msg {
            DebugStatus::SyncStatus => {
                Ok(DebugStatusResponse::SyncStatus(self.client.sync_status.clone().into()))
            }
            DebugStatus::TrackedShards => {
                Ok(DebugStatusResponse::TrackedShards(self.get_tracked_shards_view()?))
            }
            DebugStatus::EpochInfo => {
                Ok(DebugStatusResponse::EpochInfo(self.get_recent_epoch_info()?))
            }
            DebugStatus::BlockStatus(height) => {
                Ok(DebugStatusResponse::BlockStatus(self.get_last_blocks_info(height)?))
            }
            DebugStatus::ValidatorStatus => {
                Ok(DebugStatusResponse::ValidatorStatus(self.get_validator_status()?))
            }
            DebugStatus::CatchupStatus => {
                Ok(DebugStatusResponse::CatchupStatus(self.client.get_catchup_status()?))
            }
            DebugStatus::RequestedStateParts => Ok(DebugStatusResponse::RequestedStateParts(
                self.client.chain.get_requested_state_parts(),
            )),
            DebugStatus::ChainProcessingStatus => Ok(DebugStatusResponse::ChainProcessingStatus(
                self.client.chain.get_chain_processing_info(),
            )),
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
            .epoch_manager
            .get_epoch_block_producers_ordered(epoch_id, last_known_block_hash)?
            .into_iter()
            .map(|(validator_stake, is_slashed)| {
                block_producers_set.insert(validator_stake.account_id().as_str().to_owned());
                ValidatorInfo { account_id: validator_stake.take_account_id(), is_slashed }
            })
            .collect();
        let chunk_only_producers = self
            .client
            .epoch_manager
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
            self.client.epoch_manager.get_epoch_start_height(&current_block)?;

        let block = self.client.chain.get_block_by_height(epoch_start_height)?;
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
                        matches!(
                            self.client
                                .chain
                                .store()
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

        let validator_info = if is_current_block_head {
            self.client
                .epoch_manager
                .get_validator_info(ValidatorInfoIdentifier::BlockHash(current_block))?
        } else {
            self.client
                .epoch_manager
                .get_validator_info(ValidatorInfoIdentifier::EpochId(epoch_id.clone()))?
        };
        return Ok((
            EpochInfoView {
                epoch_id: epoch_id.0,
                height: block.header().height(),
                first_block: Some((*block.header().hash(), block.header().timestamp())),
                block_producers: validators.to_vec(),
                chunk_only_producers,
                validator_info: Some(validator_info),
                protocol_version: self
                    .client
                    .epoch_manager
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
            self.client.epoch_manager.get_epoch_start_height(&head.last_block_hash)?;
        let (validators, chunk_only_producers) =
            self.get_producers_for_epoch(&head.next_epoch_id, &head.last_block_hash)?;

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
                .epoch_manager
                .get_epoch_protocol_version(&head.next_epoch_id)?,
            shards_size_and_parts: vec![],
        })
    }

    fn get_tracked_shards_view(&self) -> Result<TrackedShardsView, near_chain_primitives::Error> {
        let epoch_id = self.client.chain.header_head()?.epoch_id;
        let fetch_hash = self.client.chain.header_head()?.last_block_hash;
        let me = self.client.validator_signer.as_ref().map(|x| x.validator_id().clone());

        let tracked_shards: Vec<(bool, bool)> = (0..self
            .client
            .epoch_manager
            .num_shards(&epoch_id)
            .unwrap())
            .map(|x| {
                (
                    self.client.shard_tracker.care_about_shard(me.as_ref(), &fetch_hash, x, true),
                    self.client.shard_tracker.will_care_about_shard(
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
        starting_height: Option<BlockHeight>,
    ) -> Result<DebugBlockStatusData, near_chain_primitives::Error> {
        let head = self.client.chain.head()?;
        let header_head = self.client.chain.header_head()?;

        let mut blocks: HashMap<CryptoHash, DebugBlockStatus> = HashMap::new();
        let mut missed_heights: Vec<MissedHeightInfo> = Vec::new();
        let mut last_epoch_id = head.epoch_id;
        let initial_gas_price = self.client.chain.genesis_block().header().gas_price();

        let mut height_to_fetch = starting_height.unwrap_or(header_head.height);
        let min_height_to_fetch =
            max(height_to_fetch as i64 - DEBUG_BLOCKS_TO_FETCH as i64, 0) as u64;
        let mut block_hashes_to_force_fetch = HashSet::new();
        while height_to_fetch > min_height_to_fetch || !block_hashes_to_force_fetch.is_empty() {
            let block_hashes = if height_to_fetch > min_height_to_fetch {
                let block_hashes: Vec<CryptoHash> = self
                    .client
                    .chain
                    .store()
                    .get_all_header_hashes_by_height(height_to_fetch)?
                    .into_iter()
                    .collect();
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

                let chunks = match &block {
                    Some(block) => block
                        .chunks()
                        .iter()
                        .map(|chunk| DebugChunkStatus {
                            shard_id: chunk.shard_id(),
                            chunk_hash: chunk.chunk_hash(),
                            chunk_producer: self
                                .client
                                .epoch_manager
                                .get_chunk_producer(
                                    block_header.epoch_id(),
                                    block_header.height(),
                                    chunk.shard_id(),
                                )
                                .ok(),
                            gas_used: chunk.gas_used(),
                            processing_time_ms: CryptoHashTimer::get_timer_value(
                                chunk.chunk_hash().0,
                            )
                            .map(|s| s.as_millis() as u64),
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
                            .map(|s| s.as_millis() as u64),
                        block_timestamp: block_header.raw_timestamp(),
                        gas_price_ratio: block_header.gas_price() as f64 / initial_gas_price as f64,
                    },
                );
                // TODO(robin): using last epoch id when iterating in reverse height direction is
                // not a good idea for calculating producer of missing heights. Revisit this.
                last_epoch_id = block_header.epoch_id().clone();
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
        Ok(DebugBlockStatusData {
            head: head.last_block_hash,
            header_head: header_head.last_block_hash,
            missed_heights,
            blocks: blocks.into_values().collect(),
        })
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
                self.client.epoch_manager.get_epoch_start_height(&head.last_block_hash)?
                    + self.client.chain.epoch_length,
            );
            let max_height = self.client.doomslug.get_largest_approval_height().clamp(
                head.height,
                min(head.height + DEBUG_MAX_PRODUCTION_BLOCKS_TO_SHOW, estimated_epoch_end),
            );

            #[allow(clippy::redundant_clone)]
            let mut epoch_id = head.epoch_id.clone();
            for height in
                head.height.saturating_sub(DEBUG_PRODUCTION_OLD_BLOCKS_TO_SHOW)..=max_height
            {
                let mut has_block_or_chunks_to_produce = false;
                let mut production = ProductionAtHeight::default();

                // The block may be in the last epoch from head, we need to account for that.
                if let Ok(header) = self.client.chain.get_block_header_by_height(height) {
                    epoch_id = header.epoch_id().clone();
                }

                // And if we are the block (or chunk) producer for this height - collect some timing info.
                let block_producer = self
                    .client
                    .epoch_manager
                    .get_block_producer(&epoch_id, height)
                    .map(|f| f.to_string())
                    .unwrap_or_default();

                let num_chunks = self.client.epoch_manager.num_shards(&epoch_id)?;

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
                        .epoch_manager
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
                .epoch_manager
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
            shards: self.client.epoch_manager.num_shards(&head.epoch_id).unwrap_or_default(),
            approval_history: self.client.doomslug.get_approval_history(),
            production: productions,
            banned_chunk_producers: self
                .client
                .do_not_include_chunks_from
                .iter()
                .map(|(k, _)| k.clone())
                .sorted()
                .group_by(|(k, _)| k.clone())
                .into_iter()
                .map(|(k, vs)| (k, vs.map(|(_, v)| v).collect()))
                .collect(),
        })
    }
}
fn new_peer_info_view(chain: &Chain, connected_peer_info: &ConnectedPeerInfo) -> PeerInfoView {
    let full_peer_info = &connected_peer_info.full_peer_info;
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
            .map(|x| chain.is_block_invalid(&x.hash))
            .unwrap_or_default(),
        tracked_shards: full_peer_info.chain_info.tracked_shards.clone(),
        archival: full_peer_info.chain_info.archival,
        peer_id: full_peer_info.peer_info.id.public_key().clone(),
        received_bytes_per_sec: connected_peer_info.received_bytes_per_sec,
        sent_bytes_per_sec: connected_peer_info.sent_bytes_per_sec,
        last_time_peer_requested_millis: connected_peer_info
            .last_time_peer_requested
            .elapsed()
            .whole_milliseconds() as u64,
        last_time_received_message_millis: connected_peer_info
            .last_time_received_message
            .elapsed()
            .whole_milliseconds() as u64,
        connection_established_time_millis: connected_peer_info
            .connection_established_time
            .elapsed()
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
                timestamp: chrono::DateTime::from_utc(
                    chrono::NaiveDateTime::from_timestamp_opt(d.timestamp.unix_timestamp(), 0)
                        .unwrap(),
                    chrono::Utc,
                ),
            })
            .collect(),
        tier1_connections: network_info
            .tier1_connections
            .iter()
            .map(|full_peer_info| new_peer_info_view(chain, full_peer_info))
            .collect::<Vec<_>>(),
    }
}
