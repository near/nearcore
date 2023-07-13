//! Readonly view of the chain and state of the database.
//! Useful for querying from RPC.

use crate::adapter::{
    AnnounceAccountRequest, BlockHeadersRequest, BlockRequest, StateRequestHeader,
    StateRequestPart, StateResponse, TxStatusRequest, TxStatusResponse,
};
use crate::{
    metrics, sync, GetChunk, GetExecutionOutcomeResponse, GetNextLightClientBlock, GetStateChanges,
    GetStateChangesInBlock, GetValidatorInfo, GetValidatorOrdered,
};
use actix::{Actor, Addr, Handler, SyncArbiter, SyncContext};
use near_async::messaging::CanSend;
use near_chain::types::{RuntimeAdapter, Tip};
use near_chain::{
    get_epoch_block_producers_view, Chain, ChainGenesis, ChainStoreAccess, DoomslugThresholdMode,
};
use near_chain_configs::{ClientConfig, ProtocolConfigView};
use near_chain_primitives::error::EpochErrorResultToChainError;
use near_client_primitives::types::{
    Error, GetBlock, GetBlockError, GetBlockProof, GetBlockProofError, GetBlockProofResponse,
    GetBlockWithMerkleTree, GetChunkError, GetExecutionOutcome, GetExecutionOutcomeError,
    GetExecutionOutcomesForBlock, GetGasPrice, GetGasPriceError, GetMaintenanceWindows,
    GetMaintenanceWindowsError, GetNextLightClientBlockError, GetProtocolConfig,
    GetProtocolConfigError, GetReceipt, GetReceiptError, GetSplitStorageInfo,
    GetSplitStorageInfoError, GetStateChangesError, GetStateChangesWithCauseInBlock,
    GetStateChangesWithCauseInBlockForTrackedShards, GetValidatorInfoError, Query, QueryError,
    TxStatus, TxStatusError,
};
use near_epoch_manager::shard_tracker::ShardTracker;
use near_epoch_manager::EpochManagerAdapter;
use near_network::types::{
    NetworkRequests, PeerManagerAdapter, PeerManagerMessageRequest, ReasonForBan,
    StateResponseInfo, StateResponseInfoV1, StateResponseInfoV2,
};
use near_o11y::{handler_debug_span, OpenTelemetrySpanExt, WithSpanContext, WithSpanContextExt};
use near_performance_metrics_macros::perf;
use near_primitives::block::{Block, BlockHeader};
use near_primitives::epoch_manager::epoch_info::EpochInfo;
use near_primitives::hash::CryptoHash;
use near_primitives::merkle::{merklize, PartialMerkleTree};
use near_primitives::network::AnnounceAccount;
use near_primitives::receipt::Receipt;
use near_primitives::sharding::ShardChunk;
use near_primitives::static_clock::StaticClock;
use near_primitives::syncing::{
    ShardStateSyncResponse, ShardStateSyncResponseHeader, ShardStateSyncResponseV1,
    ShardStateSyncResponseV2,
};
use near_primitives::types::{
    AccountId, BlockHeight, BlockId, BlockReference, EpochReference, Finality, MaybeBlockId,
    ShardId, SyncCheckpoint, TransactionOrReceiptId, ValidatorInfoIdentifier,
};
use near_primitives::views::validator_stake_view::ValidatorStakeView;
use near_primitives::views::{
    BlockView, ChunkView, EpochValidatorInfo, ExecutionOutcomeWithIdView,
    FinalExecutionOutcomeView, FinalExecutionOutcomeViewEnum, GasPriceView, LightClientBlockView,
    MaintenanceWindowsView, QueryRequest, QueryResponse, ReceiptView, SplitStorageInfoView,
    StateChangesKindsView, StateChangesView,
};
use near_store::{DBCol, COLD_HEAD_KEY, FINAL_HEAD_KEY, HEAD_KEY};
use std::cmp::Ordering;
use std::collections::{HashMap, VecDeque};
use std::hash::Hash;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};
use tracing::{debug, error, info, trace, warn};

/// Max number of queries that we keep.
const QUERY_REQUEST_LIMIT: usize = 500;
/// Waiting time between requests, in ms
const REQUEST_WAIT_TIME: u64 = 1000;

const POISONED_LOCK_ERR: &str = "The lock was poisoned.";

/// Request and response manager across all instances of ViewClientActor.
pub struct ViewClientRequestManager {
    /// Transaction query that needs to be forwarded to other shards
    pub tx_status_requests: lru::LruCache<CryptoHash, Instant>,
    /// Transaction status response
    pub tx_status_response: lru::LruCache<CryptoHash, FinalExecutionOutcomeView>,
    /// Query requests that need to be forwarded to other shards
    pub query_requests: lru::LruCache<String, Instant>,
    /// Query responses from other nodes (can be errors)
    pub query_responses: lru::LruCache<String, Result<QueryResponse, String>>,
    /// Receipt outcome requests
    pub receipt_outcome_requests: lru::LruCache<CryptoHash, Instant>,
}

/// View client provides currently committed (to the storage) view of the current chain and state.
pub struct ViewClientActor {
    pub adv: crate::adversarial::Controls,

    /// Validator account (if present).
    validator_account_id: Option<AccountId>,
    chain: Chain,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    shard_tracker: ShardTracker,
    runtime: Arc<dyn RuntimeAdapter>,
    network_adapter: PeerManagerAdapter,
    pub config: ClientConfig,
    request_manager: Arc<RwLock<ViewClientRequestManager>>,
    state_request_cache: Arc<Mutex<VecDeque<Instant>>>,
}

impl ViewClientRequestManager {
    pub fn new() -> Self {
        Self {
            tx_status_requests: lru::LruCache::new(QUERY_REQUEST_LIMIT),
            tx_status_response: lru::LruCache::new(QUERY_REQUEST_LIMIT),
            query_requests: lru::LruCache::new(QUERY_REQUEST_LIMIT),
            query_responses: lru::LruCache::new(QUERY_REQUEST_LIMIT),
            receipt_outcome_requests: lru::LruCache::new(QUERY_REQUEST_LIMIT),
        }
    }
}

impl ViewClientActor {
    /// Maximum number of state requests allowed per `view_client_throttle_period`.
    const MAX_NUM_STATE_REQUESTS: usize = 30;

    pub fn new(
        validator_account_id: Option<AccountId>,
        chain_genesis: &ChainGenesis,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        shard_tracker: ShardTracker,
        runtime: Arc<dyn RuntimeAdapter>,
        network_adapter: PeerManagerAdapter,
        config: ClientConfig,
        request_manager: Arc<RwLock<ViewClientRequestManager>>,
        adv: crate::adversarial::Controls,
    ) -> Result<Self, Error> {
        // TODO: should we create shared ChainStore that is passed to both Client and ViewClient?
        let chain = Chain::new_for_view_client(
            epoch_manager.clone(),
            shard_tracker.clone(),
            runtime.clone(),
            chain_genesis,
            DoomslugThresholdMode::TwoThirds,
            config.save_trie_changes,
        )?;
        Ok(ViewClientActor {
            adv,
            validator_account_id,
            chain,
            epoch_manager,
            shard_tracker,
            runtime,
            network_adapter,
            config,
            request_manager,
            state_request_cache: Arc::new(Mutex::new(VecDeque::default())),
        })
    }

    fn maybe_block_id_to_block_header(
        &self,
        block_id: MaybeBlockId,
    ) -> Result<BlockHeader, near_chain::Error> {
        match block_id {
            None => {
                let block_hash = self.chain.head()?.last_block_hash;
                self.chain.get_block_header(&block_hash)
            }
            Some(BlockId::Height(height)) => self.chain.get_block_header_by_height(height),
            Some(BlockId::Hash(block_hash)) => self.chain.get_block_header(&block_hash),
        }
    }

    fn need_request<K: Hash + Eq + Clone>(key: K, cache: &mut lru::LruCache<K, Instant>) -> bool {
        let now = StaticClock::instant();
        let need_request = match cache.get(&key) {
            Some(time) => now - *time > Duration::from_millis(REQUEST_WAIT_TIME),
            None => true,
        };
        if need_request {
            cache.put(key, now);
        }
        need_request
    }

    fn get_block_hash_by_finality(
        &self,
        finality: &Finality,
    ) -> Result<CryptoHash, near_chain::Error> {
        match finality {
            Finality::None => Ok(self.chain.head()?.last_block_hash),
            Finality::DoomSlug => Ok(*self.chain.head_header()?.last_ds_final_block()),
            Finality::Final => Ok(self.chain.final_head()?.last_block_hash),
        }
    }

    /// Returns block header by reference.
    ///
    /// Returns `None` if the reference is a `SyncCheckpoint::EarliestAvailable`
    /// reference and no such block exists yet.  This is typically translated by
    /// the caller into some form of ‘no sync block’ higher-level error.
    fn get_block_header_by_reference(
        &self,
        reference: &BlockReference,
    ) -> Result<Option<BlockHeader>, near_chain::Error> {
        match reference {
            BlockReference::BlockId(BlockId::Height(block_height)) => {
                self.chain.get_block_header_by_height(*block_height).map(Some)
            }
            BlockReference::BlockId(BlockId::Hash(block_hash)) => {
                self.chain.get_block_header(block_hash).map(Some)
            }
            BlockReference::Finality(finality) => self
                .get_block_hash_by_finality(finality)
                .and_then(|block_hash| self.chain.get_block_header(&block_hash))
                .map(Some),
            BlockReference::SyncCheckpoint(SyncCheckpoint::Genesis) => {
                Ok(Some(self.chain.genesis().clone()))
            }
            BlockReference::SyncCheckpoint(SyncCheckpoint::EarliestAvailable) => {
                let block_hash = match self.chain.get_earliest_block_hash()? {
                    Some(block_hash) => block_hash,
                    None => return Ok(None),
                };
                self.chain.get_block_header(&block_hash).map(Some)
            }
        }
    }

    /// Returns block by reference.
    ///
    /// Returns `None` if the reference is a `SyncCheckpoint::EarliestAvailable`
    /// reference and no such block exists yet.  This is typically translated by
    /// the caller into some form of ‘no sync block’ higher-level error.
    fn get_block_by_reference(
        &self,
        reference: &BlockReference,
    ) -> Result<Option<Block>, near_chain::Error> {
        match reference {
            BlockReference::BlockId(BlockId::Height(block_height)) => {
                self.chain.get_block_by_height(*block_height).map(Some)
            }
            BlockReference::BlockId(BlockId::Hash(block_hash)) => {
                self.chain.get_block(block_hash).map(Some)
            }
            BlockReference::Finality(finality) => self
                .get_block_hash_by_finality(finality)
                .and_then(|block_hash| self.chain.get_block(&block_hash))
                .map(Some),
            BlockReference::SyncCheckpoint(SyncCheckpoint::Genesis) => {
                Ok(Some(self.chain.genesis_block().clone()))
            }
            BlockReference::SyncCheckpoint(SyncCheckpoint::EarliestAvailable) => {
                let block_hash = match self.chain.get_earliest_block_hash()? {
                    Some(block_hash) => block_hash,
                    None => return Ok(None),
                };
                self.chain.get_block(&block_hash).map(Some)
            }
        }
    }

    /// Returns maintenance windows by account.
    fn get_maintenance_windows(
        &self,
        account_id: AccountId,
    ) -> Result<MaintenanceWindowsView, near_chain::Error> {
        let head = self.chain.head()?;
        let epoch_id = self.epoch_manager.get_epoch_id(&head.last_block_hash)?;
        let epoch_info: Arc<EpochInfo> = self.epoch_manager.get_epoch_info(&epoch_id)?;
        let num_shards = self.epoch_manager.num_shards(&epoch_id)?;
        let cur_block_info = self.epoch_manager.get_block_info(&head.last_block_hash)?;
        let next_epoch_start_height =
            self.epoch_manager.get_epoch_start_height(cur_block_info.hash())?
                + self.epoch_manager.get_epoch_config(&epoch_id)?.epoch_length;

        let mut windows: MaintenanceWindowsView = Vec::new();
        let mut start_block_of_window: Option<BlockHeight> = None;
        let last_block_of_epoch = next_epoch_start_height - 1;

        for block_height in head.height..next_epoch_start_height {
            let bp = epoch_info.sample_block_producer(block_height);
            let bp = epoch_info.get_validator(bp).account_id().clone();
            let cps: Vec<AccountId> = (0..num_shards)
                .map(|shard_id| {
                    let cp = epoch_info.sample_chunk_producer(block_height, shard_id);
                    let cp = epoch_info.get_validator(cp).account_id().clone();
                    cp
                })
                .collect();
            if account_id != bp && !cps.iter().any(|a| *a == account_id) {
                if let Some(start) = start_block_of_window {
                    if block_height == last_block_of_epoch {
                        windows.push(start..block_height + 1);
                        start_block_of_window = None;
                    }
                } else {
                    start_block_of_window = Some(block_height);
                }
            } else if let Some(start) = start_block_of_window {
                windows.push(start..block_height);
                start_block_of_window = None;
            }
        }
        if let Some(start) = start_block_of_window {
            windows.push(start..next_epoch_start_height);
        }
        Ok(windows)
    }

    fn handle_query(&mut self, msg: Query) -> Result<QueryResponse, QueryError> {
        let header = self.get_block_header_by_reference(&msg.block_reference);
        let header = match header {
            Ok(Some(header)) => Ok(header),
            Ok(None) => Err(QueryError::NoSyncedBlocks),
            Err(near_chain::near_chain_primitives::Error::DBNotFoundErr(_)) => {
                Err(QueryError::UnknownBlock { block_reference: msg.block_reference })
            }
            Err(near_chain::near_chain_primitives::Error::IOErr(err)) => {
                Err(QueryError::InternalError { error_message: err.to_string() })
            }
            Err(err) => Err(QueryError::Unreachable { error_message: err.to_string() }),
        }?;

        let account_id = match &msg.request {
            QueryRequest::ViewAccount { account_id, .. } => account_id,
            QueryRequest::ViewState { account_id, .. } => account_id,
            QueryRequest::ViewAccessKey { account_id, .. } => account_id,
            QueryRequest::ViewAccessKeyList { account_id, .. } => account_id,
            QueryRequest::CallFunction { account_id, .. } => account_id,
            QueryRequest::ViewCode { account_id, .. } => account_id,
        };
        let shard_id = self
            .epoch_manager
            .account_id_to_shard_id(account_id, header.epoch_id())
            .map_err(|err| QueryError::InternalError { error_message: err.to_string() })?;
        let shard_uid = self
            .epoch_manager
            .shard_id_to_uid(shard_id, header.epoch_id())
            .map_err(|err| QueryError::InternalError { error_message: err.to_string() })?;

        let tip = self.chain.head();
        let chunk_extra =
            self.chain.get_chunk_extra(header.hash(), &shard_uid).map_err(|err| match err {
                near_chain::near_chain_primitives::Error::DBNotFoundErr(_) => match tip {
                    Ok(tip) => {
                        let gc_stop_height = self.runtime.get_gc_stop_height(&tip.last_block_hash);
                        if !self.config.archive && header.height() < gc_stop_height {
                            QueryError::GarbageCollectedBlock {
                                block_height: header.height(),
                                block_hash: *header.hash(),
                            }
                        } else {
                            QueryError::UnavailableShard { requested_shard_id: shard_id }
                        }
                    }
                    Err(err) => QueryError::InternalError { error_message: err.to_string() },
                },
                near_chain::near_chain_primitives::Error::IOErr(error) => {
                    QueryError::InternalError { error_message: error.to_string() }
                }
                _ => QueryError::Unreachable { error_message: err.to_string() },
            })?;

        let state_root = chunk_extra.state_root();
        match self.runtime.query(
            shard_uid,
            state_root,
            header.height(),
            header.raw_timestamp(),
            header.prev_hash(),
            header.hash(),
            header.epoch_id(),
            &msg.request,
        ) {
            Ok(query_response) => Ok(query_response),
            Err(query_error) => Err(match query_error {
                near_chain::near_chain_primitives::error::QueryError::InternalError {
                    error_message,
                    ..
                } => QueryError::InternalError { error_message },
                near_chain::near_chain_primitives::error::QueryError::InvalidAccount {
                    requested_account_id,
                    block_height,
                    block_hash,
                } => QueryError::InvalidAccount { requested_account_id, block_height, block_hash },
                near_chain::near_chain_primitives::error::QueryError::UnknownAccount {
                    requested_account_id,
                    block_height,
                    block_hash,
                } => QueryError::UnknownAccount { requested_account_id, block_height, block_hash },
                near_chain::near_chain_primitives::error::QueryError::NoContractCode {
                    contract_account_id,
                    block_height,
                    block_hash,
                } => QueryError::NoContractCode { contract_account_id, block_height, block_hash },
                near_chain::near_chain_primitives::error::QueryError::UnknownAccessKey {
                    public_key,
                    block_height,
                    block_hash,
                } => QueryError::UnknownAccessKey { public_key, block_height, block_hash },
                near_chain::near_chain_primitives::error::QueryError::ContractExecutionError {
                    error_message,
                    block_hash,
                    block_height,
                } => QueryError::ContractExecutionError {
                    vm_error: error_message,
                    block_height,
                    block_hash,
                },
                near_chain::near_chain_primitives::error::QueryError::TooLargeContractState {
                    requested_account_id,
                    block_height,
                    block_hash,
                } => QueryError::TooLargeContractState {
                    contract_account_id: requested_account_id,
                    block_height,
                    block_hash,
                },
            }),
        }
    }

    fn get_tx_status(
        &mut self,
        tx_hash: CryptoHash,
        signer_account_id: AccountId,
        fetch_receipt: bool,
    ) -> Result<Option<FinalExecutionOutcomeViewEnum>, TxStatusError> {
        {
            let mut request_manager = self.request_manager.write().expect(POISONED_LOCK_ERR);
            if let Some(res) = request_manager.tx_status_response.pop(&tx_hash) {
                request_manager.tx_status_requests.pop(&tx_hash);
                return Ok(Some(FinalExecutionOutcomeViewEnum::FinalExecutionOutcome(res)));
            }
        }

        let head = self.chain.head()?;
        let target_shard_id = self
            .epoch_manager
            .account_id_to_shard_id(&signer_account_id, &head.epoch_id)
            .map_err(|err| TxStatusError::InternalError(err.to_string()))?;
        // Check if we are tracking this shard.
        if self.shard_tracker.care_about_shard(
            self.validator_account_id.as_ref(),
            &head.prev_block_hash,
            target_shard_id,
            true,
        ) {
            match self.chain.get_final_transaction_result(&tx_hash) {
                Ok(tx_result) => {
                    let res = if fetch_receipt {
                        let final_result =
                            self.chain.get_final_transaction_result_with_receipt(tx_result)?;
                        FinalExecutionOutcomeViewEnum::FinalExecutionOutcomeWithReceipt(
                            final_result,
                        )
                    } else {
                        FinalExecutionOutcomeViewEnum::FinalExecutionOutcome(tx_result)
                    };
                    Ok(Some(res))
                }
                Err(near_chain::Error::DBNotFoundErr(_)) => {
                    if self.chain.get_execution_outcome(&tx_hash).is_ok() {
                        Ok(None)
                    } else {
                        Err(TxStatusError::MissingTransaction(tx_hash))
                    }
                }
                Err(err) => {
                    warn!(target: "client", ?err, "Error trying to get transaction result");
                    Err(err.into())
                }
            }
        } else {
            let mut request_manager = self.request_manager.write().expect(POISONED_LOCK_ERR);
            if Self::need_request(tx_hash, &mut request_manager.tx_status_requests) {
                let target_shard_id = self
                    .epoch_manager
                    .account_id_to_shard_id(&signer_account_id, &head.epoch_id)
                    .map_err(|err| TxStatusError::InternalError(err.to_string()))?;
                let validator = self.chain.find_validator_for_forwarding(target_shard_id)?;

                self.network_adapter.send(PeerManagerMessageRequest::NetworkRequests(
                    NetworkRequests::TxStatus(validator, signer_account_id, tx_hash),
                ));
            }
            Ok(None)
        }
    }

    fn retrieve_headers(
        &mut self,
        hashes: Vec<CryptoHash>,
    ) -> Result<Vec<BlockHeader>, near_chain::Error> {
        self.chain.retrieve_headers(hashes, sync::header::MAX_BLOCK_HEADERS, None)
    }

    fn check_signature_account_announce(
        &self,
        announce_account: &AnnounceAccount,
    ) -> Result<bool, Error> {
        let announce_hash = announce_account.hash();
        let head = self.chain.head()?;

        self.epoch_manager
            .verify_validator_signature(
                &announce_account.epoch_id,
                &head.last_block_hash,
                &announce_account.account_id,
                announce_hash.as_ref(),
                &announce_account.signature,
            )
            .map_err(|e| e.into())
    }

    fn check_state_sync_request(&self) -> bool {
        let mut cache = self.state_request_cache.lock().expect(POISONED_LOCK_ERR);
        let now = StaticClock::instant();
        while let Some(&instant) = cache.front() {
            if now.saturating_duration_since(instant) > self.config.view_client_throttle_period {
                cache.pop_front();
            } else {
                // Assume that time is linear. While in different threads there might be some small differences,
                // it should not matter in practice.
                break;
            }
        }
        if cache.len() >= Self::MAX_NUM_STATE_REQUESTS {
            return false;
        }
        cache.push_back(now);
        true
    }
}

impl Actor for ViewClientActor {
    type Context = SyncContext<Self>;
}

impl Handler<WithSpanContext<Query>> for ViewClientActor {
    type Result = Result<QueryResponse, QueryError>;

    #[perf]
    fn handle(&mut self, msg: WithSpanContext<Query>, _: &mut Self::Context) -> Self::Result {
        let (_span, msg) = handler_debug_span!(target: "client", msg);
        let _timer = metrics::VIEW_CLIENT_MESSAGE_TIME.with_label_values(&["Query"]).start_timer();
        self.handle_query(msg)
    }
}

/// Handles retrieving block from the chain.
impl Handler<WithSpanContext<GetBlock>> for ViewClientActor {
    type Result = Result<BlockView, GetBlockError>;

    #[perf]
    fn handle(&mut self, msg: WithSpanContext<GetBlock>, _: &mut Self::Context) -> Self::Result {
        let (_span, msg) = handler_debug_span!(target: "client", msg);
        let _timer =
            metrics::VIEW_CLIENT_MESSAGE_TIME.with_label_values(&["GetBlock"]).start_timer();
        let block = self.get_block_by_reference(&msg.0)?.ok_or(GetBlockError::NotSyncedYet)?;
        let block_author = self
            .epoch_manager
            .get_block_producer(block.header().epoch_id(), block.header().height())
            .into_chain_error()?;
        Ok(BlockView::from_author_block(block_author, block))
    }
}

impl Handler<WithSpanContext<GetBlockWithMerkleTree>> for ViewClientActor {
    type Result = Result<(BlockView, Arc<PartialMerkleTree>), GetBlockError>;

    #[perf]
    fn handle(
        &mut self,
        msg: WithSpanContext<GetBlockWithMerkleTree>,
        ctx: &mut Self::Context,
    ) -> Self::Result {
        let (_span, msg) = handler_debug_span!(target: "client", msg);
        let _timer = metrics::VIEW_CLIENT_MESSAGE_TIME
            .with_label_values(&["GetBlockWithMerkleTree"])
            .start_timer();
        let block_view = self.handle(GetBlock(msg.0).with_span_context(), ctx)?;
        self.chain
            .store()
            .get_block_merkle_tree(&block_view.header.hash)
            .map(|merkle_tree| (block_view, merkle_tree))
            .map_err(|e| e.into())
    }
}

impl Handler<WithSpanContext<GetChunk>> for ViewClientActor {
    type Result = Result<ChunkView, GetChunkError>;

    #[perf]
    fn handle(&mut self, msg: WithSpanContext<GetChunk>, _: &mut Self::Context) -> Self::Result {
        let (_span, msg) = handler_debug_span!(target: "client", msg);
        let _timer =
            metrics::VIEW_CLIENT_MESSAGE_TIME.with_label_values(&["GetChunk"]).start_timer();
        let get_chunk_from_block = |block: Block,
                                    shard_id: ShardId,
                                    chain: &Chain|
         -> Result<ShardChunk, near_chain::Error> {
            let chunk_header = block
                .chunks()
                .get(shard_id as usize)
                .ok_or_else(|| near_chain::Error::InvalidShardId(shard_id))?
                .clone();
            let chunk_hash = chunk_header.chunk_hash();
            let chunk = chain.get_chunk(&chunk_hash)?;
            let res = ShardChunk::with_header(ShardChunk::clone(&chunk), chunk_header).ok_or(
                near_chain::Error::Other(format!(
                    "Mismatched versions for chunk with hash {}",
                    chunk_hash.0
                )),
            )?;
            Ok(res)
        };

        let chunk = match msg {
            GetChunk::ChunkHash(chunk_hash) => {
                let chunk = self.chain.get_chunk(&chunk_hash)?;
                ShardChunk::clone(&chunk)
            }
            GetChunk::BlockHash(block_hash, shard_id) => {
                let block = self.chain.get_block(&block_hash)?;
                get_chunk_from_block(block, shard_id, &self.chain)?
            }
            GetChunk::Height(height, shard_id) => {
                let block = self.chain.get_block_by_height(height)?;
                get_chunk_from_block(block, shard_id, &self.chain)?
            }
        };

        let chunk_inner = chunk.cloned_header().take_inner();
        let epoch_id = self
            .epoch_manager
            .get_epoch_id_from_prev_block(chunk_inner.prev_block_hash())
            .into_chain_error()?;
        let author = self
            .epoch_manager
            .get_chunk_producer(&epoch_id, chunk_inner.height_created(), chunk_inner.shard_id())
            .into_chain_error()?;

        Ok(ChunkView::from_author_chunk(author, chunk))
    }
}

impl Handler<WithSpanContext<TxStatus>> for ViewClientActor {
    type Result = Result<Option<FinalExecutionOutcomeViewEnum>, TxStatusError>;

    #[perf]
    fn handle(&mut self, msg: WithSpanContext<TxStatus>, _: &mut Self::Context) -> Self::Result {
        let (_span, msg) = handler_debug_span!(target: "client", msg);
        let _timer =
            metrics::VIEW_CLIENT_MESSAGE_TIME.with_label_values(&["TxStatus"]).start_timer();
        self.get_tx_status(msg.tx_hash, msg.signer_account_id, msg.fetch_receipt)
    }
}

impl Handler<WithSpanContext<GetValidatorInfo>> for ViewClientActor {
    type Result = Result<EpochValidatorInfo, GetValidatorInfoError>;

    #[perf]
    fn handle(
        &mut self,
        msg: WithSpanContext<GetValidatorInfo>,
        _: &mut Self::Context,
    ) -> Self::Result {
        let (_span, msg) = handler_debug_span!(target: "client", msg);
        let _timer = metrics::VIEW_CLIENT_MESSAGE_TIME
            .with_label_values(&["GetValidatorInfo"])
            .start_timer();
        let epoch_identifier = match msg.epoch_reference {
            EpochReference::EpochId(id) => {
                // By `EpochId` we can get only cached epochs.
                // Request for not finished epoch by `EpochId` will return an error because epoch has not been cached yet
                // If the requested one is current ongoing we need to handle it like `Latest`
                let tip = self.chain.header_head()?;
                if tip.epoch_id == id {
                    ValidatorInfoIdentifier::BlockHash(tip.last_block_hash)
                } else {
                    ValidatorInfoIdentifier::EpochId(id)
                }
            }
            EpochReference::BlockId(block_id) => {
                let block_header = match block_id {
                    BlockId::Hash(h) => self.chain.get_block_header(&h)?,
                    BlockId::Height(h) => self.chain.get_block_header_by_height(h)?,
                };
                let next_block_hash =
                    self.chain.store().get_next_block_hash(block_header.hash())?;
                let next_block_header = self.chain.get_block_header(&next_block_hash)?;
                if block_header.epoch_id() != next_block_header.epoch_id()
                    && block_header.next_epoch_id() == next_block_header.epoch_id()
                {
                    ValidatorInfoIdentifier::EpochId(block_header.epoch_id().clone())
                } else {
                    return Err(GetValidatorInfoError::ValidatorInfoUnavailable);
                }
            }
            EpochReference::Latest => {
                // use header head because this is latest from the perspective of epoch manager
                ValidatorInfoIdentifier::BlockHash(self.chain.header_head()?.last_block_hash)
            }
        };
        Ok(self.epoch_manager.get_validator_info(epoch_identifier).into_chain_error()?)
    }
}

impl Handler<WithSpanContext<GetValidatorOrdered>> for ViewClientActor {
    type Result = Result<Vec<ValidatorStakeView>, GetValidatorInfoError>;

    #[perf]
    fn handle(
        &mut self,
        msg: WithSpanContext<GetValidatorOrdered>,
        _: &mut Self::Context,
    ) -> Self::Result {
        let (_span, msg) = handler_debug_span!(target: "client", msg);
        let _timer = metrics::VIEW_CLIENT_MESSAGE_TIME
            .with_label_values(&["GetValidatorOrdered"])
            .start_timer();
        Ok(self.maybe_block_id_to_block_header(msg.block_id).and_then(|header| {
            get_epoch_block_producers_view(
                header.epoch_id(),
                header.prev_hash(),
                self.epoch_manager.as_ref(),
            )
        })?)
    }
}
/// Returns a list of change kinds per account in a store for a given block.
impl Handler<WithSpanContext<GetStateChangesInBlock>> for ViewClientActor {
    type Result = Result<StateChangesKindsView, GetStateChangesError>;

    #[perf]
    fn handle(
        &mut self,
        msg: WithSpanContext<GetStateChangesInBlock>,
        _: &mut Self::Context,
    ) -> Self::Result {
        let (_span, msg) = handler_debug_span!(target: "client", msg);
        let _timer = metrics::VIEW_CLIENT_MESSAGE_TIME
            .with_label_values(&["GetStateChangesInBlock"])
            .start_timer();
        Ok(self
            .chain
            .store()
            .get_state_changes_in_block(&msg.block_hash)?
            .into_iter()
            .map(Into::into)
            .collect())
    }
}

/// Returns a list of changes in a store for a given block filtering by the state changes request.
impl Handler<WithSpanContext<GetStateChanges>> for ViewClientActor {
    type Result = Result<StateChangesView, GetStateChangesError>;

    #[perf]
    fn handle(
        &mut self,
        msg: WithSpanContext<GetStateChanges>,
        _: &mut Self::Context,
    ) -> Self::Result {
        let (_span, msg) = handler_debug_span!(target: "client", msg);
        let _timer =
            metrics::VIEW_CLIENT_MESSAGE_TIME.with_label_values(&["GetStateChanges"]).start_timer();
        Ok(self
            .chain
            .store()
            .get_state_changes(&msg.block_hash, &msg.state_changes_request.into())?
            .into_iter()
            .map(Into::into)
            .collect())
    }
}

/// Returns a list of changes in a store with causes for a given block.
impl Handler<WithSpanContext<GetStateChangesWithCauseInBlock>> for ViewClientActor {
    type Result = Result<StateChangesView, GetStateChangesError>;

    #[perf]
    fn handle(
        &mut self,
        msg: WithSpanContext<GetStateChangesWithCauseInBlock>,
        _: &mut Self::Context,
    ) -> Self::Result {
        let (_span, msg) = handler_debug_span!(target: "client", msg);
        let _timer = metrics::VIEW_CLIENT_MESSAGE_TIME
            .with_label_values(&["GetStateChangesWithCauseInBlock"])
            .start_timer();
        Ok(self
            .chain
            .store()
            .get_state_changes_with_cause_in_block(&msg.block_hash)?
            .into_iter()
            .map(Into::into)
            .collect())
    }
}

/// Returns a hashmap where the key represents the ShardID and the value
/// is the list of changes in a store with causes for a given block.
impl Handler<WithSpanContext<GetStateChangesWithCauseInBlockForTrackedShards>> for ViewClientActor {
    type Result = Result<HashMap<ShardId, StateChangesView>, GetStateChangesError>;

    #[perf]
    fn handle(
        &mut self,
        msg: WithSpanContext<GetStateChangesWithCauseInBlockForTrackedShards>,
        _: &mut Self::Context,
    ) -> Self::Result {
        let (_span, msg) = handler_debug_span!(target: "client", msg);
        let _timer = metrics::VIEW_CLIENT_MESSAGE_TIME
            .with_label_values(&["GetStateChangesWithCauseInBlockForTrackedShards"])
            .start_timer();
        let state_changes_with_cause_in_block =
            self.chain.store().get_state_changes_with_cause_in_block(&msg.block_hash)?;

        let mut state_changes_with_cause_split_by_shard_id: HashMap<ShardId, StateChangesView> =
            HashMap::new();
        for state_change_with_cause in state_changes_with_cause_in_block {
            let account_id = state_change_with_cause.value.affected_account_id();
            let shard_id = match self
                .epoch_manager
                .account_id_to_shard_id(account_id, &msg.epoch_id)
            {
                Ok(shard_id) => shard_id,
                Err(err) => {
                    return Err(GetStateChangesError::IOError { error_message: err.to_string() })
                }
            };

            let state_changes =
                state_changes_with_cause_split_by_shard_id.entry(shard_id).or_default();
            state_changes.push(state_change_with_cause.into());
        }

        Ok(state_changes_with_cause_split_by_shard_id)
    }
}

/// Returns the next light client block, given the hash of the last block known to the light client.
/// There are three cases:
///  1. The last block known to the light client is in the same epoch as the tip:
///     - Then return the last known final block, as long as it's more recent that the last known
///  2. The last block known to the light client is in the epoch preceding that of the tip:
///     - Same as above
///  3. Otherwise, return the last final block in the epoch that follows that of the last block known
///     to the light client
impl Handler<WithSpanContext<GetNextLightClientBlock>> for ViewClientActor {
    type Result = Result<Option<Arc<LightClientBlockView>>, GetNextLightClientBlockError>;

    #[perf]
    fn handle(
        &mut self,
        msg: WithSpanContext<GetNextLightClientBlock>,
        _: &mut Self::Context,
    ) -> Self::Result {
        let (_span, msg) = handler_debug_span!(target: "client", msg);
        let _timer = metrics::VIEW_CLIENT_MESSAGE_TIME
            .with_label_values(&["GetNextLightClientBlock"])
            .start_timer();
        let last_block_header = self.chain.get_block_header(&msg.last_block_hash)?;
        let last_epoch_id = last_block_header.epoch_id().clone();
        let last_next_epoch_id = last_block_header.next_epoch_id().clone();
        let last_height = last_block_header.height();
        let head = self.chain.head()?;

        if last_epoch_id == head.epoch_id || last_next_epoch_id == head.epoch_id {
            let head_header = self.chain.get_block_header(&head.last_block_hash)?;
            let ret = Chain::create_light_client_block(
                &head_header,
                self.epoch_manager.as_ref(),
                self.chain.store(),
            )?;

            if ret.inner_lite.height <= last_height {
                Ok(None)
            } else {
                Ok(Some(Arc::new(ret)))
            }
        } else {
            match self.chain.store().get_epoch_light_client_block(&last_next_epoch_id.0) {
                Ok(light_block) => Ok(Some(light_block)),
                Err(e) => {
                    if let near_chain::Error::DBNotFoundErr(_) = e {
                        Ok(None)
                    } else {
                        Err(e.into())
                    }
                }
            }
        }
    }
}

impl Handler<WithSpanContext<GetExecutionOutcome>> for ViewClientActor {
    type Result = Result<GetExecutionOutcomeResponse, GetExecutionOutcomeError>;

    #[perf]
    fn handle(
        &mut self,
        msg: WithSpanContext<GetExecutionOutcome>,
        _: &mut Self::Context,
    ) -> Self::Result {
        let (_span, msg) = handler_debug_span!(target: "client", msg);
        let _timer = metrics::VIEW_CLIENT_MESSAGE_TIME
            .with_label_values(&["GetExecutionOutcome"])
            .start_timer();
        let (id, account_id) = match msg.id {
            TransactionOrReceiptId::Transaction { transaction_hash, sender_id } => {
                (transaction_hash, sender_id)
            }
            TransactionOrReceiptId::Receipt { receipt_id, receiver_id } => {
                (receipt_id, receiver_id)
            }
        };
        match self.chain.get_execution_outcome(&id) {
            Ok(outcome) => {
                let mut outcome_proof = outcome;
                let epoch_id =
                    self.chain.get_block(&outcome_proof.block_hash)?.header().epoch_id().clone();
                let target_shard_id = self
                    .epoch_manager
                    .account_id_to_shard_id(&account_id, &epoch_id)
                    .into_chain_error()?;
                let res = self.chain.get_next_block_hash_with_new_chunk(
                    &outcome_proof.block_hash,
                    target_shard_id,
                )?;
                if let Some((h, target_shard_id)) = res {
                    outcome_proof.block_hash = h;
                    // Here we assume the number of shards is small so this reconstruction
                    // should be fast
                    let outcome_roots = self
                        .chain
                        .get_block(&h)?
                        .chunks()
                        .iter()
                        .map(|header| header.outcome_root())
                        .collect::<Vec<_>>();
                    if target_shard_id >= (outcome_roots.len() as u64) {
                        return Err(GetExecutionOutcomeError::InconsistentState {
                            number_or_shards: outcome_roots.len(),
                            execution_outcome_shard_id: target_shard_id,
                        });
                    }
                    Ok(GetExecutionOutcomeResponse {
                        outcome_proof: outcome_proof.into(),
                        outcome_root_proof: merklize(&outcome_roots).1[target_shard_id as usize]
                            .clone(),
                    })
                } else {
                    Err(GetExecutionOutcomeError::NotConfirmed { transaction_or_receipt_id: id })
                }
            }
            Err(near_chain::Error::DBNotFoundErr(_)) => {
                let head = self.chain.head()?;
                let target_shard_id = self
                    .epoch_manager
                    .account_id_to_shard_id(&account_id, &head.epoch_id)
                    .into_chain_error()?;
                if self.shard_tracker.care_about_shard(
                    self.validator_account_id.as_ref(),
                    &head.last_block_hash,
                    target_shard_id,
                    true,
                ) {
                    Err(GetExecutionOutcomeError::UnknownTransactionOrReceipt {
                        transaction_or_receipt_id: id,
                    })
                } else {
                    Err(GetExecutionOutcomeError::UnavailableShard {
                        transaction_or_receipt_id: id,
                        shard_id: target_shard_id,
                    })
                }
            }
            Err(err) => Err(err.into()),
        }
    }
}

/// Extract the list of execution outcomes that were produced in a given block
/// (including those created for local receipts).
impl Handler<WithSpanContext<GetExecutionOutcomesForBlock>> for ViewClientActor {
    type Result = Result<HashMap<ShardId, Vec<ExecutionOutcomeWithIdView>>, String>;

    #[perf]
    fn handle(
        &mut self,
        msg: WithSpanContext<GetExecutionOutcomesForBlock>,
        _: &mut Self::Context,
    ) -> Self::Result {
        let (_span, msg) = handler_debug_span!(target: "client", msg);
        let _timer = metrics::VIEW_CLIENT_MESSAGE_TIME
            .with_label_values(&["GetExecutionOutcomesForBlock"])
            .start_timer();
        Ok(self
            .chain
            .store()
            .get_block_execution_outcomes(&msg.block_hash)
            .map_err(|e| e.to_string())?
            .into_iter()
            .map(|(k, v)| (k, v.into_iter().map(Into::into).collect()))
            .collect())
    }
}

impl Handler<WithSpanContext<GetReceipt>> for ViewClientActor {
    type Result = Result<Option<ReceiptView>, GetReceiptError>;

    #[perf]
    fn handle(&mut self, msg: WithSpanContext<GetReceipt>, _: &mut Self::Context) -> Self::Result {
        let (_span, msg) = handler_debug_span!(target: "client", msg);
        let _timer =
            metrics::VIEW_CLIENT_MESSAGE_TIME.with_label_values(&["GetReceipt"]).start_timer();
        Ok(self
            .chain
            .store()
            .get_receipt(&msg.receipt_id)?
            .map(|receipt| Receipt::clone(&receipt).into()))
    }
}

impl Handler<WithSpanContext<GetBlockProof>> for ViewClientActor {
    type Result = Result<GetBlockProofResponse, GetBlockProofError>;

    #[perf]
    fn handle(
        &mut self,
        msg: WithSpanContext<GetBlockProof>,
        _: &mut Self::Context,
    ) -> Self::Result {
        let (_span, msg) = handler_debug_span!(target: "client", msg);
        let _timer =
            metrics::VIEW_CLIENT_MESSAGE_TIME.with_label_values(&["GetBlockProof"]).start_timer();
        let block_header = self.chain.get_block_header(&msg.block_hash)?;
        let head_block_header = self.chain.get_block_header(&msg.head_block_hash)?;
        self.chain.check_blocks_final_and_canonical(&[&block_header, &head_block_header])?;
        let block_header_lite = block_header.into();
        let proof = self.chain.get_block_proof(&msg.block_hash, &msg.head_block_hash)?;
        Ok(GetBlockProofResponse { block_header_lite, proof })
    }
}

impl Handler<WithSpanContext<GetProtocolConfig>> for ViewClientActor {
    type Result = Result<ProtocolConfigView, GetProtocolConfigError>;

    #[perf]
    fn handle(
        &mut self,
        msg: WithSpanContext<GetProtocolConfig>,
        _: &mut Self::Context,
    ) -> Self::Result {
        let (_span, msg) = handler_debug_span!(target: "client", msg);
        let _timer = metrics::VIEW_CLIENT_MESSAGE_TIME
            .with_label_values(&["GetProtocolConfig"])
            .start_timer();
        let header = match self.get_block_header_by_reference(&msg.0)? {
            None => {
                return Err(GetProtocolConfigError::UnknownBlock("EarliestAvailable".to_string()))
            }
            Some(header) => header,
        };
        let config = self.runtime.get_protocol_config(header.epoch_id())?;
        Ok(config.into())
    }
}

#[cfg(feature = "test_features")]
use crate::NetworkAdversarialMessage;

#[cfg(feature = "test_features")]
impl Handler<WithSpanContext<NetworkAdversarialMessage>> for ViewClientActor {
    type Result = Option<u64>;

    #[perf]
    fn handle(
        &mut self,
        msg: WithSpanContext<NetworkAdversarialMessage>,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        let (_span, msg) = handler_debug_span!(target: "client", msg);
        let _timer = metrics::VIEW_CLIENT_MESSAGE_TIME
            .with_label_values(&["NetworkAdversarialMessage"])
            .start_timer();
        match msg {
            NetworkAdversarialMessage::AdvDisableDoomslug => {
                info!(target: "adversary", "Turning Doomslug off");
                self.adv.set_disable_doomslug(true);
            }
            NetworkAdversarialMessage::AdvDisableHeaderSync => {
                info!(target: "adversary", "Blocking header sync");
                self.adv.set_disable_header_sync(true);
            }
            NetworkAdversarialMessage::AdvSwitchToHeight(height) => {
                info!(target: "adversary", "Switching to height");
                let mut chain_store_update = self.chain.mut_store().store_update();
                chain_store_update.save_largest_target_height(height);
                chain_store_update
                    .adv_save_latest_known(height)
                    .expect("adv method should not fail");
                chain_store_update.commit().expect("adv method should not fail");
            }
            _ => panic!("invalid adversary message"),
        }
        None
    }
}

impl Handler<WithSpanContext<TxStatusRequest>> for ViewClientActor {
    type Result = Option<Box<FinalExecutionOutcomeView>>;

    #[perf]
    fn handle(
        &mut self,
        msg: WithSpanContext<TxStatusRequest>,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        let (_span, msg) = handler_debug_span!(target: "client", msg);
        let _timer =
            metrics::VIEW_CLIENT_MESSAGE_TIME.with_label_values(&["TxStatusRequest"]).start_timer();
        let TxStatusRequest { tx_hash, signer_account_id } = msg;
        if let Ok(Some(result)) = self.get_tx_status(tx_hash, signer_account_id, false) {
            Some(Box::new(result.into_outcome()))
        } else {
            None
        }
    }
}

impl Handler<WithSpanContext<TxStatusResponse>> for ViewClientActor {
    type Result = ();

    #[perf]
    fn handle(
        &mut self,
        msg: WithSpanContext<TxStatusResponse>,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        let (_span, msg) = handler_debug_span!(target: "client", msg);
        let _timer = metrics::VIEW_CLIENT_MESSAGE_TIME
            .with_label_values(&["TxStatusResponse"])
            .start_timer();
        let TxStatusResponse(tx_result) = msg;
        let tx_hash = tx_result.transaction_outcome.id;
        let mut request_manager = self.request_manager.write().expect(POISONED_LOCK_ERR);
        if request_manager.tx_status_requests.pop(&tx_hash).is_some() {
            request_manager.tx_status_response.put(tx_hash, *tx_result);
        }
    }
}

impl Handler<WithSpanContext<BlockRequest>> for ViewClientActor {
    type Result = Option<Box<Block>>;

    #[perf]
    fn handle(
        &mut self,
        msg: WithSpanContext<BlockRequest>,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        let (_span, msg) = handler_debug_span!(target: "client", msg);
        let _timer =
            metrics::VIEW_CLIENT_MESSAGE_TIME.with_label_values(&["BlockRequest"]).start_timer();
        let BlockRequest(hash) = msg;
        if let Ok(block) = self.chain.get_block(&hash) {
            Some(Box::new(block))
        } else {
            None
        }
    }
}

impl Handler<WithSpanContext<BlockHeadersRequest>> for ViewClientActor {
    type Result = Option<Vec<BlockHeader>>;

    #[perf]
    fn handle(
        &mut self,
        msg: WithSpanContext<BlockHeadersRequest>,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        let (_span, msg) = handler_debug_span!(target: "client", msg);
        let _timer = metrics::VIEW_CLIENT_MESSAGE_TIME
            .with_label_values(&["BlockHeadersRequest"])
            .start_timer();
        let BlockHeadersRequest(hashes) = msg;

        if self.adv.disable_header_sync() {
            None
        } else if let Ok(headers) = self.retrieve_headers(hashes) {
            Some(headers)
        } else {
            None
        }
    }
}

impl Handler<WithSpanContext<StateRequestHeader>> for ViewClientActor {
    type Result = Option<StateResponse>;

    #[perf]
    fn handle(
        &mut self,
        msg: WithSpanContext<StateRequestHeader>,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        let (_span, msg) = handler_debug_span!(target: "client", msg);
        let _timer = metrics::VIEW_CLIENT_MESSAGE_TIME
            .with_label_values(&["StateRequestHeader"])
            .start_timer();
        let StateRequestHeader { shard_id, sync_hash } = msg;
        if !self.check_state_sync_request() {
            return None;
        }
        let state_response = match self.chain.check_sync_hash_validity(&sync_hash) {
            Ok(true) => {
                let header = match self.chain.get_state_response_header(shard_id, sync_hash) {
                    Ok(header) => Some(header),
                    Err(e) => {
                        error!(target: "sync", "Cannot build sync header (get_state_response_header): {}", e);
                        None
                    }
                };
                match header {
                    None => ShardStateSyncResponse::V1(ShardStateSyncResponseV1 {
                        header: None,
                        part: None,
                    }),
                    Some(ShardStateSyncResponseHeader::V1(header)) => {
                        ShardStateSyncResponse::V1(ShardStateSyncResponseV1 {
                            header: Some(header),
                            part: None,
                        })
                    }
                    Some(ShardStateSyncResponseHeader::V2(header)) => {
                        ShardStateSyncResponse::V2(ShardStateSyncResponseV2 {
                            header: Some(header),
                            part: None,
                        })
                    }
                }
            }
            Ok(false) => {
                warn!(target: "sync", "sync_hash {:?} didn't pass validation, possible malicious behavior", sync_hash);
                return None;
            }
            Err(e) => match e {
                near_chain::Error::DBNotFoundErr(_) => {
                    // This case may appear in case of latency in epoch switching.
                    // Request sender is ready to sync but we still didn't get the block.
                    info!(target: "sync", "Can't get sync_hash block {:?} for state request header", sync_hash);
                    ShardStateSyncResponse::V1(ShardStateSyncResponseV1 {
                        header: None,
                        part: None,
                    })
                }
                _ => {
                    error!(target: "sync", "Failed to verify sync_hash {:?} validity, {:?}", sync_hash, e);
                    ShardStateSyncResponse::V1(ShardStateSyncResponseV1 {
                        header: None,
                        part: None,
                    })
                }
            },
        };
        match state_response {
            ShardStateSyncResponse::V1(state_response) => {
                let info = StateResponseInfo::V1(StateResponseInfoV1 {
                    shard_id,
                    sync_hash,
                    state_response,
                });
                Some(StateResponse(Box::new(info)))
            }
            state_response @ ShardStateSyncResponse::V2(_) => {
                let info = StateResponseInfo::V2(StateResponseInfoV2 {
                    shard_id,
                    sync_hash,
                    state_response,
                });
                Some(StateResponse(Box::new(info)))
            }
        }
    }
}

impl Handler<WithSpanContext<StateRequestPart>> for ViewClientActor {
    type Result = Option<StateResponse>;

    #[perf]
    fn handle(
        &mut self,
        msg: WithSpanContext<StateRequestPart>,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        let (_span, msg) = handler_debug_span!(target: "client", msg);
        let _timer = metrics::VIEW_CLIENT_MESSAGE_TIME
            .with_label_values(&["StateRequestPart"])
            .start_timer();
        let StateRequestPart { shard_id, sync_hash, part_id } = msg;
        if !self.check_state_sync_request() {
            return None;
        }
        tracing::debug!(target: "sync", ?shard_id, ?sync_hash, ?part_id, "Computing state request part");
        let state_response = match self.chain.check_sync_hash_validity(&sync_hash) {
            Ok(true) => {
                let part = match self.chain.get_state_response_part(shard_id, part_id, sync_hash) {
                    Ok(part) => Some((part_id, part)),
                    Err(e) => {
                        error!(target: "sync", "Cannot build sync part #{:?} (get_state_response_part): {}", part_id, e);
                        None
                    }
                };

                trace!(target: "sync", "Finish computation for state request part {} {} {}", shard_id, sync_hash, part_id);
                ShardStateSyncResponseV1 { header: None, part }
            }
            Ok(false) => {
                warn!(target: "sync", "sync_hash {:?} didn't pass validation, possible malicious behavior", sync_hash);
                return None;
            }
            Err(e) => match e {
                near_chain::Error::DBNotFoundErr(_) => {
                    // This case may appear in case of latency in epoch switching.
                    // Request sender is ready to sync but we still didn't get the block.
                    info!(target: "sync", "Can't get sync_hash block {:?} for state request part", sync_hash);
                    ShardStateSyncResponseV1 { header: None, part: None }
                }
                _ => {
                    error!(target: "sync", "Failed to verify sync_hash {:?} validity, {:?}", sync_hash, e);
                    ShardStateSyncResponseV1 { header: None, part: None }
                }
            },
        };
        let info =
            StateResponseInfo::V1(StateResponseInfoV1 { shard_id, sync_hash, state_response });
        Some(StateResponse(Box::new(info)))
    }
}

impl Handler<WithSpanContext<AnnounceAccountRequest>> for ViewClientActor {
    type Result = Result<Vec<AnnounceAccount>, ReasonForBan>;

    #[perf]
    fn handle(
        &mut self,
        msg: WithSpanContext<AnnounceAccountRequest>,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        let (_span, msg) = handler_debug_span!(target: "client", msg);
        let _timer = metrics::VIEW_CLIENT_MESSAGE_TIME
            .with_label_values(&["AnnounceAccountRequest"])
            .start_timer();
        let AnnounceAccountRequest(announce_accounts) = msg;

        let mut filtered_announce_accounts = Vec::new();

        for (announce_account, last_epoch) in announce_accounts {
            // Keep the announcement if it is newer than the last announcement from
            // the same account.
            if let Some(last_epoch) = last_epoch {
                match self.epoch_manager.compare_epoch_id(&announce_account.epoch_id, &last_epoch) {
                    Ok(Ordering::Greater) => {}
                    _ => continue,
                }
            }

            match self.check_signature_account_announce(&announce_account) {
                Ok(true) => {
                    filtered_announce_accounts.push(announce_account);
                }
                // TODO(gprusak): Here we ban for broadcasting accounts which have been slashed
                // according to BlockInfo for the current chain tip. It is unfair,
                // given that peers do not have perfectly synchronized heads:
                // - AFAIU each block can introduce a slashed account, so the announcement
                //   could be OK at the moment that peer has sent it out.
                // - the current epoch_id is not related to announce_account.epoch_id,
                //   so it carry a perfectly valid (outdated) information.
                Ok(false) => {
                    return Err(ReasonForBan::InvalidSignature);
                }
                // Filter out this account. This covers both good reasons to ban the peer:
                // - signature didn't match the data and public_key.
                // - account is not a validator for the given epoch
                // and cases when we were just unable to validate the data (so we shouldn't
                // ban), for example when the node is not aware of the public key for the given
                // (account_id,epoch_id) pair.
                // We currently do NOT ban the peer for either.
                // TODO(gprusak): consider whether we should change that.
                Err(e) => {
                    debug!(target: "view_client", "Failed to validate account announce signature: {}", e);
                }
            }
        }
        Ok(filtered_announce_accounts)
    }
}

impl Handler<WithSpanContext<GetGasPrice>> for ViewClientActor {
    type Result = Result<GasPriceView, GetGasPriceError>;

    #[perf]
    fn handle(
        &mut self,
        msg: WithSpanContext<GetGasPrice>,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        let (_span, msg) = handler_debug_span!(target: "client", msg);
        let _timer =
            metrics::VIEW_CLIENT_MESSAGE_TIME.with_label_values(&["GetGasPrice"]).start_timer();
        let header = self.maybe_block_id_to_block_header(msg.block_id);
        Ok(GasPriceView { gas_price: header?.gas_price() })
    }
}

impl Handler<WithSpanContext<GetMaintenanceWindows>> for ViewClientActor {
    type Result = Result<MaintenanceWindowsView, GetMaintenanceWindowsError>;

    #[perf]
    fn handle(
        &mut self,
        msg: WithSpanContext<GetMaintenanceWindows>,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        let (_span, msg) = handler_debug_span!(target: "client", msg);
        Ok(self.get_maintenance_windows(msg.account_id)?)
    }
}

impl Handler<WithSpanContext<GetSplitStorageInfo>> for ViewClientActor {
    type Result = Result<SplitStorageInfoView, GetSplitStorageInfoError>;

    fn handle(
        &mut self,
        msg: WithSpanContext<GetSplitStorageInfo>,
        _: &mut Self::Context,
    ) -> Self::Result {
        let (_span, _msg) = handler_debug_span!(target: "client", msg);
        let _d = delay_detector::DelayDetector::new(|| "client get split storage info".into());

        let store = self.chain.store().store();
        let head = store.get_ser::<Tip>(DBCol::BlockMisc, HEAD_KEY)?;
        let final_head = store.get_ser::<Tip>(DBCol::BlockMisc, FINAL_HEAD_KEY)?;
        let cold_head = store.get_ser::<Tip>(DBCol::BlockMisc, COLD_HEAD_KEY)?;

        let hot_db_kind = store.get_db_kind()?.map(|kind| kind.to_string());

        Ok(SplitStorageInfoView {
            head_height: head.map(|tip| tip.height),
            final_head_height: final_head.map(|tip| tip.height),
            cold_head_height: cold_head.map(|tip| tip.height),
            hot_db_kind,
        })
    }
}

/// Starts the View Client in a new arbiter (thread).
pub fn start_view_client(
    validator_account_id: Option<AccountId>,
    chain_genesis: ChainGenesis,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    shard_tracker: ShardTracker,
    runtime: Arc<dyn RuntimeAdapter>,
    network_adapter: PeerManagerAdapter,
    config: ClientConfig,
    adv: crate::adversarial::Controls,
) -> Addr<ViewClientActor> {
    let request_manager = Arc::new(RwLock::new(ViewClientRequestManager::new()));
    SyncArbiter::start(config.view_client_threads, move || {
        ViewClientActor::new(
            validator_account_id.clone(),
            &chain_genesis,
            epoch_manager.clone(),
            shard_tracker.clone(),
            runtime.clone(),
            network_adapter.clone(),
            config.clone(),
            request_manager.clone(),
            adv.clone(),
        )
        .unwrap()
    })
}
