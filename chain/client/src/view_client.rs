//! Readonly view of the chain and state of the database.
//! Useful for querying from RPC.

use actix::{Actor, Addr, Handler, SyncArbiter, SyncContext};
use std::cmp::Ordering;
use std::collections::{HashMap, VecDeque};
use std::hash::Hash;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use tracing::{debug, error, info, trace, warn};

use near_chain::{
    get_epoch_block_producers_view, Chain, ChainGenesis, ChainStoreAccess, DoomslugThresholdMode,
    RuntimeAdapter,
};
use near_chain_configs::{ClientConfig, ProtocolConfigView};
use near_client_primitives::types::{
    Error, GetBlock, GetBlockError, GetBlockProof, GetBlockProofError, GetBlockProofResponse,
    GetBlockWithMerkleTree, GetChunkError, GetExecutionOutcome, GetExecutionOutcomeError,
    GetExecutionOutcomesForBlock, GetGasPrice, GetGasPriceError, GetNextLightClientBlockError,
    GetProtocolConfig, GetProtocolConfigError, GetReceipt, GetReceiptError, GetStateChangesError,
    GetStateChangesWithCauseInBlock, GetStateChangesWithCauseInBlockForTrackedShards,
    GetValidatorInfoError, Query, QueryError, TxStatus, TxStatusError,
};
#[cfg(feature = "test_features")]
use near_network::types::NetworkAdversarialMessage;
use near_network::types::{
    NetworkRequests, PeerManagerAdapter, PeerManagerMessageRequest, ReasonForBan,
    StateResponseInfo, StateResponseInfoV1, StateResponseInfoV2,
};
use near_o11y::{handler_debug_span, OpenTelemetrySpanExt, WithSpanContext, WithSpanContextExt};
use near_performance_metrics_macros::perf;
use near_primitives::block::{Block, BlockHeader};
use near_primitives::hash::CryptoHash;
use near_primitives::merkle::{merklize, PartialMerkleTree};
use near_primitives::network::AnnounceAccount;
use near_primitives::receipt::Receipt;
use near_primitives::sharding::{ChunkHash, ShardChunk};
use near_primitives::syncing::{
    ShardStateSyncResponse, ShardStateSyncResponseHeader, ShardStateSyncResponseV1,
    ShardStateSyncResponseV2,
};
use near_primitives::time::Clock;
use near_primitives::types::{
    AccountId, BlockHeight, BlockId, BlockReference, EpochId, EpochReference, Finality,
    MaybeBlockId, ShardId, SyncCheckpoint, TransactionOrReceiptId, ValidatorInfoIdentifier,
};
use near_primitives::views::validator_stake_view::ValidatorStakeView;
use near_primitives::views::{
    BlockView, ChunkView, EpochValidatorInfo, ExecutionOutcomeWithIdView,
    FinalExecutionOutcomeView, FinalExecutionOutcomeViewEnum, GasPriceView, LightClientBlockView,
    QueryRequest, QueryResponse, ReceiptView, StateChangesKindsView, StateChangesView,
};
use near_store::Temperature;

use crate::adapter::{
    AnnounceAccountRequest, BlockHeadersRequest, BlockRequest, StateRequestHeader,
    StateRequestPart, StateResponse, TxStatusRequest, TxStatusResponse,
};
use crate::{
    metrics, sync, GetChunk, GetExecutionOutcomeResponse, GetNextLightClientBlock, GetStateChanges,
    GetStateChangesInBlock, GetValidatorInfo, GetValidatorOrdered,
};

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
    hot_chain: Chain,
    #[cfg(feature = "cold_store")]
    cold_chain: Option<Chain>,
    hot_runtime_adapter: Arc<dyn RuntimeAdapter>,
    #[cfg(feature = "cold_store")]
    cold_runtime_adapter: Option<Arc<dyn RuntimeAdapter>>,
    network_adapter: Arc<dyn PeerManagerAdapter>,
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
        hot_runtime_adapter: Arc<dyn RuntimeAdapter>,
        #[cfg(feature = "cold_store")] cold_runtime_adapter: Option<Arc<dyn RuntimeAdapter>>,
        network_adapter: Arc<dyn PeerManagerAdapter>,
        config: ClientConfig,
        request_manager: Arc<RwLock<ViewClientRequestManager>>,
        adv: crate::adversarial::Controls,
    ) -> Result<Self, Error> {
        // TODO: should we create shared ChainStore that is passed to both Client and ViewClient?
        let hot_chain = Chain::new_for_view_client(
            hot_runtime_adapter.clone(),
            chain_genesis,
            DoomslugThresholdMode::TwoThirds,
            !config.archive,
        )?;
        #[cfg(feature = "cold_store")]
        let cold_chain = cold_runtime_adapter
            .as_ref()
            .map(|runtime_adapter| {
                Chain::new_for_view_client(
                    runtime_adapter.clone(),
                    chain_genesis,
                    DoomslugThresholdMode::TwoThirds,
                    !config.archive,
                )
            })
            .transpose()?;
        Ok(ViewClientActor {
            adv,
            validator_account_id,
            hot_chain,
            #[cfg(feature = "cold_store")]
            cold_chain,
            hot_runtime_adapter,
            #[cfg(feature = "cold_store")]
            cold_runtime_adapter,
            network_adapter,
            config,
            request_manager,
            state_request_cache: Arc::new(Mutex::new(VecDeque::default())),
        })
    }

    /// Returns chain with data for given temperature.
    ///
    /// Panics if requested Cold chain but the node is not running with cold
    /// storage.
    fn chain(&self, temp: Temperature) -> &Chain {
        match temp {
            Temperature::Hot => &self.hot_chain,
            #[cfg(feature = "cold_store")]
            Temperature::Cold => self.cold_chain.as_ref().unwrap(),
        }
    }

    /// Returns runtime adapter with data for given temperature.
    ///
    /// Panics if requested Cold runtime adapter but the node is not running
    /// with cold storage.
    fn runtime_adapter(&self, temp: Temperature) -> &dyn RuntimeAdapter {
        match temp {
            Temperature::Hot => &*self.hot_runtime_adapter,
            #[cfg(feature = "cold_store")]
            Temperature::Cold => self.cold_runtime_adapter.as_deref().unwrap(),
        }
    }

    fn need_request<K: Hash + Eq + Clone>(key: K, cache: &mut lru::LruCache<K, Instant>) -> bool {
        let now = Clock::instant();
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
            Finality::None => Ok(self.hot_chain.head()?.last_block_hash),
            Finality::DoomSlug => Ok(*self.hot_chain.head_header()?.last_ds_final_block()),
            Finality::Final => Ok(self.hot_chain.final_head()?.last_block_hash),
        }
    }

    /// Determines temperature of a block with given block hash.
    ///
    /// Returned temperature allows fetching all data related to block at given
    /// height as well as data about previous and next blocks (if any).
    fn get_temperature(&self, _block_hash: &CryptoHash) -> Result<Temperature, near_chain::Error> {
        // TODO(#6119): Implement temperature detection.
        Ok(Temperature::Hot)
    }

    /// Determines temperature of a block with given block height.
    ///
    /// Returned temperature allows fetching all data related to block with
    /// given hash as well as data about previous and next blocks (if any).
    fn get_temperature_by_height(
        &self,
        _block_height: BlockHeight,
    ) -> Result<Temperature, near_chain::Error> {
        // TODO(#6119): Implement temperature detection.
        Ok(Temperature::Hot)
    }

    /// Determines temperature of a chunk with given chunk hash.
    fn get_temperature_by_chunk_hash(
        &self,
        _chunk_hash: &ChunkHash,
    ) -> Result<Temperature, near_chain::Error> {
        // TODO(#6119): Implement temperature detection.
        Ok(Temperature::Hot)
    }

    /// Determines temperature of data from transaction hash.
    fn get_temperature_by_tx_hash(
        &self,
        _tx_hash: &CryptoHash,
        _sender_id: &AccountId,
    ) -> Result<Temperature, near_chain::Error> {
        // TODO(#6119): Implement temperature detection.
        Ok(Temperature::Hot)
    }

    /// Determines temperature of data from receipt hash.
    fn get_temperature_by_rx_hash(
        &self,
        _rx_hash: &CryptoHash,
        _receiver_id: Option<&AccountId>,
    ) -> Result<Temperature, near_chain::Error> {
        // TODO(#6119): Implement temperature detection.
        Ok(Temperature::Hot)
    }

    /// Determines temperature of a chunk with given chunk hash.
    fn get_temperature_by_epoch_id(
        &self,
        _epoch_id: &EpochId,
    ) -> Result<Temperature, near_chain::Error> {
        // TODO(#6119): Implement temperature detection.
        Ok(Temperature::Hot)
    }

    /// Fetches block header by its hash and determines block’s temperature.
    fn get_block_header(
        &self,
        block_hash: &CryptoHash,
    ) -> Result<(Temperature, BlockHeader), near_chain::Error> {
        // DBCol::BlocHeader is not garbage collected so it can always be read
        // from hot chain.
        // TODO(#3488): Switch to reading from proper storage once BlockHeader
        // is garbage collected.
        let header = self.hot_chain.get_block_header(block_hash)?;
        let temp = self.get_temperature_by_height(header.height())?;
        Ok((temp, header))
    }

    /// Fetches block header by its height and determines block’s temperature.
    fn get_block_header_by_height(
        &self,
        block_height: BlockHeight,
    ) -> Result<(Temperature, BlockHeader), near_chain::Error> {
        let temp = self.get_temperature_by_height(block_height)?;
        let header = self.chain(temp).get_block_header_by_height(block_height)?;
        Ok((temp, header))
    }

    fn get_block_header_by_maybe_block_id(
        &self,
        block_id: MaybeBlockId,
    ) -> Result<(Temperature, BlockHeader), near_chain::Error> {
        match block_id {
            None => {
                let block_hash = self.hot_chain.head()?.last_block_hash;
                let header = self.hot_chain.get_block_header(&block_hash)?;
                Ok((Temperature::Hot, header))
            }
            Some(BlockId::Height(height)) => self.get_block_header_by_height(height),
            Some(BlockId::Hash(block_hash)) => self.get_block_header(&block_hash),
        }
    }

    /// Fetches block by its hash and determines block’s temperature.
    fn get_block(
        &self,
        block_hash: &CryptoHash,
    ) -> Result<(Temperature, Block), near_chain::Error> {
        let temp = self.get_temperature(block_hash)?;
        let block = self.chain(temp).get_block(block_hash)?;
        Ok((temp, block))
    }

    /// Fetches block by its height and determines block’s temperature.
    fn get_block_by_height(
        &self,
        block_height: BlockHeight,
    ) -> Result<(Temperature, Block), near_chain::Error> {
        let temp = self.get_temperature_by_height(block_height)?;
        let block = self.chain(temp).get_block_by_height(block_height)?;
        Ok((temp, block))
    }

    /// Returns block header by reference.
    ///
    /// Returns `None` if the reference is a `SyncCheckpoint::EarliestAvailable`
    /// reference and no such block exists yet.  This is typically translated by
    /// the caller into some form of ‘no sync block’ higher-level error.
    fn get_block_header_by_reference(
        &self,
        reference: &BlockReference,
    ) -> Result<Option<(Temperature, BlockHeader)>, near_chain::Error> {
        match reference {
            BlockReference::BlockId(BlockId::Height(block_height)) => {
                self.get_block_header_by_height(*block_height).map(Some)
            }
            BlockReference::BlockId(BlockId::Hash(block_hash)) => {
                self.get_block_header(block_hash).map(Some)
            }
            BlockReference::Finality(finality) => self
                .get_block_hash_by_finality(finality)
                .and_then(|block_hash| self.get_block_header(&block_hash))
                .map(Some),
            BlockReference::SyncCheckpoint(SyncCheckpoint::Genesis) => {
                let header = self.hot_chain.genesis().clone();
                Ok(Some((Temperature::Hot, header)))
            }
            BlockReference::SyncCheckpoint(SyncCheckpoint::EarliestAvailable) => {
                // TODO(#6119): What should this actually do?
                match self.hot_chain.get_earliest_block_hash()? {
                    Some(block_hash) => self.get_block_header(&block_hash).map(Some),
                    None => Ok(None),
                }
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
    ) -> Result<Option<(Temperature, Block)>, near_chain::Error> {
        match reference {
            BlockReference::BlockId(BlockId::Height(block_height)) => {
                self.get_block_by_height(*block_height).map(Some)
            }
            BlockReference::BlockId(BlockId::Hash(block_hash)) => {
                self.get_block(block_hash).map(Some)
            }
            BlockReference::Finality(finality) => self
                .get_block_hash_by_finality(finality)
                .and_then(|block_hash| self.get_block(&block_hash))
                .map(Some),
            BlockReference::SyncCheckpoint(SyncCheckpoint::Genesis) => {
                let block = self.hot_chain.genesis_block().clone();
                Ok(Some((Temperature::Hot, block)))
            }
            BlockReference::SyncCheckpoint(SyncCheckpoint::EarliestAvailable) => {
                // TODO(#6119): What should this actually do?
                match self.hot_chain.get_earliest_block_hash()? {
                    Some(block_hash) => self.get_block(&block_hash).map(Some),
                    None => Ok(None),
                }
            }
        }
    }

    fn handle_query(&mut self, msg: Query) -> Result<QueryResponse, QueryError> {
        let res = self.get_block_header_by_reference(&msg.block_reference);
        let (temp, header) = match res {
            Ok(Some(res)) => Ok(res),
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
        let runtime_adapter = self.runtime_adapter(temp);
        let shard_id = runtime_adapter
            .account_id_to_shard_id(account_id, header.epoch_id())
            .map_err(|err| QueryError::InternalError { error_message: err.to_string() })?;
        let shard_uid = runtime_adapter
            .shard_id_to_uid(shard_id, header.epoch_id())
            .map_err(|err| QueryError::InternalError { error_message: err.to_string() })?;

        let chain = self.chain(temp);
        let tip = chain.head();
        let chunk_extra =
            chain.get_chunk_extra(header.hash(), &shard_uid).map_err(|err| match err {
                near_chain::near_chain_primitives::Error::DBNotFoundErr(_) => match tip {
                    Ok(tip) => {
                        let gc_stop_height =
                            runtime_adapter.get_gc_stop_height(&tip.last_block_hash);
                        if !self.config.archive && header.height() < gc_stop_height {
                            QueryError::GarbageCollectedBlock {
                                block_height: header.height(),
                                block_hash: header.hash().clone(),
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
        match runtime_adapter.query(
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
        let temp = self.get_temperature_by_tx_hash(&tx_hash, &signer_account_id)?;

        {
            let mut request_manager = self.request_manager.write().expect(POISONED_LOCK_ERR);
            if let Some(res) = request_manager.tx_status_response.pop(&tx_hash) {
                request_manager.tx_status_requests.pop(&tx_hash);
                return Ok(Some(FinalExecutionOutcomeViewEnum::FinalExecutionOutcome(res)));
            }
        }

        let chain = self.chain(temp);
        let runtime_adapter = self.runtime_adapter(temp);
        let head = chain.head()?;
        let target_shard_id = runtime_adapter
            .account_id_to_shard_id(&signer_account_id, &head.epoch_id)
            .map_err(|err| TxStatusError::InternalError(err.to_string()))?;
        // Check if we are tracking this shard.
        if runtime_adapter.cares_about_shard(
            self.validator_account_id.as_ref(),
            &head.prev_block_hash,
            target_shard_id,
            true,
        ) {
            match chain.get_final_transaction_result(&tx_hash) {
                Ok(tx_result) => {
                    let res = if fetch_receipt {
                        let final_result =
                            chain.get_final_transaction_result_with_receipt(tx_result)?;
                        FinalExecutionOutcomeViewEnum::FinalExecutionOutcomeWithReceipt(
                            final_result,
                        )
                    } else {
                        FinalExecutionOutcomeViewEnum::FinalExecutionOutcome(tx_result)
                    };
                    Ok(Some(res))
                }
                Err(near_chain::Error::DBNotFoundErr(_)) => {
                    if chain.get_execution_outcome(&tx_hash).is_ok() {
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
                let target_shard_id = runtime_adapter
                    .account_id_to_shard_id(&signer_account_id, &head.epoch_id)
                    .map_err(|err| TxStatusError::InternalError(err.to_string()))?;
                let validator = chain.find_validator_for_forwarding(target_shard_id)?;

                self.network_adapter.do_send(
                    PeerManagerMessageRequest::NetworkRequests(NetworkRequests::TxStatus(
                        validator,
                        signer_account_id,
                        tx_hash,
                    ))
                    .with_span_context(),
                );
            }
            Ok(None)
        }
    }

    fn retrieve_headers(
        &mut self,
        hashes: Vec<CryptoHash>,
    ) -> Result<Vec<BlockHeader>, near_chain::Error> {
        // TODO(#3488): This takes advantage of DBCol::BlockHeader not being
        // garbage collected.  Switch to other method once that changes.
        self.hot_chain.retrieve_headers(hashes, sync::MAX_BLOCK_HEADERS, None)
    }

    fn check_signature_account_announce(
        &self,
        announce_account: &AnnounceAccount,
    ) -> Result<bool, Error> {
        let announce_hash = announce_account.hash();
        let head = self.hot_chain.head()?;

        // TODO(#6119): I believe this is always Hot, but need to verify.
        self.hot_runtime_adapter
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
        let now = Clock::instant();
        let cutoff = now - self.config.view_client_throttle_period;
        // Assume that time is linear. While in different threads there might be some small differences,
        // it should not matter in practice.
        while !cache.is_empty() && *cache.front().unwrap() < cutoff {
            cache.pop_front();
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

impl ViewClientActor {
    /// Implementation for GetBlock request.
    fn get_block_view(
        &self,
        reference: &BlockReference,
    ) -> Result<(Temperature, BlockView), GetBlockError> {
        let (temp, block) = match self.get_block_by_reference(&reference)? {
            None => return Err(GetBlockError::NotSyncedYet),
            Some(block) => block,
        };
        let block_author = self
            .runtime_adapter(temp)
            .get_block_producer(block.header().epoch_id(), block.header().height())?;
        Ok((temp, BlockView::from_author_block(block_author, block)))
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
        Ok(self.get_block_view(&msg.0)?.1)
    }
}

impl Handler<WithSpanContext<GetBlockWithMerkleTree>> for ViewClientActor {
    type Result = Result<(BlockView, Arc<PartialMerkleTree>), GetBlockError>;

    #[perf]
    fn handle(
        &mut self,
        msg: WithSpanContext<GetBlockWithMerkleTree>,
        _: &mut Self::Context,
    ) -> Self::Result {
        let (_span, msg) = handler_debug_span!(target: "client", msg);
        let _timer = metrics::VIEW_CLIENT_MESSAGE_TIME
            .with_label_values(&["GetBlockWithMerkleTree"])
            .start_timer();
        let (temp, block_view) = self.get_block_view(&msg.0)?;
        self.chain(temp)
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

        let (temp, chunk) = match msg {
            GetChunk::ChunkHash(chunk_hash) => {
                let temp = self.get_temperature_by_chunk_hash(&chunk_hash)?;
                let chunk = self.chain(temp).get_chunk(&chunk_hash)?;
                (temp, ShardChunk::clone(&chunk))
            }
            GetChunk::BlockHash(block_hash, shard_id) => {
                let (temp, block) = self.get_block(&block_hash)?;
                let chunk = get_chunk_from_block(block, shard_id, self.chain(temp))?;
                (temp, chunk)
            }
            GetChunk::Height(height, shard_id) => {
                let (temp, block) = self.get_block_by_height(height)?;
                let chunk = get_chunk_from_block(block, shard_id, self.chain(temp))?;
                (temp, chunk)
            }
        };

        let chunk_inner = chunk.cloned_header().take_inner();
        let runtime_adapter = self.runtime_adapter(temp);
        let epoch_id =
            runtime_adapter.get_epoch_id_from_prev_block(chunk_inner.prev_block_hash())?;
        let author = runtime_adapter.get_chunk_producer(
            &epoch_id,
            chunk_inner.height_created(),
            chunk_inner.shard_id(),
        )?;

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
        let (temp, epoch_identifier) = match msg.epoch_reference {
            EpochReference::EpochId(id) => {
                let temp = self.get_temperature_by_epoch_id(&id)?;
                // By `EpochId` we can get only cached epochs.  Request for not
                // finished epoch by `EpochId` will return an error because
                // epoch has not been cached yet.  If the requested one is
                // current ongoing we need to handle it like `Latest`
                let id = if temp == Temperature::Hot {
                    let tip = self.hot_chain.header_head()?;
                    if tip.epoch_id == id {
                        ValidatorInfoIdentifier::BlockHash(tip.last_block_hash)
                    } else {
                        ValidatorInfoIdentifier::EpochId(id)
                    }
                } else {
                    ValidatorInfoIdentifier::EpochId(id)
                };
                (temp, id)
            }
            EpochReference::BlockId(block_id) => {
                let (temp, header) = match block_id {
                    BlockId::Hash(h) => self.get_block_header(&h)?,
                    BlockId::Height(h) => self.get_block_header_by_height(h)?,
                };
                let chain = self.chain(temp);
                let next_block_hash = chain.store().get_next_block_hash(header.hash())?;
                let next_header = chain.get_block_header(&next_block_hash)?;
                if header.epoch_id() == next_header.epoch_id()
                    || header.next_epoch_id() != next_header.epoch_id()
                {
                    return Err(GetValidatorInfoError::ValidatorInfoUnavailable);
                }
                let id = ValidatorInfoIdentifier::EpochId(header.epoch_id().clone());
                (temp, id)
            }
            EpochReference::Latest => {
                // use header head because this is latest from the perspective
                // of epoch manager
                let hash = self.hot_chain.header_head()?.last_block_hash;
                (Temperature::Hot, ValidatorInfoIdentifier::BlockHash(hash))
            }
        };
        self.runtime_adapter(temp)
            .get_validator_info(epoch_identifier)
            .map_err(GetValidatorInfoError::from)
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
        let (temp, header) = self.get_block_header_by_maybe_block_id(msg.block_id)?;
        Ok(get_epoch_block_producers_view(
            header.epoch_id(),
            header.prev_hash(),
            self.runtime_adapter(temp),
        )?)
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
        let temp = self.get_temperature(&msg.block_hash)?;
        Ok(self
            .chain(temp)
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
            .chain(self.get_temperature(&msg.block_hash)?)
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
        let temp = self.get_temperature(&msg.block_hash)?;
        Ok(self
            .chain(temp)
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
        let temp = self.get_temperature(&msg.block_hash)?;
        let state_changes_with_cause_in_block =
            self.chain(temp).store().get_state_changes_with_cause_in_block(&msg.block_hash)?;

        let mut state_changes_with_cause_split_by_shard_id =
            HashMap::<ShardId, StateChangesView>::new();
        for state_change_with_cause in state_changes_with_cause_in_block {
            let account_id = state_change_with_cause.value.affected_account_id();
            let shard_id = match self
                .runtime_adapter(temp)
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
        let (last_block_temp, last_block_header) = self.get_block_header(&msg.last_block_hash)?;
        let last_epoch_id = last_block_header.epoch_id().clone();
        let last_next_epoch_id = last_block_header.next_epoch_id().clone();
        let last_height = last_block_header.height();
        let head = self.hot_chain.head()?;

        if last_epoch_id == head.epoch_id || last_next_epoch_id == head.epoch_id {
            let head_header = self.hot_chain.get_block_header(&head.last_block_hash)?;
            let ret = Chain::create_light_client_block(
                &head_header,
                &*self.hot_runtime_adapter,
                self.hot_chain.store(),
            )?;

            if ret.inner_lite.height <= last_height {
                Ok(None)
            } else {
                Ok(Some(Arc::new(ret)))
            }
        } else {
            match self
                .chain(last_block_temp)
                .store()
                .get_epoch_light_client_block(&last_next_epoch_id.0)
            {
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
        let (temp, id, account_id) = match msg.id {
            TransactionOrReceiptId::Transaction { transaction_hash, sender_id } => {
                let temp = self.get_temperature_by_tx_hash(&transaction_hash, &sender_id)?;
                (temp, transaction_hash, sender_id)
            }
            TransactionOrReceiptId::Receipt { receipt_id, receiver_id } => {
                let temp = self.get_temperature_by_rx_hash(&receipt_id, Some(&receiver_id))?;
                (temp, receipt_id, receiver_id)
            }
        };
        let chain = self.chain(temp);
        let runtime_adapter = self.runtime_adapter(temp);
        match chain.get_execution_outcome(&id) {
            Ok(outcome) => {
                let mut outcome_proof = outcome;
                let epoch_id =
                    chain.get_block(&outcome_proof.block_hash)?.header().epoch_id().clone();
                let target_shard_id =
                    runtime_adapter.account_id_to_shard_id(&account_id, &epoch_id)?;
                let res = chain.get_next_block_hash_with_new_chunk(
                    &outcome_proof.block_hash,
                    target_shard_id,
                )?;
                match res {
                    Some((h, target_shard_id)) => {
                        outcome_proof.block_hash = h;
                        // Here we assume the number of shards is small so this reconstruction
                        // should be fast
                        let outcome_roots = chain
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
                            outcome_root_proof: merklize(&outcome_roots).1
                                [target_shard_id as usize]
                                .clone(),
                        })
                    }
                    None => Err(GetExecutionOutcomeError::NotConfirmed {
                        transaction_or_receipt_id: id,
                    }),
                }
            }
            Err(near_chain::Error::DBNotFoundErr(_)) => {
                // TODO(#6119): Should this be chain? Or just always talk to
                // hot?  Or something even more complicated?
                let head = chain.head()?;
                let target_shard_id =
                    runtime_adapter.account_id_to_shard_id(&account_id, &head.epoch_id)?;
                if runtime_adapter.cares_about_shard(
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
        let (temp, block) = self.get_block(&msg.block_hash).map_err(|e| e.to_string())?;
        Ok(self
            .chain(temp)
            .get_block_execution_outcomes(&block)
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
        let temp = self.get_temperature_by_rx_hash(&msg.receipt_id, None)?;
        Ok(self
            .chain(temp)
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
        let (temp, header) = self.get_block_header(&msg.block_hash)?;
        let (head_temp, head_header) = self.get_block_header(&msg.head_block_hash)?;
        // TODO(#6119): Handle case when temperature of both blocks differs.
        assert_eq!(temp, head_temp, "Not implemented yet");
        let chain = self.chain(temp);
        chain.check_blocks_final_and_canonical(&[&header, &head_header])?;
        let block_header_lite = header.into();
        let proof = chain.get_block_proof(&msg.block_hash, &msg.head_block_hash)?;
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
        let (temp, header) = match self.get_block_header_by_reference(&msg.0)? {
            None => {
                return Err(GetProtocolConfigError::UnknownBlock("EarliestAvailable".to_string()))
            }
            Some(header) => header,
        };
        let config = self.runtime_adapter(temp).get_protocol_config(header.epoch_id())?;
        Ok(config.into())
    }
}

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
                let mut chain_store_update = self.hot_chain.mut_store().store_update();
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
        if let Ok((_temp, block)) = self.get_block(&hash) {
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
        // TODO(#6119): Should this depend on sync_hash?
        let chain = &self.hot_chain;
        let state_response = match chain.check_sync_hash_validity(&sync_hash) {
            Ok(true) => {
                let header = match chain.get_state_response_header(shard_id, sync_hash) {
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
        trace!(target: "sync", "Computing state request part {} {} {}", shard_id, sync_hash, part_id);
        // TODO(#6119): Should this depend on sync_hash?
        let chain = &self.hot_chain;
        let state_response = match chain.check_sync_hash_validity(&sync_hash) {
            Ok(true) => {
                let part = match chain.get_state_response_part(shard_id, part_id, sync_hash) {
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
                match self
                    .hot_runtime_adapter
                    .compare_epoch_id(&announce_account.epoch_id, &last_epoch)
                {
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
        let header = self.get_block_header_by_maybe_block_id(msg.block_id)?.1;
        Ok(GasPriceView { gas_price: header.gas_price() })
    }
}

/// Starts the View Client in a new arbiter (thread).
pub fn start_view_client(
    validator_account_id: Option<AccountId>,
    chain_genesis: ChainGenesis,
    hot_runtime_adapter: Arc<dyn RuntimeAdapter>,
    #[cfg(feature = "cold_store")] cold_runtime_adapter: Option<Arc<dyn RuntimeAdapter>>,
    network_adapter: Arc<dyn PeerManagerAdapter>,
    config: ClientConfig,
    adv: crate::adversarial::Controls,
) -> Addr<ViewClientActor> {
    let request_manager = Arc::new(RwLock::new(ViewClientRequestManager::new()));
    SyncArbiter::start(config.view_client_threads, move || {
        // ViewClientActor::start_in_arbiter(&Arbiter::current(), move |_ctx| {
        ViewClientActor::new(
            validator_account_id.clone(),
            &chain_genesis,
            hot_runtime_adapter.clone(),
            #[cfg(feature = "cold_store")]
            cold_runtime_adapter.clone(),
            network_adapter.clone(),
            config.clone(),
            request_manager.clone(),
            adv.clone(),
        )
        .unwrap()
    })
}
