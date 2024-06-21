//! Readonly view of the chain and state of the database.
//! Useful for querying from RPC.

use crate::{
    metrics, sync, GetChunk, GetExecutionOutcomeResponse, GetNextLightClientBlock, GetStateChanges,
    GetStateChangesInBlock, GetValidatorInfo, GetValidatorOrdered,
};
use actix::{Addr, SyncArbiter};
use near_async::actix_wrapper::SyncActixWrapper;
use near_async::messaging::{CanSend, Handler};
use near_async::time::{Clock, Duration, Instant};
use near_chain::types::{RuntimeAdapter, Tip};
use near_chain::{
    get_epoch_block_producers_view, Chain, ChainGenesis, ChainStoreAccess, DoomslugThresholdMode,
};
use near_chain_configs::{ClientConfig, MutableConfigValue, ProtocolConfigView};
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
use near_network::client::{
    AnnounceAccountRequest, BlockHeadersRequest, BlockRequest, StateRequestHeader,
    StateRequestPart, StateResponse, TxStatusRequest, TxStatusResponse,
};
use near_network::types::{
    NetworkRequests, PeerManagerAdapter, PeerManagerMessageRequest, ReasonForBan,
    StateResponseInfo, StateResponseInfoV2,
};
use near_performance_metrics_macros::perf;
use near_primitives::block::{Block, BlockHeader};
use near_primitives::epoch_manager::epoch_info::EpochInfo;
use near_primitives::hash::CryptoHash;
use near_primitives::merkle::{merklize, PartialMerkleTree};
use near_primitives::network::AnnounceAccount;
use near_primitives::receipt::Receipt;
use near_primitives::sharding::ShardChunk;
use near_primitives::state_sync::{
    ShardStateSyncResponse, ShardStateSyncResponseHeader, ShardStateSyncResponseV3,
};
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{
    AccountId, BlockHeight, BlockId, BlockReference, EpochReference, Finality, MaybeBlockId,
    ShardId, SyncCheckpoint, TransactionOrReceiptId, ValidatorInfoIdentifier,
};
use near_primitives::validator_signer::ValidatorSigner;
use near_primitives::views::validator_stake_view::ValidatorStakeView;
use near_primitives::views::{
    BlockView, ChunkView, EpochValidatorInfo, ExecutionOutcomeWithIdView, ExecutionStatusView,
    FinalExecutionOutcomeView, FinalExecutionOutcomeViewEnum, FinalExecutionStatus, GasPriceView,
    LightClientBlockView, MaintenanceWindowsView, QueryRequest, QueryResponse, ReceiptView,
    SignedTransactionView, SplitStorageInfoView, StateChangesKindsView, StateChangesView,
    TxExecutionStatus, TxStatusView,
};
use near_store::flat::{FlatStorageReadyStatus, FlatStorageStatus};
use near_store::{DBCol, COLD_HEAD_KEY, FINAL_HEAD_KEY, HEAD_KEY};
use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::hash::Hash;
use std::sync::{Arc, Mutex, RwLock};
use tracing::{error, info, warn};

/// Max number of queries that we keep.
const QUERY_REQUEST_LIMIT: usize = 500;
/// Waiting time between requests, in ms
const REQUEST_WAIT_TIME: i64 = 1000;

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

pub type ViewClientActor = SyncActixWrapper<ViewClientActorInner>;

/// View client provides currently committed (to the storage) view of the current chain and state.
pub struct ViewClientActorInner {
    clock: Clock,
    pub adv: crate::adversarial::Controls,

    /// Validator account (if present). This field is mutable and optional. Use with caution!
    /// Lock the value of mutable validator signer for the duration of a request to ensure consistency.
    /// Please note that the locked value should not be stored anywhere or passed through the thread boundary.
    validator: MutableConfigValue<Option<Arc<ValidatorSigner>>>,
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

impl ViewClientActorInner {
    /// Maximum number of state requests allowed per `view_client_throttle_period`.
    const MAX_NUM_STATE_REQUESTS: usize = 30;

    pub fn spawn_actix_actor(
        clock: Clock,
        validator: MutableConfigValue<Option<Arc<ValidatorSigner>>>,
        chain_genesis: ChainGenesis,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        shard_tracker: ShardTracker,
        runtime: Arc<dyn RuntimeAdapter>,
        network_adapter: PeerManagerAdapter,
        config: ClientConfig,
        adv: crate::adversarial::Controls,
    ) -> Addr<ViewClientActor> {
        SyncArbiter::start(config.view_client_threads, move || {
            // TODO: should we create shared ChainStore that is passed to both Client and ViewClient?
            let chain = Chain::new_for_view_client(
                clock.clone(),
                epoch_manager.clone(),
                shard_tracker.clone(),
                runtime.clone(),
                &chain_genesis,
                DoomslugThresholdMode::TwoThirds,
                config.save_trie_changes,
            )
            .unwrap();

            let view_client_actor = ViewClientActorInner {
                clock: clock.clone(),
                adv: adv.clone(),
                validator: validator.clone(),
                chain,
                epoch_manager: epoch_manager.clone(),
                shard_tracker: shard_tracker.clone(),
                runtime: runtime.clone(),
                network_adapter: network_adapter.clone(),
                config: config.clone(),
                request_manager: Arc::new(RwLock::new(ViewClientRequestManager::new())),
                state_request_cache: Arc::new(Mutex::new(VecDeque::default())),
            };
            SyncActixWrapper::new(view_client_actor)
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

    fn need_request<K: Hash + Eq + Clone>(
        &self,
        key: K,
        cache: &mut lru::LruCache<K, Instant>,
    ) -> bool {
        let now = self.clock.now();
        let need_request = match cache.get(&key) {
            Some(time) => now - *time > Duration::milliseconds(REQUEST_WAIT_TIME),
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
        let shard_ids = self.epoch_manager.shard_ids(&epoch_id)?;
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
            let cps: Vec<AccountId> = shard_ids
                .iter()
                .map(|&shard_id| {
                    let cp = epoch_info.sample_chunk_producer(block_height, shard_id).unwrap();
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

    // Return the lowest status the node can proof
    fn get_tx_execution_status(
        &self,
        execution_outcome: &FinalExecutionOutcomeView,
    ) -> Result<TxExecutionStatus, TxStatusError> {
        if execution_outcome.transaction_outcome.outcome.status == ExecutionStatusView::Unknown {
            return Ok(TxExecutionStatus::None);
        }

        let mut awaiting_receipt_ids: HashSet<&CryptoHash> =
            HashSet::from_iter(&execution_outcome.transaction_outcome.outcome.receipt_ids);
        awaiting_receipt_ids.extend(
            execution_outcome
                .receipts_outcome
                .iter()
                .flat_map(|outcome| &outcome.outcome.receipt_ids),
        );

        // refund receipt == last receipt in outcome.receipt_ids
        let mut awaiting_non_refund_receipt_ids: HashSet<&CryptoHash> =
            HashSet::from_iter(&execution_outcome.transaction_outcome.outcome.receipt_ids);
        awaiting_non_refund_receipt_ids.extend(execution_outcome.receipts_outcome.iter().flat_map(
            |outcome| {
                outcome.outcome.receipt_ids.split_last().map(|(_, ids)| ids).unwrap_or_else(|| &[])
            },
        ));

        let executed_receipt_ids: HashSet<&CryptoHash> = execution_outcome
            .receipts_outcome
            .iter()
            .filter_map(|outcome| {
                if outcome.outcome.status == ExecutionStatusView::Unknown {
                    None
                } else {
                    Some(&outcome.id)
                }
            })
            .collect();

        let executed_ignoring_refunds =
            awaiting_non_refund_receipt_ids.is_subset(&executed_receipt_ids);
        let executed_including_refunds = awaiting_receipt_ids.is_subset(&executed_receipt_ids);

        if let Err(_) = self.chain.check_blocks_final_and_canonical(&[self
            .chain
            .get_block_header(&execution_outcome.transaction_outcome.block_hash)?])
        {
            return if executed_ignoring_refunds {
                Ok(TxExecutionStatus::ExecutedOptimistic)
            } else {
                Ok(TxExecutionStatus::Included)
            };
        }

        if !executed_ignoring_refunds {
            return Ok(TxExecutionStatus::IncludedFinal);
        }
        if !executed_including_refunds {
            return Ok(TxExecutionStatus::Executed);
        }

        let block_hashes: BTreeSet<CryptoHash> =
            execution_outcome.receipts_outcome.iter().map(|e| e.block_hash).collect();
        let mut headers = vec![];
        for block_hash in block_hashes {
            headers.push(self.chain.get_block_header(&block_hash)?);
        }
        // We can't sort and check only the last block;
        // previous blocks may be not in the canonical chain
        Ok(match self.chain.check_blocks_final_and_canonical(&headers) {
            Err(_) => TxExecutionStatus::Executed,
            Ok(_) => TxExecutionStatus::Final,
        })
    }

    fn get_tx_status(
        &mut self,
        tx_hash: CryptoHash,
        signer_account_id: AccountId,
        fetch_receipt: bool,
        validator_signer: &Option<Arc<ValidatorSigner>>,
    ) -> Result<TxStatusView, TxStatusError> {
        {
            // TODO(telezhnaya): take into account `fetch_receipt()`
            // https://github.com/near/nearcore/issues/9545
            let mut request_manager = self.request_manager.write().expect(POISONED_LOCK_ERR);
            if let Some(res) = request_manager.tx_status_response.pop(&tx_hash) {
                request_manager.tx_status_requests.pop(&tx_hash);
                let status = self.get_tx_execution_status(&res)?;
                return Ok(TxStatusView {
                    execution_outcome: Some(FinalExecutionOutcomeViewEnum::FinalExecutionOutcome(
                        res,
                    )),
                    status,
                });
            }
        }

        let head = self.chain.head()?;
        let target_shard_id = self
            .epoch_manager
            .account_id_to_shard_id(&signer_account_id, &head.epoch_id)
            .map_err(|err| TxStatusError::InternalError(err.to_string()))?;
        // Check if we are tracking this shard.
        if self.shard_tracker.care_about_shard(
            validator_signer.as_ref().map(|v| v.validator_id()),
            &head.prev_block_hash,
            target_shard_id,
            true,
        ) {
            match self.chain.get_partial_transaction_result(&tx_hash) {
                Ok(tx_result) => {
                    let status = self.get_tx_execution_status(&tx_result)?;
                    let res = if fetch_receipt {
                        let final_result =
                            self.chain.get_transaction_result_with_receipt(tx_result)?;
                        FinalExecutionOutcomeViewEnum::FinalExecutionOutcomeWithReceipt(
                            final_result,
                        )
                    } else {
                        FinalExecutionOutcomeViewEnum::FinalExecutionOutcome(tx_result)
                    };
                    Ok(TxStatusView { execution_outcome: Some(res), status })
                }
                Err(near_chain::Error::DBNotFoundErr(_)) => {
                    if let Ok(Some(transaction)) = self.chain.chain_store.get_transaction(&tx_hash)
                    {
                        let transaction: SignedTransactionView =
                            SignedTransaction::clone(&transaction).into();
                        if let Ok(tx_outcome) = self.chain.get_execution_outcome(&tx_hash) {
                            let outcome = FinalExecutionOutcomeViewEnum::FinalExecutionOutcome(
                                FinalExecutionOutcomeView {
                                    status: FinalExecutionStatus::Started,
                                    transaction,
                                    transaction_outcome: tx_outcome.into(),
                                    receipts_outcome: vec![],
                                },
                            );
                            Ok(TxStatusView {
                                execution_outcome: Some(outcome),
                                status: TxExecutionStatus::Included,
                            })
                        } else {
                            Ok(TxStatusView {
                                execution_outcome: None,
                                status: TxExecutionStatus::Included,
                            })
                        }
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
            if self.need_request(tx_hash, &mut request_manager.tx_status_requests) {
                let target_shard_id = self
                    .epoch_manager
                    .account_id_to_shard_id(&signer_account_id, &head.epoch_id)
                    .map_err(|err| TxStatusError::InternalError(err.to_string()))?;
                let validator = self
                    .epoch_manager
                    .get_chunk_producer(
                        &head.epoch_id,
                        head.height + self.config.tx_routing_height_horizon - 1,
                        target_shard_id,
                    )
                    .map_err(|err| TxStatusError::ChainError(err.into()))?;

                self.network_adapter.send(PeerManagerMessageRequest::NetworkRequests(
                    NetworkRequests::TxStatus(validator, signer_account_id, tx_hash),
                ));
            }
            Ok(TxStatusView { execution_outcome: None, status: TxExecutionStatus::None })
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

    /// Returns true if this request needs to be **dropped** due to exceeding a
    /// rate limit of state sync requests.
    fn throttle_state_sync_request(&self) -> bool {
        let mut cache = self.state_request_cache.lock().expect(POISONED_LOCK_ERR);
        let now = self.clock.now();
        while let Some(&instant) = cache.front() {
            if now - instant > self.config.view_client_throttle_period {
                cache.pop_front();
            } else {
                // Assume that time is linear. While in different threads there might be some small differences,
                // it should not matter in practice.
                break;
            }
        }
        if cache.len() >= Self::MAX_NUM_STATE_REQUESTS {
            return true;
        }
        cache.push_back(now);
        false
    }

    fn has_state_snapshot(&self, sync_hash: &CryptoHash, shard_id: ShardId) -> Result<bool, Error> {
        let header = self.chain.get_block_header(sync_hash)?;
        let prev_header = self.chain.get_block_header(header.prev_hash())?;
        let prev_epoch_id = prev_header.epoch_id();
        let shard_uid = self.epoch_manager.shard_id_to_uid(shard_id, prev_epoch_id)?;
        let sync_prev_prev_hash = prev_header.prev_hash();
        let status = self
            .runtime
            .get_tries()
            .get_snapshot_flat_storage_status(*sync_prev_prev_hash, shard_uid)
            .map_err(|err| Error::Other(err.to_string()))?;
        match status {
            FlatStorageStatus::Ready(FlatStorageReadyStatus { flat_head }) => {
                let flat_head_header = self.chain.get_block_header(&flat_head.hash)?;
                let flat_head_epoch_id = flat_head_header.epoch_id();
                Ok(flat_head_epoch_id == prev_epoch_id)
            }
            _ => Ok(false),
        }
    }
}

impl Handler<Query> for ViewClientActorInner {
    #[perf]
    fn handle(&mut self, msg: Query) -> Result<QueryResponse, QueryError> {
        tracing::debug!(target: "client", ?msg);
        let _timer = metrics::VIEW_CLIENT_MESSAGE_TIME.with_label_values(&["Query"]).start_timer();
        self.handle_query(msg)
    }
}

/// Handles retrieving block from the chain.
impl Handler<GetBlock> for ViewClientActorInner {
    #[perf]
    fn handle(&mut self, msg: GetBlock) -> Result<BlockView, GetBlockError> {
        tracing::debug!(target: "client", ?msg);
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

impl Handler<GetBlockWithMerkleTree> for ViewClientActorInner {
    #[perf]
    fn handle(
        &mut self,
        msg: GetBlockWithMerkleTree,
    ) -> Result<(BlockView, Arc<PartialMerkleTree>), GetBlockError> {
        tracing::debug!(target: "client", ?msg);
        let _timer = metrics::VIEW_CLIENT_MESSAGE_TIME
            .with_label_values(&["GetBlockWithMerkleTree"])
            .start_timer();
        let block_view = self.handle(GetBlock(msg.0))?;
        self.chain
            .chain_store()
            .get_block_merkle_tree(&block_view.header.hash)
            .map(|merkle_tree| (block_view, merkle_tree))
            .map_err(|e| e.into())
    }
}

impl Handler<GetChunk> for ViewClientActorInner {
    #[perf]
    fn handle(&mut self, msg: GetChunk) -> Result<ChunkView, GetChunkError> {
        tracing::debug!(target: "client", ?msg);
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

impl Handler<TxStatus> for ViewClientActorInner {
    #[perf]
    fn handle(&mut self, msg: TxStatus) -> Result<TxStatusView, TxStatusError> {
        tracing::debug!(target: "client", ?msg);
        let _timer =
            metrics::VIEW_CLIENT_MESSAGE_TIME.with_label_values(&["TxStatus"]).start_timer();
        let validator_signer = self.validator.get();
        self.get_tx_status(msg.tx_hash, msg.signer_account_id, msg.fetch_receipt, &validator_signer)
    }
}

impl Handler<GetValidatorInfo> for ViewClientActorInner {
    #[perf]
    fn handle(
        &mut self,
        msg: GetValidatorInfo,
    ) -> Result<EpochValidatorInfo, GetValidatorInfoError> {
        tracing::debug!(target: "client", ?msg);
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
                    self.chain.chain_store().get_next_block_hash(block_header.hash())?;
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

impl Handler<GetValidatorOrdered> for ViewClientActorInner {
    #[perf]
    fn handle(
        &mut self,
        msg: GetValidatorOrdered,
    ) -> Result<Vec<ValidatorStakeView>, GetValidatorInfoError> {
        tracing::debug!(target: "client", ?msg);
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
impl Handler<GetStateChangesInBlock> for ViewClientActorInner {
    #[perf]
    fn handle(
        &mut self,
        msg: GetStateChangesInBlock,
    ) -> Result<StateChangesKindsView, GetStateChangesError> {
        tracing::debug!(target: "client", ?msg);
        let _timer = metrics::VIEW_CLIENT_MESSAGE_TIME
            .with_label_values(&["GetStateChangesInBlock"])
            .start_timer();
        Ok(self
            .chain
            .chain_store()
            .get_state_changes_in_block(&msg.block_hash)?
            .into_iter()
            .map(Into::into)
            .collect())
    }
}

/// Returns a list of changes in a store for a given block filtering by the state changes request.
impl Handler<GetStateChanges> for ViewClientActorInner {
    #[perf]
    fn handle(&mut self, msg: GetStateChanges) -> Result<StateChangesView, GetStateChangesError> {
        tracing::debug!(target: "client", ?msg);
        let _timer =
            metrics::VIEW_CLIENT_MESSAGE_TIME.with_label_values(&["GetStateChanges"]).start_timer();
        Ok(self
            .chain
            .chain_store()
            .get_state_changes(&msg.block_hash, &msg.state_changes_request.into())?
            .into_iter()
            .map(Into::into)
            .collect())
    }
}

/// Returns a list of changes in a store with causes for a given block.
impl Handler<GetStateChangesWithCauseInBlock> for ViewClientActorInner {
    #[perf]
    fn handle(
        &mut self,
        msg: GetStateChangesWithCauseInBlock,
    ) -> Result<StateChangesView, GetStateChangesError> {
        tracing::debug!(target: "client", ?msg);
        let _timer = metrics::VIEW_CLIENT_MESSAGE_TIME
            .with_label_values(&["GetStateChangesWithCauseInBlock"])
            .start_timer();
        Ok(self
            .chain
            .chain_store()
            .get_state_changes_with_cause_in_block(&msg.block_hash)?
            .into_iter()
            .map(Into::into)
            .collect())
    }
}

/// Returns a hashmap where the key represents the ShardID and the value
/// is the list of changes in a store with causes for a given block.
impl Handler<GetStateChangesWithCauseInBlockForTrackedShards> for ViewClientActorInner {
    #[perf]
    fn handle(
        &mut self,
        msg: GetStateChangesWithCauseInBlockForTrackedShards,
    ) -> Result<HashMap<ShardId, StateChangesView>, GetStateChangesError> {
        tracing::debug!(target: "client", ?msg);
        let _timer = metrics::VIEW_CLIENT_MESSAGE_TIME
            .with_label_values(&["GetStateChangesWithCauseInBlockForTrackedShards"])
            .start_timer();
        let state_changes_with_cause_in_block =
            self.chain.chain_store().get_state_changes_with_cause_in_block(&msg.block_hash)?;

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
impl Handler<GetNextLightClientBlock> for ViewClientActorInner {
    #[perf]
    fn handle(
        &mut self,
        msg: GetNextLightClientBlock,
    ) -> Result<Option<Arc<LightClientBlockView>>, GetNextLightClientBlockError> {
        tracing::debug!(target: "client", ?msg);
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
                self.chain.chain_store(),
            )?;

            if ret.inner_lite.height <= last_height {
                Ok(None)
            } else {
                Ok(Some(Arc::new(ret)))
            }
        } else {
            match self.chain.chain_store().get_epoch_light_client_block(&last_next_epoch_id.0) {
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

impl Handler<GetExecutionOutcome> for ViewClientActorInner {
    #[perf]
    fn handle(
        &mut self,
        msg: GetExecutionOutcome,
    ) -> Result<GetExecutionOutcomeResponse, GetExecutionOutcomeError> {
        tracing::debug!(target: "client", ?msg);
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
                        .map(|header| header.prev_outcome_root())
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
                    self.validator.get().map(|v| v.validator_id().clone()).as_ref(),
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
impl Handler<GetExecutionOutcomesForBlock> for ViewClientActorInner {
    #[perf]
    fn handle(
        &mut self,
        msg: GetExecutionOutcomesForBlock,
    ) -> Result<HashMap<ShardId, Vec<ExecutionOutcomeWithIdView>>, String> {
        tracing::debug!(target: "client", ?msg);
        let _timer = metrics::VIEW_CLIENT_MESSAGE_TIME
            .with_label_values(&["GetExecutionOutcomesForBlock"])
            .start_timer();
        Ok(self
            .chain
            .chain_store()
            .get_block_execution_outcomes(&msg.block_hash)
            .map_err(|e| e.to_string())?
            .into_iter()
            .map(|(k, v)| (k, v.into_iter().map(Into::into).collect()))
            .collect())
    }
}

impl Handler<GetReceipt> for ViewClientActorInner {
    #[perf]
    fn handle(&mut self, msg: GetReceipt) -> Result<Option<ReceiptView>, GetReceiptError> {
        tracing::debug!(target: "client", ?msg);
        let _timer =
            metrics::VIEW_CLIENT_MESSAGE_TIME.with_label_values(&["GetReceipt"]).start_timer();
        Ok(self
            .chain
            .chain_store()
            .get_receipt(&msg.receipt_id)?
            .map(|receipt| Receipt::clone(&receipt).into()))
    }
}

impl Handler<GetBlockProof> for ViewClientActorInner {
    #[perf]
    fn handle(&mut self, msg: GetBlockProof) -> Result<GetBlockProofResponse, GetBlockProofError> {
        tracing::debug!(target: "client", ?msg);
        let _timer =
            metrics::VIEW_CLIENT_MESSAGE_TIME.with_label_values(&["GetBlockProof"]).start_timer();
        let block_header = self.chain.get_block_header(&msg.block_hash)?;
        let head_block_header = self.chain.get_block_header(&msg.head_block_hash)?;
        self.chain.check_blocks_final_and_canonical(&[block_header.clone(), head_block_header])?;
        let block_header_lite = block_header.into();
        let proof = self.chain.get_block_proof(&msg.block_hash, &msg.head_block_hash)?;
        Ok(GetBlockProofResponse { block_header_lite, proof })
    }
}

impl Handler<GetProtocolConfig> for ViewClientActorInner {
    #[perf]
    fn handle(
        &mut self,
        msg: GetProtocolConfig,
    ) -> Result<ProtocolConfigView, GetProtocolConfigError> {
        tracing::debug!(target: "client", ?msg);
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
impl Handler<NetworkAdversarialMessage> for ViewClientActorInner {
    #[perf]
    fn handle(&mut self, msg: NetworkAdversarialMessage) -> Option<u64> {
        tracing::debug!(target: "client", ?msg);
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
                let mut chain_store_update = self.chain.mut_chain_store().store_update();
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

impl Handler<TxStatusRequest> for ViewClientActorInner {
    #[perf]
    fn handle(&mut self, msg: TxStatusRequest) -> Option<Box<FinalExecutionOutcomeView>> {
        tracing::debug!(target: "client", ?msg);
        let _timer =
            metrics::VIEW_CLIENT_MESSAGE_TIME.with_label_values(&["TxStatusRequest"]).start_timer();
        let TxStatusRequest { tx_hash, signer_account_id } = msg;
        let validator_signer = self.validator.get();
        if let Ok(Some(result)) = self
            .get_tx_status(tx_hash, signer_account_id, false, &validator_signer)
            .map(|s| s.execution_outcome)
        {
            Some(Box::new(result.into_outcome()))
        } else {
            None
        }
    }
}

impl Handler<TxStatusResponse> for ViewClientActorInner {
    #[perf]
    fn handle(&mut self, msg: TxStatusResponse) {
        tracing::debug!(target: "client", ?msg);
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

impl Handler<BlockRequest> for ViewClientActorInner {
    #[perf]
    fn handle(&mut self, msg: BlockRequest) -> Option<Box<Block>> {
        tracing::debug!(target: "client", ?msg);
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

impl Handler<BlockHeadersRequest> for ViewClientActorInner {
    #[perf]
    fn handle(&mut self, msg: BlockHeadersRequest) -> Option<Vec<BlockHeader>> {
        tracing::debug!(target: "client", ?msg);
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

impl Handler<StateRequestHeader> for ViewClientActorInner {
    #[perf]
    fn handle(&mut self, msg: StateRequestHeader) -> Option<StateResponse> {
        tracing::debug!(target: "client", ?msg);
        let _timer = metrics::VIEW_CLIENT_MESSAGE_TIME
            .with_label_values(&["StateRequestHeader"])
            .start_timer();
        let StateRequestHeader { shard_id, sync_hash } = msg;
        if self.throttle_state_sync_request() {
            tracing::debug!(target: "sync", ?sync_hash, "Throttle state sync requests");
            return None;
        }
        let header = match self.chain.check_sync_hash_validity(&sync_hash) {
            Ok(true) => match self.chain.get_state_response_header(shard_id, sync_hash) {
                Ok(header) => Some(header),
                Err(err) => {
                    error!(target: "sync", ?err, "Cannot build state sync header");
                    None
                }
            },
            Ok(false) => {
                warn!(target: "sync", ?sync_hash, "sync_hash didn't pass validation, possible malicious behavior");
                // Don't respond to the node, because the request is malformed.
                return None;
            }
            Err(near_chain::Error::DBNotFoundErr(_)) => {
                // This case may appear in case of latency in epoch switching.
                // Request sender is ready to sync but we still didn't get the block.
                info!(target: "sync", ?sync_hash, "Can't get sync_hash block for state request header");
                None
            }
            Err(err) => {
                error!(target: "sync", ?err, ?sync_hash, "Failed to verify sync_hash validity");
                None
            }
        };
        let state_response = match header {
            Some(header) => {
                let num_parts = header.num_state_parts();
                let cached_parts = match self
                    .chain
                    .get_cached_state_parts(sync_hash, shard_id, num_parts)
                {
                    Ok(cached_parts) => Some(cached_parts),
                    Err(err) => {
                        tracing::error!(target: "sync", ?err, ?sync_hash, shard_id, "Failed to get cached state parts");
                        None
                    }
                };
                let header = match header {
                    ShardStateSyncResponseHeader::V2(inner) => inner,
                    _ => {
                        tracing::error!(target: "sync", ?sync_hash, shard_id, "Invalid state sync header format");
                        return None;
                    }
                };

                let can_generate = self.has_state_snapshot(&sync_hash, shard_id).is_ok();
                ShardStateSyncResponse::V3(ShardStateSyncResponseV3 {
                    header: Some(header),
                    part: None,
                    cached_parts,
                    can_generate,
                })
            }
            None => ShardStateSyncResponse::V3(ShardStateSyncResponseV3 {
                header: None,
                part: None,
                cached_parts: None,
                can_generate: false,
            }),
        };
        let info =
            StateResponseInfo::V2(StateResponseInfoV2 { shard_id, sync_hash, state_response });
        Some(StateResponse(Box::new(info)))
    }
}

impl Handler<StateRequestPart> for ViewClientActorInner {
    #[perf]
    fn handle(&mut self, msg: StateRequestPart) -> Option<StateResponse> {
        tracing::debug!(target: "client", ?msg);
        let _timer = metrics::VIEW_CLIENT_MESSAGE_TIME
            .with_label_values(&["StateRequestPart"])
            .start_timer();
        let StateRequestPart { shard_id, sync_hash, part_id } = msg;
        if self.throttle_state_sync_request() {
            tracing::debug!(target: "sync", ?sync_hash, "Throttle state sync requests");
            return None;
        }
        if let Err(err) = self.has_state_snapshot(&sync_hash, shard_id) {
            tracing::debug!(target: "sync", ?err, ?sync_hash, "Node doesn't have a matching state snapshot");
            return None;
        }
        tracing::debug!(target: "sync", ?shard_id, ?sync_hash, ?part_id, "Computing state request part");
        let part = match self.chain.check_sync_hash_validity(&sync_hash) {
            Ok(true) => {
                let part = match self.chain.get_state_response_part(shard_id, part_id, sync_hash) {
                    Ok(part) => Some((part_id, part)),
                    Err(err) => {
                        error!(target: "sync", ?err, ?sync_hash, shard_id, part_id, "Cannot build state part");
                        None
                    }
                };

                tracing::trace!(target: "sync", ?sync_hash, shard_id, part_id, "Finished computation for state request part");
                part
            }
            Ok(false) => {
                warn!(target: "sync", ?sync_hash, shard_id, "sync_hash didn't pass validation, possible malicious behavior");
                // Do not respond, possible malicious behavior.
                return None;
            }
            Err(near_chain::Error::DBNotFoundErr(_)) => {
                // This case may appear in case of latency in epoch switching.
                // Request sender is ready to sync but we still didn't get the block.
                info!(target: "sync", ?sync_hash, "Can't get sync_hash block for state request part");
                None
            }
            Err(err) => {
                error!(target: "sync", ?err, ?sync_hash, "Failed to verify sync_hash validity");
                None
            }
        };
        let num_parts = part.as_ref().and_then(|_| match self.chain.get_state_response_header(shard_id, sync_hash) {
            Ok(header) => Some(header.num_state_parts()),
            Err(err) => {
                tracing::error!(target: "sync", ?err, ?sync_hash, shard_id, "Failed to get num state parts");
                None
            }
        });
        let cached_parts = num_parts.and_then(|num_parts|
            match self.chain.get_cached_state_parts(sync_hash, shard_id, num_parts) {
                Ok(cached_parts) => Some(cached_parts),
                Err(err) => {
                    tracing::error!(target: "sync", ?err, ?sync_hash, shard_id, "Failed to get cached state parts");
                    None
                }
            });
        let can_generate = part.is_some();
        let state_response = ShardStateSyncResponse::V3(ShardStateSyncResponseV3 {
            header: None,
            part,
            cached_parts,
            can_generate,
        });
        let info =
            StateResponseInfo::V2(StateResponseInfoV2 { shard_id, sync_hash, state_response });
        Some(StateResponse(Box::new(info)))
    }
}

impl Handler<AnnounceAccountRequest> for ViewClientActorInner {
    #[perf]
    fn handle(
        &mut self,
        msg: AnnounceAccountRequest,
    ) -> Result<Vec<AnnounceAccount>, ReasonForBan> {
        tracing::debug!(target: "client", ?msg);
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
                Err(err) => {
                    tracing::debug!(target: "view_client", ?err, "Failed to validate account announce signature");
                }
            }
        }
        Ok(filtered_announce_accounts)
    }
}

impl Handler<GetGasPrice> for ViewClientActorInner {
    #[perf]
    fn handle(&mut self, msg: GetGasPrice) -> Result<GasPriceView, GetGasPriceError> {
        tracing::debug!(target: "client", ?msg);
        let _timer =
            metrics::VIEW_CLIENT_MESSAGE_TIME.with_label_values(&["GetGasPrice"]).start_timer();
        let header = self.maybe_block_id_to_block_header(msg.block_id);
        Ok(GasPriceView { gas_price: header?.next_gas_price() })
    }
}

impl Handler<GetMaintenanceWindows> for ViewClientActorInner {
    #[perf]
    fn handle(
        &mut self,
        msg: GetMaintenanceWindows,
    ) -> Result<MaintenanceWindowsView, GetMaintenanceWindowsError> {
        tracing::debug!(target: "client", ?msg);
        Ok(self.get_maintenance_windows(msg.account_id)?)
    }
}

impl Handler<GetSplitStorageInfo> for ViewClientActorInner {
    fn handle(
        &mut self,
        msg: GetSplitStorageInfo,
    ) -> Result<SplitStorageInfoView, GetSplitStorageInfoError> {
        tracing::debug!(target: "client", ?msg);

        let store = self.chain.chain_store().store();
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
