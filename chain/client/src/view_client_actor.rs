//! Readonly view of the chain and state of the database.
//! Useful for querying from RPC.

use crate::{
    GetChunk, GetExecutionOutcomeResponse, GetNextLightClientBlock, GetShardChunk, GetStateChanges,
    GetStateChangesInBlock, GetValidatorInfo, GetValidatorOrdered, metrics, sync,
};
use actix::{Addr, SyncArbiter};
use near_async::actix_wrapper::SyncActixWrapper;
use near_async::messaging::{Actor, CanSend, Handler};
use near_async::time::{Clock, Duration, Instant};
use near_chain::types::{RuntimeAdapter, Tip};
use near_chain::{
    Chain, ChainGenesis, ChainStoreAccess, DoomslugThresholdMode, MerkleProofAccess,
    get_epoch_block_producers_view, retrieve_headers,
};

use near_chain_configs::{ClientConfig, MutableValidatorSigner, ProtocolConfigView};
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
use near_epoch_manager::EpochManagerAdapter;
use near_epoch_manager::shard_assignment::{account_id_to_shard_id, shard_id_to_uid};
use near_epoch_manager::shard_tracker::ShardTracker;
use near_network::client::{
    AnnounceAccountRequest, BlockHeadersRequest, BlockRequest, TxStatusRequest, TxStatusResponse,
};
use near_network::types::{
    NetworkRequests, PeerManagerAdapter, PeerManagerMessageRequest, ReasonForBan,
};
use near_performance_metrics_macros::perf;
use near_primitives::block::{Block, BlockHeader};
use near_primitives::epoch_info::EpochInfo;
use near_primitives::errors::EpochError;
use near_primitives::hash::CryptoHash;
use near_primitives::merkle::{PartialMerkleTree, merklize};
use near_primitives::network::AnnounceAccount;
use near_primitives::receipt::Receipt;
use near_primitives::sharding::ShardChunk;
use near_primitives::stateless_validation::ChunkProductionKey;
use near_primitives::types::{
    AccountId, BlockHeight, BlockId, BlockReference, EpochId, EpochReference, Finality,
    MaybeBlockId, ShardId, SyncCheckpoint, TransactionOrReceiptId, ValidatorInfoIdentifier,
};
use near_primitives::views::validator_stake_view::ValidatorStakeView;
use near_primitives::views::{
    BlockView, ChunkView, EpochValidatorInfo, ExecutionOutcomeWithIdView, ExecutionStatusView,
    FinalExecutionOutcomeView, FinalExecutionOutcomeViewEnum, FinalExecutionStatus, GasPriceView,
    LightClientBlockView, MaintenanceWindowsView, QueryRequest, QueryResponse, ReceiptView,
    SignedTransactionView, SplitStorageInfoView, StateChangesKindsView, StateChangesView,
    TxExecutionStatus, TxStatusView,
};
use near_store::{COLD_HEAD_KEY, DBCol, FINAL_HEAD_KEY, HEAD_KEY};
use parking_lot::RwLock;
use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::hash::Hash;
use std::num::NonZeroUsize;
use std::sync::Arc;
use tracing::warn;

/// Max number of queries that we keep.
const QUERY_REQUEST_LIMIT: usize = 500;
/// Waiting time between requests, in ms
const REQUEST_WAIT_TIME: i64 = 1000;

/// Request and response manager across all instances of ViewClientActor.
pub struct ViewClientRequestManager {
    /// Transaction query that needs to be forwarded to other shards
    pub tx_status_requests: lru::LruCache<CryptoHash, Instant>,
    /// Transaction status response
    pub tx_status_response: lru::LruCache<CryptoHash, FinalExecutionOutcomeView>,
}

pub type ViewClientActor = SyncActixWrapper<ViewClientActorInner>;

/// View client provides currently committed (to the storage) view of the current chain and state.
pub struct ViewClientActorInner {
    clock: Clock,
    pub adv: crate::adversarial::Controls,
    pub chain: Chain,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    shard_tracker: ShardTracker,
    runtime: Arc<dyn RuntimeAdapter>,
    network_adapter: PeerManagerAdapter,
    pub config: ClientConfig,
    request_manager: Arc<RwLock<ViewClientRequestManager>>,
}

impl ViewClientRequestManager {
    pub fn new() -> Self {
        Self {
            tx_status_requests: lru::LruCache::new(NonZeroUsize::new(QUERY_REQUEST_LIMIT).unwrap()),
            tx_status_response: lru::LruCache::new(NonZeroUsize::new(QUERY_REQUEST_LIMIT).unwrap()),
        }
    }
}

impl Actor for ViewClientActorInner {}

impl ViewClientActorInner {
    pub fn spawn_actix_actor(
        clock: Clock,
        chain_genesis: ChainGenesis,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        shard_tracker: ShardTracker,
        runtime: Arc<dyn RuntimeAdapter>,
        network_adapter: PeerManagerAdapter,
        config: ClientConfig,
        adv: crate::adversarial::Controls,
        validator_signer: MutableValidatorSigner,
    ) -> Addr<ViewClientActor> {
        SyncArbiter::start(config.view_client_threads, move || {
            let view_client_actor = ViewClientActorInner::new(
                clock.clone(),
                chain_genesis.clone(),
                epoch_manager.clone(),
                shard_tracker.clone(),
                runtime.clone(),
                network_adapter.clone(),
                config.clone(),
                adv.clone(),
                validator_signer.clone(),
            )
            .unwrap();
            SyncActixWrapper::new(view_client_actor)
        })
    }

    pub fn new(
        clock: Clock,
        chain_genesis: ChainGenesis,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        shard_tracker: ShardTracker,
        runtime: Arc<dyn RuntimeAdapter>,
        network_adapter: PeerManagerAdapter,
        config: ClientConfig,
        adv: crate::adversarial::Controls,
        validator_signer: MutableValidatorSigner,
    ) -> Result<Self, Error> {
        // TODO: should we create shared ChainStore that is passed to both Client and ViewClient?
        let chain = Chain::new_for_view_client(
            clock.clone(),
            epoch_manager.clone(),
            shard_tracker.clone(),
            runtime.clone(),
            &chain_genesis,
            DoomslugThresholdMode::TwoThirds,
            config.save_trie_changes,
            validator_signer,
        )?;
        Ok(Self {
            clock,
            adv,
            chain,
            epoch_manager,
            shard_tracker,
            runtime,
            network_adapter,
            config,
            request_manager: Arc::new(RwLock::new(ViewClientRequestManager::new())),
        })
    }

    fn maybe_block_id_to_block_header(
        &self,
        block_id: MaybeBlockId,
    ) -> Result<Arc<BlockHeader>, near_chain::Error> {
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
    /// the caller into some form of 'no sync block' higher-level error.
    fn get_block_header_by_reference(
        &self,
        reference: &BlockReference,
    ) -> Result<Option<Arc<BlockHeader>>, near_chain::Error> {
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
                Ok(Some(self.chain.genesis().clone().into()))
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
    /// the caller into some form of 'no sync block' higher-level error.
    fn get_block_by_reference(
        &self,
        reference: &BlockReference,
    ) -> Result<Option<Arc<Block>>, near_chain::Error> {
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
                Ok(Some(self.chain.genesis_block().into()))
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
        let shard_layout = self.epoch_manager.get_shard_layout(&epoch_id)?;
        let shard_ids = self.epoch_manager.shard_ids(&epoch_id)?;
        let cur_block_info = self.epoch_manager.get_block_info(&head.last_block_hash)?;
        let next_epoch_start_height =
            self.epoch_manager.get_epoch_start_height(cur_block_info.hash())?
                + self.epoch_manager.get_epoch_config(&epoch_id)?.epoch_length;

        let mut windows: MaintenanceWindowsView = Vec::new();
        let mut start_block_of_window: Option<BlockHeight> = None;
        let last_block_of_epoch = next_epoch_start_height - 1;

        // This loop does not go beyond the current epoch so it is valid to use
        // the EpochInfo and ShardLayout from the current epoch.
        for block_height in head.height..next_epoch_start_height {
            let bp = epoch_info.sample_block_producer(block_height);
            let bp = epoch_info.get_validator(bp).account_id().clone();
            let cps: Vec<AccountId> = shard_ids
                .iter()
                .map(|&shard_id| {
                    let cp = epoch_info
                        .sample_chunk_producer(&shard_layout, shard_id, block_height)
                        .unwrap();
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

    fn handle_query(&self, msg: Query) -> Result<QueryResponse, QueryError> {
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

        let shard_id = self
            .query_shard_uid(&msg.request, *header.epoch_id())
            .map_err(|err| QueryError::InternalError { error_message: err.to_string() })?;
        let shard_uid =
            shard_id_to_uid(self.epoch_manager.as_ref(), shard_id, header.epoch_id())
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
                near_chain::near_chain_primitives::error::QueryError::NoGlobalContractCode {
                    identifier,
                    block_height,
                    block_hash,
                } => QueryError::NoGlobalContractCode { identifier, block_height, block_hash },
            }),
        }
    }

    fn query_shard_uid(
        &self,
        request: &QueryRequest,
        epoch_id: EpochId,
    ) -> Result<ShardId, EpochError> {
        match &request {
            QueryRequest::ViewAccount { account_id, .. }
            | QueryRequest::ViewState { account_id, .. }
            | QueryRequest::ViewAccessKey { account_id, .. }
            | QueryRequest::ViewAccessKeyList { account_id, .. }
            | QueryRequest::CallFunction { account_id, .. }
            | QueryRequest::ViewCode { account_id, .. } => {
                account_id_to_shard_id(self.epoch_manager.as_ref(), account_id, &epoch_id)
            }
            QueryRequest::ViewGlobalContractCode { .. }
            | QueryRequest::ViewGlobalContractCodeByAccountId { .. } => {
                // for global contract queries we can use any shard_id, so just take the first one
                let shard_ids = self.epoch_manager.shard_ids(&epoch_id)?;
                Ok(*shard_ids.iter().next().expect("at least one shard should always exist"))
            }
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

        // Optimization for end-user TX latency: If the last outgoing receipt of
        // an outcome is a refund receipt, we don't wait for that and consider
        // the transaction executed or finalized a block earlier.
        let mut awaiting_non_refund_receipt_ids = awaiting_receipt_ids.clone();
        for outcome in &execution_outcome.receipts_outcome {
            let receipt_ids = outcome.outcome.receipt_ids.as_slice();
            let Some(last_receipt_id) = receipt_ids.last() else {
                continue;
            };

            // no need to apply the optimization for already executed receipts
            if executed_receipt_ids.contains(last_receipt_id) {
                continue;
            }
            // need the full receipt to determine if this is a refund or not
            // Note: We can't read from DBCol::Receipts here because we store it there too late
            // This means we have to read outgoing receipts, which isn't ideal.
            // but since this is only triggered for receipts that are not
            // executed yet, this is a rare case. And whenever we hit it, this is
            // recent data that was written probably within the last second and
            // should be fast to retrieve.
            let outgoing_receipts = self.outgoing_receipts_for_outcome(outcome)?;
            let Some(last_receipt) =
                outgoing_receipts.iter().find(|receipt| receipt.receipt_id() == last_receipt_id)
            else {
                // if we can't fetch the receipt, be conservative and assume we have to wait
                continue;
            };
            // predecessor_id == "system" means this is a refund receipt
            if last_receipt.predecessor_id().is_system() {
                awaiting_non_refund_receipt_ids.remove(last_receipt_id);
            }
        }

        let executed_ignoring_refunds =
            awaiting_non_refund_receipt_ids.is_subset(&executed_receipt_ids);
        let executed_including_refunds = awaiting_receipt_ids.is_subset(&executed_receipt_ids);

        let blocks =
            [self.chain.get_block_header(&execution_outcome.transaction_outcome.block_hash)?];
        if let Err(_) = self.chain.check_blocks_final_and_canonical(blocks.iter().map(|b| &**b)) {
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
        Ok(match self.chain.check_blocks_final_and_canonical(headers.iter().map(|v| &**v)) {
            Err(_) => TxExecutionStatus::Executed,
            Ok(_) => TxExecutionStatus::Final,
        })
    }

    /// Read the receipts that are outgoing from the given execution outcome,
    /// even before they have been validated on chain and stored in
    /// DbCol::Receipts.
    fn outgoing_receipts_for_outcome(
        &self,
        outcome: &ExecutionOutcomeWithIdView,
    ) -> Result<Arc<Vec<Receipt>>, TxStatusError> {
        let epoch_id = &self.epoch_manager.get_epoch_id(&outcome.block_hash).into_chain_error()?;
        let shard_id = account_id_to_shard_id(
            self.epoch_manager.as_ref(),
            &outcome.outcome.executor_id,
            epoch_id,
        )
        .into_chain_error()?;
        let outgoing_receipts =
            self.chain.chain_store().get_outgoing_receipts(&outcome.block_hash, shard_id)?;
        Ok(outgoing_receipts)
    }

    fn get_tx_status(
        &self,
        tx_hash: CryptoHash,
        signer_account_id: AccountId,
        fetch_receipt: bool,
    ) -> Result<TxStatusView, TxStatusError> {
        {
            // TODO(telezhnaya): take into account `fetch_receipt()`
            // https://github.com/near/nearcore/issues/9545
            let mut request_manager = self.request_manager.write();
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
        let target_shard_id =
            account_id_to_shard_id(self.epoch_manager.as_ref(), &signer_account_id, &head.epoch_id)
                .map_err(|err| TxStatusError::InternalError(err.to_string()))?;
        // Check if we are tracking this shard.
        if self.shard_tracker.cares_about_shard(&head.prev_block_hash, target_shard_id) {
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
                        let transaction =
                            SignedTransactionView::from(Arc::unwrap_or_clone(transaction));
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
            let mut request_manager = self.request_manager.write();
            if self.need_request(tx_hash, &mut request_manager.tx_status_requests) {
                let target_shard_id = account_id_to_shard_id(
                    self.epoch_manager.as_ref(),
                    &signer_account_id,
                    &head.epoch_id,
                )
                .map_err(|err| TxStatusError::InternalError(err.to_string()))?;
                let validator = self
                    .epoch_manager
                    .get_chunk_producer_info(&ChunkProductionKey {
                        epoch_id: head.epoch_id,
                        height_created: head.height + self.config.tx_routing_height_horizon - 1,
                        shard_id: target_shard_id,
                    })
                    .map(|info| info.take_account_id())
                    .map_err(|err| TxStatusError::ChainError(err.into()))?;

                self.network_adapter.send(PeerManagerMessageRequest::NetworkRequests(
                    NetworkRequests::TxStatus(validator, signer_account_id, tx_hash),
                ));
            }
            Ok(TxStatusView { execution_outcome: None, status: TxExecutionStatus::None })
        }
    }

    fn retrieve_headers(
        &self,
        hashes: Vec<CryptoHash>,
    ) -> Result<Vec<Arc<BlockHeader>>, near_chain::Error> {
        retrieve_headers(self.chain.chain_store(), hashes, sync::header::MAX_BLOCK_HEADERS)
    }

    fn check_signature_account_announce(
        &self,
        announce_account: &AnnounceAccount,
    ) -> Result<bool, Error> {
        let announce_hash = announce_account.hash();
        self.epoch_manager
            .verify_validator_signature(
                &announce_account.epoch_id,
                &announce_account.account_id,
                announce_hash.as_ref(),
                &announce_account.signature,
            )
            .map_err(|e| e.into())
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
        Ok(BlockView::from_author_block(block_author, &block))
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

fn get_chunk_from_block(
    block: &Block,
    shard_id: ShardId,
    chain: &Chain,
) -> Result<ShardChunk, near_chain::Error> {
    let epoch_id = block.header().epoch_id();
    let shard_layout = chain.epoch_manager.get_shard_layout(epoch_id)?;
    let shard_index = shard_layout.get_shard_index(shard_id)?;
    let chunk_header =
        block.chunks().get(shard_index).ok_or(near_chain::Error::InvalidShardId(shard_id))?.clone();
    let chunk_hash = chunk_header.chunk_hash().clone();
    let chunk = chain.get_chunk(&chunk_hash)?;
    let res =
        ShardChunk::with_header(ShardChunk::clone(&chunk), chunk_header).ok_or_else(|| {
            near_chain::Error::Other(format!(
                "Mismatched versions for chunk with hash {}",
                chunk_hash.0
            ))
        })?;
    Ok(res)
}

impl Handler<GetShardChunk> for ViewClientActorInner {
    #[perf]
    fn handle(&mut self, msg: GetShardChunk) -> Result<ShardChunk, GetChunkError> {
        tracing::debug!(target: "client", ?msg);
        let _timer =
            metrics::VIEW_CLIENT_MESSAGE_TIME.with_label_values(&["GetShardChunk"]).start_timer();

        match msg {
            GetShardChunk::ChunkHash(chunk_hash) => {
                let chunk = self.chain.get_chunk(&chunk_hash)?;
                Ok(ShardChunk::clone(&chunk))
            }
            GetShardChunk::BlockHash(block_hash, shard_id) => {
                let block = self.chain.get_block(&block_hash)?;
                Ok(get_chunk_from_block(&block, shard_id, &self.chain)?)
            }
            GetShardChunk::Height(height, shard_id) => {
                let block = self.chain.get_block_by_height(height)?;
                Ok(get_chunk_from_block(&block, shard_id, &self.chain)?)
            }
        }
    }
}

impl Handler<GetChunk> for ViewClientActorInner {
    #[perf]
    fn handle(&mut self, msg: GetChunk) -> Result<ChunkView, GetChunkError> {
        tracing::debug!(target: "client", ?msg);
        let _timer =
            metrics::VIEW_CLIENT_MESSAGE_TIME.with_label_values(&["GetChunk"]).start_timer();

        let chunk = match msg {
            GetChunk::ChunkHash(chunk_hash) => {
                let chunk = self.chain.get_chunk(&chunk_hash)?;
                ShardChunk::clone(&chunk)
            }
            GetChunk::BlockHash(block_hash, shard_id) => {
                let block = self.chain.get_block(&block_hash)?;
                get_chunk_from_block(&block, shard_id, &self.chain)?
            }
            GetChunk::Height(height, shard_id) => {
                let block = self.chain.get_block_by_height(height)?;
                get_chunk_from_block(&block, shard_id, &self.chain)?
            }
        };

        let chunk_inner = chunk.cloned_header().take_inner();
        let epoch_id = self
            .epoch_manager
            .get_epoch_id_from_prev_block(chunk_inner.prev_block_hash())
            .into_chain_error()?;
        let author = self
            .epoch_manager
            .get_chunk_producer_info(&ChunkProductionKey {
                epoch_id,
                height_created: chunk_inner.height_created(),
                shard_id: chunk_inner.shard_id(),
            })
            .map(|info| info.take_account_id())
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
        self.get_tx_status(msg.tx_hash, msg.signer_account_id, msg.fetch_receipt)
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
                    ValidatorInfoIdentifier::EpochId(*block_header.epoch_id())
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
            get_epoch_block_producers_view(header.epoch_id(), self.epoch_manager.as_ref())
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
            let shard_id = match account_id_to_shard_id(
                self.epoch_manager.as_ref(),
                account_id,
                &msg.epoch_id,
            ) {
                Ok(shard_id) => shard_id,
                Err(err) => {
                    return Err(GetStateChangesError::IOError { error_message: err.to_string() });
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
        let last_epoch_id = *last_block_header.epoch_id();
        let last_next_epoch_id = *last_block_header.next_epoch_id();
        let last_height = last_block_header.height();
        let head = self.chain.head()?;

        if last_epoch_id == head.epoch_id || last_next_epoch_id == head.epoch_id {
            let head_header = self.chain.get_block_header(&head.last_block_hash)?;
            let ret = Chain::create_light_client_block(
                &head_header,
                self.epoch_manager.as_ref(),
                self.chain.chain_store(),
            )?;

            if ret.inner_lite.height <= last_height { Ok(None) } else { Ok(Some(Arc::new(ret))) }
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
                    *self.chain.get_block(&outcome_proof.block_hash)?.header().epoch_id();
                let shard_layout =
                    self.epoch_manager.get_shard_layout(&epoch_id).into_chain_error()?;
                let target_shard_id =
                    account_id_to_shard_id(self.epoch_manager.as_ref(), &account_id, &epoch_id)
                        .into_chain_error()?;
                let target_shard_index = shard_layout
                    .get_shard_index(target_shard_id)
                    .map_err(Into::into)
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
                        .map(|header| *header.prev_outcome_root())
                        .collect::<Vec<_>>();
                    if target_shard_index >= outcome_roots.len() {
                        return Err(GetExecutionOutcomeError::InconsistentState {
                            number_or_shards: outcome_roots.len(),
                            execution_outcome_shard_id: target_shard_id,
                        });
                    }
                    Ok(GetExecutionOutcomeResponse {
                        outcome_proof: outcome_proof.into(),
                        outcome_root_proof: merklize(&outcome_roots).1[target_shard_index].clone(),
                    })
                } else {
                    Err(GetExecutionOutcomeError::NotConfirmed { transaction_or_receipt_id: id })
                }
            }
            Err(near_chain::Error::DBNotFoundErr(_)) => {
                let head = self.chain.head()?;
                let target_shard_id = account_id_to_shard_id(
                    self.epoch_manager.as_ref(),
                    &account_id,
                    &head.epoch_id,
                )
                .into_chain_error()?;
                if self.shard_tracker.cares_about_shard(&head.last_block_hash, target_shard_id) {
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
        let block_header = BlockHeader::clone(&block_header);
        let head_block_header = self.chain.get_block_header(&msg.head_block_hash)?;
        let head_block_header = BlockHeader::clone(&head_block_header);
        self.chain.check_blocks_final_and_canonical(&[block_header.clone(), head_block_header])?;
        let block_header_lite = block_header.into();
        let proof = self.chain.compute_past_block_proof_in_merkle_tree_of_later_block(
            &msg.block_hash,
            &msg.head_block_hash,
        )?;
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
                return Err(GetProtocolConfigError::UnknownBlock("EarliestAvailable".to_string()));
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
                tracing::info!(target: "adversary", "Turning Doomslug off");
                self.adv.set_disable_doomslug(true);
            }
            NetworkAdversarialMessage::AdvDisableHeaderSync => {
                tracing::info!(target: "adversary", "Blocking header sync");
                self.adv.set_disable_header_sync(true);
            }
            NetworkAdversarialMessage::AdvSwitchToHeight(height) => {
                tracing::info!(target: "adversary", "Switching to height");
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
        if let Ok(Some(result)) =
            self.get_tx_status(tx_hash, signer_account_id, false).map(|s| s.execution_outcome)
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
        let TxStatusResponse(tx_result) = msg;
        tracing::debug!(
            target: "client",
            tx_hash = %tx_result.transaction.hash, status = ?tx_result.status,
            tx_outcome_status = ?tx_result.transaction_outcome.outcome.status,
            num_receipt_outcomes = tx_result.receipts_outcome.len(),
            "receive TxStatusResponse"
        );
        let _timer = metrics::VIEW_CLIENT_MESSAGE_TIME
            .with_label_values(&["TxStatusResponse"])
            .start_timer();
        let tx_hash = tx_result.transaction_outcome.id;
        let mut request_manager = self.request_manager.write();
        if request_manager.tx_status_requests.pop(&tx_hash).is_some() {
            request_manager.tx_status_response.put(tx_hash, *tx_result);
        }
    }
}

impl Handler<BlockRequest> for ViewClientActorInner {
    #[perf]
    fn handle(&mut self, msg: BlockRequest) -> Option<Arc<Block>> {
        tracing::debug!(target: "client", ?msg);
        let _timer =
            metrics::VIEW_CLIENT_MESSAGE_TIME.with_label_values(&["BlockRequest"]).start_timer();
        let BlockRequest(hash) = msg;
        if let Ok(block) = self.chain.get_block(&hash) { Some(block) } else { None }
    }
}

impl Handler<BlockHeadersRequest> for ViewClientActorInner {
    #[perf]
    fn handle(&mut self, msg: BlockHeadersRequest) -> Option<Vec<Arc<BlockHeader>>> {
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
