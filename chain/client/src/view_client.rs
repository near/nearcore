//! Readonly view of the chain and state of the database.
//! Useful for querying from RPC.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::hash::Hash;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use actix::{Actor, Addr, Handler, SyncArbiter, SyncContext};
use cached::{Cached, SizedCache};
use log::{debug, error, info, trace, warn};

use near_chain::types::ValidatorInfoIdentifier;
use near_chain::{
    get_epoch_block_producers_view, Chain, ChainGenesis, ChainStoreAccess, DoomslugThresholdMode,
    ErrorKind, RuntimeAdapter,
};
use near_chain_configs::{ClientConfig, ProtocolConfigView};
use near_client_primitives::types::{
    Error, GetBlock, GetBlockError, GetBlockProof, GetBlockProofError, GetBlockProofResponse,
    GetBlockWithMerkleTree, GetChunkError, GetExecutionOutcome, GetExecutionOutcomeError,
    GetExecutionOutcomesForBlock, GetGasPrice, GetGasPriceError, GetNextLightClientBlockError,
    GetProtocolConfig, GetProtocolConfigError, GetReceipt, GetReceiptError, GetStateChangesError,
    GetStateChangesWithCauseInBlock, GetValidatorInfoError, Query, QueryError, TxStatus,
    TxStatusError,
};
#[cfg(feature = "adversarial")]
use near_network::types::NetworkAdversarialMessage;
use near_network::types::{
    NetworkViewClientMessages, NetworkViewClientResponses, ReasonForBan, StateResponseInfo,
    StateResponseInfoV1, StateResponseInfoV2,
};
use near_network::{NetworkAdapter, NetworkRequests};
use near_performance_metrics_macros::perf;
use near_performance_metrics_macros::perf_with_debug;
use near_primitives::block::{Block, BlockHeader, GenesisId, Tip};
use near_primitives::hash::CryptoHash;
use near_primitives::merkle::{merklize, PartialMerkleTree};
use near_primitives::network::AnnounceAccount;
use near_primitives::sharding::ShardChunk;
use near_primitives::syncing::{
    ShardStateSyncResponse, ShardStateSyncResponseHeader, ShardStateSyncResponseV1,
    ShardStateSyncResponseV2,
};
use near_primitives::types::{
    AccountId, BlockHeight, BlockId, BlockReference, EpochReference, Finality, MaybeBlockId,
    ShardId, TransactionOrReceiptId,
};
use near_primitives::views::validator_stake_view::ValidatorStakeView;
use near_primitives::views::{
    BlockView, ChunkView, EpochValidatorInfo, ExecutionOutcomeWithIdView,
    FinalExecutionOutcomeView, FinalExecutionOutcomeViewEnum, FinalExecutionStatus, GasPriceView,
    LightClientBlockView, QueryRequest, QueryResponse, ReceiptView, StateChangesKindsView,
    StateChangesView,
};

use crate::{
    sync, GetChunk, GetExecutionOutcomeResponse, GetNextLightClientBlock, GetStateChanges,
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
    pub tx_status_requests: SizedCache<CryptoHash, Instant>,
    /// Transaction status response
    pub tx_status_response: SizedCache<CryptoHash, FinalExecutionOutcomeView>,
    /// Query requests that need to be forwarded to other shards
    pub query_requests: SizedCache<String, Instant>,
    /// Query responses from other nodes (can be errors)
    pub query_responses: SizedCache<String, Result<QueryResponse, String>>,
    /// Receipt outcome requests
    pub receipt_outcome_requests: SizedCache<CryptoHash, Instant>,
}

#[cfg(feature = "adversarial")]
#[derive(Default)]
pub struct AdversarialControls {
    pub adv_disable_header_sync: bool,
    pub adv_disable_doomslug: bool,
    pub adv_sync_height: Option<u64>,
}

/// View client provides currently committed (to the storage) view of the current chain and state.
pub struct ViewClientActor {
    #[cfg(feature = "adversarial")]
    pub adv: Arc<RwLock<AdversarialControls>>,

    /// Validator account (if present).
    validator_account_id: Option<AccountId>,
    chain: Chain,
    runtime_adapter: Arc<dyn RuntimeAdapter>,
    network_adapter: Arc<dyn NetworkAdapter>,
    pub config: ClientConfig,
    request_manager: Arc<RwLock<ViewClientRequestManager>>,
    state_request_cache: Arc<Mutex<VecDeque<Instant>>>,
}

impl ViewClientRequestManager {
    pub fn new() -> Self {
        Self {
            tx_status_requests: SizedCache::with_size(QUERY_REQUEST_LIMIT),
            tx_status_response: SizedCache::with_size(QUERY_REQUEST_LIMIT),
            query_requests: SizedCache::with_size(QUERY_REQUEST_LIMIT),
            query_responses: SizedCache::with_size(QUERY_REQUEST_LIMIT),
            receipt_outcome_requests: SizedCache::with_size(QUERY_REQUEST_LIMIT),
        }
    }
}

impl ViewClientActor {
    /// Maximum number of state requests allowed per `view_client_throttle_period`.
    const MAX_NUM_STATE_REQUESTS: usize = 30;

    pub fn new(
        validator_account_id: Option<AccountId>,
        chain_genesis: &ChainGenesis,
        runtime_adapter: Arc<dyn RuntimeAdapter>,
        network_adapter: Arc<dyn NetworkAdapter>,
        config: ClientConfig,
        request_manager: Arc<RwLock<ViewClientRequestManager>>,
        #[cfg(feature = "adversarial")] adv: Arc<RwLock<AdversarialControls>>,
    ) -> Result<Self, Error> {
        // TODO: should we create shared ChainStore that is passed to both Client and ViewClient?
        let chain = Chain::new_for_view_client(
            runtime_adapter.clone(),
            chain_genesis,
            DoomslugThresholdMode::TwoThirds,
        )?;
        Ok(ViewClientActor {
            #[cfg(feature = "adversarial")]
            adv,
            validator_account_id,
            chain,
            runtime_adapter,
            network_adapter,
            config,
            request_manager,
            state_request_cache: Arc::new(Mutex::new(VecDeque::default())),
        })
    }

    fn maybe_block_id_to_block_hash(
        &mut self,
        block_id: MaybeBlockId,
    ) -> Result<CryptoHash, near_chain::Error> {
        match block_id {
            None => Ok(self.chain.head()?.last_block_hash),
            Some(BlockId::Height(height)) => Ok(*self.chain.get_header_by_height(height)?.hash()),
            Some(BlockId::Hash(block_hash)) => Ok(block_hash),
        }
    }

    fn need_request<K: Hash + Eq + Clone>(key: K, cache: &mut SizedCache<K, Instant>) -> bool {
        let now = Instant::now();
        let need_request = match cache.cache_get(&key) {
            Some(time) => now - *time > Duration::from_millis(REQUEST_WAIT_TIME),
            None => true,
        };
        if need_request {
            cache.cache_set(key, now);
        }
        need_request
    }

    fn get_block_hash_by_finality(
        &mut self,
        finality: &Finality,
    ) -> Result<CryptoHash, near_chain::Error> {
        let head_header = self.chain.head_header()?;
        match finality {
            Finality::None => Ok(*head_header.hash()),
            Finality::DoomSlug => Ok(*head_header.last_ds_final_block()),
            Finality::Final => self.chain.final_head().map(|t| t.last_block_hash),
        }
    }

    fn get_block_hash_by_sync_checkpoint(
        &mut self,
        synchronization_checkpoint: &near_primitives::types::SyncCheckpoint,
    ) -> Result<Option<CryptoHash>, near_chain::Error> {
        use near_primitives::types::SyncCheckpoint;

        match synchronization_checkpoint {
            SyncCheckpoint::Genesis => Ok(Some(self.chain.genesis().hash().clone())),
            SyncCheckpoint::EarliestAvailable => self.chain.get_earliest_block_hash(),
        }
    }

    fn handle_query(&mut self, msg: Query) -> Result<QueryResponse, QueryError> {
        let header = match msg.block_reference {
            BlockReference::BlockId(BlockId::Height(block_height)) => {
                self.chain.get_header_by_height(block_height)
            }
            BlockReference::BlockId(BlockId::Hash(block_hash)) => {
                self.chain.get_block_header(&block_hash)
            }
            BlockReference::Finality(ref finality) => self
                .get_block_hash_by_finality(&finality)
                .and_then(|block_hash| self.chain.get_block_header(&block_hash)),
            BlockReference::SyncCheckpoint(ref synchronization_checkpoint) => {
                if let Some(block_hash) = self
                    .get_block_hash_by_sync_checkpoint(&synchronization_checkpoint)
                    .map_err(|err| match err.kind() {
                        near_chain::near_chain_primitives::ErrorKind::DBNotFoundErr(_) => {
                            QueryError::UnknownBlock {
                                block_reference: msg.block_reference.clone(),
                            }
                        }
                        near_chain::near_chain_primitives::ErrorKind::IOErr(error_message) => {
                            QueryError::InternalError { error_message }
                        }
                        _ => QueryError::Unreachable { error_message: err.to_string() },
                    })?
                {
                    self.chain.get_block_header(&block_hash)
                } else {
                    return Err(QueryError::NoSyncedBlocks);
                }
            }
        };
        let header = header
            .map_err(|err| match err.kind() {
                near_chain::near_chain_primitives::ErrorKind::DBNotFoundErr(_) => {
                    QueryError::UnknownBlock { block_reference: msg.block_reference.clone() }
                }
                near_chain::near_chain_primitives::ErrorKind::IOErr(error_message) => {
                    QueryError::InternalError { error_message }
                }
                _ => QueryError::Unreachable { error_message: err.to_string() },
            })?
            .clone();

        let account_id = match &msg.request {
            QueryRequest::ViewAccount { account_id, .. } => account_id,
            QueryRequest::ViewState { account_id, .. } => account_id,
            QueryRequest::ViewAccessKey { account_id, .. } => account_id,
            QueryRequest::ViewAccessKeyList { account_id, .. } => account_id,
            QueryRequest::CallFunction { account_id, .. } => account_id,
            QueryRequest::ViewCode { account_id, .. } => account_id,
        };
        let shard_id = self.runtime_adapter.account_id_to_shard_id(account_id);

        let chunk_extra = self.chain.get_chunk_extra(header.hash(), shard_id).map_err(|err| {
            match err.kind() {
                near_chain::near_chain_primitives::ErrorKind::DBNotFoundErr(_) => {
                    QueryError::UnavailableShard { requested_shard_id: shard_id }
                }
                near_chain::near_chain_primitives::ErrorKind::IOErr(error_message) => {
                    QueryError::InternalError { error_message }
                }
                _ => QueryError::Unreachable { error_message: err.to_string() },
            }
        })?;

        let state_root = chunk_extra.state_root();
        match self.runtime_adapter.query(
            shard_id,
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

    fn request_receipt_outcome(
        &mut self,
        receipt_id: CryptoHash,
        last_block_hash: &CryptoHash,
    ) -> Result<(), TxStatusError> {
        if let Ok(&dst_shard_id) = self.chain.get_shard_id_for_receipt_id(&receipt_id) {
            if self.chain.get_chunk_extra(last_block_hash, dst_shard_id).is_err() {
                let mut request_manager = self.request_manager.write().expect(POISONED_LOCK_ERR);
                if Self::need_request(receipt_id, &mut request_manager.receipt_outcome_requests) {
                    let validator = self
                        .chain
                        .find_validator_for_forwarding(dst_shard_id)
                        .map_err(|e| TxStatusError::ChainError(e))?;
                    self.network_adapter
                        .do_send(NetworkRequests::ReceiptOutComeRequest(validator, receipt_id));
                }
            }
        }

        Ok(())
    }

    fn get_tx_status(
        &mut self,
        tx_hash: CryptoHash,
        signer_account_id: AccountId,
        fetch_receipt: bool,
    ) -> Result<Option<FinalExecutionOutcomeViewEnum>, TxStatusError> {
        {
            let mut request_manager = self.request_manager.write().expect(POISONED_LOCK_ERR);
            if let Some(res) = request_manager.tx_status_response.cache_remove(&tx_hash) {
                request_manager.tx_status_requests.cache_remove(&tx_hash);
                return Ok(Some(FinalExecutionOutcomeViewEnum::FinalExecutionOutcome(res)));
            }
        }

        let head = self.chain.head().map_err(|e| TxStatusError::ChainError(e))?;
        let target_shard_id = self.runtime_adapter.account_id_to_shard_id(&signer_account_id);
        // Check if we are tracking this shard.
        if self.runtime_adapter.cares_about_shard(
            self.validator_account_id.as_ref(),
            &head.last_block_hash,
            target_shard_id,
            true,
        ) {
            match self.chain.get_final_transaction_result(&tx_hash) {
                Ok(tx_result) => {
                    match &tx_result.status {
                        FinalExecutionStatus::NotStarted | FinalExecutionStatus::Started => {
                            for receipt_view in tx_result.receipts_outcome.iter() {
                                self.request_receipt_outcome(
                                    receipt_view.id,
                                    &head.last_block_hash,
                                )?;
                            }
                        }
                        FinalExecutionStatus::SuccessValue(_)
                        | FinalExecutionStatus::Failure(_) => {}
                    }
                    if fetch_receipt {
                        let final_result = self
                            .chain
                            .get_final_transaction_result_with_receipt(tx_result)
                            .map_err(|e| TxStatusError::ChainError(e))?;
                        return Ok(Some(
                            FinalExecutionOutcomeViewEnum::FinalExecutionOutcomeWithReceipt(
                                final_result,
                            ),
                        ));
                    }
                    return Ok(Some(FinalExecutionOutcomeViewEnum::FinalExecutionOutcome(
                        tx_result,
                    )));
                }
                Err(e) => match e.kind() {
                    ErrorKind::DBNotFoundErr(_) => {
                        if let Ok(execution_outcome) = self.chain.get_execution_outcome(&tx_hash) {
                            for receipt_id in execution_outcome.outcome_with_id.outcome.receipt_ids
                            {
                                self.request_receipt_outcome(receipt_id, &head.last_block_hash)?;
                            }
                            return Ok(None);
                        } else {
                            return Err(TxStatusError::MissingTransaction(tx_hash));
                        }
                    }
                    _ => {
                        warn!(target: "client", "Error trying to get transaction result: {}", e.to_string());
                        return Err(TxStatusError::ChainError(e));
                    }
                },
            }
        } else {
            let mut request_manager = self.request_manager.write().expect(POISONED_LOCK_ERR);
            if Self::need_request(tx_hash, &mut request_manager.tx_status_requests) {
                let target_shard_id =
                    self.runtime_adapter.account_id_to_shard_id(&signer_account_id);
                let validator = self
                    .chain
                    .find_validator_for_forwarding(target_shard_id)
                    .map_err(|e| TxStatusError::ChainError(e))?;

                self.network_adapter.do_send(NetworkRequests::TxStatus(
                    validator,
                    signer_account_id,
                    tx_hash,
                ));
            }
        }
        Ok(None)
    }

    fn retrieve_headers(
        &mut self,
        hashes: Vec<CryptoHash>,
    ) -> Result<Vec<BlockHeader>, near_chain::Error> {
        let header = match self.chain.find_common_header(&hashes) {
            Some(header) => header,
            None => return Ok(vec![]),
        };

        let mut headers = vec![];
        let max_height = self.chain.header_head()?.height;
        // TODO: this may be inefficient if there are a lot of skipped blocks.
        for h in header.height() + 1..=max_height {
            if let Ok(header) = self.chain.get_header_by_height(h) {
                headers.push(header.clone());
                if headers.len() >= sync::MAX_BLOCK_HEADERS as usize {
                    break;
                }
            }
        }
        Ok(headers)
    }

    fn check_signature_account_announce(
        &self,
        announce_account: &AnnounceAccount,
    ) -> Result<bool, Error> {
        let announce_hash = announce_account.hash();
        let head = self.chain.head()?;

        self.runtime_adapter
            .verify_validator_signature(
                &announce_account.epoch_id,
                &head.last_block_hash,
                &announce_account.account_id,
                announce_hash.as_ref(),
                &announce_account.signature,
            )
            .map_err(|e| e.into())
    }

    fn get_height(&self, head: &Tip) -> BlockHeight {
        #[cfg(feature = "adversarial")]
        {
            if let Some(height) = self.adv.read().unwrap().adv_sync_height {
                return height;
            }
        }

        head.height
    }

    fn check_state_sync_request(&self) -> bool {
        let mut cache = self.state_request_cache.lock().expect(POISONED_LOCK_ERR);
        let now = Instant::now();
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

impl Handler<Query> for ViewClientActor {
    type Result = Result<QueryResponse, QueryError>;

    #[perf]
    fn handle(&mut self, msg: Query, _: &mut Self::Context) -> Self::Result {
        self.handle_query(msg)
    }
}

/// Handles retrieving block from the chain.
impl Handler<GetBlock> for ViewClientActor {
    type Result = Result<BlockView, GetBlockError>;

    #[perf]
    fn handle(&mut self, msg: GetBlock, _: &mut Self::Context) -> Self::Result {
        let block = match msg.0 {
            BlockReference::Finality(finality) => {
                let block_hash = self.get_block_hash_by_finality(&finality)?;
                self.chain.get_block(&block_hash).map(Clone::clone)
            }
            BlockReference::BlockId(BlockId::Height(height)) => {
                self.chain.get_block_by_height(height).map(Clone::clone)
            }
            BlockReference::BlockId(BlockId::Hash(hash)) => {
                self.chain.get_block(&hash).map(Clone::clone)
            }
            BlockReference::SyncCheckpoint(sync_checkpoint) => {
                if let Some(block_hash) =
                    self.get_block_hash_by_sync_checkpoint(&sync_checkpoint)?
                {
                    self.chain.get_block(&block_hash).map(Clone::clone)
                } else {
                    return Err(GetBlockError::NotSyncedYet);
                }
            }
        }?;

        let block_author = self
            .runtime_adapter
            .get_block_producer(&block.header().epoch_id(), block.header().height())?;

        Ok(BlockView::from_author_block(block_author, block))
    }
}

impl Handler<GetBlockWithMerkleTree> for ViewClientActor {
    type Result = Result<(BlockView, PartialMerkleTree), GetBlockError>;

    #[perf]
    fn handle(&mut self, msg: GetBlockWithMerkleTree, ctx: &mut Self::Context) -> Self::Result {
        let block_view = self.handle(GetBlock(msg.0), ctx)?;
        self.chain
            .mut_store()
            .get_block_merkle_tree(&block_view.header.hash)
            .map(|merkle_tree| (block_view, merkle_tree.clone()))
            .map_err(|e| e.into())
    }
}

impl Handler<GetChunk> for ViewClientActor {
    type Result = Result<ChunkView, GetChunkError>;

    #[perf]
    fn handle(&mut self, msg: GetChunk, _: &mut Self::Context) -> Self::Result {
        let get_chunk_from_block = |block: Block,
                                    shard_id: ShardId,
                                    chain: &mut Chain|
         -> Result<ShardChunk, near_chain::Error> {
            let chunk_header = block
                .chunks()
                .get(shard_id as usize)
                .ok_or_else(|| near_chain::Error::from(ErrorKind::InvalidShardId(shard_id)))?
                .clone();
            let chunk_hash = chunk_header.chunk_hash();
            chain.get_chunk(&chunk_hash).and_then(|chunk| {
                ShardChunk::with_header(chunk.clone(), chunk_header).ok_or(near_chain::Error::from(
                    ErrorKind::Other(format!(
                        "Mismatched versions for chunk with hash {}",
                        chunk_hash.0
                    )),
                ))
            })
        };

        let chunk = match msg {
            GetChunk::ChunkHash(chunk_hash) => self.chain.get_chunk(&chunk_hash)?.clone(),
            GetChunk::BlockHash(block_hash, shard_id) => {
                let block = self.chain.get_block(&block_hash)?.clone();
                get_chunk_from_block(block, shard_id, &mut self.chain)?
            }
            GetChunk::Height(height, shard_id) => {
                let block = self.chain.get_block_by_height(height)?.clone();
                get_chunk_from_block(block, shard_id, &mut self.chain)?
            }
        };

        let chunk_inner = chunk.cloned_header().take_inner();
        let epoch_id =
            self.runtime_adapter.get_epoch_id_from_prev_block(chunk_inner.prev_block_hash())?;
        let author = self.runtime_adapter.get_chunk_producer(
            &epoch_id,
            chunk_inner.height_created(),
            chunk_inner.shard_id(),
        )?;

        Ok(ChunkView::from_author_chunk(author, chunk))
    }
}

impl Handler<TxStatus> for ViewClientActor {
    type Result = Result<Option<FinalExecutionOutcomeViewEnum>, TxStatusError>;

    #[perf]
    fn handle(&mut self, msg: TxStatus, _: &mut Self::Context) -> Self::Result {
        self.get_tx_status(msg.tx_hash, msg.signer_account_id, msg.fetch_receipt)
    }
}

impl Handler<GetValidatorInfo> for ViewClientActor {
    type Result = Result<EpochValidatorInfo, GetValidatorInfoError>;

    #[perf]
    fn handle(&mut self, msg: GetValidatorInfo, _: &mut Self::Context) -> Self::Result {
        let epoch_identifier = match msg.epoch_reference {
            EpochReference::EpochId(id) => ValidatorInfoIdentifier::EpochId(id),
            EpochReference::BlockId(block_id) => {
                let block_header = match block_id {
                    BlockId::Hash(h) => self.chain.get_block_header(&h)?.clone(),
                    BlockId::Height(h) => self.chain.get_header_by_height(h)?.clone(),
                };
                let next_block_hash =
                    *self.chain.mut_store().get_next_block_hash(block_header.hash())?;
                let next_block_header = self.chain.get_block_header(&next_block_hash)?.clone();
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
        self.runtime_adapter
            .get_validator_info(epoch_identifier)
            .map_err(GetValidatorInfoError::from)
    }
}

impl Handler<GetValidatorOrdered> for ViewClientActor {
    type Result = Result<Vec<ValidatorStakeView>, GetValidatorInfoError>;

    #[perf]
    fn handle(&mut self, msg: GetValidatorOrdered, _: &mut Self::Context) -> Self::Result {
        Ok(self
            .maybe_block_id_to_block_hash(msg.block_id)
            .and_then(|block_hash| self.chain.get_block_header(&block_hash).map(|h| h.clone()))
            .and_then(|header| {
                get_epoch_block_producers_view(
                    header.epoch_id(),
                    header.prev_hash(),
                    &*self.runtime_adapter,
                )
            })?)
    }
}
/// Returns a list of change kinds per account in a store for a given block.
impl Handler<GetStateChangesInBlock> for ViewClientActor {
    type Result = Result<StateChangesKindsView, GetStateChangesError>;

    #[perf]
    fn handle(&mut self, msg: GetStateChangesInBlock, _: &mut Self::Context) -> Self::Result {
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
impl Handler<GetStateChanges> for ViewClientActor {
    type Result = Result<StateChangesView, GetStateChangesError>;

    #[perf]
    fn handle(&mut self, msg: GetStateChanges, _: &mut Self::Context) -> Self::Result {
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
impl Handler<GetStateChangesWithCauseInBlock> for ViewClientActor {
    type Result = Result<StateChangesView, GetStateChangesError>;

    #[perf]
    fn handle(
        &mut self,
        msg: GetStateChangesWithCauseInBlock,
        _: &mut Self::Context,
    ) -> Self::Result {
        Ok(self
            .chain
            .store()
            .get_state_changes_with_cause_in_block(&msg.block_hash)?
            .into_iter()
            .map(Into::into)
            .collect())
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
impl Handler<GetNextLightClientBlock> for ViewClientActor {
    type Result = Result<Option<LightClientBlockView>, GetNextLightClientBlockError>;

    #[perf]
    fn handle(&mut self, msg: GetNextLightClientBlock, _: &mut Self::Context) -> Self::Result {
        let last_block_header = self.chain.get_block_header(&msg.last_block_hash)?;
        let last_epoch_id = last_block_header.epoch_id().clone();
        let last_next_epoch_id = last_block_header.next_epoch_id().clone();
        let last_height = last_block_header.height();
        let head = self.chain.head()?;

        if last_epoch_id == head.epoch_id || last_next_epoch_id == head.epoch_id {
            let head_header = self.chain.get_block_header(&head.last_block_hash)?;
            let ret = Chain::create_light_client_block(
                &head_header.clone(),
                &*self.runtime_adapter,
                self.chain.mut_store(),
            )?;

            if ret.inner_lite.height <= last_height {
                Ok(None)
            } else {
                Ok(Some(ret))
            }
        } else {
            match self.chain.mut_store().get_epoch_light_client_block(&last_next_epoch_id.0) {
                Ok(light_block) => Ok(Some(light_block.clone())),
                Err(e) => {
                    if let ErrorKind::DBNotFoundErr(_) = e.kind() {
                        Ok(None)
                    } else {
                        Err(e.into())
                    }
                }
            }
        }
    }
}

impl Handler<GetExecutionOutcome> for ViewClientActor {
    type Result = Result<GetExecutionOutcomeResponse, GetExecutionOutcomeError>;

    #[perf]
    fn handle(&mut self, msg: GetExecutionOutcome, _: &mut Self::Context) -> Self::Result {
        let (id, target_shard_id) = match msg.id {
            TransactionOrReceiptId::Transaction { transaction_hash, sender_id } => {
                (transaction_hash, self.runtime_adapter.account_id_to_shard_id(&sender_id))
            }
            TransactionOrReceiptId::Receipt { receipt_id, receiver_id } => {
                (receipt_id, self.runtime_adapter.account_id_to_shard_id(&receiver_id))
            }
        };
        match self.chain.get_execution_outcome(&id) {
            Ok(outcome) => {
                let mut outcome_proof = outcome.clone();
                let next_block_hash = self
                    .chain
                    .get_next_block_hash_with_new_chunk(&outcome_proof.block_hash, target_shard_id)?
                    .cloned();
                match next_block_hash {
                    Some(h) => {
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
            Err(e) => match e.kind() {
                ErrorKind::DBNotFoundErr(_) => {
                    let head = self.chain.head().map_err(|e| TxStatusError::ChainError(e))?;
                    if self.runtime_adapter.cares_about_shard(
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
                _ => Err(e.into()),
            },
        }
    }
}

/// Extract the list of execution outcomes that were produced in a given block
/// (including those created for local receipts).
impl Handler<GetExecutionOutcomesForBlock> for ViewClientActor {
    type Result = Result<HashMap<ShardId, Vec<ExecutionOutcomeWithIdView>>, String>;

    #[perf]
    fn handle(&mut self, msg: GetExecutionOutcomesForBlock, _: &mut Self::Context) -> Self::Result {
        Ok(self
            .chain
            .get_block_execution_outcomes(&msg.block_hash)
            .map_err(|e| e.to_string())?
            .into_iter()
            .map(|(k, v)| (k, v.into_iter().map(Into::into).collect()))
            .collect())
    }
}

impl Handler<GetReceipt> for ViewClientActor {
    type Result = Result<Option<ReceiptView>, GetReceiptError>;

    #[perf]
    fn handle(&mut self, msg: GetReceipt, _: &mut Self::Context) -> Self::Result {
        Ok(self
            .chain
            .mut_store()
            .get_receipt(&msg.receipt_id)?
            .map(|receipt| receipt.clone().into()))
    }
}

impl Handler<GetBlockProof> for ViewClientActor {
    type Result = Result<GetBlockProofResponse, GetBlockProofError>;

    #[perf]
    fn handle(&mut self, msg: GetBlockProof, _: &mut Self::Context) -> Self::Result {
        self.chain.check_block_final_and_canonical(&msg.block_hash)?;
        self.chain.check_block_final_and_canonical(&msg.head_block_hash)?;
        let block_header_lite = self.chain.get_block_header(&msg.block_hash)?.clone().into();
        let block_proof = self.chain.get_block_proof(&msg.block_hash, &msg.head_block_hash)?;
        Ok(GetBlockProofResponse { block_header_lite, proof: block_proof })
    }
}

impl Handler<GetProtocolConfig> for ViewClientActor {
    type Result = Result<ProtocolConfigView, GetProtocolConfigError>;

    #[perf]
    fn handle(&mut self, msg: GetProtocolConfig, _: &mut Self::Context) -> Self::Result {
        let block_header = match msg.0 {
            BlockReference::Finality(finality) => {
                let block_hash = self.get_block_hash_by_finality(&finality)?;
                self.chain.get_block_header(&block_hash).map(Clone::clone)
            }
            BlockReference::BlockId(BlockId::Height(height)) => {
                self.chain.get_header_by_height(height).map(Clone::clone)
            }
            BlockReference::BlockId(BlockId::Hash(hash)) => {
                self.chain.get_block_header(&hash).map(Clone::clone)
            }
            BlockReference::SyncCheckpoint(sync_checkpoint) => {
                if let Some(block_hash) =
                    self.get_block_hash_by_sync_checkpoint(&sync_checkpoint)?
                {
                    self.chain.get_block_header(&block_hash).map(Clone::clone)
                } else {
                    return Err(GetProtocolConfigError::UnknownBlock(format!(
                        "{:?}",
                        sync_checkpoint
                    )));
                }
            }
        }?;
        let config = self.runtime_adapter.get_protocol_config(block_header.epoch_id())?;
        Ok(config.into())
    }
}

impl Handler<NetworkViewClientMessages> for ViewClientActor {
    type Result = NetworkViewClientResponses;

    #[perf_with_debug]
    fn handle(&mut self, msg: NetworkViewClientMessages, _ctx: &mut Self::Context) -> Self::Result {
        match msg {
            #[cfg(feature = "adversarial")]
            NetworkViewClientMessages::Adversarial(adversarial_msg) => {
                return match adversarial_msg {
                    NetworkAdversarialMessage::AdvDisableDoomslug => {
                        info!(target: "adversary", "Turning Doomslug off");
                        self.adv.write().unwrap().adv_disable_doomslug = true;
                        self.chain.adv_disable_doomslug();
                        NetworkViewClientResponses::NoResponse
                    }
                    NetworkAdversarialMessage::AdvSetSyncInfo(height) => {
                        info!(target: "adversary", "Setting adversarial sync height: {}", height);
                        self.adv.write().unwrap().adv_sync_height = Some(height);
                        NetworkViewClientResponses::NoResponse
                    }
                    NetworkAdversarialMessage::AdvDisableHeaderSync => {
                        info!(target: "adversary", "Blocking header sync");
                        self.adv.write().unwrap().adv_disable_header_sync = true;
                        NetworkViewClientResponses::NoResponse
                    }
                    NetworkAdversarialMessage::AdvSwitchToHeight(height) => {
                        info!(target: "adversary", "Switching to height");
                        let mut chain_store_update = self.chain.mut_store().store_update();
                        chain_store_update.save_largest_target_height(height);
                        chain_store_update
                            .adv_save_latest_known(height)
                            .expect("adv method should not fail");
                        chain_store_update.commit().expect("adv method should not fail");
                        NetworkViewClientResponses::NoResponse
                    }
                    _ => panic!("invalid adversary message"),
                }
            }
            NetworkViewClientMessages::TxStatus { tx_hash, signer_account_id } => {
                if let Ok(Some(result)) = self.get_tx_status(tx_hash, signer_account_id, false) {
                    // TODO: remove this legacy support in #3204
                    let result = match result {
                        FinalExecutionOutcomeViewEnum::FinalExecutionOutcome(outcome) => outcome,
                        FinalExecutionOutcomeViewEnum::FinalExecutionOutcomeWithReceipt(
                            outcome,
                        ) => outcome.into(),
                    };
                    NetworkViewClientResponses::TxStatus(Box::new(result))
                } else {
                    NetworkViewClientResponses::NoResponse
                }
            }
            NetworkViewClientMessages::TxStatusResponse(tx_result) => {
                let tx_hash = tx_result.transaction_outcome.id;
                let mut request_manager = self.request_manager.write().expect(POISONED_LOCK_ERR);
                if request_manager.tx_status_requests.cache_remove(&tx_hash).is_some() {
                    request_manager.tx_status_response.cache_set(tx_hash, *tx_result);
                }
                NetworkViewClientResponses::NoResponse
            }
            NetworkViewClientMessages::ReceiptOutcomeRequest(receipt_id) => {
                if let Ok(outcome_with_proof) = self.chain.get_execution_outcome(&receipt_id) {
                    NetworkViewClientResponses::ReceiptOutcomeResponse(Box::new(
                        outcome_with_proof.clone(),
                    ))
                } else {
                    NetworkViewClientResponses::NoResponse
                }
            }
            NetworkViewClientMessages::ReceiptOutcomeResponse(_response) => {
                // TODO: remove rpc routing in (#3204)
                //                let have_request = {
                //                    let mut request_manager =
                //                        self.request_manager.write().expect(POISONED_LOCK_ERR);
                //                    request_manager.receipt_outcome_requests.cache_remove(response.id()).is_some()
                //                };
                //
                //                if have_request {
                //                    if let Ok(&shard_id) = self.chain.get_shard_id_for_receipt_id(response.id()) {
                //                        let block_hash = response.block_hash;
                //                        if let Ok(Some(&next_block_hash)) =
                //                            self.chain.get_next_block_hash_with_new_chunk(&block_hash, shard_id)
                //                        {
                //                            if let Ok(block) = self.chain.get_block(&next_block_hash) {
                //                                if shard_id < block.chunks().len() as u64 {
                //                                    if verify_path(
                //                                        block.chunks()[shard_id as usize].outcome_root(),
                //                                        &response.proof,
                //                                        &response.outcome_with_id.to_hashes(),
                //                                    ) {
                //                                        let mut chain_store_update =
                //                                            self.chain.mut_store().store_update();
                //                                        chain_store_update.save_outcome_with_proof(
                //                                            response.outcome_with_id.id,
                //                                            *response,
                //                                        );
                //                                        if let Err(e) = chain_store_update.commit() {
                //                                            error!(target: "view_client", "Error committing to chain store: {}", e);
                //                                        }
                //                                    }
                //                                }
                //                            }
                //                        }
                //                    }
                //                }
                NetworkViewClientResponses::NoResponse
            }
            NetworkViewClientMessages::BlockRequest(hash) => {
                if let Ok(block) = self.chain.get_block(&hash) {
                    NetworkViewClientResponses::Block(Box::new(block.clone()))
                } else {
                    NetworkViewClientResponses::NoResponse
                }
            }
            NetworkViewClientMessages::BlockHeadersRequest(hashes) => {
                #[cfg(feature = "adversarial")]
                {
                    if self.adv.read().unwrap().adv_disable_header_sync {
                        return NetworkViewClientResponses::NoResponse;
                    }
                }

                if let Ok(headers) = self.retrieve_headers(hashes) {
                    NetworkViewClientResponses::BlockHeaders(headers)
                } else {
                    NetworkViewClientResponses::NoResponse
                }
            }
            NetworkViewClientMessages::GetChainInfo => match self.chain.head() {
                Ok(head) => {
                    let height = self.get_height(&head);

                    NetworkViewClientResponses::ChainInfo {
                        genesis_id: GenesisId {
                            chain_id: self.config.chain_id.clone(),
                            hash: *self.chain.genesis().hash(),
                        },
                        height,
                        tracked_shards: self.config.tracked_shards.clone(),
                        archival: self.config.archive,
                    }
                }
                Err(err) => {
                    error!(target: "view_client", "Cannot retrieve chain head: {}", err);
                    NetworkViewClientResponses::NoResponse
                }
            },
            NetworkViewClientMessages::StateRequestHeader { shard_id, sync_hash } => {
                if !self.check_state_sync_request() {
                    return NetworkViewClientResponses::NoResponse;
                }

                let state_response = match self.chain.check_sync_hash_validity(&sync_hash) {
                    Ok(true) => {
                        let header = match self.chain.get_state_response_header(shard_id, sync_hash)
                        {
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
                        return NetworkViewClientResponses::NoResponse;
                    }
                    Err(e) => match e.kind() {
                        ErrorKind::DBNotFoundErr(_) => {
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
                        NetworkViewClientResponses::StateResponse(Box::new(info))
                    }
                    state_response @ ShardStateSyncResponse::V2(_) => {
                        let info = StateResponseInfo::V2(StateResponseInfoV2 {
                            shard_id,
                            sync_hash,
                            state_response,
                        });
                        NetworkViewClientResponses::StateResponse(Box::new(info))
                    }
                }
            }
            NetworkViewClientMessages::StateRequestPart { shard_id, sync_hash, part_id } => {
                if !self.check_state_sync_request() {
                    return NetworkViewClientResponses::NoResponse;
                }
                trace!(target: "sync", "Computing state request part {} {} {}", shard_id, sync_hash, part_id);
                let state_response = match self.chain.check_sync_hash_validity(&sync_hash) {
                    Ok(true) => {
                        let part = match self
                            .chain
                            .get_state_response_part(shard_id, part_id, sync_hash)
                        {
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
                        return NetworkViewClientResponses::NoResponse;
                    }
                    Err(e) => match e.kind() {
                        ErrorKind::DBNotFoundErr(_) => {
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
                let info = StateResponseInfo::V1(StateResponseInfoV1 {
                    shard_id,
                    sync_hash,
                    state_response,
                });
                NetworkViewClientResponses::StateResponse(Box::new(info))
            }
            NetworkViewClientMessages::AnnounceAccount(announce_accounts) => {
                let mut filtered_announce_accounts = Vec::new();

                for (announce_account, last_epoch) in announce_accounts {
                    // Keep the announcement if it is newer than the last announcement from
                    // the same account.
                    if let Some(last_epoch) = last_epoch {
                        match self
                            .runtime_adapter
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
                        Ok(false) => {
                            return NetworkViewClientResponses::Ban {
                                ban_reason: ReasonForBan::InvalidSignature,
                            };
                        }
                        // Filter this account
                        Err(e) => {
                            debug!(target: "view_client", "Failed to validate account announce signature: {}", e);
                        }
                    }
                }

                NetworkViewClientResponses::AnnounceAccount(filtered_announce_accounts)
            }
            NetworkViewClientMessages::EpochSyncRequest { epoch_id: _epoch_id } => {
                // TODO #3488
                NetworkViewClientResponses::NoResponse
            }
            NetworkViewClientMessages::EpochSyncFinalizationRequest { epoch_id: _epoch_id } => {
                // TODO #3488
                NetworkViewClientResponses::NoResponse
            }
        }
    }
}

impl Handler<GetGasPrice> for ViewClientActor {
    type Result = Result<GasPriceView, GetGasPriceError>;

    #[perf]
    fn handle(&mut self, msg: GetGasPrice, _ctx: &mut Self::Context) -> Self::Result {
        let header = self
            .maybe_block_id_to_block_hash(msg.block_id)
            .and_then(|block_hash| self.chain.get_block_header(&block_hash));
        Ok(GasPriceView { gas_price: header?.gas_price() })
    }
}

/// Starts the View Client in a new arbiter (thread).
pub fn start_view_client(
    validator_account_id: Option<AccountId>,
    chain_genesis: ChainGenesis,
    runtime_adapter: Arc<dyn RuntimeAdapter>,
    network_adapter: Arc<dyn NetworkAdapter>,
    config: ClientConfig,
    #[cfg(feature = "adversarial")] adv: Arc<RwLock<AdversarialControls>>,
) -> Addr<ViewClientActor> {
    let request_manager = Arc::new(RwLock::new(ViewClientRequestManager::new()));
    SyncArbiter::start(config.view_client_threads, move || {
        // ViewClientActor::start_in_arbiter(&Arbiter::current(), move |_ctx| {
        let validator_account_id1 = validator_account_id.clone();
        let runtime_adapter1 = runtime_adapter.clone();
        let network_adapter1 = network_adapter.clone();
        let config1 = config.clone();
        let request_manager1 = request_manager.clone();
        ViewClientActor::new(
            validator_account_id1,
            &chain_genesis,
            runtime_adapter1,
            network_adapter1,
            config1,
            request_manager1,
            #[cfg(feature = "adversarial")]
            adv.clone(),
        )
        .unwrap()
    })
}
