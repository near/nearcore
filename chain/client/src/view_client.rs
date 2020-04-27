//! Readonly view of the chain and state of the database.
//! Useful for querying from RPC.

use std::cmp::Ordering;
use std::hash::Hash;
use std::sync::Arc;
use std::time::{Duration, Instant};

use actix::{Actor, Context, Handler};
use cached::{Cached, SizedCache};
use log::{debug, error, info, warn};

use near_chain::types::ShardStateSyncResponse;
use near_chain::{
    Chain, ChainGenesis, ChainStoreAccess, DoomslugThresholdMode, ErrorKind, RuntimeAdapter, Tip,
};
use near_chain_configs::ClientConfig;
#[cfg(feature = "adversarial")]
use near_network::types::NetworkAdversarialMessage;
use near_network::types::{
    NetworkViewClientMessages, NetworkViewClientResponses, ReasonForBan, StateResponseInfo,
};
use near_network::{NetworkAdapter, NetworkRequests};
use near_primitives::block::{BlockHeader, GenesisId};
use near_primitives::hash::CryptoHash;
use near_primitives::merkle::verify_path;
use near_primitives::network::AnnounceAccount;
use near_primitives::types::{
    AccountId, BlockHeight, BlockId, BlockIdOrFinality, Finality, MaybeBlockId,
};
use near_primitives::views::{
    BlockView, ChunkView, EpochValidatorInfo, FinalExecutionOutcomeView, FinalExecutionStatus,
    GasPriceView, LightClientBlockView, QueryRequest, QueryResponse, StateChangesKindsView,
    StateChangesView,
};

use crate::types::{Error, GetBlock, GetGasPrice, Query, TxStatus, TxStatusError};
use crate::{
    sync, GetChunk, GetNextLightClientBlock, GetStateChanges, GetStateChangesInBlock,
    GetValidatorInfo,
};

/// Max number of queries that we keep.
const QUERY_REQUEST_LIMIT: usize = 500;
/// Waiting time between requests, in ms
const REQUEST_WAIT_TIME: u64 = 1000;

/// View client provides currently committed (to the storage) view of the current chain and state.
pub struct ViewClientActor {
    #[cfg(feature = "adversarial")]
    pub adv_disable_header_sync: bool,
    #[cfg(feature = "adversarial")]
    pub adv_disable_doomslug: bool,
    #[cfg(feature = "adversarial")]
    pub adv_sync_height: Option<u64>,

    /// Validator account (if present).
    validator_account_id: Option<AccountId>,
    chain: Chain,
    runtime_adapter: Arc<dyn RuntimeAdapter>,
    network_adapter: Arc<dyn NetworkAdapter>,
    pub config: ClientConfig,
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

impl ViewClientActor {
    pub fn new(
        validator_account_id: Option<AccountId>,
        chain_genesis: &ChainGenesis,
        runtime_adapter: Arc<dyn RuntimeAdapter>,
        network_adapter: Arc<dyn NetworkAdapter>,
        config: ClientConfig,
    ) -> Result<Self, Error> {
        // TODO: should we create shared ChainStore that is passed to both Client and ViewClient?
        let chain =
            Chain::new(runtime_adapter.clone(), chain_genesis, DoomslugThresholdMode::TwoThirds)?;
        Ok(ViewClientActor {
            #[cfg(feature = "adversarial")]
            adv_disable_header_sync: false,
            #[cfg(feature = "adversarial")]
            adv_disable_doomslug: false,
            #[cfg(feature = "adversarial")]
            adv_sync_height: None,
            validator_account_id,
            chain,
            runtime_adapter,
            network_adapter,
            config,
            tx_status_requests: SizedCache::with_size(QUERY_REQUEST_LIMIT),
            tx_status_response: SizedCache::with_size(QUERY_REQUEST_LIMIT),
            query_requests: SizedCache::with_size(QUERY_REQUEST_LIMIT),
            query_responses: SizedCache::with_size(QUERY_REQUEST_LIMIT),
            receipt_outcome_requests: SizedCache::with_size(QUERY_REQUEST_LIMIT),
        })
    }

    fn maybe_block_id_to_block_hash(
        &mut self,
        block_id: MaybeBlockId,
    ) -> Result<CryptoHash, near_chain::Error> {
        match block_id {
            None => Ok(self.chain.head()?.last_block_hash),
            Some(BlockId::Height(height)) => Ok(self.chain.get_header_by_height(height)?.hash()),
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

    fn get_block_hash_by_finality(&mut self, finality: &Finality) -> Result<CryptoHash, Error> {
        let head_header = self.chain.head_header()?;
        match finality {
            Finality::None => Ok(head_header.hash),
            Finality::DoomSlug => Ok(head_header.inner_rest.last_ds_final_block),
            Finality::Final => Ok(head_header.inner_rest.last_final_block),
        }
    }

    fn handle_query(&mut self, msg: Query) -> Result<Option<QueryResponse>, String> {
        if let Some(response) = self.query_responses.cache_remove(&msg.query_id) {
            self.query_requests.cache_remove(&msg.query_id);
            return response.map(Some);
        }

        let header = match msg.block_id_or_finality {
            BlockIdOrFinality::BlockId(BlockId::Height(block_height)) => {
                self.chain.get_header_by_height(block_height)
            }
            BlockIdOrFinality::BlockId(BlockId::Hash(block_hash)) => {
                self.chain.get_block_header(&block_hash)
            }
            BlockIdOrFinality::Finality(ref finality) => {
                let block_hash =
                    self.get_block_hash_by_finality(&finality).map_err(|e| e.to_string())?;
                self.chain.get_block_header(&block_hash)
            }
        };
        let header = header.map_err(|e| e.to_string())?.clone();

        let account_id = match &msg.request {
            QueryRequest::ViewAccount { account_id, .. } => account_id,
            QueryRequest::ViewState { account_id, .. } => account_id,
            QueryRequest::ViewAccessKey { account_id, .. } => account_id,
            QueryRequest::ViewAccessKeyList { account_id, .. } => account_id,
            QueryRequest::CallFunction { account_id, .. } => account_id,
        };
        let shard_id = self.runtime_adapter.account_id_to_shard_id(account_id);

        // If we have state for the shard that we query return query result directly.
        // Otherwise route query to peers.
        match self.chain.get_chunk_extra(&header.hash, shard_id) {
            Ok(chunk_extra) => {
                let state_root = chunk_extra.state_root;
                self.runtime_adapter
                    .query(
                        &state_root,
                        header.inner_lite.height,
                        header.inner_lite.timestamp,
                        &header.hash,
                        &header.inner_lite.epoch_id,
                        &msg.request,
                    )
                    .map(Some)
                    .map_err(|e| e.to_string())
            }
            Err(e) => {
                match e.kind() {
                    ErrorKind::DBNotFoundErr(_) => {}
                    _ => {
                        warn!(target: "client", "Getting chunk extra failed: {}", e.to_string());
                    }
                }
                // route request
                if Self::need_request(msg.query_id.clone(), &mut self.query_requests) {
                    let validator = self
                        .chain
                        .find_validator_for_forwarding(shard_id)
                        .map_err(|e| e.to_string())?;
                    self.network_adapter.do_send(NetworkRequests::Query {
                        query_id: msg.query_id.clone(),
                        account_id: validator,
                        block_id_or_finality: msg.block_id_or_finality.clone(),
                        request: msg.request.clone(),
                    });
                }

                Ok(None)
            }
        }
    }

    fn request_receipt_outcome(
        &mut self,
        receipt_id: CryptoHash,
        last_block_hash: &CryptoHash,
    ) -> Result<(), TxStatusError> {
        let dst_shard_id = *self
            .chain
            .get_shard_id_for_receipt_id(&receipt_id)
            .map_err(|e| TxStatusError::ChainError(e))?;
        if self.chain.get_chunk_extra(last_block_hash, dst_shard_id).is_err() {
            if Self::need_request(receipt_id, &mut self.receipt_outcome_requests) {
                let validator = self
                    .chain
                    .find_validator_for_forwarding(dst_shard_id)
                    .map_err(|e| TxStatusError::ChainError(e))?;
                self.network_adapter
                    .do_send(NetworkRequests::ReceiptOutComeRequest(validator, receipt_id));
            }
        }
        Ok(())
    }

    fn get_tx_status(
        &mut self,
        tx_hash: CryptoHash,
        signer_account_id: AccountId,
    ) -> Result<Option<FinalExecutionOutcomeView>, TxStatusError> {
        if let Some(res) = self.tx_status_response.cache_remove(&tx_hash) {
            self.tx_status_requests.cache_remove(&tx_hash);
            return Ok(Some(res));
        }
        let head = self.chain.head().map_err(|e| TxStatusError::ChainError(e))?.clone();
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
                    match tx_result.status {
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
                    return Ok(Some(tx_result));
                }
                Err(e) => match e.kind() {
                    ErrorKind::DBNotFoundErr(_) => {
                        if let Ok(execution_outcome) =
                            self.chain.get_transaction_execution_result(&tx_hash)
                        {
                            for receipt_id in execution_outcome.outcome.receipt_ids {
                                self.request_receipt_outcome(receipt_id, &head.last_block_hash)?;
                            }
                        }
                        return Err(TxStatusError::MissingTransaction(tx_hash));
                    }
                    _ => {
                        warn!(target: "client", "Error trying to get transaction result: {}", e.to_string());
                        return Err(TxStatusError::ChainError(e));
                    }
                },
            }
        } else {
            if Self::need_request(tx_hash, &mut self.tx_status_requests) {
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
        for h in header.inner_lite.height + 1..=max_height {
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
            if let Some(height) = self.adv_sync_height {
                return height;
            }
        }

        head.height
    }
}

impl Actor for ViewClientActor {
    type Context = Context<Self>;
}

/// Handles runtime query.
impl Handler<Query> for ViewClientActor {
    type Result = Result<Option<QueryResponse>, String>;

    fn handle(&mut self, msg: Query, _: &mut Context<Self>) -> Self::Result {
        self.handle_query(msg)
    }
}

/// Handles retrieving block from the chain.
impl Handler<GetBlock> for ViewClientActor {
    type Result = Result<BlockView, String>;

    fn handle(&mut self, msg: GetBlock, _: &mut Context<Self>) -> Self::Result {
        match msg.0 {
            BlockIdOrFinality::Finality(finality) => {
                let block_hash =
                    self.get_block_hash_by_finality(&finality).map_err(|e| e.to_string())?;
                self.chain.get_block(&block_hash).map(Clone::clone)
            }
            BlockIdOrFinality::BlockId(BlockId::Height(height)) => {
                self.chain.get_block_by_height(height).map(Clone::clone)
            }
            BlockIdOrFinality::BlockId(BlockId::Hash(hash)) => {
                self.chain.get_block(&hash).map(Clone::clone)
            }
        }
        .and_then(|block| {
            self.runtime_adapter
                .get_block_producer(
                    &block.header.inner_lite.epoch_id,
                    block.header.inner_lite.height,
                )
                .map(|author| BlockView::from_author_block(author, block))
        })
        .map_err(|err| err.to_string())
    }
}

impl Handler<GetChunk> for ViewClientActor {
    type Result = Result<ChunkView, String>;

    fn handle(&mut self, msg: GetChunk, _: &mut Self::Context) -> Self::Result {
        match msg {
            GetChunk::ChunkHash(chunk_hash) => self.chain.get_chunk(&chunk_hash).map(Clone::clone),
            GetChunk::BlockHash(block_hash, shard_id) => {
                self.chain.get_block(&block_hash).map(Clone::clone).and_then(|block| {
                    let chunk_hash = block
                        .chunks
                        .get(shard_id as usize)
                        .ok_or_else(|| {
                            near_chain::Error::from(ErrorKind::InvalidShardId(shard_id))
                        })?
                        .chunk_hash();
                    self.chain.get_chunk(&chunk_hash).map(Clone::clone)
                })
            }
            GetChunk::Height(height, shard_id) => {
                self.chain.get_block_by_height(height).map(Clone::clone).and_then(|block| {
                    let chunk_hash = block
                        .chunks
                        .get(shard_id as usize)
                        .ok_or_else(|| {
                            near_chain::Error::from(ErrorKind::InvalidShardId(shard_id))
                        })?
                        .chunk_hash();
                    self.chain.get_chunk(&chunk_hash).map(Clone::clone)
                })
            }
        }
        .and_then(|chunk| {
            self.chain
                .get_block_by_height(chunk.header.height_included)
                .map(|block| (block.header.inner_lite.epoch_id.clone(), chunk))
        })
        .and_then(|(epoch_id, chunk)| {
            self.runtime_adapter
                .get_chunk_producer(
                    &epoch_id,
                    chunk.header.inner.height_created,
                    chunk.header.inner.shard_id,
                )
                .map(|author| ChunkView::from_author_chunk(author, chunk))
        })
        .map_err(|err| err.to_string())
    }
}

impl Handler<TxStatus> for ViewClientActor {
    type Result = Result<Option<FinalExecutionOutcomeView>, TxStatusError>;

    fn handle(&mut self, msg: TxStatus, _: &mut Context<Self>) -> Self::Result {
        self.get_tx_status(msg.tx_hash, msg.signer_account_id)
    }
}

impl Handler<GetValidatorInfo> for ViewClientActor {
    type Result = Result<EpochValidatorInfo, String>;

    fn handle(&mut self, msg: GetValidatorInfo, _: &mut Context<Self>) -> Self::Result {
        self.maybe_block_id_to_block_hash(msg.block_id)
            .and_then(|block_hash| self.runtime_adapter.get_validator_info(&block_hash))
            .map_err(|err| err.to_string())
    }
}

/// Returns a list of change kinds per account in a store for a given block.
impl Handler<GetStateChangesInBlock> for ViewClientActor {
    type Result = Result<StateChangesKindsView, String>;

    fn handle(&mut self, msg: GetStateChangesInBlock, _: &mut Context<Self>) -> Self::Result {
        self.chain
            .store()
            .get_state_changes_in_block(&msg.block_hash)
            .map(|state_changes_kinds| state_changes_kinds.into_iter().map(Into::into).collect())
            .map_err(|e| e.to_string())
    }
}

/// Returns a list of changes in a store for a given block filtering by the state changes request.
impl Handler<GetStateChanges> for ViewClientActor {
    type Result = Result<StateChangesView, String>;

    fn handle(&mut self, msg: GetStateChanges, _: &mut Context<Self>) -> Self::Result {
        self.chain
            .store()
            .get_state_changes(&msg.block_hash, &msg.state_changes_request.into())
            .map(|state_changes| state_changes.into_iter().map(Into::into).collect())
            .map_err(|e| e.to_string())
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
    type Result = Result<Option<LightClientBlockView>, String>;

    fn handle(&mut self, request: GetNextLightClientBlock, _: &mut Context<Self>) -> Self::Result {
        let last_block_header =
            self.chain.get_block_header(&request.last_block_hash).map_err(|err| err.to_string())?;
        let last_epoch_id = last_block_header.inner_lite.epoch_id.clone();
        let last_next_epoch_id = last_block_header.inner_lite.next_epoch_id.clone();
        let last_height = last_block_header.inner_lite.height;
        let head = self.chain.head().map_err(|err| err.to_string())?;

        if last_epoch_id == head.epoch_id || last_next_epoch_id == head.epoch_id {
            let head_header = self
                .chain
                .get_block_header(&head.last_block_hash)
                .map_err(|err| err.to_string())?;
            let ret = Chain::create_light_client_block(
                &head_header.clone(),
                &*self.runtime_adapter,
                self.chain.mut_store(),
            )
            .map_err(|err| err.to_string())?;

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
                        Err(e.to_string())
                    }
                }
            }
        }
    }
}

impl Handler<NetworkViewClientMessages> for ViewClientActor {
    type Result = NetworkViewClientResponses;

    fn handle(&mut self, msg: NetworkViewClientMessages, _ctx: &mut Context<Self>) -> Self::Result {
        match msg {
            #[cfg(feature = "adversarial")]
            NetworkViewClientMessages::Adversarial(adversarial_msg) => {
                return match adversarial_msg {
                    NetworkAdversarialMessage::AdvDisableDoomslug => {
                        info!(target: "adversary", "Turning Doomslug off");
                        self.adv_disable_doomslug = true;
                        self.chain.adv_disable_doomslug();
                        NetworkViewClientResponses::NoResponse
                    }
                    NetworkAdversarialMessage::AdvSetSyncInfo(height) => {
                        info!(target: "adversary", "Setting adversarial sync height: {}", height);
                        self.adv_sync_height = Some(height);
                        NetworkViewClientResponses::NoResponse
                    }
                    NetworkAdversarialMessage::AdvDisableHeaderSync => {
                        info!(target: "adversary", "Blocking header sync");
                        self.adv_disable_header_sync = true;
                        NetworkViewClientResponses::NoResponse
                    }
                    NetworkAdversarialMessage::AdvSwitchToHeight(height) => {
                        info!(target: "adversary", "Switching to height");
                        let mut chain_store_update = self.chain.mut_store().store_update();
                        chain_store_update.save_largest_target_height(&height);
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
                if let Ok(Some(result)) = self.get_tx_status(tx_hash, signer_account_id) {
                    NetworkViewClientResponses::TxStatus(result)
                } else {
                    NetworkViewClientResponses::NoResponse
                }
            }
            NetworkViewClientMessages::TxStatusResponse(tx_result) => {
                let tx_hash = tx_result.transaction_outcome.id;
                if self.tx_status_requests.cache_remove(&tx_hash).is_some() {
                    self.tx_status_response.cache_set(tx_hash, tx_result);
                }
                NetworkViewClientResponses::NoResponse
            }
            NetworkViewClientMessages::Query { query_id, block_id_or_finality, request } => {
                let query = Query { query_id: query_id.clone(), block_id_or_finality, request };
                match self.handle_query(query) {
                    Ok(Some(r)) => {
                        NetworkViewClientResponses::QueryResponse { query_id, response: Ok(r) }
                    }
                    Ok(None) => NetworkViewClientResponses::NoResponse,
                    Err(e) => {
                        NetworkViewClientResponses::QueryResponse { query_id, response: Err(e) }
                    }
                }
            }
            NetworkViewClientMessages::QueryResponse { query_id, response } => {
                if self.query_requests.cache_get(&query_id).is_some() {
                    self.query_responses.cache_set(query_id, response);
                }
                NetworkViewClientResponses::NoResponse
            }
            NetworkViewClientMessages::ReceiptOutcomeRequest(receipt_id) => {
                if let Ok(outcome_with_proof) = self.chain.get_execution_outcome(&receipt_id) {
                    NetworkViewClientResponses::ReceiptOutcomeResponse(outcome_with_proof.clone())
                } else {
                    NetworkViewClientResponses::NoResponse
                }
            }
            NetworkViewClientMessages::ReceiptOutcomeResponse(response) => {
                if self.receipt_outcome_requests.cache_remove(response.id()).is_some() {
                    if let Ok(&shard_id) = self.chain.get_shard_id_for_receipt_id(response.id()) {
                        let block_hash = response.block_hash;
                        if let Ok(Some(&next_block_hash)) =
                            self.chain.get_next_block_hash_with_new_chunk(&block_hash, shard_id)
                        {
                            if let Ok(block) = self.chain.get_block(&next_block_hash) {
                                if shard_id < block.chunks.len() as u64 {
                                    if verify_path(
                                        block.chunks[shard_id as usize].inner.outcome_root,
                                        &response.proof,
                                        &response.outcome_with_id.to_hashes(),
                                    ) {
                                        let mut chain_store_update =
                                            self.chain.mut_store().store_update();
                                        chain_store_update.save_outcome_with_proof(
                                            response.outcome_with_id.id,
                                            response,
                                        );
                                        if let Err(e) = chain_store_update.commit() {
                                            error!(target: "view_client", "Error committing to chain store: {}", e);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                NetworkViewClientResponses::NoResponse
            }
            NetworkViewClientMessages::BlockRequest(hash) => {
                if let Ok(block) = self.chain.get_block(&hash) {
                    NetworkViewClientResponses::Block(block.clone())
                } else {
                    NetworkViewClientResponses::NoResponse
                }
            }
            NetworkViewClientMessages::BlockHeadersRequest(hashes) => {
                #[cfg(feature = "adversarial")]
                {
                    if self.adv_disable_header_sync {
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
                            hash: self.chain.genesis().hash(),
                        },
                        height,
                        tracked_shards: self.config.tracked_shards.clone(),
                    }
                }
                Err(err) => {
                    error!(target: "view_client", "{}", err);
                    NetworkViewClientResponses::NoResponse
                }
            },
            NetworkViewClientMessages::StateRequestHeader { shard_id, sync_hash } => {
                let state_response = match self.chain.get_block(&sync_hash) {
                    Ok(_) => {
                        let header = match self.chain.get_state_response_header(shard_id, sync_hash)
                        {
                            Ok(header) => Some(header),
                            Err(e) => {
                                error!(target: "sync", "Cannot build sync header (get_state_response_header): {}", e);
                                None
                            }
                        };
                        ShardStateSyncResponse { header, part: None }
                    }
                    Err(_) => {
                        // This case may appear in case of latency in epoch switching.
                        // Request sender is ready to sync but we still didn't get the block.
                        info!(target: "sync", "Can't get sync_hash block {:?} for state request header", sync_hash);
                        ShardStateSyncResponse { header: None, part: None }
                    }
                };
                NetworkViewClientResponses::StateResponse(StateResponseInfo {
                    shard_id,
                    sync_hash,
                    state_response,
                })
            }
            NetworkViewClientMessages::StateRequestPart { shard_id, sync_hash, part_id } => {
                let state_response = match self.chain.get_block(&sync_hash) {
                    Ok(_) => {
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
                        ShardStateSyncResponse { header: None, part }
                    }
                    Err(_) => {
                        // This case may appear in case of latency in epoch switching.
                        // Request sender is ready to sync but we still didn't get the block.
                        info!(target: "sync", "Can't get sync_hash block {:?} for state request part", sync_hash);
                        ShardStateSyncResponse { header: None, part: None }
                    }
                };
                NetworkViewClientResponses::StateResponse(StateResponseInfo {
                    shard_id,
                    sync_hash,
                    state_response,
                })
            }
            NetworkViewClientMessages::AnnounceAccount(announce_accounts) => {
                let mut filtered_announce_accounts = Vec::new();

                for (announce_account, last_epoch) in announce_accounts.into_iter() {
                    if let Some(last_epoch) = last_epoch {
                        match self
                            .runtime_adapter
                            .compare_epoch_id(&announce_account.epoch_id, &last_epoch)
                        {
                            Ok(Ordering::Less) => {}
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
        }
    }
}

impl Handler<GetGasPrice> for ViewClientActor {
    type Result = Result<GasPriceView, String>;

    fn handle(&mut self, msg: GetGasPrice, _ctx: &mut Self::Context) -> Self::Result {
        let header = self
            .maybe_block_id_to_block_hash(msg.block_id)
            .and_then(|block_hash| self.chain.get_block_header(&block_hash));
        header
            .map(|b| GasPriceView { gas_price: b.inner_rest.gas_price })
            .map_err(|e| e.to_string())
    }
}
