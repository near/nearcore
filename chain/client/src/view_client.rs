//! Readonly view of the chain and state of the database.
//! Useful for querying from RPC.

use std::sync::Arc;

use actix::{Actor, Context, Handler};
use log::{error, warn};

use near_chain::{Chain, ChainGenesis, ChainStoreAccess, ErrorKind, RuntimeAdapter};
use near_primitives::types::AccountId;
use near_primitives::views::{
    BlockView, ChunkView, EpochValidatorInfo, FinalExecutionOutcomeView, FinalExecutionStatus,
    GasPriceView, LightClientBlockView, QueryResponse,
};
use near_store::Store;

use crate::types::{Error, GetBlock, GetGasPrice, Query, TxStatus};
use crate::{sync, ClientConfig, GetChunk, GetNextLightClientBlock, GetValidatorInfo};
use cached::{Cached, SizedCache};
use near_network::types::{
    AnnounceAccount, NetworkViewClientMessages, NetworkViewClientResponses, ReasonForBan,
    StateResponseInfo,
};
use near_network::{NetworkAdapter, NetworkRequests};
use near_primitives::block::{BlockHeader, GenesisId};
use near_primitives::hash::CryptoHash;
use near_primitives::merkle::verify_path;
use std::cmp::Ordering;
use std::hash::Hash;
use std::time::{Duration, Instant};

/// Max number of queries that we keep.
const QUERY_REQUEST_LIMIT: usize = 500;
/// Waiting time between requests, in ms
const REQUEST_WAIT_TIME: u64 = 1000;

/// View client provides currently committed (to the storage) view of the current chain and state.
pub struct ViewClientActor {
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
        store: Arc<Store>,
        chain_genesis: &ChainGenesis,
        runtime_adapter: Arc<dyn RuntimeAdapter>,
        network_adapter: Arc<dyn NetworkAdapter>,
        config: ClientConfig,
    ) -> Result<Self, Error> {
        // TODO: should we create shared ChainStore that is passed to both Client and ViewClient?
        let chain = Chain::new(store, runtime_adapter.clone(), chain_genesis)?;
        Ok(ViewClientActor {
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

    fn handle_query(&mut self, msg: Query) -> Result<Option<QueryResponse>, String> {
        if let Some(response) = self.query_responses.cache_remove(&msg.id) {
            self.query_requests.cache_remove(&msg.id);
            return response.map(Some);
        }
        let header = self.chain.head_header().map_err(|e| e.to_string())?.clone();
        let path_parts: Vec<&str> = msg.path.split('/').collect();
        if path_parts.len() <= 1 {
            return Err("Not enough query parameters provided".to_string());
        }
        let account_id = AccountId::from(path_parts[1].clone());
        let shard_id = self.runtime_adapter.account_id_to_shard_id(&account_id);

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
                        path_parts.clone(),
                        &msg.data,
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
                if Self::need_request(msg.id.clone(), &mut self.query_requests) {
                    let validator = self
                        .chain
                        .find_validator_for_forwarding(shard_id)
                        .map_err(|e| e.to_string())?;
                    self.network_adapter.do_send(NetworkRequests::Query {
                        account_id: validator,
                        path: msg.path.clone(),
                        data: msg.data.clone(),
                        id: msg.id.clone(),
                    });
                }

                Ok(None)
            }
        }
    }

    fn get_tx_status(
        &mut self,
        tx_hash: CryptoHash,
        signer_account_id: AccountId,
    ) -> Result<Option<FinalExecutionOutcomeView>, String> {
        if let Some(res) = self.tx_status_response.cache_remove(&tx_hash) {
            self.tx_status_requests.cache_remove(&tx_hash);
            return Ok(Some(res));
        }
        let has_tx_result = match self.chain.get_execution_outcome(&tx_hash) {
            Ok(_) => true,
            Err(e) => match e.kind() {
                ErrorKind::DBNotFoundErr(_) => false,
                _ => {
                    warn!(target: "client", "Error trying to get transaction result: {}", e.to_string());
                    false
                }
            },
        };
        let head = self.chain.head().map_err(|e| e.to_string())?.clone();
        if has_tx_result {
            let tx_result = self.chain.get_final_transaction_result(&tx_hash)?;
            match tx_result.status {
                FinalExecutionStatus::NotStarted | FinalExecutionStatus::Started => {
                    for receipt_view in tx_result.receipts_outcome.iter() {
                        let dst_shard_id = *self
                            .chain
                            .get_shard_id_for_receipt_id(&receipt_view.id)
                            .map_err(|e| e.to_string())?;
                        if self.chain.get_chunk_extra(&head.last_block_hash, dst_shard_id).is_err()
                        {
                            if Self::need_request(
                                receipt_view.id,
                                &mut self.receipt_outcome_requests,
                            ) {
                                let validator = self
                                    .chain
                                    .find_validator_for_forwarding(dst_shard_id)
                                    .map_err(|e| e.to_string())?;
                                self.network_adapter.do_send(
                                    NetworkRequests::ReceiptOutComeRequest(
                                        validator,
                                        receipt_view.id,
                                    ),
                                );
                            }
                        }
                    }
                }
                FinalExecutionStatus::SuccessValue(_) | FinalExecutionStatus::Failure(_) => {}
            }
            return Ok(Some(tx_result));
        }

        if Self::need_request(tx_hash, &mut self.tx_status_requests) {
            let target_shard_id = self.runtime_adapter.account_id_to_shard_id(&signer_account_id);
            let validator = self
                .chain
                .find_validator_for_forwarding(target_shard_id)
                .map_err(|e| e.to_string())?;
            self.network_adapter.do_send(NetworkRequests::TxStatus(
                validator,
                signer_account_id,
                tx_hash,
            ));
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
        match msg {
            GetBlock::Best => match self.chain.head() {
                Ok(head) => self.chain.get_block(&head.last_block_hash).map(Clone::clone),
                Err(err) => Err(err),
            },
            GetBlock::Height(height) => self.chain.get_block_by_height(height).map(Clone::clone),
            GetBlock::Hash(hash) => self.chain.get_block(&hash).map(Clone::clone),
        }
        .map(|block| block.into())
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
        .map(|chunk| chunk.into())
        .map_err(|err| err.to_string())
    }
}

impl Handler<TxStatus> for ViewClientActor {
    type Result = Result<Option<FinalExecutionOutcomeView>, String>;

    fn handle(&mut self, msg: TxStatus, _: &mut Context<Self>) -> Self::Result {
        self.get_tx_status(msg.tx_hash, msg.signer_account_id)
    }
}

impl Handler<GetValidatorInfo> for ViewClientActor {
    type Result = Result<EpochValidatorInfo, String>;

    fn handle(&mut self, msg: GetValidatorInfo, _: &mut Context<Self>) -> Self::Result {
        self.runtime_adapter.get_validator_info(&msg.last_block_hash).map_err(|e| e.to_string())
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
            NetworkViewClientMessages::Query { path, data, id } => {
                let query = Query { path, data, id: id.clone() };
                match self.handle_query(query) {
                    Ok(Some(r)) => {
                        NetworkViewClientResponses::QueryResponse { response: Ok(r), id }
                    }
                    Ok(None) => NetworkViewClientResponses::NoResponse,
                    Err(e) => NetworkViewClientResponses::QueryResponse { response: Err(e), id },
                }
            }
            NetworkViewClientMessages::QueryResponse { response, id } => {
                if self.query_requests.cache_get(&id).is_some() {
                    self.query_responses.cache_set(id, response);
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
                if let Ok(headers) = self.retrieve_headers(hashes) {
                    NetworkViewClientResponses::BlockHeaders(headers)
                } else {
                    NetworkViewClientResponses::NoResponse
                }
            }
            NetworkViewClientMessages::GetChainInfo => match self.chain.head() {
                Ok(head) => NetworkViewClientResponses::ChainInfo {
                    genesis_id: GenesisId {
                        chain_id: self.config.chain_id.clone(),
                        hash: self.chain.genesis().hash(),
                    },
                    height: head.height,
                    weight_and_score: head.weight_and_score,
                    tracked_shards: self.config.tracked_shards.clone(),
                },
                Err(err) => {
                    error!(target: "view_client", "{}", err);
                    NetworkViewClientResponses::NoResponse
                }
            },
            NetworkViewClientMessages::StateRequest { shard_id, sync_hash, need_header, parts } => {
                if let Ok(shard_state) = self.chain.get_state_response_by_request(
                    shard_id,
                    sync_hash,
                    need_header,
                    parts,
                ) {
                    NetworkViewClientResponses::StateResponse(StateResponseInfo {
                        shard_id,
                        sync_hash,
                        shard_state,
                    })
                } else {
                    NetworkViewClientResponses::NoResponse
                }
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
                            warn!(target: "view_client", "Failed to validate account announce signature: {}", e);
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
        let header = match msg {
            GetGasPrice::None => self.chain.head_header(),
            GetGasPrice::Height(height) => self.chain.get_header_by_height(height),
            GetGasPrice::Hash(block_hash) => self.chain.get_block_header(&block_hash),
        };
        header
            .map(|b| GasPriceView { gas_price: b.inner_rest.gas_price })
            .map_err(|e| e.to_string())
    }
}
