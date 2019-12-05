//! Readonly view of the chain and state of the database.
//! Useful for querying from RPC.

use std::sync::Arc;

use actix::{Actor, Context, Handler};
use log::warn;

use near_chain::{Chain, ChainGenesis, ChainStoreAccess, ErrorKind, RuntimeAdapter};
use near_primitives::types::AccountId;
use near_primitives::views::{
    BlockView, ChunkView, EpochValidatorInfo, FinalExecutionOutcomeView, LightClientBlockView,
    QueryResponse,
};
use near_store::Store;

use crate::types::{Error, GetBlock, Query, TxStatus};
use crate::{GetChunk, GetNextLightClientBlock, GetValidatorInfo};
use cached::{Cached, SizedCache};
use near_network::types::{NetworkViewClientMessages, NetworkViewClientResponses};
use near_network::{NetworkAdapter, NetworkRequests};
use near_primitives::hash::CryptoHash;

/// Max number of queries that we keep.
const QUERY_REQUEST_LIMIT: usize = 500;

/// View client provides currently committed (to the storage) view of the current chain and state.
pub struct ViewClientActor {
    chain: Chain,
    runtime_adapter: Arc<dyn RuntimeAdapter>,
    network_adapter: Arc<dyn NetworkAdapter>,
    /// Transaction query that needs to be forwarded to other shards
    pub tx_status_requests: SizedCache<CryptoHash, ()>,
    /// Transaction status response
    pub tx_status_response: SizedCache<CryptoHash, FinalExecutionOutcomeView>,
    /// Query requests that need to be forwarded to other shards
    pub query_requests: SizedCache<String, ()>,
    /// Query responses from other nodes (can be errors)
    pub query_responses: SizedCache<String, Result<QueryResponse, String>>,
}

impl ViewClientActor {
    pub fn new(
        store: Arc<Store>,
        chain_genesis: &ChainGenesis,
        runtime_adapter: Arc<dyn RuntimeAdapter>,
        network_adapter: Arc<dyn NetworkAdapter>,
    ) -> Result<Self, Error> {
        // TODO: should we create shared ChainStore that is passed to both Client and ViewClient?
        let chain = Chain::new(store, runtime_adapter.clone(), chain_genesis)?;
        Ok(ViewClientActor {
            chain,
            runtime_adapter,
            network_adapter,
            tx_status_requests: SizedCache::with_size(QUERY_REQUEST_LIMIT),
            tx_status_response: SizedCache::with_size(QUERY_REQUEST_LIMIT),
            query_requests: SizedCache::with_size(QUERY_REQUEST_LIMIT),
            query_responses: SizedCache::with_size(QUERY_REQUEST_LIMIT),
        })
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
                let validator = self
                    .chain
                    .find_validator_for_forwarding(shard_id)
                    .map_err(|e| e.to_string())?;
                self.query_requests.cache_set(msg.id.clone(), ());
                self.network_adapter.send(NetworkRequests::Query {
                    account_id: validator,
                    path: msg.path.clone(),
                    data: msg.data.clone(),
                    id: msg.id.clone(),
                });
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
        if has_tx_result {
            let tx_result = self.chain.get_final_transaction_result(&tx_hash)?;
            return Ok(Some(tx_result));
        }
        let target_shard_id = self.runtime_adapter.account_id_to_shard_id(&signer_account_id);
        let validator =
            self.chain.find_validator_for_forwarding(target_shard_id).map_err(|e| e.to_string())?;

        self.tx_status_requests.cache_set(tx_hash, ());
        self.network_adapter.send(NetworkRequests::TxStatus(validator, signer_account_id, tx_hash));
        Ok(None)
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
                    self.chain
                        .get_chunk(&block.chunks[shard_id as usize].chunk_hash())
                        .map(Clone::clone)
                })
            }
            GetChunk::BlockHeight(block_height, shard_id) => {
                self.chain.get_block_by_height(block_height).map(Clone::clone).and_then(|block| {
                    self.chain
                        .get_chunk(&block.chunks[shard_id as usize].chunk_hash())
                        .map(Clone::clone)
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
                let tx_hash = tx_result.transaction.id;
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
        }
    }
}
