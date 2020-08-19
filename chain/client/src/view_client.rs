//! Readonly view of the chain and state of the database.
//! Useful for querying from RPC.

use std::cmp::Ordering;
use std::hash::Hash;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use actix::{Actor, Addr, Handler, SyncArbiter, SyncContext};
use cached::{Cached, SizedCache};
use log::{debug, error, info, trace, warn};

use near_chain::{
    get_epoch_block_producers_view, Chain, ChainGenesis, ChainStoreAccess, DoomslugThresholdMode,
    ErrorKind, RuntimeAdapter,
};
use near_chain_configs::ClientConfig;
#[cfg(feature = "adversarial")]
use near_network::types::NetworkAdversarialMessage;
use near_network::types::{
    NetworkViewClientMessages, NetworkViewClientResponses, ReasonForBan, StateResponseInfo,
};
use near_network::{NetworkAdapter, NetworkRequests};
use near_primitives::block::{BlockHeader, GenesisId, Tip};
use near_primitives::hash::CryptoHash;
use near_primitives::merkle::{merklize, verify_path, PartialMerkleTree};
use near_primitives::network::AnnounceAccount;
use near_primitives::syncing::ShardStateSyncResponse;
use near_primitives::types::{
    AccountId, BlockHeight, BlockId, BlockIdOrFinality, Finality, MaybeBlockId,
    TransactionOrReceiptId,
};
use near_primitives::views::{
    BlockView, ChunkView, EpochValidatorInfo, FinalExecutionOutcomeView, FinalExecutionStatus,
    GasPriceView, LightClientBlockView, QueryRequest, QueryResponse, StateChangesKindsView,
    StateChangesView, ValidatorStakeView,
};

use crate::types::{
    Error, GetBlock, GetBlockProof, GetBlockProofResponse, GetBlockWithMerkleTree,
    GetExecutionOutcome, GetGasPrice, Query, TxStatus, TxStatusError,
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
    request_manager: Arc<RwLock<ViewClientRequestManager>>,
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
    pub fn new(
        validator_account_id: Option<AccountId>,
        chain_genesis: &ChainGenesis,
        runtime_adapter: Arc<dyn RuntimeAdapter>,
        network_adapter: Arc<dyn NetworkAdapter>,
        config: ClientConfig,
        request_manager: Arc<RwLock<ViewClientRequestManager>>,
    ) -> Result<Self, Error> {
        // TODO: should we create shared ChainStore that is passed to both Client and ViewClient?
        let chain = Chain::new_for_view_client(
            runtime_adapter.clone(),
            chain_genesis,
            DoomslugThresholdMode::TwoThirds,
        )?;
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
            request_manager,
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

    fn get_block_hash_by_finality(&mut self, finality: &Finality) -> Result<CryptoHash, Error> {
        let head_header = self.chain.head_header()?;
        match finality {
            Finality::None => Ok(*head_header.hash()),
            Finality::DoomSlug => Ok(*head_header.last_ds_final_block()),
            Finality::Final => Ok(*head_header.last_final_block()),
        }
    }

    fn handle_query(&mut self, msg: Query) -> Result<Option<QueryResponse>, String> {
        {
            let mut request_manager = self.request_manager.write().expect(POISONED_LOCK_ERR);
            if let Some(response) = request_manager.query_responses.cache_remove(&msg.query_id) {
                request_manager.query_requests.cache_remove(&msg.query_id);
                return response.map(Some);
            }
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
        match self.chain.get_chunk_extra(header.hash(), shard_id) {
            Ok(chunk_extra) => {
                let state_root = chunk_extra.state_root;
                self.runtime_adapter
                    .query(
                        shard_id,
                        &state_root,
                        header.height(),
                        header.raw_timestamp(),
                        header.hash(),
                        header.epoch_id(),
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
                let mut request_manager = self.request_manager.write().expect(POISONED_LOCK_ERR);
                if Self::need_request(msg.query_id.clone(), &mut request_manager.query_requests) {
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
    ) -> Result<Option<FinalExecutionOutcomeView>, TxStatusError> {
        {
            let mut request_manager = self.request_manager.write().expect(POISONED_LOCK_ERR);
            if let Some(res) = request_manager.tx_status_response.cache_remove(&tx_hash) {
                request_manager.tx_status_requests.cache_remove(&tx_hash);
                return Ok(Some(res));
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
            if let Some(height) = self.adv_sync_height {
                return height;
            }
        }

        head.height
    }
}

impl Actor for ViewClientActor {
    type Context = SyncContext<Self>;
}

/// Handles runtime query.
impl Handler<Query> for ViewClientActor {
    type Result = Result<Option<QueryResponse>, String>;

    fn handle(&mut self, msg: Query, _: &mut Self::Context) -> Self::Result {
        self.handle_query(msg)
    }
}

/// Handles retrieving block from the chain.
impl Handler<GetBlock> for ViewClientActor {
    type Result = Result<BlockView, String>;

    fn handle(&mut self, msg: GetBlock, _: &mut Self::Context) -> Self::Result {
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
                .get_block_producer(&block.header().epoch_id(), block.header().height())
                .map(|author| BlockView::from_author_block(author, block))
        })
        .map_err(|err| err.to_string())
    }
}

impl Handler<GetBlockWithMerkleTree> for ViewClientActor {
    type Result = Result<(BlockView, PartialMerkleTree), String>;

    fn handle(&mut self, msg: GetBlockWithMerkleTree, ctx: &mut Self::Context) -> Self::Result {
        let block_view = self.handle(GetBlock(msg.0), ctx)?;
        self.chain
            .mut_store()
            .get_block_merkle_tree(&block_view.header.hash)
            .map(|merkle_tree| (block_view, merkle_tree.clone()))
            .map_err(|e| e.to_string())
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
                        .chunks()
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
                        .chunks()
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
            let epoch_id = self
                .runtime_adapter
                .get_epoch_id_from_prev_block(&chunk.header.inner.prev_block_hash)?;
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

    fn handle(&mut self, msg: TxStatus, _: &mut Self::Context) -> Self::Result {
        self.get_tx_status(msg.tx_hash, msg.signer_account_id)
    }
}

impl Handler<GetValidatorInfo> for ViewClientActor {
    type Result = Result<EpochValidatorInfo, String>;

    fn handle(&mut self, msg: GetValidatorInfo, _: &mut Self::Context) -> Self::Result {
        self.maybe_block_id_to_block_hash(msg.block_id)
            .and_then(|block_hash| self.runtime_adapter.get_validator_info(&block_hash))
            .map_err(|err| err.to_string())
    }
}

impl Handler<GetValidatorOrdered> for ViewClientActor {
    type Result = Result<Vec<ValidatorStakeView>, String>;

    fn handle(&mut self, msg: GetValidatorOrdered, _: &mut Self::Context) -> Self::Result {
        self.maybe_block_id_to_block_hash(msg.block_id)
            .and_then(|block_hash| self.chain.get_block_header(&block_hash).map(|h| h.clone()))
            .and_then(|header| {
                get_epoch_block_producers_view(
                    header.epoch_id(),
                    header.prev_hash(),
                    &*self.runtime_adapter,
                )
            })
            .map_err(|err| err.to_string())
    }
}
/// Returns a list of change kinds per account in a store for a given block.
impl Handler<GetStateChangesInBlock> for ViewClientActor {
    type Result = Result<StateChangesKindsView, String>;

    fn handle(&mut self, msg: GetStateChangesInBlock, _: &mut Self::Context) -> Self::Result {
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

    fn handle(&mut self, msg: GetStateChanges, _: &mut Self::Context) -> Self::Result {
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

    fn handle(&mut self, request: GetNextLightClientBlock, _: &mut Self::Context) -> Self::Result {
        let last_block_header =
            self.chain.get_block_header(&request.last_block_hash).map_err(|err| err.to_string())?;
        let last_epoch_id = last_block_header.epoch_id().clone();
        let last_next_epoch_id = last_block_header.next_epoch_id().clone();
        let last_height = last_block_header.height();
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

impl Handler<GetExecutionOutcome> for ViewClientActor {
    type Result = Result<GetExecutionOutcomeResponse, String>;

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
                    .get_next_block_hash_with_new_chunk(&outcome_proof.block_hash, target_shard_id)
                    .map_err(|e| e.to_string())?
                    .cloned();
                match next_block_hash {
                    Some(h) => {
                        outcome_proof.block_hash = h;
                        // Here we assume the number of shards is small so this reconstruction
                        // should be fast
                        let outcome_roots = self
                            .chain
                            .get_block(&h)
                            .map_err(|e| e.to_string())?
                            .chunks()
                            .iter()
                            .map(|header| header.inner.outcome_root)
                            .collect::<Vec<_>>();
                        if target_shard_id >= (outcome_roots.len() as u64) {
                            return Err(format!("Inconsistent state. Total number of shards is {} but the execution outcome is in shard {}", outcome_roots.len(), target_shard_id));
                        }
                        Ok(GetExecutionOutcomeResponse {
                            outcome_proof: outcome_proof.into(),
                            outcome_root_proof: merklize(&outcome_roots).1
                                [target_shard_id as usize]
                                .clone(),
                        })
                    }
                    None => Err(format!("{} has not been confirmed", id)),
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
                        Err(format!("{} does not exist", id))
                    } else {
                        Err(format!("Node doesn't track the shard where {} is executed", id))
                    }
                }
                _ => Err(e.to_string()),
            },
        }
    }
}

impl Handler<GetBlockProof> for ViewClientActor {
    type Result = Result<GetBlockProofResponse, String>;

    fn handle(&mut self, msg: GetBlockProof, _: &mut Self::Context) -> Self::Result {
        self.chain.check_block_final_and_canonical(&msg.block_hash).map_err(|e| e.to_string())?;
        self.chain
            .check_block_final_and_canonical(&msg.head_block_hash)
            .map_err(|e| e.to_string())?;
        let block_header_lite =
            self.chain.get_block_header(&msg.block_hash).map_err(|e| e.to_string())?.clone().into();
        let block_proof = self
            .chain
            .get_block_proof(&msg.block_hash, &msg.head_block_hash)
            .map_err(|e| e.to_string())?;
        Ok(GetBlockProofResponse { block_header_lite, proof: block_proof })
    }
}

impl Handler<NetworkViewClientMessages> for ViewClientActor {
    type Result = NetworkViewClientResponses;

    fn handle(&mut self, msg: NetworkViewClientMessages, _ctx: &mut Self::Context) -> Self::Result {
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
                if let Ok(Some(result)) = self.get_tx_status(tx_hash, signer_account_id) {
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
                let mut request_manager = self.request_manager.write().expect(POISONED_LOCK_ERR);
                if request_manager.query_requests.cache_get(&query_id).is_some() {
                    request_manager.query_responses.cache_set(query_id, response);
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
            NetworkViewClientMessages::ReceiptOutcomeResponse(response) => {
                let have_request = {
                    let mut request_manager =
                        self.request_manager.write().expect(POISONED_LOCK_ERR);
                    request_manager.receipt_outcome_requests.cache_remove(response.id()).is_some()
                };

                if have_request {
                    if let Ok(&shard_id) = self.chain.get_shard_id_for_receipt_id(response.id()) {
                        let block_hash = response.block_hash;
                        if let Ok(Some(&next_block_hash)) =
                            self.chain.get_next_block_hash_with_new_chunk(&block_hash, shard_id)
                        {
                            if let Ok(block) = self.chain.get_block(&next_block_hash) {
                                if shard_id < block.chunks().len() as u64 {
                                    if verify_path(
                                        block.chunks()[shard_id as usize].inner.outcome_root,
                                        &response.proof,
                                        &response.outcome_with_id.to_hashes(),
                                    ) {
                                        let mut chain_store_update =
                                            self.chain.mut_store().store_update();
                                        chain_store_update.save_outcome_with_proof(
                                            response.outcome_with_id.id,
                                            *response,
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
                    NetworkViewClientResponses::Block(Box::new(block.clone()))
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
                            hash: *self.chain.genesis().hash(),
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
                        ShardStateSyncResponse { header, part: None }
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
                            ShardStateSyncResponse { header: None, part: None }
                        }
                        _ => {
                            error!(target: "sync", "Failed to verify sync_hash {:?} validity, {:?}", sync_hash, e);
                            ShardStateSyncResponse { header: None, part: None }
                        }
                    },
                };
                NetworkViewClientResponses::StateResponse(Box::new(StateResponseInfo {
                    shard_id,
                    sync_hash,
                    state_response,
                }))
            }
            NetworkViewClientMessages::StateRequestPart { shard_id, sync_hash, part_id } => {
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
                        ShardStateSyncResponse { header: None, part }
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
                            ShardStateSyncResponse { header: None, part: None }
                        }
                        _ => {
                            error!(target: "sync", "Failed to verify sync_hash {:?} validity, {:?}", sync_hash, e);
                            ShardStateSyncResponse { header: None, part: None }
                        }
                    },
                };
                NetworkViewClientResponses::StateResponse(Box::new(StateResponseInfo {
                    shard_id,
                    sync_hash,
                    state_response,
                }))
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
        }
    }
}

impl Handler<GetGasPrice> for ViewClientActor {
    type Result = Result<GasPriceView, String>;

    fn handle(&mut self, msg: GetGasPrice, _ctx: &mut Self::Context) -> Self::Result {
        let header = self
            .maybe_block_id_to_block_hash(msg.block_id)
            .and_then(|block_hash| self.chain.get_block_header(&block_hash));
        header.map(|b| GasPriceView { gas_price: b.gas_price() }).map_err(|e| e.to_string())
    }
}

/// Starts the View Client in a new arbiter (thread).
pub fn start_view_client(
    validator_account_id: Option<AccountId>,
    chain_genesis: ChainGenesis,
    runtime_adapter: Arc<dyn RuntimeAdapter>,
    network_adapter: Arc<dyn NetworkAdapter>,
    config: ClientConfig,
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
        )
        .unwrap()
    })
}
