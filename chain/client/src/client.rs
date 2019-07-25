//! Client is responsible for tracking the chain and related pieces of infrastructure.
//! Block production is done in done in this actor as well (at the moment).

use std::collections::HashMap;
use std::ops::Sub;
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::{Duration, Instant};

use actix::{
    Actor, ActorFuture, AsyncContext, Context, ContextFutureSpawner, Handler, Recipient, WrapFuture,
};
use ansi_term::Color::{Cyan, Green, White, Yellow};
use chrono::{DateTime, Utc};
use log::{debug, error, info, warn};

use near_chain::{
    Block, BlockApproval, BlockHeader, BlockStatus, Chain, ErrorKind, Provenance, RuntimeAdapter,
    Tip, ValidTransaction,
};
use near_network::types::{PeerId, ReasonForBan};
use near_network::{
    NetworkClientMessages, NetworkClientResponses, NetworkRequests, NetworkResponses,
};
use near_pool::TransactionPool;
use near_primitives::crypto::signature::Signature;
use near_primitives::hash::CryptoHash;
use near_primitives::transaction::{ReceiptTransaction, SignedTransaction};
use near_primitives::types::{AccountId, BlockIndex, ShardId};
use near_primitives::unwrap_or_return;
use near_store::Store;

use crate::sync::{most_weight_peer, BlockSync, HeaderSync, StateSync};
use crate::types::{
    BlockProducer, ClientConfig, Error, NetworkInfo, ShardSyncStatus, Status, StatusSyncInfo,
    SyncStatus,
};
use crate::{sync, StatusResponse};

pub struct ClientActor {
    config: ClientConfig,
    sync_status: SyncStatus,
    chain: Chain,
    runtime_adapter: Arc<dyn RuntimeAdapter>,
    tx_pool: TransactionPool,
    network_actor: Recipient<NetworkRequests>,
    block_producer: Option<BlockProducer>,
    network_info: NetworkInfo,
    /// Set of approvals for the next block.
    approvals: HashMap<usize, Signature>,
    /// Timestamp when last block was received / processed. Used to timeout block production.
    last_block_processed: Instant,
    /// Keeps track of syncing headers.
    header_sync: HeaderSync,
    /// Keeps track of syncing block.
    block_sync: BlockSync,
    /// Keeps track of syncing state.
    state_sync: StateSync,
    /// Timestamp when client was started.
    started: Instant,
    /// Total number of blocks processed.
    num_blocks_processed: u64,
    /// Total number of transactions processed.
    num_tx_processed: u64,
}

fn wait_until_genesis(genesis_time: &DateTime<Utc>) {
    let now = Utc::now();
    //get chrono::Duration::num_seconds() by deducting genesis_time from now
    let chrono_seconds = genesis_time.signed_duration_since(now).num_seconds();
    //check if number of seconds in chrono::Duration larger than zero
    if chrono_seconds > 0 {
        info!(target: "chain", "Waiting until genesis: {}", chrono_seconds);
        let seconds = Duration::from_secs(chrono_seconds as u64);
        thread::sleep(seconds);
    }
}

impl ClientActor {
    pub fn new(
        config: ClientConfig,
        store: Arc<Store>,
        genesis_time: DateTime<Utc>,
        runtime_adapter: Arc<dyn RuntimeAdapter>,
        network_actor: Recipient<NetworkRequests>,
        block_producer: Option<BlockProducer>,
    ) -> Result<Self, Error> {
        wait_until_genesis(&genesis_time);
        let chain = Chain::new(store, runtime_adapter.clone(), genesis_time)?;
        let tx_pool = TransactionPool::new();
        let sync_status = SyncStatus::AwaitingPeers;
        let header_sync = HeaderSync::new(network_actor.clone());
        let block_sync = BlockSync::new(network_actor.clone(), config.block_fetch_horizon);
        let state_sync = StateSync::new(network_actor.clone(), config.state_fetch_horizon);
        if let Some(bp) = &block_producer {
            info!(target: "client", "Starting validator node: {}", bp.account_id);
        }
        Ok(ClientActor {
            config,
            sync_status,
            chain,
            runtime_adapter,
            tx_pool,
            network_actor,
            block_producer,
            network_info: NetworkInfo {
                num_active_peers: 0,
                peer_max_count: 0,
                most_weight_peers: vec![],
                received_bytes_per_sec: 0,
                sent_bytes_per_sec: 0,
            },
            approvals: HashMap::default(),
            last_block_processed: Instant::now(),
            header_sync,
            block_sync,
            state_sync,
            started: Instant::now(),
            num_blocks_processed: 0,
            num_tx_processed: 0,
        })
    }
}

impl Actor for ClientActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        // Start syncing job.
        self.start_sync(ctx);

        // Start fetching information from network.
        self.fetch_network_info(ctx);

        // Start periodic logging of current state of the client.
        self.log_summary(ctx);
    }
}

impl Handler<NetworkClientMessages> for ClientActor {
    type Result = NetworkClientResponses;

    fn handle(&mut self, msg: NetworkClientMessages, ctx: &mut Context<Self>) -> Self::Result {
        match msg {
            NetworkClientMessages::Transaction(tx) => match self.validate_tx(tx) {
                Ok(valid_transaction) => {
                    self.tx_pool.insert_transaction(valid_transaction);
                    NetworkClientResponses::ValidTx
                }
                Err(err) => NetworkClientResponses::InvalidTx(err),
            },
            NetworkClientMessages::BlockHeader(header, peer_id) => {
                self.receive_header(header, peer_id)
            }
            NetworkClientMessages::Block(block, peer_id, was_requested) => {
                self.receive_block(ctx, block, peer_id, was_requested)
            }
            NetworkClientMessages::BlockRequest(hash) => {
                if let Ok(block) = self.chain.get_block(&hash) {
                    NetworkClientResponses::Block(block.clone())
                } else {
                    NetworkClientResponses::NoResponse
                }
            }
            NetworkClientMessages::BlockHeadersRequest(hashes) => {
                if let Ok(headers) = self.retrieve_headers(hashes) {
                    NetworkClientResponses::BlockHeaders(headers)
                } else {
                    NetworkClientResponses::NoResponse
                }
            }
            NetworkClientMessages::GetChainInfo => match self.chain.head() {
                Ok(head) => NetworkClientResponses::ChainInfo {
                    genesis: self.chain.genesis().hash(),
                    height: head.height,
                    total_weight: head.total_weight,
                },
                Err(err) => {
                    error!(target: "client", "{}", err);
                    NetworkClientResponses::NoResponse
                }
            },
            NetworkClientMessages::BlockHeaders(headers, peer_id) => {
                if self.receive_headers(headers, peer_id) {
                    NetworkClientResponses::NoResponse
                } else {
                    warn!(target: "client", "Banning node for sending invalid block headers");
                    NetworkClientResponses::Ban { ban_reason: ReasonForBan::BadBlockHeader }
                }
            }
            NetworkClientMessages::BlockApproval(account_id, hash, signature) => {
                if self.collect_block_approval(&account_id, &hash, &signature) {
                    NetworkClientResponses::NoResponse
                } else {
                    warn!(target: "client", "Banning node for sending invalid block approval: {} {} {}", account_id, hash, signature);
                    NetworkClientResponses::Ban { ban_reason: ReasonForBan::BadBlockApproval }
                }
            }
            NetworkClientMessages::StateRequest(shard_id, hash) => {
                if let Ok((payload, receipts)) = self.state_request(shard_id, hash) {
                    return NetworkClientResponses::StateResponse {
                        shard_id,
                        hash,
                        payload,
                        receipts,
                    };
                }
                NetworkClientResponses::NoResponse
            }
            NetworkClientMessages::StateResponse(shard_id, hash, payload, receipts) => {
                if let SyncStatus::StateSync(sync_hash, sharded_statuses) = &mut self.sync_status {
                    if hash != *sync_hash {
                        sharded_statuses.insert(
                            shard_id,
                            ShardSyncStatus::Error(format!(
                                "Incorrect hash of the state response, expected: {}, got: {}",
                                sync_hash, hash
                            )),
                        );
                    } else {
                        match self.chain.set_shard_state(shard_id, hash, payload, receipts) {
                            Ok(()) => {
                                sharded_statuses.insert(shard_id, ShardSyncStatus::StateDone);
                            }
                            Err(err) => {
                                sharded_statuses.insert(
                                    shard_id,
                                    ShardSyncStatus::Error(format!(
                                        "Failed to set state for {} @ {}: {}",
                                        shard_id, hash, err
                                    )),
                                );
                            }
                        }
                    }
                }
                NetworkClientResponses::NoResponse
            }
        }
    }
}

impl Handler<Status> for ClientActor {
    type Result = Result<StatusResponse, String>;

    fn handle(&mut self, _: Status, _: &mut Context<Self>) -> Self::Result {
        let head = self.chain.head().map_err(|err| err.to_string())?;
        let prev_header =
            self.chain.get_block_header(&head.last_block_hash).map_err(|err| err.to_string())?;
        let latest_block_time = prev_header.timestamp.clone();
        let state_root =
            self.chain.get_post_state_root(&head.last_block_hash).map_err(|err| err.to_string())?;
        let validators = self
            .runtime_adapter
            .get_epoch_block_proposers(head.epoch_hash)
            .map_err(|err| err.to_string())?;
        Ok(StatusResponse {
            version: self.config.version.clone(),
            chain_id: self.config.chain_id.clone(),
            rpc_addr: self.config.rpc_addr.clone(),
            validators,
            sync_info: StatusSyncInfo {
                latest_block_hash: head.last_block_hash,
                latest_block_height: head.height,
                latest_state_root: state_root.clone(),
                latest_block_time,
                syncing: self.sync_status.is_syncing(),
            },
        })
    }
}

impl ClientActor {
    /// Gets called when block got accepted.
    /// Send updates over network, update tx pool and notify ourselves if it's time to produce next block.
    fn on_block_accepted(
        &mut self,
        ctx: &mut Context<ClientActor>,
        block_hash: CryptoHash,
        status: BlockStatus,
        provenance: Provenance,
    ) {
        let block = match self.chain.get_block(&block_hash) {
            Ok(block) => block.clone(),
            Err(err) => {
                error!(target: "client", "Failed to find block {} that was just accepted: {}", block_hash, err);
                return;
            }
        };

        // Update when last block was processed.
        self.last_block_processed = Instant::now();

        // Count blocks and transactions processed both in SYNC and regular modes.
        self.num_blocks_processed += 1;
        self.num_tx_processed += block.transactions.len() as u64;

        if provenance != Provenance::SYNC {
            // If we produced the block, then we want to broadcast it.
            // If received the block from another node then broadcast "header first" to minimise network traffic.
            if provenance == Provenance::PRODUCED {
                let _ = self.network_actor.do_send(NetworkRequests::Block { block: block.clone() });
            } else {
                let approval = self.get_block_approval(&block);
                let _ = self.network_actor.do_send(NetworkRequests::BlockHeaderAnnounce {
                    header: block.header.clone(),
                    approval,
                });
            }

            // If this is block producing node and next block is produced by us, schedule to produce a block after a delay.
            self.handle_scheduling_block_production(
                ctx,
                block.hash(),
                block.header.height,
                block.header.height,
            );
        }

        // Reconcile the txpool against the new block *after* we have broadcast it too our peers.
        // This may be slow and we do not want to delay block propagation.
        // We only want to reconcile the txpool against the new block *if* total weight has increased.
        if status == BlockStatus::Next || status == BlockStatus::Reorg {
            self.tx_pool.reconcile_block(&block);
        }
    }

    fn get_block_proposer(
        &self,
        epoch_hash: CryptoHash,
        height: BlockIndex,
    ) -> Result<AccountId, Error> {
        self.runtime_adapter
            .get_block_proposer(epoch_hash, height)
            .map_err(|err| Error::Other(err.to_string()))
    }

    fn get_epoch_block_proposers(&self, epoch_hash: CryptoHash) -> Result<Vec<AccountId>, Error> {
        self.runtime_adapter
            .get_epoch_block_proposers(epoch_hash)
            .map_err(|err| Error::Other(err.to_string()))
    }

    /// Create approval for given block or return none if not a block producer.
    fn get_block_approval(&mut self, block: &Block) -> Option<BlockApproval> {
        let (mut epoch_hash, offset) = self
            .runtime_adapter
            .get_epoch_offset(block.header.epoch_hash, block.header.height + 1)
            .ok()?;
        let next_block_producer_account =
            self.get_block_proposer(epoch_hash, block.header.height + 1);
        if let (Some(block_producer), Ok(next_block_producer_account)) =
            (&self.block_producer, &next_block_producer_account)
        {
            if &block_producer.account_id != next_block_producer_account {
                // TODO: fix this suboptimal code
                if offset == 0 {
                    epoch_hash = self
                        .runtime_adapter
                        .get_epoch_offset(block.header.prev_hash, block.header.height)
                        .ok()?
                        .0;
                }
                if let Ok(validators) = self.runtime_adapter.get_epoch_block_proposers(epoch_hash) {
                    if validators.contains(&block_producer.account_id) {
                        return Some(BlockApproval::new(
                            block.hash(),
                            &*block_producer.signer,
                            next_block_producer_account.clone(),
                        ));
                    }
                }
            }
        }
        None
    }

    /// Checks if we are block producer and if we are next block producer schedules calling `produce_block`.
    /// If we are not next block producer, schedule to check timeout.
    fn handle_scheduling_block_production(
        &mut self,
        ctx: &mut Context<ClientActor>,
        block_hash: CryptoHash,
        last_height: BlockIndex,
        check_height: BlockIndex,
    ) {
        let (epoch_hash, _) = unwrap_or_return!(
            self.runtime_adapter.get_epoch_offset(block_hash, check_height + 1),
            ()
        );
        let next_block_producer_account =
            unwrap_or_return!(self.get_block_proposer(epoch_hash, check_height + 1), ());
        if let Some(block_producer) = &self.block_producer {
            if block_producer.account_id.clone() == next_block_producer_account {
                ctx.run_later(self.config.min_block_production_delay, move |act, ctx| {
                    act.produce_block(ctx, block_hash, last_height, check_height + 1);
                });
            } else {
                // Otherwise, schedule timeout to check if the next block was produced.
                ctx.run_later(self.config.max_block_production_delay, move |act, ctx| {
                    act.check_block_timeout(ctx, last_height, check_height);
                });
            }
        }
    }

    /// Checks if next block was produced within timeout, if not check if we should produce next block.
    /// `last_height` is the height of the `head` at the point of scheduling,
    /// `check_height` is the height at which to call `handle_scheduling_block_production` to skip non received blocks.
    /// TODO: should we send approvals for `last_height` block to next block producer?
    fn check_block_timeout(
        &mut self,
        ctx: &mut Context<ClientActor>,
        last_height: BlockIndex,
        check_height: BlockIndex,
    ) {
        let head = unwrap_or_return!(self.chain.head(), ());
        // If height changed since we scheduled this, exit.
        if head.height != last_height {
            return;
        }
        debug!(target: "client", "Timeout for {}, current head {}, suggesting to skip", last_height, head.height);
        // Update how long ago last block arrived to reset block production timer.
        self.last_block_processed = Instant::now();
        self.handle_scheduling_block_production(
            ctx,
            head.last_block_hash,
            last_height,
            check_height + 1,
        );
    }

    /// Produce block if we are block producer for given block. If error happens, retry.
    fn produce_block(
        &mut self,
        ctx: &mut Context<ClientActor>,
        block_hash: CryptoHash,
        last_height: BlockIndex,
        next_height: BlockIndex,
    ) {
        if let Err(err) = self.produce_block_err(ctx, last_height, next_height) {
            error!(target: "client", "Block production failed: {:?}", err);
            self.handle_scheduling_block_production(ctx, block_hash, last_height, next_height - 1);
        }
    }

    /// Produce block if we are block producer for given `next_height` index.
    /// Can return error, should be called with `produce_block` to handle errors and reschedule.
    fn produce_block_err(
        &mut self,
        ctx: &mut Context<ClientActor>,
        last_height: BlockIndex,
        next_height: BlockIndex,
    ) -> Result<(), Error> {
        let block_producer = self.block_producer.as_ref().ok_or_else(|| {
            Error::BlockProducer("Called without block producer info.".to_string())
        })?;
        let head = self.chain.head()?;
        // If last height changed, this process should stop as we spun up another one.
        if head.height != last_height {
            return Ok(());
        }
        // Check that we are were called at the block that we are producer for.
        let next_block_proposer = self.get_block_proposer(head.epoch_hash, next_height)?;
        if block_producer.account_id != next_block_proposer {
            info!(target: "client", "Produce block: chain at {}, not block producer for next block.", next_height);
            return Ok(());
        }
        let state_root = self.chain.get_post_state_root(&head.last_block_hash)?.clone();
        let has_receipts =
            self.chain.get_receipts(&head.last_block_hash).map(|r| r.len() > 0).unwrap_or(false);

        // Wait until we have all approvals or timeouts per max block production delay.
        let validators = self
            .runtime_adapter
            .get_epoch_block_proposers(head.epoch_hash)
            .map_err(|err| Error::Other(err.to_string()))?;
        let total_validators = validators.len();
        let prev_same_bp = self
            .runtime_adapter
            .get_block_proposer(head.epoch_hash, last_height)
            .map_err(|err| Error::Other(err.to_string()))?
            == block_producer.account_id.clone();
        // If epoch changed, and before there was 2 validators and now there is 1 - prev_same_bp is false, but total validators right now is 1.
        let total_approvals =
            total_validators - if prev_same_bp || total_validators < 2 { 1 } else { 2 };
        if self.approvals.len() < total_approvals
            && self.last_block_processed.elapsed() < self.config.max_block_production_delay
        {
            // Schedule itself for (max BP delay - how much time passed).
            ctx.run_later(
                self.config.max_block_production_delay.sub(self.last_block_processed.elapsed()),
                move |act, ctx| {
                    act.produce_block(ctx, head.last_block_hash, last_height, next_height);
                },
            );
            return Ok(());
        }

        // If we are not producing empty blocks, skip this and call handle scheduling for the next block.
        // Also produce at least one block per epoch (produce a block even if empty if the last height was more than an epoch ago).
        if !self.config.produce_empty_blocks
            && self.tx_pool.len() == 0
            && !has_receipts
            && next_height - last_height < self.config.epoch_length
        {
            self.handle_scheduling_block_production(
                ctx,
                head.last_block_hash,
                head.height,
                next_height,
            );
            return Ok(());
        }

        let prev_header = self.chain.get_block_header(&head.last_block_hash)?;

        // Take transactions from the pool.
        let transactions = self.tx_pool.prepare_transactions(self.config.block_expected_weight)?;

        // At this point, the previous epoch hash must be available
        let (epoch_hash, _) = self
            .runtime_adapter
            .get_epoch_offset(head.last_block_hash, next_height)
            .expect("Epoch hash should exist at this point");

        let block = Block::produce(
            &prev_header,
            next_height,
            state_root,
            epoch_hash,
            transactions,
            self.approvals.drain().collect(),
            vec![],
            block_producer.signer.clone(),
        );

        self.process_block(ctx, block, Provenance::PRODUCED).map(|_| ()).map_err(|err| err.into())
    }

    /// Process block and execute callbacks.
    fn process_block(
        &mut self,
        ctx: &mut Context<ClientActor>,
        block: Block,
        provenance: Provenance,
    ) -> Result<Option<near_chain::Tip>, near_chain::Error> {
        // XXX: this is bad, there is no multithreading here, what is the better way to handle this callback?
        // TODO: replace to channels or cross beams here?
        let accepted_blocks = Arc::new(RwLock::new(vec![]));
        let result = {
            self.chain.process_block(block, provenance, |block, status, provenance| {
                accepted_blocks.write().unwrap().push((block.hash(), status, provenance));
            })
        };
        // Process all blocks that were accepted.
        for (hash, status, provenance) in accepted_blocks.write().unwrap().drain(..) {
            self.on_block_accepted(ctx, hash, status, provenance);
        }
        result
    }

    /// Processes received block, returns boolean if block was reasonable or malicious.
    fn receive_block(
        &mut self,
        ctx: &mut Context<ClientActor>,
        block: Block,
        peer_id: PeerId,
        was_requested: bool,
    ) -> NetworkClientResponses {
        let hash = block.hash();
        debug!(target: "client", "Received block {} at {} from {}", hash, block.header.height, peer_id);
        let prev_hash = block.header.prev_hash;
        let provenance =
            if was_requested { near_chain::Provenance::SYNC } else { near_chain::Provenance::NONE };
        match self.process_block(ctx, block, provenance) {
            Ok(_) => NetworkClientResponses::NoResponse,
            Err(ref err) if err.is_bad_data() => {
                NetworkClientResponses::Ban { ban_reason: ReasonForBan::BadBlock }
            }
            Err(ref err) if err.is_error() => {
                if self.sync_status.is_syncing() {
                    // While syncing, we may receive blocks that are older or from next epochs.
                    // This leads to Old Block or EpochOutOfBounds errors.
                    info!(target: "client", "Error on receival of block: {}", err);
                } else {
                    error!(target: "client", "Error on receival of block: {}", err);
                }
                NetworkClientResponses::NoResponse
            }
            Err(e) => match e.kind() {
                near_chain::ErrorKind::Orphan => {
                    if !self.chain.is_orphan(&prev_hash) && !self.sync_status.is_syncing() {
                        self.request_block_by_hash(prev_hash, peer_id)
                    }
                    NetworkClientResponses::NoResponse
                }
                _ => {
                    debug!("Process block: block {} refused by chain: {}", hash, e.kind());
                    NetworkClientResponses::NoResponse
                }
            },
        }
    }

    fn receive_header(&mut self, header: BlockHeader, peer_info: PeerId) -> NetworkClientResponses {
        let hash = header.hash();
        debug!(target: "client", "Received block header {} at {} from {}", hash, header.height, peer_info);

        // Process block by chain, if it's valid header ask for the block.
        let result = self.chain.process_block_header(&header);

        match result {
            Err(ref e) if e.is_bad_data() => {
                return NetworkClientResponses::Ban { ban_reason: ReasonForBan::BadBlockHeader }
            }
            // Some error that worth surfacing.
            Err(ref e) if e.is_error() => {
                error!(target: "client", "Error on receival of header: {}", e);
                return NetworkClientResponses::NoResponse;
            }
            // Got an error when trying to process the block header, but it's not due to
            // invalid data or underlying error. Surface as fine.
            Err(_) => return NetworkClientResponses::NoResponse,
            _ => {}
        }

        // Succesfully processed a block header and can request the full block.
        self.request_block_by_hash(header.hash(), peer_info);
        NetworkClientResponses::NoResponse
    }

    fn receive_headers(&mut self, headers: Vec<BlockHeader>, peer_id: PeerId) -> bool {
        info!(target: "client", "Received {} block headers from {}", headers.len(), peer_id);
        if headers.len() == 0 {
            return true;
        }
        match self.chain.sync_block_headers(headers) {
            Ok(_) => true,
            Err(err) => {
                if err.is_bad_data() {
                    error!(target: "client", "Error processing sync blocks: {}", err);
                    false
                } else {
                    debug!(target: "client", "Block headers refused by chain: {}", err);
                    true
                }
            }
        }
    }

    fn request_block_by_hash(&mut self, hash: CryptoHash, peer_id: PeerId) {
        match self.chain.block_exists(&hash) {
            Ok(false) => {
                // TODO: ?? should we add a wait for response here?
                let _ = self.network_actor.do_send(NetworkRequests::BlockRequest { hash, peer_id });
            }
            Ok(true) => {
                debug!(target: "client", "send_block_request_to_peer: block {} already known", hash)
            }
            Err(e) => {
                error!(target: "client", "send_block_request_to_peer: failed to check block exists: {:?}", e)
            }
        }
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
        for h in header.height + 1..=max_height {
            if let Ok(header) = self.chain.get_header_by_height(h) {
                headers.push(header.clone());
                if headers.len() >= sync::MAX_BLOCK_HEADERS as usize {
                    break;
                }
            }
        }
        Ok(headers)
    }

    /// Validate transaction and return transaction information relevant to ordering it in the mempool.
    fn validate_tx(&mut self, tx: SignedTransaction) -> Result<ValidTransaction, String> {
        let head = self.chain.head().map_err(|err| err.to_string())?;
        let state_root = self
            .chain
            .get_post_state_root(&head.last_block_hash)
            .map_err(|err| err.to_string())?
            .clone();
        self.runtime_adapter.validate_tx(0, state_root, tx)
    }

    /// Check whether need to (continue) sync.
    fn needs_syncing(&self) -> Result<(bool, u64), near_chain::Error> {
        let head = self.chain.head()?;
        let mut is_syncing = self.sync_status.is_syncing();

        let full_peer_info =
            if let Some(full_peer_info) = most_weight_peer(&self.network_info.most_weight_peers) {
                full_peer_info
            } else {
                if !self.config.skip_sync_wait {
                    warn!(target: "client", "Sync: no peers available, disabling sync");
                }
                return Ok((false, 0));
            };

        if is_syncing {
            if full_peer_info.chain_info.total_weight <= head.total_weight {
                info!(target: "client", "Sync: synced at {} @ {} [{}]", head.total_weight.to_num(), head.height, head.last_block_hash);
                is_syncing = false;
            }
        } else {
            if full_peer_info.chain_info.total_weight.to_num()
                > head.total_weight.to_num() + self.config.sync_weight_threshold
                && full_peer_info.chain_info.height
                    > head.height + self.config.sync_height_threshold
            {
                info!(
                    target: "client",
                    "Sync: height/weight: {}/{}, peer height/weight: {}/{}, enabling sync",
                    head.height,
                    head.total_weight,
                    full_peer_info.chain_info.height,
                    full_peer_info.chain_info.total_weight
                );
                is_syncing = true;
            }
        }
        Ok((is_syncing, full_peer_info.chain_info.height))
    }

    /// Starts syncing and then switches to either syncing or regular mode.
    fn start_sync(&mut self, ctx: &mut Context<ClientActor>) {
        // Wait for connections reach at least minimum peers unless skipping sync.
        if self.network_info.num_active_peers < self.config.min_num_peers
            && !self.config.skip_sync_wait
        {
            ctx.run_later(self.config.sync_step_period, move |act, ctx| {
                act.start_sync(ctx);
            });
            return;
        }
        // Start main sync loop.
        self.sync(ctx);
    }

    /// Main syncing job responsible for syncing client with other peers.
    fn sync(&mut self, ctx: &mut Context<ClientActor>) {
        // Macro to schedule to call this function later if error occurred.
        macro_rules! unwrap_or_run_later(($obj: expr) => (match $obj {
            Ok(v) => v,
            Err(err) => {
                error!(target: "sync", "Sync: Unexpected error: {}", err);
                ctx.run_later(self.config.sync_step_period, move |act, ctx| {
                    act.sync(ctx);
                });
                return;
            }
        }));

        let mut wait_period = self.config.sync_step_period;

        let currently_syncing = self.sync_status.is_syncing();
        let (needs_syncing, highest_height) = unwrap_or_run_later!(self.needs_syncing());

        if !needs_syncing {
            if currently_syncing {
                self.started = Instant::now();
                self.last_block_processed = Instant::now();
                self.sync_status = SyncStatus::NoSync;

                // Initial transition out of "syncing" state.
                // Start by handling scheduling block production if needed.
                let head = unwrap_or_run_later!(self.chain.head());
                self.handle_scheduling_block_production(
                    ctx,
                    head.last_block_hash,
                    head.height,
                    head.height,
                );
            }
            wait_period = self.config.sync_check_period;
        } else {
            // Run each step of syncing separately.
            unwrap_or_run_later!(self.header_sync.run(
                &mut self.sync_status,
                &mut self.chain,
                highest_height,
                &self.network_info.most_weight_peers
            ));
            // Only body / state sync if header height is latest.
            let header_head = unwrap_or_run_later!(self.chain.header_head());
            if header_head.height == highest_height {
                // Sync state if already running sync state or if block sync is too far.
                let sync_state = match self.sync_status {
                    SyncStatus::StateSync(_, _) => true,
                    _ => unwrap_or_run_later!(self.block_sync.run(
                        &mut self.sync_status,
                        &mut self.chain,
                        highest_height,
                        &self.network_info.most_weight_peers
                    )),
                };
                if sync_state {
                    unwrap_or_run_later!(self.state_sync.run(
                        &mut self.sync_status,
                        &mut self.chain,
                        highest_height,
                        &self.network_info.most_weight_peers,
                        // TODO: add tracking shards here.
                        vec![0],
                    ));
                }
            }
        }

        ctx.run_later(wait_period, move |act, ctx| {
            act.sync(ctx);
        });
    }

    /// Periodically fetch network info.
    fn fetch_network_info(&mut self, ctx: &mut Context<Self>) {
        // TODO: replace with push from network?
        self.network_actor
            .send(NetworkRequests::FetchInfo)
            .into_actor(self)
            .then(move |res, act, _ctx| match res {
                Ok(NetworkResponses::Info {
                    num_active_peers,
                    peer_max_count,
                    most_weight_peers,
                    sent_bytes_per_sec,
                    received_bytes_per_sec,
                }) => {
                    act.network_info.num_active_peers = num_active_peers;
                    act.network_info.peer_max_count = peer_max_count;
                    act.network_info.most_weight_peers = most_weight_peers;
                    act.network_info.sent_bytes_per_sec = sent_bytes_per_sec;
                    act.network_info.received_bytes_per_sec = received_bytes_per_sec;
                    actix::fut::ok(())
                }
                Ok(NetworkResponses::NoResponse) => actix::fut::ok(()),
                Err(e) => {
                    error!(target: "client", "Sync: recieved error or incorrect result: {}", e);
                    actix::fut::err(())
                }
            })
            .wait(ctx);

        ctx.run_later(self.config.fetch_info_period, move |act, ctx| {
            act.fetch_network_info(ctx);
        });
    }

    /// Periodically log summary.
    fn log_summary(&self, ctx: &mut Context<Self>) {
        ctx.run_later(self.config.log_summary_period, move |act, ctx| {
            // TODO: collect traffic, tx, blocks.
            let head = unwrap_or_return!(act.chain.head(), ());
            let validators = unwrap_or_return!(act.get_epoch_block_proposers(head.epoch_hash), ());
            let num_validators = validators.len();
            let is_validator = if let Some(block_producer) = &act.block_producer {
                validators.contains(&block_producer.account_id)
            } else {
                false
            };
            // Block#, Block Hash, is validator/# validators, active/max peers.
            let avg_bls = (act.num_blocks_processed as f64) / (act.started.elapsed().as_millis() as f64) * 1000.0;
            let avg_tps = (act.num_tx_processed as f64) / (act.started.elapsed().as_millis() as f64) * 1000.0;
            info!(target: "info", "{} {} {} {} {}",
                  Yellow.bold().paint(display_sync_status(&act.sync_status, &head)),
                  White.bold().paint(format!("{}/{}", if is_validator { "V" } else { "-" }, num_validators)),
                  Cyan.bold().paint(format!("{:2}/{:?}/{:2} peers", act.network_info.num_active_peers, act.network_info.most_weight_peers.len(), act.network_info.peer_max_count)),
                  Cyan.bold().paint(format!("⬇ {} ⬆ {}", pretty_bytes_per_sec(act.network_info.received_bytes_per_sec), pretty_bytes_per_sec(act.network_info.sent_bytes_per_sec))),
                  Green.bold().paint(format!("{:.2} bls {:.2} tps", avg_bls, avg_tps))
            );
            act.started = Instant::now();
            act.num_blocks_processed = 0;
            act.num_tx_processed = 0;

            act.log_summary(ctx);
        });
    }

    /// Collects block approvals. Returns false if block approval is invalid.
    fn collect_block_approval(
        &mut self,
        account_id: &AccountId,
        hash: &CryptoHash,
        signature: &Signature,
    ) -> bool {
        // TODO: figure out how to validate better before hitting the disk? For example validator and account cache to validate signature first.
        // TODO: This header is missing, should collect for later? should have better way to verify then.
        let header = unwrap_or_return!(self.chain.get_block_header(&hash), true).clone();

        // If given account is not current block proposer.
        let position = match self.get_epoch_block_proposers(header.epoch_hash) {
            Ok(validators) => validators.iter().position(|x| x == account_id),
            Err(err) => {
                error!(target: "client", "Block approval error: {}", err);
                return false;
            }
        };
        if position.is_none() {
            return false;
        }
        // Check signature is correct for given validator.
        if !self.runtime_adapter.check_validator_signature(
            &header.epoch_hash,
            account_id,
            hash.as_ref(),
            signature,
        ) {
            return false;
        }
        debug!(target: "client", "Received approval for {} from {}", hash, account_id);
        self.approvals.insert(position.unwrap(), signature.clone());
        true
    }

    fn state_request(
        &mut self,
        shard_id: ShardId,
        hash: CryptoHash,
    ) -> Result<(Vec<u8>, Vec<ReceiptTransaction>), near_chain::Error> {
        let header = self.chain.get_block_header(&hash)?;
        let prev_hash = header.prev_hash;
        let payload = self
            .runtime_adapter
            .dump_state(shard_id, header.prev_state_root)
            .map_err(|err| ErrorKind::Other(err.to_string()))?;
        let receipts = self.chain.get_receipts(&prev_hash)?.clone();
        Ok((payload, receipts))
    }
}

fn display_sync_status(sync_status: &SyncStatus, head: &Tip) -> String {
    match sync_status {
        SyncStatus::AwaitingPeers => format!("#{:>8} Waiting for peers", head.height),
        SyncStatus::NoSync => format!("#{:>8} {}", head.height, head.last_block_hash),
        SyncStatus::HeaderSync { current_height, highest_height } => {
            let percent =
                if *highest_height == 0 { 0 } else { current_height * 100 / highest_height };
            format!("#{:>8} Downloading headers {}%", head.height, percent)
        }
        SyncStatus::BodySync { current_height, highest_height } => {
            let percent =
                if *highest_height == 0 { 0 } else { current_height * 100 / highest_height };
            format!("#{:>8} Downloading blocks {}%", head.height, percent)
        }
        SyncStatus::StateSync(_sync_hash, shard_statuses) => {
            let mut res = String::from("State ");
            for (shard_id, shard_status) in shard_statuses {
                res = res
                    + format!(
                        "{}: {}",
                        shard_id,
                        match shard_status {
                            ShardSyncStatus::StateDownload {
                                start_time: _,
                                prev_update_time: _,
                                prev_downloaded_size: _,
                                downloaded_size: _,
                                total_size: _,
                            } => format!("download"),
                            ShardSyncStatus::StateValidation => format!("validation"),
                            ShardSyncStatus::StateDone => format!("done"),
                            ShardSyncStatus::Error(error) => format!("error {}", error),
                        }
                    )
                    .as_str();
            }
            res
        }
        SyncStatus::StateSyncDone => format!("State sync donee"),
    }
}

/// Format bytes per second in a nice way.
fn pretty_bytes_per_sec(num: u64) -> String {
    if num < 100 {
        // Under 0.1 kiB, display in bytes.
        format!("{} B/s", num)
    } else if num < 1024 * 1024 {
        // Under 1.0 MiB/sec display in kiB/sec.
        format!("{:.1}kiB/s", num as f64 / 1024.0)
    } else {
        format!("{:.1}MiB/s", num as f64 / (1024.0 * 1024.0))
    }
}
