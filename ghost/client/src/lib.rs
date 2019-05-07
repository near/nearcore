//! Client is responsible for tracking the chain and related pieces of infrastructure.
//! Block production is done in separate agent.

use std::collections::HashMap;
use std::ops::Sub;
use std::sync::{Arc, RwLock};
use std::time::Instant;

use actix::{
    Actor, ActorFuture, AsyncContext, Context, ContextFutureSpawner, Handler, Recipient, WrapFuture,
};
use ansi_term::Color::{Cyan, White, Yellow, Green};
use log::{debug, error, info, warn};

use near_chain::{
    Block, BlockApproval, BlockHeader, BlockStatus, Chain, Provenance, RuntimeAdapter,
    ValidTransaction,
};
use near_network::types::{PeerId, ReasonForBan};
use near_network::{
    NetworkClientMessages, NetworkClientResponses, NetworkRequests, NetworkResponses,
};
use near_pool::TransactionPool;
use near_store::Store;
use near_primitives::crypto::signature::Signature;
use near_primitives::hash::CryptoHash;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{AccountId, BlockIndex};

use crate::sync::{most_weight_peer, BlockSync, HeaderSync};
pub use crate::types::{BlockProducer, ClientConfig, Error, GetBlock, Query, NetworkInfo, SyncStatus};
use chrono::{DateTime, Utc};
use near_primitives::rpc::ABCIQueryResponse;

mod sync;
pub mod test_utils;
mod types;

pub struct ClientActor {
    config: ClientConfig,
    sync_status: SyncStatus,
    chain: Chain,
    runtime_adapter: Arc<RuntimeAdapter>,
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
    block_sync: BlockSync,
    /// Timestamp when client was started.
    started: Instant,
    /// Total number of blocks processed.
    num_blocks_processed: u64,
    /// Total number of transactions processed.
    num_tx_processed: u64,
}

impl ClientActor {
    pub fn new(
        config: ClientConfig,
        store: Arc<Store>,
        genesis_time: DateTime<Utc>,
        runtime_adapter: Arc<RuntimeAdapter>,
        network_actor: Recipient<NetworkRequests>,
        block_producer: Option<BlockProducer>,
    ) -> Result<Self, Error> {
        // TODO: Wait until genesis.
        let chain = Chain::new(store, runtime_adapter.clone(), genesis_time)?;
        let tx_pool = TransactionPool::new();
        let sync_status = SyncStatus::AwaitingPeers;
        let header_sync = HeaderSync::new(network_actor.clone());
        let block_sync = BlockSync::new(network_actor.clone());
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
            },
            approvals: HashMap::default(),
            last_block_processed: Instant::now(),
            header_sync,
            block_sync,
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
                Some(valid_transaction) => {
                    self.tx_pool.insert_transaction(valid_transaction);
                    NetworkClientResponses::NoResponse
                }
                // TODO: should we ban for invalid tx?
                None => NetworkClientResponses::NoResponse,
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
        }
    }
}

/// Handles retrieving block from the chain.
impl Handler<GetBlock> for ClientActor {
    type Result = Option<Block>;

    fn handle(&mut self, msg: GetBlock, _: &mut Context<Self>) -> Self::Result {
        match msg {
            GetBlock::Best => {
                match self.chain.head() {
                    Ok(head) => self.chain.get_block(&head.last_block_hash).map(Clone::clone).ok(),
                    _ => None,
                }
            }
            GetBlock::Height(height) => self.chain.get_block_by_height(height).map(Clone::clone).ok(),
            GetBlock::Hash(hash) => self.chain.get_block(&hash).map(Clone::clone).ok(),
        }
    }
}

/// Handles runtime query.
impl Handler<Query> for ClientActor {
    type Result = Result<ABCIQueryResponse, String>;

    fn handle(&mut self, msg: Query, _: &mut Context<Self>) -> Self::Result {
        let head = self.chain.head().map_err(|err| err.to_string())?;
        let state_root = self.chain.get_post_state_root(&head.last_block_hash).map_err(|err| err.to_string())?;
        self.runtime_adapter.query(*state_root, head.height, &msg.path, &msg.data)
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

        if provenance != Provenance::SYNC {
            self.num_blocks_processed += 1;
            self.num_tx_processed += block.transactions.len() as u64;

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
            self.handle_scheduling_block_production(ctx, block.header.height);
        }

        // Reconcile the txpool against the new block *after* we have broadcast it too our peers.
        // This may be slow and we do not want to delay block propagation.
        // We only want to reconcile the txpool against the new block *if* total weight has increased.
        if status == BlockStatus::Next || status == BlockStatus::Reorg {
            self.tx_pool.reconcile_block(&block);
        }
    }

    /// Create approval for given block or return none if not a block producer.
    fn get_block_approval(&self, block: &Block) -> Option<BlockApproval> {
        let next_block_producer_account =
            self.runtime_adapter.get_block_proposer(block.header.height + 1);
        if let (Some(block_producer), Ok(next_block_producer_account)) =
            (&self.block_producer, &next_block_producer_account)
        {
            if &block_producer.account_id != next_block_producer_account {
                if self
                    .runtime_adapter
                    .get_epoch_block_proposers(block.header.height)
                    .contains(&block_producer.account_id)
                {
                    return Some(BlockApproval::new(
                        block.hash(),
                        &*block_producer.signer,
                        next_block_producer_account.clone(),
                    ));
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
        last_height: BlockIndex,
    ) {
        // TODO: check this block producer is at all involved in this epoch. If not, check back after some time.
        let next_block_producer_account = self.runtime_adapter.get_block_proposer(last_height + 1);
        if let Some(block_producer) = &self.block_producer {
            if Ok(block_producer.account_id.clone()) == next_block_producer_account {
                ctx.run_later(self.config.min_block_production_delay, move |act, ctx| {
                    if let Err(err) = act.produce_block(ctx, last_height + 1) {
                        error!(target: "client", "Block production failed: {:?}", err);
                    }
                });
            } else {
                // Otherwise, schedule timeout to check if the next block was produced.
                ctx.run_later(self.config.max_block_production_delay, move |act, ctx| {
                    act.check_block_timeout(ctx, last_height);
                });
            }
        }
    }

    /// Checks if next block was produced within timeout, if not check if we should produce next block.
    /// TODO: should we send approvals for given block to next block producer?
    fn check_block_timeout(&mut self, ctx: &mut Context<ClientActor>, last_height: u64) {
        let head = match self.chain.head() {
            Ok(head) => head,
            Err(_) => return,
        };
        // If height changed, exit.
        if head.height != last_height {
            return;
        }
        debug!("Timeout for {}, current head {}, suggesting to skip", last_height, head.height);
        // Update how long ago last block arrived to reset block production timer.
        self.last_block_processed = Instant::now();
        self.handle_scheduling_block_production(ctx, last_height + 1);
    }

    /// Produce block if we are block producer for given block.
    fn produce_block(
        &mut self,
        ctx: &mut Context<ClientActor>,
        next_height: BlockIndex,
    ) -> Result<(), Error> {
        let block_producer = self.block_producer.as_ref().ok_or_else(|| {
            Error::BlockProducer("Called without block producer info.".to_string())
        })?;
        let head = self.chain.head()?;
        // Check that we are still at the block that we are producer for.
        if block_producer.account_id != self.runtime_adapter.get_block_proposer(next_height)? {
            info!(target: "client", "Produce block: chain at {}, not block producer for next block.", next_height);
            return Ok(());
        }
        let state_root = self.chain.get_post_state_root(&head.last_block_hash)?.clone();
        let prev = self.chain.get_block_header(&head.last_block_hash)?;

        // Wait until we have all approvals or timeouts per max block produciton delay.
        let total_authorities = self.runtime_adapter.get_epoch_block_proposers(next_height).len();
        let prev_same_bp = self.runtime_adapter.get_block_proposer(next_height - 1)?
            == block_producer.account_id.clone();
        let total_approvals = total_authorities - if prev_same_bp { 1 } else { 2 };
        if self.approvals.len() < total_approvals
            && self.last_block_processed.elapsed() < self.config.max_block_production_delay
        {
            // Schedule itself for (max BP delay - how much time passed).
            ctx.run_later(
                self.config.max_block_production_delay.sub(self.last_block_processed.elapsed()),
                move |act, ctx| {
                    if let Err(err) = act.produce_block(ctx, next_height) {
                        error!(target: "client", "Block production failed: {:?}", err);
                    }
                },
            );
            return Ok(());
        }

        // Take transactions from the pool.
        let transactions = self.tx_pool.prepare_transactions(self.config.block_expected_weight)?;
        let block = Block::produce(
            &prev,
            next_height,
            state_root,
            transactions,
            self.approvals.drain().collect(),
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
        let previous = self.chain.get_previous_header(&block.header).map(Clone::clone);
        let provenance =
            if was_requested { near_chain::Provenance::SYNC } else { near_chain::Provenance::NONE };
        match self.process_block(ctx, block, provenance) {
            Ok(_) => NetworkClientResponses::NoResponse,
            Err(ref e) if e.is_bad_data() => {
                NetworkClientResponses::Ban { ban_reason: ReasonForBan::BadBlock }
            }
            Err(ref e) if e.is_error() => {
                error!(target: "client", "Error on receival of block: {}", e);
                NetworkClientResponses::NoResponse
            }
            Err(e) => match e.kind() {
                near_chain::ErrorKind::Orphan => {
                    if let Ok(previous) = previous {
                        if !self.chain.is_orphan(&previous.hash()) {
                            debug!(
                                "Process block: received an orphan block, checking the parent: {}",
                                previous.hash()
                            );
                            self.request_block_by_hash(previous.hash(), peer_id)
                        }
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
            return false;
        }
        match self.chain.sync_block_headers(headers) {
            Ok(_) => true,
            Err(err) => {
                if err.is_bad_data() {
                    debug!(target: "client", "Block headers refused by chain: {:?}", err);
                    false
                } else {
                    error!(target: "client", "Error processing sync blocks: {:?}", err);
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
            Ok(true) => debug!("send_block_request_to_peer: block {} already known", hash),
            Err(e) => error!("send_block_request_to_peer: failed to check block exists: {:?}", e),
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
        // TODO: this may be inefficient if there is a lot of skipped blocks.
        for h in header.height + 1..max_height {
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
    fn validate_tx(&self, tx: SignedTransaction) -> Option<ValidTransaction> {
        // TODO: add actual validation.
        Some(ValidTransaction { transaction: tx })
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
                self.sync_status = SyncStatus::NoSync;

                // Initial transition out of "syncing" state.
                // Start by handling scheduling block production if needed.
                let head = unwrap_or_run_later!(self.chain.head());
                self.handle_scheduling_block_production(ctx, head.height);
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
            unwrap_or_run_later!(self.block_sync.run(
                &mut self.sync_status,
                &mut self.chain,
                highest_height,
                &self.network_info.most_weight_peers
            ));
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
                }) => {
                    act.network_info.num_active_peers = num_active_peers;
                    act.network_info.peer_max_count = peer_max_count;
                    act.network_info.most_weight_peers = most_weight_peers;
                    actix::fut::ok(())
                }
                _ => {
                    error!(target: "client", "Sync: recieved error or incorrect result.");
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
            let head = match act.chain.head() {
                Ok(head) => head,
                Err(_) => {return; }
            };
            let authorities = act.runtime_adapter.get_epoch_block_proposers(head.height);
            let num_authorities = authorities.len();
            let is_authority = if let Some(block_producer) = &act.block_producer {
                authorities.contains(&block_producer.account_id)
            } else {
                false
            };
            // Block#, Block Hash, is authority/# authorities, active/max peers.
            let avg_bls = (act.num_blocks_processed as f64) / (act.started.elapsed().as_secs() as f64);
            let avg_tps = (act.num_tx_processed as f64) / (act.started.elapsed().as_secs() as f64);
            info!(target: "info", "{} {} {} {} {}",
                  Yellow.bold().paint(format!("#{:>8}", head.height)),
                  Yellow.bold().paint(format!("{}", head.last_block_hash)),
                  White.bold().paint(format!("{}/{}", if is_authority { "V" } else { "-" }, num_authorities)),
                  Cyan.bold().paint(format!("{:2}/{:2} peers", act.network_info.num_active_peers, act.network_info.peer_max_count)),
                  Green.bold().paint(format!("{:.2} bls {:.2} tps", avg_bls, avg_tps))
            );

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
        // TODO: figure out how to validate better before hitting the disk? For example authority and account cache to validate signature first.
        let header = match self.chain.get_block_header(&hash) {
            Ok(header) => header,
            Err(_) => return false,
        };
        // If given account is not current block proposer.
        let position = self
            .runtime_adapter
            .get_epoch_block_proposers(header.height)
            .iter()
            .position(|x| x == account_id);
        if position.is_none() {
            return false;
        }
        // Check signature is correct for given authority.
        if !self.runtime_adapter.validate_authority_signature(account_id, signature) {
            return false;
        }
        debug!(target: "client", "Received approval for {} from {}", hash, account_id);
        self.approvals.insert(position.unwrap(), signature.clone());
        true
    }
}
