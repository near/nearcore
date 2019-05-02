//! Client is responsible for tracking the chain and related pieces of infrastructure.
//! Block production is done in separate agent.

use std::sync::{Arc, RwLock};

use actix::{
    Actor, ActorFuture, AsyncContext, Context, ContextFutureSpawner, Handler, Recipient, WrapFuture,
};
use ansi_term::Color::{Cyan, Yellow};
use log::{debug, error, info, warn};

use near_chain::{
    Block, BlockApproval, BlockHeader, BlockStatus, Chain, Provenance, RuntimeAdapter,
    ValidTransaction,
};
use near_network::{
    NetworkClientMessages, NetworkClientResponses, NetworkRequests, NetworkResponses, PeerInfo,
};
use near_pool::TransactionPool;
use near_store::Store;
use primitives::crypto::signer::AccountSigner;
use primitives::hash::CryptoHash;
use primitives::transaction::SignedTransaction;
use primitives::types::AccountId;

pub use crate::types::{BlockProducer, ClientConfig, Error, GetBlock, NetworkInfo, SyncStatus};
use primitives::crypto::signature::Signature;
use std::collections::HashMap;
use std::time::Instant;

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
}

impl ClientActor {
    pub fn new(
        config: ClientConfig,
        store: Arc<Store>,
        runtime_adapter: Arc<RuntimeAdapter>,
        network_actor: Recipient<NetworkRequests>,
        block_producer: Option<BlockProducer>,
    ) -> Result<Self, Error> {
        // TODO: Wait until genesis.
        let chain = Chain::new(store, runtime_adapter.clone(), config.genesis_timestamp)?;
        let tx_pool = TransactionPool::new();
        let sync_status =
            if config.skip_sync_wait { SyncStatus::NoSync } else { SyncStatus::AwaitingPeers };
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
                max_weight_peer: None,
            },
            approvals: HashMap::default(),
            last_block_processed: Instant::now(),
        })
    }
}

impl Actor for ClientActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        // Start syncing job.
        self.sync(ctx);

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
            NetworkClientMessages::BlockHeader(header, peer_info) => {
                self.receive_header(header, peer_info)
            }
            NetworkClientMessages::Block(block, peer_info, was_requested) => {
                self.receive_block(ctx, block, peer_info, was_requested)
            }
            NetworkClientMessages::GetChainInfo => match self.chain.store().head() {
                Ok(head) => NetworkClientResponses::ChainInfo {
                    height: head.height,
                    total_weight: head.total_weight,
                },
                Err(err) => {
                    error!(target: "client", "{}", err);
                    NetworkClientResponses::NoResponse
                }
            },
            NetworkClientMessages::Blocks(blocks, peer_info) => {
                // TODO: sync sync sync
                NetworkClientResponses::NoResponse
            }
            NetworkClientMessages::BlockApproval(account_id, hash, signature) => {
                if self.collect_block_approval(&account_id, &hash, &signature) {
                    return NetworkClientResponses::NoResponse;
                } else {
                    warn!(target: "client", "Banning node for sending invalid block approval: {} {} {}", account_id, hash, signature);
                    NetworkClientResponses::Ban
                }
            }
        }
    }
}

impl Handler<GetBlock> for ClientActor {
    type Result = Option<Block>;

    fn handle(&mut self, msg: GetBlock, _: &mut Context<Self>) -> Self::Result {
        match msg {
            GetBlock::Best => {
                let head = self.chain.store().head().ok();
                if let Some(head) = head {
                    self.chain.store().get_block(&head.last_block_hash).ok()
                } else {
                    None
                }
            }
            GetBlock::Hash(hash) => self.chain.store().get_block(&hash).ok(),
        }
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
        let block = match self.chain.store().get_block(&block_hash) {
            Ok(block) => block,
            Err(e) => {
                error!(target: "client", "Failed to find block that was just accepted: {}", block_hash);
                return;
            }
        };

        // Update when last block was processed.
        self.last_block_processed = Instant::now();

        if provenance != Provenance::SYNC {
            let next_block_producer_account =
                self.runtime_adapter.get_block_proposer(block.header.height + 1);

            // If we produced the block, then we want to broadcast it.
            // If received the block from another node then broadcast "header first" to minimise network traffic.
            if provenance == Provenance::PRODUCED {
                let _ = self
                    .network_actor
                    .do_send(NetworkRequests::BlockAnnounce { block: block.clone() });
            } else {
                let mut approval = None;
                if let (Some(block_producer), Ok(next_block_producer_account)) =
                    (&self.block_producer, &next_block_producer_account)
                {
                    if self
                        .runtime_adapter
                        .get_epoch_block_proposers(block.header.height)
                        .contains(&block_producer.account_id)
                    {
                        approval = Some(BlockApproval::new(
                            block.hash(),
                            &*block_producer.signer,
                            next_block_producer_account.clone(),
                        ));
                    }
                }
                let _ = self.network_actor.do_send(NetworkRequests::BlockHeaderAnnounce {
                    header: block.header.clone(),
                    approval,
                });
            }

            // If this is block producing node and next block is produced by us, schedule to produce a block after a delay.
            if let Some(block_producer) = &self.block_producer {
                if Ok(block_producer.account_id.clone()) == next_block_producer_account {
                    ctx.run_later(self.config.min_block_production_delay, move |act, ctx| {
                        if let Err(err) = act.produce_block(ctx) {
                            error!(target: "client", "Produce block failed: {:?}", err);
                        }
                    });
                }
            }
        }

        // Reconcile the txpool against the new block *after* we have broadcast it too our peers.
        // This may be slow and we do not want to delay block propagation.
        // We only want to reconcile the txpool against the new block *if* total weight has increased.
        if status == BlockStatus::Next || status == BlockStatus::Reorg {
            self.tx_pool.reconcile_block(&block);
        }
    }

    /// Produce block if we are still next block producer.
    fn produce_block(&mut self, ctx: &mut Context<ClientActor>) -> Result<(), Error> {
        let block_producer = self.block_producer.as_ref().ok_or_else(|| {
            Error::BlockProducer("Called without block producer info.".to_string())
        })?;
        let head = self.chain.store().head()?;
        // Check that we are still at the block that we are producer for.
        if block_producer.account_id != self.runtime_adapter.get_block_proposer(head.height + 1)? {
            info!(target: "client", "Produce block: chain at {}, not block producer for next block.", head.height);
            return Ok(());
        }
        let prev = self.chain.store().get_block_header(&head.last_block_hash)?;

        // Wait until we have all approvals or timeouts per max block produciton delay.
        let total_authorities =
            self.runtime_adapter.get_epoch_block_proposers(head.height + 1).len();
        let prev_same_bp = self.runtime_adapter.get_block_proposer(head.height)?
            == block_producer.account_id.clone();
        let total_approvals = total_authorities - if prev_same_bp { 1 } else { 2 };
        if self.approvals.len() < total_approvals
            && self.last_block_processed.elapsed() < self.config.max_block_production_delay
        {
            return Ok(());
        }

        let state_root = self.chain.store().get_post_state_root(&head.last_block_hash)?;
        // Take transactions from the pool.
        let transactions = self.tx_pool.prepare_transactions(self.config.block_expected_weight)?;
        let block = Block::produce(
            &prev,
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
        peer_info: PeerInfo,
        was_requested: bool,
    ) -> NetworkClientResponses {
        let hash = block.hash();
        debug!(target: "client", "Received block {} at {} from {}", hash, block.header.height, peer_info);
        let previous = self.chain.get_previous_header(&block.header);
        let provenance =
            if was_requested { near_chain::Provenance::SYNC } else { near_chain::Provenance::NONE };
        match self.process_block(ctx, block, provenance) {
            Ok(_) => NetworkClientResponses::NoResponse,
            Err(ref e) if e.is_bad_data() => NetworkClientResponses::Ban,
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
                            self.request_block_by_hash(previous.hash(), peer_info)
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

    fn receive_header(
        &mut self,
        header: BlockHeader,
        peer_info: PeerInfo,
    ) -> NetworkClientResponses {
        let hash = header.hash();
        debug!(target: "client", "Received block header {} at {} from {}", hash, header.height, peer_info);

        // Process block by chain, if it's valid header ask for the block.
        let result = self.chain.process_block_header(&header);

        match result {
            Err(ref e) if e.is_bad_data() => return NetworkClientResponses::Ban,
            // Some error that worth surfacing.
            Err(ref e) if e.is_error() => {
                error!(target: "client", "Error on receival of header: {}", e);
                return NetworkClientResponses::NoResponse;
            }
            // Got an error when trying to process the block header, but it's not due to
            // invalid data or underlying error. Surface as fine.
            Err(e) => return NetworkClientResponses::NoResponse,
            _ => {}
        }

        // Succesfully processed a block header and can request the full block.
        self.request_block_by_hash(header.hash(), peer_info);
        NetworkClientResponses::NoResponse
    }

    fn request_block_by_hash(&mut self, hash: CryptoHash, peer_info: PeerInfo) {
        match self.chain.block_exists(&hash) {
            Ok(false) => {
                // TODO: ?? should we add a wait for response here?
                let _ =
                    self.network_actor.do_send(NetworkRequests::BlockRequest { hash, peer_info });
            }
            Ok(true) => debug!("send_block_request_to_peer: block {} already known", hash),
            Err(e) => error!("send_block_request_to_peer: failed to check block exists: {:?}", e),
        }
    }

    /// Validate transaction and return transaction information relevant to ordering it in the mempool.
    fn validate_tx(&self, tx: SignedTransaction) -> Option<ValidTransaction> {
        // TODO: add actual validation.
        Some(ValidTransaction { transaction: tx })
    }

    /// Check whether need to (continue) sync.
    fn needs_syncing(&self) -> Result<(bool, u64), near_chain::Error> {
        let head = self.chain.store().head()?;
        let mut is_syncing = self.sync_status.is_syncing();

        let full_peer_info = if let Some(full_peer_info) = &self.network_info.max_weight_peer {
            full_peer_info
        } else {
            warn!(target: "client", "Sync: no peers available, disabling sync");
            return Ok((false, 0));
        };

        if is_syncing {
            if full_peer_info.chain_info.total_weight <= head.total_weight {
                info!(target: "chain", "Sync: synced at {} @ {} [{}]", head.total_weight.to_num(), head.height, head.last_block_hash);
                is_syncing = false;
            }
        } else {
            if full_peer_info.chain_info.total_weight.to_num()
                > head.total_weight.to_num() + self.config.sync_weight_threshold
                && full_peer_info.chain_info.height
                    > head.height + self.config.sync_height_threshold
            {
                info!(
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

    /// Job responsible for syncing client with other peers.
    fn sync(&mut self, ctx: &mut Context<ClientActor>) {
        match self.sync_status {
            SyncStatus::NoSync => {
                // Stops syncing and switches to producing blocks.
                let _ = self.produce_block(ctx);
                return;
            }
            SyncStatus::AwaitingPeers => {
                // Check current number of peers and if enough move to next step.
                if self.network_info.num_active_peers >= self.config.min_num_peers {
                    self.sync_status = SyncStatus::NoSync;
                }
            }
            _ => {}
        }
        ctx.run_later(self.config.sync_period, move |act, ctx| {
            act.sync(ctx);
        });
    }

    /// Periodically fetch network info.
    fn fetch_network_info(&mut self, ctx: &mut Context<Self>) {
        // TODO: replace with push from network?
        self.network_actor
            .send(NetworkRequests::FetchInfo)
            .into_actor(self)
            .then(move |res, act, ctx| match res {
                Ok(NetworkResponses::Info {
                    num_active_peers,
                    peer_max_count,
                    max_weight_peer,
                }) => {
                    act.network_info.num_active_peers = num_active_peers;
                    act.network_info.peer_max_count = peer_max_count;
                    act.network_info.max_weight_peer = max_weight_peer;
                    actix::fut::ok(())
                }
                _ => {
                    error!(target: "client", "Sync: recieved error or incorrect result.");
                    actix::fut::err(())
                }
            })
            .wait(ctx);

        ctx.run_later(self.config.log_summary_period, move |act, ctx| {
            act.fetch_network_info(ctx);
        });
    }

    /// Periodically log summary.
    fn log_summary(&self, ctx: &mut Context<Self>) {
        ctx.run_later(self.config.log_summary_period, move |act, ctx| {
            // TODO: collect traffic, tx, blocks.
            let head = match act.chain.store().head() {
                Ok(head) => head,
                Err(_) => {return; }
            };
            info!(target: "client", "{} {} {}",
                  Yellow.bold().paint(format!("#{:>8}", head.height)),
                  Yellow.bold().paint(format!("{}", head.last_block_hash)),
                  Cyan.bold().paint(format!("{:2}/{:2} peers", act.network_info.num_active_peers, act.network_info.peer_max_count)));

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
        let header = match self.chain.store().get_block_header(&hash) {
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
