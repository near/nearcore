//! Client is responsible for tracking the chain and related pieces of infrastructure.
//! Block production is done in separate agent.

use std::sync::{Arc, RwLock};
use std::time::Duration;

use ansi_term::Color::{White, Cyan};
use actix::{
    Actor, ActorFuture, Addr, Arbiter, AsyncContext, Context, ContextFutureSpawner, Handler,
    Message, Recipient, System, WrapFuture,
};
use chrono::{DateTime, Utc};
use log::{debug, error, info};

use near_chain::{
    Block, BlockHeader, BlockStatus, Chain, Provenance, RuntimeAdapter, ValidTransaction,
};
use near_network::types::PeerInfo;
use near_network::{NetworkRequests, NetworkResponses, NetworkConfig};
use near_pool::TransactionPool;
use near_store::Store;
use primitives::crypto::signer::{AccountSigner, EDSigner, InMemorySigner};
use primitives::hash::CryptoHash;
use primitives::transaction::SignedTransaction;
use primitives::types::{AccountId, BlockIndex};

#[derive(Debug)]
pub enum Error {
    Chain(near_chain::Error),
    Pool(near_pool::Error),
    BlockProducer(String),
}

impl From<near_chain::Error> for Error {
    fn from(e: near_chain::Error) -> Self {
        Error::Chain(e)
    }
}

impl From<near_pool::Error> for Error {
    fn from(e: near_pool::Error) -> Self {
        Error::Pool(e)
    }
}

pub struct ClientConfig {
    /// Genesis timestamp. Client will wait until this date to start.
    pub genesis_timestamp: DateTime<Utc>,
    /// Duration before producing block.
    pub block_production_delay: Duration,
    /// Expected block weight (num of tx, gas, etc).
    pub block_expected_weight: u32,
    /// Skip waiting for sync (for testing or single node testnet).
    pub skip_sync_wait: bool,
    /// Sync period.
    pub sync_period: Duration,
    /// Minimum number of peers to start syncing.
    pub min_num_peers: usize,
    /// Period between logging summary information.
    pub log_summary_period: Duration,
}

impl ClientConfig {
    pub fn test() -> Self {
        ClientConfig {
            genesis_timestamp: Utc::now(),
            block_production_delay: Duration::from_millis(100),
            block_expected_weight: 1000,
            skip_sync_wait: true,
            sync_period: Duration::from_millis(100),
            min_num_peers: 0,
            log_summary_period: Duration::from_secs(10),
        }
    }
}

impl Default for ClientConfig {
    fn default() -> Self {
        ClientConfig {
            genesis_timestamp: Utc::now(),
            block_production_delay: Duration::from_millis(100),
            block_expected_weight: 1000,
            skip_sync_wait: false,
            sync_period: Duration::from_millis(100),
            min_num_peers: 1,
            log_summary_period: Duration::from_secs(10),
        }
    }
}

#[derive(Debug)]
pub enum NetworkMessages {
    Transaction(SignedTransaction),
    BlockHeader(BlockHeader, PeerInfo),
    Block(Block, PeerInfo, bool),
}

impl Message for NetworkMessages {
    type Result = Result<bool, Error>;
}

/// Required information to produce blocks.
pub struct BlockProducer {
    pub account_id: AccountId,
    pub signer: Arc<EDSigner>,
}

impl From<Arc<InMemorySigner>> for BlockProducer {
    fn from(signer: Arc<InMemorySigner>) -> Self {
        BlockProducer { account_id: signer.account_id(), signer }
    }
}

/// Various status sync can be in, whether it's fast sync or archival.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SyncStatus {
    /// Not syncing / Done syncing.
    NoSync,
    /// Not enough peers to do anything yet.
    AwaitingPeers,
    /// Downloading block headers for fast sync.
    HeaderSync,
    /// Downloading state for fasy sync.
    StateDownload,
    /// Validating the full state.
    StateValidation,
    /// Finalizing state sync.
    StateDone,
    /// Catch up on blocks.
    BodySync,
}

pub struct NetworkInfo {
    num_active_peers: usize,
    peer_max_count: u32,
}

pub struct ClientActor {
    config: ClientConfig,
    sync_status: SyncStatus,
    chain: Chain,
    runtime_adapter: Arc<RuntimeAdapter>,
    tx_pool: TransactionPool,
    network_actor: Recipient<NetworkRequests>,
    block_producer: Option<BlockProducer>,
    network_info: NetworkInfo,
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
            network_info: NetworkInfo { num_active_peers: 0, peer_max_count: 0 }
        })
    }
}

impl Actor for ClientActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        match self.sync_status {
            SyncStatus::NoSync => {
                // If there is no syncing, start producing blocks.
                let _ = self.produce_block(ctx);
            }
            _ => self.sync(ctx),
        }

        self.fetch_network_info(ctx);

        self.log_summary(ctx);
    }
}

impl Handler<NetworkMessages> for ClientActor {
    type Result = Result<bool, Error>;

    fn handle(&mut self, msg: NetworkMessages, ctx: &mut Context<Self>) -> Self::Result {
        match msg {
            NetworkMessages::Transaction(tx) => match self.validate_tx(tx) {
                Some(valid_transaction) => {
                    self.tx_pool.insert_transaction(valid_transaction);
                    Ok(true)
                }
                None => Ok(false),
            },
            NetworkMessages::BlockHeader(header, peer_info) => {
                self.receive_header(header, peer_info)
            }
            NetworkMessages::Block(block, peer_info, was_requested) => {
                self.receive_block(ctx, block, peer_info, was_requested)
            }
            _ => Ok(false),
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

        if provenance != Provenance::SYNC {
            // If we produced the block, then we want to broadcast it.
            // If received the block from another node then broadcast "header first" to minimise network traffic.
            if provenance == Provenance::PRODUCED {
                let _ = self.network_actor.do_send(NetworkRequests::BlockAnnounce { block: block.clone() });
            } else {
                let _ = self.network_actor
                    .do_send(NetworkRequests::BlockHeaderAnnounce { header: block.header.clone() });
            }

            // If this is block producing node and next block is produced by us, schedule to produce a block after a delay.
            if let Some(block_producer) = &self.block_producer {
                if Some(block_producer.account_id.clone())
                    == self.runtime_adapter.get_block_proposer(block.header.height + 1)
                {
                    ctx.run_later(self.config.block_production_delay, move |act, ctx| {
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
        if Some(block_producer.account_id.clone())
            != self.runtime_adapter.get_block_proposer(head.height + 1)
        {
            info!(target: "client", "Produce block: chain at {}, not block producer for next block.", head.height);
            return Ok(());
        }
        let prev = self.chain.store().get_block_header(&head.last_block_hash)?;
        let state_root = self.chain.store().get_post_state_root(&head.last_block_hash)?;
        // Take transactions from the pool.
        let transactions = self.tx_pool.prepare_transactions(self.config.block_expected_weight)?;
        let block = Block::produce(&prev, state_root, transactions);
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
        let mut accepted_blocks = Arc::new(RwLock::new(vec![]));
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
    ) -> Result<bool, Error> {
        let hash = block.hash();
        debug!(target: "client", "Received block {} at {} from {:?}", hash, block.header.height, peer_info);
        let previous = self.chain.get_previous_header(&block.header);
        let provenance =
            if was_requested { near_chain::Provenance::SYNC } else { near_chain::Provenance::NONE };
        match self.process_block(ctx, block, provenance) {
            Ok(_) => Ok(true),
            Err(ref e) if e.is_bad_data() => Ok(false),
            Err(e) => match e.kind() {
                near_chain::ErrorKind::Orphan => {
                    if let Ok(previous) = previous {
                        if !self.chain.is_orphan(&previous.hash()) {
                            debug!(
                                "Process block: received an orphan block, checking the parent: {:}",
                                previous.hash()
                            );
                            self.request_block_by_hash(previous.hash(), peer_info)
                        }
                    }
                    Ok(true)
                }
                _ => {
                    debug!("Process block: block {} refused by chain: {}", hash, e.kind());
                    Ok(true)
                }
            },
        }
    }

    fn receive_header(&mut self, header: BlockHeader, peer_info: PeerInfo) -> Result<bool, Error> {
        let hash = header.hash();
        debug!(target: "client", "Received block header {} at {} from {:?}", hash, header.height, peer_info);

        // Process block by chain, if it's valid header ask for the block.
        let result = self.chain.process_block_header(&header);

        if let Err(e) = result {
            debug!(target: "client", "Block header {} refused by chain: {:?}", hash, e.kind());
            if e.is_bad_data() {
                return Ok(false);
            } else {
                // We got an error when trying to process the block header, but it's not due to
                // invalid data.
                return Err(e.into());
            }
        }

        // Succesfully processed a block header and can request the full block.
        self.request_block_by_hash(header.hash(), peer_info);
        Ok(true)
    }

    fn request_block_by_hash(&mut self, hash: CryptoHash, peer_info: PeerInfo) {
        match self.chain.block_exists(&hash) {
            Ok(false) => {
                // TODO: ?? should we add a wait for response here?
                self.network_actor.do_send(NetworkRequests::BlockRequest { hash, peer_info });
            }
            Ok(true) => debug!("send_block_request_to_peer: block {} already known", hash),
            Err(e) => error!("send_block_request_to_peer: failed to check block exists: {:?}", e),
        }
    }

    fn validate_tx(&self, tx: SignedTransaction) -> Option<ValidTransaction> {
        // TODO: add actual validation.
        Some(ValidTransaction { transaction: tx })
    }

    fn sync(&mut self, ctx: &mut Context<ClientActor>) {
        match self.sync_status {
            SyncStatus::NoSync => {
                return;
            }
            SyncStatus::AwaitingPeers => {
                // Check current number of peers and if enough move to next step.
                if self.network_info.num_active_peers >= self.config.min_num_peers {
                    self.sync_status = SyncStatus::HeaderSync;
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
        self.network_actor
            .send(NetworkRequests::FetchInfo)
            .into_actor(self)
            .then(move |res, act, ctx| match res {
                Ok(NetworkResponses::Info { num_active_peers, peer_max_count }) => {
                    act.network_info.num_active_peers = num_active_peers;
                    act.network_info.peer_max_count = peer_max_count;
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
                  White.bold().paint(format!("#{:>8}", head.height)),
                  White.bold().paint(format!("{}", head.last_block_hash)),
                  Cyan.bold().paint(format!("{:2}/{:2} peers", act.network_info.num_active_peers, act.network_info.peer_max_count)));

            act.log_summary(ctx);
        });
    }
}
