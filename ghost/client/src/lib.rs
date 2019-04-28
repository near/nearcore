//! Client is responsible for tracking the chain and related pieces of infrastructure.
//! Block production is done in separate agent.

use std::sync::{Arc, RwLock};

use actix::{Actor, Addr, Arbiter, Context, Handler, Message, Recipient, System};
use kvdb::KeyValueDB;
use log::{debug, error};

use near_chain::{Block, BlockHeader, BlockStatus, Chain, Provenance, RuntimeAdapter};
use near_network::types::PeerInfo;
use near_network::NetworkRequests;
use near_pool::TransactionPool;
use near_store::Store;
use primitives::hash::CryptoHash;

const POISONED_LOCK_ERR: &str = "The lock was poisoned.";
const MUST_BE_PRESENT: &str = "Must be present";

#[derive(Debug)]
pub enum Error {
    Chain(near_chain::Error),
}

impl From<near_chain::Error> for Error {
    fn from(e: near_chain::Error) -> Self {
        Error::Chain(e)
    }
}

pub enum NetworkMessages {
    BlockHeader(BlockHeader, PeerInfo),
    Block(Block, PeerInfo, bool),
}

impl Message for NetworkMessages {
    type Result = Result<bool, Error>;
}

pub struct ClientActor {
    chain: Chain,
    tx_pool: TransactionPool,
    network_actor: Recipient<NetworkRequests>,
}

impl ClientActor {
    pub fn new(
        store: Arc<Store>,
        runtime_adapter: Arc<RuntimeAdapter>,
        genesis: BlockHeader,
        network_actor: Recipient<NetworkRequests>,
    ) -> Result<Self, Error> {
        let chain = Chain::new(store, runtime_adapter, genesis);
        let tx_pool = TransactionPool::new();
        Ok(ClientActor { chain, tx_pool, network_actor })
    }
}

impl Actor for ClientActor {
    type Context = Context<Self>;
}

impl Handler<NetworkMessages> for ClientActor {
    type Result = Result<bool, Error>;

    fn handle(&mut self, msg: NetworkMessages, ctx: &mut Context<Self>) -> Self::Result {
        match msg {
            NetworkMessages::BlockHeader(header, peer_info) => {
                self.receive_header(header, peer_info)
            }
            NetworkMessages::Block(block, peer_info, was_requested) => {
                self.receive_block(block, peer_info, was_requested)
            }
            _ => Ok(false),
        }
    }
}

impl ClientActor {
    pub fn on_block_accepted(
        &self,
        block: &Block,
        block_status: BlockStatus,
        provenance: Provenance,
    ) {

    }
    fn receive_block(
        &mut self,
        block: Block,
        peer_info: PeerInfo,
        was_requested: bool,
    ) -> Result<bool, Error> {
        let hash = block.hash();
        debug!(target: "client_actor", "Received block {} at {} from {:?}", hash, block.header.height, peer_info);

        let previous = self.chain.get_previous_header(&block.header);
        let provenance =
            if was_requested { near_chain::Provenance::SYNC } else { near_chain::Provenance::NONE };

        let mut tx_pool = &mut self.tx_pool;
        let result = {
            self.chain.process_block(block, provenance, |block, status, provenance| {
                if provenance != Provenance::SYNC {
                    // If we produced the block, then we want to broadcast it.
                    // If received the block from another node then broadcast "header first" to minimise network traffic.
                    if provenance == Provenance::PRODUCED {
                        // self.peers().broadcast_block(&block);
                    } else {
                        // self.peers().broadcast_header(&block);
                    }
                }

                // Reconcile the txpool against the new block *after* we have broadcast it too our peers.
                // This may be slow and we do not want to delay block propagation.
                // We only want to reconcile the txpool against the new block *if* total weight has increased.
                if status == BlockStatus::Next || status == BlockStatus::Reorg {
                    tx_pool.reconcile_block(block);
                }
            })
        };
        match result {
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
        debug!(target: "client_actor", "Received block header {} at {} from {:?}", hash, header.height, peer_info);

        // Process block by chain, if it's valid header ask for the block.
        let result = self.chain.process_block_header(&header);

        if let Err(e) = result {
            debug!(target: "client_actor", "Block header {} refused by chain: {:?}", hash, e.kind());
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
                // TODO: ??
                self.network_actor.send(NetworkRequests::Block { hash, peer_info });
            }
            Ok(true) => debug!("send_block_request_to_peer: block {} already known", hash),
            Err(e) => error!("send_block_request_to_peer: failed to check block exists: {:?}", e),
        }
    }
}
