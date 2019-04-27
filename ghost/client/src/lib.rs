use std::sync::{Arc, RwLock};

use actix::{Actor, Addr, Arbiter, Context, Handler, Message, Recipient, System};
use kvdb::KeyValueDB;
use log::{debug, error};

use near_chain::{Block, BlockHeader, BlockStatus, Chain, ChainAdapter, RuntimeAdapter, Store};
use near_network::types::PeerInfo;
use near_network::NetworkRequests;
use near_pool::TransactionPool;
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
    chain: Arc<RwLock<Chain>>,
    network_actor: Recipient<NetworkRequests>,
}

impl ClientActor {
    pub fn new(
        chain: Arc<RwLock<Chain>>,
        network_actor: Recipient<NetworkRequests>,
    ) -> Result<Self, Error> {
        Ok(ClientActor { chain, network_actor })
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
    fn receive_block(
        &mut self,
        block: Block,
        peer_info: PeerInfo,
        was_requested: bool,
    ) -> Result<bool, Error> {
        let hash = block.hash();
        debug!(target: "client_actor", "Received block {} at {} from {:?}", hash, block.header.height, peer_info);

        let previous =
            self.chain.read().expect(POISONED_LOCK_ERR).get_previous_header(&block.header);
        let provenance =
            if was_requested { near_chain::Provenance::SYNC } else { near_chain::Provenance::NONE };
        let result = self.chain.write().expect(POISONED_LOCK_ERR).process_block(block, provenance);
        match result {
            Ok(_) => Ok(true),
            Err(ref e) if e.is_bad_data() => Ok(false),
            Err(e) => match e.kind() {
                near_chain::ErrorKind::Orphan => {
                    if let Ok(previous) = previous {
                        if !self.chain.read().expect(POISONED_LOCK_ERR).is_orphan(&previous.hash())
                        {
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
        let result = self.chain.read().expect(POISONED_LOCK_ERR).process_block_header(&header);

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
        match self.chain.read().expect(POISONED_LOCK_ERR).block_exists(&hash) {
            Ok(false) => {
                // TODO: ??
                self.network_actor.send(NetworkRequests::Block { hash, peer_info });
            }
            Ok(true) => debug!("send_block_request_to_peer: block {} already known", hash),
            Err(e) => error!("send_block_request_to_peer: failed to check block exists: {:?}", e),
        }
    }
}
