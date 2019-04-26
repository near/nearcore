use std::sync::Arc;

use log::debug;

use actix::{Actor, Addr, Arbiter, Context, Handler, Message, Recipient, System};
use kvdb::KeyValueDB;

use near_chain::{Block, BlockHeader, BlockStatus, Chain, ChainAdapter, RuntimeAdapter, Store};
use near_network::types::PeerInfo;
use near_network::NetworkRequests;

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
    Block(Block, PeerInfo),
}

impl Message for NetworkMessages {
    type Result = Result<bool, Error>;
}

pub struct ClientActor {
    chain: Chain,
    network_actor: Recipient<NetworkRequests>
}

impl ClientActor {
    pub fn new(chain: Chain, network_actor: Recipient<NetworkRequests>) -> Result<Self, Error> {
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
                self.request_block(&header, &peer_info);
                Ok(true)
            },
            _ => Ok(false)
        }
    }
}

impl ClientActor {
    fn request_block(&mut self, header: &BlockHeader, peer_info: &PeerInfo) {

    }
}