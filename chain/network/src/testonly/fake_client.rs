use crate::sink::Sink;
use crate::types::{NetworkClientMessages, NetworkClientResponses};
use actix::Actor as _;
use near_network_primitives::types::{NetworkViewClientMessages, NetworkViewClientResponses};
use near_primitives::block::{Block, BlockHeader};
use near_primitives::challenge::Challenge;
use near_primitives::hash::CryptoHash;
use near_primitives::network::AnnounceAccount;
use near_primitives::sharding::{ChunkHash, PartialEncodedChunkPart};
use near_primitives::syncing::EpochSyncResponse;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::EpochId;

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Event {
    BlockRequest(CryptoHash),
    Block(Block),
    BlockHeadersRequest(Vec<CryptoHash>),
    BlockHeaders(Vec<BlockHeader>),
    Chunk(Vec<PartialEncodedChunkPart>),
    ChunkRequest(ChunkHash),
    Transaction(SignedTransaction),
    Challenge(Challenge),
    EpochSyncRequest(EpochId),
    EpochSyncResponse(EpochSyncResponse),
    EpochSyncFinalizationRequest(EpochId),
    AnnounceAccount(Vec<(AnnounceAccount, Option<EpochId>)>),
}

pub struct Actor {
    event_sink: Sink<Event>,
}

impl actix::Actor for Actor {
    type Context = actix::Context<Self>;
}

pub fn start(event_sink: Sink<Event>) -> actix::Addr<Actor> {
    Actor { event_sink }.start()
}

impl actix::Handler<NetworkViewClientMessages> for Actor {
    type Result = NetworkViewClientResponses;
    fn handle(&mut self, msg: NetworkViewClientMessages, _ctx: &mut Self::Context) -> Self::Result {
        match msg {
            NetworkViewClientMessages::BlockRequest(block_hash) => {
                self.event_sink.push(Event::BlockRequest(block_hash));
                NetworkViewClientResponses::NoResponse
            }
            NetworkViewClientMessages::BlockHeadersRequest(req) => {
                self.event_sink.push(Event::BlockHeadersRequest(req));
                NetworkViewClientResponses::NoResponse
            }
            NetworkViewClientMessages::EpochSyncRequest { epoch_id } => {
                self.event_sink.push(Event::EpochSyncRequest(epoch_id));
                NetworkViewClientResponses::NoResponse
            }
            NetworkViewClientMessages::EpochSyncFinalizationRequest { epoch_id } => {
                self.event_sink.push(Event::EpochSyncFinalizationRequest(epoch_id));
                NetworkViewClientResponses::NoResponse
            }
            NetworkViewClientMessages::AnnounceAccount(aas) => {
                self.event_sink.push(Event::AnnounceAccount(aas.clone()));
                NetworkViewClientResponses::AnnounceAccount(aas.into_iter().map(|a| a.0).collect())
            }
            msg => {
                let msg_type: &'static str = msg.into();
                panic!("unsupported message {msg_type}")
            }
        }
    }
}

impl actix::Handler<NetworkClientMessages> for Actor {
    type Result = NetworkClientResponses;
    fn handle(&mut self, msg: NetworkClientMessages, _ctx: &mut Self::Context) -> Self::Result {
        let mut resp = NetworkClientResponses::NoResponse;
        match msg {
            NetworkClientMessages::Block(b, _, _) => self.event_sink.push(Event::Block(b)),
            NetworkClientMessages::BlockHeaders(bhs, _) => {
                self.event_sink.push(Event::BlockHeaders(bhs))
            }
            NetworkClientMessages::PartialEncodedChunkResponse(resp, _) => {
                self.event_sink.push(Event::Chunk(resp.parts))
            }
            NetworkClientMessages::PartialEncodedChunkRequest(req, _) => {
                self.event_sink.push(Event::ChunkRequest(req.chunk_hash))
            }
            NetworkClientMessages::Transaction { transaction, .. } => {
                self.event_sink.push(Event::Transaction(transaction));
                resp = NetworkClientResponses::ValidTx;
            }
            NetworkClientMessages::Challenge(c) => self.event_sink.push(Event::Challenge(c)),
            NetworkClientMessages::EpochSyncResponse(_, resp) => {
                self.event_sink.push(Event::EpochSyncResponse(*resp))
            }
            NetworkClientMessages::NetworkInfo(_) => {}
            msg => {
                let msg_type: &'static str = msg.into();
                panic!("unsupported message {msg_type}")
            }
        };
        resp
    }
}
