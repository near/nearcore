use std::time::Instant;

use actix::Message;
use near_primitives::{hash::CryptoHash, sharding::PartialEncodedChunk};

use crate::types::{
    MsgRecipient, PartialEncodedChunkForwardMsg, PartialEncodedChunkRequestMsg,
    PartialEncodedChunkResponseMsg,
};

/// The interface of the ShardsManager for the networking component. Note that this is defined
/// in near-network and not in near-chunks because near-chunks has a dependency on
/// near-network.
pub trait ShardsManagerAdapterForNetwork: Send + Sync + 'static {
    /// Processes a PartialEncodedChunk received from the network.
    /// These are received from chunk producers, containing owned parts and tracked
    /// receipt proofs.
    fn process_partial_encoded_chunk(&self, partial_encoded_chunk: PartialEncodedChunk);
    /// Processes a PartialEncodedChunkForwardMsg received from the network.
    /// These are received from part owners as an optimization (of the otherwise
    /// reactive path of requesting parts that are missing).
    fn process_partial_encoded_chunk_forward(
        &self,
        partial_encoded_chunk_forward: PartialEncodedChunkForwardMsg,
    );
    /// Processes a PartialEncodedChunkResponseMsg received from the network.
    /// These are received in response to the PartialEncodedChunkRequestMsg
    /// we have sent earlier.
    fn process_partial_encoded_chunk_response(
        &self,
        partial_encoded_chunk_response: PartialEncodedChunkResponseMsg,
        received_time: Instant,
    );
    /// Processes a PartialEncodedChunkRequestMsg received from the network.
    /// These are received from another node when they think we have the parts
    /// or receipt proofs they need.
    fn process_partial_encoded_chunk_request(
        &self,
        partial_encoded_chunk_request: PartialEncodedChunkRequestMsg,
        route_back: CryptoHash,
    );
}

#[derive(Message)]
#[rtype(result = "()")]
pub enum ShardsManagerRequestFromNetwork {
    ProcessPartialEncodedChunk(PartialEncodedChunk),
    ProcessPartialEncodedChunkForward(PartialEncodedChunkForwardMsg),
    ProcessPartialEncodedChunkResponse {
        partial_encoded_chunk_response: PartialEncodedChunkResponseMsg,
        received_time: Instant,
    },
    ProcessPartialEncodedChunkRequest {
        partial_encoded_chunk_request: PartialEncodedChunkRequestMsg,
        route_back: CryptoHash,
    },
}

impl<A: MsgRecipient<ShardsManagerRequestFromNetwork>> ShardsManagerAdapterForNetwork for A {
    fn process_partial_encoded_chunk(&self, partial_encoded_chunk: PartialEncodedChunk) {
        self.do_send(ShardsManagerRequestFromNetwork::ProcessPartialEncodedChunk(
            partial_encoded_chunk,
        ));
    }
    fn process_partial_encoded_chunk_forward(
        &self,
        partial_encoded_chunk_forward: PartialEncodedChunkForwardMsg,
    ) {
        self.do_send(ShardsManagerRequestFromNetwork::ProcessPartialEncodedChunkForward(
            partial_encoded_chunk_forward,
        ));
    }
    fn process_partial_encoded_chunk_response(
        &self,
        partial_encoded_chunk_response: PartialEncodedChunkResponseMsg,
        received_time: Instant,
    ) {
        self.do_send(ShardsManagerRequestFromNetwork::ProcessPartialEncodedChunkResponse {
            partial_encoded_chunk_response,
            received_time,
        });
    }
    fn process_partial_encoded_chunk_request(
        &self,
        partial_encoded_chunk_request: PartialEncodedChunkRequestMsg,
        route_back: CryptoHash,
    ) {
        self.do_send(ShardsManagerRequestFromNetwork::ProcessPartialEncodedChunkRequest {
            partial_encoded_chunk_request,
            route_back,
        });
    }
}
