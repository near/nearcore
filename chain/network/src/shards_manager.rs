use std::time::Instant;

use actix::Message;
use near_primitives::{hash::CryptoHash, sharding::PartialEncodedChunk};

use crate::types::{
    PartialEncodedChunkForwardMsg, PartialEncodedChunkRequestMsg, PartialEncodedChunkResponseMsg,
};

#[derive(Message, Debug)]
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
