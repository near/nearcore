use actix::Message;
use near_async::time::Instant;
use near_primitives::{hash::CryptoHash, sharding::PartialEncodedChunk};

use crate::types::{
    PartialEncodedChunkForwardMsg, PartialEncodedChunkRequestMsg, PartialEncodedChunkResponseMsg,
};

#[derive(Message, Debug, strum::IntoStaticStr, Clone, PartialEq, Eq)]
#[rtype(result = "()")]
#[allow(clippy::large_enum_variant)]
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
