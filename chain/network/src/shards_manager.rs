use crate::recv_permit::RecvMessagePermit;
use crate::types::{
    PartialEncodedChunkForwardMsg, PartialEncodedChunkRequestMsg, PartialEncodedChunkResponseMsg,
};
use near_async::time::Instant;
use near_primitives::{hash::CryptoHash, sharding::PartialEncodedChunk};

#[derive(Debug, strum::IntoStaticStr)]
#[allow(clippy::large_enum_variant)]
pub enum ShardsManagerRequestFromNetwork {
    ProcessPartialEncodedChunk(PartialEncodedChunk, RecvMessagePermit),
    ProcessPartialEncodedChunkForward(PartialEncodedChunkForwardMsg, RecvMessagePermit),
    ProcessPartialEncodedChunkResponse {
        partial_encoded_chunk_response: PartialEncodedChunkResponseMsg,
        received_time: Instant,
        recv_permit: RecvMessagePermit,
    },
    ProcessPartialEncodedChunkRequest {
        partial_encoded_chunk_request: PartialEncodedChunkRequestMsg,
        route_back: CryptoHash,
        recv_permit: RecvMessagePermit,
    },
}
