use std::time::Instant;

use near_primitives::{hash::CryptoHash, sharding::PartialEncodedChunk};

use crate::types::{
    PartialEncodedChunkForwardMsg, PartialEncodedChunkRequestMsg, PartialEncodedChunkResponseMsg,
};

/// A subset of the ShardsManagerAdapter so that this crate does not need to depend on the
/// `near-chunks` crate, for doing so would create a circular dependency.
pub trait ShardsManagerAdapterForNetwork: Send + Sync + 'static {
    fn process_partial_encoded_chunk(&self, partial_encoded_chunk: PartialEncodedChunk);
    fn process_partial_encoded_chunk_forward(
        &self,
        partial_encoded_chunk_forward: PartialEncodedChunkForwardMsg,
    );
    fn process_partial_encoded_chunk_response(
        &self,
        partial_encoded_chunk_response: PartialEncodedChunkResponseMsg,
        received_time: Instant,
    );
    fn process_partial_encoded_chunk_request(
        &self,
        partial_encoded_chunk_request: PartialEncodedChunkRequestMsg,
        route_back: CryptoHash,
    );
}
