use std::{sync::Arc, time::Instant};

use actix::Message;
use near_chain::types::Tip;
use near_network::{
    shards_manager::ShardsManagerAdapterForNetwork,
    types::{
        MsgRecipient, PartialEncodedChunkForwardMsg, PartialEncodedChunkRequestMsg,
        PartialEncodedChunkResponseMsg,
    },
};
use near_primitives::{
    hash::CryptoHash,
    merkle::MerklePath,
    receipt::Receipt,
    sharding::{EncodedShardChunk, PartialEncodedChunk, ShardChunkHeader},
    types::EpochId,
};

/// The interface of ShardsManager which can be used by any component that wishes to send a message
/// to it. It is thread safe (messages are posted to a dedicated thread that runs the
/// ShardsManager).
pub trait ShardsManagerAdapter: Send + Sync + 'static {
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
    /// Processes the header seen from a block we received, if we have not already received the
    /// header earlier from the chunk producer (via PartialEncodedChunk).
    /// This can happen if we are not a validator, or if we are a validator but somehow missed
    /// the chunk producer's message.
    fn process_chunk_header_from_block(&self, chunk_header: &ShardChunkHeader);
    /// Lets the ShardsManager know that the chain heads have been updated.
    /// For a discussion of head vs header_head, see #8154.
    fn update_chain_heads(&self, head: Tip, header_head: Tip);
    /// As a chunk producer, distributes the given chunk to the other validators (by sending
    /// PartialEncodedChunk messages to them).
    /// The partial_chunk and encoded_chunk represent the same data, just in different formats.
    ///
    /// TODO(#8422): the arguments contain redundant information.
    fn distribute_encoded_chunk(
        &self,
        partial_chunk: PartialEncodedChunk,
        encoded_chunk: EncodedShardChunk,
        merkle_paths: Vec<MerklePath>,
        outgoing_receipts: Vec<Receipt>,
    );
    /// Requests the given chunks to be fetched from other nodes.
    /// Only the parts and receipt proofs that this node cares about will be fetched; when
    /// the fetching is complete, a response of ClientAdapterForShardsManager::did_complete_chunk
    /// will be sent back to the client.
    fn request_chunks(&self, chunks_to_request: Vec<ShardChunkHeader>, prev_hash: CryptoHash);
    /// Similar to request_chunks, but for orphan chunks. Since the chunk belongs to an orphan
    /// block, the previous block is not known and thus we cannot derive epoch information from
    /// that block. Therefore, an ancestor_hash must be provided which must correspond to a
    /// block that is an ancestor of these chunks' blocks, and which must be in the same epoch.
    fn request_chunks_for_orphan(
        &self,
        chunks_to_request: Vec<ShardChunkHeader>,
        epoch_id: EpochId,
        ancestor_hash: CryptoHash,
    );
    /// In response to processing a block, checks if there are any chunks that should have been
    /// complete but are just waiting on the previous block to become available (e.g. a chunk
    /// requested by request_chunks_for_orphan, which then received all needed parts and receipt
    /// proofs, but cannot be marked as complete because the previous block isn't available),
    /// and completes them if so.
    fn check_incomplete_chunks(&self, prev_block_hash: CryptoHash);
}

#[derive(Message)]
#[rtype(result = "()")]
pub enum ShardsManagerRequest {
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
    ProcessChunkHeaderFromBlock(ShardChunkHeader),
    UpdateChainHeads {
        head: Tip,
        header_head: Tip,
    },
    DistributeEncodedChunk {
        partial_chunk: PartialEncodedChunk,
        encoded_chunk: EncodedShardChunk,
        merkle_paths: Vec<MerklePath>,
        outgoing_receipts: Vec<Receipt>,
    },
    RequestChunks {
        chunks_to_request: Vec<ShardChunkHeader>,
        prev_hash: CryptoHash,
    },
    RequestChunksForOrphan {
        chunks_to_request: Vec<ShardChunkHeader>,
        epoch_id: EpochId,
        ancestor_hash: CryptoHash,
    },
    CheckIncompleteChunks(CryptoHash),
}

impl<A: MsgRecipient<ShardsManagerRequest>> ShardsManagerAdapter for A {
    fn process_partial_encoded_chunk(&self, partial_encoded_chunk: PartialEncodedChunk) {
        self.do_send(ShardsManagerRequest::ProcessPartialEncodedChunk(partial_encoded_chunk));
    }
    fn process_partial_encoded_chunk_forward(
        &self,
        partial_encoded_chunk_forward: PartialEncodedChunkForwardMsg,
    ) {
        self.do_send(ShardsManagerRequest::ProcessPartialEncodedChunkForward(
            partial_encoded_chunk_forward,
        ));
    }
    fn process_partial_encoded_chunk_response(
        &self,
        partial_encoded_chunk_response: PartialEncodedChunkResponseMsg,
        received_time: Instant,
    ) {
        self.do_send(ShardsManagerRequest::ProcessPartialEncodedChunkResponse {
            partial_encoded_chunk_response,
            received_time,
        });
    }
    fn process_partial_encoded_chunk_request(
        &self,
        partial_encoded_chunk_request: PartialEncodedChunkRequestMsg,
        route_back: CryptoHash,
    ) {
        self.do_send(ShardsManagerRequest::ProcessPartialEncodedChunkRequest {
            partial_encoded_chunk_request,
            route_back,
        });
    }
    fn process_chunk_header_from_block(&self, chunk_header: &ShardChunkHeader) {
        self.do_send(ShardsManagerRequest::ProcessChunkHeaderFromBlock(chunk_header.clone()));
    }
    fn update_chain_heads(&self, head: Tip, header_head: Tip) {
        self.do_send(ShardsManagerRequest::UpdateChainHeads { head, header_head });
    }
    fn distribute_encoded_chunk(
        &self,
        partial_chunk: PartialEncodedChunk,
        encoded_chunk: EncodedShardChunk,
        merkle_paths: Vec<MerklePath>,
        outgoing_receipts: Vec<Receipt>,
    ) {
        self.do_send(ShardsManagerRequest::DistributeEncodedChunk {
            partial_chunk,
            encoded_chunk,
            merkle_paths,
            outgoing_receipts,
        });
    }
    fn request_chunks(&self, chunks_to_request: Vec<ShardChunkHeader>, prev_hash: CryptoHash) {
        self.do_send(ShardsManagerRequest::RequestChunks { chunks_to_request, prev_hash });
    }
    fn request_chunks_for_orphan(
        &self,
        chunks_to_request: Vec<ShardChunkHeader>,
        epoch_id: EpochId,
        ancestor_hash: CryptoHash,
    ) {
        self.do_send(ShardsManagerRequest::RequestChunksForOrphan {
            chunks_to_request,
            epoch_id,
            ancestor_hash,
        });
    }
    fn check_incomplete_chunks(&self, prev_block_hash: CryptoHash) {
        self.do_send(ShardsManagerRequest::CheckIncompleteChunks(prev_block_hash));
    }
}

/// Implements the ShardsManagerAdapterForNetwork trait for ShardsManagerAdapter.
pub struct ShardsManagerAdapterAsAdapterForNetwork {
    pub adapter: Arc<dyn ShardsManagerAdapter>,
}

impl ShardsManagerAdapterAsAdapterForNetwork {
    pub fn new(adapter: Arc<dyn ShardsManagerAdapter>) -> Self {
        Self { adapter }
    }
}

impl ShardsManagerAdapterForNetwork for ShardsManagerAdapterAsAdapterForNetwork {
    fn process_partial_encoded_chunk(&self, partial_encoded_chunk: PartialEncodedChunk) {
        self.adapter.process_partial_encoded_chunk(partial_encoded_chunk)
    }

    fn process_partial_encoded_chunk_forward(
        &self,
        partial_encoded_chunk_forward: PartialEncodedChunkForwardMsg,
    ) {
        self.adapter.process_partial_encoded_chunk_forward(partial_encoded_chunk_forward)
    }

    fn process_partial_encoded_chunk_response(
        &self,
        partial_encoded_chunk_response: PartialEncodedChunkResponseMsg,
        received_time: Instant,
    ) {
        self.adapter
            .process_partial_encoded_chunk_response(partial_encoded_chunk_response, received_time)
    }

    fn process_partial_encoded_chunk_request(
        &self,
        partial_encoded_chunk_request: PartialEncodedChunkRequestMsg,
        route_back: CryptoHash,
    ) {
        self.adapter
            .process_partial_encoded_chunk_request(partial_encoded_chunk_request, route_back)
    }
}
