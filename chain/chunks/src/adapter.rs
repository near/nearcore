use actix::Message;
use near_chain::types::Tip;
use near_network::types::MsgRecipient;
use near_primitives::{
    hash::CryptoHash,
    merkle::MerklePath,
    receipt::Receipt,
    sharding::{EncodedShardChunk, PartialEncodedChunk, ShardChunkHeader},
    types::EpochId,
};

/// The interface of ShardsManager that faces the Client.
/// It is thread safe (messages are posted to a dedicated thread that runs the
/// ShardsManager).
/// See also ShardsManagerAdapterForNetwork, which is the interface given to
/// the networking component.
/// See also ClientAdapterForShardsManager, which is the other direction - the
/// interface of the Client given to the ShardsManager.
pub trait ShardsManagerAdapterForClient: Send + Sync + 'static {
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
pub enum ShardsManagerRequestFromClient {
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

impl<A: MsgRecipient<ShardsManagerRequestFromClient>> ShardsManagerAdapterForClient for A {
    fn process_chunk_header_from_block(&self, chunk_header: &ShardChunkHeader) {
        self.do_send(ShardsManagerRequestFromClient::ProcessChunkHeaderFromBlock(
            chunk_header.clone(),
        ));
    }
    fn update_chain_heads(&self, head: Tip, header_head: Tip) {
        self.do_send(ShardsManagerRequestFromClient::UpdateChainHeads { head, header_head });
    }
    fn distribute_encoded_chunk(
        &self,
        partial_chunk: PartialEncodedChunk,
        encoded_chunk: EncodedShardChunk,
        merkle_paths: Vec<MerklePath>,
        outgoing_receipts: Vec<Receipt>,
    ) {
        self.do_send(ShardsManagerRequestFromClient::DistributeEncodedChunk {
            partial_chunk,
            encoded_chunk,
            merkle_paths,
            outgoing_receipts,
        });
    }
    fn request_chunks(&self, chunks_to_request: Vec<ShardChunkHeader>, prev_hash: CryptoHash) {
        self.do_send(ShardsManagerRequestFromClient::RequestChunks {
            chunks_to_request,
            prev_hash,
        });
    }
    fn request_chunks_for_orphan(
        &self,
        chunks_to_request: Vec<ShardChunkHeader>,
        epoch_id: EpochId,
        ancestor_hash: CryptoHash,
    ) {
        self.do_send(ShardsManagerRequestFromClient::RequestChunksForOrphan {
            chunks_to_request,
            epoch_id,
            ancestor_hash,
        });
    }
    fn check_incomplete_chunks(&self, prev_block_hash: CryptoHash) {
        self.do_send(ShardsManagerRequestFromClient::CheckIncompleteChunks(prev_block_hash));
    }
}
