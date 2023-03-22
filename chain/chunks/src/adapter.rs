use actix::Message;
use near_chain::types::Tip;
use near_primitives::{
    hash::CryptoHash,
    merkle::MerklePath,
    receipt::Receipt,
    sharding::{EncodedShardChunk, PartialEncodedChunk, ShardChunkHeader},
    types::EpochId,
};

#[derive(Message, Debug)]
#[rtype(result = "()")]
pub enum ShardsManagerRequestFromClient {
    /// Processes the header seen from a block we received, if we have not already received the
    /// header earlier from the chunk producer (via PartialEncodedChunk).
    /// This can happen if we are not a validator, or if we are a validator but somehow missed
    /// the chunk producer's message.
    ProcessChunkHeaderFromBlock(ShardChunkHeader),
    /// Lets the ShardsManager know that the chain heads have been updated.
    /// For a discussion of head vs header_head, see #8154.
    UpdateChainHeads { head: Tip, header_head: Tip },
    /// As a chunk producer, distributes the given chunk to the other validators (by sending
    /// PartialEncodedChunk messages to them).
    /// The partial_chunk and encoded_chunk represent the same data, just in different formats.
    ///
    /// TODO(#8422): the arguments contain redundant information.
    DistributeEncodedChunk {
        partial_chunk: PartialEncodedChunk,
        encoded_chunk: EncodedShardChunk,
        merkle_paths: Vec<MerklePath>,
        outgoing_receipts: Vec<Receipt>,
    },
    /// Requests the given chunks to be fetched from other nodes.
    /// Only the parts and receipt proofs that this node cares about will be fetched; when
    /// the fetching is complete, a response of ClientAdapterForShardsManager::did_complete_chunk
    /// will be sent back to the client.
    RequestChunks { chunks_to_request: Vec<ShardChunkHeader>, prev_hash: CryptoHash },
    /// Similar to request_chunks, but for orphan chunks. Since the chunk belongs to an orphan
    /// block, the previous block is not known and thus we cannot derive epoch information from
    /// that block. Therefore, an ancestor_hash must be provided which must correspond to a
    /// block that is an ancestor of these chunks' blocks, and which must be in the same epoch.
    RequestChunksForOrphan {
        chunks_to_request: Vec<ShardChunkHeader>,
        epoch_id: EpochId,
        ancestor_hash: CryptoHash,
    },
    /// In response to processing a block, checks if there are any chunks that should have been
    /// complete but are just waiting on the previous block to become available (e.g. a chunk
    /// requested by request_chunks_for_orphan, which then received all needed parts and receipt
    /// proofs, but cannot be marked as complete because the previous block isn't available),
    /// and completes them if so.
    CheckIncompleteChunks(CryptoHash),
}
