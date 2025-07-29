pub mod orphan_witness_pool;

use itertools::Itertools;
use near_async::messaging::Sender;
use near_epoch_manager::EpochManagerAdapter;
use near_network::types::{NetworkRequests, PeerManagerMessageRequest};
use near_primitives::sharding::ShardChunkHeader;
use near_primitives::stateless_validation::chunk_endorsement::ChunkEndorsement;
use near_primitives::validator_signer::ValidatorSigner;

// After validating a chunk state witness, we ideally need to send the chunk endorsement
// to just the next block producer at height h. However, it's possible that blocks at height
// h may be skipped and block producer at height h+1 picks up the chunk. We need to ensure
// that these later block producers also receive the chunk endorsement.
// Keeping a threshold of 5 block producers should be sufficient for most scenarios.
const NUM_NEXT_BLOCK_PRODUCERS_TO_SEND_CHUNK_ENDORSEMENT: u64 = 5;

/// Sends the chunk endorsement to the next
/// `NUM_NEXT_BLOCK_PRODUCERS_TO_SEND_CHUNK_ENDORSEMENT` block producers.
/// Additionally returns chunk endorsement if the signer is one of these block
/// producers, to be able to process it immediately.
pub(crate) fn send_chunk_endorsement_to_block_producers(
    chunk_header: &ShardChunkHeader,
    epoch_manager: &dyn EpochManagerAdapter,
    signer: &ValidatorSigner,
    network_sender: &Sender<PeerManagerMessageRequest>,
) -> Option<ChunkEndorsement> {
    let _span = tracing::debug_span!(
        target: "client",
        "send_chunk_endorsement",
        chunk_hash = ?chunk_header.chunk_hash(),
        height = %chunk_header.height_created(),
        shard_id = %chunk_header.shard_id(),
        validator = %signer.validator_id(),
        tag_block_production = true,
    )
    .entered();

    let epoch_id =
        epoch_manager.get_epoch_id_from_prev_block(chunk_header.prev_block_hash()).unwrap();

    // Send the chunk endorsement to the next NUM_NEXT_BLOCK_PRODUCERS_TO_SEND_CHUNK_ENDORSEMENT block producers.
    // It's possible we may reach the end of the epoch, in which case, ignore the error from get_block_producer.
    // It is possible that the same validator appears multiple times in the upcoming block producers,
    // thus we collect the unique set of account ids.
    let block_height = chunk_header.height_created();
    let block_producers = (0..NUM_NEXT_BLOCK_PRODUCERS_TO_SEND_CHUNK_ENDORSEMENT)
        .map_while(|i| epoch_manager.get_block_producer(&epoch_id, block_height + i).ok())
        .unique()
        .collect_vec();
    assert!(!block_producers.is_empty());

    let chunk_hash = chunk_header.chunk_hash();
    tracing::debug!(
        target: "client",
        chunk_hash=?chunk_hash,
        shard_id=%chunk_header.shard_id(),
        ?block_producers,
        "send_chunk_endorsement",
    );

    let endorsement = ChunkEndorsement::new(epoch_id, chunk_header, signer);
    let mut send_to_itself = None;
    for block_producer in block_producers {
        if &block_producer == signer.validator_id() {
            send_to_itself = Some(endorsement.clone());
        }
        network_sender.send(PeerManagerMessageRequest::NetworkRequests(
            NetworkRequests::ChunkEndorsement(block_producer, endorsement.clone()),
        ));
    }
    send_to_itself
}
