pub mod orphan_witness_handling;
pub mod orphan_witness_pool;

use crate::Client;
use itertools::Itertools;
use near_async::messaging::Sender;
use near_chain::chain::ChunkStateWitnessMessage;
use near_chain::stateless_validation::processing_tracker::ProcessingDoneTracker;
use near_chain_primitives::Error;
use near_epoch_manager::EpochManagerAdapter;
use near_network::types::{NetworkRequests, PeerManagerMessageRequest};
use near_o11y::log_assert;
use near_primitives::sharding::ShardChunkHeader;
use near_primitives::stateless_validation::chunk_endorsement::ChunkEndorsement;
use near_primitives::stateless_validation::state_witness::{
    ChunkStateWitness, ChunkStateWitnessSize,
};
use near_primitives::validator_signer::ValidatorSigner;
use std::sync::Arc;

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

impl Client {
    /// Responds to a network request to verify a `ChunkStateWitness`, which is
    /// sent by chunk producers after they produce a chunk.
    /// State witness is processed asynchronously, if you want to wait for the processing to finish
    /// you can use the `processing_done_tracker` argument (but it's optional, it's safe to pass None there).
    pub fn process_chunk_state_witness(
        &mut self,
        witness: ChunkStateWitness,
        raw_witness_size: ChunkStateWitnessSize,
        processing_done_tracker: Option<ProcessingDoneTracker>,
        signer: Option<Arc<ValidatorSigner>>,
    ) -> Result<(), Error> {
        let _span = tracing::debug_span!(
            target: "client",
            "client_process_chunk_state_witness",
            chunk_hash = ?witness.chunk_header().chunk_hash(),
            height = %witness.chunk_header().height_created(),
            shard_id = %witness.chunk_header().shard_id(),
            tag_witness_distribution = true,
        )
        .entered();

        // Chunk producers should not receive state witness from themselves.
        log_assert!(
            signer.is_some(),
            "Received a chunk state witness but this is not a validator node. Witness={:?}",
            witness
        );

        // Shard layout check.
        let chunk_production_key = witness.chunk_production_key();
        let epoch_id = witness.epoch_id();
        if !self
            .epoch_manager
            .get_shard_layout(&epoch_id)?
            .shard_ids()
            .contains(&chunk_production_key.shard_id)
        {
            return Err(Error::InvalidShardId(chunk_production_key.shard_id));
        }

        // Acknowledgement will be sent by the chunk validation actor.

        if self.config.save_latest_witnesses {
            self.chain.chain_store.save_latest_chunk_state_witness(&witness)?;
        }

        // Delegate all chunk validation to the chunk validation actor
        // The actor handles both cases: when prev_block is available and when it's not
        let witness_message =
            ChunkStateWitnessMessage { witness, raw_witness_size, processing_done_tracker };
        self.chunk_validation_sender.chunk_state_witness.send(witness_message);

        Ok(())
    }
}
