//! This module contains the logic of handling orphaned chunk state witnesses.
//! To process a ChunkStateWitness we need its previous block, but sometimes
//! the witness shows up before the previous block is available, so it can't be
//! processed immediately. In such cases the witness becomes an orphaned witness
//! and it's kept in the pool until the required block arrives. Once the block
//! arrives, all witnesses that were waiting for it can be processed.

use crate::Client;
use near_chain::Block;
use near_chain_primitives::Error;
use near_primitives::hash::CryptoHash;
use near_primitives::stateless_validation::state_witness::ChunkStateWitness;
use near_primitives::types::BlockHeight;
use near_primitives::validator_signer::ValidatorSigner;
use std::ops::Range;
use std::sync::Arc;

/// We keep only orphan witnesses that are within this distance of
/// the current chain head. This helps to reduce the size of
/// OrphanStateWitnessPool and protects against spam attacks.
/// The range starts at 2 because a witness at height of head+1 would
/// have the previous block available (the chain head), so it wouldn't be an orphan.
pub const ALLOWED_ORPHAN_WITNESS_DISTANCE_FROM_HEAD: Range<BlockHeight> = 2..6;

impl Client {
    pub fn handle_orphan_state_witness(
        &mut self,
        witness: ChunkStateWitness,
        witness_size: usize,
    ) -> Result<HandleOrphanWitnessOutcome, Error> {
        let chunk_header = &witness.chunk_header;
        let witness_height = chunk_header.height_created();
        let witness_shard = chunk_header.shard_id();

        let _span = tracing::debug_span!(target: "client",
            "handle_orphan_state_witness",
            witness_height,
            witness_shard,
            witness_chunk = ?chunk_header.chunk_hash(),
            witness_prev_block = ?chunk_header.prev_block_hash(),
        )
        .entered();

        // Don't save orphaned state witnesses which are far away from the current chain head.
        let chain_head = &self.chain.head()?;
        let head_distance = witness_height.saturating_sub(chain_head.height);
        if !ALLOWED_ORPHAN_WITNESS_DISTANCE_FROM_HEAD.contains(&head_distance) {
            tracing::debug!(
                target: "client",
                head_height = chain_head.height,
                "Not saving an orphaned ChunkStateWitness because its height isn't within the allowed height range");
            return Ok(HandleOrphanWitnessOutcome::TooFarFromHead {
                witness_height,
                head_height: chain_head.height,
            });
        }

        // Don't save orphaned state witnesses which are bigger than the allowed limit.
        let witness_size_u64: u64 = witness_size.try_into().map_err(|_| {
            Error::Other(format!("Cannot convert witness size to u64: {}", witness_size))
        })?;
        if witness_size_u64 > self.config.orphan_state_witness_max_size.as_u64() {
            tracing::warn!(
                target: "client",
                witness_height,
                witness_shard,
                witness_chunk = ?chunk_header.chunk_hash(),
                witness_prev_block = ?chunk_header.prev_block_hash(),
                witness_size,
                "Not saving an orphaned ChunkStateWitness because it's too big. This is unexpected.");
            return Ok(HandleOrphanWitnessOutcome::TooBig(witness_size));
        }

        // Orphan witness is OK, save it to the pool
        tracing::debug!(target: "client", "Saving an orphaned ChunkStateWitness to orphan pool");
        self.chunk_validator.orphan_witness_pool.add_orphan_state_witness(witness, witness_size);
        Ok(HandleOrphanWitnessOutcome::SavedToPool)
    }

    fn process_ready_orphan_witnesses(&mut self, new_block: &Block, signer: &Arc<ValidatorSigner>) {
        let ready_witnesses = self
            .chunk_validator
            .orphan_witness_pool
            .take_state_witnesses_waiting_for_block(new_block.hash());
        for witness in ready_witnesses {
            let header = &witness.chunk_header;
            tracing::debug!(
                target: "client",
                witness_height = header.height_created(),
                witness_shard = header.shard_id(),
                witness_chunk = ?header.chunk_hash(),
                witness_prev_block = ?header.prev_block_hash(),
                "Processing an orphaned ChunkStateWitness, its previous block has arrived."
            );
            if let Err(err) =
                self.process_chunk_state_witness_with_prev_block(witness, new_block, None, signer)
            {
                tracing::error!(target: "client", ?err, "Error processing orphan chunk state witness");
            }
        }
    }

    /// Once a new block arrives, we can process the orphaned chunk state witnesses that were waiting
    /// for this block. This function takes the ready witnesses out of the orhan pool and process them.
    /// It also removes old witnesses (below final height) from the orphan pool to save memory.
    pub fn process_ready_orphan_witnesses_and_clean_old(
        &mut self,
        new_block: &Block,
        signer: &Option<Arc<ValidatorSigner>>,
    ) {
        if let Some(signer) = signer {
            self.process_ready_orphan_witnesses(new_block, signer);
        }

        // Remove all orphan witnesses that are below the last final block of the new block.
        // They won't be used, so we can remove them from the pool to save memory.
        let last_final_block = new_block.header().last_final_block();
        // Handle genesis gracefully.
        if last_final_block == &CryptoHash::default() {
            return;
        }
        let last_final_block = match self.chain.get_block_header(last_final_block) {
            Ok(block_header) => block_header,
            Err(err) => {
                tracing::error!(
                    target: "client",
                    ?last_final_block,
                    ?err,
                    "Error getting last final block of the new block"
                );
                return;
            }
        };
        self.chunk_validator
            .orphan_witness_pool
            .remove_witnesses_below_final_height(last_final_block.height());
    }
}

/// Outcome of processing an orphaned witness.
/// If the witness is clearly invalid, it's rejected and the handler function produces an error.
/// Sometimes the witness might not be strictly invalid, but we still don't want to save it because
/// of other reasons. In such cases the handler function returns Ok(outcome) to let the caller
/// know what happened with the witness.
/// It's useful in tests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HandleOrphanWitnessOutcome {
    SavedToPool,
    TooBig(usize),
    TooFarFromHead { head_height: BlockHeight, witness_height: BlockHeight },
}
