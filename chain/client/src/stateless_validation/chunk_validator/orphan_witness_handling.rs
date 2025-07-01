//! This module contains the logic of handling orphaned chunk state witnesses.
//! To process a ChunkStateWitness we need its previous block, but sometimes
//! the witness shows up before the previous block is available, so it can't be
//! processed immediately. In such cases the witness becomes an orphaned witness
//! and it's kept in the pool until the required block arrives. Once the block
//! arrives, all witnesses that were waiting for it can be processed.

use crate::Client;
use near_chain_primitives::Error;
use near_primitives::stateless_validation::state_witness::ChunkStateWitness;
use near_primitives::types::BlockHeight;
use std::ops::Range;

/// We keep only orphan witnesses that are within this distance of
/// the current chain head. This helps to reduce the size of
/// OrphanStateWitnessPool and protects against spam attacks.
/// The range starts at 2 because a witness at height of head+1 would
/// have the previous block available (the chain head), so it wouldn't be an orphan.
pub const ALLOWED_ORPHAN_WITNESS_DISTANCE_FROM_HEAD: Range<BlockHeight> = 2..6;

impl Client {
    /// Handle an orphan state witness by performing immediate validation checks
    /// and delegating storage to the chunk validation actor if validation passes.
    pub fn handle_orphan_state_witness(
        &mut self,
        witness: ChunkStateWitness,
        witness_size: usize,
    ) -> Result<HandleOrphanWitnessOutcome, Error> {
        use crate::chunk_validation_actor::OrphanWitnessMessage;
        use near_primitives::stateless_validation::state_witness::ChunkStateWitnessSize;

        let chunk_header = witness.chunk_header();
        let witness_height = chunk_header.height_created();
        let witness_shard = chunk_header.shard_id();

        // Get chain head to check distance
        let chain_head = self.chain.chain_store().head()?;
        let head_distance = witness_height.saturating_sub(chain_head.height);

        if !ALLOWED_ORPHAN_WITNESS_DISTANCE_FROM_HEAD.contains(&head_distance) {
            tracing::debug!(
                target: "chunk_validation",
                head_height = chain_head.height,
                "Not saving an orphaned ChunkStateWitness because its height isn't within the allowed height range"
            );
            return Ok(HandleOrphanWitnessOutcome::TooFarFromHead {
                witness_height,
                head_height: chain_head.height,
            });
        }

        // Check witness size limit
        let max_orphan_witness_size =
            near_chain_configs::default_orphan_state_witness_max_size().as_u64();
        let witness_size_u64 = witness_size as u64;
        if witness_size_u64 > max_orphan_witness_size {
            tracing::warn!(
                target: "chunk_validation",
                witness_height,
                ?witness_shard,
                witness_chunk = ?chunk_header.chunk_hash(),
                witness_prev_block = ?chunk_header.prev_block_hash(),
                witness_size = witness_size_u64,
                "Not saving an orphaned ChunkStateWitness because it's too big. This is unexpected."
            );
            return Ok(HandleOrphanWitnessOutcome::TooBig(witness_size));
        }

        let orphan_message =
            OrphanWitnessMessage { witness, witness_size: witness_size as ChunkStateWitnessSize };
        self.chunk_validation_sender.orphan_witness.send(orphan_message);

        // We can reliably return SavedToPool because:
        // 1. Message delivery is reliable (unbounded channel)
        // 2. Actor performs identical validation
        // 3. Pool storage always succeeds (LRU evicts old items if full)
        // 4. The only edge case (chain head race) is extremely rare
        Ok(HandleOrphanWitnessOutcome::SavedToPool)
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
