//! This module contains the logic of handling orphaned chunk state witnesses.
//! To process a ChunkStateWitness we need its previous block, but sometimes
//! the witness shows up before the previous block is available, so it can't be
//! processed immediately. In such cases the witness becomes an orphaned witness
//! and it's kept in the pool until the required block arrives. Once the block
//! arrives, all witnesses that were waiting for it can be processed.

use crate::Client;
use itertools::Itertools;
use near_chain::Block;
use near_chain_primitives::Error;
use near_primitives::network::PeerId;
use near_primitives::stateless_validation::ChunkStateWitness;
use near_primitives::types::{BlockHeight, EpochId};
use std::ops::Range;

/// We keep only orphan witnesses that are within this distance of
/// the current chain head. This helps to reduce the size of
/// OrphanStateWitnessPool and protects against spam attacks.
/// The range starts at 2 because a witness at height of head+1 would
/// have the previous block available (the chain head), so it wouldn't be an orphan.
pub const ALLOWED_ORPHAN_WITNESS_DISTANCE_FROM_HEAD: Range<BlockHeight> = 2..6;

/// We keep only orphan witnesses which are smaller than this size.
/// This limits the maximum memory usage of OrphanStateWitnessPool.
pub const MAX_ORPHAN_WITNESS_SIZE: usize = 40_000_000;

impl Client {
    pub fn handle_orphan_state_witness(
        &mut self,
        witness: ChunkStateWitness,
    ) -> Result<HandleOrphanWitnessOutcome, Error> {
        let chunk_header = &witness.inner.chunk_header;
        let witness_height = chunk_header.height_created();
        let witness_shard = chunk_header.shard_id();

        // Don't save orphaned state witnesses which are far away from the current chain head.
        let chain_head = &self.chain.head()?;
        let head_distance = witness_height.saturating_sub(chain_head.height);
        if !ALLOWED_ORPHAN_WITNESS_DISTANCE_FROM_HEAD.contains(&head_distance) {
            tracing::debug!(
                target: "client",
                head_height = chain_head.height,
                witness_height,
                witness_shard,
                witness_chunk = ?chunk_header.chunk_hash(),
                witness_prev_block = ?chunk_header.prev_block_hash(),
                "Not saving an orphaned ChunkStateWitness because its height isn't within the allowed height range");
            return Ok(HandleOrphanWitnessOutcome::TooFarFromHead {
                witness_height,
                head_height: chain_head.height,
            });
        }

        // Don't save orphaned state witnesses which are bigger than the allowed limit.
        let witness_size = borsh::to_vec(&witness)?.len();
        if witness_size > MAX_ORPHAN_WITNESS_SIZE {
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

        // Try to find the EpochId to which this witness will belong based on its height.
        // It's not always possible to determine the exact epoch_id because the exact
        // starting height of the next epoch isn't known until it actually starts,
        // so things can get unclear around epoch boundaries.
        // Let's collect the epoch_ids in which the witness might possibly be.
        let possible_epochs =
            self.epoch_manager.possible_epochs_of_height_around_tip(&chain_head, witness_height)?;

        // Try to validate the witness assuming that it resides in one of the possible epochs.
        // The witness must pass validation in one of these epochs before it can be admitted to the pool.
        let mut epoch_validation_result: Option<Result<(), Error>> = None;
        for epoch_id in possible_epochs {
            match self.partially_validate_orphan_witness_in_epoch(&witness, &epoch_id) {
                Ok(()) => {
                    epoch_validation_result = Some(Ok(()));
                    break;
                }
                Err(err) => epoch_validation_result = Some(Err(err)),
            }
        }
        match epoch_validation_result {
            Some(Ok(())) => {} // Validation passed in one of the possible epochs, witness can be added to the pool.
            Some(Err(err)) => {
                // Validation failed in all possible epochs, reject the witness
                return Err(err);
            }
            None => {
                // possible_epochs was empty. This shouldn't happen as all epochs around the chain head are known.
                return Err(Error::Other(format!(
                "Couldn't find any matching EpochId for orphan chunk state witness with height {}",
                witness_height
            )));
            }
        }

        // Orphan witness is OK, save it to the pool
        tracing::debug!(
            target: "client",
            witness_height,
            witness_shard,
            witness_chunk = ?chunk_header.chunk_hash(),
            witness_prev_block = ?chunk_header.prev_block_hash(),
            "Saving an orphaned ChunkStateWitness to orphan pool");
        self.chunk_validator.orphan_witness_pool.add_orphan_state_witness(witness, witness_size);
        Ok(HandleOrphanWitnessOutcome::SavedTooPool)
    }

    fn partially_validate_orphan_witness_in_epoch(
        &self,
        witness: &ChunkStateWitness,
        epoch_id: &EpochId,
    ) -> Result<(), Error> {
        let chunk_header = &witness.inner.chunk_header;
        let witness_height = chunk_header.height_created();
        let witness_shard = chunk_header.shard_id();

        // Validate shard_id
        if !self.epoch_manager.get_shard_layout(&epoch_id)?.shard_ids().contains(&witness_shard) {
            return Err(Error::InvalidChunkStateWitness(format!(
                "Invalid shard_id in ChunkStateWitness: {}",
                witness_shard
            )));
        }

        // Reject witnesses for chunks for which which this node isn't a validator.
        // It's an error, as the sender shouldn't send the witness to a non-validator node.
        let Some(my_signer) = self.chunk_validator.my_signer.as_ref() else {
            return Err(Error::NotAValidator);
        };
        let chunk_validator_assignments = self.epoch_manager.get_chunk_validator_assignments(
            &epoch_id,
            witness_shard,
            witness_height,
        )?;
        if !chunk_validator_assignments.contains(my_signer.validator_id()) {
            return Err(Error::NotAChunkValidator);
        }

        // Verify signature
        if !self.epoch_manager.verify_chunk_state_witness_signature_in_epoch(&witness, &epoch_id)? {
            return Err(Error::InvalidChunkStateWitness("Invalid signature".to_string()));
        }

        Ok(())
    }

    pub fn process_ready_orphan_chunk_state_witnesses(&mut self, accepted_block: &Block) {
        let ready_witnesses = self
            .chunk_validator
            .orphan_witness_pool
            .take_state_witnesses_waiting_for_block(accepted_block.hash());
        for witness in ready_witnesses {
            let header = &witness.inner.chunk_header;
            tracing::debug!(
                target: "client",
                witness_height = header.height_created(),
                witness_shard = header.shard_id(),
                witness_chunk = ?header.chunk_hash(),
                witness_prev_block = ?header.prev_block_hash(),
                "Processing an orphaned ChunkStateWitness, its previous block has arrived."
            );
            if let Err(err) = self.process_chunk_state_witness_with_prev_block(
                witness,
                PeerId::random(), // TODO: Should peer_id even be here? https://github.com/near/stakewars-iv/issues/17
                accepted_block,
                None,
            ) {
                tracing::error!(target: "client", ?err, "Error processing orphan chunk state witness");
            }
        }
    }
}

/// Outcome of processing an orphaned witness.
/// If the witness is clearly invalid, it's rejected and the handler function produces an error.
/// Sometimes the witness might not be strictly invalid, but we still don't want to save it because
/// of other reasons. In such cases the handler function returns Ok(outcome) to let the caller
/// know what happened with the witness.
/// It's useful in tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HandleOrphanWitnessOutcome {
    SavedTooPool,
    TooBig(usize),
    TooFarFromHead { head_height: BlockHeight, witness_height: BlockHeight },
}
