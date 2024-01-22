use std::collections::HashSet;

use near_chain_primitives::Error;
use near_primitives::block::Block;
use near_primitives::chunk_validation::ChunkEndorsement;

use crate::Chain;

impl Chain {
    /// This function validates the chunk_endorsements present in the block body. Validation does the following:
    ///    - Match number of chunks/shards with number of chunk endorsements vector.
    ///    - For each chunk/shard, do the following:
    ///       - Match number of chunk validators with number of chunk endorsement signatures.
    ///       - Verify that the signature are valid for given chunk_validator index.
    ///         Essentially, signature[i] should correspond with ordered_chunk_validators[i].
    ///       - Verify that the chunk has enough stake from chunk_validators to be endorsed.
    /// Note that while getting the chunk_validator_assignments, we use the chunk_header.height_created field.
    /// This is because chunk producer sends state witness to chunk validators assignment for `height_created`.
    /// We expect the endorsements to come from these chunk validators only.
    /// The chunk can later be included in block at any height which is recorded in `height_included` field.
    pub fn validate_chunk_endorsements_in_block(&self, block: &Block) -> Result<(), Error> {
        // Number of chunks and chunk endorsements must match and must be equal to number of shards
        if block.chunks().len() != block.chunk_endorsements().len() {
            tracing::error!(
                target: "chain",
                num_chunks = block.chunks().len(),
                num_chunk_endorsement_shards = block.chunk_endorsements().len(),
                "Number of chunks and chunk endorsements does not match",
            );
            return Err(Error::InvalidChunkEndorsement);
        }

        let epoch_id =
            self.epoch_manager.get_epoch_id_from_prev_block(block.header().prev_hash())?;
        for (chunk_header, signatures) in block.chunks().iter().zip(block.chunk_endorsements()) {
            // Validation for chunks in each shard
            // The signatures from chunk validators for each shard must match the ordered_chunk_validators
            let chunk_validator_assignments = self.epoch_manager.get_chunk_validator_assignments(
                &epoch_id,
                chunk_header.shard_id(),
                chunk_header.height_created(),
            )?;
            let ordered_chunk_validators = chunk_validator_assignments.ordered_chunk_validators();
            if ordered_chunk_validators.len() != signatures.len() {
                tracing::error!(
                    target: "chain",
                    num_ordered_chunk_validators = ordered_chunk_validators.len(),
                    num_chunk_endorsement_signatures = signatures.len(),
                    "Number of ordered chunk validators and chunk endorsement signatures does not match",
                );
                return Err(Error::InvalidChunkEndorsement);
            }

            // Verify that the signature in block body are valid for given chunk_validator.
            // Signature can be either None, or Some(signature).
            // We calculate the stake of the chunk_validators for who we have the signature present.
            let mut endorsed_chunk_validators = HashSet::new();
            for (account_id, signature) in ordered_chunk_validators.into_iter().zip(signatures) {
                let Some(signature) = signature else { continue };
                let (validator, _) = self.epoch_manager.get_validator_by_account_id(
                    &epoch_id,
                    block.header().prev_hash(),
                    &account_id,
                )?;

                // Block should not be produced with an invalid signature.
                if !ChunkEndorsement::validate_signature(
                    chunk_header.chunk_hash(),
                    signature,
                    validator.public_key(),
                ) {
                    tracing::error!(
                        target: "chain",
                        "Invalid chunk endorsement signature for chunk {:?} and validator {:?}",
                        chunk_header.chunk_hash(),
                        validator.account_id(),
                    );
                    return Err(Error::InvalidChunkEndorsement);
                }

                // Add validators with signature in endorsed_chunk_validators. We later use this to check stake.
                endorsed_chunk_validators.insert(account_id);
            }

            if !chunk_validator_assignments.does_chunk_have_enough_stake(&endorsed_chunk_validators)
            {
                tracing::error!(target: "chain", "Chunk does not have enough stake to be endorsed");
                return Err(Error::InvalidChunkEndorsement);
            }
        }
        Ok(())
    }
}
