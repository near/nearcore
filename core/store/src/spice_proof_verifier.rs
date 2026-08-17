//! Verification for SPICE light-client chunk-execution proofs.
//!
//! A light client trusts a final head and its block merkle root. These functions
//! recompute that root from a served proof without trusting the serving node.

use near_primitives::hash::CryptoHash;
use near_primitives::merkle::{compute_root_from_path, compute_root_from_path_and_item};
use near_primitives::types::{ChunkExecutionRoots, SpiceChunkId};
use near_primitives::views::{ChunkExecutionProofView, ExecutionOutcomeWithIdView};

#[derive(thiserror::Error, Debug)]
pub enum SpiceProofVerificationError {
    #[error("proof is for chunk {found:?}, not the requested chunk {expected:?}")]
    UnexpectedChunkId { expected: SpiceChunkId, found: SpiceChunkId },
    #[error("certifying block header does not commit a chunk_execution_root")]
    MissingChunkExecutionRoot,
    #[error("roots proof does not recompute the certifying block's chunk_execution_root")]
    InvalidRootsProof,
    #[error("certifying block is not committed in the light client head's block merkle root")]
    InvalidBlockProof,
    #[error("proof is for outcome {found}, not the requested outcome {expected}")]
    UnexpectedOutcomeId { expected: CryptoHash, found: CryptoHash },
    #[error("outcome claims block {found}, but the roots are for block {expected}")]
    UnexpectedOutcomeBlockHash { expected: CryptoHash, found: CryptoHash },
    #[error("execution outcome proof does not recompute the chunk's outcome_root")]
    InvalidOutcomeProof,
}

/// Checks that `expected_chunk_id`'s execution roots are committed by a block that
/// the trusted `light_client_head_block_merkle_root` includes. The chunk id is
/// checked so a server cannot answer with a valid proof for a different chunk.
pub fn verify_chunk_execution_proof(
    proof: &ChunkExecutionProofView,
    expected_chunk_id: &SpiceChunkId,
    light_client_head_block_merkle_root: &CryptoHash,
) -> Result<(), SpiceProofVerificationError> {
    if proof.roots.chunk_id() != expected_chunk_id {
        return Err(SpiceProofVerificationError::UnexpectedChunkId {
            expected: expected_chunk_id.clone(),
            found: proof.roots.chunk_id().clone(),
        });
    }

    let committed_chunk_execution_root = proof
        .certifying_block_header_lite
        .inner_lite
        .chunk_execution_root
        .ok_or(SpiceProofVerificationError::MissingChunkExecutionRoot)?;

    let computed_chunk_execution_root =
        compute_root_from_path_and_item(&proof.roots_proof, &proof.roots);
    if computed_chunk_execution_root != committed_chunk_execution_root {
        return Err(SpiceProofVerificationError::InvalidRootsProof);
    }

    let certifying_block_hash = proof.certifying_block_header_lite.hash();
    let computed_head_root =
        compute_root_from_path(&proof.certifying_block_proof, certifying_block_hash);
    if &computed_head_root != light_client_head_block_merkle_root {
        return Err(SpiceProofVerificationError::InvalidBlockProof);
    }
    Ok(())
}

/// Checks that `expected_id`'s outcome hashes to a value committed by the chunk's
/// `outcome_root`. Call after [`verify_chunk_execution_proof`] to bind `roots` to the
/// trusted head.
pub fn verify_execution_outcome_proof(
    outcome_proof: &ExecutionOutcomeWithIdView,
    expected_id: &CryptoHash,
    roots: &ChunkExecutionRoots,
) -> Result<(), SpiceProofVerificationError> {
    let ChunkExecutionRoots::V1(roots) = roots;
    if &outcome_proof.id != expected_id {
        return Err(SpiceProofVerificationError::UnexpectedOutcomeId {
            expected: *expected_id,
            found: outcome_proof.id,
        });
    }
    // block_hash is not one of the hashed fields, so a server can set it to anything.
    if outcome_proof.block_hash != roots.chunk_id.block_hash {
        return Err(SpiceProofVerificationError::UnexpectedOutcomeBlockHash {
            expected: roots.chunk_id.block_hash,
            found: outcome_proof.block_hash,
        });
    }
    let outcome_hash = CryptoHash::hash_borsh(outcome_proof.to_hashes());
    let computed_outcome_root = compute_root_from_path(&outcome_proof.proof, outcome_hash);
    if computed_outcome_root != roots.outcome_root {
        return Err(SpiceProofVerificationError::InvalidOutcomeProof);
    }
    Ok(())
}
