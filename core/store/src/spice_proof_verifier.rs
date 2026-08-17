//! Verification for SPICE light-client chunk-execution proofs.
//!
//! A light client trusts a final head and its block merkle root. These functions
//! recompute that root from a served proof without trusting the serving node.

use near_primitives::hash::CryptoHash;
use near_primitives::merkle::{compute_root_from_path, compute_root_from_path_and_item};
use near_primitives::types::SpiceChunkId;
use near_primitives::views::ChunkExecutionProofView;

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
