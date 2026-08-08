//! Verification for SPICE light-client chunk-execution proofs.
//!
//! A light client trusts a final head and its block merkle root. These functions
//! recompute that root from a served proof without trusting the serving node.

use crate::trie::AccessOptions;
use crate::{PartialStorage, Trie};
use near_primitives::hash::CryptoHash;
use near_primitives::merkle::{compute_root_from_path, compute_root_from_path_and_item};
use near_primitives::state::{PartialState, TrieValue};
use near_primitives::types::{ChunkExecutionRoots, StoreValue};
use near_primitives::views::{
    ChunkExecutionProofView, ExecutionOutcomeWithIdView, StateProofTarget,
};

#[derive(thiserror::Error, Debug)]
pub enum SpiceProofVerificationError {
    #[error("certifying block header does not commit a chunk_execution_root")]
    MissingChunkExecutionRoot,
    #[error("roots proof does not recompute the certifying block's chunk_execution_root")]
    InvalidRootsProof,
    #[error("certifying block is not committed in the light client head's block merkle root")]
    InvalidBlockProof,
    #[error("execution outcome proof does not recompute the chunk's outcome_root")]
    InvalidOutcomeProof,
    #[error("state proof does not reconstruct the claimed value at the chunk's state_root")]
    InvalidStateProof,
    #[error("state proof trie access failed: {0}")]
    TrieError(String),
}

/// Checks that the chunk's execution roots are committed by a block that the
/// trusted `light_client_head_block_merkle_root` includes.
pub fn verify_chunk_execution_proof(
    proof: &ChunkExecutionProofView,
    light_client_head_block_merkle_root: &CryptoHash,
) -> Result<(), SpiceProofVerificationError> {
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

/// Checks that the outcome hashes to a value committed by the chunk's
/// `outcome_root`. Call after [`verify_chunk_execution_proof`] to bind `roots` to
/// the trusted head.
pub fn verify_execution_outcome_proof(
    outcome_proof: &ExecutionOutcomeWithIdView,
    roots: &ChunkExecutionRoots,
) -> Result<(), SpiceProofVerificationError> {
    let ChunkExecutionRoots::V1(roots) = roots;
    let outcome_hash = CryptoHash::hash_borsh(outcome_proof.to_hashes());
    let computed_outcome_root = compute_root_from_path(&outcome_proof.proof, outcome_hash);
    if computed_outcome_root != roots.outcome_root {
        return Err(SpiceProofVerificationError::InvalidOutcomeProof);
    }
    Ok(())
}

/// Verifies a state trie proof against the chunk's `state_root`: reconstructs the
/// trie from `state_proof` and checks the claimed `value` for `target`. Call after
/// [`verify_chunk_execution_proof`] to bind `roots` to the trusted head.
pub fn verify_state_proof(
    target: &StateProofTarget,
    value: Option<&StoreValue>,
    state_proof: &[TrieValue],
    roots: &ChunkExecutionRoots,
) -> Result<(), SpiceProofVerificationError> {
    let ChunkExecutionRoots::V1(roots) = roots;
    let partial_storage = PartialStorage { nodes: PartialState::TrieValues(state_proof.to_vec()) };
    let trie = Trie::from_recorded_storage(partial_storage, roots.state_root, false);
    let trie_key = target.to_trie_key().to_vec();
    let recovered_value = trie
        .get(&trie_key, AccessOptions::DEFAULT)
        .map_err(|error| SpiceProofVerificationError::TrieError(error.to_string()))?;
    let claimed_value: Option<Vec<u8>> = value.map(|value| value.clone().into());
    if recovered_value != claimed_value {
        return Err(SpiceProofVerificationError::InvalidStateProof);
    }
    Ok(())
}
