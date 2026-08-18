//! Verification for SPICE light-client proofs.
//!
//! A light client trusts a final head and its block merkle root. These functions
//! recompute that root from a served proof without trusting the serving node.

use crate::trie::AccessOptions;
use crate::{PartialStorage, Trie};
use near_primitives::hash::CryptoHash;
use near_primitives::merkle::{compute_root_from_path, compute_root_from_path_and_item};
use near_primitives::state::PartialState;
use near_primitives::types::{ChunkExecutionRoots, SpiceChunkId, StoreValue};
use near_primitives::views::{
    ChunkExecutionProofView, ExecutionOutcomeWithIdView, StateProofTarget, StateProofView,
};

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
    #[error("state proof does not reconstruct the claimed value at the chunk's state_root")]
    InvalidStateProof,
    #[error("state proof trie access failed: {0}")]
    TrieError(String),
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

/// What a verified state proof says about `target` in one shard.
#[derive(Debug)]
pub enum StateProofOutcome<'a> {
    Present(&'a StoreValue),
    /// Absent from this chunk's shard. Absence from the chain is a stronger claim.
    AbsentInShard,
}

/// Verifies a state trie proof against the chunk's `state_root`: reconstructs the
/// trie from `state_proof` and checks the claimed value for `target`. Call after
/// [`verify_chunk_execution_proof`] to bind `roots` to the trusted head.
// TODO(spice): `AbsentInShard` for a contract-code, contract-data, or access-key target
// can be raised to a global absence by pairing it with a `Present` account proof for the
// same chunk, since a shard's state only holds accounts it owns. Proving an account itself
// absent still needs an authenticated shard layout.
pub fn verify_state_proof<'a>(
    target: &StateProofTarget,
    state_proof: &'a StateProofView,
    roots: &ChunkExecutionRoots,
) -> Result<StateProofOutcome<'a>, SpiceProofVerificationError> {
    let ChunkExecutionRoots::V1(roots) = roots;
    let partial_storage =
        PartialStorage { nodes: PartialState::TrieValues(state_proof.nodes.clone()) };
    let trie = Trie::from_recorded_storage(partial_storage, roots.state_root, false);
    let trie_key = target.to_trie_key().to_vec();
    let recovered_value = trie
        .get(&trie_key, AccessOptions::DEFAULT)
        .map_err(|error| SpiceProofVerificationError::TrieError(error.to_string()))?;
    if recovered_value.as_deref() != state_proof.value.as_ref().map(|value| value.as_slice()) {
        return Err(SpiceProofVerificationError::InvalidStateProof);
    }
    Ok(match state_proof.value.as_ref() {
        Some(value) => StateProofOutcome::Present(value),
        None => StateProofOutcome::AbsentInShard,
    })
}
