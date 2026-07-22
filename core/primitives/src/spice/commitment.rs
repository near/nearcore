use crate::hash::CryptoHash;
use crate::merkle::{MerklePath, merklize, verify_path};
use crate::types::{BlockExecutionResults, BlockHeight, MerkleHash, ShardId};
use borsh::{BorshDeserialize, BorshSerialize};
use near_schema_checker_lib::ProtocolSchema;

/// Attributed commitment to a certified block's state root, a leaf of the
/// `prev_state_root` commitment set. `state_root` is the merkle root over the block's
/// per-shard certified state roots.
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct StateCommitment {
    pub block_hash: CryptoHash,
    pub block_height: BlockHeight,
    pub state_root: MerkleHash,
}

/// Attributed commitment to a certified block's outcome root, a leaf of the
/// `prev_outcome_root` commitment set. `outcome_root` is the merkle root over the
/// block's per-shard certified outcome roots.
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct OutcomeCommitment {
    pub block_hash: CryptoHash,
    pub block_height: BlockHeight,
    pub outcome_root: MerkleHash,
}

/// The SPICE execution-commitment proof carried alongside a block proof.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct SpiceCommitmentProofView {
    pub outcome_commitment_proof: MerklePath,
    pub state_commitment_proof: MerklePath,
    /// The certified block's height, to rebuild the commitment leaves.
    pub commitment_height: BlockHeight,
}

/// A block whose execution results became certified by some block's core
/// statements, with the identity needed to build its commitment set leaves.
#[derive(Debug, Clone, PartialEq)]
pub struct CertifiedBlockInfo {
    pub block_hash: CryptoHash,
    pub block_height: BlockHeight,
    pub execution_results: BlockExecutionResults,
}

impl CertifiedBlockInfo {
    pub fn state_commitment(&self) -> StateCommitment {
        StateCommitment {
            block_hash: self.block_hash,
            block_height: self.block_height,
            state_root: merklize_state_roots(&self.execution_results),
        }
    }

    pub fn outcome_commitment(&self) -> OutcomeCommitment {
        OutcomeCommitment {
            block_hash: self.block_hash,
            block_height: self.block_height,
            outcome_root: merklize_outcome_roots(&self.execution_results),
        }
    }
}

/// Per-shard certified roots sorted by `ShardId`, mirroring the shard order of
/// `Chunks::compute_state_root` / `compute_outcome_root`.
fn shard_sorted_roots(
    execution_results: &BlockExecutionResults,
    select: impl Fn(&BlockExecutionResults, ShardId) -> MerkleHash,
) -> Vec<MerkleHash> {
    let mut shard_ids: Vec<ShardId> = execution_results.0.keys().copied().collect();
    shard_ids.sort();
    shard_ids.into_iter().map(|shard_id| select(execution_results, shard_id)).collect()
}

pub fn merklize_state_roots(execution_results: &BlockExecutionResults) -> MerkleHash {
    let roots = shard_sorted_roots(execution_results, |results, shard_id| {
        *results.0[&shard_id].chunk_extra.state_root()
    });
    merklize(&roots).0
}

pub fn merklize_outcome_roots(execution_results: &BlockExecutionResults) -> MerkleHash {
    let roots = shard_sorted_roots(execution_results, |results, shard_id| {
        *results.0[&shard_id].chunk_extra.outcome_root()
    });
    merklize(&roots).0
}

fn sorted_by_height(certified: &[CertifiedBlockInfo]) -> Vec<&CertifiedBlockInfo> {
    let mut sorted: Vec<&CertifiedBlockInfo> = certified.iter().collect();
    sorted.sort_by_key(|info| info.block_height);
    sorted
}

pub fn state_commitments(certified: &[CertifiedBlockInfo]) -> Vec<StateCommitment> {
    sorted_by_height(certified).into_iter().map(CertifiedBlockInfo::state_commitment).collect()
}

pub fn outcome_commitments(certified: &[CertifiedBlockInfo]) -> Vec<OutcomeCommitment> {
    sorted_by_height(certified).into_iter().map(CertifiedBlockInfo::outcome_commitment).collect()
}

/// The two commitment set roots committed in an anchor block's `prev_state_root` /
/// `prev_outcome_root` slots. Shared by production and validation so they cannot
/// diverge on leaf bytes or order. An empty set yields `default()` roots.
pub fn compute_commitment_roots(certified: &[CertifiedBlockInfo]) -> (CryptoHash, CryptoHash) {
    (merklize(&state_commitments(certified)).0, merklize(&outcome_commitments(certified)).0)
}

/// Merkle path of `target_block_hash`'s outcome leaf within the commitment set root,
/// alongside the rebuilt leaf. `None` if the block is not in the set.
pub fn outcome_commitment_proof(
    certified: &[CertifiedBlockInfo],
    target_block_hash: &CryptoHash,
) -> Option<(MerklePath, OutcomeCommitment)> {
    let commitments = outcome_commitments(certified);
    let index = commitments.iter().position(|c| &c.block_hash == target_block_hash)?;
    let (_root, paths) = merklize(&commitments);
    Some((paths[index].clone(), commitments[index].clone()))
}

/// State analog of [`outcome_commitment_proof`].
pub fn state_commitment_proof(
    certified: &[CertifiedBlockInfo],
    target_block_hash: &CryptoHash,
) -> Option<(MerklePath, StateCommitment)> {
    let commitments = state_commitments(certified);
    let index = commitments.iter().position(|c| &c.block_hash == target_block_hash)?;
    let (_root, paths) = merklize(&commitments);
    Some((paths[index].clone(), commitments[index].clone()))
}

pub fn verify_outcome_commitment(
    commitment_root: MerkleHash,
    commitment_proof: &MerklePath,
    commitment: &OutcomeCommitment,
) -> bool {
    verify_path(commitment_root, commitment_proof, commitment)
}

pub fn verify_state_commitment(
    commitment_root: MerkleHash,
    commitment_proof: &MerklePath,
    commitment: &StateCommitment,
) -> bool {
    verify_path(commitment_root, commitment_proof, commitment)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bandwidth_scheduler::BandwidthRequests;
    use crate::congestion_info::CongestionInfo;
    use crate::types::chunk_extra::ChunkExtra;
    use crate::types::{ChunkExecutionResult, Gas};
    use near_primitives_core::types::Balance;
    use std::collections::HashMap;
    use std::sync::Arc;

    #[derive(Clone, Copy)]
    struct ShardRoots {
        shard_id: u64,
        // Dummy byte expanded to a `[byte; 32]` root hash; distinct bytes give distinct roots.
        state_root_byte: u8,
        outcome_root_byte: u8,
    }

    fn execution_results(shards: &[ShardRoots]) -> BlockExecutionResults {
        let mut results = HashMap::new();
        for shard in shards {
            let chunk_extra = ChunkExtra::new(
                &CryptoHash([shard.state_root_byte; 32]),
                CryptoHash([shard.outcome_root_byte; 32]),
                vec![],
                Gas::ZERO,
                Gas::ZERO,
                Balance::ZERO,
                Some(CongestionInfo::default()),
                BandwidthRequests::empty(),
                None,
            );
            let result =
                ChunkExecutionResult { chunk_extra, outgoing_receipts_root: CryptoHash::default() };
            results.insert(ShardId::new(shard.shard_id), Arc::new(result));
        }
        BlockExecutionResults(results)
    }

    // `num_blocks` distinct single-shard blocks; block `n` uses byte `n` for its
    // hash, height, and roots (see `ShardRoots`).
    fn generate_test_blocks(num_blocks: u8) -> Vec<CertifiedBlockInfo> {
        (1..=num_blocks)
            .map(|byte| CertifiedBlockInfo {
                block_hash: CryptoHash([byte; 32]),
                block_height: BlockHeight::from(byte),
                execution_results: execution_results(&[ShardRoots {
                    shard_id: 0,
                    state_root_byte: byte,
                    outcome_root_byte: byte + 1,
                }]),
            })
            .collect()
    }

    #[test]
    fn test_empty_set_yields_default_roots() {
        let (state_root, outcome_root) = compute_commitment_roots(&[]);
        assert_eq!(state_root, CryptoHash::default());
        assert_eq!(outcome_root, CryptoHash::default());
    }

    #[test]
    fn test_per_shard_roots_sorted_by_shard_id() {
        let shard_0 = ShardRoots { shard_id: 0, state_root_byte: 1, outcome_root_byte: 2 };
        let shard_1 = ShardRoots { shard_id: 1, state_root_byte: 3, outcome_root_byte: 4 };
        let ascending = execution_results(&[shard_0, shard_1]);
        let descending = execution_results(&[shard_1, shard_0]);
        assert_eq!(merklize_state_roots(&ascending), merklize_state_roots(&descending));
        assert_eq!(merklize_outcome_roots(&ascending), merklize_outcome_roots(&descending));
    }

    #[test]
    fn test_roots_independent_of_input_order() {
        let blocks = generate_test_blocks(3);
        let mut reversed = blocks.clone();
        reversed.reverse();
        assert_eq!(compute_commitment_roots(&blocks), compute_commitment_roots(&reversed));
    }

    #[test]
    fn test_singleton_commitment_root_is_leaf_hash() {
        let blocks = generate_test_blocks(1);
        let (_state_root, outcome_root) = compute_commitment_roots(&blocks);
        let (proof, leaf) = outcome_commitment_proof(&blocks, &blocks[0].block_hash).unwrap();
        assert!(proof.is_empty());
        assert_eq!(outcome_root, CryptoHash::hash_borsh(&leaf));
    }

    #[test]
    fn test_commitment_proofs_verify_for_every_block() {
        let blocks = generate_test_blocks(4);
        let (state_root, outcome_root) = compute_commitment_roots(&blocks);
        for block in &blocks {
            let (outcome_proof, outcome_leaf) =
                outcome_commitment_proof(&blocks, &block.block_hash).unwrap();
            assert!(verify_outcome_commitment(outcome_root, &outcome_proof, &outcome_leaf));
            assert_eq!(outcome_leaf, block.outcome_commitment());

            let (state_proof, state_leaf) =
                state_commitment_proof(&blocks, &block.block_hash).unwrap();
            assert!(verify_state_commitment(state_root, &state_proof, &state_leaf));
            assert_eq!(state_leaf, block.state_commitment());
        }
    }

    #[test]
    fn test_commitment_proof_absent_for_unknown_block() {
        let blocks = generate_test_blocks(2);
        let (committed, unknown) = blocks.split_at(1);
        assert!(outcome_commitment_proof(committed, &unknown[0].block_hash).is_none());
    }
}
