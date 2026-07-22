use crate::Chain;
use near_chain_primitives::Error;
use near_primitives::block::BlockHeader;
use near_primitives::hash::CryptoHash;
use near_primitives::merkle::MerklePath;
use near_primitives::spice::commitment::{outcome_commitment_proof, state_commitment_proof};
use near_primitives::types::BlockHeight;
use near_primitives::views::LightClientBlockLiteView;
use near_store::merkle_proof::MerkleProofAccess as _;

/// Inclusion of a certified block's execution commitment under a trusted head.
/// The anchor block is the certifier's canonical child; its `prev_state_root` /
/// `prev_outcome_root` slots carry the commitment over the certifier's set, and
/// anchors into the head's `block_merkle_root` via the classic block proof.
pub struct SpiceCommitmentProof {
    pub anchor_block_lite: LightClientBlockLiteView,
    pub anchor_block_proof: MerklePath,
    pub commitment_height: BlockHeight,
    pub state_commitment_proof: MerklePath,
    pub outcome_commitment_proof: MerklePath,
}

impl Chain {
    /// Proof that `certified_block_hash`'s execution commitment is anchored under
    /// `head_block_hash`. `None` (fails closed) when the proof is not available yet: the
    /// block is not certified on the canonical chain, or its anchor block (the certifier's
    /// canonical child) does not exist yet. `Err` is reserved for chain access failures.
    pub fn spice_commitment_proof(
        &self,
        certified_block_hash: &CryptoHash,
        head_block_hash: &CryptoHash,
    ) -> Result<Option<SpiceCommitmentProof>, Error> {
        let Some(certifier_hash) = self.chain_store().spice_certifier(certified_block_hash) else {
            return Ok(None);
        };
        let certifier_header = self.get_block_header(&certifier_hash)?;
        let anchor_hash = match self.get_block_hash_by_height(certifier_header.height() + 1) {
            Ok(anchor_hash) => anchor_hash,
            Err(Error::DBNotFoundErr(_)) => return Ok(None),
            Err(err) => return Err(err),
        };
        let anchor_header = self.get_block_header(&anchor_hash)?;
        if anchor_header.prev_hash() != &certifier_hash {
            return Ok(None);
        }

        let certified = self.spice_core_reader.certified_blocks_committed_by(&certifier_header)?;
        let Some((outcome_commitment_proof, _)) =
            outcome_commitment_proof(&certified, certified_block_hash)
        else {
            return Ok(None);
        };
        let (state_commitment_proof, _) = state_commitment_proof(&certified, certified_block_hash)
            .expect("state and outcome commitments cover the same blocks");

        let commitment_height = self.get_block_header(certified_block_hash)?.height();
        let anchor_block_lite = LightClientBlockLiteView::from(BlockHeader::clone(&anchor_header));
        let anchor_block_proof = self.compute_past_block_proof_in_merkle_tree_of_later_block(
            &anchor_hash,
            head_block_hash,
        )?;
        Ok(Some(SpiceCommitmentProof {
            anchor_block_lite,
            anchor_block_proof,
            commitment_height,
            state_commitment_proof,
            outcome_commitment_proof,
        }))
    }
}
