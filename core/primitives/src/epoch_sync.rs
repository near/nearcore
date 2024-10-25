use crate::epoch_block_info::BlockInfo;
use crate::epoch_info::EpochInfo;
use crate::merkle::PartialMerkleTree;
use crate::types::validator_stake::ValidatorStake;
use crate::utils::compression::CompressedData;
use crate::{block_header::BlockHeader, merkle::MerklePathItem};
use borsh::{BorshDeserialize, BorshSerialize};
use bytesize::ByteSize;
use near_crypto::Signature;
use near_schema_checker_lib::ProtocolSchema;
use std::fmt::Debug;

/// Proof that the blockchain history had progressed from the genesis to the
/// current epoch indicated in the proof.
///
/// A side note to better understand the fields in this proof: the last three blocks of any
/// epoch are guaranteed to have consecutive heights:
///   - H: The last final block of the epoch
///   - H + 1: The second last block of the epoch
///   - H + 2: The last block of the epoch
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct EpochSyncProof {
    /// All the relevant epochs, starting from the second epoch after genesis (i.e. genesis is
    /// epoch EpochId::default, and then the next epoch after genesis is fully determined by
    /// the genesis; after that would be the first epoch included here), to and including the
    /// current epoch, in that order.
    pub all_epochs: Vec<EpochSyncProofEpochData>,
    /// Some extra data for the last epoch before the current epoch.
    pub last_epoch: EpochSyncProofLastEpochData,
    /// Extra information to initialize the current epoch we're syncing to.
    pub current_epoch: EpochSyncProofCurrentEpochData,
}

const MAX_UNCOMPRESSED_EPOCH_SYNC_PROOF_SIZE: u64 = ByteSize::mib(500).0;
const EPOCH_SYNC_COMPRESSION_LEVEL: i32 = 3;

#[derive(
    Clone,
    PartialEq,
    Eq,
    BorshSerialize,
    BorshDeserialize,
    derive_more::From,
    derive_more::AsRef,
    ProtocolSchema,
)]
pub struct CompressedEpochSyncProof(Box<[u8]>);
impl
    CompressedData<
        EpochSyncProof,
        MAX_UNCOMPRESSED_EPOCH_SYNC_PROOF_SIZE,
        EPOCH_SYNC_COMPRESSION_LEVEL,
    > for CompressedEpochSyncProof
{
}

impl Debug for CompressedEpochSyncProof {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompressedEpochSyncProof")
            .field("len", &self.0.len())
            .field("proof", &self.decode())
            .finish()
    }
}

/// Data needed for each epoch covered in the epoch sync proof.
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct EpochSyncProofEpochData {
    /// The block producers and their stake, for this epoch. This is verified
    /// against the `next_bp_hash` of the `last_final_block_header` of the epoch before this.
    pub block_producers: Vec<ValidatorStake>,
    /// Whether the block producers are encoded in the old format for computing the bp_hash.
    /// The encodings between old and new format do not collide, so this field does not need
    /// to be proven.
    pub use_old_bp_hash_format: bool,
    /// The last final block header of the epoch (i.e. third last block of the epoch).
    /// This is verified against `this_epoch_endorsements_for_last_final_block`.
    pub last_final_block_header: BlockHeader,
    /// Endorsements for the last final block, which comes from the second last block of the epoch.
    /// Since it has a consecutive height from the final block, the approvals included in it are
    /// are guaranteed to be endorsements which directly endorse the final block.
    ///
    /// Note an important subtlety: This is *not* the complete set of approvals included in the
    /// second last block. This is a subset of them that correspond to only this epoch's block
    /// producers, as the next epoch's block producers are also required to sign this block. We
    /// do not include the next epoch's block producers' endorsements here, as we ultimately
    /// would not have a reliable way to verify the next epoch's block producers (it would result in
    /// circular reasoning since the next epoch's block producers are verified against this epoch's
    /// final block), so even if we included them we would not be able to use them meaningfully.
    pub this_epoch_endorsements_for_last_final_block: Vec<Option<Box<Signature>>>,
}

/// Data needed to initialize the epoch sync boundary.
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct EpochSyncProofLastEpochData {
    /// The following six fields are used to derive the epoch_sync_data_hash included in any
    /// BlockHeaderV3. This is used to verify all the data we need around the epoch sync
    /// boundary, against `last_final_block_header` in the second last epoch data.
    pub epoch_info: EpochInfo,
    pub next_epoch_info: EpochInfo,
    pub next_next_epoch_info: EpochInfo,
    pub first_block_in_epoch: BlockInfo,
    pub last_block_in_epoch: BlockInfo,
    pub second_last_block_in_epoch: BlockInfo,
}

/// Data needed to initialize the current epoch we're syncing to.
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct EpochSyncProofCurrentEpochData {
    /// The first block header that begins the epoch. It is proven using a merkle proof
    /// against `last_final_block_header` in the current epoch data. Note that we cannot
    /// use signatures to prove this like for the final block, because the first block
    /// header may not have a consecutive height afterwards.
    pub first_block_header_in_epoch: BlockHeader,
    // The last two block headers are also needed for various purposes after epoch sync.
    // TODO(#11931): do we really need these?
    // TODO(#12259) These 2 fields are currently unverified.
    pub last_block_header_in_prev_epoch: BlockHeader,
    pub second_last_block_header_in_prev_epoch: BlockHeader,
    // Used to prove the block against the merkle root
    // included in the final block in this next epoch (included in LastEpochData).
    // TODO(#12255) This field is currently ungenerated and unverified.
    pub merkle_proof_for_first_block: Vec<MerklePathItem>,
    // Partial merkle tree for the first block in this next epoch.
    // It is necessary and sufficient to calculate next blocks merkle roots.
    // It is proven using `first_block_header_in_epoch`.
    pub partial_merkle_tree_for_first_block: PartialMerkleTree,
}
