use crate::block_header::BlockHeader;
use crate::epoch_block_info::BlockInfo;
use crate::epoch_info::EpochInfo;
use crate::merkle::PartialMerkleTree;
use crate::types::validator_stake::ValidatorStake;
use crate::utils::compression::CompressedData;
use borsh::{BorshDeserialize, BorshSerialize};
use bytesize::ByteSize;
use near_crypto::Signature;
use std::fmt::Debug;

/// Proof that the blockchain history had progressed from the genesis (not included here) to the
/// current epoch indicated in the proof.
///
/// A side note to better understand the fields in this proof: the last three blocks of any
/// epoch are guaranteed to have consecutive heights:
///   - H: The last final block of the epoch
///   - H + 1: The second last block of the epoch
///   - H + 2: The last block of the epoch
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct EpochSyncProof {
    /// All the past epochs, starting from the first epoch after genesis, to
    /// the last epoch before the current epoch.
    pub past_epochs: Vec<EpochSyncProofPastEpochData>,
    /// Some extra data for the last epoch before the current epoch.
    pub last_epoch: EpochSyncProofLastEpochData,
    /// Extra information to initialize the current epoch we're syncing to.
    pub current_epoch: EpochSyncProofCurrentEpochData,
}

const MAX_UNCOMPRESSED_EPOCH_SYNC_PROOF_SIZE: u64 = ByteSize::mib(500).0;
const EPOCH_SYNC_COMPRESSION_LEVEL: i32 = 3;

#[derive(
    Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, derive_more::From, derive_more::AsRef,
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

/// Data needed for each epoch in the past.
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct EpochSyncProofPastEpochData {
    /// The block producers and their stake, for this epoch. This is verified
    /// against the `next_bp_hash` of the `last_final_block_header` of the epoch before this.
    pub block_producers: Vec<ValidatorStake>,
    /// The last final block header of the epoch (i.e. third last block of the epoch).
    /// This is verified against the `approvals_for_last_final_block`.
    pub last_final_block_header: BlockHeader,
    /// Approvals for the last final block, which comes from the second last block of the epoch.
    /// Since it has a consecutive height from the final block, the approvals are guaranteed to
    /// be endorsements which directly endorse the final block.
    pub approvals_for_last_final_block: Vec<Option<Box<Signature>>>,
}

/// Data needed to initialize the epoch sync boundary.
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct EpochSyncProofLastEpochData {
    /// The following six fields are used to derive the epoch_sync_data_hash included in any
    /// BlockHeaderV3. This is used to verify all the data we need around the epoch sync
    /// boundary.
    pub epoch_info: EpochInfo,
    pub next_epoch_info: EpochInfo,
    pub next_next_epoch_info: EpochInfo,
    pub first_block_in_epoch: BlockInfo,
    pub last_block_in_epoch: BlockInfo,
    pub second_last_block_in_epoch: BlockInfo,

    /// Any final block header in the next epoch (i.e. current epoch for the whole proof).
    /// This is used to provide the `epoch_sync_data_hash` mentioned above.
    pub final_block_header_in_next_epoch: BlockHeader,
    /// Approvals for `final_block_header_in_next_epoch`, used to prove that block header
    /// is valid.
    pub approvals_for_final_block_in_next_epoch: Vec<Option<Box<Signature>>>,
}

/// Data needed to initialize the current epoch we're syncing to.
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct EpochSyncProofCurrentEpochData {
    /// The first block header that begins the epoch. It is proven using a merkle proof
    /// against the final block provided in the LastEpochData. Note that we cannot use signatures
    /// to prove this like the other cases, because the first block header may not have a
    /// consecutive height afterwards.
    pub first_block_header_in_epoch: BlockHeader,
    // TODO(#11932): can this be proven or derived?
    pub first_block_info_in_epoch: BlockInfo,
    // The last two block headers are also needed for various purposes after epoch sync.
    // TODO(#11931): do we really need these?
    pub last_block_header_in_prev_epoch: BlockHeader,
    pub second_last_block_header_in_prev_epoch: BlockHeader,
    // TODO(#11932): I'm not sure if this can be used to prove the block against the merkle root
    // included in the final block in this next epoch (included in LastEpochData). We may need to
    // include another merkle proof.
    pub merkle_proof_for_first_block: PartialMerkleTree,
}
