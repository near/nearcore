use crate::epoch_block_info::BlockInfo;
use crate::epoch_info::EpochInfo;
use crate::merkle::PartialMerkleTree;
use crate::types::validator_stake::ValidatorStake;
use crate::utils::compression::CompressedData;
use crate::version::BLOCK_HEADER_V3_PROTOCOL_VERSION;
use crate::{block_header::BlockHeader, merkle::MerklePathItem};
use borsh::{BorshDeserialize, BorshSerialize};
use bytesize::ByteSize;
use near_crypto::Signature;
use near_primitives_core::types::ProtocolVersion;
use near_schema_checker_lib::ProtocolSchema;
use std::fmt::Debug;

/// Versioned enum for EpochSyncProof. Because this structure is sent over the network and also
/// persisted on disk, we want it to be deserializable even if the structure changes in the future.
/// There's no guarantee that there's any compatibility (most likely not), but being able to
/// deserialize it will allow us to modify the code in the future to properly perform any upgrades.
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub enum EpochSyncProof {
    V1(EpochSyncProofV1),
}

impl EpochSyncProof {
    /// Right now this would never fail, but in the future this API can be changed.
    pub fn into_v1(self) -> EpochSyncProofV1 {
        match self {
            EpochSyncProof::V1(v1) => v1,
        }
    }

    /// Right now this would never fail, but in the future this API can be changed.
    pub fn as_v1(&self) -> &EpochSyncProofV1 {
        match self {
            EpochSyncProof::V1(v1) => v1,
        }
    }
}

/// Proof that the blockchain history had progressed from the genesis to the
/// current epoch indicated in the proof.
///
/// A side note to better understand the fields in this proof: the last three blocks of any
/// epoch are guaranteed to have consecutive heights:
///   - H: The last final block of the epoch
///   - H + 1: The second last block of the epoch
///   - H + 2: The last block of the epoch
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct EpochSyncProofV1 {
    /// All the relevant epochs, starting from the second epoch after genesis (i.e. genesis is
    /// epoch EpochId::default, and then the next epoch after genesis is fully determined by
    /// the genesis; after that would be the first epoch included here), to and including the
    /// current epoch, in that order.
    ///
    /// The first entry in this list is proven against the genesis. Then, each entry is proven
    /// against the previous entry, thereby validating the entire list by induction.
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

/// For epoch sync we need to keep track of when the block producer hash format changed.
/// This is required for the correct calculation of the proof. See [`use_versioned_bp_hash_format`]
pub fn should_use_versioned_bp_hash_format(protocol_version: ProtocolVersion) -> bool {
    protocol_version >= BLOCK_HEADER_V3_PROTOCOL_VERSION
}

/// Data needed for each epoch covered in the epoch sync proof.
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct EpochSyncProofEpochData {
    /// The block producers and their stake, for this epoch. This is verified
    /// against the `next_bp_hash` of the `last_final_block_header` of the epoch before this.
    pub block_producers: Vec<ValidatorStake>,
    /// Whether the block producers are encoded in the versioned format for computing the bp_hash.
    /// This is verified together with `block_producers` against `next_bp_hash` of the
    /// `last_final_block_header` of the epoch before this. This field does not need to be trusted,
    /// because given any valid bp hash, there is only one possible value of this boolean that
    /// could pass verification, because the two encodings do not collide.
    ///
    /// Specifically, the reason that the two encodings do not collide is:
    ///  - The old version is the borsh encoding of a vector of `ValidatorStakeV1`, meaning the
    ///    first few bytes are:
    ///       | vec length (4 bytes) | account id len (4 bytes) | account id (variable length) | ...
    ///  - The old version is the borsh encoding of a vector of `ValidatorStake` which is an enum,
    ///    so the first few bytes are:
    ///       | vec length (4 bytes) | enum tag (1 byte) | ...
    ///  - Right now, the enum tag is always 0 because there's only ValidatorStakeV2, so
    ///     - The only way for these two to collide is if the account id length has a lowest byte of
    ///       zero, which is impossible because a valid AccountId has length between 2 and 64.
    ///  - In the future, the enum tag can be larger. However, assuming that the first element of
    ///    ValidatorStakeVx is always AccountId, then the next 4 bytes after enum tag is the length
    ///    of the account id, but to have a collision the first 3 bytes of that must be zeros, which
    ///    is again impossible.
    pub use_versioned_bp_hash_format: bool,
    /// The last final block header of the epoch (i.e. third last block of the epoch).
    /// This is verified against `this_epoch_endorsements_for_last_final_block`.
    pub last_final_block_header: BlockHeader,
    /// Endorsements for the last final block, which comes from the second last block of the epoch.
    /// Since it has a consecutive height from the final block, the approvals included in it are
    /// guaranteed to be endorsements which directly endorse the final block.
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
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct EpochSyncProofLastEpochData {
    /// The following six fields are used to derive the epoch_sync_data_hash included in the
    /// first block of the epoch right after (assuming it is a BlockHeaderV3 or newer). This
    /// is used to verify all the data we need around the epoch sync boundary, against
    /// `first_block_header_in_epoch` in `current_epoch`.
    pub epoch_info: EpochInfo,
    pub next_epoch_info: EpochInfo,
    pub next_next_epoch_info: EpochInfo,
    pub first_block_in_epoch: BlockInfo,
    pub last_block_in_epoch: BlockInfo,
    pub second_last_block_in_epoch: BlockInfo,
}

/// Data needed to initialize the current epoch we're syncing to.
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct EpochSyncProofCurrentEpochData {
    /// The first block header that begins the epoch. It is proven using a merkle proof
    /// against `last_final_block_header` in the last entry of `all_epochs`. Note that we cannot
    /// use signatures to prove this like for the final block, because the first block header may
    /// not have a consecutive height afterwards.
    pub first_block_header_in_epoch: BlockHeader,
    /// The last two block headers are also needed for various purposes after epoch sync.
    /// They are proven against the `first_block_header_in_epoch`.
    pub last_block_header_in_prev_epoch: BlockHeader,
    pub second_last_block_header_in_prev_epoch: BlockHeader,
    /// Used to prove `first_block_header_in_epoch` against the `last_final_block_header` of
    /// the last entry of `all_epochs`.
    pub merkle_proof_for_first_block: Vec<MerklePathItem>,
    /// Partial merkle tree for the first block in this epoch. It is needed to construct future
    /// partial merkle trees for any blocks that follow.
    /// This is proven against the merkle root and block ordinal in `first_block_header_in_epoch`
    /// (as there is only one unique correct partial merkle tree for a specific root and a specific
    /// block ordinal).
    pub partial_merkle_tree_for_first_block: PartialMerkleTree,
}
