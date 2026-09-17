use super::DataManagerError;
use borsh::{BorshDeserialize, BorshSerialize};
use near_primitives::hash::{CryptoHash, hash};
use near_primitives::merkle::{MerklePath, verify_path_with_index};
use near_primitives::reed_solomon::{
    InsertPartResult, ReedSolomonEncoder, ReedSolomonEncoderDeserialize,
    ReedSolomonEncoderSerialize, ReedSolomonPartsTracker, reed_solomon_part_length,
};
use near_primitives::sharding::ReceiptProof;
use near_primitives::spice::partial_data::SpiceDataCommitment;
use near_primitives::spice::state_witness::SpiceChunkStateWitness;
use near_primitives::types::{AccountId, BlockHeight, ShardId, SpiceChunkId};
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Arc;

/// Identity of one piece of distributed data the engine tracks.
// TODO(spice-data-distribution): witnesses and contract code move here when their
// paths switch to the engine. At that moment it can be replaced with `SpiceDataIdentifier`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DataId {
    /// `source` is the chunk whose execution produced the receipts; `to_shard` is their
    /// destination. Produced by `source`'s producers, needed by next-block producers of
    /// `to_shard`.
    ReceiptProof { source: SpiceChunkId, to_shard: ShardId },
}

impl DataId {
    pub fn receipt_proof(
        block_hash: CryptoHash,
        from_shard_id: ShardId,
        to_shard_id: ShardId,
    ) -> Self {
        Self::ReceiptProof {
            source: SpiceChunkId { block_hash, shard_id: from_shard_id },
            to_shard: to_shard_id,
        }
    }

    /// Checks that decoded data is the data this id names.
    pub(crate) fn verify_data(&self, data: &SpiceData) -> Result<(), AssembledDataError> {
        let DataId::ReceiptProof { source, to_shard } = self;
        let SpiceData::ReceiptProof(proof) = data else {
            return Err(AssembledDataError::IdAndDataMismatch);
        };
        if &proof.1.to_shard_id != to_shard {
            return Err(AssembledDataError::InvalidToShardId);
        }
        if proof.1.from_shard_id != source.shard_id {
            return Err(AssembledDataError::InvalidFromShardId);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, BorshSerialize, BorshDeserialize)]
pub(crate) enum SpiceData {
    ReceiptProof(ReceiptProof),
    StateWitness(Box<SpiceChunkStateWitness>),
}

impl ReedSolomonEncoderSerialize for SpiceData {}

impl ReedSolomonEncoderDeserialize for SpiceData {}

/// An item produced by others and fetched by us.
pub(crate) struct FetchItem {
    /// Height of the item's block.
    pub(crate) height: BlockHeight,
    /// Tracks the state of commitments.
    pub(super) commitments: HashMap<SpiceDataCommitment, CommitmentState>,
    /// Maps each sender's `AccountId` to the commitment it contributed to.
    pub(super) commitment_by_contributor: HashMap<AccountId, SpiceDataCommitment>,
}

/// What the engine holds for one claimed commitment of an item.
#[derive(Debug)]
pub(super) enum CommitmentState {
    /// Collecting parts toward a decode.
    Tracking(CodedTracker),
    /// Decoded, to data or to garbage. Nothing more can arrive under it.
    Settled,
}

impl FetchItem {
    pub(crate) fn new(height: BlockHeight) -> Self {
        Self { height, commitments: HashMap::new(), commitment_by_contributor: HashMap::new() }
    }

    /// Senders contributed to `commitment`.
    pub(super) fn contributors(&self, commitment: &SpiceDataCommitment) -> HashSet<&AccountId> {
        self.commitment_by_contributor
            .iter()
            .filter(|(_, bound)| *bound == commitment)
            .map(|(contributor, _)| contributor)
            .collect()
    }

    /// Inserts a verified part under its commitment. Any claim binds the sender to the
    /// commitment; a claim of the wrong width or part length settles the commitment as
    /// bogus. A decoding insert settles it in the same call. Errors are attributable to
    /// the sender.
    pub(crate) fn insert_part(
        &mut self,
        encoder: &Arc<ReedSolomonEncoder>,
        id: &DataId,
        sender: &AccountId,
        verified: VerifiedCodedPart,
    ) -> Result<PartInsertResult, DataManagerError> {
        let VerifiedCodedPart { commitment, total_parts, ordinal, part } = verified;
        if self.commitment_by_contributor.get(sender).is_some_and(|bound| bound != &commitment) {
            return Err(DataManagerError::ConflictingCommitment);
        }
        self.commitment_by_contributor.insert(sender.clone(), commitment.clone());

        if matches!(self.commitments.get(&commitment), Some(CommitmentState::Settled)) {
            return Ok(PartInsertResult::Settled);
        }
        // TODO(spice-data-distribution): cap encoded_length against the max payload size;
        // the only cap today is MAX_ENCODED_LENGTH inside the decode.
        let encoded_length =
            usize::try_from(commitment.encoded_length).expect("encoded length should fit in usize");
        // equal widths plus a verified proof imply the ordinal is in range
        let malformed = if total_parts != encoder.total_parts() {
            Some(DataManagerError::WrongTotalParts)
        } else if part.len() != reed_solomon_part_length(encoded_length, encoder.data_parts()) {
            Some(DataManagerError::WrongPartLength)
        } else {
            None
        };
        if let Some(error) = malformed {
            // no part verifies under a well-formed commitment of another width or part
            // length, so the commitment is bogus: settled like a garbage decode
            self.commitments.insert(commitment, CommitmentState::Settled);
            return Err(error);
        }
        let state = self.commitments.entry(commitment.clone()).or_insert_with(|| {
            CommitmentState::Tracking(CodedTracker::new(encoder.clone(), encoded_length))
        });
        let CommitmentState::Tracking(tracker) = state else {
            unreachable!("a settled commitment was returned above");
        };
        let result = tracker.insert_part(id, &commitment, ordinal, part);
        match &result {
            PartInsertResult::Decoded(_) | PartInsertResult::Garbage(_) => {
                *state = CommitmentState::Settled;
            }
            PartInsertResult::Accepted
            | PartInsertResult::Duplicate
            | PartInsertResult::Settled => {}
        }
        if let PartInsertResult::Garbage(error) = &result {
            let contributors = self.contributors(&commitment);
            tracing::debug!(target: "spice_data_distribution", ?id, ?error, ?contributors, "commitment decoded to garbage");
        }
        Ok(result)
    }
}

/// A coded part whose merkle proof was verified against its commitment's root;
/// [`Self::verify`] is the only way to construct one.
#[derive(Debug)]
pub(crate) struct VerifiedCodedPart {
    commitment: SpiceDataCommitment,
    /// Leaf count of the tree the proof was verified against.
    total_parts: usize,
    ordinal: usize,
    part: Box<[u8]>,
}

impl VerifiedCodedPart {
    pub(crate) fn verify(
        commitment: &SpiceDataCommitment,
        total_parts: usize,
        ordinal: u64,
        part: Box<[u8]>,
        merkle_proof: &MerklePath,
    ) -> Result<Self, DataManagerError> {
        if !verify_path_with_index(
            commitment.root,
            merkle_proof,
            &part,
            ordinal,
            total_parts as u64,
        ) {
            return Err(DataManagerError::InvalidMerkleProof);
        }
        // the index check above bounds the ordinal by `total_parts`, a usize
        let ordinal = usize::try_from(ordinal).expect("verified ordinal fits in usize");
        Ok(Self { commitment: commitment.clone(), total_parts, ordinal, part })
    }

    // TODO(spice-data-distribution): these accessors only feed the old witness ingress
    // path; they go when witnesses move onto the engine (#16275).
    pub(crate) fn ordinal(&self) -> usize {
        self.ordinal
    }

    pub(crate) fn into_part(self) -> Box<[u8]> {
        self.part
    }
}

/// Accumulates parts toward decoding under one claimed commitment.
pub(crate) struct CodedTracker {
    parts: ReedSolomonPartsTracker<SpiceData>,
}

impl fmt::Debug for CodedTracker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CodedTracker")
            .field("parts_present", &self.parts.data_parts_present())
            .field("parts_required", &self.parts.data_parts_required())
            .field("encoded_length", &self.parts.encoded_length())
            .finish()
    }
}

impl CodedTracker {
    fn new(encoder: Arc<ReedSolomonEncoder>, encoded_length: usize) -> Self {
        Self { parts: ReedSolomonPartsTracker::new(encoder, encoded_length) }
    }

    /// Inserts a part; the decoding insert checks the data against `commitment`'s hash
    /// and `id`.
    fn insert_part(
        &mut self,
        id: &DataId,
        commitment: &SpiceDataCommitment,
        ordinal: usize,
        part: Box<[u8]>,
    ) -> PartInsertResult {
        match self.parts.insert_part(ordinal, part, None) {
            InsertPartResult::Accepted => PartInsertResult::Accepted,
            InsertPartResult::PartAlreadyAvailable => PartInsertResult::Duplicate,
            InsertPartResult::InvalidPartOrd => {
                unreachable!("verified ordinal is below the tracker's part count")
            }
            InsertPartResult::Decoded(result) => {
                let checked =
                    result.map_err(|_| AssembledDataError::Undecodable).and_then(|data| {
                        if hash(&borsh::to_vec(&data).unwrap()) != commitment.hash {
                            return Err(AssembledDataError::HashMismatch);
                        }
                        id.verify_data(&data)?;
                        Ok(data)
                    });
                match checked {
                    Ok(data) => PartInsertResult::Decoded(data),
                    Err(error) => PartInsertResult::Garbage(error),
                }
            }
        }
    }
}

#[must_use = "a Decoded carries the delivered data"]
#[derive(Debug)]
pub(crate) enum PartInsertResult {
    Accepted,
    Duplicate,
    /// The commitment was already decoded; the part was not needed.
    Settled,
    /// The commitment decoded to this data, which matches the committed hash and the id.
    Decoded(SpiceData),
    /// The commitment reached K parts but yielded no data matching its hash and id.
    Garbage(AssembledDataError),
}

/// Why decoded data was rejected.
#[derive(Debug, thiserror::Error)]
pub(crate) enum AssembledDataError {
    #[error("decoding assembled data failed")]
    Undecodable,
    #[error("decoded data does not match the committed hash")]
    HashMismatch,
    #[error("decoded data doesn't match id")]
    IdAndDataMismatch,
    #[error("decoded receipt proof to_shard_id is invalid")]
    InvalidToShardId,
    #[error("decoded receipt proof from_shard_id is invalid")]
    InvalidFromShardId,
}
