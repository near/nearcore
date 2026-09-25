use super::DataId;
use borsh::{BorshDeserialize, BorshSerialize};
use near_async::time::Instant;
use near_primitives::hash::hash;
use near_primitives::merkle::{MerklePath, verify_path_with_index};
use near_primitives::reed_solomon::{
    InsertPartResult, ReedSolomonEncoder, ReedSolomonEncoderDeserialize,
    ReedSolomonEncoderSerialize, ReedSolomonPartsTracker, reed_solomon_part_length,
};
use near_primitives::sharding::ReceiptProof;
use near_primitives::spice::partial_data::SpiceDataCommitment;
use near_primitives::spice::state_witness::SpiceChunkStateWitness;
use near_primitives::types::{AccountId, BlockHeight};
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Arc;

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
    /// The producers of the item's data, one per part ordinal.
    pub(crate) sources: Vec<AccountId>,
    /// Whether a decode has been handed to the consumer. Only then can the store hold the
    /// item's data.
    pub(crate) delivered: bool,
    /// Tracks the state of commitments.
    pub(super) commitments: HashMap<SpiceDataCommitment, CommitmentState>,
    /// Each producer that sent a verifying part or was asked for one.
    pub(super) producers: HashMap<AccountId, ProducerState>,
}

/// One producer's part in fetching this item.
#[derive(Debug, Default)]
pub(super) struct ProducerState {
    /// The commitment this producer backed, once one of its parts verified.
    pub(super) commitment: Option<SpiceDataCommitment>,
    /// When this node asked it, while that request is unanswered.
    pub(super) requested_at: Option<Instant>,
}

#[derive(Debug)]
pub(super) enum CommitmentState {
    /// Collecting parts toward a decode.
    Tracking(CodedTracker),
    /// Decoded, to data or to garbage. Nothing more can arrive under it.
    Settled,
}

impl FetchItem {
    pub(crate) fn new(height: BlockHeight, sources: Vec<AccountId>) -> Self {
        Self {
            height,
            sources,
            delivered: false,
            commitments: HashMap::new(),
            producers: HashMap::new(),
        }
    }

    /// The tracker still collecting under `commitment`, if any.
    pub(super) fn tracker_mut(
        &mut self,
        commitment: &SpiceDataCommitment,
    ) -> Option<&mut CodedTracker> {
        match self.commitments.get_mut(commitment) {
            Some(CommitmentState::Tracking(tracker)) => Some(tracker),
            Some(CommitmentState::Settled) | None => None,
        }
    }

    /// Senders contributed to `commitment`.
    pub(super) fn contributors(&self, commitment: &SpiceDataCommitment) -> HashSet<&AccountId> {
        self.producers
            .iter()
            .filter(|(_, state)| state.commitment.as_ref() == Some(commitment))
            .map(|(producer, _)| producer)
            .collect()
    }

    /// Inserts a verified part under its commitment. Any claim binds the sender to the
    /// commitment; a claim of the wrong width or part length settles the commitment as
    /// garbage. A decoding insert settles it in the same call.
    pub(crate) fn insert_part(
        &mut self,
        encoder: &Arc<ReedSolomonEncoder>,
        id: &DataId,
        sender: &AccountId,
        verified: VerifiedCodedPart,
    ) -> PartInsertResult {
        let VerifiedCodedPart { commitment, total_parts, ordinal, part } = verified;
        let producer = self.producers.entry(sender.clone()).or_default();
        if producer.commitment.as_ref().is_some_and(|bound| bound != &commitment) {
            return PartInsertResult::ConflictingCommitment;
        }
        producer.commitment = Some(commitment.clone());

        if matches!(self.commitments.get(&commitment), Some(CommitmentState::Settled)) {
            return PartInsertResult::Settled;
        }
        // TODO(spice-data-distribution): cap encoded_length against the max payload size;
        // the only cap today is MAX_ENCODED_LENGTH inside the decode.
        let encoded_length =
            usize::try_from(commitment.encoded_length).expect("encoded length should fit in usize");
        // No part verifies under a well-formed commitment of another width or part length,
        // so such a claim is garbage. Equal widths plus a verified proof imply the ordinal
        // is in range.
        let result = if total_parts != encoder.total_parts() {
            PartInsertResult::Garbage(AssembledDataError::WrongTotalParts)
        } else if part.len() != reed_solomon_part_length(encoded_length, encoder.data_parts()) {
            PartInsertResult::Garbage(AssembledDataError::WrongPartLength)
        } else {
            let state = self.commitments.entry(commitment.clone()).or_insert_with(|| {
                CommitmentState::Tracking(CodedTracker::new(encoder.clone(), encoded_length))
            });
            let CommitmentState::Tracking(tracker) = state else {
                unreachable!("a settled commitment was returned above");
            };
            tracker.insert_part(id, &commitment, ordinal, part)
        };
        match &result {
            PartInsertResult::Decoded(_) | PartInsertResult::Garbage(_) => {
                self.commitments.insert(commitment.clone(), CommitmentState::Settled);
            }
            PartInsertResult::Accepted
            | PartInsertResult::Duplicate
            | PartInsertResult::Settled
            | PartInsertResult::ConflictingCommitment => {}
        }
        if let PartInsertResult::Garbage(error) = &result {
            let contributors = self.contributors(&commitment);
            tracing::debug!(target: "spice_data_distribution", ?id, ?error, ?contributors, "commitment settled as garbage");
        }
        result
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
    ) -> Option<Self> {
        if !verify_path_with_index(
            commitment.root,
            merkle_proof,
            &part,
            ordinal,
            total_parts as u64,
        ) {
            return None;
        }
        // the index check above bounds the ordinal by `total_parts`, a usize
        let ordinal = usize::try_from(ordinal).expect("verified ordinal fits in usize");
        Some(Self { commitment: commitment.clone(), total_parts, ordinal, part })
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
    total_parts: usize,
    /// Rotates the pool members asked for missing ordinals.
    /// Position in the pool's rotation; starts at random so requesters spread over the
    /// pool, and moves past each member asked.
    pub(super) rotation_cursor: u64,
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
        Self {
            total_parts: encoder.total_parts(),
            parts: ReedSolomonPartsTracker::new(encoder, encoded_length),
            rotation_cursor: rand::random(),
        }
    }

    /// Ordinals not held yet.
    pub(super) fn missing_ordinals(&self) -> Vec<u64> {
        (0..self.total_parts)
            .filter(|ordinal| !self.parts.has_part(*ordinal))
            .map(|ordinal| ordinal as u64)
            .collect()
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
    /// The commitment settled without data.
    Garbage(AssembledDataError),
    /// The sender already backed another commitment. Part ignored.
    ConflictingCommitment,
}

/// Why a commitment settled without data
#[derive(Debug, thiserror::Error)]
pub(crate) enum AssembledDataError {
    #[error("part was verified against a different total parts count")]
    WrongTotalParts,
    #[error("part length does not match the commitment's encoded length")]
    WrongPartLength,
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
