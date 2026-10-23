use super::DataId;
use borsh::{BorshDeserialize, BorshSerialize};
use near_primitives::hash::hash;
use near_primitives::merkle::{MerklePath, verify_path_with_index};
use near_primitives::reed_solomon::{
    InsertPartResult, ReedSolomonEncoder, ReedSolomonEncoderDeserialize,
    ReedSolomonEncoderSerialize, ReedSolomonPartsTracker, reed_solomon_part_length,
};
use near_primitives::sharding::ReceiptProof;
use near_primitives::spice::partial_data::SpiceDataCommitment;
use near_primitives::spice::state_witness::SpiceChunkStateWitness;
use near_primitives::types::{AccountId, BlockHeight, EpochId};
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

/// A producer of an item.
#[derive(Debug)]
pub(super) struct ItemProducer {
    pub(super) account_id: AccountId,
    /// The commitment of its first verified part; parts under any other are refused.
    pub(super) commitment: Option<SpiceDataCommitment>,
}

/// An item produced by others and fetched by us.
pub(crate) struct FetchItem {
    /// Height of the item's block.
    pub(crate) height: BlockHeight,
    /// Epoch of the item's block.
    pub(super) epoch_id: EpochId,
    /// Epoch after the block's. Producers may sign with their keys in either.
    pub(super) next_epoch_id: EpochId,
    /// The item's producers, in the parts' encoding order.
    pub(super) producers: Vec<ItemProducer>,
    /// Tracks the state of commitments.
    pub(super) commitments: HashMap<SpiceDataCommitment, CommitmentState>,
}

#[derive(Debug)]
pub(super) enum CommitmentState {
    /// Collecting parts toward a decode.
    Tracking(CodedTracker),
    /// Decoded, to data or to garbage. Nothing more can arrive under it.
    Settled,
}

impl FetchItem {
    pub(crate) fn new(
        height: BlockHeight,
        epoch_id: EpochId,
        next_epoch_id: EpochId,
        producers: Vec<AccountId>,
    ) -> Self {
        let producers = producers
            .into_iter()
            .map(|account_id| ItemProducer { account_id, commitment: None })
            .collect();
        Self { height, epoch_id, next_epoch_id, producers, commitments: HashMap::new() }
    }

    /// Each commitment still collecting parts, with the producers bound to it and its tracker.
    pub(super) fn live_pools(&self) -> impl Iterator<Item = (Vec<&AccountId>, &CodedTracker)> {
        self.commitments.iter().filter_map(|(commitment, state)| match state {
            CommitmentState::Tracking(tracker) => {
                Some((self.contributors(commitment).into_iter().collect(), tracker))
            }
            CommitmentState::Settled => None,
        })
    }

    /// Producers not bound to any commitment, each with its own ordinal.
    pub(super) fn unbound_producers(&self) -> impl Iterator<Item = (u64, &AccountId)> {
        self.producers
            .iter()
            .enumerate()
            .filter(|(_, slot)| slot.commitment.is_none())
            .map(|(ordinal, slot)| (ordinal as u64, &slot.account_id))
    }

    /// Senders contributed to `commitment`.
    pub(super) fn contributors(&self, commitment: &SpiceDataCommitment) -> HashSet<&AccountId> {
        self.producers
            .iter()
            .filter(|slot| slot.commitment.as_ref() == Some(commitment))
            .map(|slot| &slot.account_id)
            .collect()
    }

    /// Whether `commitment` was decoded, to data or to garbage.
    pub(super) fn is_settled(&self, commitment: &SpiceDataCommitment) -> bool {
        matches!(self.commitments.get(commitment), Some(CommitmentState::Settled))
    }

    /// Index of the producer with `account_id`; `None` if it is not a producer of the item.
    pub(super) fn producer_index(&self, account_id: &AccountId) -> Option<usize> {
        self.producers.iter().position(|slot| &slot.account_id == account_id)
    }

    /// Inserts a verified part sent by the producer at `producer_index`, under its commitment.
    /// Any claim binds the producer to the commitment; a claim of the wrong width or part length
    /// settles the commitment as garbage. A decoding insert settles it in the same call.
    pub(crate) fn insert_part(
        &mut self,
        encoder: &Arc<ReedSolomonEncoder>,
        id: &DataId,
        producer_index: usize,
        verified: VerifiedCodedPart,
    ) -> PartInsertResult {
        let VerifiedCodedPart { commitment, total_parts, ordinal, part } = verified;
        let bound = &mut self.producers[producer_index].commitment;
        if bound.as_ref().is_some_and(|bound| bound != &commitment) {
            return PartInsertResult::ConflictingCommitment;
        }
        *bound = Some(commitment.clone());

        if self.is_settled(&commitment) {
            return PartInsertResult::AlreadySettled;
        }
        // TODO(spice-data-distribution): cap encoded_length against the max payload size;
        // the only cap today is MAX_ENCODED_LENGTH inside the decode.
        let encoded_length =
            usize::try_from(commitment.encoded_length).expect("encoded length should fit in usize");
        // equal widths plus a verified proof imply the ordinal is in range
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
            | PartInsertResult::AlreadySettled
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

    /// Ordinals not held yet.
    pub(super) fn missing_ordinals(&self) -> Vec<u64> {
        (0..self.parts.total_parts())
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
    /// The commitment was already settled, to data or to garbage; the part was not needed.
    AlreadySettled,
    /// The commitment decoded to this data, which matches the committed hash and the id.
    Decoded(SpiceData),
    /// The commitment settled without data.
    Garbage(AssembledDataError),
    /// The sender already sent parts under another commitment. Part ignored.
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
