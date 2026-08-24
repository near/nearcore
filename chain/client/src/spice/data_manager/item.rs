use borsh::{BorshDeserialize, BorshSerialize};
use near_async::time::{Clock, Instant};
use near_primitives::hash::CryptoHash;
use near_primitives::reed_solomon::{
    InsertPartResult, ReedSolomonEncoder, ReedSolomonEncoderDeserialize,
    ReedSolomonEncoderSerialize, ReedSolomonPartsTracker, reed_solomon_part_length,
};
use near_primitives::sharding::ReceiptProof;
use near_primitives::spice::partial_data::{SpiceDataCommitment, SpiceDataIdentifier};
use near_primitives::spice::state_witness::SpiceChunkStateWitness;
use near_primitives::stateless_validation::contract_distribution::CodeHash;
use near_primitives::types::{AccountId, BlockHeight, ShardId, SpiceChunkId};
use std::collections::{HashMap, HashSet};
use std::mem::replace;
use std::sync::Arc;

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub(crate) enum SpiceData {
    ReceiptProof(ReceiptProof),
    StateWitness(Box<SpiceChunkStateWitness>),
}

impl ReedSolomonEncoderSerialize for SpiceData {}

impl ReedSolomonEncoderDeserialize for SpiceData {}

/// Unified identity for every kind handled by the fetch engine.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum DataId {
    /// Coded. The chunk it validates; produced by its producers, needed by its validators.
    Witness(SpiceChunkId),
    /// Coded. `source` produced the receipts; needed by next-block producers of `to_shard`.
    ReceiptProof { source: SpiceChunkId, to_shard: ShardId },
    /// A blob keyed by hash alone: one item per hash however many chunks need it; context
    /// lives on [`FetchItem::anchor`].
    ContractCode { code_hash: CodeHash },
}

impl DataId {
    /// Anchor block for coded kinds; `None` for contract code, whose anchor is on the item.
    pub(crate) fn block_hash(&self) -> Option<&CryptoHash> {
        match self {
            Self::Witness(chunk) => Some(&chunk.block_hash),
            Self::ReceiptProof { source, .. } => Some(&source.block_hash),
            Self::ContractCode { .. } => None,
        }
    }

    pub(crate) fn transfer_unit(&self) -> TransferUnit {
        match self {
            Self::Witness(_) | Self::ReceiptProof { .. } => TransferUnit::ErasureCoded,
            Self::ContractCode { .. } => TransferUnit::Blob,
        }
    }
}

impl From<SpiceDataIdentifier> for DataId {
    fn from(id: SpiceDataIdentifier) -> Self {
        match id {
            SpiceDataIdentifier::Witness { block_hash, shard_id } => {
                Self::Witness(SpiceChunkId { block_hash, shard_id })
            }
            SpiceDataIdentifier::ReceiptProof { block_hash, from_shard_id, to_shard_id } => {
                Self::ReceiptProof {
                    source: SpiceChunkId { block_hash, shard_id: from_shard_id },
                    to_shard: to_shard_id,
                }
            }
        }
    }
}

impl TryFrom<&DataId> for SpiceDataIdentifier {
    type Error = ();

    fn try_from(id: &DataId) -> Result<Self, Self::Error> {
        match id {
            DataId::Witness(chunk) => {
                Ok(Self::Witness { block_hash: chunk.block_hash, shard_id: chunk.shard_id })
            }
            DataId::ReceiptProof { source, to_shard } => Ok(Self::ReceiptProof {
                block_hash: source.block_hash,
                from_shard_id: source.shard_id,
                to_shard_id: *to_shard,
            }),
            DataId::ContractCode { .. } => Err(()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TransferUnit {
    ErasureCoded,
    Blob,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Lane {
    /// Consensus-critical: we are an assigned validator or next-block producer.
    Priority,
    /// RPC, state sync, or catch-up; never starves `Priority`.
    Background,
}

pub(crate) enum Item {
    /// Produced by others; we fetch it.
    Fetch(FetchItem),
    /// Produced by us; we serve it.
    Produce(ProduceItem),
}

impl Item {
    pub(crate) fn height(&self) -> BlockHeight {
        match self {
            Self::Fetch(item) => item.height,
            Self::Produce(item) => item.height,
        }
    }
}

pub(crate) struct ProduceItem {
    pub(crate) state: ProduceState,
    pub(crate) height: BlockHeight,
}

pub(crate) enum ProduceState {
    /// Assigned to produce it; execution not finished yet.
    Producing,
    /// Artifact in store. `codes` is the contract hashes the chunk accessed (witness items
    /// only) — the serve-side index for code pulls; `served` counts bytes served per
    /// requester.
    ReadyToServe { codes: HashSet<CodeHash>, served: HashMap<AccountId, u64> },
}

/// The fetch lifecycle. No terminal "have" state: done-ness is the durable artifact
/// checked by [`super::DataKind::is_done`]; removal is by expiry.
pub(crate) enum FetchState {
    /// Wanted, but no unit has arrived and pulling is not armed; waiting for the push.
    Need,
    /// At least one unit arrived, or pulling was armed speculatively.
    Collecting(Assembly),
    /// Assembled data handed to the consumer; parked until its verdict, so a re-pushed
    /// part cannot deliver twice. `residual` keeps the losing trackers.
    Delivered { attribution: DataAttribution, residual: Assembly },
    /// Consumer verified and persisted the artifact; only the attribution is kept, for
    /// late faults. Terminal until expiry.
    ProcessedLocally { attribution: DataAttribution },
}

/// Runs a state transition that needs ownership of the current state; the state `f`
/// returns is written back before this returns.
fn transition<T>(state: &mut FetchState, f: impl FnOnce(FetchState) -> (FetchState, T)) -> T {
    // The transient `Need` is never observable.
    let (next, result) = f(replace(state, FetchState::Need));
    *state = next;
    result
}

pub(crate) struct FetchItem {
    pub(crate) state: FetchState,
    pub(crate) lane: Lane,
    /// Captured at seed time; drives expiry. For contract code, the height of `anchor`'s
    /// block, re-synced when the anchor moves.
    pub(crate) height: BlockHeight,
    /// Contract code only (`None` for coded kinds): a chunk whose execution accessed the
    /// code. Resolves the source pool and is carried in pulls as the claimed chunk;
    /// `height` mirrors its block. Re-aimed as fresher interest arrives.
    pub(crate) anchor: Option<SpiceChunkId>,
    /// Commitments rejected for this item — a failed consumer verdict or a garbage
    /// decode. Parts under them are rejected on arrival.
    pub(crate) banned_commitments: HashSet<SpiceDataCommitment>,
    /// When the first unit arrived; `None` until then. Anchors the wait-for-push grace
    /// clock.
    pub(crate) first_unit_at: Option<Instant>,
}

impl FetchItem {
    pub(crate) fn waiting_for_push(
        lane: Lane,
        height: BlockHeight,
        anchor: Option<SpiceChunkId>,
    ) -> Self {
        Self {
            state: FetchState::Need,
            lane,
            height,
            anchor,
            banned_commitments: HashSet::new(),
            first_unit_at: None,
        }
    }

    pub(crate) fn collecting(
        assembly: Assembly,
        lane: Lane,
        height: BlockHeight,
        anchor: Option<SpiceChunkId>,
    ) -> Self {
        Self {
            state: FetchState::Collecting(assembly),
            lane,
            height,
            anchor,
            banned_commitments: HashSet::new(),
            first_unit_at: None,
        }
    }

    pub(crate) fn open(&mut self, assembly: Assembly) -> bool {
        if !matches!(self.state, FetchState::Need) {
            return false;
        }
        self.state = FetchState::Collecting(assembly);
        true
    }

    pub(crate) fn insert_verified_coded_part(
        &mut self,
        clock: &Clock,
        commitment: &SpiceDataCommitment,
        sender: &AccountId,
        ordinal: usize,
        part: Box<[u8]>,
    ) -> Result<PartInsertResult, AssemblyError> {
        if self.banned_commitments.contains(commitment) {
            return Err(AssemblyError::BannedCommitment);
        }
        let FetchState::Collecting(assembly) = &mut self.state else {
            return Err(AssemblyError::NotCollecting);
        };
        let result = assembly.insert_verified_coded_part(commitment, sender, ordinal, part)?;
        if matches!(result, PartInsertResult::Garbage { .. }) {
            self.banned_commitments.insert(commitment.clone());
            if !assembly.has_parts() {
                self.first_unit_at = None;
            }
        } else if matches!(result, PartInsertResult::Accepted | PartInsertResult::Complete(_))
            && self.first_unit_at.is_none()
        {
            self.first_unit_at = Some(clock.now());
        }
        Ok(result)
    }

    /// Hands the decoded data over, keeping the losing trackers as the residual. A
    /// completion that fails `DataKind::verify_assembled` also goes through here,
    /// immediately followed by [`Self::mark_failed`].
    pub(crate) fn mark_delivered(
        &mut self,
        completed: CompletedCodedData,
    ) -> Result<SpiceData, AssemblyError> {
        transition(&mut self.state, |state| match state {
            FetchState::Collecting(mut assembly) => {
                match assembly.take_attribution(&completed.commitment) {
                    Ok(attribution) => {
                        let delivered = FetchState::Delivered { attribution, residual: assembly };
                        (delivered, Ok(completed.data))
                    }
                    Err(error) => (FetchState::Collecting(assembly), Err(error)),
                }
            }
            state => (state, Err(AssemblyError::NotCollecting)),
        })
    }

    pub(crate) fn mark_verified(&mut self) -> Result<(), AssemblyError> {
        transition(&mut self.state, |state| match state {
            FetchState::Delivered { attribution, .. } => {
                (FetchState::ProcessedLocally { attribution }, Ok(()))
            }
            state => (state, Err(AssemblyError::NotDelivered)),
        })
    }

    pub(crate) fn mark_failed(&mut self, clock: &Clock) -> Result<Vec<AccountId>, AssemblyError> {
        transition(&mut self.state, |state| match state {
            FetchState::Delivered { attribution, residual } => {
                let contributors = attribution.contributors();
                self.banned_commitments.insert(attribution.winning);
                // the wait for the verdict does not count toward the pull timer
                self.first_unit_at = residual.has_parts().then(|| clock.now());
                (FetchState::Collecting(residual), Ok(contributors))
            }
            state => (state, Err(AssemblyError::NotDelivered)),
        })
    }
}

pub(crate) enum Assembly {
    /// One tracker per commitment; a sender may back only one, which bounds the trackers.
    Coded { encoder: Arc<ReedSolomonEncoder>, trackers: HashMap<SpiceDataCommitment, CodedTracker> },
    /// Nothing accumulates: the expected hash is the `DataId`; a matching response
    /// delivers on arrival.
    Blob,
}

impl Assembly {
    pub(crate) fn coded(encoder: Arc<ReedSolomonEncoder>) -> Self {
        Self::Coded { encoder, trackers: HashMap::new() }
    }

    /// A returned `Complete` must be resolved (delivered or failed) before the next
    /// insert; a completed tracker never survives the call that completed it.
    pub(crate) fn insert_verified_coded_part(
        &mut self,
        commitment: &SpiceDataCommitment,
        sender: &AccountId,
        ordinal: usize,
        part: Box<[u8]>,
    ) -> Result<PartInsertResult, AssemblyError> {
        let Self::Coded { encoder, trackers } = self else {
            return Err(AssemblyError::WrongTransferUnit);
        };
        debug_assert!(
            !trackers.values().any(CodedTracker::is_complete),
            "a completion was left unresolved"
        );
        if ordinal >= encoder.total_parts() {
            return Err(AssemblyError::InvalidOrdinal);
        }
        if trackers.iter().any(|(other, tracker)| {
            other != commitment && tracker.senders.iter().flatten().any(|seen| seen == sender)
        }) {
            return Err(AssemblyError::ConflictingCommitment);
        }
        // TODO(spice-data-distribution): cap encoded_length against the max payload size;
        // the only cap today is MAX_ENCODED_LENGTH inside the decode.
        let encoded_length = usize::try_from(commitment.encoded_length)
            .map_err(|_| AssemblyError::EncodedLengthTooLarge)?;
        if part.len() != reed_solomon_part_length(encoded_length, encoder.data_parts()) {
            return Err(AssemblyError::WrongPartLength);
        }
        let tracker = trackers
            .entry(commitment.clone())
            .or_insert_with(|| CodedTracker::new(encoder.clone(), encoded_length));
        match tracker.insert_part(ordinal, part, sender)? {
            TrackerInsertResult::Accepted => Ok(PartInsertResult::Accepted),
            TrackerInsertResult::Duplicate => Ok(PartInsertResult::Duplicate),
            TrackerInsertResult::Complete(data) => {
                Ok(PartInsertResult::Complete(CompletedCodedData {
                    commitment: commitment.clone(),
                    data,
                }))
            }
            TrackerInsertResult::Garbage => {
                let tracker =
                    trackers.remove(commitment).expect("tracker should exist for this commitment");
                Ok(PartInsertResult::Garbage { contributors: tracker.contributors() })
            }
        }
    }

    pub(crate) fn is_complete(&self) -> bool {
        match self {
            Self::Coded { trackers, .. } => trackers.values().any(CodedTracker::is_complete),
            Self::Blob => false,
        }
    }

    /// Ordinals to ask for: an ordinal is skipped only if held under every commitment.
    pub(crate) fn missing_ordinals(&self) -> Vec<u64> {
        let Self::Coded { encoder, trackers } = self else {
            return Vec::new();
        };
        (0..encoder.total_parts())
            .filter(|ordinal| {
                trackers.is_empty() || trackers.values().any(|tracker| !tracker.has_part(*ordinal))
            })
            .map(|ordinal| ordinal as u64)
            .collect()
    }

    pub(crate) fn has_parts(&self) -> bool {
        match self {
            Self::Coded { trackers, .. } => {
                trackers.values().any(|tracker| tracker.part_count() > 0)
            }
            Self::Blob => false,
        }
    }

    fn take_attribution(
        &mut self,
        commitment: &SpiceDataCommitment,
    ) -> Result<DataAttribution, AssemblyError> {
        let Self::Coded { trackers, .. } = self else {
            return Err(AssemblyError::WrongTransferUnit);
        };
        let tracker = trackers.remove(commitment).ok_or(AssemblyError::UnknownCommitment)?;
        if !tracker.is_complete() {
            trackers.insert(commitment.clone(), tracker);
            return Err(AssemblyError::IncompleteCommitment);
        }
        Ok(DataAttribution { winning: commitment.clone(), senders: tracker.senders })
    }
}

/// Accumulates parts toward decoding under one claimed commitment and records who sent
/// each ordinal.
pub(crate) struct CodedTracker {
    parts: ReedSolomonPartsTracker<SpiceData>,
    /// Per-ordinal sender of the parts we hold.
    senders: Vec<Option<AccountId>>,
}

impl CodedTracker {
    fn new(encoder: Arc<ReedSolomonEncoder>, encoded_length: usize) -> Self {
        let total_parts = encoder.total_parts();
        Self {
            parts: ReedSolomonPartsTracker::new(encoder, encoded_length),
            senders: vec![None; total_parts],
        }
    }

    fn insert_part(
        &mut self,
        ordinal: usize,
        part: Box<[u8]>,
        sender: &AccountId,
    ) -> Result<TrackerInsertResult, AssemblyError> {
        match self.parts.insert_part(ordinal, part, None) {
            InsertPartResult::Accepted => {
                self.senders[ordinal] = Some(sender.clone());
                Ok(TrackerInsertResult::Accepted)
            }
            InsertPartResult::PartAlreadyAvailable => Ok(TrackerInsertResult::Duplicate),
            InsertPartResult::InvalidPartOrd => Err(AssemblyError::InvalidOrdinal),
            InsertPartResult::Decoded(result) => {
                self.senders[ordinal] = Some(sender.clone());
                Ok(match result {
                    Ok(data) => TrackerInsertResult::Complete(data),
                    Err(error) => {
                        tracing::warn!(target: "spice_data_distribution", ?error, "decoding assembled data failed");
                        TrackerInsertResult::Garbage
                    }
                })
            }
        }
    }

    fn contributors(&self) -> Vec<AccountId> {
        distinct_senders(&self.senders)
    }

    pub(crate) fn is_complete(&self) -> bool {
        self.parts.has_enough_parts()
    }

    pub(crate) fn has_part(&self, ordinal: usize) -> bool {
        self.parts.has_part(ordinal)
    }

    pub(crate) fn part_count(&self) -> usize {
        self.parts.data_parts_present()
    }

    pub(crate) fn total_parts_size(&self) -> usize {
        self.parts.total_parts_size()
    }

    pub(crate) fn charges_by_sender(&self) -> Vec<(AccountId, usize)> {
        let part_length =
            reed_solomon_part_length(self.parts.encoded_length(), self.parts.data_parts_required());
        let mut charges = HashMap::new();
        for sender in self.senders.iter().flatten() {
            *charges.entry(sender.clone()).or_default() += part_length;
        }
        charges.into_iter().collect()
    }
}

enum TrackerInsertResult {
    Accepted,
    Duplicate,
    Complete(SpiceData),
    /// The commitment reached K parts but did not decode.
    Garbage,
}

#[derive(Debug)]
pub(crate) enum PartInsertResult {
    Accepted,
    Duplicate,
    Complete(CompletedCodedData),
    /// The commitment decoded to garbage: its tracker is gone and it is banned. The
    /// accounts listed backed it.
    Garbage {
        contributors: Vec<AccountId>,
    },
}

#[derive(Debug)]
pub(crate) struct CompletedCodedData {
    commitment: SpiceDataCommitment,
    data: SpiceData,
}

impl CompletedCodedData {
    pub(crate) fn data(&self) -> &SpiceData {
        &self.data
    }

    pub(crate) fn assembled(&self) -> AssembledData<'_> {
        AssembledData::Coded { commitment: &self.commitment, data: &self.data }
    }
}

pub(crate) enum AssembledData<'a> {
    Coded { commitment: &'a SpiceDataCommitment, data: &'a SpiceData },
    Blob(&'a [u8]),
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum AssemblyError {
    #[error("item is not collecting")]
    NotCollecting,
    #[error("item is not waiting for a verdict")]
    NotDelivered,
    #[error("transfer unit does not match the assembly")]
    WrongTransferUnit,
    #[error("part ordinal is out of range")]
    InvalidOrdinal,
    #[error("encoded length does not fit in memory")]
    EncodedLengthTooLarge,
    #[error("part length does not match the commitment's encoded length")]
    WrongPartLength,
    #[error("sender already backed another commitment")]
    ConflictingCommitment,
    #[error("commitment was rejected after validation")]
    BannedCommitment,
    #[error("commitment is not tracked")]
    UnknownCommitment,
    #[error("commitment is incomplete")]
    IncompleteCommitment,
}

/// Who to blame for a fault on the delivered data: the winning commitment's senders only.
#[derive(Debug)]
pub(crate) struct DataAttribution {
    pub(crate) winning: SpiceDataCommitment,
    senders: Vec<Option<AccountId>>,
}

impl DataAttribution {
    /// The distinct accounts that sent the delivered parts. The only accessor: a fault on
    /// the delivered data is attributed to these senders and no others.
    pub(crate) fn contributors(&self) -> Vec<AccountId> {
        distinct_senders(&self.senders)
    }
}

fn distinct_senders(senders: &[Option<AccountId>]) -> Vec<AccountId> {
    let mut distinct: Vec<AccountId> = Vec::new();
    for sender in senders.iter().flatten() {
        if !distinct.contains(sender) {
            distinct.push(sender.clone());
        }
    }
    distinct
}
