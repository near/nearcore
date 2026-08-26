use borsh::{BorshDeserialize, BorshSerialize};
use near_async::time::{Clock, Instant};
use near_primitives::hash::hash;
use near_primitives::merkle::{MerklePath, verify_path_with_index};
use near_primitives::reed_solomon::{
    InsertPartResult, ReedSolomonEncoder, ReedSolomonEncoderDeserialize,
    ReedSolomonEncoderSerialize, ReedSolomonPartsTracker, reed_solomon_part_length,
};
use near_primitives::sharding::ReceiptProof;
use near_primitives::spice::partial_data::SpiceDataCommitment;
use near_primitives::spice::state_witness::SpiceChunkStateWitness;
use near_primitives::types::AccountId;
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

/// The fetch lifecycle. No terminal "have" state: the store is the source of truth for
/// done-ness — the consumer persists the verified data (e.g. a receipt proof in the
/// chain store) and the engine consults that.
pub(crate) enum FetchState {
    /// Wanted, but no unit has arrived and pulling has not started; waiting for the push.
    WaitingForPush,
    /// At least one unit arrived, or speculative pulling started.
    Collecting(Assembly),
    /// Assembled data handed to the consumer; parked until its verdict, so a re-pushed
    /// part cannot deliver twice. `residual` keeps the incomplete trackers.
    Delivered { attribution: DataAttribution, residual: Assembly },
    /// Consumer verified and persisted the artifact; terminal until expiry. The
    /// attribution stays because a fault can be discovered after local verification (for example the
    /// certified result for the chunk differs from locally verified witness) and must still map back to the senders.
    ProcessedLocally { attribution: DataAttribution },
}

/// Runs a state transition that takes ownership of the (parts of) current state.
///
/// # Arguments
/// - `f` consumes the current state and returns `(next_state, result)`; to reject the transition, return the received state unchanged.
fn transition<T>(state: &mut FetchState, f: impl FnOnce(FetchState) -> (FetchState, T)) -> T {
    // Borrow checker needs a placeholder for state (because it is `&mut` and we like to move out parts of it).
    // `WaitingForPush` is used as one. It is never observed and is overwritten next line.
    let (next, result) = f(replace(state, FetchState::WaitingForPush));
    *state = next;
    result
}

pub(crate) struct FetchItem {
    pub(crate) state: FetchState,
    /// When the first unit arrived; `None` until then. Anchors the wait-for-push grace clock.
    pub(crate) first_unit_at: Option<Instant>,
}

impl FetchItem {
    pub(crate) fn waiting_for_push() -> Self {
        Self { state: FetchState::WaitingForPush, first_unit_at: None }
    }

    pub(crate) fn collecting(encoder: Arc<ReedSolomonEncoder>) -> Self {
        Self { state: FetchState::Collecting(Assembly::new(encoder)), first_unit_at: None }
    }

    /// Starts a speculative pull: a waiting item begins collecting before any part
    /// arrived. Does nothing unless the item is waiting for the push.
    pub(crate) fn start_pulling(&mut self, encoder: Arc<ReedSolomonEncoder>) -> bool {
        if !matches!(self.state, FetchState::WaitingForPush) {
            return false;
        }
        self.state = FetchState::Collecting(Assembly::new(encoder));
        true
    }

    /// Opens a waiting item on its first part; a completing part parks the item in
    /// `Delivered` in the same call. `NotCollecting` means the item is parked awaiting
    /// a verdict or already processed.
    // TODO(spice-data-distribution): consider accepting parts into the residual while
    // parked in `Delivered`, rejecting only a would-be-completing part; today all parts
    // are rejected and a failed verdict recovers via an immediate pull.
    pub(crate) fn insert_part(
        &mut self,
        clock: &Clock,
        encoder: &Arc<ReedSolomonEncoder>,
        sender: &AccountId,
        verified: VerifiedCodedPart,
    ) -> Result<PartInsertResult, AssemblyError> {
        let commitment = verified.commitment.clone();
        let result = match &mut self.state {
            FetchState::WaitingForPush => {
                // a rejected part must leave a waiting item waiting, so promote
                // only after the insert succeeds
                let mut assembly = Assembly::new(encoder.clone());
                let result = assembly.insert_part(sender, verified)?;
                self.state = FetchState::Collecting(assembly);
                result
            }
            FetchState::Collecting(assembly) => assembly.insert_part(sender, verified)?,
            _ => return Err(AssemblyError::NotCollecting),
        };
        let FetchState::Collecting(assembly) = &self.state else {
            unreachable!("an accepted insert leaves the item collecting");
        };
        match &result {
            PartInsertResult::Garbage { .. } => {
                if !assembly.has_parts() {
                    self.first_unit_at = None;
                }
            }
            PartInsertResult::Accepted | PartInsertResult::Complete(_) => {
                if self.first_unit_at.is_none() {
                    self.first_unit_at = Some(clock.now());
                }
            }
            PartInsertResult::Duplicate => {}
        }
        if matches!(result, PartInsertResult::Complete(_)) {
            transition(&mut self.state, |state| {
                let FetchState::Collecting(mut assembly) = state else {
                    unreachable!("a completing insert leaves the item collecting");
                };
                let attribution = assembly.take_attribution(&commitment);
                (FetchState::Delivered { attribution, residual: assembly }, ())
            });
        }
        Ok(result)
    }

    pub(crate) fn mark_verified(&mut self) -> Result<(), AssemblyError> {
        transition(&mut self.state, |state| match state {
            FetchState::Delivered { attribution, .. } => {
                (FetchState::ProcessedLocally { attribution }, Ok(()))
            }
            state => (state, Err(AssemblyError::NotDelivered)),
        })
    }

    pub(crate) fn mark_failed(&mut self) -> Result<HashSet<AccountId>, AssemblyError> {
        transition(&mut self.state, |state| match state {
            FetchState::Delivered { attribution, mut residual } => {
                let contributors = attribution.contributors();
                residual.ban(attribution.decoded);
                // an empty residual means the only evidence was the banned commitment's
                // own parts, so existence is unproven again
                if !residual.has_parts() {
                    self.first_unit_at = None;
                }
                (FetchState::Collecting(residual), Ok(contributors))
            }
            state => (state, Err(AssemblyError::NotDelivered)),
        })
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
    ) -> Result<Self, AssemblyError> {
        if !verify_path_with_index(
            commitment.root,
            merkle_proof,
            &part,
            ordinal,
            total_parts as u64,
        ) {
            return Err(AssemblyError::InvalidMerkleProof);
        }
        let ordinal = usize::try_from(ordinal).map_err(|_| AssemblyError::InvalidOrdinal)?;
        Ok(Self { commitment: commitment.clone(), total_parts, ordinal, part })
    }

    // TODO(spice-data-distribution): these accessors only feed the old ingress path;
    // they go when C1a routes ingress through the engine.
    pub(crate) fn ordinal(&self) -> usize {
        self.ordinal
    }

    pub(crate) fn into_part(self) -> Box<[u8]> {
        self.part
    }
}

pub(crate) struct Assembly {
    encoder: Arc<ReedSolomonEncoder>,
    /// One tracker per commitment; a sender may back only one, which bounds the trackers.
    trackers: HashMap<SpiceDataCommitment, CodedTracker>,
    /// Commitments rejected for this item — a failed consumer verdict or a garbage
    /// decode. Parts under them are rejected on arrival.
    banned: HashSet<SpiceDataCommitment>,
    /// The one commitment each sender provided parts for. Outlives the trackers, so a
    /// sender whose commitment was dropped as garbage cannot loop through fresh
    /// commitments.
    commitment_by_sender: HashMap<AccountId, SpiceDataCommitment>,
}

impl Assembly {
    pub(crate) fn new(encoder: Arc<ReedSolomonEncoder>) -> Self {
        Self {
            encoder,
            trackers: HashMap::new(),
            banned: HashSet::new(),
            commitment_by_sender: HashMap::new(),
        }
    }

    fn ban(&mut self, commitment: SpiceDataCommitment) {
        self.banned.insert(commitment);
    }

    #[cfg(test)]
    pub(crate) fn is_banned(&self, commitment: &SpiceDataCommitment) -> bool {
        self.banned.contains(commitment)
    }

    /// A returned `Complete` must be resolved (delivered or failed) before the next
    /// insert; a completed tracker never survives the call that completed it.
    pub(crate) fn insert_part(
        &mut self,
        sender: &AccountId,
        verified: VerifiedCodedPart,
    ) -> Result<PartInsertResult, AssemblyError> {
        let VerifiedCodedPart { commitment, total_parts, ordinal, part } = verified;
        if self.banned.contains(&commitment) {
            return Err(AssemblyError::BannedCommitment);
        }
        debug_assert!(
            !self.trackers.values().any(CodedTracker::is_complete),
            "a completion was left unresolved"
        );
        // equal widths plus a verified proof imply the ordinal is in range
        if total_parts != self.encoder.total_parts() {
            return Err(AssemblyError::WrongTotalParts);
        }
        if self.commitment_by_sender.get(sender).is_some_and(|provided| provided != &commitment) {
            return Err(AssemblyError::ConflictingCommitment);
        }
        // TODO(spice-data-distribution): cap encoded_length against the max payload size;
        // the only cap today is MAX_ENCODED_LENGTH inside the decode.
        let encoded_length =
            usize::try_from(commitment.encoded_length).expect("encoded length should fit in usize");
        if part.len() != reed_solomon_part_length(encoded_length, self.encoder.data_parts()) {
            return Err(AssemblyError::WrongPartLength);
        }
        let tracker = self
            .trackers
            .entry(commitment.clone())
            .or_insert_with(|| CodedTracker::new(self.encoder.clone(), commitment.clone()));
        let result = tracker.insert_part(ordinal, part, sender)?;
        self.commitment_by_sender.insert(sender.clone(), commitment.clone());
        if matches!(result, PartInsertResult::Garbage { .. }) {
            self.trackers.remove(&commitment);
            self.banned.insert(commitment);
        }
        Ok(result)
    }

    pub(crate) fn is_complete(&self) -> bool {
        self.trackers.values().any(CodedTracker::is_complete)
    }

    /// Ordinals to ask for: an ordinal is skipped only if held under every commitment.
    pub(crate) fn missing_ordinals(&self) -> Vec<u64> {
        (0..self.encoder.total_parts())
            .filter(|ordinal| {
                self.trackers.is_empty()
                    || self.trackers.values().any(|tracker| !tracker.has_part(*ordinal))
            })
            .map(|ordinal| ordinal as u64)
            .collect()
    }

    pub(crate) fn has_parts(&self) -> bool {
        self.trackers.values().any(|tracker| tracker.part_count() > 0)
    }

    /// Removes the completed tracker, yielding who to blame for its data.
    fn take_attribution(&mut self, commitment: &SpiceDataCommitment) -> DataAttribution {
        let tracker =
            self.trackers.remove(commitment).expect("completed commitment should be tracked");
        DataAttribution { decoded: commitment.clone(), senders: tracker.senders }
    }

    #[cfg(test)]
    pub(crate) fn tracked_commitments(&self) -> HashSet<&SpiceDataCommitment> {
        self.trackers.keys().collect()
    }
}

/// Accumulates parts toward decoding under one claimed commitment and records who sent
/// each ordinal.
pub(crate) struct CodedTracker {
    parts: ReedSolomonPartsTracker<SpiceData>,
    /// Per-ordinal sender of the parts we hold.
    senders: Vec<Option<AccountId>>,
    /// The commitment the parts are tracked under
    commitment: SpiceDataCommitment,
}

impl CodedTracker {
    fn new(encoder: Arc<ReedSolomonEncoder>, commitment: SpiceDataCommitment) -> Self {
        let encoded_length =
            usize::try_from(commitment.encoded_length).expect("encoded length should fit in usize");
        let total_parts = encoder.total_parts();
        Self {
            parts: ReedSolomonPartsTracker::new(encoder, encoded_length),
            senders: vec![None; total_parts],
            commitment,
        }
    }

    fn insert_part(
        &mut self,
        ordinal: usize,
        part: Box<[u8]>,
        sender: &AccountId,
    ) -> Result<PartInsertResult, AssemblyError> {
        match self.parts.insert_part(ordinal, part, None) {
            InsertPartResult::Accepted => {
                self.senders[ordinal] = Some(sender.clone());
                Ok(PartInsertResult::Accepted)
            }
            InsertPartResult::PartAlreadyAvailable => Ok(PartInsertResult::Duplicate),
            InsertPartResult::InvalidPartOrd => Err(AssemblyError::InvalidOrdinal),
            InsertPartResult::Decoded(result) => {
                self.senders[ordinal] = Some(sender.clone());
                Ok(match result {
                    Ok(data) if hash(&borsh::to_vec(&data).unwrap()) == self.commitment.hash => {
                        PartInsertResult::Complete(data)
                    }
                    Ok(_) => {
                        tracing::warn!(target: "spice_data_distribution", "decoded data does not match the committed hash");
                        PartInsertResult::Garbage { contributors: self.contributors() }
                    }
                    Err(error) => {
                        tracing::warn!(target: "spice_data_distribution", ?error, "decoding assembled data failed");
                        PartInsertResult::Garbage { contributors: self.contributors() }
                    }
                })
            }
        }
    }

    fn contributors(&self) -> HashSet<AccountId> {
        self.senders.iter().flatten().cloned().collect()
    }

    fn is_complete(&self) -> bool {
        self.parts.has_enough_parts()
    }

    fn has_part(&self, ordinal: usize) -> bool {
        self.parts.has_part(ordinal)
    }

    fn part_count(&self) -> usize {
        self.parts.data_parts_present()
    }
}

#[must_use = "a Complete carries the delivered data"]
#[derive(Debug)]
pub(crate) enum PartInsertResult {
    Accepted,
    Duplicate,
    /// The commitment decoded to this data and it matches the committed hash.
    Complete(SpiceData),
    /// The commitment reached K parts but yielded no data matching its hash — a failed
    /// decode or a hash mismatch. Carries the list of accounts provided parts for it.
    Garbage {
        contributors: HashSet<AccountId>,
    },
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum AssemblyError {
    #[error("item is not collecting")]
    NotCollecting,
    #[error("item is not waiting for a verdict")]
    NotDelivered,
    #[error("part merkle proof does not verify against the commitment root")]
    InvalidMerkleProof,
    #[error("part ordinal is out of range")]
    InvalidOrdinal,
    #[error("part was verified against a different total parts count")]
    WrongTotalParts,
    #[error("part length does not match the commitment's encoded length")]
    WrongPartLength,
    #[error("sender already backed another commitment")]
    ConflictingCommitment,
    #[error("commitment was rejected after validation")]
    BannedCommitment,
}

/// Decoded commitment bundled with accounts which provided its parts.
#[derive(Debug)]
pub(crate) struct DataAttribution {
    pub(super) decoded: SpiceDataCommitment,
    /// Per-ordinal sender of the parts we hold.
    senders: Vec<Option<AccountId>>,
}

impl DataAttribution {
    /// Accounts that provided parts for the decoded commitment.
    pub(crate) fn contributors(&self) -> HashSet<AccountId> {
        self.senders.iter().flatten().cloned().collect()
    }
}
