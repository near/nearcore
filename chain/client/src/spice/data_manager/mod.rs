//! Fetch-engine state for SPICE data distribution.

mod fetchable;
mod item;
mod scheduler;

pub(crate) use fetchable::DataPolicy;
use fetchable::ReceiptProofPolicy;
pub use item::DataId;
pub(crate) use item::{AssembledDataError, SpiceData, VerifiedCodedPart};
use item::{FetchItem, FetchState, InFlightRequest, Item, PartInsertResult};
use near_async::time::{Clock, Instant};
use near_chain::Error;
use near_epoch_manager::EpochManagerAdapter;
use near_epoch_manager::shard_tracker::ShardTracker;
use near_primitives::block_header::BlockHeader;
use near_primitives::reed_solomon::ReedSolomonEncoderCache;
use near_primitives::spice::partial_data::{SpiceDataCommitment, SpiceDataPart};
use near_primitives::types::{AccountId, BlockHeight};
use near_store::adapter::chain_store::ChainStoreAdapter;
use rand::rngs::StdRng;
pub(crate) use scheduler::WakeAt;
use scheduler::{DeadlineScheduler, TimingConfig};
use std::cmp::Ordering;
use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::hash::{Hash, Hasher as _};
use std::sync::Arc;
use time::ext::InstantExt as _;

/// Scheduling class of an item.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Lane {
    /// Consensus-critical: this node is an assigned validator or producer for it.
    Priority,
    /// RPC, state-sync or catch-up traffic; never starves `Priority`.
    // TODO(spice-data-distribution): constructed once lanes are classified per item (#16275).
    #[allow(dead_code)]
    Background,
}

impl Lane {
    /// Urgency rank; greater is more urgent, so `max` escalates.
    fn rank(self) -> u8 {
        match self {
            Lane::Background => 0,
            Lane::Priority => 1,
        }
    }
}

impl Ord for Lane {
    fn cmp(&self, other: &Self) -> Ordering {
        self.rank().cmp(&other.rank())
    }
}

impl PartialOrd for Lane {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(test)]
mod tests;

#[derive(Debug, thiserror::Error)]
pub(crate) enum DataManagerError {
    #[error("no item tracks this data")]
    UnknownItem,
    #[error("commitment decoded to garbage: {0}")]
    GarbageCommitment(AssembledDataError),
    #[error("item is not collecting")]
    NotCollecting,
    #[error("item is not waiting for a verification result")]
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

/// Outcome of accepting parts for an item.
#[must_use]
#[derive(Debug)]
pub(crate) enum ReceivedParts {
    /// Parts accepted; the item is still collecting. Carries the wake-up this call scheduled, if any.
    Collecting(WakeAt),
    /// The parts completed the item; the data is handed to the consumer.
    Complete(SpiceData),
    /// No item tracks the id, or the item is past collecting (delivered or processed).
    NotWanted,
}

/// One pull request to send: the ordinals still missing for each item `producer` holds.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct PullRequest {
    pub(crate) producer: AccountId,
    pub(crate) wants: BTreeMap<DataId, BTreeSet<u64>>,
}

/// Index of the source to ask in `round`. The start is a hash of the id and the
/// requester, so requesters spread over the sources; each round moves one along.
pub(crate) fn rotated_source_index(
    num_sources: usize,
    id: &impl Hash,
    requester: &AccountId,
    round: u64,
) -> usize {
    let mut hasher = DefaultHasher::new();
    id.hash(&mut hasher);
    requester.hash(&mut hasher);
    (hasher.finish().wrapping_add(round) % num_sources as u64) as usize
}

/// The per-data-type policies, one per [`DataId`] variant.
pub(crate) struct Policies {
    receipt_proofs: ReceiptProofPolicy,
}

impl Policies {
    pub(crate) fn new(
        chain_store: ChainStoreAdapter,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        shard_tracker: ShardTracker,
    ) -> Self {
        Self { receipt_proofs: ReceiptProofPolicy::new(chain_store, epoch_manager, shard_tracker) }
    }

    fn for_id(&self, id: &DataId) -> &dyn DataPolicy {
        match id {
            DataId::ReceiptProof { .. } => &self.receipt_proofs,
        }
    }
}

/// Fans out per-block queries over every policy; dispatches per-id calls to the policy
/// of `id`'s data type.
impl DataPolicy for Policies {
    fn needed_ids(&self, block: &BlockHeader) -> Result<Vec<DataId>, Error> {
        self.receipt_proofs.needed_ids(block)
    }

    fn is_done(&self, id: &DataId) -> Result<bool, Error> {
        self.for_id(id).is_done(id)
    }

    fn sources(&self, id: &DataId) -> Result<Vec<AccountId>, Error> {
        self.for_id(id).sources(id)
    }
}

/// Owns the per-item fetch lifecycle: what this node still needs, the parts received so
/// far and who sent them, and when an item stops being relevant.
/// Validation results are reported back via [`Self::on_verified`]/[`Self::on_failed`].
/// Calls that can schedule a pull return a [`WakeAt`]: the instant to call
/// [`Self::on_wake`] at, when the earliest deadline changed since the last one returned.
// TODO(spice-data-distribution): only receipt proofs route here; witnesses still live
// on the old actor path (#16275).
pub(crate) struct SpiceDataManager<P: DataPolicy = Policies> {
    clock: Clock,
    /// Jitters the retry intervals; injected so tests are deterministic.
    rng: StdRng,
    timing: TimingConfig,
    encoders: ReedSolomonEncoderCache,
    policies: P,
    /// All tracked items, in any state.
    items: HashMap<DataId, Item>,
    /// Ids of tracked items, indexed by their block's height as captured when first tracked
    items_by_height: BTreeMap<BlockHeight, Vec<DataId>>,
    /// Pull wake-ups for the items. Entries cannot be removed, so a completed or
    /// re-scheduled item leaves stale ones behind; [`Self::on_wake`] filters them.
    scheduler: DeadlineScheduler<DataId>,
    /// Highest final execution head reported; `None` until the first report. Items at
    /// or below it can never be applied.
    final_execution_head: Option<BlockHeight>,
}

/// Marks `at` as the item's pull wake-up.
// TODO(spice-data-distribution): classify the scheduling lane per item; everything is
// priority until a lower-priority consumer exists (#16275).
fn schedule_pull_wake(
    scheduler: &mut DeadlineScheduler<DataId>,
    item: &mut FetchItem,
    id: &DataId,
    at: Instant,
) {
    item.pull.next_deadline = Some(at);
    scheduler.schedule(id.clone(), at, Lane::Priority);
}

/// The source to ask for `id` in rotation `round`; `None` if none can be resolved.
fn select_source(
    policies: &impl DataPolicy,
    id: &DataId,
    requester: &AccountId,
    round: u64,
) -> Option<AccountId> {
    let mut sources = match policies.sources(id) {
        Ok(sources) => sources,
        Err(err) => {
            tracing::error!(target: "spice_data_distribution", ?err, ?id, "failed to resolve the sources to pull from");
            return None;
        }
    };
    if sources.is_empty() {
        return None;
    }
    let index = rotated_source_index(sources.len(), id, requester, round);
    Some(sources.swap_remove(index))
}

impl<P: DataPolicy> SpiceDataManager<P> {
    pub(crate) fn new(clock: Clock, rng: StdRng, data_parts_ratio: f64, policies: P) -> Self {
        Self {
            clock,
            rng,
            timing: TimingConfig::default(),
            encoders: ReedSolomonEncoderCache::new(data_parts_ratio),
            policies,
            items: HashMap::new(),
            items_by_height: BTreeMap::new(),
            scheduler: DeadlineScheduler::default(),
            final_execution_head: None,
        }
    }

    /// Whether an item for `id` exists, in any state.
    #[cfg(test)]
    pub(crate) fn is_tracking(&self, id: &DataId) -> bool {
        self.items.contains_key(id)
    }

    /// Starts tracking every item this node needs from `block` and doesn't already have or track. Idempotent.
    pub(crate) fn on_block(&mut self, block: &BlockHeader) -> Result<(), Error> {
        let height = block.height();
        // The chain is past the block, so its data can never be applied.
        if self.final_execution_head.is_some_and(|head| height <= head) {
            return Ok(());
        }
        for id in self.policies.needed_ids(block)? {
            if self.items.contains_key(&id) || self.policies.is_done(&id)? {
                continue;
            }
            self.items_by_height.entry(height).or_default().push(id.clone());
            self.items.insert(id, Item::Fetch(FetchItem::waiting_for_push(height)));
        }
        Ok(())
    }

    /// The only insert path for received units. Verifies each part against the
    /// commitment and inserts it. A completing insert checks the decoded data against
    /// the committed hash and the id: a failure bans the commitment, a match returns the
    /// data for handoff to the consumer, leaving the item parked until the consumer's
    /// (local) verification result arrives. Errors are attributable to the sender. A
    /// pull scheduled by a message that then fails on a later part is reported by the
    /// next call that returns a wake-up.
    pub(crate) fn on_parts_received(
        &mut self,
        sender: &AccountId,
        id: &DataId,
        commitment: &SpiceDataCommitment,
        parts: Vec<SpiceDataPart>,
        total_parts: usize,
    ) -> Result<ReceivedParts, DataManagerError> {
        let Some(Item::Fetch(item)) = self.items.get_mut(id) else {
            return Ok(ReceivedParts::NotWanted);
        };
        item.pull.clear_in_flight_from(sender);
        let mut had_units = item.first_unit_at.is_some();
        let encoder = self.encoders.entry(total_parts);
        // TODO(spice-data-distribution): verify every part before inserting any; today
        // the first bad part aborts the loop without undoing earlier inserts (#16275).
        for SpiceDataPart { part_ord, part, merkle_proof } in parts {
            let verified =
                VerifiedCodedPart::verify(commitment, total_parts, part_ord, part, &merkle_proof)?;
            match item.insert_part(&self.clock, &encoder, id, sender, verified) {
                Ok(PartInsertResult::Complete(data)) => return Ok(ReceivedParts::Complete(data)),
                Ok(PartInsertResult::Garbage { contributors, error }) => {
                    tracing::debug!(target: "spice_data_distribution", ?id, ?contributors, "commitment decoded to garbage");
                    return Err(DataManagerError::GarbageCommitment(error));
                }
                Ok(PartInsertResult::Accepted | PartInsertResult::Duplicate) => {
                    // The first unit proves the data exists; give the other producers'
                    // pushes a moment to land, then pull whatever is still missing.
                    if !had_units && item.first_unit_at.is_some() {
                        had_units = true;
                        let at =
                            self.clock.now().add_signed(self.timing.pull_delay_after_first_unit);
                        schedule_pull_wake(&mut self.scheduler, item, id, at);
                    }
                }
                Err(DataManagerError::NotCollecting) => return Ok(ReceivedParts::NotWanted),
                Err(err) => return Err(err),
            }
        }
        Ok(ReceivedParts::Collecting(self.scheduler.take_wake()))
    }

    /// Consumer validated and persisted the delivered data (so `is_done` holds for it from now on).
    /// A verification result for an expired item is rejected without effect.
    pub(crate) fn on_verified(&mut self, id: &DataId) -> Result<(), DataManagerError> {
        let Some(Item::Fetch(item)) = self.items.get_mut(id) else {
            return Err(DataManagerError::UnknownItem);
        };
        item.mark_verified()?;
        Ok(())
    }

    /// Consumer rejected the delivered data: the delivered commitment is banned and
    /// collecting resumes from the remaining trackers. A verification result for an
    /// expired item is rejected without effect.
    pub(crate) fn on_failed(&mut self, id: &DataId) -> Result<WakeAt, DataManagerError> {
        let Some(Item::Fetch(item)) = self.items.get_mut(id) else {
            return Err(DataManagerError::UnknownItem);
        };
        // TODO(spice-data-distribution): feed the contributors into reputation (#16275).
        let contributors = item.mark_failed()?;
        tracing::debug!(target: "spice_data_distribution", ?id, ?contributors, "delivered data failed consumer validation");
        // The residual is incomplete, so the pull for the gaps is due immediately.
        schedule_pull_wake(&mut self.scheduler, item, id, self.clock.now());
        Ok(self.scheduler.take_wake())
    }

    /// The consumer needed `ids` and their data has not arrived — the pull trigger. A
    /// still-waiting item starts collecting, with its first pull scheduled after
    /// `pull_delay_after_gate` to let a straggler push land. No effect on an unknown
    /// item or one already past waiting.
    pub(crate) fn start_pulling(&mut self, ids: &[DataId]) -> WakeAt {
        for id in ids {
            let Some(Item::Fetch(item)) = self.items.get_mut(id) else {
                continue;
            };
            if !matches!(item.state, FetchState::WaitingForPush) {
                continue;
            }
            let total_parts = match self.policies.sources(id) {
                Ok(sources) if !sources.is_empty() => sources.len(),
                Ok(_) => {
                    tracing::error!(target: "spice_data_distribution", ?id, "no sources to pull from");
                    continue;
                }
                Err(err) => {
                    tracing::error!(target: "spice_data_distribution", ?err, ?id, "failed to resolve the sources to pull from");
                    continue;
                }
            };
            let encoder = self.encoders.entry(total_parts);
            assert!(item.start_pulling(encoder), "a waiting item starts pulling");
            let at = self.clock.now().add_signed(self.timing.pull_delay_after_gate);
            schedule_pull_wake(&mut self.scheduler, item, id, at);
        }
        self.scheduler.take_wake()
    }

    /// A pull wake-up fired. Runs the retry decision for every item whose deadline is
    /// due and returns the requests to send, one per producer, with the next wake-up.
    /// Without a `requester` nothing is sent, but the items stay scheduled. A stale
    /// wake-up is harmless: it finds nothing due.
    pub(crate) fn on_wake(
        &mut self,
        now: Instant,
        requester: Option<&AccountId>,
    ) -> (Vec<PullRequest>, WakeAt) {
        let mut wants_by_producer: BTreeMap<AccountId, BTreeMap<DataId, BTreeSet<u64>>> =
            BTreeMap::new();
        for id in self.due_items(now) {
            if let Some((producer, ordinals)) = self.retry(&id, now, requester) {
                wants_by_producer
                    .entry(producer)
                    .or_default()
                    .insert(id, ordinals.into_iter().collect());
            }
        }
        let requests = wants_by_producer
            .into_iter()
            .map(|(producer, wants)| PullRequest { producer, wants })
            .collect();
        (requests, self.scheduler.take_wake())
    }

    /// Pops the due pull wake-ups and keeps the ones that are still real: the item
    /// exists, is still collecting, and the popped instant is its current deadline.
    /// Without the last check every completion or re-schedule would fire a spurious
    /// pull off the entry it left behind.
    fn due_items(&mut self, now: Instant) -> Vec<DataId> {
        self.scheduler
            .pop_due(now)
            .into_iter()
            .filter(|(id, at)| {
                let Some(Item::Fetch(item)) = self.items.get(id) else {
                    return false;
                };
                matches!(item.state, FetchState::Collecting(_))
                    && item.pull.next_deadline == Some(*at)
            })
            .map(|(id, _)| id)
            .collect()
    }

    /// The retry decision for one due item: drops the timed-out requests, picks the
    /// rotated source for the ordinals still missing and not in flight, records the
    /// request, and reschedules the item whether or not one goes out. A wake caused by a
    /// request timeout re-requests without advancing the backoff ladder: timing out is
    /// the peer's failure, not a retry.
    // TODO(spice-data-distribution): stripe the missing set across several sources,
    // preferring ones that have not already sent an ordinal (#16275).
    fn retry(
        &mut self,
        id: &DataId,
        now: Instant,
        requester: Option<&AccountId>,
    ) -> Option<(AccountId, Vec<u64>)> {
        let Some(Item::Fetch(item)) = self.items.get_mut(id) else {
            return None;
        };
        // TODO(spice-data-distribution): feed the timed-out sources into reputation (#16275).
        let timed_out = item.pull.take_timed_out_requests(&self.timing, now);
        let FetchState::Collecting(assembly) = &item.state else {
            return None;
        };
        debug_assert!(!assembly.is_complete(), "a collecting item cannot be complete");
        let missing: Vec<u64> = assembly
            .missing_ordinals()
            .into_iter()
            .filter(|ordinal| !item.pull.has_in_flight_request_for(*ordinal))
            .collect();
        let source = match requester {
            Some(requester) if !missing.is_empty() => {
                select_source(&self.policies, id, requester, item.pull.requests_sent)
            }
            _ => None,
        };
        let request = source.map(|source| (source, missing));
        if let Some((source, ordinals)) = &request {
            item.pull.in_flight.push(InFlightRequest {
                source: source.clone(),
                sent_at: now,
                ordinals: ordinals.clone(),
            });
            item.pull.requests_sent += 1;
            if timed_out.is_empty() {
                item.pull.backoff.note_retry();
            }
        }
        let at = item.pull.reschedule(&self.timing, now, &mut self.rng);
        schedule_pull_wake(&mut self.scheduler, item, id, at);
        request
    }

    /// The final execution head advanced: the chain is past every item at or below it,
    /// so their data can no longer be applied. Removes them, and [`Self::on_block`] refuses
    /// them from now on.
    pub(crate) fn on_final_execution_head(&mut self, height: BlockHeight) {
        self.final_execution_head = self.final_execution_head.max(Some(height));
        let Some(next_height) = height.checked_add(1) else {
            return;
        };
        let live = self.items_by_height.split_off(&next_height);
        let expired = std::mem::replace(&mut self.items_by_height, live);
        for (bucket_height, ids) in expired {
            for id in ids {
                let Item::Fetch(item) =
                    self.items.get(&id).expect("index entry names a tracked item");
                assert_eq!(item.height, bucket_height, "index entry height matches its item");
                self.items.remove(&id);
            }
        }
    }
}
