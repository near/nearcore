//! Who to ask for missing parts, and when: paced by processed blocks, no clock.

use super::data_id::Herd;
use super::fetchable::CertifiedFrontier;
use super::item::{CommitmentState, FetchItem};
use super::{DataId, DataPolicy, SpiceDataManager};
use near_primitives::spice::partial_data::SpiceDataCommitment;
use near_primitives::types::{AccountId, BlockHeight};
use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::hash::{Hash, Hasher as _};

/// One pull request to send: the ordinals asked of `producer` for each item.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PullRequest {
    pub(crate) producer: AccountId,
    pub(crate) wants: BTreeMap<DataId, BTreeSet<u64>>,
}

/// Pacing of pulls, counted in processed blocks.
#[derive(Debug, Clone)]
pub(crate) struct PullConfig {
    /// Processed blocks a request may stay unanswered before it is sent again: to the next
    /// pool member for a tracker's gaps, to the same producer for its own ordinal.
    pub(crate) reask_after_blocks: BlockHeight,
    /// Open, not-done items per herd pulled at one processed block.
    pub(crate) pull_window: usize,
}

impl Default for PullConfig {
    fn default() -> Self {
        Self { reask_after_blocks: 1, pull_window: 4 }
    }
}

/// Index of the source to ask in `round`. The start is a hash of the key and the
/// requester, so requesters spread over the sources; each round moves one along.
pub(crate) fn rotated_source_index(
    num_sources: usize,
    key: &impl Hash,
    requester: &AccountId,
    round: u64,
) -> usize {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    requester.hash(&mut hasher);
    (hasher.finish().wrapping_add(round) % num_sources as u64) as usize
}

/// One outstanding pull request to one source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct InFlightRequest {
    pub(super) source: AccountId,
    /// Height of the processed block the request was sent at.
    pub(super) sent_at_height: BlockHeight,
}

/// A tracker's pull requests: the one outstanding, and how many were sent, which rotates
/// the pool member asked.
#[derive(Debug, Default)]
pub(super) struct PullState {
    pub(super) in_flight: Option<InFlightRequest>,
    pub(super) requests_sent: u64,
}

impl PullState {
    /// Drops the outstanding request once it has gone unanswered for `reask_after_blocks`
    /// processed blocks. Then, if none is outstanding, records a request to the next member
    /// of `pool` in rotation and returns it.
    pub(super) fn next_source(
        &mut self,
        pool: &[AccountId],
        key: &impl Hash,
        requester: &AccountId,
        height: BlockHeight,
        reask_after_blocks: BlockHeight,
    ) -> Option<AccountId> {
        let unanswered = self.in_flight.as_ref().is_some_and(|request| {
            height.saturating_sub(request.sent_at_height) >= reask_after_blocks
        });
        if unanswered {
            self.in_flight = None;
        }
        if self.in_flight.is_some() || pool.is_empty() {
            return None;
        }
        let index = rotated_source_index(pool.len(), key, requester, self.requests_sent);
        let source = pool[index].clone();
        self.in_flight = Some(InFlightRequest { source: source.clone(), sent_at_height: height });
        self.requests_sent += 1;
        Some(source)
    }

    /// Forgets the outstanding request if it went to `source`: it answered.
    fn clear_in_flight_from(&mut self, source: &AccountId) {
        if self.in_flight.as_ref().is_some_and(|request| &request.source == source) {
            self.in_flight = None;
        }
    }
}

impl FetchItem {
    /// Producers to ask at `height`, with the ordinals to ask each: one rotated backer per
    /// live commitment for its missing ordinals, unless a request is outstanding, and every
    /// unbound producer for its own ordinal, unless asked recently. Records the asks.
    pub(super) fn pull_wants(
        &mut self,
        id: &DataId,
        sources: &[AccountId],
        requester: &AccountId,
        height: BlockHeight,
        reask_after_blocks: BlockHeight,
    ) -> BTreeMap<AccountId, BTreeSet<u64>> {
        let mut wants: BTreeMap<AccountId, BTreeSet<u64>> = BTreeMap::new();
        let live: Vec<SpiceDataCommitment> = self
            .commitments
            .iter()
            .filter(|(_, state)| matches!(state, CommitmentState::Tracking(_)))
            .map(|(commitment, _)| commitment.clone())
            .collect();
        for commitment in live {
            let mut pool: Vec<AccountId> =
                self.contributors(&commitment).into_iter().cloned().collect();
            pool.sort();
            let tracker = self.tracker_mut(&commitment).expect("live commitment is tracked");
            let Some(source) = tracker.pull.next_source(
                &pool,
                &(id, &commitment),
                requester,
                height,
                reask_after_blocks,
            ) else {
                continue;
            };
            wants.entry(source).or_default().extend(tracker.missing_ordinals());
        }
        for (producer, ordinal) in self.own_ordinal_pulls(sources, height, reask_after_blocks) {
            wants.entry(producer).or_default().insert(ordinal);
        }
        wants
    }

    /// Producers to ask for their own ordinal at `height`: those bound to no commitment and
    /// not asked within the last `reask_after_blocks` processed blocks. Records the asks.
    fn own_ordinal_pulls(
        &mut self,
        sources: &[AccountId],
        height: BlockHeight,
        reask_after_blocks: BlockHeight,
    ) -> Vec<(AccountId, u64)> {
        let mut pulls = Vec::new();
        for (ordinal, producer) in sources.iter().enumerate() {
            if self.commitment_by_contributor.contains_key(producer) {
                continue;
            }
            let asked_recently = self
                .own_ordinal_in_flight
                .get(producer)
                .is_some_and(|sent_at| height.saturating_sub(*sent_at) < reask_after_blocks);
            if asked_recently {
                continue;
            }
            self.own_ordinal_in_flight.insert(producer.clone(), height);
            pulls.push((producer.clone(), ordinal as u64));
        }
        pulls
    }

    /// `sender` answered: forgets every outstanding request to it.
    pub(super) fn note_answer_from(&mut self, sender: &AccountId) {
        self.own_ordinal_in_flight.remove(sender);
        for state in self.commitments.values_mut() {
            if let CommitmentState::Tracking(tracker) = state {
                tracker.pull.clear_in_flight_from(sender);
            }
        }
    }
}

impl<P: DataPolicy> SpiceDataManager<P> {
    /// Removes the open items whose data is already in the store.
    pub(super) fn retire_done_items(&mut self, frontier: &CertifiedFrontier) {
        let mut done = Vec::new();
        for id in self.items_by_height.values().flatten() {
            let item = self.items.get(id).expect("index entry names a tracked item");
            if !self.policies.is_pull_open(id, item.height, frontier) {
                continue;
            }
            match self.policies.is_done(id) {
                Ok(true) => done.push(id.clone()),
                Ok(false) => {}
                Err(err) => {
                    tracing::error!(target: "spice_data_distribution", ?err, ?id, "failed to check whether the item is done");
                }
            }
        }
        for id in done {
            self.remove_item(&id);
        }
    }

    /// The requests to send at processed `height`, grouped by producer: walks the open
    /// items from the lowest height up, takes the lowest `pull_window` per herd, and asks
    /// each for its wants. Nothing without a `requester`.
    pub(super) fn pull_requests(
        &mut self,
        height: BlockHeight,
        frontier: &CertifiedFrontier,
        requester: Option<&AccountId>,
    ) -> Vec<PullRequest> {
        let Some(requester) = requester else {
            return Vec::new();
        };
        let reask_after_blocks = self.pull_config.reask_after_blocks;
        let mut pulled_per_herd: HashMap<Herd, usize> = HashMap::new();
        let mut wants_by_producer: BTreeMap<AccountId, BTreeMap<DataId, BTreeSet<u64>>> =
            BTreeMap::new();
        for id in self.items_by_height.values().flatten() {
            let item = self.items.get_mut(id).expect("index entry names a tracked item");
            if !self.policies.is_pull_open(id, item.height, frontier) {
                continue;
            }
            let pulled = pulled_per_herd.entry(id.herd()).or_default();
            if *pulled >= self.pull_config.pull_window {
                continue;
            }
            *pulled += 1;
            let sources = match self.policies.sources(id) {
                Ok(sources) => sources,
                Err(err) => {
                    tracing::error!(target: "spice_data_distribution", ?err, ?id, "failed to resolve the sources to pull from");
                    continue;
                }
            };
            for (producer, ordinals) in
                item.pull_wants(id, &sources, requester, height, reask_after_blocks)
            {
                wants_by_producer
                    .entry(producer)
                    .or_default()
                    .entry(id.clone())
                    .or_default()
                    .extend(ordinals);
            }
        }
        wants_by_producer
            .into_iter()
            .map(|(producer, wants)| PullRequest { producer, wants })
            .collect()
    }
}
