use super::item::{CommitmentState, FetchItem};
use super::{ChainView, DataId, DataPolicy, SpiceDataManager};
use near_async::time::{Duration, Instant};
use near_primitives::spice::partial_data::SpiceDataCommitment;
use near_primitives::types::{AccountId, BlockHeight, ShardId};
use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::hash::{Hash, Hasher as _};
use std::mem::replace;
use time::ext::InstantExt as _;

/// One pull request to send: the ordinals asked of `producer` for each item.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PullRequest {
    pub(crate) producer: AccountId,
    pub(crate) wants: BTreeMap<DataId, BTreeSet<u64>>,
}

/// Pull related settings controlling the pace/rates.
#[derive(Debug, Clone)]
pub(crate) struct PullConfig {
    /// How long a pull request stays outstanding before it is sent again.
    pub(crate) request_timeout: Duration,
    /// Limits outstanding requests to one producer. Across items.
    pub(crate) max_outstanding_per_producer: usize,
    /// Items one request may carry.
    pub(crate) max_ids_per_request: usize,
    /// Ordinals one request may carry, summed over its items.
    pub(crate) max_parts_per_request: usize,
}

impl Default for PullConfig {
    fn default() -> Self {
        Self {
            request_timeout: Duration::milliseconds(1200),
            max_outstanding_per_producer: 4,
            max_ids_per_request: 32,
            max_parts_per_request: 256,
        }
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

/// The requests each producer holds from this node at one trigger, against the cap.
pub(super) struct ProducerBudget {
    cap: usize,
    outstanding: HashMap<AccountId, usize>,
}

impl ProducerBudget {
    fn new<'a>(cap: usize, outstanding: impl Iterator<Item = &'a AccountId>) -> Self {
        let mut budget = Self { cap, outstanding: HashMap::new() };
        for producer in outstanding {
            budget.take(producer);
        }
        budget
    }

    fn has_slot(&self, producer: &AccountId) -> bool {
        self.outstanding.get(producer).copied().unwrap_or(0) < self.cap
    }

    fn take(&mut self, producer: &AccountId) {
        *self.outstanding.entry(producer.clone()).or_default() += 1;
    }
}

/// One outstanding pull request to one source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct InFlightRequest {
    pub(super) source: AccountId,
    /// When the request was sent.
    pub(super) sent_at: Instant,
}

/// A tracker's pull requests state.
#[derive(Debug, Default)]
pub(super) struct PullState {
    pub(super) in_flight: Option<InFlightRequest>,
    /// Used to rotate the pool members requested for missing ordinals.
    pub(super) rotation_cursor: u64,
}

impl PullState {
    /// Drops the outstanding request once it has gone unanswered for `request_timeout` as of
    /// `now`.
    fn drop_stale(&mut self, now: Instant, request_timeout: Duration) {
        let stale = self
            .in_flight
            .as_ref()
            .is_some_and(|request| now.signed_duration_since(request.sent_at) >= request_timeout);
        if stale {
            self.in_flight = None;
        }
    }

    /// If no request is outstanding, records one to the first member of `pool`, in rotation
    /// order, with a slot in `budget`, and returns it. With no such member the rotation
    /// stays where it is.
    pub(super) fn next_source(
        &mut self,
        pool: &[AccountId],
        key: &impl Hash,
        requester: &AccountId,
        now: Instant,
        budget: &mut ProducerBudget,
    ) -> Option<AccountId> {
        if self.in_flight.is_some() || pool.is_empty() {
            return None;
        }
        let start = rotated_source_index(pool.len(), key, requester, self.rotation_cursor);
        let member = |offset: usize| &pool[(start + offset) % pool.len()];
        let offset = (0..pool.len()).find(|offset| budget.has_slot(member(*offset)))?;
        let source = member(offset).clone();
        budget.take(&source);
        self.in_flight = Some(InFlightRequest { source: source.clone(), sent_at: now });
        // the next rotation starts right after the member asked
        self.rotation_cursor += offset as u64 + 1;
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
    /// Drops every request unanswered for `request_timeout` as of `now`.
    pub(super) fn drop_stale_requests(&mut self, now: Instant, request_timeout: Duration) {
        self.requests_to_unbound
            .retain(|_, sent_at| now.signed_duration_since(*sent_at) < request_timeout);
        for tracker in self.live_trackers_mut() {
            tracker.pull.drop_stale(now, request_timeout);
        }
    }

    /// The producer of each request this item holds.
    pub(super) fn outstanding_requests(&self) -> impl Iterator<Item = &AccountId> {
        let tracker_requests = self.commitments.values().filter_map(|state| match state {
            CommitmentState::Tracking(tracker) => {
                tracker.pull.in_flight.as_ref().map(|request| &request.source)
            }
            CommitmentState::Settled => None,
        });
        self.requests_to_unbound.keys().chain(tracker_requests)
    }

    /// Producers to ask at `now`, with the ordinals to ask each.
    pub(super) fn pull_wants(
        &mut self,
        id: &DataId,
        requester: &AccountId,
        now: Instant,
        budget: &mut ProducerBudget,
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
            let Some(source) =
                tracker.pull.next_source(&pool, &(id, &commitment), requester, now, budget)
            else {
                continue;
            };
            wants.entry(source).or_default().extend(tracker.missing_ordinals());
        }
        for (ordinal, producer) in self.sources.iter().enumerate() {
            if self.commitment_by_contributor.contains_key(producer)
                || self.requests_to_unbound.contains_key(producer)
                || !budget.has_slot(producer)
            {
                continue;
            }
            budget.take(producer);
            self.requests_to_unbound.insert(producer.clone(), now);
            wants.entry(producer.clone()).or_default().insert(ordinal as u64);
        }
        wants
    }

    /// `sender` answered: forgets every outstanding request to it.
    pub(super) fn note_answer_from(&mut self, sender: &AccountId) {
        self.requests_to_unbound.remove(sender);
        for tracker in self.live_trackers_mut() {
            tracker.pull.clear_in_flight_from(sender);
        }
    }
}

impl<P: DataPolicy + ChainView> SpiceDataManager<P> {
    /// Removes the pullable items whose delivered data is in the store.
    pub(super) fn retire_done_items(&mut self, certified_frontier: &HashMap<ShardId, BlockHeight>) {
        let mut done = Vec::new();
        for id in self.items_by_height.values().flatten() {
            let item = self.items.get(id).expect("index entry names a tracked item");
            if !item.delivered || !self.policies.is_pullable(id, item.height, certified_frontier) {
                continue;
            }
            match self.policies.is_done(id) {
                Ok(true) => done.push(id.clone()),
                Ok(false) => {}
                Err(err) => {
                    tracing::debug!(target: "spice_data_distribution", ?err, ?id, "failed to check whether the item is done");
                }
            }
        }
        for id in done {
            self.remove_item(&id);
        }
    }

    /// The requests to send at `now`, grouped by producer.
    pub(super) fn pull_requests(
        &mut self,
        now: Instant,
        certified_frontier: &HashMap<ShardId, BlockHeight>,
        requester: Option<&AccountId>,
    ) -> Vec<PullRequest> {
        let Some(requester) = requester else {
            return Vec::new();
        };
        let request_timeout = self.pull_config.request_timeout;
        for item in self.items.values_mut() {
            item.drop_stale_requests(now, request_timeout);
        }
        let mut budget = ProducerBudget::new(
            self.pull_config.max_outstanding_per_producer,
            self.items.values().flat_map(FetchItem::outstanding_requests),
        );
        let mut wants_by_producer: BTreeMap<AccountId, BTreeMap<DataId, BTreeSet<u64>>> =
            BTreeMap::new();
        for id in self.items_by_height.values().flatten() {
            let item = self.items.get_mut(id).expect("index entry names a tracked item");
            if !self.policies.is_pullable(id, item.height, certified_frontier) {
                continue;
            }
            for (producer, ordinals) in item.pull_wants(id, requester, now, &mut budget) {
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
            .flat_map(|(producer, wants)| self.pack_requests(producer, wants))
            .collect()
    }

    /// Splits a producer's wants into requests within `max_ids_per_request` and
    /// `max_parts_per_request`, items in order.
    fn pack_requests(
        &self,
        producer: AccountId,
        wants: BTreeMap<DataId, BTreeSet<u64>>,
    ) -> Vec<PullRequest> {
        let PullConfig { max_ids_per_request, max_parts_per_request, .. } = self.pull_config;
        let mut requests = Vec::new();
        let mut current: BTreeMap<DataId, BTreeSet<u64>> = BTreeMap::new();
        let mut current_parts = 0;
        for (id, ordinals) in wants {
            debug_assert!(
                ordinals.len() <= max_parts_per_request,
                "one item's ask exceeds the parts a request may carry"
            );
            let overflows = current.len() + 1 > max_ids_per_request
                || current_parts + ordinals.len() > max_parts_per_request;
            if overflows && !current.is_empty() {
                let wants = replace(&mut current, BTreeMap::new());
                requests.push(PullRequest { producer: producer.clone(), wants });
                current_parts = 0;
            }
            current_parts += ordinals.len();
            current.insert(id, ordinals);
        }
        if !current.is_empty() {
            requests.push(PullRequest { producer, wants: current });
        }
        requests
    }
}
