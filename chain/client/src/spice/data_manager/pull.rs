use super::item::{CodedTracker, CommitmentState, FetchItem};
use super::{DataId, DataPolicy, SpiceDataManager};
use near_async::time::{Duration, Instant};
use near_primitives::spice::partial_data::SpiceDataCommitment;
use near_primitives::types::AccountId;
use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::hash::{Hash, Hasher as _};
use std::mem::take;
use std::num::NonZeroUsize;
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
    pub(crate) max_ids_per_request: NonZeroUsize,
    /// Ordinals one request may carry, summed over its items.
    pub(crate) max_parts_per_request: NonZeroUsize,
}

impl Default for PullConfig {
    fn default() -> Self {
        Self {
            request_timeout: Duration::milliseconds(600),
            max_outstanding_per_producer: 4,
            max_ids_per_request: const { NonZeroUsize::new(32).unwrap() },
            max_parts_per_request: const { NonZeroUsize::new(256).unwrap() },
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

/// The requests outstanding for each producer from this node at one trigger, against the cap.
pub(super) struct ProducerBudget {
    cap: usize,
    outstanding: HashMap<AccountId, usize>,
}

impl ProducerBudget {
    fn new<'a>(cap: usize, outstanding: impl Iterator<Item = &'a AccountId>) -> Self {
        let mut budget = Self { cap, outstanding: HashMap::new() };
        for producer in outstanding {
            *budget.outstanding.entry(producer.clone()).or_default() += 1;
        }
        budget
    }

    /// Takes a slot for `producer` if it has one under the cap.
    fn try_take(&mut self, producer: &AccountId) -> bool {
        let outstanding = self.outstanding.entry(producer.clone()).or_default();
        if *outstanding >= self.cap {
            return false;
        }
        *outstanding += 1;
        true
    }
}

impl CodedTracker {
    /// The first member of `pool`, in rotation order, with a slot in `budget`: takes the
    /// slot and moves the rotation past that member. With no such member the rotation
    /// stays where it is.
    fn take_next_source(
        &mut self,
        pool: &[AccountId],
        budget: &mut ProducerBudget,
    ) -> Option<AccountId> {
        if pool.is_empty() {
            return None;
        }
        let start = (self.rotation_cursor % pool.len() as u64) as usize;
        let member = |offset: usize| &pool[(start + offset) % pool.len()];
        let offset = (0..pool.len()).find(|offset| budget.try_take(member(*offset)))?;
        let source = member(offset).clone();
        // the next rotation starts right after the member asked
        self.rotation_cursor = self.rotation_cursor.wrapping_add(offset as u64 + 1);
        Some(source)
    }
}

impl FetchItem {
    /// Drops every pull unanswered for `request_timeout` as of `now`.
    pub(super) fn drop_stale_pulls(&mut self, now: Instant, request_timeout: Duration) {
        for (_, state) in &mut self.producers {
            let stale = state
                .requested_at
                .is_some_and(|sent_at| now.signed_duration_since(sent_at) >= request_timeout);
            if stale {
                state.requested_at = None;
            }
        }
    }

    /// The producers with a pull from this item unanswered.
    pub(super) fn outstanding_pulls(&self) -> impl Iterator<Item = &AccountId> {
        self.producers
            .iter()
            .filter(|(_, state)| state.requested_at.is_some())
            .map(|(producer, _)| producer)
    }

    /// Producers to ask at `now`, with the ordinals to ask each. A bound producer is asked
    /// only by its commitment's tracker, one request at a time; an unbound one only for its
    /// own ordinal.
    pub(super) fn pull_wants(
        &mut self,
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
            let asked = self.producers.iter().any(|(_, state)| {
                state.commitment.as_ref() == Some(&commitment) && state.requested_at.is_some()
            });
            if asked {
                continue;
            }
            let mut pool: Vec<AccountId> =
                self.contributors(&commitment).into_iter().cloned().collect();
            pool.sort();
            let tracker = self.tracker_mut(&commitment).expect("live commitment is tracked");
            let Some(source) = tracker.take_next_source(&pool, budget) else {
                continue;
            };
            let missing = tracker.missing_ordinals();
            self.producer_mut(&source).expect("pool member is a producer").requested_at = Some(now);
            wants.entry(source).or_default().extend(missing);
        }
        for (ordinal, (producer, state)) in self.producers.iter_mut().enumerate() {
            let engaged = state.commitment.is_some() || state.requested_at.is_some();
            if engaged || !budget.try_take(producer) {
                continue;
            }
            state.requested_at = Some(now);
            wants.entry(producer.clone()).or_default().insert(ordinal as u64);
        }
        wants
    }

    /// `sender` answered: forgets the pull outstanding to it.
    pub(super) fn note_pull_response(&mut self, sender: &AccountId) {
        if let Some(state) = self.producer_mut(sender) {
            state.requested_at = None;
        }
    }
}

impl<P: DataPolicy> SpiceDataManager<P> {
    /// The requests to send at `now`, grouped by producer.
    pub(super) fn pull_requests(&mut self, now: Instant) -> Vec<PullRequest> {
        let request_timeout = self.pull_config.request_timeout;
        for item in self.items.values_mut() {
            item.drop_stale_pulls(now, request_timeout);
        }
        let mut budget = ProducerBudget::new(
            self.pull_config.max_outstanding_per_producer,
            self.items.values().flat_map(FetchItem::outstanding_pulls),
        );
        let mut wants_by_producer: BTreeMap<AccountId, BTreeMap<DataId, BTreeSet<u64>>> =
            BTreeMap::new();
        for id in self.items_by_height.values().flatten() {
            let item = self.items.get_mut(id).expect("index entry names a tracked item");
            if !item.is_pullable() {
                continue;
            }
            for (producer, ordinals) in item.pull_wants(now, &mut budget) {
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
    /// `max_parts_per_request`; one item's ordinals may span requests.
    fn pack_requests(
        &self,
        producer: AccountId,
        wants: BTreeMap<DataId, BTreeSet<u64>>,
    ) -> Vec<PullRequest> {
        let max_ids_per_request = self.pull_config.max_ids_per_request.get();
        let max_parts_per_request = self.pull_config.max_parts_per_request.get();
        let mut requests = Vec::new();
        let mut current: BTreeMap<DataId, BTreeSet<u64>> = BTreeMap::new();
        let mut current_parts = 0;
        for (id, ordinals) in wants {
            let mut ordinals = ordinals.into_iter().peekable();
            while ordinals.peek().is_some() {
                let full =
                    current.len() >= max_ids_per_request || current_parts >= max_parts_per_request;
                if full && !current.is_empty() {
                    let wants = take(&mut current);
                    requests.push(PullRequest { producer: producer.clone(), wants });
                    current_parts = 0;
                }
                let room = max_parts_per_request - current_parts;
                let chunk: BTreeSet<u64> = ordinals.by_ref().take(room).collect();
                current_parts += chunk.len();
                current.insert(id.clone(), chunk);
            }
        }
        if !current.is_empty() {
            requests.push(PullRequest { producer, wants: current });
        }
        requests
    }
}
