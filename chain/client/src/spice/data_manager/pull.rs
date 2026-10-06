use super::item::{CodedTracker, CommitmentState, FetchItem};
use super::{DataId, DataPolicy, SpiceDataManager};
use near_async::time::{Duration, Instant};
use near_primitives::spice::partial_data::SpiceDataCommitment;
use near_primitives::types::AccountId;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
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
    /// How long a pull stays outstanding before it is asked again.
    pub(crate) request_timeout: Duration,
    /// Limits outstanding pulls to one producer, one per item asked, across items.
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

/// The pulls this node has in flight, and per producer how many and on how many pullable
/// items it can still be asked.
#[derive(Default)]
pub(super) struct OutstandingPulls {
    /// When each request was sent, by the item and the producer asked.
    sent_at: HashMap<DataId, HashMap<AccountId, Instant>>,
    /// The same requests, oldest first.
    by_send_time: BTreeSet<(Instant, DataId, AccountId)>,
    load: HashMap<AccountId, ProducerLoad>,
}

#[derive(Default)]
struct ProducerLoad {
    /// Requests in flight to the producer.
    outstanding: usize,
    /// Pullable items on which the producer is unbound or bound to a commitment still
    /// collecting.
    askable_items: usize,
}

impl OutstandingPulls {
    pub(super) fn is_outstanding(&self, id: &DataId, producer: &AccountId) -> bool {
        self.sent_at.get(id).is_some_and(|by_producer| by_producer.contains_key(producer))
    }

    /// Records a request to `producer` for `id` sent at `now`, if the producer has fewer than
    /// `cap` in flight.
    fn try_issue(&mut self, id: &DataId, producer: &AccountId, now: Instant, cap: usize) -> bool {
        let load = self.load.entry(producer.clone()).or_default();
        if load.outstanding >= cap {
            return false;
        }
        load.outstanding += 1;
        let previous = self.sent_at.entry(id.clone()).or_default().insert(producer.clone(), now);
        assert!(previous.is_none(), "a producer is asked for an item once at a time");
        self.by_send_time.insert((now, id.clone(), producer.clone()));
        true
    }

    /// Forgets the request to `producer` for `id`, if any.
    pub(super) fn clear(&mut self, id: &DataId, producer: &AccountId) {
        let Some(sent_at) = self.remove_sent_at(id, producer) else {
            return;
        };
        self.by_send_time.remove(&(sent_at, id.clone(), producer.clone()));
        self.update_load(producer, |load| load.outstanding -= 1);
    }

    /// Forgets the requests unanswered for `request_timeout` as of `now`.
    fn drop_stale(&mut self, now: Instant, request_timeout: Duration) {
        while let Some((sent_at, _, _)) = self.by_send_time.first() {
            if now.signed_duration_since(*sent_at) < request_timeout {
                break;
            }
            let (_, id, producer) = self.by_send_time.pop_first().expect("first entry exists");
            self.remove_sent_at(&id, &producer);
            self.update_load(&producer, |load| load.outstanding -= 1);
        }
    }

    fn remove_sent_at(&mut self, id: &DataId, producer: &AccountId) -> Option<Instant> {
        let by_producer = self.sent_at.get_mut(id)?;
        let sent_at = by_producer.remove(producer)?;
        if by_producer.is_empty() {
            self.sent_at.remove(id);
        }
        Some(sent_at)
    }

    /// One more pullable item on which each of `producers` can be asked.
    pub(super) fn add_askable(&mut self, producers: impl IntoIterator<Item = AccountId>) {
        for producer in producers {
            self.load.entry(producer).or_default().askable_items += 1;
        }
    }

    /// One fewer pullable item on which each of `producers` can be asked.
    pub(super) fn remove_askable(&mut self, producers: impl IntoIterator<Item = AccountId>) {
        for producer in producers {
            self.update_load(&producer, |load| load.askable_items -= 1);
        }
    }

    /// The producers with an item to be asked on and fewer than `cap` requests in flight.
    fn unsaturated_askable(&self, cap: usize) -> HashSet<AccountId> {
        self.load
            .iter()
            .filter(|(_, load)| load.askable_items > 0 && load.outstanding < cap)
            .map(|(producer, _)| producer.clone())
            .collect()
    }

    /// The producers asked for `id` with the request in flight.
    #[cfg(test)]
    pub(super) fn asked_for(&self, id: &DataId) -> HashSet<AccountId> {
        self.sent_at
            .get(id)
            .map(|by_producer| by_producer.keys().cloned().collect())
            .unwrap_or_default()
    }

    fn is_saturated(&self, producer: &AccountId, cap: usize) -> bool {
        self.load.get(producer).is_some_and(|load| load.outstanding >= cap)
    }

    /// Applies `update` to `producer`'s load, dropping the entry once it is empty.
    fn update_load(&mut self, producer: &AccountId, update: impl FnOnce(&mut ProducerLoad)) {
        let load =
            self.load.get_mut(producer).expect("a producer with a request or an item has a load");
        update(load);
        if load.outstanding == 0 && load.askable_items == 0 {
            self.load.remove(producer);
        }
    }
}

impl CodedTracker {
    /// The first member of `pool`, in rotation order, that `try_issue` accepts; moves the
    /// rotation past that member. With no such member the rotation stays where it is.
    fn take_next_source(
        &mut self,
        pool: &[AccountId],
        mut try_issue: impl FnMut(&AccountId) -> bool,
    ) -> Option<AccountId> {
        if pool.is_empty() {
            return None;
        }
        let start = (self.rotation_cursor % pool.len() as u64) as usize;
        let member = |offset: usize| &pool[(start + offset) % pool.len()];
        let offset = (0..pool.len()).find(|offset| try_issue(member(*offset)))?;
        let source = member(offset).clone();
        // the next rotation starts right after the member asked
        self.rotation_cursor = self.rotation_cursor.wrapping_add(offset as u64 + 1);
        Some(source)
    }
}

impl FetchItem {
    /// Producers to ask for `id` at `now`, with the ordinals to ask each, each request
    /// recorded in `outstanding`. A bound producer is asked only by its commitment's tracker,
    /// one request at a time; an unbound one only for its own ordinal.
    fn pull_wants(
        &mut self,
        id: &DataId,
        now: Instant,
        outstanding: &mut OutstandingPulls,
        cap: usize,
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
            if pool.iter().any(|member| outstanding.is_outstanding(id, member)) {
                continue;
            }
            pool.sort();
            let tracker = self.tracker_mut(&commitment).expect("live commitment is tracked");
            let Some(source) = tracker
                .take_next_source(&pool, |member| outstanding.try_issue(id, member, now, cap))
            else {
                continue;
            };
            // TODO(spice-data-distribution): ask only for as many missing ordinals as decoding
            // still needs.
            wants.entry(source).or_default().extend(tracker.missing_ordinals());
        }
        for (ordinal, (producer, bound)) in self.producers.iter().enumerate() {
            let engaged = bound.is_some() || outstanding.is_outstanding(id, producer);
            if engaged || !outstanding.try_issue(id, producer, now, cap) {
                continue;
            }
            wants.entry(producer.clone()).or_default().insert(ordinal as u64);
        }
        wants
    }
}

impl<P: DataPolicy> SpiceDataManager<P> {
    /// The requests to send at `now`, grouped by producer. Walks the pullable items from the
    /// lowest height and stops once no producer with an item to be asked on has a free slot.
    pub(super) fn pull_requests(&mut self, now: Instant) -> Vec<PullRequest> {
        let cap = self.pull_config.max_outstanding_per_producer;
        self.outstanding.drop_stale(now, self.pull_config.request_timeout);
        let mut unsaturated = self.outstanding.unsaturated_askable(cap);
        let mut wants_by_producer: BTreeMap<AccountId, BTreeMap<DataId, BTreeSet<u64>>> =
            BTreeMap::new();
        for id in self.pullable.values().flatten() {
            if unsaturated.is_empty() {
                break;
            }
            #[cfg(test)]
            {
                self.items_visited_by_pulls += 1;
            }
            let item = self.items.get_mut(id).expect("index entry names a tracked item");
            for (producer, ordinals) in item.pull_wants(id, now, &mut self.outstanding, cap) {
                if self.outstanding.is_saturated(&producer, cap) {
                    unsaturated.remove(&producer);
                }
                wants_by_producer.entry(producer).or_default().insert(id.clone(), ordinals);
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
            for ordinal in ordinals {
                let full = current_parts == max_parts_per_request
                    || (current.len() == max_ids_per_request && !current.contains_key(&id));
                if full {
                    requests.push(PullRequest {
                        producer: producer.clone(),
                        wants: take(&mut current),
                    });
                    current_parts = 0;
                }
                current.entry(id.clone()).or_default().insert(ordinal);
                current_parts += 1;
            }
        }
        if !current.is_empty() {
            requests.push(PullRequest { producer, wants: current });
        }
        requests
    }
}
