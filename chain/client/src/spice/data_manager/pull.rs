use super::item::{CodedTracker, CommitmentState, FetchItem};
use super::{DataId, FetchPolicy, SpiceDataManager};
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

/// The pulls this node has in flight, and per producer how many and for how many pullable
/// items it is a source.
#[derive(Default)]
pub(super) struct Pulls {
    /// When each request was sent, by the item and the producer pulled from.
    sent_at: HashMap<DataId, HashMap<AccountId, Instant>>,
    /// The same requests, oldest first.
    by_send_time: BTreeSet<(Instant, DataId, AccountId)>,
    load: HashMap<AccountId, ProducerLoad>,
}

#[derive(Default)]
struct ProducerLoad {
    /// Requests in flight to the producer.
    outstanding: usize,
    /// Pullable items this producer is a source for: unbound on them, or bound to a commitment
    /// still collecting.
    sourced_item_count: usize,
}

impl Pulls {
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
        assert!(previous.is_none(), "an item is pulled from a producer once at a time");
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

    /// One more pullable item for which each of `producers` is a source.
    pub(super) fn add_sources(&mut self, producers: impl IntoIterator<Item = AccountId>) {
        for producer in producers {
            self.load.entry(producer).or_default().sourced_item_count += 1;
        }
    }

    /// One fewer pullable item for which each of `producers` is a source.
    pub(super) fn remove_sources(&mut self, producers: impl IntoIterator<Item = AccountId>) {
        for producer in producers {
            self.update_load(&producer, |load| load.sourced_item_count -= 1);
        }
    }

    /// How many pullable items could still be pulled from each producer: items it is a source
    /// for, minus those already being pulled from it. Only producers with a free slot and a count
    /// above zero.
    fn sourced_item_counts_left(&self, cap: usize) -> HashMap<AccountId, usize> {
        self.load
            .iter()
            .filter(|(_, load)| load.outstanding < cap)
            .map(|(producer, load)| {
                (producer.clone(), load.sourced_item_count.saturating_sub(load.outstanding))
            })
            .filter(|(_, items_left)| *items_left > 0)
            .collect()
    }

    /// The producers `id` is being pulled from.
    #[cfg(test)]
    pub(super) fn pulling_from(&self, id: &DataId) -> HashSet<AccountId> {
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
        if load.outstanding == 0 && load.sourced_item_count == 0 {
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
    /// Ordinals of `id` to pull at `now`, grouped by source.
    /// A bound producer is pulled from only by its commitment's tracker, one request at a time;
    /// an unbound one only for its own ordinal.
    fn ordinals_to_pull(
        &mut self,
        id: &DataId,
        now: Instant,
        pulls: &mut Pulls,
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
            if pool.iter().any(|member| pulls.is_outstanding(id, member)) {
                continue;
            }
            pool.sort();
            let tracker = self.tracker_mut(&commitment).expect("live commitment is tracked");
            let Some(source) =
                tracker.take_next_source(&pool, |member| pulls.try_issue(id, member, now, cap))
            else {
                continue;
            };
            // TODO(spice-data-distribution): ask only for as many missing ordinals as decoding
            // still needs.
            wants.entry(source).or_default().extend(tracker.missing_ordinals());
        }
        for (ordinal, (producer, bound)) in self.producers.iter().enumerate() {
            let engaged = bound.is_some() || pulls.is_outstanding(id, producer);
            if engaged || !pulls.try_issue(id, producer, now, cap) {
                continue;
            }
            wants.entry(producer.clone()).or_default().insert(ordinal as u64);
        }
        wants
    }
}

impl<P: FetchPolicy> SpiceDataManager<P> {
    /// The requests to send at `now`, grouped by producer.
    /// Traverses the pullable items from the lowest height and picks the pulls while there are
    /// producers with free pull slots.
    /// Side effects:
    ///  - drops stale requests
    ///  - records every request it returns as "in flight"
    ///  - advances each item's source rotation
    pub(super) fn pulls_to_issue(&mut self, now: Instant) -> Vec<PullRequest> {
        let cap = self.pull_config.max_outstanding_per_producer;
        self.pulls.drop_stale(now, self.pull_config.request_timeout);
        let mut items_left = self.pulls.sourced_item_counts_left(cap);
        let mut wants_by_producer: BTreeMap<AccountId, BTreeMap<DataId, BTreeSet<u64>>> =
            BTreeMap::new();
        for id in self.pullable_items.values().flatten() {
            if items_left.is_empty() {
                break;
            }
            #[cfg(test)]
            {
                self.items_visited_by_traversal += 1;
            }
            let item = self.items.get_mut(id).expect("index entry names a tracked item");
            let sources_counted: HashSet<AccountId> = item
                .pull_sources()
                .into_iter()
                .filter(|producer| {
                    items_left.contains_key(producer) && !self.pulls.is_outstanding(id, producer)
                })
                .collect();
            for (producer, ordinals) in item.ordinals_to_pull(id, now, &mut self.pulls, cap) {
                wants_by_producer.entry(producer).or_default().insert(id.clone(), ordinals);
            }
            for producer in sources_counted {
                let left = items_left.get_mut(&producer).expect("a counted source has items left");
                *left -= 1;
                if *left == 0 || self.pulls.is_saturated(&producer, cap) {
                    items_left.remove(&producer);
                }
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
