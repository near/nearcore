use super::item::FetchItem;
use super::items::Items;
use super::{DataId, MAX_REQUESTED_DATA_IDS, MAX_REQUESTED_PARTS, MAX_REQUESTED_WITNESSES};
use near_async::time::{Duration, Instant};
use near_primitives::types::AccountId;
use rand::seq::SliceRandom;
use rand::thread_rng;
#[cfg(test)]
use std::collections::HashSet;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::mem::take;
use std::num::NonZeroUsize;
use time::ext::InstantExt as _;

/// One pull request to send: the ordinals requested from `producer` for each item.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PullRequest {
    pub(crate) producer: AccountId,
    pub(crate) wants: BTreeMap<DataId, BTreeSet<u64>>,
}

/// Pacing and size limits of pulling.
#[derive(Debug, Clone)]
pub(crate) struct Config {
    /// How long a request stays outstanding before it is repeated.
    pub(crate) request_timeout: Duration,
    /// Heights of pushed items pulled at one trigger: the lowest ones holding pullable items.
    pub(crate) pulled_heights_of_pushed_items: NonZeroUsize,
    /// Heights of pull-only items pulled at one trigger for each destination shard: the lowest
    /// ones holding pullable items into it.
    pub(crate) pulled_heights_of_pull_only_items: NonZeroUsize,
    /// Items one request may carry.
    pub(crate) max_ids_per_request: NonZeroUsize,
    /// Witness items one request may carry.
    pub(crate) max_witnesses_per_request: NonZeroUsize,
    /// Ordinals one request may carry, summed over its items.
    pub(crate) max_parts_per_request: NonZeroUsize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            request_timeout: Duration::milliseconds(600),
            pulled_heights_of_pushed_items: const { NonZeroUsize::new(4).unwrap() },
            pulled_heights_of_pull_only_items: const { NonZeroUsize::new(2).unwrap() },
            max_ids_per_request: const { NonZeroUsize::new(MAX_REQUESTED_DATA_IDS).unwrap() },
            max_witnesses_per_request: const { NonZeroUsize::new(MAX_REQUESTED_WITNESSES).unwrap() },
            max_parts_per_request: const { NonZeroUsize::new(MAX_REQUESTED_PARTS).unwrap() },
        }
    }
}

/// Everything about pulls.
/// At a trigger decides which pullable items to request, from which producers,
/// when an unanswered request is to be repeated.
/// Packs all that into requests.
pub(super) struct Pulls {
    config: Config,
    /// The last request made to each producer for each item.
    requests: HashMap<DataId, HashMap<AccountId, Request>>,
}

/// A request made to one producer for one item.
struct Request {
    created_at: Instant,
    /// A verified answer arrived; until then the request is outstanding for `request_timeout`
    /// after `created_at`.
    answered: bool,
}

impl Pulls {
    pub(super) fn new(config: Config) -> Self {
        Self { config, requests: HashMap::new() }
    }

    /// Records a verified answer from `producer` for `id`.
    pub(super) fn record_answer(&mut self, id: &DataId, producer: &AccountId) {
        let request = self.requests.get_mut(id).and_then(|requests| requests.get_mut(producer));
        if let Some(request) = request {
            request.answered = true;
        }
    }

    /// Forgets the requests made for `id`.
    pub(super) fn forget_requests(&mut self, id: &DataId) {
        self.requests.remove(id);
    }

    /// The requests to issue at `now`, grouped by producer, those for pushed items first.
    /// Traverses the pullable pushed items of the `pulled_heights_of_pushed_items` lowest heights and the pullable
    /// pull-only items of the `pulled_heights_of_pull_only_items` lowest heights into each destination shard,
    /// looked up in `items`. Records every request it returns as created at `now`.
    pub(super) fn to_issue(&mut self, now: Instant, items: &Items) -> Vec<PullRequest> {
        self.issue(now, items, |_| true)
    }

    /// The requests `to_issue` returns, restricted to receipt proofs.
    pub(super) fn receipt_proofs_to_issue(
        &mut self,
        now: Instant,
        items: &Items,
    ) -> Vec<PullRequest> {
        self.issue(now, items, |id| matches!(id, DataId::ReceiptProof(_)))
    }

    fn issue(
        &mut self,
        now: Instant,
        items: &Items,
        included: fn(&DataId) -> bool,
    ) -> Vec<PullRequest> {
        let trigger = Trigger::new(now, self.config.request_timeout);
        let push_ids = items
            .lowest_pullable_pushed(self.config.pulled_heights_of_pushed_items)
            .filter(|id| included(id));
        let push_wants = wants_by_producer(push_ids, items, &mut self.requests, &trigger);
        let pull_only_ids = items
            .lowest_pullable_pull_only(self.config.pulled_heights_of_pull_only_items)
            .filter(|id| included(id));
        let pull_only_wants = wants_by_producer(pull_only_ids, items, &mut self.requests, &trigger);
        // TODO(spice-data-distribution): send pull-only requests in a serve-side lane of their own,
        // so they cannot crowd out the requests for pushed items under the server's rate limit.
        push_wants
            .into_iter()
            .chain(pull_only_wants)
            .flat_map(|(producer, wants)| self.pack_requests(producer, wants))
            .collect()
    }

    /// Splits a producer's wants into requests within the request limits; one item's ordinals
    /// may span requests.
    // TODO(spice-data-distribution): a request the server's rate limit drops takes the receipt
    // proofs packed with a witness with it, until the timeout retries them.
    fn pack_requests(
        &self,
        producer: AccountId,
        wants: BTreeMap<DataId, BTreeSet<u64>>,
    ) -> Vec<PullRequest> {
        let max_ids_per_request = self.config.max_ids_per_request.get();
        let max_witnesses_per_request = self.config.max_witnesses_per_request.get();
        let max_parts_per_request = self.config.max_parts_per_request.get();
        let mut requests = Vec::new();
        let mut current: BTreeMap<DataId, BTreeSet<u64>> = BTreeMap::new();
        let mut current_parts = 0;
        let mut current_witnesses = 0;
        for (id, ordinals) in wants {
            let is_witness = matches!(id, DataId::Witness(_));
            for ordinal in ordinals {
                let new_id = !current.contains_key(&id);
                let full = current_parts == max_parts_per_request
                    || (new_id && current.len() == max_ids_per_request)
                    || (new_id && is_witness && current_witnesses == max_witnesses_per_request);
                if full {
                    requests.push(PullRequest {
                        producer: producer.clone(),
                        wants: take(&mut current),
                    });
                    current_parts = 0;
                    current_witnesses = 0;
                }
                if is_witness && !current.contains_key(&id) {
                    current_witnesses += 1;
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

    /// The producers with an outstanding request for `id` at `now`.
    #[cfg(test)]
    pub(super) fn pulling_from(&self, id: &DataId, now: Instant) -> HashSet<AccountId> {
        let Some(requests) = self.requests.get(id) else {
            return HashSet::new();
        };
        let trigger = Trigger::new(now, self.config.request_timeout);
        requests
            .iter()
            .filter(|(_, request)| request.is_outstanding(&trigger))
            .map(|(producer, _)| producer.clone())
            .collect()
    }

    /// Whether any request is remembered for `id`.
    #[cfg(test)]
    pub(super) fn holds_state_for(&self, id: &DataId) -> bool {
        self.requests.contains_key(id)
    }
}

/// The ordinals to request for `ids`, looked up in `items`, grouped by producer.
fn wants_by_producer<'a>(
    ids: impl Iterator<Item = &'a DataId>,
    items: &Items,
    requests: &mut HashMap<DataId, HashMap<AccountId, Request>>,
    trigger: &Trigger,
) -> BTreeMap<AccountId, BTreeMap<DataId, BTreeSet<u64>>> {
    let mut wants_by_producer: BTreeMap<AccountId, BTreeMap<DataId, BTreeSet<u64>>> =
        BTreeMap::new();
    for id in ids {
        let item = items.get(id).expect("pullable id names a tracked item");
        let item_requests = ItemRequests { id, requests, trigger };
        for (producer, ordinals) in item_requests.ordinals_to_request(item) {
            wants_by_producer.entry(producer).or_default().insert(id.clone(), ordinals);
        }
    }
    wants_by_producer
}

impl Request {
    fn new(created_at: Instant) -> Self {
        Self { created_at, answered: false }
    }

    fn is_outstanding(&self, trigger: &Trigger) -> bool {
        !self.answered && self.created_at > trigger.stale_cutoff
    }
}

/// One trigger's view of request timing.
struct Trigger {
    /// The creation time of every request this trigger issues.
    at: Instant,
    /// Requests created at or before this time are stale.
    stale_cutoff: Instant,
}

impl Trigger {
    fn new(now: Instant, request_timeout: Duration) -> Self {
        Self { at: now, stale_cutoff: now.sub_signed(request_timeout) }
    }
}

/// One item's requests, judged at one trigger.
struct ItemRequests<'a> {
    id: &'a DataId,
    /// The last request made to each producer, by item.
    requests: &'a mut HashMap<DataId, HashMap<AccountId, Request>>,
    trigger: &'a Trigger,
}

impl ItemRequests<'_> {
    /// Ordinals of `item` to request, grouped by source.
    /// Each live commitment's pool gets one request at a time, made to its least recently
    /// requested member; each unbound producer is requested only its own ordinal.
    fn ordinals_to_request(mut self, item: &FetchItem) -> BTreeMap<AccountId, BTreeSet<u64>> {
        let mut wants: BTreeMap<AccountId, BTreeSet<u64>> = BTreeMap::new();
        for (pool, tracker) in item.live_pools() {
            let Some(source) = self.select_source(&pool) else {
                continue;
            };
            // TODO(spice-data-distribution): request only as many missing ordinals as decoding
            // still needs.
            wants.entry(source).or_default().extend(tracker.missing_ordinals());
        }
        for (ordinal, producer) in item.unbound_producers() {
            if self.try_request(producer) {
                wants.entry(producer.clone()).or_default().insert(ordinal);
            }
        }
        wants
    }

    fn last_request(&self, producer: &AccountId) -> Option<&Request> {
        self.requests.get(self.id)?.get(producer)
    }

    fn is_outstanding(&self, producer: &AccountId) -> bool {
        self.last_request(producer).is_some_and(|request| request.is_outstanding(self.trigger))
    }

    fn record(&mut self, producer: AccountId) {
        let request = Request::new(self.trigger.at);
        self.requests.entry(self.id.clone()).or_default().insert(producer, request);
    }

    /// Picks the member of `pool` to request from and records the request.
    /// Returns `None` for an empty pool or while any member has an outstanding request.
    /// Prefers a member never requested from, then the one requested from longest ago.
    /// Breaks ties at random, so requesters spread over the pool.
    fn select_source(&mut self, pool: &[&AccountId]) -> Option<AccountId> {
        if pool.iter().any(|member| self.is_outstanding(member)) {
            return None;
        }
        let last_created = |member: &AccountId| self.last_request(member).map(|r| r.created_at);
        let oldest = pool.iter().map(|member| last_created(member)).min()?;
        let candidates: Vec<&AccountId> =
            pool.iter().copied().filter(|member| last_created(member) == oldest).collect();
        let source = candidates
            .choose(&mut thread_rng())
            .copied()
            .expect("the oldest creation time comes from a member of the pool")
            .clone();
        self.record(source.clone());
        Some(source)
    }

    /// Requests from `producer` unless its request is outstanding.
    fn try_request(&mut self, producer: &AccountId) -> bool {
        if self.is_outstanding(producer) {
            return false;
        }
        self.record(producer.clone());
        true
    }
}
