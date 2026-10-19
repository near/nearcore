use super::item::FetchItem;
use super::{DataId, remove_indexed};
use near_async::time::{Duration, Instant};
use near_primitives::types::{AccountId, BlockHeight};
use rand::seq::SliceRandom;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
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
    /// Heights pulled at one trigger: the lowest heights holding pullable items.
    pub(crate) pulled_heights: NonZeroUsize,
    /// Items one request may carry.
    pub(crate) max_ids_per_request: NonZeroUsize,
    /// Ordinals one request may carry, summed over its items.
    pub(crate) max_parts_per_request: NonZeroUsize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            request_timeout: Duration::milliseconds(600),
            pulled_heights: const { NonZeroUsize::new(4).unwrap() },
            max_ids_per_request: const { NonZeroUsize::new(32).unwrap() },
            max_parts_per_request: const { NonZeroUsize::new(256).unwrap() },
        }
    }
}

/// Everything about pulls.
/// At a trigger decides which pullable items to request, from which producers,
/// when an unanswered request is to be repeated.
/// Packs all that into requests.
pub(super) struct Pulls {
    config: Config,
    /// Ids of tracked items that may be pulled, by height.
    pullable: BTreeMap<BlockHeight, HashSet<DataId>>,
    /// The last request made to each producer for each item.
    requests: HashMap<DataId, HashMap<AccountId, Request>>,
}

/// A request made to one producer for one item.
struct Request {
    created_at: Instant,
    status: RequestStatus,
}

enum RequestStatus {
    /// No verified answer yet; outstanding until `request_timeout` after it was created.
    Issued,
    /// A verified answer arrived.
    Answered,
}

impl Pulls {
    pub(super) fn new(config: Config) -> Self {
        Self { config, pullable: BTreeMap::new(), requests: HashMap::new() }
    }

    /// Makes `id`, tracked at `height`, open to pulling.
    pub(super) fn mark_pullable(&mut self, id: DataId, height: BlockHeight) {
        self.pullable.entry(height).or_default().insert(id);
    }

    /// Records a verified answer from `producer` for `id`.
    pub(super) fn record_answer(&mut self, id: &DataId, producer: &AccountId) {
        let request = self.requests.get_mut(id).and_then(|requests| requests.get_mut(producer));
        if let Some(request) = request {
            request.status = RequestStatus::Answered;
        }
    }

    /// Removes `id`, tracked at `height`, from the pullable items and forgets its requests.
    pub(super) fn forget(&mut self, id: &DataId, height: BlockHeight) {
        remove_indexed(&mut self.pullable, height, id);
        self.requests.remove(id);
    }

    /// Removes every item below `height` from the pullable items and forgets their requests.
    pub(super) fn forget_below(&mut self, height: BlockHeight) {
        let mut below = take(&mut self.pullable);
        self.pullable = below.split_off(&height);
        for id in below.into_values().flatten() {
            self.requests.remove(&id);
        }
    }

    /// The requests to issue at `now`, grouped by producer.
    /// Traverses the pullable items of the `pulled_heights` lowest heights, looked up in `items`.
    /// Records every request it returns as created at `now`.
    pub(super) fn to_issue(
        &mut self,
        now: Instant,
        items: &HashMap<DataId, FetchItem>,
    ) -> Vec<PullRequest> {
        let trigger = Trigger::new(now, self.config.request_timeout);
        let mut wants_by_producer: BTreeMap<AccountId, BTreeMap<DataId, BTreeSet<u64>>> =
            BTreeMap::new();
        for id in self.pullable.values().take(self.config.pulled_heights.get()).flatten() {
            let item = items.get(id).expect("pullable id names a tracked item");
            let requests = ItemRequests { id, requests: &mut self.requests, trigger: &trigger };
            for (producer, ordinals) in requests.ordinals_to_request(item) {
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
        let max_ids_per_request = self.config.max_ids_per_request.get();
        let max_parts_per_request = self.config.max_parts_per_request.get();
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

    #[cfg(test)]
    pub(super) fn set_config(&mut self, config: Config) {
        self.config = config;
    }

    #[cfg(test)]
    pub(super) fn is_pullable(&self, id: &DataId) -> bool {
        self.pullable.values().any(|ids| ids.contains(id))
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

impl Request {
    fn new(created_at: Instant) -> Self {
        Self { created_at, status: RequestStatus::Issued }
    }

    fn is_outstanding(&self, trigger: &Trigger) -> bool {
        matches!(self.status, RequestStatus::Issued) && self.created_at > trigger.stale_cutoff
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
        let candidates: Vec<&&AccountId> =
            pool.iter().filter(|member| last_created(member) == oldest).collect();
        let source = (**candidates
            .choose(&mut rand::thread_rng())
            .expect("the oldest creation time comes from a member of the pool"))
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
