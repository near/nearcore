//! This module contains implementation of the core bandwidth scheduler algorithm.
//!
//! ## Overview of the algorithm
//!
//! Every shard sends out some outgoing receipts to other shards at every height. Bandwidth
//! scheduler is used to limit how many bytes of receipts are sent from one shard to another.
//! Sending too many receipts could cause overload, the nodes could have too little bandwidth to
//! transfer all of the receipts in time.
//!
//! Bandwidth scheduler tries to ensure that:
//! - Every shard sends out at most `max_shard_bandwidth` bytes of receipts at every height.
//! - Every shard receives at most `max_shard_bandwidth` bytes of receipts at every height.
//! - The bandwidth is assigned in a fair way. At full load every link (pair of shards) sends and
//!   receives the same amount of bandwidth on average, there are no favorites.
//! - Bandwidth utilization is high.
//!
//! When a shard has some buffered receipts that it wants to send to another shard, it has to create
//! a bandwidth request and pass it to the scheduler. Bandwidth scheduler looks at bandwidth
//! requests from all shards and decides how much bandwidth to grant on each link (pair of shards).
//! Every shard runs the same algorithm with the same inputs and calculates the same bandwidth
//! grants. Then the shards send out receipts, but no more than the granted bandwidth. After that
//! the shards generate new requests based on buffered receipts and the whole process repeats at the
//! next height.
//!
//! A bandwidth request contains a list of values that could be granted for that link. The sender
//! shard hopes that the scheduler will grant the highest option, which would allow it to send the
//! most receipts, but if the scheduler chooses a lower value, the sender will send out fewer
//! receipts.
//!
//! ## Base bandwidth
//!
//! It's not necessary to make a bandwidth request if the shard wants to send less than "base
//! bandwidth" of receipts. Base bandwidth is the amount of bandwidth that is always granted on
//! every link, regardless of the requests. This helps to lower the total number of generated
//! requests, usually shards don't need to send out more than the base bandwidth. (At the moment of
//! writing there are 6 shards, and base bandwidth is ~50kB, situation might change in the future).
//!
//! ## Allowance
//!
//! There is also a concept of "allowance" - every link (pair of sender and receiver shards) has an
//! allowance. Allowance is a way to ensure fairness. Every link receives a fair amount of allowance
//! on every height. When bandwidth is granted on a link, the link's allowance is decreased by the
//! granted amount. Requests on links with higher allowance have priority over requests on links
//! with lower allowance. Links that send more than their fair share are deprioritized, which keeps
//! things fair. It's a similar idea to the [Token Bucket](https://en.wikipedia.org/wiki/Token_bucket).
//! Link allowances are persisted in the state trie, as they're used to track fairness across
//! multiple heights.
//!
//! An intuitive way to think about allowance is that it keeps track how much each link sent
//! recently and lowers priority of links that recently sent a lot of receipts, which gives other a
//! fair chance.
//!
//! Imagine a situation where one link wants to send a 2MB receipt at every height, and other links
//! want to send a ton of small receipts to the same shard. Without allowance, the link with 2MB
//! receipts would always get 2MB of bandwidth assigned, and other links would get less than that,
//! which would be unfair. Thanks to allowance, the scheduler will grant some bandwidth to the 2MB
//! link, but then it will decrease the allowance on that link, which will deprioritize it and other
//! links will get their fair share.
//!
//! ## The core algorithm
//!
//! The algorithm works in 4 stages:
//! 1) Give out a fair share of allowance to every link.
//! 2) Grant base bandwidth on every link. Decrease allowance by granted bandwidth.
//! 3) Process bandwidth requests. Order all bandwidth requests by the link's allowance. Take the
//!    request with the highest allowance and try to grant the first proposed value. Check if it's
//!    possible to grant the value without violating any restrictions. If yes, grant the bandwidth
//!    and decrease the allowance accordingly. Then remove the granted value from the request and
//!    put it back into the queue with new allowance. If no, remove the request from the queue, it
//!    will not be fulfilled.
//! 4) Distribute remaining bandwidth. If there's some bandwidth left after granting base bandwidth
//!    and processing all requests, distribute it over all links in a fair manner to improve
//!    bandwidth utilization.
//!
//! ## Congestion control
//!
//! Bandwidth scheduler limits only the size of outgoing receipts, the gas is limited by congestion
//! control. It's important to make sure that these two are integrated properly. Situations where
//! one limit allows sending receipts, but the other doesn't could lead to liveness issues.
//!
//! To avoid liveness problems, the scheduler checks which shards are fully congested, and doesn't
//! grant any bandwidth on links to these shards (except for the allowed sender shard). This
//! prevents situations where the scheduler would grant bandwidth on some link, but no receipts
//! would be sent because of congestion. There is a guarantee that for every bandwidth grant, the
//! shard will be able to send at least one receipt, which is enough to ensure liveness.
//!
//! There can still be unlucky coincidences where the scheduler grants a lot of bandwidth on a link,
//! but the shard can send only a few receipts because of the gas limit enforced by congestion
//! control. This is not ideal, in the future we might consider merging these two algorithm into one
//! better algorithm, but it is good enough for now.
//!
//! ## Missing chunks
//!
//! Special care has to be taken to handle missing chunks. When a chunk is missing, it doesn't
//! process the receipts that were sent to it at previous heights. If other shards keep sending
//! receipts to the shard with missing chunks, these receipts will accumulate and the size of
//! incoming receipts can grow into infinity. This would also mean that the state witness for the
//! next chunk would be huge, which would cause problems.
//!
//! Bandwidth scheduler handles this using a simple rule - if the last chunk was missing on some
//! shard, don't send any receipts to this shard, it still hasn't processed the previous ones.
//! This rule is enough to ensure that a shard doesn't send too much to another shard that has
//! missing chunks.
//!
//! The situation is worse when the chunks are missing on the sender shard. Receipts sent during
//! previous chunk's application are received when the next non-missing chunk on the sender shard
//! appears. This could be arbitrarily far into the future and in the meantime other shards could
//! send receipts to the same receiver, and the receiver could receive more receipts than it should.
//! This is harder to deal with, the current version of bandwidth scheduler doesn't have a mechanism
//! to prevent that.
//!
//! Future improvements:
//! a) Add proper handling of missing chunks on sender shard. The scheduler could look at how much
//!    was granted on other senders which have missing chunks and forbid producing too many receipts
//!    to a receiver.
//!
//! b) It's a bit wasteful to not send anything when a chunk is missing. We could track how much a
//!    shard already sent to the shard with missing chunks at previous heights, and allow to send
//!    receipts to it, but make sure that their total size doesn't exceed the grant that was given when
//!    the chunk wasn't missing. This would improve bandwidth utilization in scenarios with many
//!    missing chunks.
//!
//! ## Resharding
//!
//! During resharding the list of existing shards changes. The only moment that is really
//! problematic for the bandwidth scheduler is the resharding boundary. At the boundary the shards
//! that send receipts will be from the old layout, while the receiving shards will be from the new
//! layout. At all other heights senders and receivers are from the same layout, so there are no
//! problems.
//! Ideally the bandwidth scheduler would make sure that bandwidth is properly granted when the sets
//! of senders and receivers are different, but this not implemented for now. The grants will be
//! slightly wrong (but still within limits) on the resharding boundary. The amount of work needed
//! to support scheduling at the boundary exceeds the benefits. For now reshardings happen very
//! rarely, so grants are very rarely wrong, although this might change in the future.
//!
//! To properly handle resharding we would have to:
//! * Use different ShardLayouts for sender shards and receiver shards
//! * Interpret bandwidth requests using the BandwidthSchedulerParams that they were created with
//! * Make sure that `BandwidthSchedulerParams` are correct on the resharding boundary
//!
//! It's doable, but it's out of scope for the initial version of the scheduler.

use std::collections::{BTreeMap, VecDeque};

use near_primitives::bandwidth_scheduler::{
    Bandwidth, BandwidthRequest, BandwidthRequestValues, BandwidthRequests,
    BandwidthSchedulerParams, BandwidthSchedulerState, BandwidthSchedulerStateV1,
    BlockBandwidthRequests, LinkAllowance,
};
use near_primitives::shard_layout::ShardLayout;
use near_primitives::types::{ShardId, ShardIndex};
use rand::SeedableRng;
use rand::seq::SliceRandom;
use rand_chacha::ChaCha20Rng;

/// How many bytes of outgoing receipts can be sent from one shard to another at the current height.
/// Produced by the bandwidth scheduler.
#[derive(Default)]
pub struct GrantedBandwidth {
    pub granted: BTreeMap<(ShardId, ShardId), Bandwidth>,
}

impl GrantedBandwidth {
    pub fn get_granted_bandwidth(&self, sender: ShardId, receiver: ShardId) -> Bandwidth {
        self.granted.get(&(sender, receiver)).copied().unwrap_or(0)
    }
}

pub struct BandwidthScheduler {
    shard_layout: ShardLayout,
    /// Configuration parameters for the algorithm.
    params: BandwidthSchedulerParams,
    /// For every link keeps information whether sending receipts on this link is allowed.
    /// Keeps result of `Self::calculate_is_link_allowed()` for every pair of shards
    is_link_allowed_map: ShardLinkMap<bool>,
    /// Each shard can send and receive at most `max_shard_bandwidth` bytes of receipts.
    /// This is tracked in the `sender_budget` and `receiver_budget` fields, which keep
    /// track of how much more a shard can send or receive before hitting the limit.
    /// The sender and receiver budgets are decreased when bandwidth is granted on some link.
    /// It's not possible to grant more bandwidth than the sender or receiver budget.
    /// (ShardIndex -> Bandwidth)
    sender_budget: ShardIndexMap<Bandwidth>,
    receiver_budget: ShardIndexMap<Bandwidth>,
    /// Allowance for every link
    /// Bandwidth scheduler uses `allowance` to ensure fairness.
    /// Every link receives a fair amount of allowance on every height.
    /// When bandwidth is granted on a link, the link's allowance is decreased
    /// by the granted amount. Requests on links with higher allowance have priority
    /// over requests on links with lower allowance.
    /// Links that send more than their fair share are deprioritized, which keeps things fair.
    /// (ShardLink -> Bandwidth)
    link_allowances: ShardLinkMap<Bandwidth>,
    /// How much bandwidth was granted on each link.
    /// (ShardLink -> Bandwidth)
    granted_bandwidth: ShardLinkMap<Bandwidth>,
    /// Rng used to resolve ties when multiple requests have the same allowance.
    rng: ChaCha20Rng,
}

impl BandwidthScheduler {
    pub fn run(
        shard_layout: ShardLayout,
        state: &mut BandwidthSchedulerState,
        params: &BandwidthSchedulerParams,
        bandwidth_requests: &BlockBandwidthRequests,
        shards_status: &BTreeMap<ShardId, ShardStatus>,
        rng_seed: [u8; 32],
    ) -> GrantedBandwidth {
        if shard_layout.num_shards() == 0 {
            // No shards, nothing to grant.
            return GrantedBandwidth { granted: BTreeMap::new() };
        }

        let state = match state {
            BandwidthSchedulerState::V1(v1) => v1,
        };

        // Convert link allowances to the internal representation.
        let mut link_allowances: ShardLinkMap<Bandwidth> = ShardLinkMap::new(&shard_layout);
        for link_allowance in &state.link_allowances {
            let sender_index_opt = shard_layout.get_shard_index(link_allowance.sender);
            let receiver_index_opt = shard_layout.get_shard_index(link_allowance.receiver);
            match (sender_index_opt, receiver_index_opt) {
                (Ok(sender_index), Ok(receiver_index)) => {
                    let link = ShardLink::new(sender_index, receiver_index);
                    link_allowances.insert(link, link_allowance.allowance);
                }
                _ => {} // The allowance was for a shard that is not in the current set of shards. // TODO(bandwidth_scheduler) - add a warning?
            }
        }

        // Initialize the allowed link map based on shard statuses
        let mut shard_status_by_index: ShardIndexMap<ShardStatus> =
            ShardIndexMap::new(&shard_layout);
        for (shard_id, status) in shards_status {
            if let Ok(idx) = shard_layout.get_shard_index(*shard_id) {
                shard_status_by_index.insert(idx, *status);
            }
        }

        let mut is_link_allowed_map: ShardLinkMap<bool> = ShardLinkMap::new(&shard_layout);
        for sender_index in shard_layout.shard_indexes() {
            for receiver_index in shard_layout.shard_indexes() {
                let is_allowed = Self::calculate_is_link_allowed(
                    sender_index,
                    receiver_index,
                    &shard_status_by_index,
                );
                is_link_allowed_map
                    .insert(ShardLink::new(sender_index, receiver_index), is_allowed);
            }
        }

        // Convert bandwidth requests to representation used in the algorithm.
        let mut scheduler_bandwidth_requests: Vec<SchedulerBandwidthRequest> = Vec::new();
        for (sender_shard, shard_bandwidth_requests) in
            &bandwidth_requests.shards_bandwidth_requests
        {
            let requests = match shard_bandwidth_requests {
                BandwidthRequests::V1(requests_v1) => &requests_v1.requests,
            };

            for bandwidth_request in requests {
                // Convert request to the internal representation. It might turn out that the
                // request isn't applicable (e.g. shard ids from other layout, too little bandwidth
                // requested), in which case the function returns `None` and the request is ignored.
                // TODO(bandwidth_scheduler) - add a warning?
                if let Some(request) = SchedulerBandwidthRequest::new(
                    *sender_shard,
                    bandwidth_request,
                    params,
                    &shard_layout,
                ) {
                    scheduler_bandwidth_requests.push(request);
                }
            }
        }

        let sender_budget = ShardIndexMap::new(&shard_layout);
        let receiver_budget = ShardIndexMap::new(&shard_layout);
        let granted_bandwidth = ShardLinkMap::new(&shard_layout);

        // Init the scheduler state
        let mut scheduler = BandwidthScheduler {
            shard_layout,
            is_link_allowed_map,
            sender_budget,
            receiver_budget,
            link_allowances,
            granted_bandwidth,
            params: *params,
            rng: ChaCha20Rng::from_seed(rng_seed),
        };

        // Run the core algorithm
        let grants = scheduler.schedule_bandwidth(scheduler_bandwidth_requests);

        // Update the persistent scheduler state
        scheduler.update_scheduler_state(state);

        grants
    }

    fn schedule_bandwidth(&mut self, requests: Vec<SchedulerBandwidthRequest>) -> GrantedBandwidth {
        self.init_budgets();
        self.increase_allowances();
        self.grant_base_bandwidth();
        self.process_bandwidth_requests(requests);
        self.distribute_remaining_bandwidth();

        self.get_final_granted_bandwidth()
    }

    /// Initialize sender and receiver budgets. Every shard can send and receive at most `max_shard_bandwidth`.
    fn init_budgets(&mut self) {
        for sender in self.shard_layout.shard_indexes() {
            self.sender_budget.insert(sender, self.params.max_shard_bandwidth);
        }
        for receiver in self.shard_layout.shard_indexes() {
            self.receiver_budget.insert(receiver, self.params.max_shard_bandwidth);
        }
    }
    /// Give every link a fair amount of allowance at every height.
    fn increase_allowances(&mut self) {
        // In an ideal, fair world, every link would send the same amount of bandwidth.
        // There would be `max_bandwidth / num_shards` sent on every link, fully saturating
        // all senders and receivers.
        let num_shards = self.shard_layout.num_shards();
        assert_ne!(num_shards, 0);
        let fair_link_bandwidth = self.params.max_shard_bandwidth / num_shards;

        for link in self.iter_links() {
            self.increase_allowance(&link, fair_link_bandwidth);
        }
    }

    /// Grant base bandwidth on every link.
    /// Base bandwidth is the amount of bandwidth that is granted on every link, regardless of the requests.
    /// It's not necessary to make a request if a shard wants to send less receipts than the base bandwidth,
    /// which helps lower the total number of requests.
    fn grant_base_bandwidth(&mut self) {
        for link in self.iter_links() {
            // Do not care if granting base bandwidth fails for some links, it could happen if some links are not allowed.
            let _ignore_err = self.try_grant_bandwidth(&link, self.params.base_bandwidth);
        }
    }

    fn process_bandwidth_requests(&mut self, requests: Vec<SchedulerBandwidthRequest>) {
        // Bandwidth requests, ordered by link allowance.
        let mut requests_by_allowance: BTreeMap<Bandwidth, Vec<SchedulerBandwidthRequest>> =
            BTreeMap::new();
        for request in requests {
            requests_by_allowance
                .entry(self.get_allowance(&request.link))
                .or_insert_with(Vec::new)
                .push(request);
        }

        // Process requests in order of decreasing link allowance. Higher allowance means higher priority.
        while let Some((_allowance, mut requests)) = requests_by_allowance.pop_last() {
            // Shuffle requests that have the same allowance to resolve ties. Without the shuffle one link
            // could end up being processed before the other every time, which is not fair.
            requests.shuffle(&mut self.rng);
            for mut request in requests {
                // Try to grant the first bandwidth increase from the request.
                let Some(bandwidth_increase) = request.bandwidth_increases.pop_front() else {
                    continue;
                };
                match self.try_grant_bandwidth(&request.link, bandwidth_increase) {
                    TryGrantOutcome::Granted => {
                        // Granting bandwidth succeeded. Decrease the allowance and put the request back into the queue.
                        // The rest of requested bandwidth increases will be processed when the request is taken out
                        // of the priority queue again.
                        if !request.bandwidth_increases.is_empty() {
                            requests_by_allowance
                                .entry(self.get_allowance(&request.link))
                                .or_insert_with(Vec::new)
                                .push(request);
                        }
                    }
                    TryGrantOutcome::NotGranted => {
                        // Can't grant the next bandwidth increase for this request.
                        // Discard the request, there's nothing more we can do to fulfill it.
                        continue;
                    }
                }
            }
        }
    }

    /// After granting the base bandwidth and processing all bandwidth requests, there could be some
    /// remaining unused bandwidth that could be granted on the links. This function distributes the
    /// remaining bandwidth over all the links in a fair manner to improve bandwidth utilization.
    fn distribute_remaining_bandwidth(&mut self) {
        let remaining_bandwidth_grants =
            super::distribute_remaining::distribute_remaining_bandwidth(
                &self.sender_budget,
                &self.receiver_budget,
                &self.is_link_allowed_map,
                &self.shard_layout,
            );
        for link in self.iter_links() {
            if let Some(remaining_grant) = remaining_bandwidth_grants.get(&link) {
                self.grant_more_bandwidth(&link, *remaining_grant);
            }
        }
    }

    /// Convert granted bandwidth from internal representation to the representation returned by scheduler.
    fn get_final_granted_bandwidth(&self) -> GrantedBandwidth {
        let mut granted = BTreeMap::new();
        for link in self.iter_links() {
            if let Some(granted_bandwidth) = self.granted_bandwidth.get(&link) {
                granted.insert(
                    (
                        self.shard_layout.get_shard_id(link.sender).unwrap(),
                        self.shard_layout.get_shard_id(link.receiver).unwrap(),
                    ),
                    *granted_bandwidth,
                );
            }
        }
        GrantedBandwidth { granted }
    }

    /// Iterate over all links from senders to receivers without borrowing &self.
    /// Allows to modify other fields of the struct while iterating over links.
    fn iter_links(&self) -> impl Iterator<Item = ShardLink> + 'static {
        let num_indexes: usize =
            self.shard_layout.num_shards().try_into().expect("num_shards doesn't fit into usize!");
        (0..num_indexes)
            .map(move |sender_idx| {
                (0..num_indexes).map(move |receiver_idx| ShardLink::new(sender_idx, receiver_idx))
            })
            .flatten()
    }

    fn get_allowance(&self, link: &ShardLink) -> Bandwidth {
        self.link_allowances.get(link).copied().unwrap_or_else(|| self.default_link_allowance())
    }

    /// Increase allowance on the link, but don't exceed the maximum allowance.
    fn increase_allowance(&mut self, link: &ShardLink, increase: Bandwidth) {
        let current_allowance = self.get_allowance(link);
        let mut new_allowance = current_allowance.saturating_add(increase);
        if new_allowance > self.params.max_allowance {
            new_allowance = self.params.max_allowance;
        }
        self.link_allowances.insert(*link, new_allowance);
    }

    /// Decrease allowance on the link, but don't go below zero.
    fn decrease_allowance(&mut self, link: &ShardLink, decrease: Bandwidth) {
        let current_allowance = self.get_allowance(link);
        let new_allowance = current_allowance.saturating_sub(decrease);
        self.link_allowances.insert(*link, new_allowance);
    }

    /// New links start with zero allowance. They will get their fair share of bandwidth,
    /// and they can accumulate allowance if they want to send more receipts.
    fn default_link_allowance(&self) -> Bandwidth {
        0
    }

    /// Try to grant some bandwidth on the link.
    /// On success returns TryGrantOutcome::Granted
    /// If granting more bandwidth is not possible because of some restrictions, returns TryGrantOutcome::NotGranted.
    fn try_grant_bandwidth(&mut self, link: &ShardLink, bandwidth: Bandwidth) -> TryGrantOutcome {
        if !self.is_link_allowed(link) {
            // Not allowed to send anything on this link. Receiver is too congested or had a missing chunk.
            return TryGrantOutcome::NotGranted;
        }

        let sender_budget = self.sender_budget.get(&link.sender).copied().unwrap_or(0);
        let receiver_budget = self.receiver_budget.get(&link.receiver).copied().unwrap_or(0);

        if sender_budget < bandwidth || receiver_budget < bandwidth {
            // Sender or receiver can't send this much as they would go over the per-shard budget.
            return TryGrantOutcome::NotGranted;
        }

        // Ok, grant the bandwidth
        self.sender_budget.insert(link.sender, sender_budget - bandwidth);
        self.receiver_budget.insert(link.receiver, receiver_budget - bandwidth);
        self.decrease_allowance(link, bandwidth);
        self.grant_more_bandwidth(link, bandwidth);

        TryGrantOutcome::Granted
    }

    /// Add new granted bandwidth to the link. Doesn't adjust allowance or budgets.
    fn grant_more_bandwidth(&mut self, link: &ShardLink, bandwidth: Bandwidth) {
        let current_granted = self.granted_bandwidth.get(link).copied().unwrap_or(0);
        let new_granted = current_granted.checked_add(bandwidth).unwrap_or_else(|| {
            tracing::warn!(target: "runtime", "Granting bandwidth on link {:?} would overflow, this is unexpected. Granting max bandwidth instead", link);
            Bandwidth::MAX
        });
        self.granted_bandwidth.insert(*link, new_granted);
    }

    fn is_link_allowed(&self, link: &ShardLink) -> bool {
        *self.is_link_allowed_map.get(link).unwrap_or(&false)
    }

    /// Decide if it's allowed to send receipts on the link, based on shard statuses.
    /// Makes sure that receipts are not sent to fully congested shards or shards with missing chunks.
    fn calculate_is_link_allowed(
        sender_index: ShardIndex,
        receiver_index: ShardIndex,
        shards_status: &ShardIndexMap<ShardStatus>,
    ) -> bool {
        let Some(receiver_status) = shards_status.get(&receiver_index) else {
            // Receiver shard status unknown - don't send anything on the link, just to be safe.
            return false;
        };

        if receiver_status.last_chunk_missing {
            // The chunk was missing, receipts sent previously were not processed.
            // Don't send anything to avoid accumulation of incoming receipts on the receiver shard.
            return false;
        }

        let sender_status_opt = shards_status.get(&sender_index);
        if let Some(sender_status) = sender_status_opt {
            if sender_status.last_chunk_missing {
                // The chunk on sender's shard is missing. Don't grant any bandwidth on links from a shard
                // that is missing and won't send out anything at this height.
                // Bandwidth scheduler calculates outgoing bandwidth for chunks that are currently
                // being applied. The sender's chunk is not being applied right now, so it wouldn't
                // send out any receipts, it'd be wasteful to grant bandwidth on this link.
                return false;
            }
        }

        // Only the "allowed shard" is allowed to send receipts to a fully congested shard.
        if receiver_status.is_fully_congested {
            if Some(sender_index) == receiver_status.allowed_sender_shard_index {
                return true;
            } else {
                return false;
            }
        }

        true
    }

    /// Update the persistent scheduler state after running the scheduler algorithm.
    /// This state is persisted in the trie between runs.
    fn update_scheduler_state(&self, state: &mut BandwidthSchedulerStateV1) {
        let mut new_state_allowances: Vec<LinkAllowance> = Vec::new();

        for link in self.iter_links() {
            if let Some(link_allowance) = self.link_allowances.get(&link) {
                new_state_allowances.push(LinkAllowance {
                    sender: self.shard_layout.get_shard_id(link.sender).unwrap(),
                    receiver: self.shard_layout.get_shard_id(link.receiver).unwrap(),
                    allowance: *link_allowance,
                });
            }
        }

        state.link_allowances = new_state_allowances;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum TryGrantOutcome {
    Granted,
    NotGranted,
}

/// Shard status which helps decide whether it's ok to send receipts to a shard.
/// Scheduler doesn't allow sending receipts to shards that are fully congested
/// or had a missing chunk.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ShardStatus {
    /// Is this shard fully congested? If yes, only the allowed sender shard can send receipts to it.
    pub is_fully_congested: bool,
    /// Was the last chunk on this shard missing?
    /// If the last chunk was missing, receipts sent previously were not processed.
    pub last_chunk_missing: bool,
    /// The shard that is allowed to send receipts to this shard when the receiver is fully congested.
    pub allowed_sender_shard_index: Option<ShardIndex>,
}

/// A representation of `BandwidthRequest` used by the bandwidth scheduler.
/// Instead of absolute values of bandwidth, this struct contains consecutive
/// increases in bandwidth. The scheduler will go over the increases and either
/// grant them or refuse them.
/// The increases already take into account the base bandwidth that is granted on all links.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct SchedulerBandwidthRequest {
    /// Request to send receipts between those two shards.
    pub link: ShardLink,
    /// Requests to increase the granted bandwidth on the link.
    pub bandwidth_increases: VecDeque<Bandwidth>,
}

impl SchedulerBandwidthRequest {
    pub fn new(
        sender_shard: ShardId,
        bandwidth_request: &BandwidthRequest,
        params: &BandwidthSchedulerParams,
        layout: &ShardLayout,
    ) -> Option<Self> {
        let Ok(sender_index) = layout.get_shard_index(sender_shard) else {
            // Request from a shard that is not in the current set of shards.
            return None;
        };
        let Ok(receiver_index) = layout.get_shard_index(bandwidth_request.to_shard.into()) else {
            // Request to a shard that is not in the current set of shards.
            return None;
        };
        let link = ShardLink::new(sender_index, receiver_index);

        let mut bandwidth_increases = VecDeque::new();

        // Keeps track of the total bandwidth that would be granted by the requested increases.
        // Base bandwidth is already granted on all links, so we start with that.
        let mut current_total = params.base_bandwidth;

        let request_values = BandwidthRequestValues::new(params).values;
        for bit_idx in 0..bandwidth_request.requested_values_bitmap.len() {
            if !bandwidth_request.requested_values_bitmap.get_bit(bit_idx) {
                continue;
            }

            // Request for the total value of bandwidth that should be granted on the link.
            let requested_value = request_values[bit_idx];
            if requested_value <= current_total {
                continue;
            }
            // Convert the absolute value to a bandwidth increase.
            bandwidth_increases.push_back(requested_value - current_total);
            current_total = requested_value;
        }

        if bandwidth_increases.is_empty() {
            return None;
        }

        Some(Self { link, bandwidth_increases })
    }
}

/// Represents a link between a sender shard that sends receipts and a receiver shard that receives them.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ShardLink {
    /// Sender shard
    pub sender: ShardIndex,
    /// Receiver shard
    pub receiver: ShardIndex,
}

impl ShardLink {
    pub fn new(sender: ShardIndex, receiver: ShardIndex) -> Self {
        Self { sender, receiver }
    }
}

/// Equivalent to BTreeMap<ShardIndex, T>
/// Accessing a value is done by indexing into an array, which is faster than a lookup in BTreeMap or HashMap.
/// Should be used only with indexes from the same layout that was given in the constructor!
pub struct ShardIndexMap<T> {
    data: Vec<Option<T>>,
}

impl<T> ShardIndexMap<T> {
    pub fn new(layout: &ShardLayout) -> Self {
        let num_indexes: usize =
            layout.num_shards().try_into().expect("num_shards doesn't fit into usize");
        let mut data = Vec::with_capacity(num_indexes);
        // T might not implement Clone, so we can't use vec![None; mapping.indexes_len()]
        for _ in 0..num_indexes {
            data.push(None);
        }
        Self { data }
    }

    pub fn get(&self, index: &ShardIndex) -> Option<&T> {
        self.data[*index].as_ref()
    }

    pub fn get_mut(&mut self, index: &ShardIndex) -> Option<&mut T> {
        self.data[*index].as_mut()
    }

    pub fn insert(&mut self, index: ShardIndex, value: T) {
        self.data[index] = Some(value);
    }
}

/// Equivalent to BTreeMap<ShardLink, T>
/// Accessing a value is done by indexing into an array, which is faster than a lookup in BTreeMap or HashMap.
/// Should be used only with indexes from the same layout that was given in the constructor!
pub struct ShardLinkMap<T> {
    data: Vec<Option<T>>,
    num_indexes: usize,
}

impl<T> ShardLinkMap<T> {
    pub fn new(layout: &ShardLayout) -> Self {
        let num_indexes: usize =
            layout.num_shards().try_into().expect("Can't convert u64 to usize");
        let data_len = num_indexes * num_indexes;
        let mut data = Vec::with_capacity(data_len);
        for _ in 0..data_len {
            data.push(None);
        }
        Self { data, num_indexes }
    }

    pub fn get(&self, link: &ShardLink) -> Option<&T> {
        self.data[self.data_index_for_link(link)].as_ref()
    }

    pub fn insert(&mut self, link: ShardLink, value: T) {
        let data_index = self.data_index_for_link(&link);
        self.data[data_index] = Some(value);
    }

    #[cfg(test)]
    pub fn num_indexes(&self) -> usize {
        self.num_indexes
    }

    fn data_index_for_link(&self, link: &ShardLink) -> usize {
        debug_assert!(
            link.sender < self.num_indexes,
            "Sender index out of bounds! num_indexes: {}, link: {:?}",
            self.num_indexes,
            link
        );
        debug_assert!(
            link.receiver < self.num_indexes,
            "Receiver index out of bounds! num_indexes: {}, link: {:?}",
            self.num_indexes,
            link
        );
        link.sender * self.num_indexes + link.receiver
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use near_primitives::bandwidth_scheduler::{
        BandwidthRequest, BandwidthRequestBitmap, BandwidthRequests, BandwidthRequestsV1,
        BandwidthSchedulerParams, BandwidthSchedulerState, BandwidthSchedulerStateV1,
        BlockBandwidthRequests, LinkAllowance,
    };
    use near_primitives::hash::CryptoHash;
    use near_primitives::shard_layout::ShardLayout;
    use near_primitives::types::ShardId;

    use crate::bandwidth_scheduler::scheduler::ShardStatus;

    use super::BandwidthScheduler;

    /// Run bandwidth scheduler on worst-case scenario that should take as much CPU time as possible.
    /// Measures the time and prints it to stdout.
    fn measure_scheduler_worst_case_performance(num_shards: u64) {
        let shard_layout = ShardLayout::multi_shard(num_shards, 0);

        // Standard params
        let params = BandwidthSchedulerParams::for_test(num_shards);

        // Every link has a different allowance, which should make the `requests_by_allowance` BTreeMap
        // as large as possible.
        // Allowances have to be a bit lower than max to avoid a scenario where `increase_allowances()`
        // sets all allowances to max.
        let mut link_allowances: Vec<LinkAllowance> = Vec::new();
        let allowance_increase = params.max_shard_bandwidth / num_shards;
        let mut next_allowance = params.max_allowance - allowance_increase;
        for sender in shard_layout.shard_ids() {
            for receiver in shard_layout.shard_ids() {
                link_allowances.push(LinkAllowance { sender, receiver, allowance: next_allowance });
                next_allowance -= 1;
            }
        }
        let mut scheduler_state = BandwidthSchedulerState::V1(BandwidthSchedulerStateV1 {
            link_allowances,
            sanity_check_hash: CryptoHash::default(),
        });

        // Shards are not congested, scheduler can grant as many requests as possible.
        let shards_status = shard_layout
            .shard_ids()
            .map(|shard_id| {
                (
                    shard_id,
                    ShardStatus {
                        is_fully_congested: false,
                        last_chunk_missing: false,
                        allowed_sender_shard_index: None,
                    },
                )
            })
            .collect();

        // Every shard wants to send the maximum number of small receipts to all other shards.
        // Every bandwidth request requests all the values that it can, and there is a bandwidth
        // request between every pair of shards.
        let mut shards_bandwidth_requests: BTreeMap<ShardId, BandwidthRequests> = BTreeMap::new();
        for sender in shard_layout.shard_ids() {
            let mut requests = Vec::new();
            for receiver in shard_layout.shard_ids() {
                let mut request = BandwidthRequest {
                    to_shard: receiver.into(),
                    requested_values_bitmap: BandwidthRequestBitmap::new(),
                };
                for i in 0..request.requested_values_bitmap.len() {
                    request.requested_values_bitmap.set_bit(i, true);
                }
                requests.push(request);
            }
            shards_bandwidth_requests
                .insert(sender, BandwidthRequests::V1(BandwidthRequestsV1 { requests }));
        }

        let start_time = std::time::Instant::now();
        BandwidthScheduler::run(
            shard_layout,
            &mut scheduler_state,
            &params,
            &BlockBandwidthRequests { shards_bandwidth_requests },
            &shards_status,
            [0; 32],
        );
        let elapsed = start_time.elapsed();
        let millis = elapsed.as_secs_f64() * 1000.0;
        println!("Running scheduler with {} shards: {:.2} ms", num_shards, millis);
    }

    /// Benchmark how long it takes to run the scheduler in a worst-case scenario for different numbers of shards.
    ///
    /// Run with:
    /// cargo test -p node-runtime --release test_scheduler_worst_case_performance -- --nocapture
    ///
    /// Running scheduler with 6 shards: 0.13 ms
    /// Running scheduler with 10 shards: 0.19 ms
    /// Running scheduler with 32 shards: 1.85 ms
    /// Running scheduler with 64 shards: 5.80 ms
    /// Running scheduler with 128 shards: 23.98 ms
    /// Running scheduler with 256 shards: 97.44 ms
    /// Running scheduler with 512 shards: 385.97 ms
    #[test]
    fn test_scheduler_worst_case_performance() {
        for num_shards in [6, 10, 32, 64, 128, 256, 512] {
            measure_scheduler_worst_case_performance(num_shards);
        }
    }
}
