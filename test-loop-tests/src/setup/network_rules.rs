//! Rules on what leaves a node over the network: from the moment a rule is added until it is
//! lifted, the node drops, delays, holds back, rewrites or reports every outgoing request the
//! rule matches. Rules are keyed by account, so a node that is killed and restarted keeps its
//! rules, and a node added later is covered by the rules added for its account before.
//!
//! A request goes through the observers of the node's peer manager, then the rules of the
//! node's account in the order they were added, then the handlers that route it. A rule
//! sees a request after the rules before it had their say, so a rewrite can be dropped by a
//! later rule. A request a rule let go later, delayed or released from a hold, goes to the
//! handlers directly.
//!
//! A block a rule keeps from the network, by dropping it, holding it or replacing it with a
//! different one, stays with its keeper, and the layer swallows every request that would
//! fetch it by the back door: a `BlockRequest` naming its hash, from whichever node and
//! whichever path sends it, and a `BlockHeadersRequest` to the keeper while the keeper has
//! shown nothing above it. The test-loop peer manager answers both kinds of request by
//! handing the response straight to the requester's client, so the request is the only
//! place to intercept. What was kept and swallowed is reported by [`NetworkRules::kept_blocks`].

use super::peer_manager_actor::{DeferredNetworkRequest, TestLoopPeerManagerActor};
use near_async::messaging::CanSend as _;
use near_async::test_loop::sender::TestLoopSender;
use near_async::time::Duration;
use near_network::types::NetworkRequests;
use near_primitives::block::Block;
use near_primitives::hash::CryptoHash;
use near_primitives::network::PeerId;
use near_primitives::types::{AccountId, BlockHeight};
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

/// The release rule of [`Action::Hold`]: given a held request and the latest block height the
/// node has shown it knows, whether the request may go out now.
pub type Release = Box<dyn Fn(&NetworkRequests, BlockHeight) -> bool + Send + Sync>;

/// What a rule does with a request it matches.
pub enum Action {
    /// The request never leaves the node.
    Drop,
    /// The request leaves the node this much later, in test-loop time.
    Delay(Duration),
    /// [`Self::Delay`] by an amount the function picks per request, for a rule whose delay
    /// is decided when the request is sent rather than when the rule is added.
    DelayBy(Box<dyn Fn(&NetworkRequests) -> Duration + Send + Sync>),
    /// The request is held until its release rule says so. The rule is asked again each time
    /// the node sends anything, with the latest block height the node has shown it knows: the
    /// height of a block it broadcast, or the parent of a block it approved. Released
    /// requests go out in the order they were held, ahead of the request that triggered the
    /// check.
    Hold(Release),
    /// The request is replaced by what the function returns, sent in order: a rewrite, a
    /// duplicate, or nothing at all.
    Replace(Box<dyn Fn(NetworkRequests) -> Vec<NetworkRequests> + Send + Sync>),
    /// The request is reported to the function and goes out unchanged.
    Observe(Box<dyn Fn(&NetworkRequests) + Send + Sync>),
}

/// The part of a rule the test keeps: switches it off and says how often it matched. A rule
/// added with a tag can be found again by it, see [`NetworkRules::tagged`].
#[derive(Clone)]
pub struct RuleHandle(Arc<RuleState>);

#[derive(Default)]
struct RuleState {
    lifted: AtomicBool,
    matched: AtomicU64,
}

impl RuleHandle {
    /// Switches the rule off: nothing matches it from now on, and whatever it still holds goes
    /// out the next time the node sends anything.
    pub fn lift(&self) {
        self.0.lifted.store(true, Ordering::Relaxed);
    }

    /// How many requests the rule matched so far, so a test can check that it fired.
    pub fn matched(&self) -> u64 {
        self.0.matched.load(Ordering::Relaxed)
    }
}

struct Rule {
    account_id: AccountId,
    tag: Option<&'static str>,
    matches: Box<dyn Fn(&NetworkRequests) -> bool + Send + Sync>,
    action: Action,
    state: Arc<RuleState>,
}

impl Rule {
    fn lifted(&self) -> bool {
        self.state.lifted.load(Ordering::Relaxed)
    }
}

/// A block a rule kept from the network.
struct KeptBlock {
    height: BlockHeight,
    /// The account whose rule kept it, the only node that has it.
    keeper: AccountId,
    /// Whether a hold rule still holds it.
    held: bool,
}

/// What the rules kept from the network and what they swallowed for it, see
/// [`NetworkRules::kept_blocks`].
#[derive(Clone, Debug, Default)]
pub struct KeptBlocks {
    /// The hashes of the blocks a rule dropped, held or replaced.
    pub hashes: HashSet<CryptoHash>,
    /// How many requests for a kept block were swallowed.
    pub requests_swallowed: u64,
    /// How many header requests to a keeper of a live kept block were swallowed.
    pub header_requests_swallowed: u64,
}

/// Every rule of the test, shared by the peer managers of all nodes.
#[derive(Clone, Default)]
pub struct NetworkRules(Arc<Mutex<Inner>>);

#[derive(Default)]
struct Inner {
    rules: Vec<Arc<Rule>>,
    /// Per account, the requests its hold rules keep, oldest first, each with the rule holding it.
    held: HashMap<AccountId, Vec<(Arc<Rule>, NetworkRequests)>>,
    /// Per account, the latest block height the node has shown it knows.
    seen_heights: HashMap<AccountId, BlockHeight>,
    /// Per account, the sender of the node's peer manager, which delayed requests go out from.
    peer_managers: HashMap<AccountId, TestLoopSender<TestLoopPeerManagerActor>>,
    /// Per account, the peer id of its node, which header requests name their target by.
    peer_ids: HashMap<AccountId, PeerId>,
    /// The blocks the rules kept from the network, by hash.
    kept: HashMap<CryptoHash, KeptBlock>,
    requests_swallowed: u64,
    header_requests_swallowed: u64,
}

/// What is left of a request after the rules: what they released and what goes out now.
pub(crate) struct Applied {
    pub released: Vec<NetworkRequests>,
    pub requests: Vec<NetworkRequests>,
}

impl NetworkRules {
    /// Adds a rule on the outgoing requests of `account_id`, after the rules added before.
    pub fn add(
        &self,
        account_id: AccountId,
        matches: impl Fn(&NetworkRequests) -> bool + Send + Sync + 'static,
        action: Action,
    ) -> RuleHandle {
        self.add_rule(account_id, None, Box::new(matches), action)
    }

    /// [`Self::add`] with a tag, by which the rule can be found again: what a feature built
    /// on rules uses instead of a registry of its own.
    pub fn add_tagged(
        &self,
        account_id: AccountId,
        tag: &'static str,
        matches: impl Fn(&NetworkRequests) -> bool + Send + Sync + 'static,
        action: Action,
    ) -> RuleHandle {
        self.add_rule(account_id, Some(tag), Box::new(matches), action)
    }

    /// Adds an observer of the outgoing requests of `account_id` ahead of the account's
    /// rules, so it reports every request as the node sent it, before any rule has its
    /// say: what a test needs when another rule on the same requests delays, holds or
    /// rewrites them, since a request a rule let go later goes to the handlers directly,
    /// past the rules behind it.
    pub fn observe_first(
        &self,
        account_id: AccountId,
        matches: impl Fn(&NetworkRequests) -> bool + Send + Sync + 'static,
        observe: impl Fn(&NetworkRequests) + Send + Sync + 'static,
    ) -> RuleHandle {
        let state = Arc::new(RuleState::default());
        let rule = Rule {
            account_id,
            tag: None,
            matches: Box::new(matches),
            action: Action::Observe(Box::new(observe)),
            state: state.clone(),
        };
        self.0.lock().rules.insert(0, Arc::new(rule));
        RuleHandle(state)
    }

    fn add_rule(
        &self,
        account_id: AccountId,
        tag: Option<&'static str>,
        matches: Box<dyn Fn(&NetworkRequests) -> bool + Send + Sync>,
        action: Action,
    ) -> RuleHandle {
        let state = Arc::new(RuleState::default());
        let rule = Rule { account_id, tag, matches, action, state: state.clone() };
        self.0.lock().rules.push(Arc::new(rule));
        RuleHandle(state)
    }

    /// The handles of the rules of `account_id` tagged `tag`, in the order they were added,
    /// lifted ones included.
    pub fn tagged(&self, account_id: &AccountId, tag: &str) -> Vec<RuleHandle> {
        self.0
            .lock()
            .rules
            .iter()
            .filter(|rule| &rule.account_id == account_id && rule.tag == Some(tag))
            .map(|rule| RuleHandle(rule.state.clone()))
            .collect()
    }

    /// The handles of every rule tagged `tag`, over all accounts, in the order they were
    /// added, lifted ones included.
    pub fn all_tagged(&self, tag: &str) -> Vec<RuleHandle> {
        self.0
            .lock()
            .rules
            .iter()
            .filter(|rule| rule.tag == Some(tag))
            .map(|rule| RuleHandle(rule.state.clone()))
            .collect()
    }

    /// The blocks the rules kept from the network so far and the requests swallowed for them.
    pub fn kept_blocks(&self) -> KeptBlocks {
        let inner = self.0.lock();
        KeptBlocks {
            hashes: inner.kept.keys().copied().collect(),
            requests_swallowed: inner.requests_swallowed,
            header_requests_swallowed: inner.header_requests_swallowed,
        }
    }

    /// Records where the requests of `account_id` go out from and the peer id of its node,
    /// when the node is set up. A restarted node replaces the entries of the node it replaces.
    pub(crate) fn register_peer_manager(
        &self,
        account_id: &AccountId,
        sender: TestLoopSender<TestLoopPeerManagerActor>,
        peer_id: PeerId,
    ) {
        let mut inner = self.0.lock();
        inner.peer_managers.insert(account_id.clone(), sender);
        inner.peer_ids.insert(account_id.clone(), peer_id);
    }

    /// Runs `request`, which `account_id` is sending, through the account's rules.
    pub(crate) fn apply(&self, account_id: &AccountId, request: NetworkRequests) -> Applied {
        let mut inner = self.0.lock();
        if let Some(height) = seen_height_of(&request) {
            let seen = inner.seen_heights.entry(account_id.clone()).or_default();
            *seen = (*seen).max(height);
        }
        let seen_height = inner.seen_heights.get(account_id).copied().unwrap_or_default();
        let released = inner.release(account_id, seen_height);
        if inner.swallows(&request) {
            return Applied { released, requests: Vec::new() };
        }
        let rules: Vec<Arc<Rule>> =
            inner.rules.iter().filter(|rule| &rule.account_id == account_id).cloned().collect();
        let mut requests = vec![request];
        for rule in rules {
            if rule.lifted() {
                continue;
            }
            let mut passed = Vec::new();
            for request in requests {
                if !(rule.matches)(&request) {
                    passed.push(request);
                    continue;
                }
                rule.state.matched.fetch_add(1, Ordering::Relaxed);
                match &rule.action {
                    Action::Drop => inner.keep_block(account_id, &request, false),
                    Action::Delay(delay) => inner.defer(account_id, request, *delay),
                    Action::DelayBy(delay) => {
                        let delay = delay(&request);
                        inner.defer(account_id, request, delay);
                    }
                    Action::Hold(_) => {
                        inner.keep_block(account_id, &request, true);
                        inner
                            .held
                            .entry(account_id.clone())
                            .or_default()
                            .push((rule.clone(), request));
                    }
                    Action::Replace(replace) => {
                        let original = block_of(&request).map(|block| *block.hash());
                        let replaced = replace(request);
                        if let Some(hash) = original {
                            let kept = !replaced.iter().any(|request| {
                                block_of(request).is_some_and(|b| *b.hash() == hash)
                            });
                            if kept {
                                // The original is gone; what is known of it is its hash and
                                // height, which the replacements may have changed.
                                inner.kept.entry(hash).or_insert_with(|| KeptBlock {
                                    height: replaced
                                        .iter()
                                        .find_map(block_of)
                                        .map_or(seen_height, |b| b.header().height()),
                                    keeper: account_id.clone(),
                                    held: false,
                                });
                            }
                        }
                        passed.extend(replaced);
                    }
                    Action::Observe(observe) => {
                        observe(&request);
                        passed.push(request);
                    }
                }
            }
            requests = passed;
        }
        Applied { released, requests }
    }
}

impl Inner {
    /// The held requests of `account_id` whose rule releases them now, or whose rule was
    /// lifted, in the order they were held.
    fn release(
        &mut self,
        account_id: &AccountId,
        seen_height: BlockHeight,
    ) -> Vec<NetworkRequests> {
        let Some(held) = self.held.get_mut(account_id) else {
            return Vec::new();
        };
        let mut released = Vec::new();
        let mut kept = Vec::new();
        for (rule, request) in held.drain(..) {
            let release_now = rule.lifted()
                || match &rule.action {
                    Action::Hold(release) => release(&request, seen_height),
                    _ => true,
                };
            if release_now {
                released.push(request);
            } else {
                kept.push((rule, request));
            }
        }
        *held = kept;
        for request in &released {
            if let Some(block) = block_of(request) {
                if let Some(kept) = self.kept.get_mut(block.hash()) {
                    kept.held = false;
                }
            }
        }
        released
    }

    /// Sends `request` from `account_id`'s peer manager `delay` later.
    fn defer(&self, account_id: &AccountId, request: NetworkRequests, delay: Duration) {
        let sender = self
            .peer_managers
            .get(account_id)
            .expect("the node's peer manager is registered at setup");
        sender.clone().with_delay(delay).send(DeferredNetworkRequest(request));
    }

    /// Records the block `request` broadcasts, if it is one, as kept by `account_id`.
    fn keep_block(&mut self, account_id: &AccountId, request: &NetworkRequests, held: bool) {
        let Some(block) = block_of(request) else {
            return;
        };
        self.kept.entry(*block.hash()).or_insert_with(|| KeptBlock {
            height: block.header().height(),
            keeper: account_id.clone(),
            held,
        });
    }

    /// Whether `request` would fetch a kept block, counting it when so: a request for its
    /// hash, or a header request to its keeper while the kept block is live, that is held
    /// or not yet built over by the keeper.
    fn swallows(&mut self, request: &NetworkRequests) -> bool {
        match request {
            NetworkRequests::BlockRequest { hash, .. } if self.kept.contains_key(hash) => {
                self.requests_swallowed += 1;
                true
            }
            NetworkRequests::BlockHeadersRequest { peer_id, .. } => {
                let to_live_keeper = self.kept.values().any(|kept| {
                    let live = kept.held
                        || self.seen_heights.get(&kept.keeper).copied().unwrap_or_default()
                            <= kept.height;
                    live && self.peer_ids.get(&kept.keeper) == Some(peer_id)
                });
                if to_live_keeper {
                    self.header_requests_swallowed += 1;
                }
                to_live_keeper
            }
            _ => false,
        }
    }
}

/// The block `request` broadcasts, if it is a block broadcast.
fn block_of(request: &NetworkRequests) -> Option<&Arc<Block>> {
    match request {
        NetworkRequests::Block { block } => Some(block),
        _ => None,
    }
}

/// The block height a request shows its sender knows: a block it broadcasts, or the parent of
/// a block it approves.
fn seen_height_of(request: &NetworkRequests) -> Option<BlockHeight> {
    match request {
        NetworkRequests::Block { block } => Some(block.header().height()),
        NetworkRequests::Approval { approval_message } => {
            approval_message.approval.target_height.checked_sub(1)
        }
        _ => None,
    }
}
