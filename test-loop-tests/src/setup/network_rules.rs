//! Rules on what leaves a node over the network: from the moment a rule is added until it is
//! lifted, the node drops, delays, holds back or reports every outgoing request the rule
//! matches. Rules are keyed by account, so a node that is killed and
//! restarted keeps its rules, and a node added later is covered by the rules added for its
//! account before.

use super::peer_manager_actor::{DeferredNetworkRequest, TestLoopPeerManagerActor};
use super::state::SharedState;
use near_async::messaging::CanSend as _;
use near_async::test_loop::sender::TestLoopSender;
use near_async::time::Duration;
use near_network::types::NetworkRequests;
use near_primitives::block_header::ApprovalInner;
use near_primitives::types::{AccountId, BlockHeight};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

/// The release rule of [`Action::Hold`]: given a held request and the latest block height the
/// node has shown it knows, whether the request may go out now.
pub type Release = Box<dyn Fn(&NetworkRequests, BlockHeight) -> bool + Send + Sync>;

/// Which requests a rule applies to.
pub struct NetworkRequestsMatcher(Box<dyn Fn(&NetworkRequests) -> bool + Send + Sync>);

impl NetworkRequestsMatcher {
    pub fn new(matches: impl Fn(&NetworkRequests) -> bool + Send + Sync + 'static) -> Self {
        Self(Box::new(matches))
    }

    pub fn matches(&self, request: &NetworkRequests) -> bool {
        (self.0)(request)
    }
}

/// What a rule does with a request it matches.
pub enum Action {
    /// The request never leaves the node. A dropped block broadcast does not keep the block
    /// from the network: peers can still fetch it by hash.
    Drop,
    /// The request leaves the node later, in test-loop time, by an amount the function picks
    /// per request. See [`Self::delay`] for a fixed delay.
    DelayBy(Box<dyn Fn(&NetworkRequests) -> Duration + Send + Sync>),
    /// The request is held until its release rule says so. The rule is asked again each time
    /// the node sends anything, with the latest block height the node has shown it knows.
    Hold(Release),
    /// The request is reported to the function and goes out unchanged.
    Observe(Box<dyn Fn(&NetworkRequests) + Send + Sync>),
}

impl Action {
    /// Every matched request leaves the node `delay` later, in test-loop time.
    pub fn delay(delay: Duration) -> Self {
        Self::DelayBy(Box::new(move |_| delay))
    }
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

impl Default for RuleHandle {
    fn default() -> Self {
        Self(Arc::new(RuleState::default()))
    }
}

impl RuleHandle {
    /// Switches the rule off: nothing matches it from now on, and whatever it still holds goes
    /// out the next time the node sends anything.
    pub fn lift(&self) {
        self.0.lifted.store(true, Ordering::Relaxed);
    }

    /// How many requests the rule matched so far.
    pub fn matched(&self) -> u64 {
        self.0.matched.load(Ordering::Relaxed)
    }
}

struct Rule {
    account_id: AccountId,
    tag: Option<&'static str>,
    matches: NetworkRequestsMatcher,
    action: Action,
    state: Arc<RuleState>,
}

impl Rule {
    fn lifted(&self) -> bool {
        self.state.lifted.load(Ordering::Relaxed)
    }
}

/// The network of one node, through which a test adds rules on the requests the node sends.
/// Obtained via [`super::env::TestLoopEnv::node_network`] and
/// [`super::env::TestLoopEnv::node_network_for_account`].
pub struct TestLoopNodeNetwork<'a> {
    account_id: &'a AccountId,
    shared_state: &'a SharedState,
}

impl<'a> TestLoopNodeNetwork<'a> {
    pub(crate) fn new(account_id: &'a AccountId, shared_state: &'a SharedState) -> Self {
        Self { account_id, shared_state }
    }

    /// From now on `action` applies to every request the node sends that `matches`, until the
    /// handle is lifted. The rule is keyed by the node's account, so it stays on a node that
    /// is killed and restarted.
    pub fn add_rule(&self, matches: NetworkRequestsMatcher, action: Action) -> RuleHandle {
        self.shared_state.network_rules.add(self.account_id.clone(), matches, action)
    }

    /// [`Self::add_rule`] with [`Action::Observe`]: from now on `observe` reports every
    /// request the node sends that `matches` and the rules added before let through. Added
    /// before any other rule, it sees every request the node sends.
    pub fn observe_messages(
        &self,
        matches: NetworkRequestsMatcher,
        observe: impl Fn(&NetworkRequests) + Send + Sync + 'static,
    ) -> RuleHandle {
        self.add_rule(matches, Action::Observe(Box::new(observe)))
    }

    /// From now on the node drops every request it sends that `matches`.
    pub fn drop_messages(&self, matches: NetworkRequestsMatcher) -> RuleHandle {
        self.add_rule(matches, Action::Drop)
    }
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
}

/// The outcome of one send through the rules, in the order the peer manager dispatches it.
pub(crate) struct Applied {
    /// Requests held by earlier sends that this send lets go.
    pub released_held: Vec<NetworkRequests>,
    /// The request being sent, unless a rule dropped, delayed or held it. The sender gets
    /// its answer.
    pub outgoing: Option<NetworkRequests>,
}

impl NetworkRules {
    /// Adds a rule on the outgoing requests of `account_id`, after the rules added before.
    pub fn add(
        &self,
        account_id: AccountId,
        matches: NetworkRequestsMatcher,
        action: Action,
    ) -> RuleHandle {
        self.add_rule(account_id, None, matches, action)
    }

    /// [`Self::add`] with a tag, by which the rule can be found again: what a feature built
    /// on rules uses instead of a registry of its own.
    pub fn add_tagged(
        &self,
        account_id: AccountId,
        tag: &'static str,
        matches: NetworkRequestsMatcher,
        action: Action,
    ) -> RuleHandle {
        self.add_rule(account_id, Some(tag), matches, action)
    }

    /// [`Self::add`] under an existing handle, which the new rule shares with the rules added
    /// under it before: lifting the handle switches them all off, and its count covers them
    /// all.
    pub fn add_with_handle(
        &self,
        handle: &RuleHandle,
        account_id: AccountId,
        tag: Option<&'static str>,
        matches: NetworkRequestsMatcher,
        action: Action,
    ) {
        let rule = Rule { account_id, tag, matches: matches, action, state: handle.0.clone() };
        self.0.lock().rules.push(Arc::new(rule));
    }

    fn add_rule(
        &self,
        account_id: AccountId,
        tag: Option<&'static str>,
        matches: NetworkRequestsMatcher,
        action: Action,
    ) -> RuleHandle {
        let handle = RuleHandle::default();
        let rule = Rule { account_id, tag, matches, action, state: handle.0.clone() };
        self.0.lock().rules.push(Arc::new(rule));
        handle
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

    /// Records where the requests of `account_id` go out from, when the node is set up. A
    /// restarted node replaces the entry of the node it replaces, and what the rules held for
    /// that node is discarded with it.
    pub(crate) fn register_peer_manager(
        &self,
        account_id: &AccountId,
        sender: TestLoopSender<TestLoopPeerManagerActor>,
    ) {
        let mut inner = self.0.lock();
        inner.peer_managers.insert(account_id.clone(), sender);
        inner.held.remove(account_id);
    }

    /// Runs `request`, which `account_id` is sending, through the account's rules.
    pub(crate) fn apply(&self, account_id: &AccountId, request: NetworkRequests) -> Applied {
        let released_held = self.note_sent(account_id, &request);
        let rules: Vec<Arc<Rule>> = {
            let inner = self.0.lock();
            inner.rules.iter().filter(|rule| &rule.account_id == account_id).cloned().collect()
        };
        for rule in rules {
            if rule.lifted() || !rule.matches.matches(&request) {
                continue;
            }
            rule.state.matched.fetch_add(1, Ordering::Relaxed);
            match &rule.action {
                Action::Drop => {}
                Action::DelayBy(delay) => {
                    let delay = delay(&request);
                    self.defer(account_id, request, delay);
                }
                Action::Hold(_) => {
                    self.0
                        .lock()
                        .held
                        .entry(account_id.clone())
                        .or_default()
                        .push((rule.clone(), request));
                }
                Action::Observe(observe) => {
                    observe(&request);
                    continue;
                }
            }
            return Applied { released_held, outgoing: None };
        }
        Applied { released_held, outgoing: Some(request) }
    }

    /// Runs `request`, which a rule let `account_id` send late, past the rules after the one
    /// that let it go. It shows what the node knows just like a request sent on time.
    pub(crate) fn apply_deferred(
        &self,
        account_id: &AccountId,
        request: NetworkRequests,
    ) -> Applied {
        let released_held = self.note_sent(account_id, &request);
        Applied { released_held, outgoing: Some(request) }
    }

    /// Raises the height `account_id` has shown it knows to what `request` shows, and returns
    /// the held requests of the account this releases.
    fn note_sent(&self, account_id: &AccountId, request: &NetworkRequests) -> Vec<NetworkRequests> {
        let (held, seen_height) = {
            let mut inner = self.0.lock();
            let seen = inner.seen_heights.entry(account_id.clone()).or_default();
            if let Some(height) = seen_height_of(request) {
                *seen = (*seen).max(height);
            }
            let seen_height = *seen;
            (inner.held.remove(account_id).unwrap_or_default(), seen_height)
        };
        if held.is_empty() {
            return Vec::new();
        }
        let (released, mut still_held): (Vec<_>, Vec<_>) =
            held.into_iter().partition(|(rule, request)| {
                rule.lifted()
                    || match &rule.action {
                        Action::Hold(release) => release(request, seen_height),
                        _ => true,
                    }
            });
        let mut inner = self.0.lock();
        // Requests a release rule got held in the meantime stay behind the older ones.
        let held = inner.held.entry(account_id.clone()).or_default();
        still_held.append(held);
        *held = still_held;
        released.into_iter().map(|(_, request)| request).collect()
    }

    /// Sends `request` from `account_id`'s peer manager `delay` later.
    fn defer(&self, account_id: &AccountId, request: NetworkRequests, delay: Duration) {
        let sender = self
            .0
            .lock()
            .peer_managers
            .get(account_id)
            .expect("the node's peer manager is registered at setup")
            .clone();
        sender.with_delay(delay).send(DeferredNetworkRequest(request));
    }
}

/// The block height a request shows its sender knows: a block it broadcasts, or the parent of
/// a block it approves or skips.
fn seen_height_of(request: &NetworkRequests) -> Option<BlockHeight> {
    match request {
        NetworkRequests::Block { block } => Some(block.header().height()),
        NetworkRequests::Approval { approval_message } => {
            let approval = &approval_message.approval;
            match approval.inner {
                ApprovalInner::Endorsement(_) => approval.target_height.checked_sub(1),
                ApprovalInner::Skip(parent_height) => Some(parent_height),
            }
        }
        _ => None,
    }
}
