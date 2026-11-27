//! Rules on what travels between nodes: from the moment a rule is added until it is lifted, the
//! network drops, delays, holds back or reports every request the rule matches. Rules are keyed
//! by account, so a node that is killed and restarted keeps its rules, and a node added later is
//! covered by the rules added for its account before.

use super::peer_manager_actor::{DeferredNetworkRequest, TestLoopPeerManagerActor};
use super::state::SharedState;
use near_async::messaging::CanSend as _;
use near_async::test_loop::sender::TestLoopSender;
use near_async::time::Duration;
use near_network::types::NetworkRequests;
use near_primitives::block_header::ApprovalInner;
use near_primitives::types::{AccountId, BlockHeight};
use parking_lot::Mutex;
use std::collections::{BTreeMap, HashMap};
use std::mem;
use std::ops::Range;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock, Weak};

/// The release rule of [`Action::Hold`]: given a held request and the latest block height its
/// sender has shown it knows, whether the request may go out now.
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

    /// Matches every block broadcast at a height in `heights`. Requested blocks are answers to
    /// a `BlockRequest`, not broadcasts, so they are not matched.
    pub fn block_broadcasts(heights: Range<BlockHeight>) -> Self {
        Self::new(move |request| match request {
            NetworkRequests::Block { block } => heights.contains(&block.header().height()),
            _ => false,
        })
    }
}

/// What a rule does with a request it matches.
pub enum Action {
    /// The request never arrives. A dropped block broadcast does not keep the block from the
    /// network: peers can still fetch it by hash.
    Drop,
    /// The request arrives later, in test-loop time, by an amount the function picks per
    /// request. See [`Self::delay`] for a fixed delay.
    DelayBy(Box<dyn Fn(&NetworkRequests) -> Duration + Send + Sync>),
    /// The request is held until its release rule says so, or the rule is lifted. The release
    /// rule is asked again each time the sender sends anything, with the latest block height
    /// the sender has shown it knows.
    Hold(Release),
    /// The request is reported to the function and goes on unchanged.
    Observe(Box<dyn Fn(&NetworkRequests) + Send + Sync>),
}

impl Action {
    /// Every matched request arrives `delay` later, in test-loop time.
    pub fn delay(delay: Duration) -> Self {
        Self::DelayBy(Box::new(move |_| delay))
    }

    /// Every matched request is held until the rule is lifted.
    pub fn hold_until_lifted() -> Self {
        Self::Hold(Box::new(|_, _| false))
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
    /// The rules the handle's rules were added to, so lifting can let go of what they hold.
    rules: OnceLock<Weak<Mutex<Inner>>>,
}

impl Default for RuleHandle {
    fn default() -> Self {
        Self(Arc::new(RuleState::default()))
    }
}

impl RuleHandle {
    /// Switches the rule off: nothing matches it from now on, and whatever it still holds goes
    /// out right away, each request from its sender's peer manager, in the order it was held.
    pub fn lift(&self) {
        self.0.lifted.store(true, Ordering::Relaxed);
        if let Some(rules) = self.0.rules.get().and_then(Weak::upgrade) {
            NetworkRules(rules).release_lifted();
        }
    }

    /// How many requests the rule matched so far. An incoming rule counts each sender's
    /// request once.
    pub fn matched(&self) -> u64 {
        self.0.matched.load(Ordering::Relaxed)
    }
}

/// Whose requests a rule applies to.
#[derive(Clone, PartialEq, Eq)]
enum Scope {
    /// What the account sends, as a whole.
    Outgoing(AccountId),
    /// What reaches `to`, from `from` alone or, when it is `None`, from any sender.
    Incoming { to: AccountId, from: Option<AccountId> },
}

impl Scope {
    /// The account whose network the rule was added through.
    fn account_id(&self) -> &AccountId {
        match self {
            Self::Outgoing(account_id) => account_id,
            Self::Incoming { to, from } => from.as_ref().unwrap_or(to),
        }
    }

    /// Whether the rule applies to what `from` hands to `to`.
    fn covers_delivery(&self, from: &AccountId, to: &AccountId) -> bool {
        match self {
            Self::Outgoing(_) => false,
            Self::Incoming { to: rule_to, from: rule_from } => {
                rule_to == to && rule_from.as_ref().is_none_or(|rule_from| rule_from == from)
            }
        }
    }
}

struct Rule {
    scope: Scope,
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

/// The network of one node, through which a test adds rules on the requests the node sends
/// and receives. Obtained via [`super::env::TestLoopEnv::node_network`] and
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
    pub fn add_outgoing_rule(&self, matches: NetworkRequestsMatcher, action: Action) -> RuleHandle {
        self.shared_state.network_rules.add_outgoing(self.account_id.clone(), matches, action)
    }

    /// From now on `action` applies to every request that `matches` on its way to the node,
    /// from any sender, until the handle is lifted. The other recipients of the request are
    /// not affected: a block broadcast can be dropped for this node alone. See
    /// [`NetworkRules::add_incoming`] for what an incoming rule cannot see.
    pub fn add_incoming_rule(&self, matches: NetworkRequestsMatcher, action: Action) -> RuleHandle {
        self.shared_state.network_rules.add_incoming(self.account_id.clone(), matches, action)
    }

    /// From now on `action` applies to every request that `matches` on its way from the node
    /// to `to`, until the handle is lifted. The node's other recipients are not affected: a
    /// block broadcast can be dropped for `to` alone.
    pub fn add_rule_to(
        &self,
        to: &AccountId,
        matches: NetworkRequestsMatcher,
        action: Action,
    ) -> RuleHandle {
        self.shared_state.network_rules.add_between(
            self.account_id.clone(),
            to.clone(),
            matches,
            action,
        )
    }

    /// [`Self::add_outgoing_rule`] with [`Action::Observe`]: from now on `observe` reports every
    /// request the node sends that `matches` and the rules added before let through.
    pub fn observe_messages(
        &self,
        matches: NetworkRequestsMatcher,
        observe: impl Fn(&NetworkRequests) + Send + Sync + 'static,
    ) -> RuleHandle {
        self.add_outgoing_rule(matches, Action::Observe(Box::new(observe)))
    }

    /// From now on the node drops every request it sends that `matches`.
    pub fn drop_messages(&self, matches: NetworkRequestsMatcher) -> RuleHandle {
        self.add_outgoing_rule(matches, Action::Drop)
    }
}

/// Every rule of the test, shared by the peer managers of all nodes.
#[derive(Clone, Default)]
pub struct NetworkRules(Arc<Mutex<Inner>>);

#[derive(Default)]
struct Inner {
    /// Rules, in the order they run.
    rules: Vec<Arc<Rule>>,
    /// Per sending account, the requests hold rules keep, oldest first, each with the rule
    /// holding it.
    held: BTreeMap<AccountId, Vec<(Arc<Rule>, PendingRequest)>>,
    /// Per account, the latest block height the node has shown it knows.
    seen_heights: HashMap<AccountId, BlockHeight>,
    /// Per account, the sender of the node's peer manager, which delayed requests go out from.
    peer_managers: HashMap<AccountId, TestLoopSender<TestLoopPeerManagerActor>>,
}

/// A request on its way to the peer manager's handlers, which route it to its recipients.
#[derive(Debug)]
pub struct PendingRequest {
    pub request: NetworkRequests,
    /// The one recipient the request goes to, when an incoming rule of that recipient let it
    /// go late. The incoming rules are not asked again. `None` for a request on its way to
    /// all its recipients, each of which the incoming rules are asked about.
    pub only_to: Option<AccountId>,
}

impl PendingRequest {
    pub fn to_all(request: NetworkRequests) -> Self {
        Self { request, only_to: None }
    }
}

/// The outcome of one send through the rules, in the order the peer manager routes it.
pub(crate) struct Applied {
    /// Requests held by earlier sends that this send lets go.
    pub released_held: Vec<PendingRequest>,
    /// The request being sent, unless an outgoing rule dropped, delayed or held it. The sender
    /// gets its answer.
    pub outgoing: Option<NetworkRequests>,
}

impl NetworkRules {
    /// Adds a rule on the outgoing requests of `account_id`, after the rules added before.
    pub fn add_outgoing(
        &self,
        account_id: AccountId,
        matches: NetworkRequestsMatcher,
        action: Action,
    ) -> RuleHandle {
        self.add_rule(Scope::Outgoing(account_id), None, matches, action)
    }

    /// Adds a rule on the requests on their way to `account_id`, after the rules added
    /// before. It is asked once per sender and recipient, when the sender's network hands the
    /// request over, after the sender's outgoing rules let it go. It does not see state sync
    /// requests, which pick their destination as they are sent, nor the answers to
    /// `BlockRequest` and `BlockHeadersRequest`, which no peer manager sends.
    pub fn add_incoming(
        &self,
        account_id: AccountId,
        matches: NetworkRequestsMatcher,
        action: Action,
    ) -> RuleHandle {
        self.add_rule(Scope::Incoming { to: account_id, from: None }, None, matches, action)
    }

    /// [`Self::add_incoming`] on what `from` alone sends to `to`.
    pub fn add_between(
        &self,
        from: AccountId,
        to: AccountId,
        matches: NetworkRequestsMatcher,
        action: Action,
    ) -> RuleHandle {
        self.add_rule(Scope::Incoming { to, from: Some(from) }, None, matches, action)
    }

    /// [`Self::add_outgoing`] with a tag, by which the rule can be found again: what a feature
    /// built on rules uses instead of a registry of its own.
    pub fn add_tagged(
        &self,
        account_id: AccountId,
        tag: &'static str,
        matches: NetworkRequestsMatcher,
        action: Action,
    ) -> RuleHandle {
        self.add_rule(Scope::Outgoing(account_id), Some(tag), matches, action)
    }

    /// [`Self::add_outgoing`] under an existing handle, which the new rule shares with the rules
    /// added under it before: lifting the handle switches them all off, and its count covers
    /// them all.
    pub fn add_with_handle(
        &self,
        handle: &RuleHandle,
        account_id: AccountId,
        tag: Option<&'static str>,
        matches: NetworkRequestsMatcher,
        action: Action,
    ) {
        let scope = Scope::Outgoing(account_id);
        let rule = Rule { scope, tag, matches, action, state: handle.0.clone() };
        self.link(handle);
        self.0.lock().rules.push(Arc::new(rule));
    }

    fn add_rule(
        &self,
        scope: Scope,
        tag: Option<&'static str>,
        matches: NetworkRequestsMatcher,
        action: Action,
    ) -> RuleHandle {
        let handle = RuleHandle::default();
        let rule = Rule { scope, tag, matches, action, state: handle.0.clone() };
        self.link(&handle);
        self.0.lock().rules.push(Arc::new(rule));
        handle
    }

    /// Lets `handle` reach these rules when it is lifted. A handle only ever belongs to one
    /// test's rules.
    fn link(&self, handle: &RuleHandle) {
        let _ = handle.0.rules.set(Arc::downgrade(&self.0));
    }

    /// Sends right away what lifted rules still hold, each request from its sender's peer
    /// manager, in the order it was held.
    fn release_lifted(&self) {
        let released: Vec<(AccountId, PendingRequest)> = {
            let mut inner = self.0.lock();
            let mut released = Vec::new();
            for (account_id, held) in &mut inner.held {
                let (lifted, kept): (Vec<_>, Vec<_>) =
                    mem::take(held).into_iter().partition(|(rule, _)| rule.lifted());
                *held = kept;
                released
                    .extend(lifted.into_iter().map(|(_, pending)| (account_id.clone(), pending)));
            }
            released
        };
        for (account_id, pending) in released {
            self.defer(&account_id, pending, Duration::ZERO);
        }
    }

    /// The handles of the rules of `account_id` tagged `tag`, in the order they were added,
    /// lifted ones included.
    pub fn tagged(&self, account_id: &AccountId, tag: &str) -> Vec<RuleHandle> {
        self.0
            .lock()
            .rules
            .iter()
            .filter(|rule| rule.scope.account_id() == account_id && rule.tag == Some(tag))
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
    /// that node to send is discarded with it.
    pub(crate) fn register_peer_manager(
        &self,
        account_id: &AccountId,
        sender: TestLoopSender<TestLoopPeerManagerActor>,
    ) {
        let mut inner = self.0.lock();
        inner.peer_managers.insert(account_id.clone(), sender);
        inner.held.remove(account_id);
    }

    /// Whether any incoming rule is still on, so the peer managers need to ask the rules
    /// about each recipient.
    pub(crate) fn has_incoming_rules(&self) -> bool {
        self.0
            .lock()
            .rules
            .iter()
            .any(|rule| matches!(rule.scope, Scope::Incoming { .. }) && !rule.lifted())
    }

    /// Runs `request`, which `account_id` is sending, through the account's outgoing rules.
    pub(crate) fn apply(&self, account_id: &AccountId, request: NetworkRequests) -> Applied {
        let released_held = self.note_sent(account_id, &request);
        let scope = Scope::Outgoing(account_id.clone());
        let rules = self.rules_where(|rule_scope| rule_scope == &scope);
        let outgoing = match self.taken_by(&rules, &request) {
            Some(rule) => {
                self.take(account_id, rule, PendingRequest::to_all(request));
                None
            }
            None => Some(request),
        };
        Applied { released_held, outgoing }
    }

    /// Runs `request`, which `from` is handing to `to`, through the incoming rules of `to` that
    /// cover `from`, and says whether it reaches `to` now.
    pub(crate) fn deliver(
        &self,
        from: &AccountId,
        to: &AccountId,
        request: &NetworkRequests,
    ) -> bool {
        let rules = self.rules_where(|scope| scope.covers_delivery(from, to));
        let Some(rule) = self.taken_by(&rules, request) else {
            return true;
        };
        let pending = PendingRequest { request: request.clone(), only_to: Some(to.clone()) };
        self.take(from, rule, pending);
        false
    }

    /// Runs `pending`, which a rule let `account_id` send late, past the rules after the one
    /// that let it go. It shows what the node knows just like a request sent on time.
    pub(crate) fn apply_deferred(
        &self,
        account_id: &AccountId,
        pending: PendingRequest,
    ) -> Vec<PendingRequest> {
        let mut released = self.note_sent(account_id, &pending.request);
        released.push(pending);
        released
    }

    /// The rules whose scope `applies`, in the order they run.
    fn rules_where(&self, applies: impl Fn(&Scope) -> bool) -> Vec<Arc<Rule>> {
        let inner = self.0.lock();
        inner.rules.iter().filter(|rule| applies(&rule.scope)).cloned().collect()
    }

    /// Runs `request` through `rules` up to the first that drops, delays or holds it, which it
    /// returns. Observers on the way report it.
    fn taken_by(&self, rules: &[Arc<Rule>], request: &NetworkRequests) -> Option<Arc<Rule>> {
        for rule in rules {
            if rule.lifted() || !rule.matches.matches(request) {
                continue;
            }
            rule.state.matched.fetch_add(1, Ordering::Relaxed);
            match &rule.action {
                Action::Observe(observe) => observe(request),
                Action::Drop | Action::DelayBy(_) | Action::Hold(_) => return Some(rule.clone()),
            }
        }
        None
    }

    /// Does to `pending`, which `from` is sending, what `rule` says.
    fn take(&self, from: &AccountId, rule: Arc<Rule>, pending: PendingRequest) {
        match &rule.action {
            Action::Drop => {}
            Action::DelayBy(delay) => {
                let delay = delay(&pending.request);
                self.defer(from, pending, delay);
            }
            Action::Hold(_) => {
                self.0.lock().held.entry(from.clone()).or_default().push((rule.clone(), pending));
            }
            Action::Observe(_) => unreachable!("an observer takes no request"),
        }
    }

    /// Raises the height `account_id` has shown it knows to what `request` shows, and returns
    /// the held requests of the account this releases.
    fn note_sent(&self, account_id: &AccountId, request: &NetworkRequests) -> Vec<PendingRequest> {
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
            held.into_iter().partition(|(rule, pending)| {
                rule.lifted()
                    || match &rule.action {
                        Action::Hold(release) => release(&pending.request, seen_height),
                        _ => true,
                    }
            });
        let mut inner = self.0.lock();
        // Requests a release rule got held in the meantime stay behind the older ones.
        let held = inner.held.entry(account_id.clone()).or_default();
        still_held.append(held);
        *held = still_held;
        released.into_iter().map(|(_, pending)| pending).collect()
    }

    /// Sends `pending` from `account_id`'s peer manager `delay` later.
    fn defer(&self, account_id: &AccountId, pending: PendingRequest, delay: Duration) {
        let sender = self
            .0
            .lock()
            .peer_managers
            .get(account_id)
            .expect("the node's peer manager is registered at setup")
            .clone();
        sender.with_delay(delay).send(DeferredNetworkRequest(pending));
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
