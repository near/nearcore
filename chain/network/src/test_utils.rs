use crate::network_protocol::PeerInfo;
use crate::types::ReasonForBan;
use crate::types::{
    MsgRecipient, NetworkInfo, NetworkResponses, PeerManagerMessageRequest,
    PeerManagerMessageResponse, SetChainInfo,
};
use crate::PeerManagerActor;
use actix::{Actor, ActorContext, Context, Handler, MailboxError, Message};
use futures::future::BoxFuture;
use futures::{future, Future, FutureExt};
use near_crypto::{KeyType, SecretKey};
use near_o11y::{handler_debug_span, OpenTelemetrySpanExt, WithSpanContext};
use near_primitives::hash::hash;
use near_primitives::network::PeerId;
use near_primitives::types::EpochId;
use near_primitives::utils::index_to_bytes;
use once_cell::sync::Lazy;
use rand::{thread_rng, RngCore};
use std::collections::{HashMap, HashSet, VecDeque};
use std::net::TcpListener;
use std::ops::ControlFlow;
use std::sync::{Arc, Mutex, RwLock};
use tracing::debug;

static OPENED_PORTS: Lazy<Mutex<HashSet<u16>>> = Lazy::new(|| Mutex::new(HashSet::new()));

/// Returns available port.
pub fn open_port() -> u16 {
    // Use port 0 to allow the OS to assign an open port.
    // TcpListener's Drop impl will unbind the port as soon as listener goes out of scope.
    // We retry multiple times and store selected port in OPENED_PORTS to avoid port collision among
    // multiple tests.
    let max_attempts = 100;

    for _ in 0..max_attempts {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();

        let mut opened_ports = OPENED_PORTS.lock().unwrap();

        if !opened_ports.contains(&port) {
            opened_ports.insert(port);
            return port;
        }
    }

    panic!("Failed to find an open port after {} attempts.", max_attempts);
}

// `peer_id_from_seed` generate `PeerId` from seed for unit tests
pub fn peer_id_from_seed(seed: &str) -> PeerId {
    PeerId::new(SecretKey::from_seed(KeyType::ED25519, seed).public_key())
}

// `convert_boot_nodes` generate list of `PeerInfos` for unit tests
pub fn convert_boot_nodes(boot_nodes: Vec<(&str, u16)>) -> Vec<PeerInfo> {
    let mut result = vec![];
    for (peer_seed, port) in boot_nodes {
        let id = peer_id_from_seed(peer_seed);
        result.push(PeerInfo::new(id, format!("127.0.0.1:{}", port).parse().unwrap()))
    }
    result
}

/// Timeouts by stopping system without any condition and raises panic.
/// Useful in tests to prevent them from running forever.
#[allow(unreachable_code)]
pub fn wait_or_panic(max_wait_ms: u64) {
    actix::spawn(tokio::time::sleep(tokio::time::Duration::from_millis(max_wait_ms)).then(|_| {
        panic!("Timeout exceeded.");
        future::ready(())
    }));
}

/// Waits until condition or timeouts with panic.
/// Use in tests to check for a condition and stop or fail otherwise.
///
/// Prefer using [`wait_or_timeout`], which is not specific to actix.
///
/// # Example
///
/// ```rust,ignore
/// use actix::{System, Actor};
/// use near_network::test_utils::WaitOrTimeoutActor;
/// use std::time::{Instant, Duration};
///
/// near_actix_test_utils::run_actix(async {
///     let start = Instant::now();
///     WaitOrTimeoutActor::new(
///         Box::new(move |ctx| {
///             if start.elapsed() > Duration::from_millis(10) {
///                 System::current().stop()
///             }
///         }),
///         1000,
///         60000,
///     ).start();
/// });
/// ```
pub struct WaitOrTimeoutActor {
    f: Box<dyn FnMut(&mut Context<WaitOrTimeoutActor>)>,
    check_interval_ms: u64,
    max_wait_ms: u64,
    ms_slept: u64,
}

impl WaitOrTimeoutActor {
    pub fn new(
        f: Box<dyn FnMut(&mut Context<WaitOrTimeoutActor>)>,
        check_interval_ms: u64,
        max_wait_ms: u64,
    ) -> Self {
        WaitOrTimeoutActor { f, check_interval_ms, max_wait_ms, ms_slept: 0 }
    }

    fn wait_or_timeout(&mut self, ctx: &mut Context<Self>) {
        (self.f)(ctx);

        near_performance_metrics::actix::run_later(
            ctx,
            tokio::time::Duration::from_millis(self.check_interval_ms),
            move |act, ctx| {
                act.ms_slept += act.check_interval_ms;
                if act.ms_slept > act.max_wait_ms {
                    println!("BBBB Slept {}; max_wait_ms {}", act.ms_slept, act.max_wait_ms);
                    panic!("Timed out waiting for the condition");
                }
                act.wait_or_timeout(ctx);
            },
        );
    }
}

impl Actor for WaitOrTimeoutActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Context<Self>) {
        self.wait_or_timeout(ctx);
    }
}

/// Blocks until `cond` returns `ControlFlow::Break`, checking it every
/// `check_interval_ms`.
///
/// If condition wasn't fulfilled within `max_wait_ms`, returns an error.
pub async fn wait_or_timeout<C, F, T>(
    check_interval_ms: u64,
    max_wait_ms: u64,
    mut cond: C,
) -> Result<T, tokio::time::error::Elapsed>
where
    C: FnMut() -> F,
    F: Future<Output = ControlFlow<T>>,
{
    assert!(
        check_interval_ms < max_wait_ms,
        "interval shorter than wait time, did you swap the argument order?"
    );
    let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(check_interval_ms));
    tokio::time::timeout(tokio::time::Duration::from_millis(max_wait_ms), async {
        loop {
            interval.tick().await;
            if let ControlFlow::Break(res) = cond().await {
                break res;
            }
        }
    })
    .await
}

// Gets random PeerId
pub fn random_peer_id() -> PeerId {
    let sk = SecretKey::from_random(KeyType::ED25519);
    PeerId::new(sk.public_key())
}

// Gets random EpochId
pub fn random_epoch_id() -> EpochId {
    EpochId(hash(index_to_bytes(thread_rng().next_u64()).as_ref()))
}

// Compare whenever routing table match.
pub fn expected_routing_tables(
    got: &HashMap<PeerId, Vec<PeerId>>,
    want: &[(PeerId, Vec<PeerId>)],
) -> bool {
    if got.len() != want.len() {
        return false;
    }

    for (target, want_peers) in want {
        let got_peers = match got.get(target) {
            Some(ps) => ps,
            None => {
                return false;
            }
        };
        if got_peers.len() != want_peers.len() {
            return false;
        }
        for peer in want_peers {
            if !got_peers.contains(peer) {
                return false;
            }
        }
    }
    true
}

/// `GetInfo` gets `NetworkInfo` from `PeerManager`.
#[derive(Message)]
#[rtype(result = "NetworkInfo")]
pub struct GetInfo {}

impl Handler<WithSpanContext<GetInfo>> for PeerManagerActor {
    type Result = crate::types::NetworkInfo;

    fn handle(&mut self, msg: WithSpanContext<GetInfo>, _ctx: &mut Context<Self>) -> Self::Result {
        let (_span, _msg) = handler_debug_span!(target: "network", msg);
        self.get_network_info()
    }
}

// `StopSignal is used to stop PeerManagerActor for unit tests
#[derive(Message, Default)]
#[rtype(result = "()")]
pub struct StopSignal {
    pub should_panic: bool,
}

impl StopSignal {
    pub fn should_panic() -> Self {
        Self { should_panic: true }
    }
}

impl Handler<WithSpanContext<StopSignal>> for PeerManagerActor {
    type Result = ();

    fn handle(
        &mut self,
        msg: WithSpanContext<StopSignal>,
        ctx: &mut Self::Context,
    ) -> Self::Result {
        let (_span, msg) = handler_debug_span!(target: "network", msg);
        debug!(target: "network", "Receive Stop Signal.");

        if msg.should_panic {
            panic!("Node crashed");
        } else {
            ctx.stop();
        }
    }
}

/// Ban peer for unit tests.
/// Calls `try_ban_peer` in `PeerManagerActor`.
#[derive(Message)]
#[rtype(result = "()")]
pub struct BanPeerSignal {
    pub peer_id: PeerId,
    pub ban_reason: ReasonForBan,
}

impl BanPeerSignal {
    pub fn new(peer_id: PeerId) -> Self {
        Self { peer_id, ban_reason: ReasonForBan::None }
    }
}

impl Handler<WithSpanContext<BanPeerSignal>> for PeerManagerActor {
    type Result = ();

    fn handle(
        &mut self,
        msg: WithSpanContext<BanPeerSignal>,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        let (_span, msg) = handler_debug_span!(target: "network", msg);
        debug!(target: "network", "Ban peer: {:?}", msg.peer_id);
        self.state.disconnect_and_ban(&self.clock, &msg.peer_id, msg.ban_reason);
    }
}

// Mocked `PeerManager` adapter, has a queue of `PeerManagerMessageRequest` messages.
#[derive(Default)]
pub struct MockPeerManagerAdapter {
    pub requests: Arc<RwLock<VecDeque<PeerManagerMessageRequest>>>,
}

impl MsgRecipient<WithSpanContext<PeerManagerMessageRequest>> for MockPeerManagerAdapter {
    fn send(
        &self,
        msg: WithSpanContext<PeerManagerMessageRequest>,
    ) -> BoxFuture<'static, Result<PeerManagerMessageResponse, MailboxError>> {
        self.do_send(msg);
        future::ok(PeerManagerMessageResponse::NetworkResponses(NetworkResponses::NoResponse))
            .boxed()
    }

    fn do_send(&self, msg: WithSpanContext<PeerManagerMessageRequest>) {
        self.requests.write().unwrap().push_back(msg.msg);
    }
}

impl MsgRecipient<WithSpanContext<SetChainInfo>> for MockPeerManagerAdapter {
    fn send(
        &self,
        _msg: WithSpanContext<SetChainInfo>,
    ) -> BoxFuture<'static, Result<(), MailboxError>> {
        async { Ok(()) }.boxed()
    }
    fn do_send(&self, _msg: WithSpanContext<SetChainInfo>) {}
}

impl MockPeerManagerAdapter {
    pub fn pop(&self) -> Option<PeerManagerMessageRequest> {
        self.requests.write().unwrap().pop_front()
    }
    pub fn pop_most_recent(&self) -> Option<PeerManagerMessageRequest> {
        self.requests.write().unwrap().pop_back()
    }
}

#[derive(Message, Clone, Debug)]
#[rtype(result = "()")]
pub struct SetAdvOptions {
    pub set_max_peers: Option<u64>,
}
