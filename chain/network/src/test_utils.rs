use crate::types::{
    MsgRecipient, NetworkInfo, NetworkResponses, PeerManagerMessageRequest,
    PeerManagerMessageResponse, SetChainInfo,
};
use crate::PeerManagerActor;
use actix::{Actor, ActorContext, Context, Handler, MailboxError, Message};
use futures::future::BoxFuture;
use futures::{future, Future, FutureExt};
use near_crypto::{KeyType, SecretKey};
use near_network_primitives::types::{PeerInfo, ReasonForBan};
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
use std::time::Duration;
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
    actix::spawn(tokio::time::sleep(Duration::from_millis(max_wait_ms)).then(|_| {
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
            Duration::from_millis(self.check_interval_ms),
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
    let mut interval = tokio::time::interval(Duration::from_millis(check_interval_ms));
    tokio::time::timeout(Duration::from_millis(max_wait_ms), async {
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

impl Handler<GetInfo> for PeerManagerActor {
    type Result = crate::types::NetworkInfo;

    fn handle(&mut self, _msg: GetInfo, _ctx: &mut Context<Self>) -> Self::Result {
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

impl Handler<StopSignal> for PeerManagerActor {
    type Result = ();

    fn handle(&mut self, msg: StopSignal, ctx: &mut Self::Context) -> Self::Result {
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

impl Handler<BanPeerSignal> for PeerManagerActor {
    type Result = ();

    fn handle(&mut self, msg: BanPeerSignal, _ctx: &mut Self::Context) -> Self::Result {
        debug!(target: "network", "Ban peer: {:?}", msg.peer_id);
        self.try_ban_peer(&msg.peer_id, msg.ban_reason);
    }
}

// Mocked `PeerManager` adapter, has a queue of `PeerManagerMessageRequest` messages.
#[derive(Default)]
pub struct MockPeerManagerAdapter {
    pub requests: Arc<RwLock<VecDeque<PeerManagerMessageRequest>>>,
}

impl MsgRecipient<PeerManagerMessageRequest> for MockPeerManagerAdapter {
    fn send(
        &self,
        msg: PeerManagerMessageRequest,
    ) -> BoxFuture<'static, Result<PeerManagerMessageResponse, MailboxError>> {
        self.do_send(msg);
        future::ok(PeerManagerMessageResponse::NetworkResponses(NetworkResponses::NoResponse))
            .boxed()
    }

    fn do_send(&self, msg: PeerManagerMessageRequest) {
        self.requests.write().unwrap().push_back(msg);
    }
}

impl MsgRecipient<SetChainInfo> for MockPeerManagerAdapter {
    fn send(&self, _msg: SetChainInfo) -> BoxFuture<'static, Result<(), MailboxError>> {
        async { Ok(()) }.boxed()
    }
    fn do_send(&self, _msg: SetChainInfo) {}
}

impl MockPeerManagerAdapter {
    pub fn pop(&self) -> Option<PeerManagerMessageRequest> {
        self.requests.write().unwrap().pop_front()
    }
}

pub mod test_features {
    use crate::test_utils::convert_boot_nodes;
    use crate::types::{NetworkClientMessages, NetworkClientResponses};
    use crate::PeerManagerActor;
    use actix::actors::mocker::Mocker;
    use actix::Actor;
    use near_network_primitives::config;
    use near_network_primitives::types::{NetworkViewClientMessages, NetworkViewClientResponses};
    use near_primitives::block::GenesisId;
    use near_store::Store;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    /// Mock for `ClientActor`
    type ClientMock = Mocker<NetworkClientMessages>;
    /// Mock for `ViewClientActor`
    type ViewClientMock = Mocker<NetworkViewClientMessages>;

    // Make peer manager for unit tests
    pub fn make_peer_manager(
        store: Store,
        mut config: config::NetworkConfig,
        boot_nodes: Vec<(&str, u16)>,
        peer_max_count: u32,
    ) -> PeerManagerActor {
        config.boot_nodes = convert_boot_nodes(boot_nodes);
        config.max_num_peers = peer_max_count;
        let counter = Arc::new(AtomicUsize::new(0));
        let counter1 = counter.clone();
        let client_addr = ClientMock::mock(Box::new(move |_msg, _ctx| {
            Box::new(Some(NetworkClientResponses::NoResponse))
        }))
        .start();

        let view_client_addr = ViewClientMock::mock(Box::new(move |msg, _ctx| {
            let msg = msg.downcast_ref::<NetworkViewClientMessages>().unwrap();
            match msg {
                NetworkViewClientMessages::AnnounceAccount(accounts) => {
                    if !accounts.is_empty() {
                        counter1.fetch_add(1, Ordering::SeqCst);
                    }
                    Box::new(Some(NetworkViewClientResponses::AnnounceAccount(
                        accounts.clone().into_iter().map(|obj| obj.0).collect(),
                    )))
                }
                _ => Box::new(Some(NetworkViewClientResponses::NoResponse)),
            }
        }))
        .start();
        PeerManagerActor::new(
            store,
            config,
            client_addr.recipient(),
            view_client_addr.recipient(),
            GenesisId::default(),
        )
        .unwrap()
    }
}

#[cfg_attr(feature = "deepsize_feature", derive(deepsize::DeepSizeOf))]
#[derive(Message, Clone, Debug)]
#[rtype(result = "()")]
pub struct SetAdvOptions {
    pub set_max_peers: Option<u64>,
}
