use crate::stats::metrics::NetworkMetrics;
use crate::types::{
    NetworkInfo, NetworkResponses, PeerManagerAdapter, PeerManagerMessageRequest,
    PeerManagerMessageResponse,
};
use crate::PeerManagerActor;
use actix::{Actor, ActorContext, Context, Handler, MailboxError, Message, Recipient};
use futures::future::BoxFuture;
use futures::{future, FutureExt};
use near_crypto::{KeyType, SecretKey};
use near_network_primitives::types::{PeerInfo, ReasonForBan};
use near_primitives::hash::hash;
use near_primitives::network::PeerId;
use near_primitives::types::EpochId;
use near_primitives::utils::index_to_bytes;
use once_cell::sync::{Lazy, OnceCell};
use rand::{thread_rng, RngCore};
use std::collections::{HashMap, HashSet, VecDeque};
use std::net::TcpListener;
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
    current: HashMap<PeerId, Vec<PeerId>>,
    expected: Vec<(PeerId, Vec<PeerId>)>,
) -> bool {
    if current.len() != expected.len() {
        return false;
    }

    for (peer, paths) in expected.into_iter() {
        let cur_paths = current.get(&peer);
        if cur_paths.is_none() {
            return false;
        }
        let cur_paths = cur_paths.unwrap();
        if cur_paths.len() != paths.len() {
            return false;
        }
        for next_hop in paths.into_iter() {
            if !cur_paths.contains(&next_hop) {
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

/// `GetMetrics` gets `NetworkMetrics` from `PeerManager`.
#[derive(Message)]
#[rtype(result = "Arc<NetworkMetrics>")]
pub struct GetMetrics {}

impl Handler<GetMetrics> for PeerManagerActor {
    type Result = Arc<NetworkMetrics>;

    fn handle(&mut self, _msg: GetMetrics, _ctx: &mut Context<Self>) -> Self::Result {
        self.network_metrics.clone()
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

impl PeerManagerAdapter for MockPeerManagerAdapter {
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

impl MockPeerManagerAdapter {
    pub fn pop(&self) -> Option<PeerManagerMessageRequest> {
        self.requests.write().unwrap().pop_front()
    }
}

#[cfg(feature = "test_features")]
pub mod test_features {
    use crate::routing::routing_table_actor::{start_routing_table_actor, RoutingTableActor};
    use crate::test_utils::{convert_boot_nodes, open_port};
    use crate::types::{NetworkClientMessages, NetworkClientResponses};
    use crate::PeerManagerActor;
    use actix::actors::mocker::Mocker;
    use actix::{Actor, Addr};
    use near_network_primitives::types::{
        NetworkConfig, NetworkViewClientMessages, NetworkViewClientResponses,
    };
    use near_primitives::block::GenesisId;
    use near_primitives::network::PeerId;
    use near_store::test_utils::create_test_store;
    use near_store::Store;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    /// Mock for `ClientActor`
    type ClientMock = Mocker<NetworkClientMessages>;
    /// Mock for `ViewClientActor`
    type ViewClientMock = Mocker<NetworkViewClientMessages>;

    // Start PeerManagerActor, and RoutingTableActor together and returns pairs of addresses
    // for each of them.
    pub fn make_peer_manager_routing_table_addr_pair(
    ) -> (Addr<PeerManagerActor>, Addr<RoutingTableActor>) {
        let seed = "test2";
        let port = open_port();

        let net_config = NetworkConfig::from_seed(seed, port);
        let store = create_test_store();
        let routing_table_addr =
            start_routing_table_actor(PeerId::new(net_config.public_key.clone()), store.clone());
        let peer_manager_addr = make_peer_manager(
            store,
            net_config,
            vec![("test1", open_port())],
            10,
            routing_table_addr.clone(),
        )
        .0
        .start();
        (peer_manager_addr, routing_table_addr)
    }

    // Make peer manager for unit tests
    //
    // Returns:
    //    PeerManagerActor
    //    PeerId - PeerId associated with given actor
    //    Arc<AtomicUsize> - shared pointer for counting the number of received
    //                       `NetworkViewClientMessages::AnnounceAccount` messages
    pub fn make_peer_manager(
        store: Store,
        mut config: NetworkConfig,
        boot_nodes: Vec<(&str, u16)>,
        peer_max_count: u32,
        routing_table_addr: Addr<RoutingTableActor>,
    ) -> (PeerManagerActor, PeerId, Arc<AtomicUsize>) {
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
                NetworkViewClientMessages::GetChainInfo => {
                    Box::new(Some(NetworkViewClientResponses::ChainInfo {
                        genesis_id: GenesisId::default(),
                        height: 1,
                        tracked_shards: vec![],
                        archival: false,
                    }))
                }
                _ => Box::new(Some(NetworkViewClientResponses::NoResponse)),
            }
        }))
        .start();
        let peer_id = PeerId::new(config.public_key.clone());
        (
            PeerManagerActor::new(
                store,
                config,
                client_addr.recipient(),
                view_client_addr.recipient(),
                routing_table_addr,
            )
            .unwrap(),
            peer_id,
            counter,
        )
    }
}

#[derive(Default)]
pub struct NetworkRecipient {
    peer_manager_recipient: OnceCell<Recipient<PeerManagerMessageRequest>>,
}

impl NetworkRecipient {
    pub fn set_recipient(&self, peer_manager_recipient: Recipient<PeerManagerMessageRequest>) {
        self.peer_manager_recipient
            .set(peer_manager_recipient)
            .expect("can't `set_recipient` twice");
    }
}

impl PeerManagerAdapter for NetworkRecipient {
    fn send(
        &self,
        msg: PeerManagerMessageRequest,
    ) -> BoxFuture<'static, Result<PeerManagerMessageResponse, MailboxError>> {
        self.peer_manager_recipient.wait().send(msg).boxed()
    }

    fn do_send(&self, msg: PeerManagerMessageRequest) {
        let _ = self.peer_manager_recipient.wait().do_send(msg);
    }
}

#[cfg_attr(feature = "deepsize_feature", derive(deepsize::DeepSizeOf))]
#[derive(Message, Clone, Debug)]
#[rtype(result = "()")]
pub struct SetAdvOptions {
    pub disable_edge_signature_verification: Option<bool>,
    pub disable_edge_propagation: Option<bool>,
    pub disable_edge_pruning: Option<bool>,
    pub set_max_peers: Option<u64>,
}

#[cfg(feature = "test_features")]
#[cfg_attr(feature = "deepsize_feature", derive(deepsize::DeepSizeOf))]
#[derive(Message, Clone, Debug)]
#[rtype(result = "()")]
pub struct SetRoutingTable {
    pub add_edges: Option<Vec<near_network_primitives::types::Edge>>,
    pub remove_edges: Option<Vec<near_network_primitives::types::SimpleEdge>>,
    pub prune_edges: Option<bool>,
}
