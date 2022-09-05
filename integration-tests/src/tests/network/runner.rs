use crate::tests::network::multiset::MultiSet;
use actix::{Actor, Addr};
use anyhow::{anyhow, bail, Context};
use near_chain::test_utils::{KeyValueRuntime, ValidatorSchedule};
use near_chain::{Chain, ChainGenesis};
use near_chain_configs::ClientConfig;
use near_client::{start_client, start_view_client};
use near_crypto::KeyType;
use near_logger_utils::init_test_logger;
use near_network::actix::ActixSystem;
use near_network::blacklist;
use near_network::broadcast;
use near_network::config;
use near_network::test_utils::{
    expected_routing_tables, open_port, peer_id_from_seed, BanPeerSignal, GetInfo,
};
use near_network::time;
use near_network::types::NetworkRecipient;
use near_network::types::{
    OutboundTcpConnect, PeerInfo, Ping as NetPing, Pong as NetPong, ROUTED_MESSAGE_TTL,
};
use near_network::types::{PeerManagerMessageRequest, PeerManagerMessageResponse};
use near_network::{Event, PeerManagerActor};
use near_primitives::block::GenesisId;
use near_primitives::network::PeerId;
use near_primitives::types::{AccountId, ValidatorId};
use near_primitives::validator_signer::InMemoryValidatorSigner;
use near_telemetry::{TelemetryActor, TelemetryConfig};
use std::collections::HashSet;
use std::future::Future;
use std::iter::Iterator;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::pin::Pin;
use std::sync::Arc;
use tracing::debug;

pub type ControlFlow = std::ops::ControlFlow<()>;
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;
pub type ActionFn =
    Box<dyn for<'a> Fn(&'a mut RunningInfo) -> BoxFuture<'a, anyhow::Result<ControlFlow>>>;

/// Sets up a node with a valid Client, Peer
fn setup_network_node(
    account_id: AccountId,
    validators: Vec<AccountId>,
    chain_genesis: ChainGenesis,
    config: config::NetworkConfig,
) -> Addr<PeerManagerActor> {
    let store = near_store::test_utils::create_test_node_storage();

    let num_validators = validators.len() as ValidatorId;

    let vs = ValidatorSchedule::new().block_producers_per_epoch(vec![validators]);
    let runtime = Arc::new(KeyValueRuntime::new_with_validators(
        store.get_store(near_store::Temperature::Hot),
        vs,
        5,
    ));
    let signer = Arc::new(InMemoryValidatorSigner::from_seed(
        account_id.clone(),
        KeyType::ED25519,
        account_id.as_ref(),
    ));
    let telemetry_actor = TelemetryActor::new(TelemetryConfig::default()).start();

    let db = store.into_inner(near_store::Temperature::Hot);
    let mut client_config = ClientConfig::test(false, 100, 200, num_validators, false, true);
    client_config.archive = config.archive;
    client_config.ttl_account_id_router = config.ttl_account_id_router.try_into().unwrap();
    let genesis_block = Chain::make_genesis_block(&*runtime, &chain_genesis).unwrap();
    let genesis_id = GenesisId {
        chain_id: client_config.chain_id.clone(),
        hash: genesis_block.header().hash().clone(),
    };
    let network_adapter = Arc::new(NetworkRecipient::default());
    let adv = near_client::adversarial::Controls::default();
    let client_actor = start_client(
        client_config.clone(),
        chain_genesis.clone(),
        runtime.clone(),
        config.node_id(),
        network_adapter.clone(),
        Some(signer),
        telemetry_actor,
        None,
        adv.clone(),
    )
    .0;
    let view_client_actor = start_view_client(
        config.validator.as_ref().map(|v| v.account_id()),
        chain_genesis.clone(),
        runtime.clone(),
        network_adapter.clone(),
        client_config,
        adv,
    );
    let peer_manager = PeerManagerActor::spawn(
        time::Clock::real(),
        db.clone(),
        config,
        client_actor.recipient(),
        view_client_actor.recipient(),
        genesis_id,
    )
    .unwrap();
    network_adapter.set_recipient(peer_manager.clone());
    peer_manager
}

#[derive(Debug, Clone)]
pub struct Ping {
    pub source: usize,
    pub nonce: u64,
}

#[derive(Debug, Clone)]
pub struct Pong {
    pub source: usize,
    pub nonce: u64,
}

// TODO: Deprecate this in favor of separate functions.
#[derive(Debug, Clone)]
pub enum Action {
    AddEdge {
        from: usize,
        to: usize,
        force: bool,
    },
    CheckRoutingTable(usize, Vec<(usize, Vec<usize>)>),
    CheckAccountId(usize, Vec<usize>),
    // Send ping from `source` with `nonce` to `target`
    PingTo {
        source: usize,
        target: usize,
        nonce: u64,
    },
    // Check for `source` received pings and pongs.
    CheckPingPong(usize, Vec<Ping>, Vec<Pong>),
    // Send stop signal to some node.
    Stop(usize),
    // Wait time in milliseconds
    Wait(time::Duration),
    #[allow(dead_code)]
    SetOptions {
        target: usize,
        max_num_peers: Option<u64>,
    },
}

pub struct RunningInfo {
    runner: Runner,
    nodes: Vec<Option<NodeHandle>>,
}

struct StateMachine {
    actions: Vec<ActionFn>,
}

async fn check_routing_table(
    info: &mut RunningInfo,
    u: usize,
    want: Vec<(usize, Vec<usize>)>,
) -> anyhow::Result<ControlFlow> {
    let want_rt: Vec<_> = want
        .into_iter()
        .map(|(target, routes)| {
            let peers =
                routes.into_iter().map(|hop| info.runner.test_config[hop].peer_id()).collect();
            (info.runner.test_config[target].peer_id(), peers)
        })
        .collect();
    let pm = info.get_node(u)?.actix.addr.clone();
    let resp = pm.send(PeerManagerMessageRequest::FetchRoutingTable).await?;
    let rt = match resp {
        PeerManagerMessageResponse::FetchRoutingTable(rt) => rt,
        _ => bail!("bad response"),
    };
    if expected_routing_tables(&rt.next_hops, &want_rt) {
        return Ok(ControlFlow::Break(()));
    }
    Ok(ControlFlow::Continue(()))
}

async fn check_account_id(
    info: &mut RunningInfo,
    source: usize,
    known_validators: Vec<usize>,
) -> anyhow::Result<ControlFlow> {
    let mut expected_known = vec![];
    for u in known_validators.clone() {
        expected_known.push(info.runner.test_config[u].account_id.clone());
    }
    let pm = &info.get_node(source)?.actix.addr;
    let rt = match pm.send(PeerManagerMessageRequest::FetchRoutingTable).await? {
        PeerManagerMessageResponse::FetchRoutingTable(rt) => rt,
        _ => bail!("bad response"),
    };
    for v in &expected_known {
        if !rt.account_peers.contains_key(v) {
            return Ok(ControlFlow::Continue(()));
        }
    }
    Ok(ControlFlow::Break(()))
}

async fn check_ping_pong(
    info: &mut RunningInfo,
    source: usize,
    want_pings: Vec<Ping>,
    want_pongs: Vec<Pong>,
) -> anyhow::Result<ControlFlow> {
    let want_pings: MultiSet<NetPing> = want_pings
        .iter()
        .map(|p| NetPing { nonce: p.nonce, source: info.runner.test_config[p.source].peer_id() })
        .collect();
    let want_pongs: MultiSet<NetPong> = want_pongs
        .iter()
        .map(|p| NetPong { nonce: p.nonce, source: info.runner.test_config[p.source].peer_id() })
        .collect();
    let node: &mut NodeHandle = info.nodes[source].as_mut().unwrap();

    loop {
        if !node.pings.is_subset(&want_pings) {
            bail!("got_pings = {:?}, want_pings = {want_pings:?}", node.pings);
        }
        if !node.pongs.is_subset(&want_pongs) {
            bail!("got_pongs = {:?}, want_pongs = {want_pongs:?}", node.pongs);
        }
        if node.pings == want_pings && node.pongs == want_pongs {
            return Ok(ControlFlow::Break(()));
        }
        node.consume_event().await;
    }
}

impl StateMachine {
    fn new() -> Self {
        Self { actions: vec![] }
    }

    pub fn push_action(&mut self, action: ActionFn) {
        self.actions.push(action);
    }

    pub fn push(&mut self, action: Action) {
        let num_prev_actions = self.actions.len();
        let action_clone = 0; // action.clone();
        match action {
            #[allow(unused_variables)]
            Action::SetOptions { target, max_num_peers } => {
                self.actions.push(Box::new(move |info:&mut RunningInfo| Box::pin(async move {
                    debug!(target: "network", num_prev_actions, action = ?action_clone, "runner.rs: Action");
                    info.get_node(target)?.actix.addr.send(PeerManagerMessageRequest::SetAdvOptions(near_network::test_utils::SetAdvOptions {
                        set_max_peers: max_num_peers,
                    })).await?;
                    Ok(ControlFlow::Break(()))
                })));
            }
            Action::AddEdge { from, to, force } => {
                self.actions.push(Box::new(move |info: &mut RunningInfo| Box::pin(async move {
                    debug!(target: "network", num_prev_actions, action = ?action_clone, "runner.rs: Action");
                    let pm = info.get_node(from)?.actix.addr.clone();
                    let peer_info = info.runner.test_config[to].peer_info();
                    tracing::debug!(target: "dupa", "{from} -> {to} ({} -> {})",info.runner.test_config[from].peer_info().id,peer_info.id);
                    let peer_id = peer_info.id.clone();
                    pm.send(PeerManagerMessageRequest::OutboundTcpConnect(
                        OutboundTcpConnect(peer_info),
                    )).await?;
                    if !force {
                        return Ok(ControlFlow::Break(()))
                    }
                    let res = pm.send(GetInfo{}).await?;
                    for peer in &res.connected_peers {
                        if peer.full_peer_info.peer_info.id==peer_id {
                            return Ok(ControlFlow::Break(()))
                        }
                    }
                    Ok(ControlFlow::Continue(()))
                })));
            }
            Action::CheckRoutingTable(u, expected) => {
                self.actions.push(Box::new(move |info| {
                    Box::pin(check_routing_table(info, u, expected.clone()))
                }));
            }
            Action::CheckAccountId(source, known_validators) => {
                self.actions.push(Box::new(move |info| {
                    Box::pin(check_account_id(info, source, known_validators.clone()))
                }));
            }
            Action::PingTo { source, nonce, target } => {
                self.actions.push(Box::new(move |info: &mut RunningInfo| Box::pin(async move {
                    debug!(target: "network", num_prev_actions, action = ?action_clone, "runner.rs: Action");
                    let target = info.runner.test_config[target].peer_id();
                    info.get_node(source)?.actix.addr.send(PeerManagerMessageRequest::PingTo{
                        nonce, target,
                    }).await?;
                    Ok(ControlFlow::Break(()))
                })));
            }
            Action::Stop(source) => {
                self.actions.push(Box::new(move |info: &mut RunningInfo| Box::pin(async move {
                    debug!(target: "network", num_prev_actions, action = ?action_clone, "runner.rs: Action");
                    info.stop_node(source);
                    Ok(ControlFlow::Break(()))
                })));
            }
            Action::Wait(t) => {
                self.actions.push(Box::new(move |_info: &mut RunningInfo| Box::pin(async move {
                    debug!(target: "network", num_prev_actions, action = ?action_clone, "runner.rs: Action");
                    tokio::time::sleep(t.try_into().unwrap()).await;
                    Ok(ControlFlow::Break(()))
                })));
            }
            Action::CheckPingPong(source, pings, pongs) => {
                self.actions.push(Box::new(move |info| {
                    Box::pin(check_ping_pong(info, source, pings.clone(), pongs.clone()))
                }));
            }
        }
    }
}

struct TestConfig {
    max_num_peers: u32,
    routed_message_ttl: u8,
    boot_nodes: Vec<usize>,
    blacklist: HashSet<Option<usize>>,
    whitelist: HashSet<usize>,
    outbound_disabled: bool,
    ban_window: time::Duration,
    ideal_connections: Option<(u32, u32)>,
    minimum_outbound_peers: Option<u32>,
    safe_set_size: Option<u32>,
    archive: bool,

    account_id: AccountId,
    port: u16,
}

impl TestConfig {
    fn new(id: usize) -> Self {
        Self {
            max_num_peers: 100,
            routed_message_ttl: ROUTED_MESSAGE_TTL,
            boot_nodes: vec![],
            blacklist: HashSet::new(),
            whitelist: HashSet::new(),
            outbound_disabled: true,
            ban_window: time::Duration::seconds(1),
            ideal_connections: None,
            minimum_outbound_peers: None,
            safe_set_size: None,
            archive: false,

            account_id: format!("test{}", id).parse().unwrap(),
            port: open_port(),
        }
    }

    fn addr(&self) -> SocketAddr {
        let ip = Ipv4Addr::LOCALHOST;
        SocketAddr::V4(SocketAddrV4::new(ip, self.port))
    }

    fn peer_id(&self) -> PeerId {
        peer_id_from_seed(&self.account_id)
    }

    fn peer_info(&self) -> PeerInfo {
        PeerInfo::new(self.peer_id(), self.addr())
    }
}

pub struct Runner {
    test_config: Vec<TestConfig>,
    state_machine: StateMachine,
    validators: Vec<AccountId>,
    chain_genesis: ChainGenesis,
}

struct NodeHandle {
    actix: ActixSystem<PeerManagerActor>,
    events: broadcast::Receiver<Event>,
    pings: MultiSet<NetPing>,
    pongs: MultiSet<NetPong>,
}

impl NodeHandle {
    async fn consume_event(&mut self) {
        match self.events.recv().await {
            Event::Ping(ping) => self.pings.insert(ping),
            Event::Pong(pong) => self.pongs.insert(pong),
            _ => {}
        }
    }
}

impl Runner {
    pub fn new(num_nodes: usize, num_validators: usize) -> Self {
        let test_config: Vec<_> = (0..num_nodes).map(TestConfig::new).collect();
        let validators =
            test_config[0..num_validators].iter().map(|c| c.account_id.clone()).collect();
        Self {
            test_config,
            validators,
            state_machine: StateMachine::new(),
            chain_genesis: ChainGenesis::test(),
        }
    }

    /// Add node `v` to the blacklist of node `u`.
    /// If passed `Some(v)` it is created a blacklist entry like:
    ///
    ///     127.0.0.1:PORT_OF_NODE_V
    ///
    /// Use None (instead of Some(v)) if you want to add all other nodes to the blacklist.
    /// If passed None it is created a blacklist entry like:
    ///
    ///     127.0.0.1
    ///
    pub fn add_to_blacklist(mut self, u: usize, v: Option<usize>) -> Self {
        self.test_config[u].blacklist.insert(v);
        self
    }

    /// Add node `v` to the whitelist of node `u`.
    /// If passed `v` an entry of the following form is added to the whitelist:
    ///     PEER_ID_OF_NODE_V@127.0.0.1:PORT_OF_NODE_V
    pub fn add_to_whitelist(mut self, u: usize, v: usize) -> Self {
        self.test_config[u].whitelist.insert(v);
        self
    }

    /// Set node `u` as archival node.
    pub fn set_as_archival(mut self, u: usize) -> Self {
        self.test_config[u].archive = true;
        self
    }

    /// Specify boot nodes. By default there are no boot nodes.
    pub fn use_boot_nodes(mut self, boot_nodes: Vec<usize>) -> Self {
        self.apply_all(move |test_config| {
            test_config.boot_nodes = boot_nodes.clone();
        });
        self
    }

    pub fn ideal_connections(
        mut self,
        ideal_connections_lo: u32,
        ideal_connections_hi: u32,
    ) -> Self {
        self.apply_all(move |test_config| {
            test_config.ideal_connections = Some((ideal_connections_lo, ideal_connections_hi));
        });
        self
    }

    pub fn minimum_outbound_peers(mut self, minimum_outbound_peers: u32) -> Self {
        self.apply_all(move |test_config| {
            test_config.minimum_outbound_peers = Some(minimum_outbound_peers);
        });
        self
    }

    pub fn safe_set_size(mut self, safe_set_size: u32) -> Self {
        self.apply_all(move |test_config| {
            test_config.safe_set_size = Some(safe_set_size);
        });
        self
    }

    pub fn max_num_peers(mut self, max_num_peers: u32) -> Self {
        self.apply_all(move |test_config| {
            test_config.max_num_peers = max_num_peers;
        });
        self
    }

    /// Set ban window range.
    pub fn ban_window(mut self, ban_window: time::Duration) -> Self {
        self.apply_all(move |test_config| test_config.ban_window = ban_window);
        self
    }

    /// Set routed message ttl.
    pub fn routed_message_ttl(mut self, routed_message_ttl: u8) -> Self {
        self.apply_all(move |test_config| {
            test_config.routed_message_ttl = routed_message_ttl;
        });
        self
    }

    /// Allow message to connect among themselves without triggering new connections.
    pub fn enable_outbound(mut self) -> Self {
        self.apply_all(|test_config| {
            test_config.outbound_disabled = false;
        });
        self
    }

    /// Add an action to be executed by the Runner. Actions are executed sequentially.
    /// Each action is executed after the previous action succeed.
    pub fn push(&mut self, action: Action) {
        self.state_machine.push(action);
    }

    /// Add an action to be executed by the Runner. Actions are executed sequentially.
    /// Each action is executed after the previous action succeed.
    pub fn push_action(&mut self, action: ActionFn) {
        self.state_machine.push_action(action);
    }

    fn apply_all<F>(&mut self, mut apply: F)
    where
        F: FnMut(&mut TestConfig) -> (),
    {
        for test_config in self.test_config.iter_mut() {
            apply(test_config);
        }
    }

    async fn setup_node(&self, node_id: usize) -> anyhow::Result<NodeHandle> {
        tracing::debug!("starting {node_id}");
        let config = &self.test_config[node_id];

        let boot_nodes =
            config.boot_nodes.iter().map(|ix| self.test_config[*ix].peer_info()).collect();
        let blacklist: blacklist::Blacklist = config
            .blacklist
            .iter()
            .map(|x| match x {
                Some(x) => blacklist::Entry::from_addr(self.test_config[*x].addr()),
                None => blacklist::Entry::from_ip(Ipv4Addr::LOCALHOST.into()),
            })
            .collect();
        let whitelist =
            config.whitelist.iter().map(|ix| self.test_config[*ix].peer_info()).collect();

        let mut network_config = config::NetworkConfig::from_seed(&config.account_id, config.port);
        network_config.ban_window = config.ban_window;
        network_config.max_num_peers = config.max_num_peers;
        network_config.ttl_account_id_router = time::Duration::seconds(5);
        network_config.routed_message_ttl = config.routed_message_ttl;
        network_config.blacklist = blacklist;
        network_config.whitelist_nodes = whitelist;
        network_config.outbound_disabled = config.outbound_disabled;
        network_config.boot_nodes = boot_nodes;
        network_config.archive = config.archive;
        let (send_events, recv_events) = broadcast::unbounded_channel();
        network_config.event_sink = send_events.sink();

        config.ideal_connections.map(|(lo, hi)| {
            network_config.ideal_connections_lo = lo;
            network_config.ideal_connections_hi = hi;
        });
        config.safe_set_size.map(|sss| {
            network_config.safe_set_size = sss;
        });
        config.minimum_outbound_peers.map(|mop| {
            network_config.minimum_outbound_peers = mop;
        });

        let account_id = config.account_id.clone();
        let validators = self.validators.clone();
        let chain_genesis = self.chain_genesis.clone();

        Ok(NodeHandle {
            actix: ActixSystem::spawn(|| {
                setup_network_node(account_id, validators, chain_genesis, network_config)
            })
            .await,
            events: recv_events,
            pings: MultiSet::default(),
            pongs: MultiSet::default(),
        })
    }

    async fn build(self) -> anyhow::Result<RunningInfo> {
        let mut nodes = vec![];
        for node_id in 0..self.test_config.len() {
            nodes.push(Some(self.setup_node(node_id).await?));
        }
        Ok(RunningInfo { runner: self, nodes })
    }
}

/// Executes the test.
/// It will fail if it doesn't solve all actions.
/// start_test will block until test is complete.
pub fn start_test(runner: Runner) -> anyhow::Result<()> {
    init_test_logger();
    let r = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    r.block_on(async {
        let mut info = runner.build().await?;
        let actions = std::mem::take(&mut info.runner.state_machine.actions);
        let actions_count = actions.len();

        let timeout = tokio::time::Duration::from_secs(15);
        let step = tokio::time::Duration::from_millis(10);
        let start = tokio::time::Instant::now();
        for (i, a) in actions.into_iter().enumerate() {
            tracing::debug!("[starting action {i}]");
            loop {
                let done =
                    tokio::time::timeout_at(start + timeout, a(&mut info)).await.with_context(
                        || format!("timeout while executing action {i}/{actions_count}"),
                    )??;
                match done {
                    ControlFlow::Break(()) => break,
                    ControlFlow::Continue(()) => {}
                }
                tokio::time::sleep(step).await;
            }
        }
        // Stop the running nodes.
        for i in 0..info.nodes.len() {
            info.stop_node(i);
        }
        Ok(())
    })
}

impl RunningInfo {
    fn get_node(&self, node_id: usize) -> anyhow::Result<&NodeHandle> {
        self.nodes[node_id].as_ref().ok_or(anyhow!("node is down"))
    }
    fn stop_node(&mut self, node_id: usize) {
        tracing::debug!("stopping {node_id}");
        self.nodes[node_id].take();
    }

    async fn start_node(&mut self, node_id: usize) -> anyhow::Result<()> {
        self.stop_node(node_id);
        self.nodes[node_id] = Some(self.runner.setup_node(node_id).await?);
        Ok(())
    }
    fn change_account_id(&mut self, node_id: usize, account_id: AccountId) {
        self.runner.test_config[node_id].account_id = account_id;
    }
}

pub fn assert_expected_peers(node_id: usize, peers: Vec<usize>) -> ActionFn {
    Box::new(move |info: &mut RunningInfo| {
        let peers = peers.clone();
        Box::pin(async move {
            let pm = &info.get_node(node_id)?.actix.addr;
            let network_info = pm.send(GetInfo {}).await?;
            let got: HashSet<_> = network_info
                .connected_peers
                .into_iter()
                .map(|i| i.full_peer_info.peer_info.id)
                .collect();
            let want: HashSet<_> =
                peers.iter().map(|i| info.runner.test_config[*i].peer_id()).collect();
            if got != want {
                bail!("node {node_id} has peers {got:?}, want {want:?}");
            }
            return Ok(ControlFlow::Break(()));
        })
    })
}

/// Check that the number of connections of `node_id` is in the range:
/// [expected_connections_lo, expected_connections_hi]
/// Use None to denote semi-open interval
pub fn check_expected_connections(
    node_id: usize,
    expected_connections_lo: Option<usize>,
    expected_connections_hi: Option<usize>,
) -> ActionFn {
    Box::new(move |info: &mut RunningInfo| {
        Box::pin(async move {
            debug!(target: "network", node_id, expected_connections_lo, ?expected_connections_hi, "runner.rs: check_expected_connections");
            let pm = &info.get_node(node_id)?.actix.addr;
            let res = pm.send(GetInfo {}).await?;
            //debug!(target: "dupa", "check_expected_connections: {:?}",res.connected_peers.iter().map(|info|info.full_peer_info.peer_info.id.clone()).collect::<Vec<_>>());
            if expected_connections_lo.map_or(false, |l| l > res.num_connected_peers) {
                return Ok(ControlFlow::Continue(()));
            }
            if expected_connections_hi.map_or(false, |h| h < res.num_connected_peers) {
                return Ok(ControlFlow::Continue(()));
            }
            Ok(ControlFlow::Break(()))
        })
    })
}

async fn check_direct_connection_inner(
    info: &mut RunningInfo,
    node_id: usize,
    target_id: usize,
) -> anyhow::Result<ControlFlow> {
    let target_peer_id = info.runner.test_config[target_id].peer_id();
    debug!(target: "network",  node_id, ?target_id, "runner.rs: check_direct_connection");
    let pm = &info.get_node(node_id)?.actix.addr;
    let rt = match pm.send(PeerManagerMessageRequest::FetchRoutingTable).await? {
        PeerManagerMessageResponse::FetchRoutingTable(rt) => rt,
        _ => bail!("bad response"),
    };
    let routes = if let Some(routes) = rt.next_hops.get(&target_peer_id) {
        routes
    } else {
        debug!(target: "network", ?target_peer_id, node_id, target_id,
            "runner.rs: check_direct_connection NO ROUTES!",
        );
        return Ok(ControlFlow::Continue(()));
    };
    debug!(target: "network", ?target_peer_id, ?routes, node_id, target_id,
        "runner.rs: check_direct_connection",
    );
    if !routes.contains(&target_peer_id) {
        return Ok(ControlFlow::Continue(()));
    }
    Ok(ControlFlow::Break(()))
}

/// Check that `node_id` has a direct connection to `target_id`.
pub fn check_direct_connection(node_id: usize, target_id: usize) -> ActionFn {
    Box::new(move |info| Box::pin(check_direct_connection_inner(info, node_id, target_id)))
}

/// Restart a node that was already stopped.
pub fn restart(node_id: usize) -> ActionFn {
    Box::new(move |info: &mut RunningInfo| {
        Box::pin(async move {
            debug!(target: "network", ?node_id, "runner.rs: restart");
            info.start_node(node_id).await?;
            Ok(ControlFlow::Break(()))
        })
    })
}

async fn ban_peer_inner(
    info: &mut RunningInfo,
    target_peer: usize,
    banned_peer: usize,
) -> anyhow::Result<ControlFlow> {
    debug!(target: "network", target_peer, banned_peer, "runner.rs: ban_peer");
    let banned_peer_id = info.runner.test_config[banned_peer].peer_id();
    let pm = &info.get_node(target_peer)?.actix.addr;
    pm.send(BanPeerSignal::new(banned_peer_id)).await?;
    Ok(ControlFlow::Break(()))
}

/// Ban peer `banned_peer` from perspective of `target_peer`.
pub fn ban_peer(target_peer: usize, banned_peer: usize) -> ActionFn {
    Box::new(move |info| Box::pin(ban_peer_inner(info, target_peer, banned_peer)))
}

/// Change account id from a stopped peer. Notice this will also change its peer id, since
/// peer_id is derived from account id with NetworkConfig::from_seed
pub fn change_account_id(node_id: usize, account_id: AccountId) -> ActionFn {
    Box::new(move |info: &mut RunningInfo| {
        let account_id = account_id.clone();
        Box::pin(async move {
            info.change_account_id(node_id, account_id);
            Ok(ControlFlow::Break(()))
        })
    })
}

/// Wait for predicate to return True.
#[allow(dead_code)]
pub fn wait_for<T>(predicate: T) -> ActionFn
where
    T: 'static + Fn() -> bool,
{
    let predicate = Arc::new(predicate);
    Box::new(move |_info: &mut RunningInfo| {
        let predicate = predicate.clone();
        Box::pin(async move {
            debug!(target: "network", "runner.rs: wait_for predicate");
            if predicate() {
                return Ok(ControlFlow::Break(()));
            }
            Ok(ControlFlow::Continue(()))
        })
    })
}
