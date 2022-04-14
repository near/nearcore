use std::collections::HashSet;
use std::future::Future;
use std::iter::Iterator;
#[allow(unused_imports)]
use std::sync::{Arc, RwLock};
use std::time::Duration;

use actix::{Actor, Addr, AsyncContext};
use anyhow::{anyhow, bail};
use chrono::DateTime;
use near_primitives::time::Utc;
use tracing::debug;

use near_chain::test_utils::KeyValueRuntime;
use near_chain::ChainGenesis;
use near_chain_configs::ClientConfig;
use near_client::{start_client, start_view_client};
use near_crypto::KeyType;
use near_logger_utils::init_test_logger;
use near_network::test_utils::{
    convert_boot_nodes, expected_routing_tables, open_port, peer_id_from_seed, BanPeerSignal,
    GetInfo, NetworkRecipient,
};

use near_network::routing::start_routing_table_actor;
#[cfg(feature = "test_features")]
use near_network::test_utils::SetAdvOptions;
use near_network::types::PeerManagerMessageRequest;
use near_network::types::{NetworkRequests, NetworkResponses};
use near_network::PeerManagerActor;
use near_network_primitives::types::{
    NetworkConfig, OutboundTcpConnect, PeerInfo, ROUTED_MESSAGE_TTL,
};
use near_primitives::network::PeerId;
use near_primitives::types::{AccountId, ValidatorId};
use near_primitives::validator_signer::InMemoryValidatorSigner;
use near_store::test_utils::create_test_store;
use near_telemetry::{TelemetryActor, TelemetryConfig};
use std::pin::Pin;

pub type ControlFlow = std::ops::ControlFlow<()>;
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;
pub type ActionFn =
    Box<dyn for<'a> Fn(&'a mut RunningInfo) -> BoxFuture<'a, anyhow::Result<ControlFlow>>>;

/// Sets up a node with a valid Client, Peer
pub fn setup_network_node(
    account_id: AccountId,
    validators: Vec<AccountId>,
    genesis_time: DateTime<Utc>,
    config: NetworkConfig,
) -> Addr<PeerManagerActor> {
    let store = create_test_store();

    let num_validators = validators.len() as ValidatorId;

    let runtime =
        Arc::new(KeyValueRuntime::new_with_validators(store.clone(), vec![validators], 1, 1, 5));
    let signer = Arc::new(InMemoryValidatorSigner::from_seed(
        account_id.clone(),
        KeyType::ED25519,
        account_id.as_ref(),
    ));
    let telemetry_actor = TelemetryActor::new(TelemetryConfig::default()).start();
    let mut chain_genesis = ChainGenesis::test();
    chain_genesis.time = genesis_time;

    let peer_manager = PeerManagerActor::create(move |ctx| {
        let mut client_config = ClientConfig::test(false, 100, 200, num_validators, false, true);
        client_config.archive = config.archive;
        client_config.ttl_account_id_router = config.ttl_account_id_router;
        let network_adapter = NetworkRecipient::default();
        network_adapter.set_recipient(ctx.address().recipient());
        let network_adapter = Arc::new(network_adapter);
        #[cfg(feature = "test_features")]
        let adv = Arc::new(RwLock::new(Default::default()));

        let client_actor = start_client(
            client_config.clone(),
            chain_genesis.clone(),
            runtime.clone(),
            PeerId::new(config.public_key.clone()),
            network_adapter.clone(),
            Some(signer),
            telemetry_actor,
            None,
            #[cfg(feature = "test_features")]
            adv.clone(),
        )
        .0;
        let view_client_actor = start_view_client(
            config.account_id.clone(),
            chain_genesis.clone(),
            runtime.clone(),
            network_adapter,
            client_config,
            #[cfg(feature = "test_features")]
            adv.clone(),
        );

        let routing_table_addr =
            start_routing_table_actor(PeerId::new(config.public_key.clone()), store.clone());

        PeerManagerActor::new(
            store.clone(),
            config,
            client_actor.recipient(),
            view_client_actor.recipient(),
            routing_table_addr,
        )
        .unwrap()
    });

    peer_manager
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
    PingTo(usize, usize, usize),
    // Check for `source` received pings and pongs.
    CheckPingPong(usize, Vec<(usize, usize, Option<usize>)>, Vec<(usize, usize, Option<usize>)>),
    // Send stop signal to some node.
    Stop(usize),
    // Wait time in milliseconds
    Wait(Duration),
    #[allow(dead_code)]
    SetOptions {
        target: usize,
        max_num_peers: Option<u64>,
    },
}

pub struct RunningInfo {
    runner: Runner,
    nodes: Vec<Option<NodeHandle>>,
    peers_info: Vec<PeerInfo>,
}

struct StateMachine {
    actions: Vec<ActionFn>,
}

async fn check_routing_table(
    info: &mut RunningInfo,
    u: usize,
    expected: Vec<(usize, Vec<usize>)>,
) -> anyhow::Result<ControlFlow> {
    let mut expected_rt = vec![];
    for (target, routes) in expected {
        let mut peers = vec![];
        for hop in routes {
            peers.push(info.peers_info[hop].id.clone());
        }
        expected_rt.push((info.peers_info[target].id.clone(), peers));
    }
    let pm = info.get_node(u)?.addr.clone();
    let resp = pm
        .send(PeerManagerMessageRequest::NetworkRequests(NetworkRequests::FetchRoutingTable))
        .await?;
    let rt = if let NetworkResponses::RoutingTableInfo(rt) = resp.as_network_response() {
        rt
    } else {
        bail!("bad response")
    };
    if expected_routing_tables((*rt.peer_forwarding.as_ref()).clone(), expected_rt) {
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
        expected_known.push(info.peers_info[u].account_id.clone().unwrap());
    }
    let pm = &info.get_node(source)?.addr;
    let resp = pm
        .send(PeerManagerMessageRequest::NetworkRequests(NetworkRequests::FetchRoutingTable))
        .await?;
    let rt = if let NetworkResponses::RoutingTableInfo(rt) = resp.as_network_response() {
        rt
    } else {
        bail!("bad response")
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
    pings: Vec<(usize, usize, Option<usize>)>,
    pongs: Vec<(usize, usize, Option<usize>)>,
) -> anyhow::Result<ControlFlow> {
    let mut pings_expected = vec![];
    for (nonce, source, count) in pings {
        pings_expected.push((nonce, info.peers_info[source].id.clone(), count));
    }
    let mut pongs_expected = vec![];
    for (nonce, source, count) in pongs {
        pongs_expected.push((nonce, info.peers_info[source].id.clone(), count));
    }
    let pm = &info.get_node(source)?.addr;
    let resp = pm
        .send(PeerManagerMessageRequest::NetworkRequests(NetworkRequests::FetchPingPongInfo))
        .await?;
    let (pings, pongs) =
        if let NetworkResponses::PingPongInfo { pings, pongs } = resp.as_network_response() {
            (pings, pongs)
        } else {
            bail!("bad response")
        };
    if pings.len() != pings_expected.len() {
        return Ok(ControlFlow::Continue(()));
    }
    for (nonce, source, count) in pings_expected {
        let ping = if let Some(ping) = pings.get(&nonce) {
            ping
        } else {
            return Ok(ControlFlow::Continue(()));
        };
        if ping.0.source != source || count.map_or(false, |c| c != ping.1) {
            return Ok(ControlFlow::Continue(()));
        }
    }
    if pongs.len() != pongs_expected.len() {
        return Ok(ControlFlow::Continue(()));
    }
    for (nonce, source, count) in pongs_expected {
        let pong = if let Some(pong) = pongs.get(&nonce) {
            pong
        } else {
            return Ok(ControlFlow::Continue(()));
        };
        if pong.0.source != source || count.map_or(false, |c| c != pong.1) {
            return Ok(ControlFlow::Continue(()));
        }
    }
    Ok(ControlFlow::Break(()))
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
                    #[allow(unused_variables)]
                    let addr = info.get_node(target)?.addr.clone();
                    #[cfg(feature = "test_features")]
                    addr.send(PeerManagerMessageRequest::SetAdvOptions(SetAdvOptions {
                        disable_edge_signature_verification: None,
                        disable_edge_propagation: None,
                        disable_edge_pruning: None,
                        set_max_peers: max_num_peers,
                    })).await?;
                    Ok(ControlFlow::Break(()))
                })));
            }
            Action::AddEdge { from, to, force } => {
                self.actions.push(Box::new(move |info: &mut RunningInfo| Box::pin(async move {
                    debug!(target: "network", num_prev_actions, action = ?action_clone, "runner.rs: Action");
                    let pm = info.get_node(from)?.addr.clone();
                    let peer_info = info.peers_info[to].clone();
                    let peer_id = peer_info.id.clone();
                    pm.send(PeerManagerMessageRequest::OutboundTcpConnect(
                        OutboundTcpConnect { peer_info },
                    )).await?;
                    if !force {
                        return Ok(ControlFlow::Break(()))
                    }
                    let res = pm.send(GetInfo{}).await?;
                    for peer in &res.connected_peers {
                        if peer.peer_info.id==peer_id {
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
            Action::PingTo(source, nonce, target) => {
                self.actions.push(Box::new(move |info: &mut RunningInfo| Box::pin(async move {
                    debug!(target: "network", num_prev_actions, action = ?action_clone, "runner.rs: Action");
                    let target = info.peers_info[target].id.clone();
                    info.get_node(source)?.addr.send(PeerManagerMessageRequest::NetworkRequests(NetworkRequests::PingTo(
                        nonce, target,
                    ))).await?;
                    Ok(ControlFlow::Break(()))
                })));
            }
            Action::Stop(source) => {
                self.actions.push(Box::new(move |info: &mut RunningInfo| Box::pin(async move {
                    debug!(target: "network", num_prev_actions, action = ?action_clone, "runner.rs: Action");
                    info.stop_node(source).await?;
                    Ok(ControlFlow::Break(()))
                })));
            }
            Action::Wait(t) => {
                self.actions.push(Box::new(move |_info: &mut RunningInfo| Box::pin(async move {
                    debug!(target: "network", num_prev_actions, action = ?action_clone, "runner.rs: Action");
                    tokio::time::sleep(t).await;
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
    outbound_disabled: bool,
    ban_window: Duration,
    ideal_connections: Option<(u32, u32)>,
    minimum_outbound_peers: Option<u32>,
    safe_set_size: Option<u32>,
    archive: bool,
}

impl TestConfig {
    fn new() -> Self {
        Self {
            max_num_peers: 100,
            routed_message_ttl: ROUTED_MESSAGE_TTL,
            boot_nodes: vec![],
            blacklist: HashSet::new(),
            outbound_disabled: true,
            ban_window: Duration::from_secs(1),
            ideal_connections: None,
            minimum_outbound_peers: None,
            safe_set_size: None,
            archive: false,
        }
    }
}

pub struct Runner {
    num_nodes: usize,
    num_validators: usize,
    test_config: Vec<TestConfig>,
    state_machine: StateMachine,

    accounts_id: Option<Vec<AccountId>>,
    ports: Option<Vec<u16>>,
    validators: Option<Vec<AccountId>>,
    genesis_time: Option<DateTime<Utc>>,
}

struct NodeHandle {
    addr: Addr<PeerManagerActor>,
    send_stop: tokio::sync::oneshot::Sender<std::convert::Infallible>,
    handle: std::thread::JoinHandle<anyhow::Result<()>>,
}

impl NodeHandle {
    async fn stop(self) -> anyhow::Result<()> {
        let handle = self.handle;
        drop(self.send_stop);
        tokio::task::spawn_blocking(|| handle.join().map_err(|_| anyhow!("node panicked"))?)
            .await??;
        Ok(())
    }
}

impl Runner {
    pub fn new(num_nodes: usize, num_validators: usize) -> Self {
        Self {
            num_nodes,
            num_validators,
            test_config: (0..num_nodes).map(|_| TestConfig::new()).collect(),
            state_machine: StateMachine::new(),
            accounts_id: None,
            ports: None,
            validators: None,
            genesis_time: None,
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
    pub fn ban_window(mut self, ban_window: Duration) -> Self {
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
        let accounts_id = self.accounts_id.as_ref().unwrap();
        let ports = self.ports.as_ref().unwrap();
        let test_config = &self.test_config[node_id];

        let boot_nodes = convert_boot_nodes(
            test_config
                .boot_nodes
                .iter()
                .map(|ix| (accounts_id[*ix].as_ref(), ports[*ix]))
                .collect(),
        );

        let blacklist = test_config
            .blacklist
            .iter()
            .map(|x| {
                if let Some(x) = x {
                    format!("127.0.0.1:{}", ports[*x])
                } else {
                    "127.0.0.1".to_string()
                }
            })
            .collect();

        let mut network_config =
            NetworkConfig::from_seed(accounts_id[node_id].as_ref(), ports[node_id]);

        network_config.ban_window = test_config.ban_window;
        network_config.max_num_peers = test_config.max_num_peers;
        network_config.ttl_account_id_router = Duration::from_secs(5);
        network_config.routed_message_ttl = test_config.routed_message_ttl;
        network_config.blacklist = blacklist;
        network_config.outbound_disabled = test_config.outbound_disabled;
        network_config.boot_nodes = boot_nodes;
        network_config.archive = test_config.archive;

        network_config.ideal_connections_lo =
            test_config.ideal_connections.map_or(network_config.ideal_connections_lo, |(lo, _)| lo);
        network_config.ideal_connections_hi =
            test_config.ideal_connections.map_or(network_config.ideal_connections_hi, |(_, hi)| hi);
        network_config.safe_set_size =
            test_config.safe_set_size.unwrap_or(network_config.safe_set_size);
        network_config.minimum_outbound_peers =
            test_config.minimum_outbound_peers.unwrap_or(network_config.minimum_outbound_peers);

        let (send_pm, recv_pm) = tokio::sync::oneshot::channel();
        let (send_stop, recv_stop) = tokio::sync::oneshot::channel();
        let handle = std::thread::spawn({
            let account_id = accounts_id[node_id].clone();
            let validators = self.validators.clone().unwrap();
            let genesis_time = self.genesis_time.unwrap();
            move || {
                actix::System::new().block_on(async move {
                    send_pm
                        .send(setup_network_node(
                            account_id,
                            validators,
                            genesis_time,
                            network_config,
                        ))
                        .map_err(|_| anyhow!("send failed"))?;
                    // recv_stop is expected to get closed.
                    recv_stop.await.unwrap_err();
                    Ok(())
                })
            }
        });
        let addr = recv_pm.await?;
        Ok(NodeHandle { addr, send_stop, handle })
    }

    async fn build(mut self) -> anyhow::Result<RunningInfo> {
        let accounts_id: Vec<_> = (0..self.num_nodes)
            .map(|ix| format!("test{}", ix).parse::<AccountId>().unwrap())
            .collect();
        let ports: Vec<_> = (0..self.num_nodes).map(|_| open_port()).collect();

        let validators: Vec<_> = accounts_id.iter().cloned().take(self.num_validators).collect();

        let mut peers_info =
            convert_boot_nodes(accounts_id.iter().map(|x| x.as_ref()).zip(ports.clone()).collect());

        for (validator, peer_info) in validators.iter().zip(peers_info.iter_mut()) {
            peer_info.account_id = Some(validator.clone());
        }

        self.genesis_time = Some(Utc::now());
        self.accounts_id = Some(accounts_id);
        self.ports = Some(ports);
        self.validators = Some(validators);

        let mut nodes = vec![];
        for (node_id, _) in self.test_config.iter().enumerate() {
            nodes.push(Some(self.setup_node(node_id).await?));
        }
        Ok(RunningInfo { runner: self, nodes, peers_info })
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

        let timeout = Duration::from_secs(15);
        let step = Duration::from_millis(10);
        let start = tokio::time::Instant::now();
        for (i, a) in actions.into_iter().enumerate() {
            loop {
                tokio::select! {
                    done = a(&mut info) => {
                        match done? {
                            ControlFlow::Break(_) => { break }
                            ControlFlow::Continue(_) => {}
                        }
                    }
                    () = tokio::time::sleep_until(start + timeout) => {
                        bail!("timeout while executing action {i}/{actions_count}");
                    }
                }
                tokio::time::sleep(step).await;
            }
        }
        // Stop the running nodes.
        for i in 0..info.nodes.len() {
            info.stop_node(i).await?;
        }
        return Ok(());
    })
}

impl RunningInfo {
    fn get_node(&self, node_id: usize) -> anyhow::Result<&NodeHandle> {
        self.nodes[node_id].as_ref().ok_or(anyhow!("node is down"))
    }
    async fn stop_node(&mut self, node_id: usize) -> anyhow::Result<()> {
        if let Some(n) = self.nodes[node_id].take() {
            n.stop().await?;
        }
        Ok(())
    }

    async fn start_node(&mut self, node_id: usize) -> anyhow::Result<()> {
        self.stop_node(node_id).await?;
        self.nodes[node_id] = Some(self.runner.setup_node(node_id).await?);
        Ok(())
    }
    fn change_account_id(&mut self, node_id: usize, account_id: AccountId) -> anyhow::Result<()> {
        self.runner.accounts_id.as_mut().unwrap()[node_id] = account_id.clone();
        self.peers_info[node_id].id = peer_id_from_seed(account_id.as_ref());
        if self.peers_info[node_id].account_id.is_some() {
            self.peers_info[node_id].account_id = Some(account_id);
        }
        Ok(())
    }
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
            let pm = &info.get_node(node_id)?.addr;
            let res = pm.send(GetInfo {}).await?;
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
    let target_peer_id = info.peers_info[target_id].id.clone();
    debug!(target: "network",  node_id, ?target_id, "runner.rs: check_direct_connection");
    let pm = &info.get_node(node_id)?.addr;
    let resp = pm
        .send(PeerManagerMessageRequest::NetworkRequests(NetworkRequests::FetchRoutingTable))
        .await?;
    let rt = if let NetworkResponses::RoutingTableInfo(rt) = resp.as_network_response() {
        rt
    } else {
        bail!("bad response");
    };
    let routes = if let Some(routes) = rt.peer_forwarding.get(&target_peer_id) {
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
    let banned_peer_id = info.peers_info[banned_peer].id.clone();
    let pm = &info.get_node(target_peer)?.addr;
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
            // debug!(target: "network",  ?node_id, ?account_id, "runner.rs: change_account_id");
            info.change_account_id(node_id, account_id)?;
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
