use actix::{Actor, Addr};
use anyhow::{anyhow, bail, Context};
use near_async::actix::AddrWithAutoSpanContextExt;
use near_async::actix_wrapper::{spawn_actix_actor, ActixWrapper};
use near_async::messaging::{noop, IntoMultiSender, IntoSender, LateBoundSender};
use near_async::time::{self, Clock};
use near_chain::types::RuntimeAdapter;
use near_chain::{Chain, ChainGenesis};
use near_chain_configs::{ClientConfig, Genesis, GenesisConfig};
use near_chunks::shards_manager_actor::start_shards_manager;
use near_client::adapter::client_sender_for_network;
use near_client::{start_client, PartialWitnessActor, SyncAdapter, ViewClientActorInner};
use near_epoch_manager::shard_tracker::ShardTracker;
use near_epoch_manager::EpochManager;
use near_network::actix::ActixSystem;
use near_network::blacklist;
use near_network::config;
use near_network::tcp;
use near_network::test_utils::{expected_routing_tables, peer_id_from_seed, GetInfo};
use near_network::types::{
    PeerInfo, PeerManagerMessageRequest, PeerManagerMessageResponse, ROUTED_MESSAGE_TTL,
};
use near_network::PeerManagerActor;
use near_o11y::testonly::init_test_logger;
use near_o11y::WithSpanContextExt;
use near_primitives::block::GenesisId;
use near_primitives::network::PeerId;
use near_primitives::test_utils::create_test_signer;
use near_primitives::types::{AccountId, ValidatorId};
use near_primitives::validator_signer::ValidatorSigner;
use near_store::genesis::initialize_genesis_state;
use near_telemetry::{TelemetryActor, TelemetryConfig};
use nearcore::NightshadeRuntime;
use std::collections::HashSet;
use std::future::Future;
use std::iter::Iterator;
use std::net::{Ipv6Addr, SocketAddr};
use std::pin::Pin;
use std::sync::{Arc, RwLock};
use tracing::debug;

pub(crate) type ControlFlow = std::ops::ControlFlow<()>;
pub(crate) type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;
pub(crate) type ActionFn =
    Box<dyn for<'a> Fn(&'a mut RunningInfo) -> BoxFuture<'a, anyhow::Result<ControlFlow>>>;

/// Sets up a node with a valid Client, Peer
fn setup_network_node(
    account_id: AccountId,
    validators: Vec<AccountId>,
    chain_genesis: ChainGenesis,
    config: config::NetworkConfig,
) -> Addr<PeerManagerActor> {
    let node_storage = near_store::test_utils::create_test_node_storage_default();
    let num_validators = validators.len() as ValidatorId;

    let mut genesis = Genesis::test(validators, 1);
    genesis.config.epoch_length = 5;
    let tempdir = tempfile::tempdir().unwrap();
    initialize_genesis_state(node_storage.get_hot_store(), &genesis, Some(tempdir.path()));
    let epoch_manager = EpochManager::new_arc_handle(node_storage.get_hot_store(), &genesis.config);
    let shard_tracker = ShardTracker::new_empty(epoch_manager.clone());
    let runtime = NightshadeRuntime::test(
        tempdir.path(),
        node_storage.get_hot_store(),
        &genesis.config,
        epoch_manager.clone(),
    );
    let signer = Arc::new(create_test_signer(account_id.as_str()));
    let telemetry_actor =
        ActixWrapper::new(TelemetryActor::new(TelemetryConfig::default())).start();

    let db = node_storage.into_inner(near_store::Temperature::Hot);
    let mut client_config =
        ClientConfig::test(false, 100, 200, num_validators, false, true, true, true);
    client_config.archive = config.archive;
    client_config.ttl_account_id_router = config.ttl_account_id_router.try_into().unwrap();
    let genesis_block =
        Chain::make_genesis_block(epoch_manager.as_ref(), runtime.as_ref(), &chain_genesis)
            .unwrap();
    let genesis_id = GenesisId {
        chain_id: client_config.chain_id.clone(),
        hash: *genesis_block.header().hash(),
    };
    let network_adapter = LateBoundSender::new();
    let shards_manager_adapter = LateBoundSender::new();
    let adv = near_client::adversarial::Controls::default();
    let state_sync_adapter = Arc::new(RwLock::new(SyncAdapter::new(
        noop().into_sender(),
        noop().into_sender(),
        SyncAdapter::actix_actor_maker(),
    )));
    let client_actor = start_client(
        Clock::real(),
        client_config.clone(),
        chain_genesis.clone(),
        epoch_manager.clone(),
        shard_tracker.clone(),
        runtime.clone(),
        config.node_id(),
        state_sync_adapter,
        network_adapter.as_multi_sender(),
        shards_manager_adapter.as_sender(),
        Some(signer.clone()),
        telemetry_actor.with_auto_span_context().into_sender(),
        None,
        None,
        adv.clone(),
        None,
        noop().into_multi_sender(),
        true,
        None,
    )
    .client_actor;
    let view_client_addr = ViewClientActorInner::spawn_actix_actor(
        Clock::real(),
        config.validator.as_ref().map(|v| v.account_id()),
        chain_genesis,
        epoch_manager.clone(),
        shard_tracker.clone(),
        runtime.clone(),
        network_adapter.as_multi_sender(),
        client_config.clone(),
        adv,
    );
    let (shards_manager_actor, _) = start_shards_manager(
        epoch_manager.clone(),
        shard_tracker,
        network_adapter.as_sender(),
        client_actor.clone().with_auto_span_context().into_sender(),
        Some(signer.validator_id().clone()),
        runtime.store().clone(),
        client_config.chunk_request_retry_period,
    );
    let (partial_witness_actor, _) = spawn_actix_actor(PartialWitnessActor::new(
        Clock::real(),
        network_adapter.as_multi_sender(),
        client_actor.clone().with_auto_span_context().into_multi_sender(),
        signer,
        epoch_manager,
    ));
    shards_manager_adapter.bind(shards_manager_actor.with_auto_span_context());
    let peer_manager = PeerManagerActor::spawn(
        time::Clock::real(),
        db.clone(),
        config,
        client_sender_for_network(client_actor, view_client_addr),
        shards_manager_adapter.as_sender(),
        partial_witness_actor.with_auto_span_context().into_multi_sender(),
        genesis_id,
    )
    .unwrap();
    network_adapter.bind(peer_manager.clone().with_auto_span_context());
    peer_manager
}

// TODO: Deprecate this in favor of separate functions.
#[derive(Debug, Clone)]
pub(crate) enum Action {
    AddEdge { from: usize, to: usize, force: bool },
    CheckRoutingTable(usize, Vec<(usize, Vec<usize>)>),
    // Send stop signal to some node.
    Stop(usize),
    // Wait time in milliseconds
    Wait(time::Duration),
}

pub(crate) struct RunningInfo {
    runner: Runner,
    nodes: Vec<Option<NodeHandle>>,
}

struct StateMachine {
    actions: Vec<ActionFn>,
}

async fn check_routing_table(
    info: &RunningInfo,
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
    let resp = pm.send(PeerManagerMessageRequest::FetchRoutingTable.with_span_context()).await?;
    let rt = match resp {
        PeerManagerMessageResponse::FetchRoutingTable(rt) => rt,
        _ => bail!("bad response"),
    };
    if expected_routing_tables(&rt.next_hops, &want_rt) {
        return Ok(ControlFlow::Break(()));
    }
    Ok(ControlFlow::Continue(()))
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
            Action::AddEdge { from, to, force } => {
                self.actions.push(Box::new(move |info: &mut RunningInfo| Box::pin(async move {
                    debug!(target: "test", num_prev_actions, action = ?action_clone, "runner.rs: Action");
                    let pm = info.get_node(from)?.actix.addr.clone();
                    let peer_info = info.runner.test_config[to].peer_info();
                    match tcp::Stream::connect(&peer_info, tcp::Tier::T2).await {
                        Ok(stream) => { pm.send(PeerManagerMessageRequest::OutboundTcpConnect(stream).with_span_context()).await?; },
                        Err(err) => tracing::debug!("tcp::Stream::connect({peer_info}): {err}"),
                    }
                    if !force {
                        return Ok(ControlFlow::Break(()))
                    }
                    let peer_id = peer_info.id.clone();
                    let res = pm.send(GetInfo{}.with_span_context()).await?;
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
            Action::Stop(source) => {
                self.actions.push(Box::new(move |info: &mut RunningInfo| Box::pin(async move {
                    debug!(target: "test", num_prev_actions, action = ?action_clone, "runner.rs: Action");
                    info.stop_node(source);
                    Ok(ControlFlow::Break(()))
                })));
            }
            Action::Wait(t) => {
                self.actions.push(Box::new(move |_info: &mut RunningInfo| Box::pin(async move {
                    debug!(target: "test", num_prev_actions, action = ?action_clone, "runner.rs: Action");
                    tokio::time::sleep(t.try_into().unwrap()).await;
                    Ok(ControlFlow::Break(()))
                })));
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
    node_addr: tcp::ListenerAddr,
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
            node_addr: tcp::ListenerAddr::reserve_for_test(),
        }
    }

    fn addr(&self) -> SocketAddr {
        *self.node_addr
    }

    fn peer_id(&self) -> PeerId {
        peer_id_from_seed(self.account_id.as_ref())
    }

    fn peer_info(&self) -> PeerInfo {
        PeerInfo::new(self.peer_id(), self.addr())
    }
}

pub(crate) struct Runner {
    test_config: Vec<TestConfig>,
    state_machine: StateMachine,
    validators: Vec<AccountId>,
    chain_genesis: ChainGenesis,
}

struct NodeHandle {
    actix: ActixSystem<PeerManagerActor>,
}

impl Runner {
    pub fn new(num_nodes: usize, num_validators: usize) -> Self {
        let test_config: Vec<_> = (0..num_nodes).map(TestConfig::new).collect();
        let validators =
            test_config[0..num_validators].iter().map(|c| c.account_id.clone()).collect();
        let chain_genesis = ChainGenesis::new(&GenesisConfig::test(Clock::real()));
        Self { test_config, validators, state_machine: StateMachine::new(), chain_genesis }
    }

    /// Add node `v` to the whitelist of node `u`.
    /// If passed `v` an entry of the following form is added to the whitelist:
    ///     PEER_ID_OF_NODE_V@localhost:PORT_OF_NODE_V
    pub fn add_to_whitelist(mut self, u: usize, v: usize) -> Self {
        self.test_config[u].whitelist.insert(v);
        self
    }

    /// Specify boot nodes. By default there are no boot nodes.
    pub fn use_boot_nodes(mut self, boot_nodes: Vec<usize>) -> Self {
        self.apply_all(move |test_config| {
            test_config.boot_nodes.clone_from(&boot_nodes);
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
                None => blacklist::Entry::from_ip(Ipv6Addr::LOCALHOST.into()),
            })
            .collect();
        let whitelist =
            config.whitelist.iter().map(|ix| self.test_config[*ix].peer_info()).collect();

        let mut network_config =
            config::NetworkConfig::from_seed(config.account_id.as_ref(), config.node_addr);
        network_config.peer_store.ban_window = config.ban_window;
        network_config.max_num_peers = config.max_num_peers;
        network_config.ttl_account_id_router = time::Duration::seconds(5);
        network_config.routed_message_ttl = config.routed_message_ttl;
        network_config.peer_store.blacklist = blacklist;
        network_config.whitelist_nodes = whitelist;
        network_config.outbound_disabled = config.outbound_disabled;
        network_config.peer_store.boot_nodes = boot_nodes;
        network_config.archive = config.archive;

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
pub(crate) fn start_test(runner: Runner) -> anyhow::Result<()> {
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
            tracing::debug!(target: "test", "[starting action {i}]");
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
        self.nodes[node_id].as_ref().ok_or_else(|| anyhow!("node is down"))
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

pub(crate) fn assert_expected_peers(node_id: usize, peers: Vec<usize>) -> ActionFn {
    Box::new(move |info: &mut RunningInfo| {
        let peers = peers.clone();
        Box::pin(async move {
            let pm = &info.get_node(node_id)?.actix.addr;
            let network_info = pm.send(GetInfo {}.with_span_context()).await?;
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
pub(crate) fn check_expected_connections(
    node_id: usize,
    expected_connections_lo: Option<usize>,
    expected_connections_hi: Option<usize>,
) -> ActionFn {
    Box::new(move |info: &mut RunningInfo| {
        Box::pin(async move {
            debug!(target: "test", node_id, expected_connections_lo, ?expected_connections_hi, "runner.rs: check_expected_connections");
            let pm = &info.get_node(node_id)?.actix.addr;
            let res = pm.send(GetInfo {}.with_span_context()).await?;
            if expected_connections_lo.is_some_and(|l| l > res.num_connected_peers) {
                return Ok(ControlFlow::Continue(()));
            }
            if expected_connections_hi.is_some_and(|h| h < res.num_connected_peers) {
                return Ok(ControlFlow::Continue(()));
            }
            Ok(ControlFlow::Break(()))
        })
    })
}

/// Restart a node that was already stopped.
pub(crate) fn restart(node_id: usize) -> ActionFn {
    Box::new(move |info: &mut RunningInfo| {
        Box::pin(async move {
            debug!(target: "test", ?node_id, "runner.rs: restart");
            info.start_node(node_id).await?;
            Ok(ControlFlow::Break(()))
        })
    })
}

/// Change account id from a stopped peer. Notice this will also change its peer id, since
/// peer_id is derived from account id with NetworkConfig::from_seed
pub(crate) fn change_account_id(node_id: usize, account_id: AccountId) -> ActionFn {
    Box::new(move |info: &mut RunningInfo| {
        let account_id = account_id.clone();
        Box::pin(async move {
            info.change_account_id(node_id, account_id);
            Ok(ControlFlow::Break(()))
        })
    })
}
