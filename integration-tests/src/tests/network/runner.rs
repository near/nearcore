use std::collections::HashSet;
use std::iter::Iterator;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use actix::{Actor, Addr, AsyncContext, Context, Handler, Message, System};
use chrono::DateTime;
use futures::{future, FutureExt, TryFutureExt};
use near_primitives::time::Utc;
use tracing::debug;

use near_actix_test_utils::run_actix;
use near_chain::test_utils::KeyValueRuntime;
use near_chain::ChainGenesis;
use near_chain_configs::ClientConfig;
use near_client::{start_client, start_view_client};
use near_crypto::KeyType;
use near_logger_utils::init_test_logger;
use near_network::test_utils::{
    convert_boot_nodes, expected_routing_tables, open_port, peer_id_from_seed, BanPeerSignal,
    GetInfo, NetworkRecipient, StopSignal, WaitOrTimeoutActor,
};

use near_network::routing::start_routing_table_actor;
#[cfg(feature = "test_features")]
use near_network::test_utils::SetAdvOptions;
use near_network::types::{NetworkRequests, NetworkResponses};
use near_network::types::{PeerManagerMessageRequest, PeerManagerMessageResponse};
use near_network::PeerManagerActor;
use near_network_primitives::types::blacklist_from_iter;
use near_network_primitives::types::{
    NetworkConfig, OutboundTcpConnect, PeerInfo, ROUTED_MESSAGE_TTL,
};
use near_primitives::network::PeerId;
use near_primitives::types::{AccountId, ValidatorId};
use near_primitives::validator_signer::InMemoryValidatorSigner;
use near_store::test_utils::create_test_store;
use near_telemetry::{TelemetryActor, TelemetryConfig};

pub type SharedRunningInfo = Arc<RwLock<RunningInfo>>;

pub type ActionFn = Box<
    dyn FnMut(SharedRunningInfo, Arc<AtomicBool>, &mut Context<WaitOrTimeoutActor>, Addr<Runner>),
>;

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
    AddEdge(usize, usize),
    CheckRoutingTable(usize, Vec<(usize, Vec<usize>)>),
    CheckAccountId(usize, Vec<usize>),
    // Send ping from `source` with `nonce` to `target`
    PingTo(usize, usize, usize),
    // Check for `source` received pings and pongs.
    CheckPingPong(usize, Vec<(usize, usize, Option<usize>)>, Vec<(usize, usize, Option<usize>)>),
    // Send stop signal to some node.
    Stop(usize),
    // Wait time in milliseconds
    Wait(usize),
    #[cfg(feature = "test_features")]
    SetOptions {
        target: usize,
        max_num_peers: Option<usize>,
    },
}

#[derive(Clone)]
pub struct RunningInfo {
    pm_addr: Vec<Addr<PeerManagerActor>>,
    peers_info: Vec<PeerInfo>,
}

struct StateMachine {
    actions: Vec<ActionFn>,
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
        let action_clone = action.clone();
        let can_write_log = Arc::new(AtomicBool::new(true));
        match action {
            #[cfg(feature = "test_features")]
            Action::SetOptions { target, max_num_peers } => {
                self.actions.push(Box::new(
                    move |info: SharedRunningInfo,
                          flag: Arc<AtomicBool>,
                          _ctx: &mut Context<WaitOrTimeoutActor>,
                          _runner| {
                        if can_write_log.swap(false, Ordering::Relaxed) == true {
                            debug!(target: "network", num_prev_actions, action = ?action_clone, "runner.rs: Action");
                        }
                        let addr = info.read().unwrap().pm_addr[target].clone();
                        actix::spawn(
                            addr.send(PeerManagerMessageRequest::SetAdvOptions(SetAdvOptions {
                                disable_edge_signature_verification: None,
                                disable_edge_propagation: None,
                                disable_edge_pruning: None,
                                set_max_peers: max_num_peers,
                            }))
                            .then(move |res| match res {
                                Ok(_) => {
                                    flag.store(true, Ordering::Relaxed);
                                    future::ready(())
                                }
                                Err(e) => {
                                    panic!("Error setting options. {:?}", e);
                                }
                            }),
                        );
                    },
                ));
            }
            Action::AddEdge(u, v) => {
                self.actions.push(Box::new(
                    move |info: SharedRunningInfo,
                          flag: Arc<AtomicBool>,
                          _ctx: &mut Context<WaitOrTimeoutActor>,
                          _runner| {
                        if can_write_log.swap(false, Ordering::Relaxed) == true {
                            debug!(target: "network", num_prev_actions, action = ?action_clone, "runner.rs: Action");
                        }

                        let addr = info.read().unwrap().pm_addr[u].clone();
                        let peer_info = info.read().unwrap().peers_info[v].clone();
                        actix::spawn(
                            addr.send(PeerManagerMessageRequest::OutboundTcpConnect(
                                OutboundTcpConnect { peer_info },
                            ))
                            .then(move |res| match res {
                                Ok(_) => {
                                    flag.store(true, Ordering::Relaxed);
                                    future::ready(())
                                }
                                Err(e) => {
                                    panic!("Error adding edge. {:?}", e);
                                }
                            }),
                        );
                    },
                ));
            }
            Action::CheckRoutingTable(u, expected) => self.actions.push(Box::new(
                move |info: SharedRunningInfo,
                      flag: Arc<AtomicBool>,
                      _ctx: &mut Context<WaitOrTimeoutActor>,
                      _runner| {
                    if can_write_log.swap(false, Ordering::Relaxed) == true {
                        debug!(target: "network", num_prev_actions, action = ?action_clone, "runner.rs: Action");
                    }

                    let expected = expected
                        .clone()
                        .into_iter()
                        .map(|(target, routes)| {
                            (
                                info.read().unwrap().peers_info[target].id.clone(),
                                routes
                                    .into_iter()
                                    .map(|hop| info.read().unwrap().peers_info[hop].id.clone())
                                    .collect(),
                            )
                        })
                        .collect();

                    actix::spawn(
                        info.read()
                            .unwrap()
                            .pm_addr
                            .get(u)
                            .unwrap()
                            .send(PeerManagerMessageRequest::NetworkRequests(
                                NetworkRequests::FetchRoutingTable,
                            ))
                            .map_err(|_| ())
                            .and_then(move |res: PeerManagerMessageResponse| {
                                if let NetworkResponses::RoutingTableInfo(routing_table) =
                                    res.as_network_response()
                                {
                                    if expected_routing_tables(
                                        (*routing_table.peer_forwarding.as_ref()).clone(),
                                        expected,
                                    ) {
                                        flag.store(true, Ordering::Relaxed);
                                    }
                                }
                                future::ok(())
                            })
                            .map(drop),
                    );
                },
            )),
            Action::CheckAccountId(source, known_validators) => {
                self.actions.push(Box::new(
                    move |info: SharedRunningInfo,
                          flag: Arc<AtomicBool>,
                          _ctx: &mut Context<WaitOrTimeoutActor>,
                          _runner| {
                        if can_write_log.swap(false, Ordering::Relaxed) == true {
                            debug!(target: "network", num_prev_actions, action = ?action_clone, "runner.rs: Action");
                        }

                        let expected_known: Vec<_> = known_validators
                            .clone()
                            .into_iter()
                            .map(|u| info.read().unwrap().peers_info[u].account_id.clone().unwrap())
                            .collect();

                        actix::spawn(
                            info.read()
                                .unwrap()
                                .pm_addr
                                .get(source)
                                .unwrap()
                                .send(PeerManagerMessageRequest::NetworkRequests(
                                    NetworkRequests::FetchRoutingTable,
                                ))
                                .map_err(|_| ())
                                .and_then(move |res| {
                                    if let NetworkResponses::RoutingTableInfo(routing_table) =
                                        res.as_network_response()
                                    {
                                        if expected_known.into_iter().all(|validator| {
                                            routing_table.account_peers.contains_key(validator.as_ref())
                                        }) {
                                            flag.store(true, Ordering::Relaxed);
                                        }
                                    }
                                    future::ok(())
                                })
                                .map(drop),
                        );
                    },
                ));
            }
            Action::PingTo(source, nonce, target) => {
                self.actions.push(Box::new(
                    move |info: SharedRunningInfo,
                          flag: Arc<AtomicBool>,
                          _ctx: &mut Context<WaitOrTimeoutActor>,
                          _runner| {
                        if can_write_log.swap(false, Ordering::Relaxed) == true {
                            debug!(target: "network", num_prev_actions, action = ?action_clone, "runner.rs: Action");
                        }

                        let target = info.read().unwrap().peers_info[target].id.clone();
                        let _ = info.read().unwrap().pm_addr[source].do_send(
                            PeerManagerMessageRequest::NetworkRequests(NetworkRequests::PingTo(
                                nonce, target,
                            )),
                        );
                        flag.store(true, Ordering::Relaxed);
                    },
                ));
            }
            Action::Stop(source) => {
                self.actions.push(Box::new(
                    move |info: SharedRunningInfo,
                          flag: Arc<AtomicBool>,
                          _ctx: &mut Context<WaitOrTimeoutActor>,
                          _runner| {
                        if can_write_log.swap(false, Ordering::Relaxed) == true {
                            debug!(target: "network", num_prev_actions, action = ?action_clone, "runner.rs: Action");
                        }

                        actix::spawn(
                            info.read()
                                .unwrap()
                                .pm_addr
                                .get(source)
                                .unwrap()
                                .send(StopSignal::default())
                                .map_err(|_| ())
                                .and_then(move |_| {
                                    flag.store(true, Ordering::Relaxed);
                                    future::ok(())
                                })
                                .map(drop),
                        );
                    },
                ));
            }
            Action::Wait(time) => {
                self.actions.push(Box::new(
                    move |_info: SharedRunningInfo,
                          flag: Arc<AtomicBool>,
                          ctx: &mut Context<WaitOrTimeoutActor>,
                          _runner| {
                        if can_write_log.swap(false, Ordering::Relaxed) == true {
                            debug!(target: "network", num_prev_actions, action = ?action_clone, "runner.rs: Action");
                        }

                        ctx.run_later(Duration::from_millis(time as u64), move |_, _| {
                            flag.store(true, Ordering::Relaxed);
                        });
                    },
                ));
            }
            Action::CheckPingPong(source, pings, pongs) => {
                self.actions.push(Box::new(
                    move |info: SharedRunningInfo,
                          flag: Arc<AtomicBool>,
                          _ctx: &mut Context<WaitOrTimeoutActor>,
                          _runner| {
                        if can_write_log.swap(false, Ordering::Relaxed) == true {
                            debug!(target: "network", num_prev_actions, action = ?action_clone, "runner.rs: Action");
                        }

                        let pings_expected: Vec<_> = pings
                            .clone()
                            .into_iter()
                            .map(|(nonce, source, count)| {
                                (nonce, info.read().unwrap().peers_info[source].id.clone(), count)
                            })
                            .collect();

                        let pongs_expected: Vec<_> = pongs
                            .clone()
                            .into_iter()
                            .map(|(nonce, source, count)| {
                                (nonce, info.read().unwrap().peers_info[source].id.clone(), count)
                            })
                            .collect();
                        actix::spawn(
                            info.read()
                                .unwrap()
                                .pm_addr
                                .get(source)
                                .unwrap()
                                .send(PeerManagerMessageRequest::NetworkRequests(
                                    NetworkRequests::FetchPingPongInfo,
                                ))
                                .map_err(|_| ())
                                .and_then(move |res| {
                                    if let NetworkResponses::PingPongInfo { pings, pongs } =
                                        res.as_network_response()
                                    {
                                        let ping_ok = pings.len() == pings_expected.len()
                                            && pings_expected.clone().into_iter().all(
                                                |(nonce, source, count)| {
                                                    pings.get(&nonce).map_or(false, |ping| {
                                                        ping.0.source == source
                                                            && (count.is_none()
                                                                || count.unwrap() == ping.1)
                                                    })
                                                },
                                            );

                                        let pong_ok = pongs.len() == pongs_expected.len()
                                            && pongs_expected.clone().into_iter().all(
                                                |(nonce, source, count)| {
                                                    pongs.get(&nonce).map_or(false, |pong| {
                                                        pong.0.source == source
                                                            && (count.is_none()
                                                                || count.unwrap() == pong.1)
                                                    })
                                                },
                                            );
                                        if ping_ok && pong_ok {
                                            flag.store(true, Ordering::Relaxed);
                                        }
                                    }

                                    future::ok(())
                                })
                                .map(drop),
                        );
                    },
                ));
            }
        }
    }
}

struct TestConfig {
    max_num_peers: usize,
    routed_message_ttl: u8,
    boot_nodes: Vec<usize>,
    blacklist: HashSet<Option<usize>>,
    outbound_disabled: bool,
    ban_window: Duration,
    ideal_connections: Option<(usize, usize)>,
    minimum_outbound_peers: Option<usize>,
    safe_set_size: Option<usize>,
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
    state_machine: Option<StateMachine>,

    info: Option<Arc<RwLock<RunningInfo>>>,

    accounts_id: Option<Vec<AccountId>>,
    ports: Option<Vec<u16>>,
    validators: Option<Vec<AccountId>>,
    genesis_time: Option<DateTime<Utc>>,
}

impl Runner {
    pub fn new(num_nodes: usize, num_validators: usize) -> Self {
        Self {
            num_nodes,
            num_validators,
            test_config: (0..num_nodes).map(|_| TestConfig::new()).collect(),
            state_machine: Some(StateMachine::new()),
            info: None,
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
        ideal_connections_lo: usize,
        ideal_connections_hi: usize,
    ) -> Self {
        self.apply_all(move |test_config| {
            test_config.ideal_connections = Some((ideal_connections_lo, ideal_connections_hi));
        });
        self
    }

    pub fn minimum_outbound_peers(mut self, minimum_outbound_peers: usize) -> Self {
        self.apply_all(move |test_config| {
            test_config.minimum_outbound_peers = Some(minimum_outbound_peers);
        });
        self
    }

    pub fn safe_set_size(mut self, safe_set_size: usize) -> Self {
        self.apply_all(move |test_config| {
            test_config.safe_set_size = Some(safe_set_size);
        });
        self
    }

    pub fn max_num_peers(mut self, max_num_peers: usize) -> Self {
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
        self.state_machine.as_mut().unwrap().push(action);
    }

    /// Add an action to be executed by the Runner. Actions are executed sequentially.
    /// Each action is executed after the previous action succeed.
    pub fn push_action(&mut self, action: ActionFn) {
        self.state_machine.as_mut().unwrap().push_action(action);
    }

    fn apply_all<F>(&mut self, mut apply: F)
    where
        F: FnMut(&mut TestConfig) -> (),
    {
        for test_config in self.test_config.iter_mut() {
            apply(test_config);
        }
    }

    fn setup_node(&self, node_id: usize) -> Addr<PeerManagerActor> {
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

        let blacklist = blacklist_from_iter(test_config.blacklist.iter().map(|x| {
            if let Some(x) = x {
                format!("127.0.0.1:{}", ports[*x])
            } else {
                "127.0.0.1".to_string()
            }
        }));

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

        setup_network_node(
            accounts_id[node_id].clone(),
            self.validators.clone().unwrap(),
            self.genesis_time.unwrap(),
            network_config,
        )
    }

    fn build(&mut self) -> RunningInfo {
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

        let pm_addr: Vec<_> = self
            .test_config
            .iter()
            .enumerate()
            .map(|(node_id, _)| self.setup_node(node_id))
            .collect();

        RunningInfo { pm_addr, peers_info }
    }
}

/// Use to start running the test.
/// It will fail if it doesn't solve all actions.
pub fn start_test(runner: Runner) {
    run_actix(async {
        runner.start();
    })
}

impl Actor for Runner {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        let addr = ctx.address();

        init_test_logger();

        let info = self.build();

        let mut pointer = None;
        let mut flag = Arc::new(AtomicBool::new(true));
        let mut state_machine = self.state_machine.take().unwrap();
        self.info = Some(Arc::new(RwLock::new(info)));

        let info = self.info.as_ref().cloned().unwrap();

        let can_write_log = Arc::new(AtomicBool::new(true));
        WaitOrTimeoutActor::new(
            Box::new(move |ctx| {
                if can_write_log.swap(false, Ordering::Relaxed) == true {
                    debug!(target: "network", "runner.rs: WaitOrTimeoutActor");
                }
                if flag.load(Ordering::Relaxed) {
                    pointer = Some(pointer.map_or(0, |x| x + 1));
                    flag = Arc::new(AtomicBool::new(false));
                }

                if pointer.unwrap() == state_machine.actions.len() {
                    System::current().stop();
                } else {
                    let action = state_machine.actions.get_mut(pointer.unwrap()).unwrap();
                    action(info.clone(), flag.clone(), ctx, addr.clone());
                }
            }),
            1,
            15000,
        )
        .start();
    }
}

#[derive(Message)]
#[rtype(result = "()")]
enum RunnerMessage {
    StartNode(usize),
    ChangeAccountId(usize, AccountId),
}

impl Handler<RunnerMessage> for Runner {
    type Result = ();
    fn handle(&mut self, msg: RunnerMessage, _ctx: &mut Self::Context) -> Self::Result {
        match msg {
            RunnerMessage::StartNode(node_id) => {
                let pm = self.setup_node(node_id);
                let info = self.info.as_ref().cloned().unwrap();
                let mut write_info = info.write().unwrap();
                write_info.pm_addr[node_id] = pm;
            }
            RunnerMessage::ChangeAccountId(node_id, account_id) => {
                self.accounts_id.as_mut().unwrap()[node_id] = account_id.clone();
                let info = self.info.as_ref().cloned().unwrap();
                let mut write_info = info.write().unwrap();

                write_info.peers_info[node_id].id = peer_id_from_seed(account_id.as_ref());
                if write_info.peers_info[node_id].account_id.is_some() {
                    write_info.peers_info[node_id].account_id = Some(account_id);
                }
            }
        }
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
    let can_write_log = Arc::new(AtomicBool::new(true));
    Box::new(
        move |info: SharedRunningInfo,
              flag: Arc<AtomicBool>,
              _ctx: &mut Context<WaitOrTimeoutActor>,
              _runner| {
            if can_write_log.swap(false, Ordering::Relaxed) == true {
                debug!(target: "network", node_id, expected_connections_lo, ?expected_connections_hi, "runner.rs: check_expected_connections");
            }

            actix::spawn(
                info.read()
                    .unwrap()
                    .pm_addr
                    .get(node_id)
                    .unwrap()
                    .send(GetInfo {})
                    .map_err(|_| ())
                    .and_then(move |res| {
                        let left = if let Some(expected_connections_lo) = expected_connections_lo {
                            expected_connections_lo <= res.num_connected_peers
                        } else {
                            true
                        };

                        let right = if let Some(expected_connections_hi) = expected_connections_hi {
                            res.num_connected_peers <= expected_connections_hi
                        } else {
                            true
                        };

                        if left && right {
                            flag.store(true, Ordering::Relaxed);
                        }
                        future::ok(())
                    })
                    .map(drop),
            );
        },
    )
}

/// Check that `node_id` has a direct connection to `target_id`.
pub fn check_direct_connection(node_id: usize, target_id: usize) -> ActionFn {
    let can_write_log = Arc::new(AtomicBool::new(true));
    let can_write_log2 = Arc::new(AtomicBool::new(true));
    let can_write_log3 = Arc::new(AtomicBool::new(true));
    Box::new(
        move |info: SharedRunningInfo,
              flag: Arc<AtomicBool>,
              _ctx: &mut Context<WaitOrTimeoutActor>,
              _runner| {
            let info = info.read().unwrap();
            let target_peer_id = info.peers_info[target_id].id.clone();
            if can_write_log.swap(false, Ordering::Relaxed) == true {
                debug!(target: "network",  node_id, ?target_id, "runner.rs: check_direct_connection");
            }
            let can_write_log2 = can_write_log2.clone();
            let can_write_log3 = can_write_log3.clone();

            actix::spawn(
                info.pm_addr
                    .get(node_id)
                    .unwrap()
                    .send(PeerManagerMessageRequest::NetworkRequests(
                        NetworkRequests::FetchRoutingTable,
                    ))
                    .map_err(|_| ())
                    .and_then(move |res| {
                        if let NetworkResponses::RoutingTableInfo(routing_table) =
                            res.as_network_response()
                        {
                            if let Some(routes) = routing_table.peer_forwarding.get(&target_peer_id)
                            {
                                if routes.contains(&target_peer_id) {
                                    flag.store(true, Ordering::Relaxed);
                                }
                                if can_write_log2.swap(false, Ordering::Relaxed) == true {
                                    debug!(target: "network",
                                        ?target_peer_id, ?routes, flag = flag.load(Ordering::Relaxed),
                                        node_id, target_id,
                                        "runner.rs: check_direct_connection",
                                    );
                                }
                            }
                            else {
                                if can_write_log3.swap(false, Ordering::Relaxed) == true {
                                    debug!(target: "network",
                                        ?target_peer_id, flag = flag.load(Ordering::Relaxed),
                                        node_id, target_id,
                                        "runner.rs: check_direct_connection NO ROUTES!",
                                    );
                                }
                            }
                        } else {
                            panic!("IMPOSSIBLE");
                        }
                        future::ok(())
                    })
                    .map(drop),
            );
        },
    )
}

/// Restart a node that was already stopped.
pub fn restart(node_id: usize) -> ActionFn {
    let can_write_log = Arc::new(AtomicBool::new(true));
    Box::new(
        move |_info: SharedRunningInfo,
              flag: Arc<AtomicBool>,
              _ctx: &mut Context<WaitOrTimeoutActor>,
              runner: Addr<Runner>| {
            if can_write_log.swap(false, Ordering::Relaxed) == true {
                debug!(target: "network", ?node_id, "runner.rs: restart");
            }
            actix::spawn(
                runner
                    .send(RunnerMessage::StartNode(node_id))
                    .map_err(|_| ())
                    .and_then(move |_| {
                        flag.store(true, Ordering::Relaxed);
                        future::ok(())
                    })
                    .map(drop),
            );
        },
    )
}

/// Ban peer `banned_peer` from perspective of `target_peer`.
pub fn ban_peer(target_peer: usize, banned_peer: usize) -> ActionFn {
    let can_write_log = Arc::new(AtomicBool::new(true));
    Box::new(
        move |info: SharedRunningInfo,
              flag: Arc<AtomicBool>,
              _ctx: &mut Context<WaitOrTimeoutActor>,
              _runner| {
            if can_write_log.swap(false, Ordering::Relaxed) == true {
                debug!(target: "network", target_peer, banned_peer, "runner.rs: ban_peer");
            }
            let info = info.read().unwrap();
            let banned_peer_id = info.peers_info[banned_peer].id.clone();
            actix::spawn(
                info.pm_addr
                    .get(target_peer)
                    .unwrap()
                    .send(BanPeerSignal::new(banned_peer_id))
                    .map_err(|_| ())
                    .and_then(move |_| {
                        flag.store(true, Ordering::Relaxed);
                        future::ok(())
                    })
                    .map(drop),
            );
        },
    )
}

/// Change account id from a stopped peer. Notice this will also change its peer id, since
/// peer_id is derived from account id with NetworkConfig::from_seed
pub fn change_account_id(node_id: usize, account_id: AccountId) -> ActionFn {
    let can_write_log = Arc::new(AtomicBool::new(true));
    Box::new(
        move |_info: SharedRunningInfo,
              flag: Arc<AtomicBool>,
              _ctx: &mut Context<WaitOrTimeoutActor>,
              runner: Addr<Runner>| {
            if can_write_log.swap(false, Ordering::Relaxed) == true {
                debug!(target: "network",  ?node_id, ?account_id, "runner.rs: change_account_id");
            }
            actix::spawn(
                runner
                    .send(RunnerMessage::ChangeAccountId(node_id, account_id.clone()))
                    .map_err(|_| ())
                    .and_then(move |_| {
                        flag.store(true, Ordering::Relaxed);
                        future::ok(())
                    })
                    .map(drop),
            );
        },
    )
}

/// Wait for predicate to return True.
#[cfg(feature = "test_features")]
pub fn wait_for<T>(predicate: T) -> ActionFn
where
    T: 'static + Fn() -> bool,
{
    let can_write_log = Arc::new(AtomicBool::new(true));
    Box::new(
        move |_info: SharedRunningInfo,
              flag: Arc<AtomicBool>,
              _ctx: &mut Context<WaitOrTimeoutActor>,
              _runner: Addr<Runner>| {
            if can_write_log.swap(false, Ordering::Relaxed) == true {
                debug!(target: "network", "runner.rs: wait_for predicate");
            }
            if predicate() {
                flag.store(true, Ordering::Relaxed);
            }
        },
    )
}
