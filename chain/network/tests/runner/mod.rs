use std::collections::HashSet;
use std::iter::Iterator;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use actix::{Actor, Addr, AsyncContext, Context, Handler, Message, System};
use chrono::{DateTime, Utc};
use futures::{future, FutureExt, TryFutureExt};

use near_chain::test_utils::KeyValueRuntime;
use near_chain::ChainGenesis;
use near_chain_configs::ClientConfig;
use near_client::{start_view_client, ClientActor};
use near_crypto::KeyType;
use near_logger_utils::init_test_logger;
use near_network::test_utils::{
    convert_boot_nodes, expected_routing_tables, open_port, peer_id_from_seed, BanPeerSignal,
    GetInfo, StopSignal, WaitOrTimeout,
};
use near_network::types::{OutboundTcpConnect, ROUTED_MESSAGE_TTL};
use near_network::utils::blacklist_from_iter;
use near_network::{
    NetworkConfig, NetworkRecipient, NetworkRequests, NetworkResponses, PeerInfo, PeerManagerActor,
};
use near_primitives::types::{AccountId, ValidatorId};
use near_primitives::validator_signer::InMemoryValidatorSigner;
use near_primitives::version::PROTOCOL_VERSION;
use near_store::test_utils::create_test_store;
use near_telemetry::{TelemetryActor, TelemetryConfig};
use num_rational::Rational;

pub type SharedRunningInfo = Arc<RwLock<RunningInfo>>;

pub type ActionFn =
    Box<dyn FnMut(SharedRunningInfo, Arc<AtomicBool>, &mut Context<WaitOrTimeout>, Addr<Runner>)>;

/// Sets up a node with a valid Client, Peer
pub fn setup_network_node(
    account_id: String,
    validators: Vec<String>,
    genesis_time: DateTime<Utc>,
    config: NetworkConfig,
) -> Addr<PeerManagerActor> {
    let store = create_test_store();

    let num_validators = validators.len() as ValidatorId;

    let runtime = Arc::new(KeyValueRuntime::new_with_validators(
        store.clone(),
        vec![validators.into_iter().map(Into::into).collect()],
        1,
        1,
        5,
    ));
    let signer = Arc::new(InMemoryValidatorSigner::from_seed(
        account_id.as_str(),
        KeyType::ED25519,
        account_id.as_str(),
    ));
    let telemetry_actor = TelemetryActor::new(TelemetryConfig::default()).start();
    let chain_genesis = ChainGenesis::new(
        genesis_time,
        0,
        1_000_000,
        100,
        1_000_000_000,
        1_000_000_000,
        Rational::from_integer(0),
        Rational::from_integer(0),
        1000,
        5,
        PROTOCOL_VERSION,
    );

    let peer_manager = PeerManagerActor::create(move |ctx| {
        let mut client_config = ClientConfig::test(false, 100, 200, num_validators, false);
        client_config.ttl_account_id_router = config.ttl_account_id_router;
        let network_adapter = NetworkRecipient::new();
        network_adapter.set_recipient(ctx.address().recipient());
        let network_adapter = Arc::new(network_adapter);
        let client_actor = ClientActor::new(
            client_config.clone(),
            chain_genesis.clone(),
            runtime.clone(),
            config.public_key.clone().into(),
            network_adapter.clone(),
            Some(signer),
            telemetry_actor,
            false,
        )
        .unwrap()
        .start();
        let view_client_actor = start_view_client(
            config.account_id.clone(),
            chain_genesis.clone(),
            runtime.clone(),
            network_adapter.clone(),
            client_config,
        );

        PeerManagerActor::new(
            store.clone(),
            config,
            client_actor.recipient(),
            view_client_actor.recipient(),
        )
        .unwrap()
    });

    peer_manager
}

// TODO: Deprecate this in favor of separate functions.
pub enum Action {
    AddEdge(usize, usize),
    CheckRoutingTable(usize, Vec<(usize, Vec<usize>)>),
    CheckAccountId(usize, Vec<usize>),
    // Send ping from `source` with `nonce` to `target`
    PingTo(usize, usize, usize),
    // Check for `source` received pings and pongs.
    CheckPingPong(usize, Vec<(usize, usize)>, Vec<(usize, usize)>),
    // Send stop signal to some node.
    Stop(usize),
    // Wait time in milliseconds
    Wait(usize),
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
        match action {
            Action::AddEdge(u, v) => {
                self.actions.push(Box::new(
                    move |info: SharedRunningInfo,
                          flag: Arc<AtomicBool>,
                          _ctx: &mut Context<WaitOrTimeout>,
                          _runner| {
                        let addr = info.read().unwrap().pm_addr[u].clone();
                        let peer_info = info.read().unwrap().peers_info[v].clone();
                        actix::spawn(addr.send(OutboundTcpConnect { peer_info }).then(
                            move |res| match res {
                                Ok(_) => {
                                    flag.store(true, Ordering::Relaxed);
                                    future::ready(())
                                }
                                Err(e) => {
                                    panic!("Error adding edge. {:?}", e);
                                }
                            },
                        ));
                    },
                ));
            }
            Action::CheckRoutingTable(u, expected) => self.actions.push(Box::new(
                move |info: SharedRunningInfo,
                      flag: Arc<AtomicBool>,
                      _ctx: &mut Context<WaitOrTimeout>,
                      _runner| {
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
                            .send(NetworkRequests::FetchRoutingTable)
                            .map_err(|_| ())
                            .and_then(move |res| {
                                if let NetworkResponses::RoutingTableInfo(routing_table) = res {
                                    if expected_routing_tables(
                                        routing_table.peer_forwarding,
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
                          _ctx: &mut Context<WaitOrTimeout>,
                          _runner| {
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
                                .send(NetworkRequests::FetchRoutingTable)
                                .map_err(|_| ())
                                .and_then(move |res| {
                                    if let NetworkResponses::RoutingTableInfo(routing_table) = res {
                                        if expected_known.into_iter().all(|validator| {
                                            routing_table.account_peers.contains_key(&validator)
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
                          _ctx: &mut Context<WaitOrTimeout>,
                          _runner| {
                        let target = info.read().unwrap().peers_info[target].id.clone();
                        let _ = info.read().unwrap().pm_addr[source]
                            .do_send(NetworkRequests::PingTo(nonce, target));
                        flag.store(true, Ordering::Relaxed);
                    },
                ));
            }
            Action::Stop(source) => {
                self.actions.push(Box::new(
                    move |info: SharedRunningInfo,
                          flag: Arc<AtomicBool>,
                          _ctx: &mut Context<WaitOrTimeout>,
                          _runner| {
                        actix::spawn(
                            info.read()
                                .unwrap()
                                .pm_addr
                                .get(source)
                                .unwrap()
                                .send(StopSignal::new())
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
                          ctx: &mut Context<WaitOrTimeout>,
                          _runner| {
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
                          _ctx: &mut Context<WaitOrTimeout>,
                          _runner| {
                        let pings_expected: Vec<_> = pings
                            .clone()
                            .into_iter()
                            .map(|(nonce, source)| {
                                (nonce, info.read().unwrap().peers_info[source].id.clone())
                            })
                            .collect();

                        let pongs_expected: Vec<_> = pongs
                            .clone()
                            .into_iter()
                            .map(|(nonce, source)| {
                                (nonce, info.read().unwrap().peers_info[source].id.clone())
                            })
                            .collect();

                        actix::spawn(
                            info.read()
                                .unwrap()
                                .pm_addr
                                .get(source)
                                .unwrap()
                                .send(NetworkRequests::FetchPingPongInfo)
                                .map_err(|_| ())
                                .and_then(move |res| {
                                    if let NetworkResponses::PingPongInfo { pings, pongs } = res {
                                        let len_matches = pings.len() == pings_expected.len()
                                            && pongs.len() == pongs_expected.len();

                                        let ping_ok = len_matches
                                            && pings_expected.into_iter().all(|(nonce, source)| {
                                                pings
                                                    .get(&nonce)
                                                    .map_or(false, |ping| ping.source == source)
                                            });

                                        let pong_ok = len_matches
                                            && pongs_expected.into_iter().all(|(nonce, source)| {
                                                pongs
                                                    .get(&nonce)
                                                    .map_or(false, |pong| pong.source == source)
                                            });

                                        if len_matches && ping_ok && pong_ok {
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
    max_num_peers: u32,
    routed_message_ttl: u8,
    boot_nodes: Vec<usize>,
    blacklist: HashSet<Option<usize>>,
    outbound_disabled: bool,
    ban_window: Duration,
    ideal_connections: Option<(u32, u32)>,
    minimum_outbound_peers: Option<u32>,
    safe_set_size: Option<u32>,
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
            test_config.minimum_outbound_peers = Some(safe_set_size);
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
                .map(|ix| (accounts_id[*ix].as_str(), ports[*ix]))
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
            NetworkConfig::from_seed(accounts_id[node_id].as_str(), ports[node_id].clone());

        network_config.ban_window = test_config.ban_window;
        network_config.max_num_peers = test_config.max_num_peers;
        network_config.ttl_account_id_router = Duration::from_secs(5);
        network_config.routed_message_ttl = test_config.routed_message_ttl;
        network_config.blacklist = blacklist;
        network_config.outbound_disabled = test_config.outbound_disabled;
        network_config.boot_nodes = boot_nodes;

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
            self.genesis_time.clone().unwrap(),
            network_config,
        )
    }

    fn build(&mut self) -> RunningInfo {
        let accounts_id: Vec<_> = (0..self.num_nodes).map(|ix| format!("test{}", ix)).collect();
        let ports: Vec<_> = (0..self.num_nodes).map(|_| open_port()).collect();

        let validators: Vec<_> =
            accounts_id.iter().map(|x| x.clone()).take(self.num_validators).collect();

        let mut peers_info =
            convert_boot_nodes(accounts_id.iter().map(|x| x.as_str()).zip(ports.clone()).collect());

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
    System::run(|| {
        runner.start();
    })
    .unwrap();
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

        WaitOrTimeout::new(
            Box::new(move |ctx| {
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
            50,
            15000,
        )
        .start();
    }
}

#[derive(Message)]
#[rtype(result = "()")]
enum RunnerMessage {
    StartNode(usize),
    ChangeAccountId(usize, String),
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

                write_info.peers_info[node_id].id = peer_id_from_seed(account_id.as_str());
                if write_info.peers_info[node_id].account_id.is_some() {
                    write_info.peers_info[node_id].account_id = Some(account_id);
                }
            }
        }
    }
}

/// Check that `node_id` has at least `expected_connections` as active peers.
pub fn check_expected_connections(node_id: usize, expected_connections: usize) -> ActionFn {
    Box::new(
        move |info: SharedRunningInfo,
              flag: Arc<AtomicBool>,
              _ctx: &mut Context<WaitOrTimeout>,
              _runner| {
            actix::spawn(
                info.read()
                    .unwrap()
                    .pm_addr
                    .get(node_id)
                    .unwrap()
                    .send(GetInfo {})
                    .map_err(|_| ())
                    .and_then(move |res| {
                        if res.num_active_peers >= expected_connections {
                            flag.store(true, Ordering::Relaxed);
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
    Box::new(
        move |_info: SharedRunningInfo,
              flag: Arc<AtomicBool>,
              _ctx: &mut Context<WaitOrTimeout>,
              runner: Addr<Runner>| {
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
    Box::new(
        move |info: SharedRunningInfo,
              flag: Arc<AtomicBool>,
              _ctx: &mut Context<WaitOrTimeout>,
              _runner| {
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
pub fn change_account_id(node_id: usize, account_id: String) -> ActionFn {
    Box::new(
        move |_info: SharedRunningInfo,
              flag: Arc<AtomicBool>,
              _ctx: &mut Context<WaitOrTimeout>,
              runner: Addr<Runner>| {
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
pub fn wait_for<T>(predicate: T) -> ActionFn
where
    T: 'static + Fn() -> bool,
{
    Box::new(
        move |_info: SharedRunningInfo,
              flag: Arc<AtomicBool>,
              _ctx: &mut Context<WaitOrTimeout>,
              _runner: Addr<Runner>| {
            if predicate() {
                flag.store(true, Ordering::Relaxed);
            }
        },
    )
}
