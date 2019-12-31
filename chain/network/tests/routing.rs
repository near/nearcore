use std::iter::Iterator;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use actix::{Actor, Addr, AsyncContext, Context, System};
use chrono::{DateTime, Utc};
use futures::{future, FutureExt, TryFutureExt};

use near_chain::test_utils::KeyValueRuntime;
use near_chain::ChainGenesis;
use near_client::{BlockProducer, ClientActor, ClientConfig, ViewClientActor};
use near_crypto::{InMemorySigner, KeyType};
use near_network::test_utils::{
    convert_boot_nodes, expected_routing_tables, open_port, WaitOrTimeout,
};
use near_network::types::{BlockedPorts, OutboundTcpConnect, StopSignal, ROUTED_MESSAGE_TTL};
use near_network::utils::blacklist_from_vec;
use near_network::{
    NetworkConfig, NetworkRecipient, NetworkRequests, NetworkResponses, PeerInfo, PeerManagerActor,
};
use near_primitives::test_utils::init_test_logger;
use near_primitives::types::ValidatorId;
use near_store::test_utils::create_test_store;
use near_telemetry::{TelemetryActor, TelemetryConfig};
use std::collections::{HashMap, HashSet};
use std::net::IpAddr;

/// Sets up a node with a valid Client, Peer
pub fn setup_network_node(
    account_id: String,
    port: u16,
    boot_nodes: Vec<(String, u16)>,
    validators: Vec<String>,
    genesis_time: DateTime<Utc>,
    peer_max_count: u32,
    blacklist: HashMap<IpAddr, BlockedPorts>,
    routed_message_ttl: u8,
    outbound_disabled: bool,
) -> Addr<PeerManagerActor> {
    let store = create_test_store();

    // Network config
    let ttl_account_id_router = Duration::from_millis(2000);
    let mut config = NetworkConfig::from_seed(account_id.as_str(), port);

    config.outbound_disabled = true;
    config.peer_max_count = peer_max_count;
    config.ttl_account_id_router = ttl_account_id_router;
    config.routed_message_ttl = routed_message_ttl;
    config.blacklist = blacklist;
    config.outbound_disabled = outbound_disabled;

    let boot_nodes = boot_nodes.iter().map(|(acc_id, port)| (acc_id.as_str(), *port)).collect();
    config.boot_nodes = convert_boot_nodes(boot_nodes);

    let num_validators = validators.len() as ValidatorId;

    let runtime = Arc::new(KeyValueRuntime::new_with_validators(
        store.clone(),
        vec![validators.into_iter().map(Into::into).collect()],
        1,
        1,
        5,
    ));
    let signer = Arc::new(InMemorySigner::from_seed(
        account_id.as_str(),
        KeyType::ED25519,
        account_id.as_str(),
    ));
    let block_producer = BlockProducer::from(signer.clone());
    let telemetry_actor = TelemetryActor::new(TelemetryConfig::default()).start();
    let chain_genesis =
        ChainGenesis::new(genesis_time, 1_000_000, 100, 1_000_000_000, 0, 0, 1000, 5);

    let peer_manager = PeerManagerActor::create(move |ctx| {
        let mut client_config = ClientConfig::test(false, 100, 200, num_validators);
        client_config.ttl_account_id_router = ttl_account_id_router;
        let network_adapter = NetworkRecipient::new();
        network_adapter.set_recipient(ctx.address().recipient());
        let network_adapter = Arc::new(network_adapter);
        let client_actor = ClientActor::new(
            client_config,
            store.clone(),
            chain_genesis.clone(),
            runtime.clone(),
            config.public_key.clone().into(),
            network_adapter.clone(),
            Some(block_producer),
            telemetry_actor,
        )
        .unwrap()
        .start();
        let view_client_actor = ViewClientActor::new(
            store.clone(),
            &chain_genesis,
            runtime.clone(),
            network_adapter.clone(),
        )
        .unwrap()
        .start();

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

enum Action {
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
struct RunningInfo {
    pm_addr: Vec<Addr<PeerManagerActor>>,
    peers_info: Vec<PeerInfo>,
}

struct StateMachine {
    actions: Vec<Box<dyn FnMut(RunningInfo, Arc<AtomicBool>, &mut Context<WaitOrTimeout>)>>,
}

impl StateMachine {
    fn new() -> Self {
        Self { actions: vec![] }
    }

    pub fn push(&mut self, action: Action) {
        match action {
            Action::AddEdge(u, v) => {
                self.actions.push(Box::new(
                    move |info: RunningInfo,
                          flag: Arc<AtomicBool>,
                          _ctx: &mut Context<WaitOrTimeout>| {
                        let addr = info.pm_addr[u].clone();
                        let peer_info = info.peers_info[v].clone();
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
                move |info: RunningInfo,
                      flag: Arc<AtomicBool>,
                      _ctx: &mut Context<WaitOrTimeout>| {
                    let expected = expected
                        .clone()
                        .into_iter()
                        .map(|(target, routes)| {
                            (
                                info.peers_info[target].id.clone(),
                                routes
                                    .into_iter()
                                    .map(|hop| info.peers_info[hop].id.clone())
                                    .collect(),
                            )
                        })
                        .collect();

                    actix::spawn(
                        info.pm_addr
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
                    move |info: RunningInfo,
                          flag: Arc<AtomicBool>,
                          _ctx: &mut Context<WaitOrTimeout>| {
                        let expected_known: Vec<_> = known_validators
                            .clone()
                            .into_iter()
                            .map(|u| info.peers_info[u].account_id.clone().unwrap())
                            .collect();

                        actix::spawn(
                            info.pm_addr
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
                    move |info: RunningInfo,
                          flag: Arc<AtomicBool>,
                          _ctx: &mut Context<WaitOrTimeout>| {
                        let target = info.peers_info[target].id.clone();
                        let _ =
                            info.pm_addr[source].do_send(NetworkRequests::PingTo(nonce, target));
                        flag.store(true, Ordering::Relaxed);
                    },
                ));
            }
            Action::Stop(source) => {
                self.actions.push(Box::new(
                    move |info: RunningInfo,
                          flag: Arc<AtomicBool>,
                          _ctx: &mut Context<WaitOrTimeout>| {
                        actix::spawn(
                            info.pm_addr
                                .get(source)
                                .unwrap()
                                .send(StopSignal {})
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
                    move |_info: RunningInfo,
                          flag: Arc<AtomicBool>,
                          ctx: &mut Context<WaitOrTimeout>| {
                        ctx.run_later(Duration::from_millis(time as u64), move |_, _| {
                            flag.store(true, Ordering::Relaxed);
                        });
                    },
                ));
            }
            Action::CheckPingPong(source, pings, pongs) => {
                self.actions.push(Box::new(
                    move |info: RunningInfo,
                          flag: Arc<AtomicBool>,
                          _ctx: &mut Context<WaitOrTimeout>| {
                        let pings_expected: Vec<_> = pings
                            .clone()
                            .into_iter()
                            .map(|(nonce, source)| (nonce, info.peers_info[source].id.clone()))
                            .collect();

                        let pongs_expected: Vec<_> = pongs
                            .clone()
                            .into_iter()
                            .map(|(nonce, source)| (nonce, info.peers_info[source].id.clone()))
                            .collect();

                        actix::spawn(
                            info.pm_addr
                                .get(source)
                                .unwrap()
                                .send(NetworkRequests::FetchPingPongInfo)
                                .map_err(|_| ())
                                .and_then(move |res| {
                                    if let NetworkResponses::PingPongInfo { pings, pongs } = res {
                                        let ping_ok =
                                            pings_expected.into_iter().all(|(nonce, source)| {
                                                pings
                                                    .get(&nonce)
                                                    .map_or(false, |ping| ping.source == source)
                                            });

                                        let pong_ok =
                                            pongs_expected.into_iter().all(|(nonce, source)| {
                                                pongs
                                                    .get(&nonce)
                                                    .map_or(false, |pong| pong.source == source)
                                            });

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

struct Runner {
    num_nodes: usize,
    num_validators: usize,
    peer_max_count: u32,
    state_machine: Option<StateMachine>,
    /// If v is in the list from u, it means v is blacklisted from u point of view.
    /// blacklist[u].contains(v) <=> v is blacklisted from u.
    /// If None is in the list of node u, it means that all other nodes are blacklisted.
    /// It add 127.0.0.1 to the blacklist of node u.
    blacklist: HashMap<usize, HashSet<Option<usize>>>,
    boot_nodes: Vec<usize>,
    routed_message_ttl: u8,
    outbound_disabled: bool,
}

impl Runner {
    fn new(num_nodes: usize, num_validators: usize, peer_max_count: u32) -> Self {
        Self {
            num_nodes,
            num_validators,
            peer_max_count,
            state_machine: Some(StateMachine::new()),
            blacklist: HashMap::new(),
            boot_nodes: vec![],
            routed_message_ttl: ROUTED_MESSAGE_TTL,
            outbound_disabled: true,
        }
    }

    fn add_to_blacklist(mut self, u: usize, v: Option<usize>) -> Self {
        self.blacklist.entry(u).or_insert_with(HashSet::new).insert(v);
        self
    }

    fn use_boot_nodes(mut self, boot_nodes: Vec<usize>) -> Self {
        self.boot_nodes = boot_nodes;
        self
    }

    fn routed_message_ttl(mut self, routed_message_ttl: u8) -> Self {
        self.routed_message_ttl = routed_message_ttl;
        self
    }

    fn enable_outbound(mut self) -> Self {
        self.outbound_disabled = false;
        self
    }

    fn push(&mut self, action: Action) {
        self.state_machine.as_mut().unwrap().push(action);
    }

    fn build(&self) -> RunningInfo {
        let genesis_time = Utc::now();

        let accounts_id: Vec<_> = (0..self.num_nodes).map(|ix| format!("test{}", ix)).collect();
        let ports: Vec<_> = (0..self.num_nodes).map(|_| open_port()).collect();

        let validators: Vec<_> =
            accounts_id.iter().map(|x| x.clone()).take(self.num_validators).collect();

        let mut peers_info =
            convert_boot_nodes(accounts_id.iter().map(|x| x.as_str()).zip(ports.clone()).collect());

        for (validator, peer_info) in validators.iter().zip(peers_info.iter_mut()) {
            peer_info.account_id = Some(validator.clone());
        }

        let boot_nodes: Vec<_> =
            self.boot_nodes.iter().map(|ix| (accounts_id[*ix].clone(), ports[*ix])).collect();

        let pm_addr: Vec<_> = (0..self.num_nodes)
            .map(|ix| {
                let blacklist = blacklist_from_vec(
                    &self
                        .blacklist
                        .get(&ix)
                        .cloned()
                        .unwrap_or_else(HashSet::new)
                        .iter()
                        .map(|x| {
                            if let Some(x) = x {
                                format!("127.0.0.1:{}", ports[*x]).parse().unwrap()
                            } else {
                                "127.0.0.1".parse().unwrap()
                            }
                        })
                        .collect(),
                );

                setup_network_node(
                    accounts_id[ix].clone(),
                    ports[ix].clone(),
                    boot_nodes.clone(),
                    validators.clone(),
                    genesis_time,
                    self.peer_max_count,
                    blacklist,
                    self.routed_message_ttl,
                    self.outbound_disabled,
                )
            })
            .collect();

        RunningInfo { pm_addr, peers_info }
    }

    fn run(&mut self) {
        let info = self.build();

        let mut pointer = None;
        let mut flag = Arc::new(AtomicBool::new(true));
        let mut state_machine = self.state_machine.take().unwrap();

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
                    action(info.clone(), flag.clone(), ctx);
                }
            }),
            50,
            15000,
        )
        .start();
    }
}

#[test]
fn simple() {
    init_test_logger();

    System::run(|| {
        let mut runner = Runner::new(2, 1, 0);

        runner.push(Action::AddEdge(0, 1));
        runner.push(Action::CheckRoutingTable(0, vec![(1, vec![1])]));
        runner.push(Action::CheckRoutingTable(1, vec![(0, vec![0])]));

        runner.run();
    })
    .unwrap();
}

#[test]
fn from_boot_nodes() {
    init_test_logger();

    System::run(|| {
        let mut runner = Runner::new(2, 1, 1).use_boot_nodes(vec![0]).enable_outbound();

        runner.push(Action::CheckRoutingTable(0, vec![(1, vec![1])]));
        runner.push(Action::CheckRoutingTable(1, vec![(0, vec![0])]));

        runner.run();
    })
    .unwrap();
}

#[test]
fn three_nodes_path() {
    init_test_logger();

    System::run(|| {
        let mut runner = Runner::new(3, 2, 0);

        runner.push(Action::AddEdge(0, 1));
        runner.push(Action::AddEdge(1, 2));
        runner.push(Action::CheckRoutingTable(1, vec![(0, vec![0]), (2, vec![2])]));
        runner.push(Action::CheckRoutingTable(0, vec![(1, vec![1]), (2, vec![1])]));
        runner.push(Action::CheckRoutingTable(2, vec![(1, vec![1]), (0, vec![1])]));

        runner.run();
    })
    .unwrap();
}

#[test]
fn three_nodes_star() {
    init_test_logger();

    System::run(|| {
        let mut runner = Runner::new(3, 2, 0);

        runner.push(Action::AddEdge(0, 1));
        runner.push(Action::AddEdge(1, 2));
        runner.push(Action::CheckRoutingTable(1, vec![(0, vec![0]), (2, vec![2])]));
        runner.push(Action::CheckRoutingTable(0, vec![(1, vec![1]), (2, vec![1])]));
        runner.push(Action::CheckRoutingTable(2, vec![(1, vec![1]), (0, vec![1])]));
        runner.push(Action::AddEdge(0, 2));
        runner.push(Action::CheckRoutingTable(1, vec![(0, vec![0]), (2, vec![2])]));
        runner.push(Action::CheckRoutingTable(0, vec![(1, vec![1]), (2, vec![2])]));
        runner.push(Action::CheckRoutingTable(2, vec![(1, vec![1]), (0, vec![0])]));

        runner.run();
    })
    .unwrap();
}

#[test]
fn join_components() {
    init_test_logger();

    System::run(|| {
        let mut runner = Runner::new(4, 4, 0);

        runner.push(Action::AddEdge(0, 1));
        runner.push(Action::AddEdge(2, 3));
        runner.push(Action::CheckRoutingTable(0, vec![(1, vec![1])]));
        runner.push(Action::CheckRoutingTable(1, vec![(0, vec![0])]));
        runner.push(Action::CheckRoutingTable(2, vec![(3, vec![3])]));
        runner.push(Action::CheckRoutingTable(3, vec![(2, vec![2])]));
        runner.push(Action::AddEdge(0, 2));
        runner.push(Action::AddEdge(3, 1));
        runner
            .push(Action::CheckRoutingTable(0, vec![(1, vec![1]), (2, vec![2]), (3, vec![1, 2])]));
        runner
            .push(Action::CheckRoutingTable(3, vec![(1, vec![1]), (2, vec![2]), (0, vec![1, 2])]));
        runner
            .push(Action::CheckRoutingTable(1, vec![(0, vec![0]), (3, vec![3]), (2, vec![0, 3])]));
        runner
            .push(Action::CheckRoutingTable(2, vec![(0, vec![0]), (3, vec![3]), (1, vec![0, 3])]));

        runner.run();
    })
    .unwrap();
}

#[test]
fn account_propagation() {
    init_test_logger();

    System::run(|| {
        let mut runner = Runner::new(3, 2, 0);

        runner.push(Action::AddEdge(0, 1));
        runner.push(Action::CheckAccountId(1, vec![0, 1]));
        runner.push(Action::AddEdge(0, 2));
        runner.push(Action::CheckAccountId(2, vec![0, 1]));

        runner.run();
    })
    .unwrap();
}

#[test]
fn ping_simple() {
    init_test_logger();

    System::run(|| {
        let mut runner = Runner::new(2, 2, 0);

        runner.push(Action::AddEdge(0, 1));
        runner.push(Action::CheckRoutingTable(0, vec![(1, vec![1])]));
        runner.push(Action::PingTo(0, 0, 1));
        runner.push(Action::CheckPingPong(1, vec![(0, 0)], vec![]));
        runner.push(Action::CheckPingPong(0, vec![], vec![(0, 1)]));

        runner.run();
    })
    .unwrap();
}

#[test]
fn ping_jump() {
    init_test_logger();

    System::run(|| {
        let mut runner = Runner::new(3, 2, 0);

        runner.push(Action::AddEdge(0, 1));
        runner.push(Action::AddEdge(1, 2));
        runner.push(Action::CheckRoutingTable(0, vec![(1, vec![1]), (2, vec![1])]));
        runner.push(Action::PingTo(0, 0, 2));
        runner.push(Action::CheckPingPong(2, vec![(0, 0)], vec![]));
        runner.push(Action::CheckPingPong(0, vec![], vec![(0, 2)]));

        runner.run();
    })
    .unwrap();
}

/// Test routed messages are not dropped if have enough TTL.
/// Spawn three nodes and connect them in a line:
///
/// 0 ---- 1 ----- 2
///
/// Set routed message ttl to 2, so routed message can't pass through more than 2 edges.
/// Send Ping from 0 to 2. It should arrive since there are only 2 edges from 0 to 2.
/// Check Ping arrive at node 2 and later Pong arrive at node 0.
#[test]
fn test_dont_drop_after_ttl() {
    init_test_logger();

    System::run(|| {
        let mut runner = Runner::new(3, 1, 3).routed_message_ttl(2);

        runner.push(Action::AddEdge(0, 1));
        runner.push(Action::AddEdge(1, 2));
        runner.push(Action::CheckRoutingTable(0, vec![(1, vec![1]), (2, vec![1])]));
        runner.push(Action::PingTo(0, 0, 2));
        runner.push(Action::CheckPingPong(2, vec![(0, 0)], vec![]));
        runner.push(Action::CheckPingPong(0, vec![], vec![(0, 2)]));

        runner.run();
    })
    .unwrap();
}

/// Test routed messages are dropped if don't have enough TTL.
/// Spawn three nodes and connect them in a line:
///
/// 0 ---- 1 ----- 2
///
/// Set routed message ttl to 1, so routed message can't pass through more than 1 edges.
/// Send Ping from 0 to 2. It should not arrive since there are 2 edges from 0 to 2.
/// Check none of Ping and Pong arrived.
#[test]
fn test_drop_after_ttl() {
    init_test_logger();

    System::run(|| {
        let mut runner = Runner::new(3, 1, 3).routed_message_ttl(1);

        runner.push(Action::AddEdge(0, 1));
        runner.push(Action::AddEdge(1, 2));
        runner.push(Action::CheckRoutingTable(0, vec![(1, vec![1]), (2, vec![1])]));
        runner.push(Action::PingTo(0, 0, 2));
        runner.push(Action::Wait(100));
        runner.push(Action::CheckPingPong(2, vec![], vec![]));
        runner.push(Action::CheckPingPong(0, vec![], vec![]));

        runner.run();
    })
    .unwrap();
}

#[test]
fn simple_remove() {
    init_test_logger();

    System::run(|| {
        let mut runner = Runner::new(3, 3, 0);

        runner.push(Action::AddEdge(0, 1));
        runner.push(Action::AddEdge(1, 2));
        runner.push(Action::CheckRoutingTable(0, vec![(1, vec![1]), (2, vec![1])]));
        runner.push(Action::CheckRoutingTable(2, vec![(1, vec![1]), (0, vec![1])]));
        runner.push(Action::Stop(1));
        runner.push(Action::CheckRoutingTable(0, vec![]));
        runner.push(Action::CheckRoutingTable(2, vec![]));

        runner.run();
    })
    .unwrap();
}

#[test]
fn square() {
    init_test_logger();

    System::run(|| {
        let mut runner = Runner::new(4, 4, 0);

        runner.push(Action::AddEdge(0, 1));
        runner.push(Action::AddEdge(1, 2));
        runner.push(Action::AddEdge(2, 3));
        runner.push(Action::AddEdge(3, 0));
        runner
            .push(Action::CheckRoutingTable(0, vec![(1, vec![1]), (3, vec![3]), (2, vec![1, 3])]));
        runner.push(Action::Stop(1));
        runner.push(Action::CheckRoutingTable(0, vec![(3, vec![3]), (2, vec![3])]));
        runner.push(Action::CheckRoutingTable(2, vec![(3, vec![3]), (0, vec![3])]));
        runner.push(Action::CheckRoutingTable(3, vec![(2, vec![2]), (0, vec![0])]));

        runner.run();
    })
    .unwrap();
}

/// Spin up four nodes and connect them in a square.
/// Each node will have at most two connections.
/// Turn off two non adjacent nodes, and check other two nodes create
/// a connection among them.
#[test]
fn churn_attack() {
    init_test_logger();

    System::run(|| {
        let mut runner = Runner::new(4, 4, 1).enable_outbound();

        runner.push(Action::AddEdge(0, 1));
        runner.push(Action::AddEdge(1, 2));
        runner.push(Action::AddEdge(2, 3));
        runner.push(Action::AddEdge(3, 0));
        runner
            .push(Action::CheckRoutingTable(0, vec![(1, vec![1]), (3, vec![3]), (2, vec![1, 3])]));
        runner
            .push(Action::CheckRoutingTable(2, vec![(1, vec![1]), (3, vec![3]), (0, vec![1, 3])]));
        runner.push(Action::Stop(1));
        runner.push(Action::Stop(3));
        runner.push(Action::CheckRoutingTable(0, vec![(2, vec![2])]));
        runner.push(Action::CheckRoutingTable(2, vec![(0, vec![0])]));

        runner.run();
    })
    .unwrap();
}

#[test]
fn blacklist_01() {
    init_test_logger();

    System::run(|| {
        let mut runner = Runner::new(2, 2, 1).add_to_blacklist(0, Some(1)).use_boot_nodes(vec![0]);

        runner.push(Action::Wait(100));
        runner.push(Action::CheckRoutingTable(1, vec![]));
        runner.push(Action::CheckRoutingTable(0, vec![]));

        runner.run();
    })
    .unwrap();
}

#[test]
fn blacklist_10() {
    init_test_logger();

    System::run(|| {
        let mut runner = Runner::new(2, 2, 1).add_to_blacklist(1, Some(0)).use_boot_nodes(vec![0]);

        runner.push(Action::Wait(100));
        runner.push(Action::CheckRoutingTable(1, vec![]));
        runner.push(Action::CheckRoutingTable(0, vec![]));

        runner.run();
    })
    .unwrap();
}

#[test]
fn blacklist_all() {
    init_test_logger();

    System::run(|| {
        let mut runner = Runner::new(2, 2, 1).add_to_blacklist(0, None).use_boot_nodes(vec![0]);

        runner.push(Action::Wait(100));
        runner.push(Action::CheckRoutingTable(1, vec![]));
        runner.push(Action::CheckRoutingTable(0, vec![]));

        runner.run();
    })
    .unwrap();
}
