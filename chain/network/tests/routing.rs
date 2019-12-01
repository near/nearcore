use std::iter::Iterator;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use actix::{Actor, Addr, AsyncContext, System};
use chrono::{DateTime, Utc};
use futures::{future, Future};

use near_chain::test_utils::KeyValueRuntime;
use near_chain::ChainGenesis;
use near_client::{BlockProducer, ClientActor, ClientConfig};
use near_crypto::{InMemorySigner, KeyType};
use near_network::test_utils::{
    convert_boot_nodes, expected_routing_tables, open_port, WaitOrTimeout,
};
use near_network::types::{OutboundTcpConnect, StopSignal};
use near_network::{NetworkConfig, NetworkRequests, NetworkResponses, PeerInfo, PeerManagerActor};
use near_primitives::test_utils::init_test_logger;
use near_store::test_utils::create_test_store;
use near_telemetry::{TelemetryActor, TelemetryConfig};

/// Sets up a node with a valid Client, Peer
pub fn setup_network_node(
    account_id: String,
    port: u16,
    boot_nodes: Vec<(String, u16)>,
    validators: Vec<String>,
    genesis_time: DateTime<Utc>,
    peer_max_count: u32,
) -> Addr<PeerManagerActor> {
    let store = create_test_store();

    // Network config
    let ttl_account_id_router = Duration::from_millis(2000);
    let mut config = NetworkConfig::from_seed(account_id.as_str(), port);
    config.peer_max_count = peer_max_count;
    config.ttl_account_id_router = ttl_account_id_router;

    let boot_nodes = boot_nodes.iter().map(|(acc_id, port)| (acc_id.as_str(), *port)).collect();
    config.boot_nodes = convert_boot_nodes(boot_nodes);

    let num_validators = validators.len();

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
        let client_actor = ClientActor::new(
            client_config,
            store.clone(),
            chain_genesis,
            runtime,
            config.public_key.clone().into(),
            ctx.address().recipient(),
            Some(block_producer),
            telemetry_actor,
        )
        .unwrap()
        .start();

        PeerManagerActor::new(store.clone(), config, client_actor.recipient()).unwrap()
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
}

#[derive(Clone)]
struct RunningInfo {
    pm_addr: Vec<Addr<PeerManagerActor>>,
    peers_info: Vec<PeerInfo>,
}

struct StateMachine {
    actions: Vec<Box<dyn FnMut(RunningInfo, Arc<AtomicBool>)>>,
}

impl StateMachine {
    fn new() -> Self {
        Self { actions: vec![] }
    }

    pub fn push(&mut self, action: Action) {
        match action {
            Action::AddEdge(u, v) => {
                self.actions.push(Box::new(move |info: RunningInfo, flag: Arc<AtomicBool>| {
                    let addr = info.pm_addr[u].clone();
                    let peer_info = info.peers_info[v].clone();
                    actix::spawn(addr.send(OutboundTcpConnect { peer_info }).then(move |res| {
                        match res {
                            Ok(_) => {
                                flag.store(true, Ordering::Relaxed);
                                future::ok(())
                            }
                            Err(e) => {
                                panic!("Error adding edge. {:?}", e);
                            }
                        }
                    }));
                }));
            }
            Action::CheckRoutingTable(u, expected) => {
                self.actions.push(Box::new(move |info: RunningInfo, flag: Arc<AtomicBool>| {
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
                            }),
                    );
                }))
            }
            Action::CheckAccountId(source, known_validators) => {
                self.actions.push(Box::new(move |info: RunningInfo, flag: Arc<AtomicBool>| {
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
                            }),
                    );
                }));
            }
            Action::PingTo(source, nonce, target) => {
                self.actions.push(Box::new(move |info: RunningInfo, flag: Arc<AtomicBool>| {
                    let target = info.peers_info[target].id.clone();
                    let _ = info.pm_addr[source].do_send(NetworkRequests::PingTo(nonce, target));
                    flag.store(true, Ordering::Relaxed);
                }));
            }
            Action::Stop(source) => {
                self.actions.push(Box::new(move |info: RunningInfo, flag: Arc<AtomicBool>| {
                    actix::spawn(
                        info.pm_addr
                            .get(source)
                            .unwrap()
                            .send(StopSignal {})
                            .map_err(|_| ())
                            .and_then(move |_| {
                                flag.store(true, Ordering::Relaxed);
                                future::ok(())
                            }),
                    );
                }));
            }
            Action::CheckPingPong(source, pings, pongs) => {
                self.actions.push(Box::new(move |info: RunningInfo, flag: Arc<AtomicBool>| {
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
                            }),
                    );
                }));
            }
        }
    }
}

struct Runner {
    num_nodes: usize,
    num_validators: usize,
    peer_max_count: u32,
    state_machine: Option<StateMachine>,
}

impl Runner {
    fn new(num_nodes: usize, num_validators: usize, peer_max_count: u32) -> Self {
        Self { num_nodes, num_validators, peer_max_count, state_machine: Some(StateMachine::new()) }
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

        // Save validators accounts on
        for (validator, peer_info) in validators.iter().zip(peers_info.iter_mut()) {
            peer_info.account_id = Some(validator.clone());
        }

        let pm_addr: Vec<_> = (0..self.num_nodes)
            .map(|ix| {
                setup_network_node(
                    accounts_id[ix].clone(),
                    ports[ix].clone(),
                    vec![],
                    validators.clone(),
                    genesis_time,
                    self.peer_max_count,
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

        // TODO(MarX, #1312): Switch WaitOrTimeout for other mechanism that triggers events on given timeouts
        //  instead of using fixed `check_interval_ms`.
        WaitOrTimeout::new(
            Box::new(move |_| {
                if flag.load(Ordering::Relaxed) {
                    pointer = Some(pointer.map_or(0, |x| x + 1));
                    flag = Arc::new(AtomicBool::new(false));
                }

                if pointer.unwrap() == state_machine.actions.len() {
                    System::current().stop();
                } else {
                    let action = state_machine.actions.get_mut(pointer.unwrap()).unwrap();
                    action(info.clone(), flag.clone());
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
        let mut runner = Runner::new(4, 4, 1);

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
