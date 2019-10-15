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
use near_crypto::InMemoryBlsSigner;
use near_network::test_utils::{
    convert_boot_nodes, expected_routing_tables, open_port, WaitOrTimeout,
};
use near_network::types::OutboundTcpConnect;
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
    let signer = Arc::new(InMemoryBlsSigner::from_seed(account_id.as_str(), account_id.as_str()));
    let block_producer = BlockProducer::from(signer.clone());
    let telemetry_actor = TelemetryActor::new(TelemetryConfig::default()).start();
    let chain_genesis = ChainGenesis::new(genesis_time, 1_000_000, 100, 1_000_000_000, 0, 0, 1000);

    let peer_manager = PeerManagerActor::create(move |ctx| {
        let mut client_config = ClientConfig::test(false, 100, num_validators);
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
}

#[derive(Clone)]
struct RunningInfo {
    pm_addr: Vec<Addr<PeerManagerActor>>,
    peer_info: Vec<PeerInfo>,
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
                    let peer_info = info.peer_info[v].clone();
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
                    let expected1 = expected
                        .clone()
                        .into_iter()
                        .map(|(target, routes)| {
                            (
                                info.peer_info[target].id.clone(),
                                routes
                                    .into_iter()
                                    .map(|hop| info.peer_info[hop].id.clone())
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
                                    // println!("\nROUTING TABLE: {:?}\n", routing_table);
                                    // println!("\nEXPECTED: {:?}\n", expected1);

                                    if expected_routing_tables(routing_table, expected1) {
                                        flag.store(true, Ordering::Relaxed);
                                    }
                                }
                                future::ok(())
                            }),
                    );
                }))
            }
        }
    }
}

struct Runner {
    num_nodes: usize,
    num_validators: usize,
    state_machine: Option<StateMachine>,
}

impl Runner {
    fn new(num_nodes: usize, num_validators: usize) -> Self {
        Self { num_nodes, num_validators, state_machine: Some(StateMachine::new()) }
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

        let peer_info =
            convert_boot_nodes(accounts_id.iter().map(|x| x.as_str()).zip(ports.clone()).collect());
        let pm_addr: Vec<_> = (0..self.num_nodes)
            .map(|ix| {
                setup_network_node(
                    accounts_id[ix].clone(),
                    ports[ix].clone(),
                    vec![],
                    validators.clone(),
                    genesis_time,
                    0,
                )
            })
            .collect();

        RunningInfo { pm_addr, peer_info }
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
        let mut runner = Runner::new(2, 1);

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
        let mut runner = Runner::new(3, 2);

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
        let mut runner = Runner::new(3, 2);

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
        let mut runner = Runner::new(4, 4);

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

// TODO(Marx, #1312): Test handshake nonce after Added -> Removed -> Added

// TODO(MarX, #1312): What happens with Outbound connection if it doesn't receive the Handshake.
//      In this case new edge will be broadcasted but will be unusable from this node POV.
//      The simplest approach here is broadcast edge removal if we receive new edge that we belongs
//      to, but we are not connected to this peer. Note, if we have already broadcasted edge with
//      higher nonce, forget this new connection.

// TODO(MarX, #1312): Test routing (between peers / between validator)
