use std::iter::Iterator;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use actix::{Actor, Addr, AsyncContext, System};
use chrono::{DateTime, Utc};
use futures::{future, Future};

use near_chain::test_utils::KeyValueRuntime;
use near_chain::ChainGenesis;
use near_client::{BlockProducer, ClientActor, ClientConfig};
use near_crypto::InMemoryBlsSigner;
use near_network::test_utils::{convert_boot_nodes, open_port, vec_ref_to_str, WaitOrTimeout};
use near_network::types::NetworkInfo;
use near_network::{NetworkConfig, NetworkRequests, NetworkResponses, PeerManagerActor};
use near_primitives::test_utils::init_test_logger;
use near_primitives::types::AccountId;
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

fn wait_routing_table_state(
    peer_managers: Vec<Addr<PeerManagerActor>>,
    counters: Vec<Arc<AtomicUsize>>,
    flag: Arc<AtomicBool>,
    accounts_id: Vec<AccountId>,
    expected_accounts: Vec<AccountId>,
) {
    for ((pm, count), cur_acc_id) in peer_managers.iter().zip(counters.iter()).zip(accounts_id) {
        let pm = pm.clone();
        let count = count.clone();

        let counters = counters.clone();
        let flag = flag.clone();
        let expected_accounts = expected_accounts.clone();

        if count.load(Ordering::Relaxed) == 0 {
            actix::spawn(pm.send(NetworkRequests::FetchInfo).then(move |res| {
                if let NetworkResponses::Info(NetworkInfo {
                    known_producers: account_peers, ..
                }) = res.unwrap()
                {
                    let mut count_expected = 0;

                    for acc_id in expected_accounts.into_iter() {
                        if acc_id == cur_acc_id {
                            assert!(!account_peers.contains(&acc_id));
                        } else {
                            if account_peers.contains(&acc_id) {
                                count_expected += 1;
                            } else {
                                // missing somme account id in routes yet.
                                return future::result(Ok(()));
                            }
                        }
                    }

                    if count_expected == account_peers.len() {
                        count.fetch_add(1, Ordering::Relaxed);
                        if counters.iter().all(|counter| counter.load(Ordering::Relaxed) == 1) {
                            flag.store(true, Ordering::Relaxed);
                        }
                    }
                }
                future::result(Ok(()))
            }));
        }
    }
}

enum States<F>
where
    F: FnMut(),
{
    CheckpointRoutingTableState(Vec<AccountId>),
    Action(Option<F>),
}

impl<F> States<F>
where
    F: FnMut(),
{
    fn get(&mut self) -> States<F> {
        match self {
            States::CheckpointRoutingTableState(expected_accounts) => {
                States::CheckpointRoutingTableState(expected_accounts.clone())
            }
            States::Action(action) => States::Action(action.take()),
        }
    }
}

struct StateMachine<F>
where
    F: FnMut(),
{
    peer_managers: Vec<Addr<PeerManagerActor>>,
    accounts_id: Vec<AccountId>,
    flag: Arc<AtomicBool>,
    counters: Vec<Arc<AtomicUsize>>,
    states: Vec<States<F>>,
    pointer: usize,
}

impl<F> StateMachine<F>
where
    F: FnMut(),
{
    fn new(peer_managers: Vec<Addr<PeerManagerActor>>, accounts_id: Vec<AccountId>) -> Self {
        let counters = (0..peer_managers.len()).map(|_| Arc::new(AtomicUsize::new(0))).collect();
        Self {
            peer_managers,
            accounts_id,
            flag: Arc::new(AtomicBool::new(false)),
            counters,
            states: Vec::new(),
            pointer: 0,
        }
    }

    fn add_checkpoint(&mut self, expected_accounts: Vec<AccountId>) {
        self.states.push(States::CheckpointRoutingTableState(expected_accounts));
    }

    fn add_action(&mut self, action: F) {
        self.states.push(States::Action(Some(action)));
    }

    fn step(&mut self) -> bool {
        if self.flag.load(Ordering::Relaxed) {
            self.flag.store(false, Ordering::Relaxed);
            for counter in self.counters.iter() {
                counter.store(0, Ordering::Relaxed);
            }
            self.pointer += 1;
        }

        if self.pointer == self.states.len() {
            return true;
        }

        let state = self.states.get_mut(self.pointer).unwrap();

        match state.get() {
            States::Action(action) => {
                let mut action = action.unwrap();
                action();
                self.flag.store(true, Ordering::Relaxed);
            }
            States::CheckpointRoutingTableState(expected_accounts) => {
                wait_routing_table_state(
                    self.peer_managers.clone(),
                    self.counters.clone(),
                    self.flag.clone(),
                    self.accounts_id.clone(),
                    expected_accounts.clone(),
                );
            }
        }

        false
    }
}

fn check_routing_table(
    accounts_id: Vec<String>,
    adjacency_list: Vec<Vec<usize>>,
    peer_max_count: u32,
    max_wait_ms: u64,
    validator_mask: Vec<bool>,
) {
    init_test_logger();

    System::run(move || {
        let total_nodes = accounts_id.len();

        let ports: Vec<_> = (0..total_nodes).map(|_| open_port()).collect();
        let genesis_time = Utc::now();

        let boot_nodes = adjacency_list
            .into_iter()
            .map(|adj| adj.into_iter().map(|u| (accounts_id[u].clone(), ports[u])).collect());

        let validators: Vec<_> = accounts_id
            .iter()
            .zip(validator_mask.iter())
            .filter_map(|(acc_id, is_val)| if *is_val { Some(acc_id.clone()) } else { None })
            .collect();

        // Peer managers with its counters
        let peer_managers: Vec<_> = accounts_id
            .iter()
            .zip(boot_nodes)
            .enumerate()
            .map(|(ix, (account_id, boot_nodes))| {
                setup_network_node(
                    account_id.clone(),
                    ports[ix],
                    boot_nodes,
                    validators.clone(),
                    genesis_time,
                    peer_max_count,
                )
            })
            .collect();

        let mut state_machine = StateMachine::new(peer_managers, accounts_id.clone());

        state_machine.add_checkpoint(validators.clone());
        // Dummy closure
        state_machine.add_action(|| {});

        //        let mut accounts_without_1 = accounts_id.clone();
        //        accounts_without_1.remove(1);
        //        state_machine.add_checkpoint(accounts_without_1);

        WaitOrTimeout::new(
            Box::new(move |_| {
                if state_machine.step() {
                    System::current().stop();
                }
            }),
            100,
            max_wait_ms,
        )
        .start();
    })
    .unwrap();
}

#[test]
fn three_nodes_two_validators() {
    check_routing_table(
        vec_ref_to_str(vec!["test0", "test1", "test2"]),
        vec![vec![], vec![0], vec![0]],
        10,
        3000,
        vec![true, true, false],
    );
}

#[test]
fn long_path() {
    let num_peers = 7;
    let max_peer_connections = 2;

    let accounts_id = (0..num_peers).map(|ix| format!("test{}", ix)).collect();
    let adjacency_list = (0..num_peers)
        .map(|ix| {
            let mut neigs = vec![];
            if ix > 0 {
                neigs.push(ix - 1);
            }
            if ix + 1 < num_peers {
                neigs.push(ix + 1);
            }
            neigs
        })
        .collect();

    let mut validator_mask = vec![false; num_peers];
    validator_mask[0] = true;
    validator_mask[num_peers - 1] = true;

    check_routing_table(accounts_id, adjacency_list, max_peer_connections, 3000, validator_mask);
}
