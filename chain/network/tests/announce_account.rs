use actix::{Actor, Addr, AsyncContext, System};
use chrono::{DateTime, Utc};
use futures::{future, Future};
use near_chain::test_utils::KeyValueRuntime;
use near_client::{BlockProducer, ClientActor, ClientConfig};
use near_network::test_utils::{convert_boot_nodes, open_port, WaitOrTimeout};
use near_network::types::NetworkInfo;
use near_network::{NetworkConfig, NetworkRequests, NetworkResponses, PeerManagerActor};
use near_primitives::crypto::signer::InMemorySigner;
use near_primitives::test_utils::init_test_logger;
use near_store::test_utils::create_test_store;
use near_telemetry::{TelemetryActor, TelemetryConfig};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use near_chain::ChainGenesis;

/// Sets up a node with a valid Client, Peer
pub fn setup_network_node(
    account_id: &'static str,
    port: u16,
    boot_nodes: Vec<(&str, u16)>,
    validators: Vec<&'static str>,
    genesis_time: DateTime<Utc>,
) -> Addr<PeerManagerActor> {
    let store = create_test_store();
    let mut config = NetworkConfig::from_seed(account_id, port);
    config.boot_nodes = convert_boot_nodes(boot_nodes);

    let runtime = Arc::new(KeyValueRuntime::new_with_validators(
        store.clone(),
        vec![validators.into_iter().map(Into::into).collect()],
        1,
    ));
    let signer = Arc::new(InMemorySigner::from_seed(account_id, account_id));
    let block_producer = BlockProducer::from(signer.clone());
    let telemetry_actor = TelemetryActor::new(TelemetryConfig::default()).start();
    let chain_genesis = ChainGenesis::new(genesis_time, 1_000_000);

    let peer_manager = PeerManagerActor::create(move |ctx| {
        let client_actor = ClientActor::new(
            ClientConfig::test(false, 100),
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

/// Check that Accounts Id are propagated properly through the network, even when all peers are not
/// connected to each other. Though it is necessary that the network is connected.
fn check_account_id_propagation(
    accounts_id: Vec<&'static str>,
    adjacency_list: Vec<Vec<usize>>,
    max_wait_ms: u64,
) {
    init_test_logger();

    System::run(move || {
        let total_nodes = accounts_id.len();

        let ports: Vec<_> = (0..total_nodes).map(|_| open_port()).collect();
        let genesis_time = Utc::now();

        let boot_nodes = adjacency_list
            .into_iter()
            .map(|adj| adj.into_iter().map(|u| (accounts_id[u].clone(), ports[u])).collect());

        // Peer managers with its counters
        let peer_managers: Vec<_> = accounts_id
            .iter()
            .zip(boot_nodes)
            .enumerate()
            .map(|(ix, (account_id, boot_nodes))| {
                (
                    setup_network_node(
                        account_id,
                        ports[ix],
                        boot_nodes,
                        accounts_id.clone(),
                        genesis_time,
                    ),
                    Arc::new(AtomicUsize::new(0)),
                )
            })
            .collect();

        WaitOrTimeout::new(
            Box::new(move |_| {
                for (pm, count) in peer_managers.iter() {
                    let pm = pm.clone();
                    let count = count.clone();

                    let counters: Vec<_> =
                        peer_managers.iter().map(|(_, counter)| counter.clone()).collect();

                    if count.load(Ordering::Relaxed) == 0 {
                        actix::spawn(pm.send(NetworkRequests::FetchInfo { level: 1 }).then(
                            move |res| {
                                if let NetworkResponses::Info(NetworkInfo { routes, .. }) =
                                    res.unwrap()
                                {
                                    if routes.unwrap().len() == total_nodes {
                                        count.fetch_add(1, Ordering::Relaxed);

                                        if counters
                                            .iter()
                                            .all(|counter| counter.load(Ordering::Relaxed) == 1)
                                        {
                                            System::current().stop();
                                        }
                                    }
                                }
                                future::result(Ok(()))
                            },
                        ));
                    }
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
fn two_nodes() {
    check_account_id_propagation(vec!["test1", "test2"], vec![vec![1], vec![0]], 2000);
}

#[test]
fn three_nodes_clique() {
    check_account_id_propagation(
        vec!["test1", "test2", "test3"],
        vec![vec![1, 2], vec![0, 2], vec![0, 1]],
        2000,
    );
}

#[test]
fn three_nodes_path() {
    check_account_id_propagation(
        vec!["test1", "test2", "test3"],
        vec![vec![1], vec![0, 2], vec![1]],
        2000,
    );
}

#[test]
fn four_nodes_star() {
    check_account_id_propagation(
        vec!["test1", "test2", "test3", "test4"],
        vec![vec![1, 2, 3], vec![0], vec![0], vec![0]],
        2000,
    );
}

#[test]
fn four_nodes_path() {
    check_account_id_propagation(
        vec!["test1", "test2", "test3", "test4"],
        vec![vec![1], vec![0, 2], vec![1, 3], vec![2]],
        2000,
    );
}

#[test]
#[should_panic]
fn four_nodes_disconnected() {
    check_account_id_propagation(
        vec!["test1", "test2", "test3", "test4"],
        vec![vec![1], vec![0], vec![3], vec![2]],
        2000,
    );
}

#[test]
fn four_nodes_directed() {
    check_account_id_propagation(
        vec!["test1", "test2", "test3", "test4"],
        vec![vec![1], vec![], vec![1], vec![2]],
        2000,
    );
}
