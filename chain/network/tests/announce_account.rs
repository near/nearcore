use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use actix::actors::mocker::Mocker;
use actix::{Actor, Addr, AsyncContext, System};
use chrono::{DateTime, Utc};
use futures::{future, Future};

use near_chain::test_utils::KeyValueRuntime;
use near_chain::ChainGenesis;
use near_client::{BlockProducer, ClientActor, ClientConfig};
use near_crypto::{InMemorySigner, KeyType};
use near_network::test_utils::{convert_boot_nodes, open_port, vec_ref_to_str, WaitOrTimeout};
use near_network::types::{AnnounceAccount, NetworkInfo, PeerId, SyncData};
use near_network::{
    NetworkClientMessages, NetworkClientResponses, NetworkConfig, NetworkRequests,
    NetworkResponses, PeerManagerActor,
};
use near_primitives::block::{GenesisId, WeightAndScore};
use near_primitives::hash::hash;
use near_primitives::test_utils::init_integration_logger;
use near_primitives::types::EpochId;
use near_store::test_utils::create_test_store;
use near_telemetry::{TelemetryActor, TelemetryConfig};
use testlib::test_helpers::heavy_test;

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
    let mut config = NetworkConfig::from_seed(account_id.as_str(), port);
    config.peer_max_count = peer_max_count;

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
        let client_actor = ClientActor::new(
            ClientConfig::test(false, 100, 200, num_validators),
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

/// Check that Accounts Id are propagated properly through the network, even when all peers aren't
/// directly connected to each other. Though it is necessary that the network is connected.
fn check_account_id_propagation(
    accounts_id: Vec<String>,
    adjacency_list: Vec<Vec<usize>>,
    peer_max_count: u32,
    max_wait_ms: u64,
) {
    init_integration_logger();

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
                        account_id.clone(),
                        ports[ix],
                        boot_nodes,
                        accounts_id.clone(),
                        genesis_time,
                        peer_max_count,
                    ),
                    Arc::new(AtomicUsize::new(0)),
                )
            })
            .collect();

        WaitOrTimeout::new(
            Box::new(move |_| {
                for (_, (pm, count)) in peer_managers.iter().enumerate() {
                    let pm = pm.clone();
                    let count = count.clone();

                    let counters: Vec<_> =
                        peer_managers.iter().map(|(_, counter)| counter.clone()).collect();

                    if count.load(Ordering::Relaxed) == 0 {
                        actix::spawn(pm.send(NetworkRequests::FetchInfo).then(move |res| {
                            if let NetworkResponses::Info(NetworkInfo { known_producers, .. }) =
                                res.unwrap()
                            {
                                if known_producers.len() == total_nodes {
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
                        }));
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

/// Make Peer Manager with mocked client ready to accept any announce account.
/// Used for `test_infinite_loop`
pub fn make_peer_manager(
    seed: &str,
    port: u16,
    boot_nodes: Vec<(&str, u16)>,
    peer_max_count: u32,
) -> (PeerManagerActor, PeerId, Arc<AtomicUsize>) {
    let store = create_test_store();
    let mut config = NetworkConfig::from_seed(seed, port);
    config.boot_nodes = convert_boot_nodes(boot_nodes);
    config.peer_max_count = peer_max_count;
    let counter = Arc::new(AtomicUsize::new(0));
    let counter1 = counter.clone();
    let client_addr = ClientMock::mock(Box::new(move |msg, _ctx| {
        let msg = msg.downcast_ref::<NetworkClientMessages>().unwrap();
        match msg {
            NetworkClientMessages::AnnounceAccount(accounts) => {
                if !accounts.is_empty() {
                    counter1.fetch_add(1, Ordering::SeqCst);
                }
                Box::new(Some(NetworkClientResponses::AnnounceAccount(
                    accounts.clone().into_iter().map(|obj| obj.0).collect(),
                )))
            }
            NetworkClientMessages::GetChainInfo => {
                Box::new(Some(NetworkClientResponses::ChainInfo {
                    genesis_id: GenesisId::default(),
                    height: 1,
                    weight_and_score: WeightAndScore::from_ints(0, 0),
                    tracked_shards: vec![],
                }))
            }
            _ => Box::new(Some(NetworkClientResponses::NoResponse)),
        }
    }))
    .start();
    let peer_id = config.public_key.clone().into();
    (PeerManagerActor::new(store, config, client_addr.recipient()).unwrap(), peer_id, counter)
}

#[test]
fn two_nodes() {
    heavy_test(|| {
        check_account_id_propagation(
            vec_ref_to_str(vec!["test1", "test2"]),
            vec![vec![1], vec![0]],
            10,
            5000,
        );
    });
}

#[test]
fn three_nodes_clique() {
    heavy_test(|| {
        check_account_id_propagation(
            vec_ref_to_str(vec!["test1", "test2", "test3"]),
            vec![vec![1, 2], vec![0, 2], vec![0, 1]],
            10,
            5000,
        );
    });
}

#[test]
fn three_nodes_path() {
    heavy_test(|| {
        check_account_id_propagation(
            vec_ref_to_str(vec!["test1", "test2", "test3"]),
            vec![vec![1], vec![0, 2], vec![1]],
            10,
            5000,
        );
    });
}

#[test]
fn four_nodes_star() {
    heavy_test(|| {
        check_account_id_propagation(
            vec_ref_to_str(vec!["test1", "test2", "test3", "test4"]),
            vec![vec![1, 2, 3], vec![0], vec![0], vec![0]],
            10,
            5000,
        );
    });
}

#[test]
fn four_nodes_path() {
    heavy_test(|| {
        check_account_id_propagation(
            vec_ref_to_str(vec!["test1", "test2", "test3", "test4"]),
            vec![vec![1], vec![0, 2], vec![1, 3], vec![2]],
            10,
            5000,
        );
    });
}

#[test]
#[should_panic]
fn four_nodes_disconnected() {
    heavy_test(|| {
        check_account_id_propagation(
            vec_ref_to_str(vec!["test1", "test2", "test3", "test4"]),
            vec![vec![1], vec![0], vec![3], vec![2]],
            10,
            5000,
        );
    });
}

#[test]
fn four_nodes_directed() {
    heavy_test(|| {
        check_account_id_propagation(
            vec_ref_to_str(vec!["test1", "test2", "test3", "test4"]),
            vec![vec![1], vec![], vec![1], vec![2]],
            10,
            5000,
        );
    });
}

#[test]
fn circle() {
    heavy_test(|| {
        let num_peers = 7;
        let max_peer_connections = 2;

        let accounts_id = (0..num_peers).map(|ix| format!("test{}", ix)).collect();
        let adjacency_list = (0..num_peers)
            .map(|ix| vec![(ix + num_peers - 1) % num_peers, (ix + 1) % num_peers])
            .collect();

        check_account_id_propagation(accounts_id, adjacency_list, max_peer_connections, 5000);
    });
}

#[test]
fn star_2_connections() {
    heavy_test(|| {
        let num_peers = 7;
        let max_peer_connections = 2;

        let accounts_id = (0..num_peers).map(|ix| format!("test{}", ix)).collect();
        let adjacency_list = (0..num_peers)
            .map(|ix| {
                let size = if ix > 0 { 1 } else { 0 };
                vec![0; size]
            })
            .collect();

        check_account_id_propagation(accounts_id, adjacency_list, max_peer_connections, 5000);
    });
}

#[test]
fn circle_extra_connection() {
    heavy_test(|| {
        let num_peers = 7;
        let max_peer_connections = 3;

        let accounts_id = (0..num_peers).map(|ix| format!("test{}", ix)).collect();
        let adjacency_list = (0..num_peers)
            .map(|ix| vec![(ix + num_peers - 1) % num_peers, (ix + 1) % num_peers])
            .collect();

        check_account_id_propagation(accounts_id, adjacency_list, max_peer_connections, 5000);
    });
}

type ClientMock = Mocker<ClientActor>;

#[test]
fn test_infinite_loop() {
    init_integration_logger();
    System::run(|| {
        let (port1, port2) = (open_port(), open_port());
        let (pm1, peer_id1, counter1) = make_peer_manager("test1", port1, vec![], 10);
        let (pm2, peer_id2, counter2) =
            make_peer_manager("test2", port2, vec![("test1", port1)], 10);
        let pm1 = pm1.start();
        let pm2 = pm2.start();
        let request1 = NetworkRequests::Sync {
            peer_id: peer_id1.clone(),
            sync_data: SyncData::account(AnnounceAccount {
                account_id: "near".to_string(),
                peer_id: peer_id1.clone(),
                epoch_id: Default::default(),
                signature: Default::default(),
            }),
        };
        let request2 = NetworkRequests::Sync {
            peer_id: peer_id1.clone(),
            sync_data: SyncData::account(AnnounceAccount {
                account_id: "near".to_string(),
                peer_id: peer_id2.clone(),
                epoch_id: Default::default(),
                signature: Default::default(),
            }),
        };

        let state = Arc::new(AtomicUsize::new(0));
        let start = Instant::now();

        WaitOrTimeout::new(
            Box::new(move |_| {
                let state_value = state.load(Ordering::SeqCst);

                let state1 = state.clone();
                if state_value == 0 {
                    actix::spawn(pm2.clone().send(NetworkRequests::FetchInfo).then(move |res| {
                        if let Ok(NetworkResponses::Info(info)) = res {
                            if !info.active_peers.is_empty() {
                                state1.store(1, Ordering::SeqCst);
                            }
                        }
                        future::ok(())
                    }));
                } else if state_value == 1 {
                    actix::spawn(pm1.clone().send(request1.clone()).then(move |res| {
                        assert!(res.is_ok());
                        state1.store(2, Ordering::SeqCst);
                        future::ok(())
                    }));
                } else if state_value == 2 {
                    if counter1.load(Ordering::SeqCst) == 1 && counter2.load(Ordering::SeqCst) == 1
                    {
                        state.store(3, Ordering::SeqCst);
                    }
                } else if state_value == 3 {
                    actix::spawn(pm1.clone().send(request1.clone()).then(move |res| {
                        assert!(res.is_ok());
                        future::ok(())
                    }));
                    actix::spawn(pm2.clone().send(request2.clone()).then(move |res| {
                        assert!(res.is_ok());
                        future::ok(())
                    }));
                    state.store(4, Ordering::SeqCst);
                } else if state_value == 4 {
                    assert_eq!(counter1.load(Ordering::SeqCst), 1);
                    assert_eq!(counter2.load(Ordering::SeqCst), 1);
                    if Instant::now().duration_since(start).as_millis() > 800 {
                        System::current().stop();
                    }
                }
            }),
            100,
            10000,
        )
        .start();
    })
    .unwrap();
}
