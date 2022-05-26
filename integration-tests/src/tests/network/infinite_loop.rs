use near_primitives::time::Instant;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use actix::actors::mocker::Mocker;
use actix::{Actor, System};
use futures::{future, FutureExt};

use near_actix_test_utils::run_actix;
use near_client::ClientActor;
use near_logger_utils::init_integration_logger;

use near_network::routing::start_routing_table_actor;
use near_network::test_utils::{convert_boot_nodes, open_port, GetInfo, WaitOrTimeoutActor};
use near_network::types::{
    NetworkClientResponses, NetworkRequests, PeerManagerMessageRequest, RoutingTableUpdate,
};
use near_network::PeerManagerActor;
use near_network_primitives::types::{
    NetworkConfig, NetworkViewClientMessages, NetworkViewClientResponses,
};
use near_primitives::block::GenesisId;
use near_primitives::network::{AnnounceAccount, PeerId};
use near_store::test_utils::create_test_store;

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
    config.max_num_peers = peer_max_count;
    let counter = Arc::new(AtomicUsize::new(0));
    let counter1 = counter.clone();
    let client_addr = ClientMock::mock(Box::new(move |_msg, _ctx| {
        Box::new(Some(NetworkClientResponses::NoResponse))
    }))
    .start();
    let view_client_addr = ViewClientMock::mock(Box::new(move |msg, _ctx| {
        let msg = msg.downcast_ref::<NetworkViewClientMessages>().unwrap();
        match msg {
            NetworkViewClientMessages::AnnounceAccount(accounts) => {
                if !accounts.is_empty() {
                    counter1.fetch_add(1, Ordering::SeqCst);
                }
                Box::new(Some(NetworkViewClientResponses::AnnounceAccount(
                    accounts.clone().into_iter().map(|obj| obj.0).collect(),
                )))
            }
            NetworkViewClientMessages::GetChainInfo => {
                Box::new(Some(NetworkViewClientResponses::ChainInfo {
                    genesis_id: GenesisId::default(),
                    height: 1,
                    tracked_shards: vec![],
                    archival: false,
                }))
            }
            _ => Box::new(Some(NetworkViewClientResponses::NoResponse)),
        }
    }))
    .start();

    let net_config = NetworkConfig::from_seed(seed, port);
    let routing_table_addr =
        start_routing_table_actor(net_config.node_id(), store.clone());

    let peer_id = net_config.node_id();
    (
        PeerManagerActor::new(
            store,
            config,
            client_addr.recipient(),
            view_client_addr.recipient(),
            routing_table_addr,
        )
        .unwrap(),
        peer_id,
        counter,
    )
}

type ClientMock = Mocker<ClientActor>;
type ViewClientMock = Mocker<ClientActor>;

#[test]
fn test_infinite_loop() {
    init_integration_logger();
    run_actix(async {
        let (port1, port2) = (open_port(), open_port());
        let (pm1, peer_id1, counter1) = make_peer_manager("test1", port1, vec![], 10);
        let (pm2, peer_id2, counter2) =
            make_peer_manager("test2", port2, vec![("test1", port1)], 10);
        let pm1 = pm1.start();
        let pm2 = pm2.start();
        let request1 = NetworkRequests::SyncRoutingTable {
            peer_id: peer_id1.clone(),
            routing_table_update: RoutingTableUpdate::from_accounts(vec![AnnounceAccount {
                account_id: "near".parse().unwrap(),
                peer_id: peer_id1.clone(),
                epoch_id: Default::default(),
                signature: Default::default(),
            }]),
        };
        let request2 = NetworkRequests::SyncRoutingTable {
            peer_id: peer_id1,
            routing_table_update: RoutingTableUpdate::from_accounts(vec![AnnounceAccount {
                account_id: "near".parse().unwrap(),
                peer_id: peer_id2,
                epoch_id: Default::default(),
                signature: Default::default(),
            }]),
        };

        let state = Arc::new(AtomicUsize::new(0));
        let start = Instant::now();

        WaitOrTimeoutActor::new(
            Box::new(move |_| {
                let state_value = state.load(Ordering::SeqCst);

                let state1 = state.clone();
                if state_value == 0 {
                    actix::spawn(pm2.clone().send(GetInfo {}).then(move |res| {
                        if let Ok(info) = res {
                            if !info.connected_peers.is_empty() {
                                state1.store(1, Ordering::SeqCst);
                            }
                        }
                        future::ready(())
                    }));
                } else if state_value == 1 {
                    actix::spawn(
                        pm1.clone()
                            .send(PeerManagerMessageRequest::NetworkRequests(request1.clone()))
                            .then(move |res| {
                                assert!(res.is_ok());
                                state1.store(2, Ordering::SeqCst);
                                future::ready(())
                            }),
                    );
                } else if state_value == 2 {
                    if counter1.load(Ordering::SeqCst) == 1 && counter2.load(Ordering::SeqCst) == 1
                    {
                        state.store(3, Ordering::SeqCst);
                    }
                } else if state_value == 3 {
                    actix::spawn(
                        pm1.clone()
                            .send(PeerManagerMessageRequest::NetworkRequests(request1.clone()))
                            .then(move |res| {
                                assert!(res.is_ok());
                                future::ready(())
                            }),
                    );
                    actix::spawn(
                        pm2.clone()
                            .send(PeerManagerMessageRequest::NetworkRequests(request2.clone()))
                            .then(move |res| {
                                assert!(res.is_ok());
                                future::ready(())
                            }),
                    );
                    state.store(4, Ordering::SeqCst);
                } else if state_value == 4 {
                    assert_eq!(counter1.load(Ordering::SeqCst), 1);
                    assert_eq!(counter2.load(Ordering::SeqCst), 1);
                    if Instant::now().saturating_duration_since(start).as_millis() > 800 {
                        System::current().stop();
                    }
                }
            }),
            100,
            10000,
        )
        .start();
    });
}
