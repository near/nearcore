use crate::routing::routing_table_actor::start_routing_table_actor;
use crate::test_utils::{convert_boot_nodes, open_port};
use crate::types::{NetworkConfig, NetworkViewClientMessages, NetworkViewClientResponses};
use crate::{NetworkClientMessages, NetworkClientResponses, PeerManagerActor, RoutingTableActor};
use actix::actors::mocker::Mocker;
use actix::{Actor, Addr};
use near_primitives::block::GenesisId;
use near_primitives::borsh::maybestd::sync::atomic::Ordering;
use near_primitives::network::PeerId;
use near_store::test_utils::create_test_store;
use near_store::Store;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

/// Mock for `ClientActor`
type ClientMock = Mocker<NetworkClientMessages>;
/// Mock for `ViewClientActor`
type ViewClientMock = Mocker<NetworkViewClientMessages>;

// Start PeerManagerActor, and RoutingTableActor together and returns pairs of addresses
// for each of them.
pub fn make_peer_manager_routing_table_addr_pair(
) -> (Addr<PeerManagerActor>, Addr<RoutingTableActor>) {
    let seed = "test2";
    let port = open_port();

    let net_config = NetworkConfig::from_seed(seed, port);
    let store = create_test_store();
    let routing_table_addr =
        start_routing_table_actor(net_config.public_key.clone().into(), store.clone());
    let peer_manager_addr = make_peer_manager(
        store,
        net_config,
        vec![("test1", open_port())],
        10,
        routing_table_addr.clone(),
    )
    .0
    .start();
    (peer_manager_addr, routing_table_addr)
}

// Make peer manager for unit tests
//
// Returns:
//    PeerManagerActor
//    PeerId - PeerId associated with given actor
//    Arc<AtomicUsize> - shared pointer for counting the number of received
//                       `NetworkViewClientMessages::AnnounceAccount` messages
pub fn make_peer_manager(
    store: Arc<Store>,
    mut config: NetworkConfig,
    boot_nodes: Vec<(&str, u16)>,
    peer_max_count: u32,
    routing_table_addr: Addr<RoutingTableActor>,
) -> (PeerManagerActor, PeerId, Arc<AtomicUsize>) {
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
    let peer_id = config.public_key.clone().into();
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
