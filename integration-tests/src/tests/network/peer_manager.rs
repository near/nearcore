use actix::Actor;
use actix::{Handler, SyncContext};
use std::sync::Arc;

use near_network::types::{NetworkClientMessages, NetworkClientResponses};
use near_network_primitives::types::{NetworkViewClientMessages, NetworkViewClientResponses};

#[derive(Default)]
struct MockClientActor {
    on_view_client_callback:
        Option<Arc<dyn Fn(&NetworkViewClientMessages) -> NetworkViewClientResponses + Send + Sync>>,
}

impl Actor for MockClientActor {
    type Context = SyncContext<Self>;
}

impl Handler<NetworkViewClientMessages> for MockClientActor {
    type Result = NetworkViewClientResponses;

    fn handle(&mut self, msg: NetworkViewClientMessages, _ctx: &mut Self::Context) -> Self::Result {
        match &self.on_view_client_callback {
            Some(callback) => callback.as_ref()(&msg),
            None => match msg {
                NetworkViewClientMessages::GetChainInfo => NetworkViewClientResponses::ChainInfo {
                    genesis_id: Default::default(),
                    height: 1,
                    tracked_shards: vec![],
                    archival: false,
                },
                _ => NetworkViewClientResponses::NoResponse,
            },
        }
    }
}

impl Handler<NetworkClientMessages> for MockClientActor {
    type Result = NetworkClientResponses;
    fn handle(&mut self, _msg: NetworkClientMessages, _ctx: &mut Self::Context) -> Self::Result {
        return NetworkClientResponses::NoResponse;
    }
}

// This test is trying to see how PeerManager reacts to repeated AnnounceAccounts requests.
// We want to make sure that we broadcast them only once.
#[cfg(feature = "test_features")]
#[test]
fn repeated_announce_accounts() {
    use actix::System;
    use actix::{Addr, SyncArbiter};
    use futures::{future, FutureExt};
    use near_actix_test_utils::run_actix;
    use near_crypto::Signature;
    use near_logger_utils::init_test_logger;
    use near_network::routing::start_routing_table_actor;
    use near_network::test_utils::GetBroadcastMessageCount;
    use near_network::test_utils::{open_port, WaitOrTimeoutActor};
    use near_network::types::{NetworkRequests, PeerManagerMessageRequest, RoutingTableUpdate};
    use near_network::PeerManagerActor;
    use near_network_primitives::types::NetworkConfig;
    use near_primitives::network::AnnounceAccount;
    use near_primitives::network::PeerId;
    use near_primitives::types::AccountId;
    use near_primitives::types::EpochId;
    use near_store::test_utils::create_test_store;
    use std::str::FromStr;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Barrier;

    fn start_mock_client(
        on_view_client_callback: Option<
            Arc<dyn Fn(&NetworkViewClientMessages) -> NetworkViewClientResponses + Send + Sync>,
        >,
    ) -> Addr<MockClientActor> {
        SyncArbiter::start(5, move || MockClientActor {
            on_view_client_callback: on_view_client_callback.clone(),
        })
    }

    run_actix(async {
        init_test_logger();
        let store = create_test_store();
        let config = NetworkConfig::from_seed("test1", open_port());
        let barrier = Arc::new(Barrier::new(3));
        let mock_client_addr = start_mock_client(Some(Arc::new(move |msg| {
            barrier.wait();
            match msg {
                NetworkViewClientMessages::AnnounceAccount(accounts) => {
                    NetworkViewClientResponses::AnnounceAccount(
                        accounts.into_iter().map(|it| it.0.clone()).collect(),
                    )
                }
                NetworkViewClientMessages::GetChainInfo => NetworkViewClientResponses::ChainInfo {
                    genesis_id: Default::default(),
                    height: 1,
                    tracked_shards: vec![],
                    archival: false,
                },
                _ => NetworkViewClientResponses::NoResponse,
            }
        })));
        let routing_table_addr = start_routing_table_actor(config.node_id(), store.clone());
        let pm = PeerManagerActor::new(
            store,
            config,
            mock_client_addr.clone().recipient(),
            mock_client_addr.recipient(),
            routing_table_addr,
        )
        .unwrap()
        .start();

        // Create a random announce account and try sending it 10 times.
        let peer_id = PeerId::random();
        let announce_account = AnnounceAccount {
            account_id: AccountId::from_str("abc").unwrap(),
            peer_id: peer_id.clone(),
            epoch_id: EpochId::default(),
            signature: Signature::default(),
        };
        let routing_table_update = RoutingTableUpdate::from_accounts(vec![announce_account]);
        for _ in 0..10 {
            pm.do_send(PeerManagerMessageRequest::NetworkRequests(
                NetworkRequests::SyncRoutingTable {
                    peer_id: peer_id.clone(),
                    routing_table_update: routing_table_update.clone(),
                },
            ));
        }

        let successes = Arc::new(AtomicUsize::new(0));
        WaitOrTimeoutActor::new(
            Box::new(move |_| {
                actix::spawn({
                    let foo = successes.clone();
                    pm.send(GetBroadcastMessageCount { msg_type: "SyncRoutingTable" }).then(
                        move |res| {
                            let sync_routing_table_sent = res.unwrap();
                            if sync_routing_table_sent == 1 {
                                // Try it couple times - we want to be sure that it didn't hit in the middle of processing.
                                if foo.fetch_add(1, Ordering::SeqCst) > 4 {
                                    System::current().stop();
                                }
                            }
                            assert!(
                                sync_routing_table_sent <= 1,
                                "Too many sync routing table requests"
                            );
                            future::ready(())
                        },
                    )
                });
            }),
            100,
            5000,
        )
        .start();
    });
}
