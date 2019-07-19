use actix::{Actor, Addr, AsyncContext, System};
use chrono::Utc;
use near_chain::test_utils::KeyValueRuntime;
use near_client::{BlockProducer, ClientActor, ClientConfig, ViewClientActor};
use near_network::test_utils::{convert_boot_nodes, open_port};
use near_network::{NetworkConfig, PeerManagerActor};
use near_primitives::crypto::signer::InMemorySigner;
use near_primitives::test_utils::init_test_logger;
use near_store::test_utils::create_test_store;
use std::sync::Arc;

/// Sets up ClientActor and ViewClientActor with PeerManager.
pub fn setup_network_node(
    account_id: &'static str,
    port: u16,
    boot_nodes: Vec<(&str, u16)>,
    validators: Vec<&'static str>,
) -> (Addr<ClientActor>, Addr<ViewClientActor>) {
    let store = create_test_store();
    let mut config = NetworkConfig::from_seed(account_id, port);
    config.boot_nodes = convert_boot_nodes(boot_nodes);

    let runtime = Arc::new(KeyValueRuntime::new_with_validators(
        store.clone(),
        validators.into_iter().map(Into::into).collect(),
    ));
    let genesis_time = Utc::now();
    let signer = Arc::new(InMemorySigner::from_seed(account_id, account_id));
    let block_producer = BlockProducer::from(signer.clone());

    let view_client =
        ViewClientActor::new(store.clone(), genesis_time.clone(), runtime.clone()).unwrap().start();

    let client = ClientActor::create(move |ctx| {
        let peer_id = config.public_key.clone().into();

        let network_actor = PeerManagerActor::new(store.clone(), config, ctx.address().recipient())
            .unwrap()
            .start();

        ClientActor::new(
            ClientConfig::test(false),
            store.clone(),
            genesis_time,
            runtime,
            network_actor.recipient(),
            Some(block_producer),
            peer_id,
        )
        .unwrap()
    });

    (client, view_client)
}

#[test]
fn two_nodes() {
    init_test_logger();
    //    let x = setup_with_peer_manager(vec!["test1", "test2"], "test1");

    System::run(|| {
        let (port1, port2) = (open_port(), open_port());
        System::current().stop();

        //        let pm1 = make_peer_manager("test1", port1, vec![("test2", port2)]).start();
        //        let _pm2 = make_peer_manager("test2", port2, vec![("test1", port1)]).start();
        //
        //        WaitOrTimeout::new(
        //            Box::new(move |_| {
        //                //                actix::spawn(pm1.send(NetworkRequests::FetchInfo).then(move |res| {
        //                //                    if let NetworkResponses::Info { num_active_peers, .. } = res.unwrap() {
        //                //                        if num_active_peers == 1 {
        //                //                            System::current().stop();
        //                //                        }
        //                //                    }
        //                //                    future::result(Ok(()))
        //                //                }));
        //            }),
        //            100,
        //            2000,
        //        )
        //        .start();
    })
    .unwrap();
}
