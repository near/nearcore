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
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

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
        validators.into_iter().map(Into::into).collect(),
    ));
    let signer = Arc::new(InMemorySigner::from_seed(account_id, account_id));
    let block_producer = BlockProducer::from(signer.clone());

    let peer_manager = PeerManagerActor::create(move |ctx| {
        let client_actor = ClientActor::new(
            ClientConfig::test(false),
            store.clone(),
            genesis_time,
            runtime,
            ctx.address().recipient(),
            Some(block_producer),
            config.public_key.clone().into(),
        )
        .unwrap()
        .start();

        PeerManagerActor::new(store.clone(), config, client_actor.recipient()).unwrap()
    });

    peer_manager
}

#[test]
fn two_nodes() {
    init_test_logger();

    System::run(|| {
        let test1 = "test1";
        let test2 = "test2";
        let validators = vec![test1, test2];
        let (port1, port2) = (open_port(), open_port());
        let genesis_time = Utc::now();

        let pm1 = setup_network_node(
            test1,
            port1,
            vec![(test2, port2)],
            validators.clone(),
            genesis_time,
        );
        let pm2 = setup_network_node(
            test2,
            port2,
            vec![(test1, port1)],
            validators.clone(),
            genesis_time,
        );

        let count1 = Arc::new(AtomicUsize::new(0));
        let count2 = Arc::new(AtomicUsize::new(0));

        WaitOrTimeout::new(
            Box::new(move |_| {
                for (pm, count) in vec![(&pm1, count1.clone()), (&pm2, count2.clone())].into_iter()
                {
                    let count1_c = count1.clone();
                    let count2_c = count2.clone();
                    actix::spawn(pm.send(NetworkRequests::FetchInfo { level: 1 }).then(
                        move |res| {
                            if let NetworkResponses::Info(NetworkInfo { routes, .. }) = res.unwrap()
                            {
                                if routes.unwrap().len() == 2 {
                                    count.fetch_add(1, Ordering::Relaxed);

                                    if count1_c.load(Ordering::Relaxed) >= 1
                                        && count2_c.load(Ordering::Relaxed) >= 1
                                    {
                                        System::current().stop();
                                    }
                                }
                            }
                            future::result(Ok(()))
                        },
                    ));
                }
            }),
            100,
            2000,
        )
        .start();
    })
    .unwrap();
}
