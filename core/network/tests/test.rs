extern crate network;
extern crate parking_lot;
extern crate primitives;
extern crate rand;
extern crate substrate_network_libp2p;

use network::protocol::{Protocol, ProtocolConfig, TransactionPool};
use network::service::start_thread;
use network::test_utils::*;
use network::transaction_pool::Pool;
use parking_lot::Mutex;
use primitives::types::SignedTransaction;
use rand::Rng;
use std::sync::Arc;
use std::thread;
use std::time;
use substrate_network_libp2p::{
    NetworkConfiguration, ProtocolId, RegisteredProtocol, Service as NetworkService,
};

struct Service {
    network: Arc<Mutex<NetworkService>>,
    protocol: Arc<Protocol<SignedTransaction>>,
    bg_thread: Option<thread::JoinHandle<()>>,
}

impl Service {
    fn new(
        config: ProtocolConfig,
        net_config: NetworkConfiguration,
        protocol_id: ProtocolId,
        tx_pool: Arc<Mutex<TransactionPool<SignedTransaction>>>,
    ) -> Arc<Service> {
        let version = [1 as u8];
        let registered = RegisteredProtocol::new(protocol_id, &version);
        let protocol = Arc::new(Protocol::new(config, tx_pool));
        let (thread, network) = start_thread(net_config, protocol.clone(), registered)
            .expect("start_thread should not fail");
        Arc::new(Service {
            network: network,
            protocol: protocol,
            bg_thread: Some(thread),
        })
    }
}

fn create_services(num_services: u32) -> Vec<Arc<Service>> {
    let base_address = "/ip4/127.0.0.1/tcp/".to_string();
    let base_port = rand::thread_rng().gen_range(30000, 60000);
    let mut addresses = Vec::new();
    for i in 0..num_services {
        let port = base_port + i;
        addresses.push(base_address.clone() + &port.to_string());
    }
    // spin up a root service that does not have bootnodes and
    // have other services have this service as their boot node
    // may want to abstract this out to enable different configurations
    let secret = create_secret();
    let root_config = test_config_with_secret(&addresses[0], vec![], secret);
    let tx_pool = Arc::new(Mutex::new(Pool::new()));
    let root_service = Service::new(
        ProtocolConfig::default(),
        root_config,
        ProtocolId::default(),
        tx_pool,
    );
    let boot_node = addresses[0].clone() + "/p2p/" + &raw_key_to_peer_id_str(secret);
    let mut services = vec![root_service];
    for i in 1..num_services {
        let config = test_config(&addresses[i as usize], vec![boot_node.clone()]);
        let tx_pool = Arc::new(Mutex::new(Pool::new()));
        let service = Service::new(
            ProtocolConfig::default(),
            config,
            ProtocolId::default(),
            tx_pool,
        );
        services.push(service);
    }
    services
}

#[test]
fn test_send_message() {
    init_logger();
    let services = create_services(2) as Vec<Arc<Service>>;
    thread::sleep(time::Duration::from_secs(1));
    for service in services {
        for peer in service.protocol.sample_peers(1) {
            let message = fake_tx_message();
            service
                .protocol
                .send_message(&service.network, peer, message);
        }
    }
    thread::sleep(time::Duration::from_secs(1));
}

#[test]
fn test_tx_pool() {
    init_logger();
    let services = create_services(2) as Vec<Arc<Service>>;
    thread::sleep(time::Duration::from_secs(1));
    for service in services.clone() {
        for peer in service.protocol.sample_peers(1) {
            let message = fake_tx_message();
            service
                .protocol
                .send_message(&service.network, peer, message);
        }
    }
    thread::sleep(time::Duration::from_secs(1));
    for service in services {
        let mut tx_pool = service.protocol.tx_pool.lock();
        let txs = tx_pool.get();
        assert_eq!(txs.len(), 1);
    }
}
