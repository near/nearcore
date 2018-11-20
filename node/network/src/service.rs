use error::Error;
use futures::{self, stream, Future, Stream};
use io::NetSyncIo;
use parking_lot::Mutex;
use primitives::traits::{Block, GenericResult};
use protocol::{self, Protocol, ProtocolConfig, Transaction};
use std::io;
use std::sync::Arc;
use std::time::Duration;
pub use substrate_network_libp2p::NetworkConfiguration;
use substrate_network_libp2p::{
    start_service, RegisteredProtocol, Service as NetworkService, ServiceEvent,
};
use tokio::timer::Interval;

const TICK_TIMEOUT: Duration = Duration::from_millis(1000);

#[allow(dead_code)]
pub struct Service<T> {
    pub network: Arc<Mutex<NetworkService>>,
    pub protocol: Arc<Protocol<T>>,
}

impl<T: Transaction> Service<T> {
    pub fn new<B: Block>(
        config: ProtocolConfig,
        net_config: NetworkConfiguration,
        tx_callback: fn(T) -> GenericResult,
    ) -> Result<Service<T>, Error> {
        let protocol = Arc::new(Protocol::new(config, tx_callback));
        let version = [protocol::CURRENT_VERSION as u8];
        let registered = RegisteredProtocol::new(config.protocol_id, &version);
        let service = match start_service(net_config, Some(registered)) {
            Ok(s) => Arc::new(Mutex::new(s)),
            Err(e) => return Err(e.into()),
        };
        Ok(Service {
            network: service,
            protocol,
        })
    }
}

pub fn generate_service_task<T: Transaction, B: Block>(
    network_service: Arc<Mutex<NetworkService>>,
    protocol: Arc<Protocol<T>>,
) -> impl Future<Item = (), Error = ()> {
    // Interval for performing maintenance on the protocol handler.
    let timer = Interval::new_interval(TICK_TIMEOUT)
        .for_each({
            let protocol = protocol.clone();
            let network_service = network_service.clone();
            move |_| {
                protocol.maintain_peers(&mut NetSyncIo::new(
                    &network_service,
                    protocol.config.protocol_id,
                ));
                Ok(())
            }
        }).then(|res| {
            match res {
                Ok(()) => (),
                Err(err) => error!("Error in the propagation timer: {:?}", err),
            };
            Ok(())
        });
    let network_service1 = network_service.clone();
    let network = stream::poll_fn(move || network_service1.lock().poll()).for_each(move |event| {
        let mut net_sync = NetSyncIo::new(&network_service, protocol.config.protocol_id);
        debug!(target: "sub-libp2p", "event: {:?}", event);
        match event {
            ServiceEvent::CustomMessage {
                node_index, data, ..
            } => {
                protocol.on_message::<B>(&mut net_sync, node_index, &data);
            }
            ServiceEvent::OpenedCustomProtocol { node_index, .. } => {
                protocol.on_peer_connected::<B>(&mut net_sync, node_index);
            }
            ServiceEvent::ClosedCustomProtocol { node_index, .. } => {
                protocol.on_peer_disconnected(node_index);
            }
            _ => {
                debug!("TODO");
                ()
            }
        };
        Ok(())
    });
    let futures: Vec<Box<Future<Item = (), Error = io::Error> + Send>> =
        vec![Box::new(timer), Box::new(network)];

    futures::select_all(futures)
        .and_then(move |_| {
            info!("Networking ended");
            Ok(())
        }).map_err(|(r, _, _)| r)
        .map_err(|e| {
            debug!(target: "sub-libp2p", "service error: {:?}", e);
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use primitives::types::SignedTransaction;
    use std::thread;
    use std::time;
    use test_utils::*;
    use MockBlock;

    #[test]
    fn test_send_message() {
        let services = create_test_services(2);
        let mut runtime = tokio::runtime::Runtime::new().unwrap();
        for service in services.iter() {
            let task = generate_service_task::<SignedTransaction, MockBlock>(
                service.network.clone(),
                service.protocol.clone(),
            );
            runtime.spawn(task);
        }
        thread::sleep(time::Duration::from_millis(1000));
        for service in services {
            for peer in service.protocol.sample_peers(1).unwrap() {
                let message = fake_tx_message();
                let mut net_sync =
                    NetSyncIo::new(&service.network, service.protocol.config.protocol_id);
                service.protocol.send_message(&mut net_sync, peer, &message);
            }
        }
        thread::sleep(time::Duration::from_millis(1000));
    }
}
