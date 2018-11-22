use client::Client;
use error::Error;
use futures::{self, stream, Future, Stream};
use io::NetSyncIo;
use parking_lot::Mutex;
use primitives::traits::Block;
use protocol::ProtocolHandler;
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
const BLOCK_PROD_TIMEOUT: Duration = Duration::from_secs(2);

pub struct Service<B: Block, T: Transaction, H: ProtocolHandler<T>> {
    pub network: Arc<Mutex<NetworkService>>,
    pub protocol: Arc<Protocol<B, T, H>>,
}

impl<B: Block, T: Transaction, H: ProtocolHandler<T>> Service<B, T, H> {
    pub fn new(
        config: ProtocolConfig,
        net_config: NetworkConfiguration,
        handler: H,
        client: Arc<Client<B>>,
    ) -> Result<Service<B, T, H>, Error> {
        let version = [protocol::CURRENT_VERSION as u8];
        let registered = RegisteredProtocol::new(config.protocol_id, &version);
        let protocol = Arc::new(Protocol::new(config, handler, client));
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

pub fn generate_service_task<B, T, H>(
    network_service: Arc<Mutex<NetworkService>>,
    protocol: Arc<Protocol<B, T, H>>,
) -> impl Future<Item = (), Error = ()>
where
    B: Block,
    T: Transaction,
    H: ProtocolHandler<T>,
{
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

    // events handler
    let network_service1 = network_service.clone();
    let network = stream::poll_fn(move || network_service1.lock().poll()).for_each({
        let network_service = network_service.clone();
        let protocol = protocol.clone();
        move |event| {
            let mut net_sync = NetSyncIo::new(&network_service, protocol.config.protocol_id);
            debug!(target: "sub-libp2p", "event: {:?}", event);
            match event {
                ServiceEvent::CustomMessage {
                    node_index, data, ..
                } => {
                    protocol.on_message(&mut net_sync, node_index, &data);
                }
                ServiceEvent::OpenedCustomProtocol { node_index, .. } => {
                    protocol.on_peer_connected(&mut net_sync, node_index);
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
        }
    });

    // block producing logic (fake for now)
    let block_production = Interval::new_interval(BLOCK_PROD_TIMEOUT)
        .for_each({
            move |_| {
                let mut net_sync = NetSyncIo::new(&network_service, protocol.config.protocol_id);
                protocol.prod_block(&mut net_sync);
                Ok(())
            }
        }).then(|res| {
            match res {
                Ok(()) => (),
                Err(err) => error!("Error in the block_production {:?}", err),
            };
            Ok(())
        });

    let futures: Vec<Box<Future<Item = (), Error = io::Error> + Send>> = vec![
        Box::new(timer),
        Box::new(network),
        Box::new(block_production),
    ];

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

    #[test]
    fn test_send_message() {
        let services = create_test_services(2);
        let mut runtime = tokio::runtime::Runtime::new().unwrap();
        for service in services.iter() {
            let task = generate_service_task::<MockBlock, SignedTransaction, MockProtocolHandler>(
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
