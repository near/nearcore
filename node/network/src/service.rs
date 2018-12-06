use client::chain::Chain;
use error::Error;
use futures::{stream, Future, Stream};
use io::NetSyncIo;
use primitives::traits::{Block, Header as BlockHeader};
use protocol::ProtocolHandler;
use protocol::{self, Protocol, ProtocolConfig};
use std::sync::Arc;
use parking_lot::{Mutex, RwLock};
use std::time::Duration;
pub use substrate_network_libp2p::NetworkConfiguration;
use substrate_network_libp2p::{
    start_service, RegisteredProtocol, Service as NetworkService, ServiceEvent,
};
use tokio::timer::Interval;

const TICK_TIMEOUT: Duration = Duration::from_millis(1000);

pub struct Service<B: Block, H: ProtocolHandler> {
    pub network: Arc<Mutex<NetworkService>>,
    pub protocol: Arc<Protocol<B, H>>,
}

impl<B: Block, H: ProtocolHandler> Service<B, H> {
    pub fn new(
        config: ProtocolConfig,
        net_config: NetworkConfiguration,
        handler: H,
        chain: Arc<RwLock<Chain<B>>>,
    ) -> Result<Service<B, H>, Error> {
        let version = [protocol::CURRENT_VERSION as u8];
        let registered = RegisteredProtocol::new(config.protocol_id, &version);
        let protocol = Arc::new(Protocol::new(config, handler, chain));
        let service = match start_service(net_config, Some(registered)) {
            Ok(s) => Arc::new(Mutex::new(s)),
            Err(e) => return Err(e.into()),
        };
        Ok(Service { network: service, protocol })
    }
}

pub fn generate_service_task<B, H, Header>(
    network_service: &Arc<Mutex<NetworkService>>,
    protocol: &Arc<Protocol<B, H>>,
//) -> impl Future<Item = (), Error = ()>
) -> Box<impl Future<Item=(), Error=()>>
where
    B: Block,
    H: ProtocolHandler,
    Header: BlockHeader,
{
    // Interval for performing maintenance on the protocol handler.
    let timer = Interval::new_interval(TICK_TIMEOUT)
        .for_each({
            let network_service1 = network_service.clone();
            let protocol1 = protocol.clone();
            move |_| {
                protocol1.maintain_peers(&mut NetSyncIo::new(
                    network_service1.clone(),
                    protocol1.config.protocol_id,
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
    let network = stream::poll_fn({
        let network_service1 = network_service.clone();
        move || network_service1.lock().poll()
    }).for_each({
        let network_service1 = network_service.clone();
        let protocol1 = protocol.clone();
        move |event| {
            let mut net_sync =
                NetSyncIo::new(network_service1.clone(), protocol1.config.protocol_id);
            debug!(target: "sub-libp2p", "event: {:?}", event);
            match event {
                ServiceEvent::CustomMessage { node_index, data, .. } => {
                    protocol1.on_message::<Header>(&mut net_sync, node_index, &data);
                }
                ServiceEvent::OpenedCustomProtocol { node_index, .. } => {
                    protocol1.on_peer_connected::<Header>(&mut net_sync, node_index);
                }
                ServiceEvent::ClosedCustomProtocol { node_index, .. } => {
                    protocol1.on_peer_disconnected(node_index);
                }
                _ => {
                    debug!("TODO");
                    ()
                }
            };
            Ok(())
        }
    });

    Box::new(network.select(timer).and_then(|_| {
        info!("Networking stopped");
        Ok(())
    }).map_err(|(e, _)| debug!("Networking/Maintenance error {:?}", e)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;
    use test_utils::*;
    use tokio::timer::Delay;

    #[test]
    fn test_send_message() {
        let services = create_test_services(2);
        let mut runtime = tokio::runtime::current_thread::Runtime::new().unwrap();
        for service in services.iter() {
            let task = generate_service_task::<MockBlock, MockProtocolHandler, MockBlockHeader>(
                &service.network,
                &service.protocol,
            );
            runtime.spawn(task);
        }

        let when = Instant::now() + Duration::from_millis(1000);
        let send_messages =
            Delay::new(when).map_err(|e| panic!("timer failed; err={:?}", e)).and_then(move |_| {
                for service in services.iter() {
                    for peer in service.protocol.sample_peers(1).unwrap() {
                        let message = fake_tx_message();
                        let mut net_sync = NetSyncIo::new(
                            service.network.clone(),
                            service.protocol.config.protocol_id,
                        );
                        service.protocol.send_message(&mut net_sync, peer, &message);
                    }
                }
                Ok(())
            });
        runtime.block_on(send_messages).unwrap();
    }
}
