use client::Client;
use error::Error;
use futures::{self, Future, stream, Stream};
use io::NetSyncIo;
use primitives::traits::{Block, Header as BlockHeader};
use protocol::{self, Protocol, ProtocolConfig};
use protocol::ProtocolHandler;
use std::cell::RefCell;
use std::io;
use std::rc::Rc;
use std::time::Duration;
use substrate_network_libp2p::{
    RegisteredProtocol, Service as NetworkService, ServiceEvent, start_service,
};
pub use substrate_network_libp2p::NetworkConfiguration;
use tokio::timer::Interval;
use std::sync::Arc;

const TICK_TIMEOUT: Duration = Duration::from_millis(1000);
const BLOCK_PROD_TIMEOUT: Duration = Duration::from_secs(2);

pub struct Service<B: Block, H: ProtocolHandler> {
    pub network: Rc<RefCell<NetworkService>>,
    pub protocol: Rc<Protocol<B, H>>,
}

impl<B: Block, H: ProtocolHandler> Service<B, H> {
    pub fn new(
        config: ProtocolConfig,
        net_config: NetworkConfiguration,
        handler: H,
        client: Arc<Client<B>>,
    ) -> Result<Service<B, H>, Error> {
        let version = [protocol::CURRENT_VERSION as u8];
        let registered = RegisteredProtocol::new(config.protocol_id, &version);
        let protocol = Rc::new(Protocol::new(config, handler, client));
        let service = match start_service(net_config, Some(registered)) {
            Ok(s) => Rc::new(RefCell::new(s)),
            Err(e) => return Err(e.into()),
        };
        Ok(Service { network: service, protocol })
    }
}

pub fn generate_service_task<B, H, Header>(
    network_service: Rc<RefCell<NetworkService>>,
    protocol: Rc<Protocol<B, H>>,
) -> impl Future<Item=(), Error=()>
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
        move || {
            network_service1.borrow_mut().poll()
        }
    }).for_each({
            let network_service1 = network_service.clone();
            let protocol1 = protocol.clone();
            move |event| {
                let mut net_sync = NetSyncIo::new(network_service1.clone(), protocol1.config.protocol_id);
                debug!(target: "sub-libp2p", "event: {:?}", event);
                match event {
                    ServiceEvent::CustomMessage {
                        node_index, data, ..
                    } => {
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

    // block producing logic (fake for now)
    let block_production = Interval::new_interval(BLOCK_PROD_TIMEOUT)
        .for_each({
            move |_| {
                let mut net_sync = NetSyncIo::new(network_service.clone(), protocol.config.protocol_id);
                protocol.prod_block::<Header>(&mut net_sync);
                Ok(())
            }
        }).then(|res| {
        match res {
            Ok(()) => (),
            Err(err) => error!("Error in the block_production {:?}", err),
        };
        Ok(())
    });

    let futures: Vec<Box<Future<Item=(), Error=io::Error>>> = vec![
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
    use std::time::Instant;
    use super::*;
    use test_utils::*;
    use tokio::timer::Delay;

    #[test]
    fn test_send_message() {
        let services = create_test_services(2);
        let mut runtime = tokio::runtime::current_thread::Runtime::new().unwrap();
        for service in services.iter() {
            let task = generate_service_task::<MockBlock, MockProtocolHandler, MockBlockHeader>(
                service.network.clone(),
                service.protocol.clone(),
            );
            runtime.spawn(task);
        }

        let when = Instant::now() + Duration::from_millis(1000);
        let send_messages = Delay::new(when)
            .map_err(|e| panic!("timer failed; err={:?}", e))
            .and_then(move |_| {
                for service in services.iter() {
                    for peer in service.protocol.sample_peers(1).unwrap() {
                        let message = fake_tx_message();
                        let mut net_sync =
                            NetSyncIo::new(service.network.clone(), service.protocol.config.protocol_id);
                        service.protocol.send_message(&mut net_sync, peer, &message);
                    }
                }
                Ok(())
            });
        runtime.block_on(send_messages).unwrap();
    }
}
