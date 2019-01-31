use primitives::types::AccountId;
use primitives::serialize::Encode;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::io::{Error, ErrorKind};
use tokio::net::{TcpStream, TcpListener};
use tokio_codec::{Framed};
use tokio::prelude::stream::SplitStream;
use tokio::timer::Interval;
use std::time::Duration;
use futures::sync::mpsc::{channel, Sender, Receiver};
use futures::{Stream, Future, Sink, future};
use log::{error, debug};
use serde_derive::{Serialize, Deserialize};
use rand::{thread_rng, seq::IteratorRandom};
use parking_lot::RwLock;
use std::sync::Arc;
use tokio_serde_cbor::Codec;
use configs::network::NetworkConfig;
use primitives::types::PeerId;
use log::info;

use crate::message::Message;

#[derive(PartialEq, Eq, Hash, Clone, Debug, Serialize, Deserialize)]
/// Information about a peer
struct Peer {
    /// address of peer
    addr: SocketAddr,
    /// peer id
    id: PeerId,
    /// account id, peer may not always have one
    account_id: Option<AccountId>,
}

/// struct that handles connection with peer. Due to Rust lifetime restrictions,
/// we have to clone some fields of Service when spawning tasks that use them. 
/// To avoid duplicate code, we put the fields of Service that need cloning into one struct.
struct ConnectionHandler {
    account_to_peer: Arc<RwLock<HashMap<AccountId, PeerId>>>,
    connected_peers: Arc<RwLock<HashMap<PeerId, Sender<ServiceEvent>>>>,
    peer_state: Arc<RwLock<HashMap<SocketAddr, ConnectionState>>>,
    peer_info: Arc<RwLock<HashMap<PeerId, Peer>>>,
    message_tx: Sender<NetworkEvent>,
    banned_nodes: Arc<RwLock<HashSet<PeerId>>>,
}

impl ConnectionHandler {
    /// take a raw tcpstream and split it into sink and stream. Consume the sink
    /// in a task so that we can send data through sender and be able to clone sender
    /// freely. Consume the stream to spawn a task that handles event generated on stream
    /// returns the sender
    fn convert_stream(
        self,
        addr: SocketAddr,
        stream: TcpStream
    ) -> Sender<ServiceEvent> {
        let (sender, receiver) = channel(1024);
        let (sink, stream) = Framed::new(stream, Codec::new()).split();
        // spawn the task that forwards what receiver receives to send through sink
        tokio::spawn(
            receiver
                .forward(sink.sink_map_err(|e| error!("Error sending data the sink: {}", e)))
                .map(|_| ())
        );
        self.spawn_event_task(addr, sender.clone(), stream);
        sender
    }

    fn send_network_event(&self, event: NetworkEvent) {
        tokio::spawn(
            self.message_tx
                .clone()
                .send(event)
                .map(|_| ())
                .map_err(|e| error!("Error sending network message: {}", e))
        );
    }

    fn spawn_event_task(
        self,
        addr: SocketAddr,
        sender: Sender<ServiceEvent>,
        stream: SplitStream<Framed<TcpStream, Codec<ServiceEvent, ServiceEvent>>>
    ) {
        let task = stream.for_each(move |event| {
            debug!(target: "service", "service event: {:?}", event);
            match event {
                ServiceEvent::HandShake { peer_id, account_id } => {
                    if self.banned_nodes.read().contains(&peer_id) {
                        // node is banned, we end the connection
                        self.peer_state.write().remove(&addr);
                        let mut sender = sender.clone();
                        tokio::spawn(
                            future::poll_fn(move || 
                                sender.close().map_err(|e| error!("Error in closing channel: {}", e))
                            )
                        );
                        return Ok(());
                    }
                    if let Some(account_id) = account_id.clone() {
                        self.account_to_peer.write().insert(account_id, peer_id);
                    }
                    self.peer_state.write().entry(addr).and_modify(|e| {
                        *e = ConnectionState::Connected;
                    });
                    self.connected_peers.write().insert(peer_id, sender.clone());
                    let peer = Peer { addr, id: peer_id, account_id: account_id.clone() };
                    self.peer_info.write().insert(peer_id, peer);
                    let network_event = NetworkEvent::PeerConnected {
                        peer_id, account_id
                    };
                    self.send_network_event(network_event);
                }
                ServiceEvent::Message { peer_id, data } => {
                    let network_event = NetworkEvent::Message { peer_id, data };
                    self.send_network_event(network_event);
                }
                ServiceEvent::NodeClosed { peer_id } => {
                    self.connected_peers.write().remove(&peer_id);
                    let peer_info = self.peer_info.read();
                    let peer = peer_info
                        .get(&peer_id)
                        .expect("cannot find info of connected peer");
                    self.peer_state.write().remove(&peer.addr);
                    let network_event = NetworkEvent::PeerClosed { peer_id };
                    self.send_network_event(network_event);
                }
                ServiceEvent::AccountInfo { info, .. } => {
                    self.account_to_peer.write().extend(info);
                }
            };
            Ok(())
        }).map_err(|e| error!("Error when receiving: {}", e));
        tokio::spawn(task);
    }
}

impl Clone for ConnectionHandler {
    fn clone(&self) -> Self {
        ConnectionHandler {
            account_to_peer: self.account_to_peer.clone(),
            connected_peers: self.connected_peers.clone(),
            peer_state: self.peer_state.clone(),
            peer_info: self.peer_info.clone(),
            message_tx: self.message_tx.clone(),
            banned_nodes: self.banned_nodes.clone(),
        }
    }
}

/// event generated by service
/// should be sent to protocol for further processing
#[derive(Debug)]
pub enum NetworkEvent {
    PeerConnected {
        peer_id: PeerId,
        account_id: Option<AccountId>,
    },
    Message {
        peer_id: PeerId,
        data: Vec<u8>,
    },
    PeerClosed {
        peer_id: PeerId
    },
}

/// records malicious behavior on the network
pub enum Severity {
    /// peer didn't respond within certain time period
    Timeout,
    /// peer behaved maliciously
    Bad(String)
}

enum ConnectionState {
    Pending,
    Handshaking,
    Connected,
}

pub struct Service {
    /// peer id of the node
    peer_id: PeerId,
    /// account id of the node
    account_id: Option<AccountId>,
    // TODO: listen on multiple address
    // use option so that we can take listener out
    listener: Option<TcpListener>,
    /// Local info about accounts
    account_to_peer: Arc<RwLock<HashMap<AccountId, PeerId>>>,
    /// connected peers and the sender channel to the peer
    connected_peers: Arc<RwLock<HashMap<PeerId, Sender<ServiceEvent>>>>,
    /// state of the connection with peer
    peer_state: Arc<RwLock<HashMap<SocketAddr, ConnectionState>>>,
    /// PeerId to peer info
    peer_info: Arc<RwLock<HashMap<PeerId, Peer>>>,
    /// number of peers to gossip
    gossip_num: usize,
    /// gossip frequency
    gossip_period: Duration,
    /// channel that sends custom message for further processing
    message_tx: Sender<NetworkEvent>,
    /// nodes that misbehaved and are banned
    banned_nodes: Arc<RwLock<HashSet<PeerId>>>,
}

impl Service {
    pub fn init(
        config: NetworkConfig,
        message_tx: Sender<NetworkEvent>,
        message_rx: Receiver<Result<(PeerId, Message), (PeerId, Severity)>>,
        account_id: Option<AccountId>
    ) {
        let mut service = Self::new(config.clone(), message_tx, account_id);
        let listener = service.listener.take().expect("listener already taken");
        let service = Arc::new(service);
        tokio::spawn(futures::lazy(move || {
            service.spawn_background_tasks(listener);
            for node in config.boot_nodes.iter() {
                if let Err(e) = service.dial(*node) {
                    error!("error dialing node: {}", e);
                }
            }
            tokio::spawn(message_rx.for_each(move |m| {
                match m {
                    Ok((peer_id, message)) => {
                        let data = message.encode().expect("cannot encode message");
                        let fut = service.clone().send_message(&peer_id, data);
                        tokio::spawn(fut);
                    }
                    Err((peer_id, Severity::Timeout)) => {
                        service.clone().drop_node(peer_id);
                    }
                    Err((peer_id, Severity::Bad(_))) => {
                        service.clone().ban_node(peer_id);
                    }
                };
                Ok(())
            }));
            Ok(())
        }));
    }

    fn new(config: NetworkConfig, message_tx: Sender<NetworkEvent>, account_id: Option<AccountId>) -> Self {
        let listener = TcpListener::bind(&config.listen_addr).expect("Cannot bind to address");
        Service {
            peer_id: config.peer_id,
            account_id,
            listener: Some(listener),
            account_to_peer: Arc::new(RwLock::new(HashMap::new())),
            connected_peers: Arc::new(RwLock::new(HashMap::new())),
            peer_state: Arc::new(RwLock::new(HashMap::new())),
            peer_info: Arc::new(RwLock::new(HashMap::new())),
            gossip_num: 3,
            gossip_period: Duration::from_secs(10),
            message_tx,
            banned_nodes: Arc::new(RwLock::new(HashSet::new())),
        }
    }

    fn get_connection_handler(&self) -> ConnectionHandler {
        ConnectionHandler {
            account_to_peer: self.account_to_peer.clone(),
            connected_peers: self.connected_peers.clone(),
            peer_state: self.peer_state.clone(),
            peer_info: self.peer_info.clone(),
            message_tx: self.message_tx.clone(),
            banned_nodes: self.banned_nodes.clone(),
        }
    }

    /// try to dial peer, if we are already connected to the peer or are waiting to connect,
    /// returns error. Otherwise we spawn a task that initiates the connection
    pub fn dial(&self, addr: SocketAddr) -> Result<(), Error> {
        if self.peer_state.read().contains_key(&addr) {
            return Err(Error::new(
                ErrorKind::Other,
                format!("Already dialed peer on addr: {}", addr)
            ));
        }
        self.peer_state.write().insert(addr, ConnectionState::Pending);
        let connection_handler = self.get_connection_handler();
        let peer_id = self.peer_id;
        let account_id = self.account_id.clone();
        let task = TcpStream::connect(&addr).map(move |stream| {
            let sender = connection_handler.convert_stream(addr, stream);
            Self::send_handshake_message(sender, peer_id, account_id);
        }).map_err(|_| ());
        tokio::spawn(task);
        Ok(())
    }

    fn spawn_listening_task(&self, listener: TcpListener) {
        let peer_state = self.peer_state.clone();
        let connection_handler = self.get_connection_handler();
        let peer_id = self.peer_id;
        let account_id = self.account_id.clone();
        let task = listener.incoming().for_each(move |socket| {
            let peer_addr = socket.peer_addr()?;
            let sender = connection_handler.clone().convert_stream(peer_addr, socket);
            Self::send_handshake_message(sender, peer_id, account_id.clone());
            peer_state.write().insert(peer_addr, ConnectionState::Handshaking);
            Ok(())
        }).map_err(|e| error!("Error when listening: {:?}", e));
        tokio::spawn(task);
    }

    /// gossip account info to some of peers
    // TODO: find efficient way of gossiping. Maybe store what has been gossiped to each peer?
    fn spawn_gossip_task(&self) {
        let connected_peers = self.connected_peers.clone();
        let account_to_peer = self.account_to_peer.clone();
        let gossip_num = self.gossip_num;
        let task = Interval::new_interval(self.gossip_period)
        .map_err(|e| error!("Timer error: {}", e))
        .for_each(move |_| {
            let mut rng = thread_rng();
            let connected_peers = connected_peers.read();
            for peer_id in connected_peers.keys().choose_multiple(&mut rng, gossip_num) {
                // peer_id must exist, so we force unwrap here
                let sender = connected_peers.get(peer_id).unwrap().clone();
                let account_to_peer = account_to_peer.read();
                let event = ServiceEvent::AccountInfo {
                    peer_id: *peer_id,
                    info: account_to_peer.clone()
                };
                tokio::spawn(
                    sender
                        .send(event)
                        .map(|_| ())
                        .map_err(|e| error!("Error sending account info: {:?}", e))
                );
            }
            Ok(())
        });
        tokio::spawn(task);
    }

    /// spawn all background tasks, including listening on port,
    /// gossiping to peers periodically, etc. Must be used in a task
    fn spawn_background_tasks(&self, listner: TcpListener) {
        self.spawn_listening_task(listner);
        self.spawn_gossip_task();
    }

    /// sending message to peer. Must be used in a task
    pub fn send_message(&self, peer: &PeerId, data: Vec<u8>) -> impl Future<Item = (), Error = ()> {
        if let Some(sender) = self.connected_peers.write().get(&peer) {
            let sender = sender.clone();
            let message_event = ServiceEvent::Message {
                peer_id: self.peer_id,
                data,
            };
            sender.send(message_event)
                .map(|_| ())
                .map_err(|e| error!("Error sending message: {:?}", e))
        } else {
            // TODO: route through peers
            unimplemented!("unknown peer")
        }
    }

    /// send handshake message through sender
    fn send_handshake_message(
        sender: Sender<ServiceEvent>,
        peer_id: PeerId,
        account_id: Option<AccountId>
    ) {
        let handshake_msg = ServiceEvent::HandShake { 
            peer_id,
            account_id,
        };
        tokio::spawn(
            sender.send(handshake_msg).map(|_| ()).map_err(|e| {
                error!("Error when sending handshake message: {:?}", e);
            })
        );
    }

    /// disconnect from peer
    fn drop_node(&self, peer_id: PeerId) {
        let sender = self.connected_peers.write().remove(&peer_id);
        if let Some(mut sender) = sender {
            tokio::spawn(
                future::poll_fn(move || 
                    sender.close().map_err(|e| error!("Error in closing channel: {}", e))
                )
            );
        } else {
            // This should not happen. However, in case this happened, since it is not fatal,
            // we just do nothing and log it
            error!("Trying to drop a node {} that is not connected", peer_id);
        }
        self.peer_info
            .write()
            .remove(&peer_id)
            .map(|peer| self.peer_state.write().remove(&peer.addr));
    }

    /// ban a peer and disconnect from them
    // TODO: Should the ban be temporary and have a time period for which it is valid?
    fn ban_node(&self, peer_id: PeerId) {
        self.drop_node(peer_id);
        self.banned_nodes.write().insert(peer_id);
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum ServiceEvent {
    /// remote closed connection
    NodeClosed {
        peer_id: PeerId,
    },
    /// handshake message
    HandShake {
        peer_id: PeerId,
        account_id: Option<AccountId>,
    },
    /// custom message
    Message {
        peer_id: PeerId,
        data: Vec<u8>
    },
    /// account info received from peer
    AccountInfo {
        peer_id: PeerId,
        info: HashMap<AccountId, PeerId>,
    }
}

//#[cfg(test)]
//mod tests {
//    use super::*;
//    use ::primitives::hash::hash_struct;
//    use std::thread;
//    use std::time::Duration;
//    use std::sync::Arc;
//    use parking_lot::Mutex;
//    use ::tokio::timer::Interval;
//
//    impl Peer {
//        fn new(addr: SocketAddr, id: PeerId, account_id: Option<AccountId>) -> Self {
//            Peer { addr, id, account_id }
//        }
//    }
//
//    impl Service {
//        fn _spawn_listening_task(&mut self) {
//            let listener = self.listener.take().expect("listener already taken");
//            self.spawn_listening_task(listener);
//        }
//    }
//
//    #[test]
//    fn test_two_peers() {
//        let config1 = NetworkConfig::new("127.0.0.1:3000", hash_struct(&0), vec![]);
//        let config2 = NetworkConfig::new("127.0.0.1:3001", hash_struct(&1), vec![]);
//        let (message_tx1, _) = channel(1024);
//        let (message_tx2, _) = channel(1024);
//        let service1 = Arc::new(Mutex::new(Service::new(config1.clone(), message_tx1)));
//        let service2 = Arc::new(Mutex::new(Service::new(config2, message_tx2)));
//        let peer = Peer::new(config1.listen_addr, config1.peer_id, None);
//
//        let task = futures::lazy({
//            let service1 = service1.clone();
//            let service2 = service2.clone();
//            move || {
//                service1.lock()._spawn_listening_task();
//                service2.lock()._spawn_listening_task();
//                service2.lock().dial(peer.addr).unwrap();
//                Ok(())
//            }
//        });
//        thread::spawn(move || tokio::run(task));
//        while service1.lock().connected_peers.read().len() < 1
//            || service2.lock().connected_peers.read().len() < 1 {
//            thread::sleep(Duration::from_secs(1));
//        }
//    }
//
//    #[test]
//    fn test_send_message() {
//        let config1 = NetworkConfig::new("127.0.0.1:3002", hash_struct(&0), vec![]);
//        let config2 = NetworkConfig::new("127.0.0.1:3003", hash_struct(&1), vec![]);
//        let (message_tx1, _) = channel(1024);
//        let (message_tx2, message_rx2) = channel(1024);
//        let service1 = Arc::new(Mutex::new(Service::new(config1.clone(), message_tx1)));
//        let service2 = Arc::new(Mutex::new(Service::new(config2, message_tx2)));
//        let peer = Peer::new(config1.listen_addr, config1.peer_id, None);
//        let timeout = Duration::from_secs(5);
//        let message_queue = Arc::new(Mutex::new(vec![]));
//        thread::spawn({
//            let queue = message_queue.clone();
//            move || {
//                let task = Interval::new_interval(timeout)
//                    .map(|_| None)
//                    .map_err(|e| println!("{}", e))
//                    .select(message_rx2.map(Some))
//                    .for_each(move |m| {
//                        queue.lock().push(m);
//                        Ok(())
//                    });
//                tokio::run(task);
//            }
//        });
//
//        // connect two peers
//        let task = futures::lazy({
//            let service1 = service1.clone();
//            let service2 = service2.clone();
//            move || {
//                service1.lock()._spawn_listening_task();
//                service2.lock()._spawn_listening_task();
//                service2.lock().dial(peer.addr).unwrap();
//                Ok(())
//            }
//        });
//        thread::spawn(move || tokio::run(task));
//        // wait until connected
//        while service1.lock().connected_peers.read().len() < 1
//            || service2.lock().connected_peers.read().len() < 1 {
//            thread::sleep(Duration::from_secs(1));
//        }
//
//        // send message
//        let message = b"hello".to_vec();
//        let peer = {
//            let service = service1.lock();
//            let connected_peers = service.connected_peers.read();
//            connected_peers.keys().cloned().collect::<Vec<_>>()[0]
//        };
//
//        thread::spawn(move || {
//            tokio::run(
//                futures::lazy({
//                    let service = service1.clone();
//                    move || {
//                        tokio::spawn(service.lock().send_message(&peer, message));
//                        Ok(())
//                    }
//                })
//            )
//        });
//        // wait until message is received
//        while message_queue.lock().len() < 2 {
//            thread::sleep(Duration::from_secs(1));
//        }
//
//        let message = message_queue.lock().pop().unwrap();
//        if let Some(NetworkEvent::Message { peer_id, data }) = message {
//            assert_eq!(data, b"hello".to_vec());
//            assert_eq!(peer_id, config1.peer_id);
//        } else {
//            assert!(false);
//        }
//    }
//}
