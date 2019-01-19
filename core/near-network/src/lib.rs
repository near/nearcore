use ::primitives::types::AccountId;
use ::primitives::hash::CryptoHash;
use ::primitives::traits::{Encode, Decode};
use std::collections::{HashMap, HashSet, VecDeque};
use std::net::SocketAddr;
use std::io::{Error, ErrorKind};
use ::tokio::net::{TcpStream, TcpListener, tcp::ConnectFuture};
use ::tokio_codec::{Framed};
use ::tokio::prelude::stream::SplitStream;
use ::futures::sync::mpsc::{channel, Sender};
use ::futures::{Poll, Stream, Async, Future, Sink};
use ::bytes::Bytes;
use ::log::{debug, error};
use ::serde_derive::{Serialize, Deserialize};
use ::rand::{thread_rng, seq::IteratorRandom};

mod codec;
use crate::codec::Codec;

#[derive(PartialEq, Eq, Hash, Clone, Debug, Serialize, Deserialize)]
pub struct Peer {
    /// address of peer
    addr: SocketAddr,
    /// peer id
    id: PeerId,
    /// account id, peer may not always have one
    account_id: Option<AccountId>,
}

impl Peer {
    pub fn new(addr: SocketAddr, id: PeerId, account_id: Option<AccountId>) -> Self {
        Peer {
            addr,
            id,
            account_id
        }
    }
}

/// Handshake info we exchange when first establish connection with a peer. 
// TODO: make it secure
#[derive(Serialize, Deserialize)]
pub struct HandShake {
    peer_id: PeerId,
    account_id: Option<AccountId>,
}

/// unique identifier for nodes on the network
// Use hash for now
pub type PeerId = CryptoHash;

// This is very awkward because we cannot copy sink and have to use a channel
// to tunnel the data through. Definitely need to revisit this later to check for
// better solutions
type NearStream = (Sender<Bytes>, SplitStream<Framed<TcpStream, Codec>>);

/// convert raw TcpStream to NearStream
fn convert_stream(stream: TcpStream) -> NearStream {
    let (sender, receiver) = channel(1024);
    let (sink, stream) = Framed::new(stream, Codec::new()).split();
    // spawn the task that forwards what receiver receives to send through sink
    tokio::spawn(
        sink.send_all(
            receiver.map_err(|e| Error::new(ErrorKind::BrokenPipe, format!("{:?}", e)))
        )
        .map(|_| ())
        .map_err(|e| error!("Error when forwarding: {:?}", e))
    );
    (sender, stream)
}

pub struct Service {
    /// peer id of the node
    peer_id: PeerId,
    /// account id of the node
    account_id: Option<AccountId>,
    // TODO: listen on multiple address
    listener: TcpListener,
    /// Local info about accounts
    account_to_peer: HashMap<AccountId, Peer>,
    /// peers that we are trying to connect to 
    pending_peers: HashSet<SocketAddr>,
    /// pending connection futures
    pending_connections: VecDeque<(SocketAddr, ConnectFuture)>,
    /// peers that we have established connection with, but
    /// have not exchanged handshake information yet
    handshaking_peers: HashMap<SocketAddr, NearStream>,
    /// connected peers and the corresponding stream
    connected_peers: HashMap<PeerId, NearStream>,
    /// PeerId to peer info
    peer_info: HashMap<PeerId, Peer>,
    /// number of peers to gossip
    gossip_num: usize,
}

impl Service {
    pub fn new(addr: &str, peer_id: PeerId) -> Self {
        let addr = addr.parse::<SocketAddr>().expect("Incorrect address");
        let listener = TcpListener::bind(&addr).expect("Cannot bind to address");
        Service {
            peer_id,
            account_id: None,
            listener,
            account_to_peer: HashMap::new(),
            pending_peers: HashSet::new(),
            pending_connections: VecDeque::new(),
            handshaking_peers: HashMap::new(),
            connected_peers: HashMap::new(),
            peer_info: HashMap::new(),
            gossip_num: 3
        }
    }

    pub fn new_with_account_id(addr: &str, peer_id: PeerId, account_id: AccountId) -> Self {
        let mut service = Self::new(addr, peer_id);
        service.account_id = Some(account_id);
        service
    }

    /// try to dial peer, if we are already connected to the peer or are waiting to connect,
    /// do nothing. Otherwise we create a future that represents the connection and put it
    /// to the pending connection queue
    pub fn dial(&mut self, peer: &Peer) {
        if self.pending_peers.contains(&peer.addr)
            || self.handshaking_peers.contains_key(&peer.addr)
            || self.connected_peers.contains_key(&peer.id) {
            return;
        }
        self.pending_peers.insert(peer.addr);
        let connect_future = TcpStream::connect(&peer.addr);
        self.pending_connections.push_back((peer.addr, connect_future));
    }

    /// sending message to peer. Must be used in a task
    // TODO: consider not spawning the task here and instead put the future
    // into some queue that can be polled later
    pub fn send_message(&self, peer: &PeerId, data: Vec<u8>) {
        if let Some((sender, _)) = self.connected_peers.get(&peer) {
            let sender = sender.clone();
            tokio::spawn(
                sender.send(data.into())
                .map(|_| ())
                .map_err(|e| error!("Error sending message: {:?}", e))
            );
        } else {
            // TODO: route through peers
            unimplemented!("not connected to peer");
        }
    }

    /// gossip account info to some of peers
    // TODO: find efficient way of gossiping. Maybe store what has been gossiped to each peer?
    pub fn gossip_account_info(&self) {
        let mut rng = thread_rng();
        for peer_id in self.connected_peers.keys().choose_multiple(&mut rng, self.gossip_num) {
            let message = self.account_to_peer.clone().encode().expect("failed to encode message");
            self.send_message(peer_id, message);
        }
    }

    /// send handshake message through sender
    fn send_handshake_message(&self, sender: Sender<Bytes>) -> Result<(), Error> {
        let handshake_msg = HandShake { 
            peer_id: self.peer_id,
            account_id: self.account_id.clone(),
        };
        let data = handshake_msg
            .encode()
            .ok_or_else(|| Error::new(
                ErrorKind::InvalidData,
                "cannot encode handshake message"
            ))?;
        tokio::spawn(
            sender.send(data.into()).map(|_| ()).map_err(|e| {
                error!("Error when sending handshake message: {:?}", e);
            })
        );
        Ok(())
    }
}

#[derive(Debug)]
pub enum ServiceEvent {
    /// connection opened by remote
    NodeOpened {
        addr: SocketAddr,
    },
    /// connected to remote
    NodeReached {
        addr: SocketAddr
    },
    /// remote closed connection
    NodeClosed {
        account_id: AccountId
    },
    /// connection established
    PeerConnected {
        peer_id: PeerId
    },
    /// custom message
    Message {
        peer_id: PeerId,
        data: Bytes
    },
}

impl Stream for Service {
    type Item = ServiceEvent;
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<ServiceEvent>, Error> {
        // poll pending connections in a round robin fashion, the futures that are
        // not ready are put back to the end of the queue.
        if let Some((addr, mut fut)) = self.pending_connections.pop_front() {
            match fut.poll()? {
                Async::Ready(stream) => {
                    let (sender, stream) = convert_stream(stream);
                    self.pending_peers.remove(&addr);
                    self.handshaking_peers.insert(addr, (sender.clone(), stream));
                    self.send_handshake_message(sender)?;
                    let event = ServiceEvent::NodeReached { addr };
                    return Ok(Async::Ready(Some(event)));
                }
                Async::NotReady => {
                    self.pending_connections.push_back((addr, fut));
                },
            }
        }
        // poll handshaking connections
        let addrs: Vec<_> = self.handshaking_peers.keys().cloned().collect();
        for addr in addrs {
            // force unwrap because value must exist
            let (_, stream) = self.handshaking_peers.get_mut(&addr).unwrap();
            match stream.poll()? {
                Async::Ready(Some(frame)) => {
                    // receive handshake message
                    let message: HandShake = Decode::decode(&frame)
                      .ok_or_else(|| Error::new(ErrorKind::InvalidData, "cannot decode data"))?;
                    let stream = self.handshaking_peers.remove(&addr).unwrap();
                    let peer = Peer::new(addr, message.peer_id, message.account_id);
                    self.connected_peers.insert(message.peer_id, stream);
                    self.peer_info.insert(message.peer_id, peer);
                    let event = ServiceEvent::PeerConnected {
                        peer_id: message.peer_id
                    };
                    
                    return Ok(Async::Ready(Some(event)));
                }
                Async::Ready(None) => (),
                Async::NotReady => (),
            }
        }
        
        // poll existing connections
        for (peer_id, (_, stream)) in self.connected_peers.iter_mut() {
            match stream.poll()? {
                Async::Ready(Some(frame)) => {
                    let event = ServiceEvent::Message {
                        peer_id: *peer_id,
                        data: frame
                    };
                    return Ok(Async::Ready(Some(event)))
                }
                Async::Ready(None) => (),
                Async::NotReady => (),
            }
        }
        // poll listener
        match self.listener.poll_accept()? {
            Async::Ready((stream, socket_addr)) => {
                let (sender, stream) = convert_stream(stream);
                self.handshaking_peers.insert(socket_addr, (sender.clone(), stream));
                self.send_handshake_message(sender)?;
                return Ok(Async::Ready(Some(ServiceEvent::NodeOpened { addr: socket_addr })));
            }
            Async::NotReady => (),
        }
        Ok(Async::NotReady)
    }
}

pub fn spawn_service_task(service: Service) {
    let task = service.for_each(|event| {
        debug!(target: "near-network", "{:?}", event);
        Ok(())
    }).map_err(|e| error!("Error on network service: {:?}", e));
    tokio::spawn(task);
}

#[cfg(test)]
mod tests {
    use super::*;
    use ::primitives::hash::hash_struct;
    use std::thread;
    use std::time::Duration;
    use std::sync::Arc;
    use parking_lot::Mutex;
    use futures::stream;

    #[test]
    fn test_two_peers() {
        let addr1 = "127.0.0.1:3000";
        let addr2 = "127.0.0.1:3001";
        let peer_id1 = hash_struct(&0);
        let peer_id2 = hash_struct(&1);
        let service1 = Arc::new(Mutex::new(Service::new(addr1.clone(), peer_id1)));
        let service2 = Arc::new(Mutex::new(Service::new(addr2.clone(), peer_id2)));
        let peer = Peer::new(addr1.parse::<SocketAddr>().unwrap(), peer_id1, None);
        
        let task = futures::lazy({
            let service1 = service1.clone();
            let service2 = service2.clone();
            move || {
                service2.lock().dial(&peer);
                let task1 = stream::poll_fn(move || service1.lock().poll()).for_each(|e| {
                    println!("service1 event: {:?}", e);
                    Ok(())
                }).map_err(|e| println!("error: {:?}", e));
                tokio::spawn(task1);
                let task2 = stream::poll_fn(move || service2.lock().poll()).for_each(|e| {
                    println!("service2 event: {:?}", e);
                    Ok(())
                }).map_err(|e| println!("error: {:?}", e));
                tokio::spawn(task2);
                Ok(()) 
            }
        });
        thread::spawn(move || tokio::run(task));
        while service1.lock().connected_peers.len() < 1 
            || service2.lock().connected_peers.len() < 1 {
            thread::sleep(Duration::from_secs(1));
        }
    }

    #[test]
    fn test_send_message() {
        let addr1 = "127.0.0.1:3002";
        let addr2 = "127.0.0.1:3003";
        let peer_id1 = hash_struct(&0);
        let peer_id2 = hash_struct(&1);
        let service1 = Arc::new(Mutex::new(Service::new(addr1.clone(), peer_id1)));
        let service2 = Arc::new(Mutex::new(Service::new(addr2.clone(), peer_id2)));
        let peer = Peer::new(addr1.parse::<SocketAddr>().unwrap(), peer_id1, None);
        let event_queue = Arc::new(Mutex::new(vec![]));
        // connect two peers
        let task = futures::lazy({
            let service1 = service1.clone();
            let service2 = service2.clone();
            let events = event_queue.clone();
            move || {
                service2.lock().dial(&peer);
                let task1 = stream::poll_fn(move || service1.lock().poll()).for_each(|e| {
                    println!("service1 event: {:?}", e);
                    Ok(())
                }).map_err(|e| println!("error: {:?}", e));
                tokio::spawn(task1);
                let task2 = stream::poll_fn(move || service2.lock().poll()).for_each(move |e| {
                    println!("service2 event: {:?}", e);
                    events.lock().push(e);
                    Ok(())
                }).map_err(|e| println!("error: {:?}", e));
                tokio::spawn(task2);
                Ok(()) 
            }
        });
        thread::spawn(move || tokio::run(task));
        // wait until connected
        while service1.lock().connected_peers.len() < 1 
            || service2.lock().connected_peers.len() < 1 {
            thread::sleep(Duration::from_secs(1));
        }
        // send message
        event_queue.lock().clear();
        let message = b"hello".to_vec();
        let peer = service1.lock().connected_peers.keys().cloned().collect::<Vec<_>>()[0];
        thread::spawn(move || {
            tokio::run(
                futures::lazy({
                    let service = service1.clone();
                    move || {
                        service.lock().send_message(&peer, message);
                        Ok(())
                    }
                })
            )
        });
        // wait until message is received
        while event_queue.lock().len() < 1 {
            thread::sleep(Duration::from_secs(1));
        }

        let message = event_queue.lock().pop().unwrap();
        if let ServiceEvent::Message {data, peer_id} = message {
            assert_eq!(data, b"hello".to_vec());
            assert_eq!(peer_id, peer_id1);
        } else {
            assert!(false);
        }
    }
}