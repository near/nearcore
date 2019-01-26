use futures::stream::Stream;
use futures::sync::mpsc::{channel, Receiver, Sender};
use futures::{try_ready, Async, Future, Poll};
use parking_lot::RwLock;
use primitives::types::AccountId;
use primitives::types::PeerId;
use serde_derive::{Deserialize, Serialize};
use std::collections::HashMap;
use std::collections::HashSet;
use std::io::{Error, ErrorKind};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::codec::Framed;
use tokio::net::tcp::ConnectFuture;
use tokio::net::TcpStream;
use tokio::prelude::stream::{SplitSink, SplitStream};
use tokio::timer::Delay;
use tokio_serde_cbor::Codec;

/// How long do we wait for connection to be established.
const CONNECT_TIMEOUT_MS: u64 = 100;
/// How long do we wait for the initial handshake.
const INIT_HANDSHAKE_TIMEOUT_MS: u64 = 100;
/// How long to we wait for someone to reply to our handshake with their handshake.
const RESPONSE_HANDSHAKE_TIMEOUT_MS: u64 = 100;
/// Frequency of gossiping the peers info.
const GOSSIP_INTERVAL_MS: u64 = 5000;
/// How many peers should we gossip info to.
const GOSSIP_SAMPLE_SIZE: usize = 5;
/// Only happens if we made a mistake in our code and allowed certain optional fields to be None
/// during the states that they are not supposed to be None.
const STATE_ERR: &str = "Some fields are expected to be not None at the given state";

#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
pub enum PeerMessage {
    HandShake { id: PeerId, account_id: Option<AccountId>, peers_info: PeersInfo },
    InfoGossip(PeersInfo),
    Message,
}

/// Info about the peer. If peer is an authority then we also know its account id.
#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
pub struct PeerInfo {
    id: PeerId,
    addr: SocketAddr,
    account_id: Option<AccountId>,
}

pub type PeersInfo = Vec<PeerInfo>;

/// Note, the peer that establishes the connection is the one that sends the handshake.
pub enum PeerState {
    /// Someone unknown has established connection with us and we are waiting for them to send us
    /// the handshake. `stream`, `sink`, and `timeout` are not `None`.
    IncomingConnection(SocketAddr),
    /// We know address of the boot node, but have not attempted to connect to it, yet.
    UnconnectedBootNode(SocketAddr),
    /// We are attempting to connect to the boot node. `connect` and `timeout` are not `None`.
    ConnectingBootNode(SocketAddr),
    /// We connected to the boot node and sent them the handshake, now we are waiting for the reply.
    /// `stream`, `sink`, and `timeout` are not `None`.
    ConnectedBootNode(SocketAddr),
    /// Someone gossiped to us the info of the peer, but we have not established the connection yet.
    UnconnectedGossip(PeerInfo),
    /// We connected to the gossiped node and sent them the handshake, now we are waiting for the
    /// reply handshake. `stream`, `sink`, and `timeout` are not `None`.
    ConnectedGossip(PeerInfo),
    /// We have complete information about the peer and are now ready to exchange the messages.
    /// We have attached the channel to the sink and spawned the forwarding task.
    /// `stream` is not `None`.
    Complete(PeerInfo),
    /// The peer was dropped while pending.
    DroppedPending(SocketAddr),
    /// The peer was dropped while being usable.
    Dropped(PeerInfo),
}

pub type Addrs = Arc<RwLock<HashSet<SocketAddr>>>;
pub type IdToPeer = Arc<RwLock<HashMap<PeerId, (PeerInfo, Sender<()>)>>>;

pub struct Peer {
    state: PeerState,
    pending_peers: Addrs,
    peers: IdToPeer,

    connect: Option<ConnectFuture>,
    stream: Option<SplitStream<Framed<TcpStream, Codec<PeerMessage, PeerMessage>>>>,
    sink: Option<SplitSink<Framed<TcpStream, Codec<PeerMessage, PeerMessage>>>>,
    timeout: Option<Delay>,
}

impl Peer {
    /// A guard that makes sure there is no concurrent initialization for the same
    /// address.
    fn guarded_init(self, addr: SocketAddr) -> Result<Self, Error> {
        {
            let mut guard = self.pending_peers.write();
            if guard.contains(&addr) {
                return Err(Error::new(ErrorKind::AddrInUse, format!("Address is already used.")));
            }
            guard.insert(addr.clone());
        }
        Ok(self)
    }

    /// Initialize peer from the incoming opened connection.
    pub fn from_incoming_conn(
        socket: TcpStream,
        pending_peers: Addrs,
        peers: IdToPeer,
    ) -> Result<Self, Error> {
        let addr = socket.peer_addr()?;
        let (sink, stream) = Framed::new(socket, Codec::new()).split();
        let timeout = Delay::new(Instant::now() + Duration::from_millis(INIT_HANDSHAKE_TIMEOUT_MS));

        Self {
            state: PeerState::IncomingConnection(addr.clone()),
            pending_peers,
            peers,
            connect: None,
            stream: Some(stream),
            sink: Some(sink),
            timeout: Some(timeout),
        }
        .guarded_init(addr)
    }

    /// Initialize peer from the boot node address.
    pub fn from_boot_node(
        addr: SocketAddr,
        pending_peers: Addrs,
        peers: IdToPeer,
    ) -> Result<Self, Error> {
        Self {
            state: PeerState::UnconnectedBootNode(addr.clone()),
            pending_peers,
            peers,
            connect: None,
            stream: None,
            sink: None,
            timeout: None,
        }
        .guarded_init(addr)
    }

    /// Initialize peer from the gossiped info.
    pub fn from_gossiped(
        info: PeerInfo,
        pending_peers: Addrs,
        peers: IdToPeer,
    ) -> Result<Self, Error> {
        let addr = info.addr.clone();
        Self {
            state: PeerState::UnconnectedGossip(info),
            pending_peers,
            peers,
            connect: None,
            stream: None,
            sink: None,
            timeout: None,
        }
        .guarded_init(addr)
    }

    fn merge_peers_info(&self, peers_info: PeersInfo) {}

    fn complete_and_handshake(&self) {
        /// Also move us from pending to complete.
        unimplemented!()
    }

    fn handshake(&self) {
        unimplemented!()
    }
}

/// Converts CBOR Error to IO Error.
fn cbor_err(err: tokio_serde_cbor::Error) -> Error {
    Error::new(ErrorKind::InvalidData, format!("Error decoding message: {}", err))
}

/// Converts Timer Error to IO Error.
fn timer_err(err: tokio::timer::Error) -> Error {
    Error::new(ErrorKind::Other, format!("Timer error: {}", err))
}

fn get_delay(delay_ms: u64) -> Delay {
    Delay::new(Instant::now() + Duration::from_millis(delay_ms))
}

// TODO: Add macro "drop_on_timeout!" based on try_ready!.
impl Stream for Peer {
    type Item = ();
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        use self::PeerMessage::*;
        use self::PeerState::*;
        loop {
            self.state = match &self.state {
                IncomingConnection(addr) => {
                    match self.stream.as_mut().expect(STATE_ERR).poll().map_err(cbor_err)? {
                        Async::Ready(None) => DroppedPending(addr.clone()),
                        // That is what we were waiting for.
                        Async::Ready(Some(HandShake { id, account_id, peers_info })) => {
                            self.complete_and_handshake();
                            self.merge_peers_info(peers_info);
                            Complete(PeerInfo { id, account_id, addr: addr.clone() })
                        }
                        // Any other message returned by the stream is irrelevant.
                        Async::NotReady | Async::Ready(_) => {
                            try_ready!(self
                                .timeout
                                .as_mut()
                                .expect(STATE_ERR)
                                .poll()
                                .map_err(timer_err));
                            DroppedPending(addr.clone())
                        }
                    }
                }
                UnconnectedBootNode(addr) => {
                    // This state is transitional.
                    self.connect = Some(TcpStream::connect(addr));
                    self.timeout = Some(get_delay(CONNECT_TIMEOUT_MS));
                    ConnectingBootNode(addr.clone())
                }
                ConnectingBootNode(addr) => match self.connect.as_mut().expect(STATE_ERR).poll()? {
                    Async::Ready(socket) => {
                        let (sink, stream) = Framed::new(socket, Codec::new()).split();
                        self.sink = Some(sink);
                        self.stream = Some(stream);
                        self.handshake();
                        self.timeout = Some(get_delay(RESPONSE_HANDSHAKE_TIMEOUT_MS));
                        ConnectedBootNode(addr.clone())
                    }
                    Async::NotReady => {
                        try_ready!(self
                            .timeout
                            .as_mut()
                            .expect(STATE_ERR)
                            .poll()
                            .map_err(timer_err));
                        DroppedPending(addr.clone())
                    }
                },
                ConnectedBootNode { .. } => unimplemented!(),
                UnconnectedGossip { .. } => unimplemented!(),
                ConnectedGossip { .. } => unimplemented!(),
                Complete { .. } => unimplemented!(),
                DroppedPending { .. } => unimplemented!(),
                Dropped { .. } => unimplemented!(),
            };
        }
        unimplemented!()
    }
}
