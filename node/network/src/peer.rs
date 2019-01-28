use crate::all_peers::AllPeers;
use futures::stream::Stream;
use futures::sync::mpsc::{channel, Receiver, Sender};
use futures::{try_ready, Async, Future, Poll, Sink};
use log::{info, warn};
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
/// Only happens if we made a mistake in our code and allowed certain optional fields to be None
/// during the states that they are not supposed to be None.
const STATE_ERR: &str = "Some fields are expected to be not None at the given state";

#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
pub enum PeerMessage {
    Handshake { id: PeerId, account_id: Option<AccountId>, peers_info: PeersInfo },
    InfoGossip(PeersInfo),
    Message(Vec<u8>),
}

/// Info about the peer. If peer is an authority then we also know its account id.
#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
pub struct PeerInfo {
    pub id: Option<PeerId>,
    pub addr: SocketAddr,
    pub account_id: Option<AccountId>,
}

pub type PeersInfo = Vec<PeerInfo>;

/// Note, the peer that establishes the connection is the one that sends the handshake.
pub enum PeerState {
    /// Someone unknown has established connection with us and we are waiting for them to send us
    /// the handshake. `stream`, `out_msg_tx`, and `timeout` are not `None`.
    IncomingConnection(SocketAddr),
    /// We know the `PeerInfo` (if it is a boot node, only addr), but have not attempted to connect
    /// to it, yet.
    Unconnected(PeerInfo),
    /// We are attempting to connect to the node. `connect` and `timeout` are not `None`.
    Connecting(PeerInfo),
    /// We connected to the and sent them the handshake, now we are waiting for the reply.
    /// `stream`, `sink`, and `timeout` are not `None`.
    Connected(PeerInfo),
    /// We have performed the handshake exchange and are now ready to exchange other messages.
    /// We have attached the channel to the sink and spawned the forwarding task.
    /// `stream` is not `None`.
    Ready(PeerInfo),
    /// The peer was dropped while pending.
    DroppedPending(SocketAddr),
    /// The peer was dropped while being usable.
    Dropped(PeerInfo),
}

pub struct Peer {
    /// `PeerId` of the current node.
    node_id: PeerId,
    /// `AccountId` of the current node.
    node_account_id: Option<AccountId>,
    /// `Peer` object is a state machine. This is its state.
    state: PeerState,
    /// Information on all peers.
    all_peers: Arc<RwLock<AllPeers>>,

    /// In case when someone establishes with us an unsolicited connection, all we have initially
    /// is the connection object.
    connect: Option<ConnectFuture>,
    /// Stream from which we forward messages, ignoring `Handshake` and `InfoGossip`.
    stream: Option<SplitStream<Framed<TcpStream, Codec<PeerMessage, PeerMessage>>>>,
    /// Clonable sender into which other parts our infra can put the messages that should be sent
    /// over the network.
    out_msg_tx: Option<Sender<PeerMessage>>,
    /// Objects for storing timeout. We need several timeouts for the peer, but only one at a time,
    /// so we use one timeout object only.
    timeout: Option<Delay>,
}

impl Peer {
    /// Initialize peer from the incoming opened connection.
    pub fn from_incoming_conn(
        node_id: PeerId,
        node_account_id: Option<AccountId>,
        socket: TcpStream,
        all_peers: Arc<RwLock<AllPeers>>,
    ) -> Result<Self, Error> {
        let addr = socket.peer_addr()?;
        if !all_peers.write().add_incoming_peer(&PeerInfo {
            id: None,
            account_id: None,
            addr: addr.clone(),
        }) {
            return Err(Error::new(
                ErrorKind::AddrInUse,
                format!("Address {} is already locked.", addr),
            ));
        }
        let (sink, stream) = Framed::new(socket, Codec::new()).split();
        let timeout = Delay::new(Instant::now() + Duration::from_millis(INIT_HANDSHAKE_TIMEOUT_MS));
        Ok(Self {
            node_id,
            node_account_id,
            state: PeerState::IncomingConnection(addr.clone()),
            all_peers,
            connect: None,
            stream: Some(stream),
            out_msg_tx: Some(spawn_sink_task(sink)),
            timeout: Some(timeout),
        })
    }

    /// Initialize peer from the either boot node address or gossiped info.
    pub fn from_known(
        info: PeerInfo,
        node_id: PeerId,
        node_account_id: Option<AccountId>,
        all_peers: Arc<RwLock<AllPeers>>,
    ) -> Self {
        let addr = info.addr.clone();
        Self {
            node_id,
            node_account_id,
            state: PeerState::Unconnected(info),
            all_peers,
            connect: None,
            stream: None,
            out_msg_tx: None,
            timeout: None,
        }
    }

    /// Spawns a task that handshakes with the peer.
    fn handshake(&self) {
        let task = self
            .out_msg_tx
            .as_ref()
            .expect(STATE_ERR)
            .clone()
            .send(PeerMessage::Handshake {
                id: self.node_id,
                account_id: self.node_account_id.clone(),
                peers_info: self.all_peers.read().peers_info(),
            })
            .map(|_| ())
            .map_err(|e| warn!(target: "network", "Error sending handshake to the peer. {}", e));
        tokio::spawn(task);
    }
}

/// Task that forwards messages from multiple senders into the sink.
fn spawn_sink_task(
    sink: SplitSink<Framed<TcpStream, Codec<PeerMessage, PeerMessage>>>,
) -> Sender<PeerMessage> {
    let (out_msg_tx, out_msg_rx) = channel(1024);
    let task = out_msg_rx
        .forward(sink.sink_map_err(|e| {
            warn!(
            target: "network",
            "Error forwarding outgoing messages to the TcpStream sink: {}", e)
        }))
        .map(|_| ());
    tokio::spawn(task);
    out_msg_tx
}

/// Converts CBOR Error to IO Error.
fn cbor_err(err: tokio_serde_cbor::Error) -> Error {
    Error::new(ErrorKind::InvalidData, format!("Error decoding message: {}", err))
}

/// Converts Timer Error to IO Error.
fn timer_err(err: tokio::timer::Error) -> Error {
    Error::new(ErrorKind::Other, format!("Timer error: {}", err))
}

/// Constructs `Delay` object from the given delay in ms.
fn get_delay(delay_ms: u64) -> Delay {
    Delay::new(Instant::now() + Duration::from_millis(delay_ms))
}

/// If timeout is reached then return `DroppedPending`, otherwise return `Async::NotReady`.
macro_rules! drop_on_timeout {
    ($self_:ident, $addr:expr) => {{
        try_ready!($self_.timeout.as_mut().expect(STATE_ERR).poll().map_err(timer_err));
        DroppedPending($addr.clone())
    }};
}

macro_rules! poll_stream {
    ($self_:ident) => {{
        $self_.stream.as_mut().expect(STATE_ERR).poll().map_err(cbor_err)
    }};
}

impl Stream for Peer {
    type Item = (PeerId, Vec<u8>);
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        use self::PeerMessage::*;
        use self::PeerState::*;
        loop {
            self.state = match &self.state {
                IncomingConnection(addr) => {
                    // Wait for the first handshake. Do not forget to reply to it.
                    match poll_stream!(self)? {
                        Async::Ready(None) => DroppedPending(addr.clone()),
                        Async::Ready(Some(Handshake { id, account_id, peers_info })) => {
                            self.handshake();
                            let info = PeerInfo { id: Some(id), account_id, addr: addr.clone() };
                            self.all_peers.write().merge_peers_info(peers_info).promote_to_ready(
                                &info,
                                self.out_msg_tx.as_ref().expect(STATE_ERR),
                            );
                            info!(target: "network", "Ready {:?}", &info);
                            Ready(info)
                        }
                        // Any other message returned by the stream is irrelevant.
                        Async::NotReady | Async::Ready(_) => drop_on_timeout!(self, addr),
                    }
                }
                Unconnected(info) => {
                    info!(target: "network", "Unconnected {:?}", &info);
                    // This state is transient.
                    self.connect = Some(TcpStream::connect(&info.addr));
                    self.timeout = Some(get_delay(CONNECT_TIMEOUT_MS));
                    info!(target: "network", "Connecting {:?}", &info);
                    Connecting(info.clone())
                }
                Connecting(info) => match self.connect.as_mut().expect(STATE_ERR).poll()? {
                    Async::Ready(socket) => {
                        let (sink, stream) = Framed::new(socket, Codec::new()).split();
                        self.stream = Some(stream);
                        self.timeout = Some(get_delay(RESPONSE_HANDSHAKE_TIMEOUT_MS));
                        self.out_msg_tx = Some(spawn_sink_task(sink));
                        self.handshake();
                        info!(target: "network", "Connected {:?}", &info);
                        Connected(info.clone())
                    }
                    Async::NotReady => drop_on_timeout!(self, info.addr),
                },
                Connected(info) => {
                    // Wait for the handshake reply. Almost equivalent to `IncommingConnection`.
                    match poll_stream!(self)? {
                        Async::Ready(None) => DroppedPending(info.addr.clone()),
                        Async::Ready(Some(Handshake { id, account_id, peers_info })) => {
                            if info.id != Some(id) || info.account_id != account_id {
                                warn!(
                                    target: "network",
                                    "Known info does not match the handshake. Known: {:?}; Handshake: {:?} ",
                                    info, (&id, &account_id)
                                );
                            }
                            let info =
                                PeerInfo { id: Some(id), addr: info.addr.clone(), account_id };
                            self.all_peers.write().merge_peers_info(peers_info).promote_to_ready(
                                &info,
                                self.out_msg_tx.as_ref().expect(STATE_ERR),
                            );
                            info!(target: "network", "Ready {:?}", &info);
                            Ready(info)
                        }
                        // Any other message returned by the stream is irrelevant.
                        Async::NotReady | Async::Ready(_) => drop_on_timeout!(self, info.addr),
                    }
                }
                Ready(info) => match try_ready!(poll_stream!(self)) {
                    None => Dropped(info.clone()),
                    Some(Message(data)) => {
                        let id = info.id.as_ref().expect(STATE_ERR).clone();
                        return Ok(Async::Ready(Some((id, data))));
                    }
                    Some(InfoGossip(peers_info)) => {
                        self.all_peers.write().merge_peers_info(peers_info);
                        continue;
                    }
                    Some(h @ Handshake { .. }) => {
                        info!(target: "network", "Unexpected handshake {:?} from {:?}", h, info);
                        continue;
                    }
                },
                DroppedPending(addr) => {
                    info!(target: "network", "DroppedPending {}", &addr);
                    // Remove peer from lock and close this stream.
                    self.all_peers.write().drop_lock(addr);
                    return Ok(Async::Ready(None));
                }
                Dropped(info) => {
                    info!(target: "network", "Dropped {:?}", &info);
                    // Remove peer from ready and close this stream.
                    self.all_peers.write().drop_ready(info);
                    return Ok(Async::Ready(None));
                }
            };
        }
    }
}
