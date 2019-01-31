use futures::stream::Stream;
use futures::stream::StreamFuture;
use futures::sync::mpsc::{channel, Sender};
use futures::{try_ready, Async, Future, Poll, Sink};
use log::{info, warn};
use primitives::types::AccountId;
use primitives::types::PeerId;
use serde_derive::{Deserialize, Serialize};
use std::borrow::Borrow;
use std::sync::RwLock;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt;
use std::hash::Hash;
use std::hash::Hasher;
use std::io::{Error, ErrorKind};
use std::net::SocketAddr;
use std::ops::Deref;
use std::ops::DerefMut;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::codec::Framed;
use tokio::net::tcp::ConnectFuture;
use tokio::net::TcpStream;
use tokio::prelude::stream::SplitStream;
use tokio::timer::Delay;
use tokio::timer::Timeout;
use tokio::util::FutureExt;
use tokio_serde_cbor::Codec;

/// How long do we wait for connection to be established.
const CONNECT_TIMEOUT: Duration = Duration::from_millis(1000);
/// How long do we wait for the initial handshake.
const INIT_HANDSHAKE_TIMEOUT: Duration = Duration::from_millis(1000);
/// How long to we wait for someone to reply to our handshake with their handshake.
const RESPONSE_HANDSHAKE_TIMEOUT: Duration = Duration::from_millis(1000);
/// Only happens if we made a mistake in our code and allowed certain optional fields to be None
/// during the states that they are not supposed to be None.
const STATE_ERR: &str = "Some fields are expected to be not None at the given state";
const POISONED_LOCK_ERR: &str = "The lock was poisoned.";

#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
pub enum PeerMessage {
    Handshake { info: PeerInfo, peers_info: PeersInfo },
    InfoGossip(PeersInfo),
    Message(Vec<u8>),
}

pub type PeersInfo = Vec<PeerInfo>;

/// Info about the peer. If peer is an authority then we also know its account id.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PeerInfo {
    pub id: PeerId,
    pub addr: SocketAddr,
    pub account_id: Option<AccountId>,
}

impl PartialEq for PeerInfo {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Hash for PeerInfo {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl Eq for PeerInfo {}

impl fmt::Display for PeerInfo {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.addr.port())
        //        if let Some(acc) = self.account_id.as_ref() {
        //            write!(f, "({}, {}, {})", self.id, self.addr, acc)
        //        } else {
        //            write!(f, "({}, {})", self.id, self.addr)
        //        }
    }
}

impl Borrow<PeerId> for PeerInfo {
    fn borrow(&self) -> &PeerId {
        &self.id
    }
}

/// Note, the peer that establishes the connection is the one that sends the handshake.
pub enum PeerState {
    /// Someone unknown has established connection with us and we are waiting for them to send us
    /// the handshake.
    IncomingConnection {
        stream: Option<Framed<TcpStream, Codec<PeerMessage, PeerMessage>>>,
        hand_timeout: Delay,
    },
    Unconnected {
        info: PeerInfo,
        /// When to connect.
        connect_timer: Delay,
    },
    /// We are attempting to connect to the node.
    Connecting { info: PeerInfo, connect: ConnectFuture, conn_timeout: Delay },
    /// We connected and sent them the handshake, now we are waiting for the reply.
    Connected {
        info: PeerInfo,
        stream: Option<SplitStream<Framed<TcpStream, Codec<PeerMessage, PeerMessage>>>>,
        out_msg_tx: Sender<PeerMessage>,
        hand_timeout: Delay,
    },
    /// We have performed the handshake exchange and are now ready to exchange other messages.
    Ready {
        info: PeerInfo,
        stream: SplitStream<Framed<TcpStream, Codec<PeerMessage, PeerMessage>>>,
        out_msg_tx: Sender<PeerMessage>,
    },
}

pub type LockedPeerState = Arc<RwLock<PeerState>>;
pub type AllPeerStates = Arc<RwLock<HashMap<PeerInfo, LockedPeerState>>>;

pub struct Peer {
    /// Info of the current node.
    node_info: PeerInfo,
    /// `Peer` object is a state machine. This is its state.
    state: LockedPeerState,
    /// Information on all peers.
    all_peer_states: AllPeerStates,
    /// Channel where the peer places incoming messages.
    inc_msg_tx: Sender<(PeerId, Vec<u8>)>,
    /// How long do we wait before reconnecting to the peer.
    reconnect_delay: Duration,
}

impl Peer {
    fn spawn_peer(self) {
        let inc_msg_tx = self.inc_msg_tx.clone();
        tokio::spawn(
            self.map_err(|e| warn!(target: "network", "Error receiving message: {}", e))
                .forward(inc_msg_tx.sink_map_err(
                    |e| warn!(target: "network", "Error forwarding incoming messages: {}", e),
                ))
                .map(|_| ()),
        );
    }

    /// Spawn peer from incoming connection.
    pub fn spawn_incoming_conn(
        node_info: PeerInfo,
        socket: TcpStream,
        all_peer_states: AllPeerStates,
        inc_msg_tx: Sender<(PeerId, Vec<u8>)>,
        reconnect_delay: Duration,
    ) {
        let stream = Some(Framed::new(socket, Codec::new()));
        let hand_timeout = get_delay(INIT_HANDSHAKE_TIMEOUT);
        let state = Arc::new(RwLock::new(PeerState::IncomingConnection { stream, hand_timeout }));
        let peer = Self { node_info, state, all_peer_states, inc_msg_tx, reconnect_delay };
        peer.spawn_peer();
    }

    /// Try spawning peers from known information about them.
    pub fn spawn_from_known(
        node_info: PeerInfo,
        peers_info: PeersInfo,
        all_peer_states: AllPeerStates,
        inc_msg_tx: Sender<(PeerId, Vec<u8>)>,
        reconnect_delay: Duration,
        // When this node should start connecting itself.
        connect_at: Instant,
    ) {
        let all_peer_states1 = all_peer_states.clone();
        let mut guard = all_peer_states.write().expect(POISONED_LOCK_ERR);
        for info in &peers_info {
            if info == &node_info {
                // We do not want to connect to ourselves.
                continue;
            }
            match guard.entry(info.clone()) {
                // This peer is already present.
                Entry::Occupied(_) => continue,
                Entry::Vacant(v) => {
                    let connect_timer = Delay::new(connect_at); // It will initialize itself instantaneously.
                    let state = Arc::new(RwLock::new(PeerState::Unconnected {
                        info: info.clone(),
                        connect_timer,
                    }));
                    v.insert(state.clone());
                    let peer = Self {
                        node_info: node_info.clone(),
                        state,
                        all_peer_states: all_peer_states1.clone(),
                        inc_msg_tx: inc_msg_tx.clone(),
                        reconnect_delay: reconnect_delay.clone(),
                    };
                    peer.spawn_peer();
                }
            }
        }
    }
}

/// Splits the framed stream, attaches channel to the sink, sends handshake down the sink,
/// returns the channel and the stream for reading incoming messages.
fn framed_stream_to_channel_with_handshake(
    node_info: &PeerInfo,
    peers_info: Vec<PeerInfo>,
    framed_stream: Framed<TcpStream, Codec<PeerMessage, PeerMessage>>,
) -> (Sender<PeerMessage>, SplitStream<Framed<TcpStream, Codec<PeerMessage, PeerMessage>>>) {
    let (sink, stream) = framed_stream.split();
    let (out_msg_tx, out_msg_rx) = channel(1024);
    // Create the task that places the handshake down the channel.
    let handshake = PeerMessage::Handshake { info: node_info.clone(), peers_info };
    let hand_task = out_msg_tx
        .clone()
        .send(handshake)
        .map(|_| ())
        .map_err(|e| warn!(target: "network", "Error sending handshake {}", e));
    let fwd_task = out_msg_rx
        .forward(sink.sink_map_err(|e| {
            warn!(
            target: "network",
            "Error forwarding outgoing messages to the TcpStream sink: {}", e)
        }))
        .map(|_| ());
    tokio::spawn(hand_task.then(|_| fwd_task));
    (out_msg_tx, stream)
}

/// Converts CBOR Error to IO Error.
fn cbor_err(err: tokio_serde_cbor::Error) -> Error {
    Error::new(ErrorKind::InvalidData, format!("Error decoding message: {}", err))
}

/// Converts Timer Error to IO Error.
fn timer_err(err: tokio::timer::Error) -> Error {
    Error::new(ErrorKind::Other, format!("Timer error: {}", err))
}

fn timeout_err<E>(err: E) -> Error {
    Error::new(ErrorKind::TimedOut, "Timed out")
}

/// Constructs `Delay` object from the given delay in ms.
fn get_delay(delay: Duration) -> Delay {
    Delay::new(Instant::now() + delay)
}

impl Stream for Peer {
    type Item = (PeerId, Vec<u8>);
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        use self::PeerMessage::*;
        use self::PeerState::*;
        loop {
            let mut state_guard = self.state.write().expect(POISONED_LOCK_ERR);
            *state_guard = match state_guard.deref_mut() {
                IncomingConnection { stream, hand_timeout } => {
                    match stream.as_mut().expect(STATE_ERR).poll() {
                        // If connection was closed then close the stream
                        Ok(Async::Ready(None)) => return Ok(Async::Ready(None)),
                        Ok(Async::Ready(Some(Handshake { info, peers_info }))) => {
                            let mut all_peer_states = self.all_peer_states.write().expect(POISONED_LOCK_ERR);
                            match all_peer_states.entry(info.clone()) {
                                // We do not know about this peer. Add it as Ready.
                                Entry::Vacant(entry) => {
                                    // Add it and become ready, see below.
                                    entry.insert(self.state.clone());
                                }
                                // We know this peer already.
                                Entry::Occupied(mut entry) => {
                                    // Check its state.
                                    let other_state = entry.get().clone();
                                    match other_state.read().expect(POISONED_LOCK_ERR).deref() {
                                        Unconnected { .. } => {
                                            // It is unconnected, so we take its place, and become ready
                                            // see below.
                                            entry.insert(self.state.clone());
                                        }
                                        // The other connection is already in use, so we close this stream.
                                        Ready { .. } => return Ok(Async::Ready(None)),
                                        // Anything else requires a tie breaker.
                                        _ => {
                                            if info.id < self.node_info.id {
                                                // Keep this connection, take the place of the other
                                                // connection, and become ready, see below.
                                                entry.insert(self.state.clone());
                                            } else {
                                                // Drop this connection.
                                                return Ok(Async::Ready(None));
                                            }
                                        }
                                    };
                                }
                            };
                            let (out_msg_tx, stream) = framed_stream_to_channel_with_handshake(
                                &self.node_info,
                                all_peer_states.keys().cloned().collect(),
                                stream.take().expect(STATE_ERR),
                            );
                            Ready { info, stream, out_msg_tx }
                        }
                        // If error was received then log it and continue.
                        Err(e) => {
                            warn!(target: "network", "Error receiving data from incoming connection {}", e);
                            continue;
                        }
                        // If it is not ready yet or some other message was received then check for
                        // the timer.
                        Ok(Async::NotReady) | Ok(Async::Ready(Some(_))) => {
                            try_ready!(hand_timeout.poll().map_err(timer_err));
                            // Timer has expired, close the stream.
                            return Ok(Async::Ready(None));
                        }
                    }
                }
                Unconnected { info, connect_timer } => {
                    try_ready!(connect_timer.poll().map_err(timer_err));
                    let connect = TcpStream::connect(&info.addr);
                    let conn_timeout = get_delay(CONNECT_TIMEOUT);
                    Connecting { info: info.clone(), connect, conn_timeout }
                }
                Connecting { info, connect, conn_timeout } => match connect.poll() {
                    Ok(Async::Ready(socket)) => {
                        let framed_stream = Framed::new(socket, Codec::new());
                        let (out_msg_tx, stream) = framed_stream_to_channel_with_handshake(
                            &self.node_info,
                            self.all_peer_states.read().expect(POISONED_LOCK_ERR).keys().cloned().collect(),
                            framed_stream,
                        );
                        let hand_timeout = get_delay(RESPONSE_HANDSHAKE_TIMEOUT);
                        Connected {
                            info: info.clone(),
                            stream: Some(stream),
                            out_msg_tx,
                            hand_timeout,
                        }
                    }
                    Ok(Async::NotReady) => {
                        try_ready!(conn_timeout.poll().map_err(timer_err));
                        // We have not locked this peer yet, because we do not know its info,
                        // because we did not have a successful handshake. Try again later.
                        Unconnected {
                            info: info.clone(),
                            connect_timer: get_delay(self.reconnect_delay),
                        }
                    }
                    // Connection returned error. Should try again later.
                    Err(e) => Unconnected {
                        info: info.clone(),
                        connect_timer: get_delay(self.reconnect_delay),
                    },
                },
                Connected { info, stream, out_msg_tx, hand_timeout } =>
                // Wait for the handshake reply.
                {
                    match stream.as_mut().expect(STATE_ERR).poll().map_err(cbor_err) {
                        // The connection was closed. Try again later.
                        Ok(Async::Ready(None)) => Unconnected {
                            info: info.clone(),
                            connect_timer: get_delay(self.reconnect_delay),
                        },
                        Ok(Async::Ready(Some(Handshake { info: hand_info, peers_info }))) => {
                            if info.id != hand_info.id
                                || info.account_id != hand_info.account_id
                                || info.addr != hand_info.addr
                            {
                                // Known info does not match the handshake. Try again later with
                                // the new info.
                                Unconnected {
                                    info: hand_info,
                                    connect_timer: get_delay(self.reconnect_delay),
                                }
                            } else {
                                Ready {
                                    info: info.clone(),
                                    stream: stream.take().expect(STATE_ERR),
                                    out_msg_tx: out_msg_tx.clone(),
                                }
                            }
                        }
                        // Any other message returned by the stream is irrelevant.
                        Ok(Async::NotReady) | Ok(Async::Ready(_)) => {
                            try_ready!(hand_timeout.poll().map_err(timer_err));
                            Unconnected {
                                info: info.clone(),
                                connect_timer: get_delay(self.reconnect_delay),
                            }
                        }
                        Err(e) => {
                            warn!(target: "network", "Error while trying to get a handshake {}", e);
                            try_ready!(hand_timeout.poll().map_err(timer_err));
                            Unconnected {
                                info: info.clone(),
                                connect_timer: get_delay(self.reconnect_delay),
                            }
                        }
                    }
                }
                Ready { info, stream, out_msg_tx } => match stream.poll().map_err(cbor_err) {
                    // Connection was closed. Reconnect later.
                    Ok(Async::Ready(None)) => Unconnected {
                        info: info.clone(),
                        connect_timer: get_delay(self.reconnect_delay),
                    },
                    // Actual message transmitted over the network.
                    Ok(Async::Ready(Some(Message(data)))) => {
                        return Ok(Async::Ready(Some((info.id.clone(), data))));
                    }
                    Ok(Async::Ready(Some(InfoGossip(peers_info)))) => {
                        Self::spawn_from_known(
                            self.node_info.clone(),
                            peers_info,
                            self.all_peer_states.clone(),
                            self.inc_msg_tx.clone(),
                            self.reconnect_delay.clone(),
                            Instant::now() + self.reconnect_delay.clone(),
                        );
                        continue;
                    }
                    Ok(Async::Ready(Some(Handshake { info: hand_info, .. }))) => {
                        info!(target: "network", "Unexpected handshake {} from {}", hand_info, info);
                        continue;
                    }
                    Err(e) => {
                        warn!(target: "network", "Error while communicating with Ready peer {}", e);
                        continue;
                    }
                    Ok(Async::NotReady) => {
                        return Ok(Async::NotReady);
                    }
                },
            };
        }
    }
}
