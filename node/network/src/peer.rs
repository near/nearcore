use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::io::{Error, ErrorKind};
use std::ops::DerefMut;
use std::sync::{Arc, RwLock};
use std::sync::RwLockWriteGuard;
use std::time::{Duration, Instant};
use std::net::SocketAddr;

use futures::{Async, Future, Poll, Sink, try_ready};
use futures::stream::Stream;
use futures::sync::mpsc::{channel, Sender};
use log::{debug, info, warn};
use tokio::codec::Framed;
use tokio::net::tcp::ConnectFuture;
use tokio::net::TcpStream;
use tokio::prelude::stream::SplitStream;
use tokio::timer::Delay;

use primitives::chain::ChainState;
use primitives::network::{
    PeerInfo, Handshake, PeerMessage, PeersInfo, ConnectedInfo
};
use primitives::serialize::Encode;
use primitives::types::PeerId;

use super::message::{Message, PROTOCOL_VERSION};
use super::codec::Codec;

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

/// Note, the peer that establishes the connection is the one that sends the handshake.
pub enum PeerState {
    /// Someone unknown has established connection with us and we are waiting for them to send us
    /// the handshake.
    IncomingConnection {
        stream: Option<Framed<TcpStream, Codec>>,
        hand_timeout: Delay,
        // Whether it should terminate ASAP. We keep this flag in the state to ensure we it is under
        // the same lock as the state.
        evicted: bool,
    },
    /// We know some info about this account, but we have not connected to it.
    Unconnected {
        info: PeerInfo,
        /// When to connect.
        connect_timer: Delay,
        // Whether it should terminate ASAP.
        evicted: bool,
    },
    /// We are attempting to connect to the node.
    Connecting { info: PeerInfo, connect: ConnectFuture, conn_timeout: Delay, evicted: bool },
    /// We connected and sent them the handshake, now we are waiting for the reply.
    Connected {
        info: PeerInfo,
        stream: Option<SplitStream<Framed<TcpStream, Codec>>>,
        out_msg_tx: Sender<PeerMessage>,
        hand_timeout: Delay,
        evicted: bool,
    },
    /// We have performed the handshake exchange and are now ready to exchange other messages.
    Ready {
        info: PeerInfo,
        stream: SplitStream<Framed<TcpStream, Codec>>,
        out_msg_tx: Sender<PeerMessage>,
        evicted: bool,
    },
}

pub type LockedPeerState = Arc<RwLock<PeerState>>;
pub type AllPeerStates = Arc<RwLock<HashMap<PeerId, LockedPeerState>>>;

pub trait ChainStateRetriever: Sized + Send + Clone + 'static {
    fn get_chain_state(&self) -> ChainState;
}

pub struct Peer<T> {
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
    /// Chain state on peer connection.
    chain_state_retriever: T,
}

impl<T: ChainStateRetriever> Peer<T> {
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
        chain_state_retriever: T,
    ) {
        let stream = Some(Framed::new(socket, Codec::new()));
        let hand_timeout = get_delay(INIT_HANDSHAKE_TIMEOUT);
        let state = Arc::new(RwLock::new(PeerState::IncomingConnection {
            stream,
            hand_timeout,
            evicted: false,
        }));
        let peer = Self {
            node_info,
            state,
            all_peer_states,
            inc_msg_tx,
            reconnect_delay,
            chain_state_retriever,
        };
        peer.spawn_peer();
    }

    /// Try spawning peers from known information about them.
    pub fn spawn_from_known(
        node_info: PeerInfo,
        peers_info: PeersInfo,
        all_peer_states: AllPeerStates,
        all_peer_states_guard: &mut RwLockWriteGuard<HashMap<PeerId, LockedPeerState>>,
        inc_msg_tx: Sender<(PeerId, Vec<u8>)>,
        reconnect_delay: Duration,
        // When this node should start connecting itself.
        connect_at: Instant,
        chain_state_retriever: T,
    ) {
        let all_peer_states1 = all_peer_states.clone();
        for info in &peers_info {
            if info.id == node_info.id {
                // We do not want to connect to ourselves.
                continue;
            }
            match all_peer_states_guard.entry(info.id) {
                // This peer is already present.
                Entry::Occupied(_) => continue,
                Entry::Vacant(v) => {
                    let connect_timer = Delay::new(connect_at); // It will initialize itself instantaneously.
                    let state = Arc::new(RwLock::new(PeerState::Unconnected {
                        info: info.clone(),
                        connect_timer,
                        evicted: false,
                    }));
                    v.insert(state.clone());
                    let peer = Self {
                        node_info: node_info.clone(),
                        state,
                        all_peer_states: all_peer_states1.clone(),
                        inc_msg_tx: inc_msg_tx.clone(),
                        reconnect_delay,
                        chain_state_retriever: chain_state_retriever.clone(),
                    };
                    peer.spawn_peer();
                }
            }
        }
    }

    fn on_peer_connected(&self, handshake: Handshake) {
        let data = Encode::encode(&Message::Connected(handshake.connected_info)).unwrap();
        tokio::spawn(
            self.inc_msg_tx
                .clone()
                .send((handshake.peer_id, data))
                .map(|_| ())
                .map_err(|err| warn!("Failed to send message: {}", err)),
        );
    }
}

/// Splits the framed stream, attaches channel to the sink, sends handshake down the sink,
/// returns the channel and the stream for reading incoming messages.
fn framed_stream_to_channel_with_handshake(
    node_info: &PeerInfo,
    peers_info: Vec<PeerInfo>,
    framed_stream: Framed<TcpStream, Codec>,
    chain_state: ChainState,
) -> (Sender<PeerMessage>, SplitStream<Framed<TcpStream, Codec>>) {
    let (sink, stream) = framed_stream.split();
    let (out_msg_tx, out_msg_rx) = channel(1024);
    let handshake = PeerMessage::Handshake(Handshake {
        version: PROTOCOL_VERSION,
        peer_id: node_info.id,
        account_id: node_info.account_id.clone(),
        listen_port: node_info.addr_port(),
        peers_info,
        connected_info: ConnectedInfo { chain_state },
    });
    // Create the task that places the handshake down the channel.
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

/// Converts Timer Error to IO Error.
fn timer_err(err: tokio::timer::Error) -> Error {
    Error::new(ErrorKind::Other, format!("Timer error: {}", err))
}

/// Constructs `Delay` object from the given delay in ms.
fn get_delay(delay: Duration) -> Delay {
    Delay::new(Instant::now() + delay)
}

/// Provides convenience access to the `evicted` flag in the peer.
fn get_evicted_flag(state: &mut PeerState) -> &mut bool {
    use self::PeerState::*;
    match state {
        IncomingConnection { evicted, .. }
        | Unconnected { evicted, .. }
        | Connecting { evicted, .. }
        | Connected { evicted, .. }
        | Ready { evicted, .. } => evicted,
    }
}

/// Provides convenience access to the `peer_info` in the peer state.
pub fn get_peer_info(state: &PeerState) -> Option<&PeerInfo> {
    use self::PeerState::*;
    match state {
        IncomingConnection { .. } => None,
        Unconnected { info, .. }
        | Connecting { info, .. }
        | Connected { info, .. }
        | Ready { info, .. } => Some(info),
    }
}

impl<T: ChainStateRetriever> Stream for Peer<T> {
    type Item = (PeerId, Vec<u8>);
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        use self::PeerMessage::*;
        use self::PeerState::*;
        loop {
            let mut all_peer_states = self.all_peer_states.write().expect(POISONED_LOCK_ERR);
            // Need to compute this before going into match, otherwise dead lock on the one of the states.
            let all_peer_info = all_peer_states.values().filter_map(|state| get_peer_info(state.write().expect(POISONED_LOCK_ERR).deref_mut()).cloned()).collect();
            let mut state_guard = self.state.write().expect(POISONED_LOCK_ERR);
            // First, check the eviction condition.
            if *get_evicted_flag(state_guard.deref_mut()) {
                return Ok(Async::Ready(None));
            }
            // Then do the state machine step.
            *state_guard = match state_guard.deref_mut() {
                IncomingConnection { stream, hand_timeout, .. } => {
                    let peer_addr = stream.as_mut().expect(STATE_ERR).get_ref().peer_addr();
                    match stream.as_mut().expect(STATE_ERR).poll() {
                        // If connection was closed then close the stream
                        Ok(Async::Ready(None)) => {
                            return Ok(Async::Ready(None));
                        }
                        Ok(Async::Ready(Some(Handshake(handshake)))) => {
                            if handshake.peer_id == self.node_info.id {
                                panic!("Received info about itself. Contr-adversarial behavior is not implemented yet.");
                            }
                            match all_peer_states.entry(handshake.peer_id) {
                                // We do not know about this peer. Add it as Ready.
                                Entry::Vacant(entry) => {
                                    // Add it and become ready, see below.
                                    entry.insert(self.state.clone());
                                }
                                // We know this peer already.
                                Entry::Occupied(mut entry) => {
                                    // Check its state.
                                    let entry_clone = entry.get().clone();
                                    let mut entry_guard =
                                        entry_clone.write().expect(POISONED_LOCK_ERR);
                                    match entry_guard.deref_mut() {
                                        old_state @ Unconnected { .. } => {
                                            // It is unconnected, so we take its place, and become ready
                                            // see below.
                                            *get_evicted_flag(old_state) = true;
                                            entry.insert(self.state.clone());
                                        }
                                        // The other connection is already in use, so we close this stream.
                                        Ready { .. } => {
                                            return Ok(Async::Ready(None));
                                        }
                                        // Anything else requires a tie breaker.
                                        old_state => {
                                            if handshake.peer_id < self.node_info.id {
                                                // Keep this connection, take the place of the other
                                                // connection, and become ready, see below.
                                                *get_evicted_flag(old_state) = true;
                                                entry.insert(self.state.clone());
                                            } else {
                                                // Drop this connection.
                                                return Ok(Async::Ready(None));
                                            }
                                        }
                                    };
                                }
                            };
                            self.on_peer_connected(handshake.clone());
                            // Re-insert new entry with updated info.
                            let val = all_peer_states.remove(&handshake.peer_id).unwrap();
                            all_peer_states.insert(handshake.peer_id, val);
                            let addr = if peer_addr.is_ok() { handshake.listen_port.map(|port| SocketAddr::new(peer_addr.unwrap().ip(), port)) } else { None };
                            let info = PeerInfo {
                                id: handshake.peer_id,
                                addr,
                                account_id: handshake.account_id,
                            };
                            let (out_msg_tx, stream) = framed_stream_to_channel_with_handshake(
                                &self.node_info,
                                all_peer_info,
                                stream.take().expect(STATE_ERR),
                                self.chain_state_retriever.get_chain_state(),
                            );
                            Ready { info, stream, out_msg_tx, evicted: false }
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
                Unconnected { info, connect_timer, .. } => {
                    try_ready!(connect_timer.poll().map_err(timer_err));
                    // TODO: add other state for peers who don't have open port, to not keep trying to connect to them.
                    if info.addr.is_some() {
                        let connect = TcpStream::connect(&info.addr.unwrap());
                        let conn_timeout = get_delay(CONNECT_TIMEOUT);
                        Connecting { info: info.clone(), connect, conn_timeout, evicted: false }
                    } else {
                        Unconnected { info: info.clone(), connect_timer: get_delay(self.reconnect_delay * 10), evicted: false }
                    }
                }
                Connecting { info, connect, conn_timeout, .. } => match connect.poll() {
                    Ok(Async::Ready(socket)) => {
                        let framed_stream = Framed::new(socket, Codec::new());
                        let (out_msg_tx, stream) = framed_stream_to_channel_with_handshake(
                            &self.node_info,
                            all_peer_info,
                            framed_stream,
                            self.chain_state_retriever.get_chain_state(),
                        );
                        let hand_timeout = get_delay(RESPONSE_HANDSHAKE_TIMEOUT);
                        Connected {
                            info: info.clone(),
                            stream: Some(stream),
                            out_msg_tx,
                            hand_timeout,
                            evicted: false,
                        }
                    }
                    Ok(Async::NotReady) => {
                        try_ready!(conn_timeout.poll().map_err(timer_err));
                        // We have not locked this peer yet, because we do not know its info,
                        // because we did not have a successful handshake. Try again later.
                        Unconnected {
                            info: info.clone(),
                            connect_timer: get_delay(self.reconnect_delay),
                            evicted: false,
                        }
                    }
                    // Connection returned error. Should try again later.
                    Err(e) => {
                        debug!(target: "network", "Failed to connect to a known peer {}", e);
                        Unconnected {
                            info: info.clone(),
                            connect_timer: get_delay(self.reconnect_delay),
                            evicted: false,
                        }
                    }
                },
                Connected { info, stream, out_msg_tx, hand_timeout, .. } =>
                // Wait for the handshake reply.
                {
                    match stream.as_mut().expect(STATE_ERR).poll() {
                        // The connection was closed. Try again later.
                        Ok(Async::Ready(None)) => Unconnected {
                            info: info.clone(),
                            connect_timer: get_delay(self.reconnect_delay),
                            evicted: false,
                        },
                        Ok(Async::Ready(Some(Handshake(handshake)))) => {
                            // TODO: make sure this condition is correct for nodes that reconnect from different IP addresses.
                            if info.id != handshake.peer_id {
                                // Known info does not match the handshake. Try again later with
                                // the new info.
                                Unconnected {
                                    info: PeerInfo { id: handshake.peer_id, addr: None, account_id: handshake.account_id},
                                    connect_timer: get_delay(self.reconnect_delay),
                                    evicted: false,
                                }
                            } else {
                                if info.account_id != handshake.account_id {
                                    info.account_id = handshake.account_id.clone();
                                }
                                self.on_peer_connected(handshake);
                                Ready {
                                    info: info.clone(),
                                    stream: stream.take().expect(STATE_ERR),
                                    out_msg_tx: out_msg_tx.clone(),
                                    evicted: false,
                                }
                            }
                        }
                        // Any other message returned by the stream is irrelevant.
                        Ok(Async::NotReady) | Ok(Async::Ready(_)) => {
                            try_ready!(hand_timeout.poll().map_err(timer_err));
                            Unconnected {
                                info: info.clone(),
                                connect_timer: get_delay(self.reconnect_delay),
                                evicted: false,
                            }
                        }
                        Err(e) => {
                            warn!(target: "network", "Error while trying to get a handshake {}", e);
                            try_ready!(hand_timeout.poll().map_err(timer_err));
                            Unconnected {
                                info: info.clone(),
                                connect_timer: get_delay(self.reconnect_delay),
                                evicted: false,
                            }
                        }
                    }
                }
                Ready { info, stream, .. } => match stream.poll() {
                    // Connection was closed. Reconnect later.
                    Ok(Async::Ready(None)) => Unconnected {
                        info: info.clone(),
                        connect_timer: get_delay(self.reconnect_delay),
                        evicted: false,
                    },
                    // Actual message transmitted over the network.
                    Ok(Async::Ready(Some(Message(data)))) => {
                        return Ok(Async::Ready(Some((info.id, data))));
                    }
                    Ok(Async::Ready(Some(InfoGossip(peers_info)))) => {
                        Self::spawn_from_known(
                            self.node_info.clone(),
                            peers_info,
                            self.all_peer_states.clone(),
                            &mut all_peer_states,
                            self.inc_msg_tx.clone(),
                            self.reconnect_delay,
                            Instant::now() + self.reconnect_delay,
                            self.chain_state_retriever.clone(),
                        );
                        continue;
                    }
                    Ok(Async::Ready(Some(Handshake(handshake)))) => {
                        info!(target: "network", "Unexpected handshake {:?} from {}", handshake, info);
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
