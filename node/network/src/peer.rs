use crate::all_peers::AllPeers;
use futures::stream::Stream;
use futures::sync::mpsc::{channel, Sender};
use futures::{try_ready, Async, Future, Poll, Sink};
use log::{info, warn};
use parking_lot::RwLock;
use primitives::types::AccountId;
use primitives::types::PeerId;
use serde_derive::{Deserialize, Serialize};
use std::borrow::Borrow;
use std::fmt;
use std::hash::Hash;
use std::hash::Hasher;
use std::io::{Error, ErrorKind};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::codec::Framed;
use tokio::net::tcp::ConnectFuture;
use tokio::net::TcpStream;
use tokio::prelude::stream::SplitStream;
use tokio::timer::Delay;
use tokio_serde_cbor::Codec;

/// How long do we wait for connection to be established.
const CONNECT_TIMEOUT_MS: u64 = 1000;
/// How long do we wait for the initial handshake.
const INIT_HANDSHAKE_TIMEOUT_MS: u64 = 1000;
/// How long to we wait for someone to reply to our handshake with their handshake.
const RESPONSE_HANDSHAKE_TIMEOUT_MS: u64 = 1000;
/// Only happens if we made a mistake in our code and allowed certain optional fields to be None
/// during the states that they are not supposed to be None.
const STATE_ERR: &str = "Some fields are expected to be not None at the given state";

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
        if let Some(acc) = self.account_id.as_ref() {
            write!(f, "({}, {}, {})", self.id, self.addr, acc)
        } else {
            write!(f, "({}, {})", self.id, self.addr)
        }
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
        // Wrapped in `Option` to make it take-able.
        framed_stream: Option<Framed<TcpStream, Codec<PeerMessage, PeerMessage>>>,
        conn_timeout: Delay,
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
    },
    /// The peer was dropped while pending.
    DroppedPending { info: PeerInfo },
    /// The peer was dropped while being usable.
    Dropped { info: PeerInfo },
}

pub struct Peer {
    /// Info of the current node.
    node_info: PeerInfo,
    /// `Peer` object is a state machine. This is its state.
    state: PeerState,
    /// Information on all peers.
    all_peers: Arc<RwLock<AllPeers>>,
}

impl Peer {
    /// Initialize peer from the incoming opened connection.
    pub fn from_incoming_conn(
        node_info: PeerInfo,
        socket: TcpStream,
        all_peers: Arc<RwLock<AllPeers>>,
    ) -> Self {
        let framed_stream = Some(Framed::new(socket, Codec::new()));
        let conn_timeout =
            Delay::new(Instant::now() + Duration::from_millis(INIT_HANDSHAKE_TIMEOUT_MS));
        Self {
            node_info,
            state: PeerState::IncomingConnection { framed_stream, conn_timeout },
            all_peers,
        }
    }

    /// Initialize peer from the either boot node address or gossiped info.
    pub fn from_known(
        node_info: PeerInfo,
        info: PeerInfo,
        all_peers: Arc<RwLock<AllPeers>>,
    ) -> Self {
        // This state is transient.
        let connect = TcpStream::connect(&info.addr);
        let conn_timeout = get_delay(CONNECT_TIMEOUT_MS);
        Self { node_info, state: PeerState::Connecting { info, connect, conn_timeout }, all_peers }
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

/// Constructs `Delay` object from the given delay in ms.
fn get_delay(delay_ms: u64) -> Delay {
    Delay::new(Instant::now() + Duration::from_millis(delay_ms))
}

impl Stream for Peer {
    type Item = (PeerId, Vec<u8>);
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        use self::PeerMessage::*;
        use self::PeerState::*;
        loop {
            self.state = match &mut self.state {
                IncomingConnection { framed_stream, conn_timeout } => {
                    // Wait for the first handshake. Do not forget to reply to it.
                    match framed_stream.as_mut().expect(STATE_ERR).poll().map_err(cbor_err)? {
                        Async::Ready(None) => return Ok(Async::Ready(None)),
                        Async::Ready(Some(Handshake { info, peers_info })) => {
                            // We received a handshake, this peer can be immediately considered
                            // Ready.
                            let mut guard = self.all_peers.write();
                            if !guard.add_incoming_peer(&info) {
                                return Ok(Async::Ready(None));
                            }
                            let (out_msg_tx, stream) = framed_stream_to_channel_with_handshake(
                                &self.node_info,
                                guard.peers_info(),
                                framed_stream.take().expect(STATE_ERR),
                            );
                            guard.merge_peers_info(peers_info).promote_to_ready(&info, &out_msg_tx);
                            info!(target: "network", "{} <= {}", self.node_info, info);
                            Ready { info, stream }
                        }
                        // Any other message returned by the stream is irrelevant.
                        Async::NotReady | Async::Ready(_) => {
                            try_ready!(conn_timeout.poll().map_err(timer_err));
                            // We have not locked it anyway, so no reason to transfer to dropped states.
                            return Ok(Async::Ready(None));
                        }
                    }
                }
                Connecting { info, connect, conn_timeout } => match connect.poll()? {
                    Async::Ready(socket) => {
                        let framed_stream = Framed::new(socket, Codec::new());
                        let (out_msg_tx, stream) = framed_stream_to_channel_with_handshake(
                            &self.node_info,
                            self.all_peers.read().peers_info(),
                            framed_stream,
                        );
                        let hand_timeout = get_delay(RESPONSE_HANDSHAKE_TIMEOUT_MS);
                        Connected {
                            info: info.clone(),
                            stream: Some(stream),
                            out_msg_tx,
                            hand_timeout,
                        }
                    }
                    Async::NotReady => {
                        try_ready!(conn_timeout.poll().map_err(timer_err));
                        // We have not locked this peer yet, because we do not know its info,
                        // because we did not have a successful handshake. Therefore we do not move
                        // it to dropped state.
                        DroppedPending {info: info.clone()}
                    }
                },
                Connected { info, stream, out_msg_tx, hand_timeout } => {
                    // Wait for the handshake reply. Almost equivalent to `IncommingConnection`.
                    match stream.as_mut().expect(STATE_ERR).poll().map_err(cbor_err)? {
                        Async::Ready(None) => return Ok(Async::Ready(None)),
                        Async::Ready(Some(Handshake { info: hand_info, peers_info })) => {
                            if info.id != hand_info.id
                                || info.account_id != hand_info.account_id
                                || info.addr != hand_info.addr
                            {
                                warn!(
                                    target: "network",
                                    "Known info does not match the handshake. Known: {}; Handshake: {} ",
                                    info, &hand_info
                                );
                                DroppedPending {info: info.clone()}
                            } else {
                                self.all_peers
                                    .write()
                                    .merge_peers_info(peers_info)
                                    .promote_to_ready(info, out_msg_tx);
                                info!(target: "network", "{} => {}", self.node_info, info);
                                Ready {
                                    info: info.clone(),
                                    stream: stream.take().expect(STATE_ERR),
                                }
                            }
                        }
                        // Any other message returned by the stream is irrelevant.
                        Async::NotReady | Async::Ready(_) => {
                            try_ready!(hand_timeout.poll().map_err(timer_err));
                            DroppedPending {info: info.clone()}
                        }
                    }
                }
                Ready { info, stream } => match try_ready!(stream.poll().map_err(cbor_err)) {
                    None => Dropped { info: info.clone() },
                    Some(Message(data)) => {
                        return Ok(Async::Ready(Some((info.id.clone(), data))));
                    }
                    Some(InfoGossip(peers_info)) => {
                        self.all_peers.write().merge_peers_info(peers_info);
                        continue;
                    }
                    Some(Handshake { info: hand_info, .. }) => {
                        info!(target: "network", "Unexpected handshake {} from {}", hand_info, info);
                        continue;
                    }
                },
                DroppedPending { info } => {
                    info!(target: "network", "{} =/= {}", self.node_info, info);
                    // Remove peer from lock and close this stream.
                    self.all_peers.write().drop_lock(info);
                    return Ok(Async::Ready(None));
                }
                Dropped { info } => {
                    info!(target: "network", "{} =/= {}", self.node_info, info);
                    // Remove peer from ready and close this stream.
                    self.all_peers.write().drop_ready(info);
                    return Ok(Async::Ready(None));
                }
            };
        }
    }
}
