use crate::network_protocol::{
    Encoding, Handshake, HandshakeFailureReason, PartialEdgeInfo, PeerChainInfoV2, PeerIdOrHash,
    PeerMessage, Ping, RawRoutedMessage, RoutedMessageBody,
};
use crate::types::StateResponseInfo;
use bytes::buf::{Buf, BufMut};
use bytes::BytesMut;
use near_crypto::{KeyType, SecretKey};
use near_primitives::block::GenesisId;
use near_primitives::hash::CryptoHash;
use near_primitives::network::PeerId;
use near_primitives::time::{Duration, Instant, Utc};
use near_primitives::types::{AccountId, BlockHeight, EpochId, ShardId};
use near_primitives::version::{ProtocolVersion, PROTOCOL_VERSION};
use std::io;
use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Represents a connection to a peer, and provides only minimal functionality.
/// Almost none of the usual NEAR network logic is implemented, and the caller
/// will receive messages via the recv() function. Currently the only message
/// you can send is a Ping message, but in the future we can add more stuff there
/// (e.g. sending blocks/chunk parts/etc)
pub struct Connection {
    my_peer_id: PeerId,
    peer_id: PeerId,
    secret_key: SecretKey,
    stream: TcpStream,
    buf: BytesMut,
    recv_timeout: Duration,
}

/// The types of messages it's possible to receive from a `Peer`. Any PeerMessage
/// we receive that doesn't fit one of these will just be logged and dropped.
#[derive(Debug)]
pub enum ReceivedMessage {
    AnnounceAccounts(Vec<(AccountId, PeerId, EpochId)>),
    Pong { nonce: u64, source: PeerId },
    VersionedStateResponse(StateResponseInfo),
}

impl TryFrom<PeerMessage> for ReceivedMessage {
    // the only possible error here is that it's not implemented
    type Error = ();

    fn try_from(m: PeerMessage) -> Result<Self, Self::Error> {
        match m {
            PeerMessage::Routed(r) => match &r.body {
                RoutedMessageBody::Pong(p) => {
                    Ok(Self::Pong { nonce: p.nonce, source: p.source.clone() })
                }
                RoutedMessageBody::VersionedStateResponse(state_response_info) => {
                    Ok(Self::VersionedStateResponse(state_response_info.clone()))
                }
                _ => Err(()),
            },
            PeerMessage::SyncRoutingTable(r) => Ok(Self::AnnounceAccounts(
                r.accounts.into_iter().map(|a| (a.account_id, a.peer_id, a.epoch_id)).collect(),
            )),
            _ => Err(()),
        }
    }
}

impl std::fmt::Debug for Connection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        let addr = match self.stream.peer_addr() {
            Ok(a) => format!("{:?}", a),
            Err(e) => format!("Err({:?})", e),
        };
        write!(f, "{:?}@{:?}", &self.peer_id, addr)
    }
}

#[derive(thiserror::Error, Debug)]
pub enum ConnectError {
    #[error(transparent)]
    IO(std::io::Error),
    #[error("handshake failed {0:?}")]
    HandshakeFailure(HandshakeFailureReason),
    #[error("received unexpected message before the handshake: {0:?}")]
    UnexpectedFirstMessage(PeerMessage),
}

impl Connection {
    /// Connect to the NEAR node at `peer_id`@`addr`. The inputs are used to build out handshake,
    /// and this function will return a `Peer` when a handshake has been received successfully.
    pub async fn connect(
        addr: SocketAddr,
        peer_id: PeerId,
        my_protocol_version: Option<ProtocolVersion>,
        chain_id: &str,
        genesis_hash: CryptoHash,
        head_height: BlockHeight,
        recv_timeout: Duration,
    ) -> Result<Self, ConnectError> {
        let secret_key = SecretKey::from_random(KeyType::ED25519);
        let my_peer_id = PeerId::new(secret_key.public_key());

        let start = Instant::now();
        let stream =
            tokio::time::timeout(recv_timeout.try_into().unwrap(), TcpStream::connect(addr))
                .await
                .map_err(|e| ConnectError::IO(e.into()))?
                .map_err(ConnectError::IO)?;
        tracing::info!(
            target: "network", "Connection to {}@{:?} established. latency: {}",
            &peer_id, &addr, start.elapsed(),
        );

        let mut peer = Self {
            stream,
            peer_id,
            buf: BytesMut::with_capacity(1024),
            secret_key,
            my_peer_id,
            recv_timeout,
        };
        peer.do_handshake(
            my_protocol_version.unwrap_or(PROTOCOL_VERSION),
            chain_id,
            genesis_hash,
            head_height,
        )
        .await?;

        Ok(peer)
    }

    async fn do_handshake(
        &mut self,
        protocol_version: ProtocolVersion,
        chain_id: &str,
        genesis_hash: CryptoHash,
        head_height: BlockHeight,
    ) -> Result<(), ConnectError> {
        let handshake = PeerMessage::Tier2Handshake(Handshake {
            protocol_version,
            oldest_supported_version: protocol_version - 2,
            sender_peer_id: self.my_peer_id.clone(),
            target_peer_id: self.peer_id.clone(),
            // we have to set this even if we have no intention of listening since otherwise
            // the peer will drop our connection
            sender_listen_port: Some(24567),
            sender_chain_info: PeerChainInfoV2 {
                genesis_id: GenesisId { chain_id: chain_id.to_string(), hash: genesis_hash },
                height: head_height,
                tracked_shards: vec![0],
                archival: false,
            },
            partial_edge_info: PartialEdgeInfo::new(
                &self.my_peer_id,
                &self.peer_id,
                1,
                &self.secret_key,
            ),
            owned_account: None,
        });

        self.write_message(&handshake).await.map_err(ConnectError::IO)?;

        let start = Instant::now();

        let (message, timestamp) = self.recv_message().await.map_err(ConnectError::IO)?;

        match message {
            // TODO: maybe check the handshake for sanity
            PeerMessage::Tier2Handshake(_) => {
                tracing::info!(target: "network", "handshake latency: {}", timestamp - start);
            }
            PeerMessage::HandshakeFailure(_peer_info, reason) => {
                return Err(ConnectError::HandshakeFailure(reason))
            }
            _ => return Err(ConnectError::UnexpectedFirstMessage(message)),
        };

        Ok(())
    }

    async fn write_message(&mut self, msg: &PeerMessage) -> io::Result<()> {
        let mut msg = msg.serialize(Encoding::Proto);
        let mut buf = (msg.len() as u32).to_le_bytes().to_vec();
        buf.append(&mut msg);
        self.stream.write_all(&buf).await
    }

    async fn do_read(&mut self) -> io::Result<()> {
        let n = tokio::time::timeout(
            self.recv_timeout.try_into().unwrap(),
            self.stream.read_buf(&mut self.buf),
        )
        .await??;
        tracing::trace!(target: "network", "Read {} bytes from {:?}", n, self.stream.peer_addr());
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "no more bytes available, but expected to receive a message",
            ));
        }
        Ok(())
    }

    // read at least 4 bytes, but probably the whole message in most cases
    async fn read_msg_length(&mut self) -> io::Result<(usize, Instant)> {
        let mut first_byte_time = None;
        while self.buf.remaining() < 4 {
            self.do_read().await?;
            if first_byte_time.is_none() {
                first_byte_time = Some(Instant::now());
            }
        }
        let len = u32::from_le_bytes(self.buf[..4].try_into().unwrap());
        // If first_byte_time is None, there were already 4 bytes from last time,
        // and they must have come before a partial frame.
        // So the Instant::now() is not quite correct, since the time was really in the past,
        // but this is prob not so important
        Ok((len as usize, first_byte_time.unwrap_or(Instant::now())))
    }

    // Reads from the socket until there is at least one full PeerMessage available.
    async fn recv_message(&mut self) -> io::Result<(PeerMessage, Instant)> {
        let (msg_length, first_byte_time) = self.read_msg_length().await?;

        while self.buf.remaining() < msg_length + 4 {
            self.do_read().await?;
        }

        self.buf.advance(4);
        let msg = PeerMessage::deserialize(Encoding::Proto, &self.buf[..msg_length]);
        self.buf.advance(msg_length);

        // make sure we can probably read the next message in one syscall next time
        let max_len_after_next_read = self.buf.chunk_mut().len() + self.buf.remaining();
        if max_len_after_next_read < 512 {
            self.buf.reserve(512 - max_len_after_next_read);
        }
        msg.map(|m| {
            tracing::debug!(target: "network", "received PeerMessage::{} len: {}", &m, msg_length);
            (m, first_byte_time)
        })
        .map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("error parsing protobuf of length {}", msg_length),
            )
        })
    }

    /// Reads from the socket until we receive some message that we care to pass to the caller
    /// (that is, represented in `ReceivedMessage`).
    pub async fn recv(&mut self) -> io::Result<(ReceivedMessage, Instant)> {
        loop {
            let (msg, timestamp) = self.recv_message().await?;
            if let Ok(m) = ReceivedMessage::try_from(msg) {
                return Ok((m, timestamp));
            }
        }
    }

    /// Try to send a Ping message to the given target, with the given nonce and ttl
    pub async fn send_ping(&mut self, target: &PeerId, nonce: u64, ttl: u8) -> anyhow::Result<()> {
        let body = RoutedMessageBody::Ping(Ping { nonce, source: self.my_peer_id.clone() });
        let msg = RawRoutedMessage { target: PeerIdOrHash::PeerId(target.clone()), body }.sign(
            &self.secret_key,
            ttl,
            Some(Utc::now_utc()),
        );

        self.write_message(&PeerMessage::Routed(Box::new(msg))).await?;

        Ok(())
    }

    /// Try to send a StateRequestPart message to the given target, with the given nonce and ttl
    pub async fn send_state_part_request(
        &mut self,
        target: &PeerId,
        shard_id: ShardId,
        block_hash: CryptoHash,
        part_id: u64,
        ttl: u8,
    ) -> anyhow::Result<()> {
        let body = RoutedMessageBody::StateRequestPart(shard_id, block_hash, part_id);
        let msg = RawRoutedMessage { target: PeerIdOrHash::PeerId(target.clone()), body }.sign(
            &self.secret_key,
            ttl,
            Some(Utc::now_utc()),
        );

        self.write_message(&PeerMessage::Routed(Box::new(msg))).await?;

        Ok(())
    }
}
