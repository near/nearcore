use crate::types::{Encoding, Handshake, HandshakeFailureReason, PeerMessage};
use bytes::buf::{Buf, BufMut};
use bytes::BytesMut;
use near_crypto::{KeyType, SecretKey};
use near_network_primitives::time::Utc;
use near_network_primitives::types::{
    AccountOrPeerIdOrHash, PartialEdgeInfo, PeerChainInfoV2, Ping, RawRoutedMessage,
    RoutedMessageBody,
};
use near_primitives::block::GenesisId;
use near_primitives::hash::CryptoHash;
use near_primitives::network::PeerId;
use near_primitives::types::{AccountId, BlockHeight, EpochId};
use near_primitives::version::{ProtocolVersion, PROTOCOL_VERSION};
use std::io;
use std::net::SocketAddr;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

pub struct Peer {
    my_peer_id: PeerId,
    peer_id: PeerId,
    secret_key: SecretKey,
    stream: TcpStream,
    buf: BytesMut,
}

pub enum Message {
    AnnounceAccounts(Vec<(AccountId, PeerId, EpochId)>),
    Pong { nonce: u64, source: PeerId },
}

impl Message {
    fn from_peer_message(m: PeerMessage) -> Option<Self> {
        match m {
            PeerMessage::Routed(r) => match &r.body {
                RoutedMessageBody::Pong(p) => {
                    Some(Self::Pong { nonce: p.nonce, source: p.source.clone() })
                }
                _ => None,
            },
            PeerMessage::SyncRoutingTable(r) => Some(Self::AnnounceAccounts(
                r.accounts.into_iter().map(|a| (a.account_id, a.peer_id, a.epoch_id)).collect(),
            )),
            _ => None,
        }
    }
}

impl std::fmt::Debug for Peer {
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
    IO(#[from] std::io::Error),
    #[error("handshake failed {0:?}")]
    HandshakeFailure(HandshakeFailureReason),
}

impl Peer {
    pub async fn connect(
        addr: SocketAddr,
        peer_id: PeerId,
        my_protocol_version: Option<ProtocolVersion>,
        chain_id: &str,
        genesis_hash: CryptoHash,
        head_height: BlockHeight,
    ) -> Result<Self, ConnectError> {
        let secret_key = SecretKey::from_random(KeyType::ED25519);
        let my_peer_id = PeerId::new(secret_key.public_key());

        let start = Instant::now();
        let stream = tokio::time::timeout(Duration::from_secs(2), TcpStream::connect(addr))
            .await
            .map_err(Into::<io::Error>::into)??;
        tracing::info!(
            target: "network", "Connection to {:?}@{:?} established. latency: {:?}",
            &peer_id, &addr, start.elapsed(),
        );

        let mut peer =
            Self { stream, peer_id, buf: BytesMut::with_capacity(1024), secret_key, my_peer_id };
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
        let handshake = PeerMessage::Handshake(Handshake::new(
            protocol_version,
            self.my_peer_id.clone(),
            self.peer_id.clone(),
            // we have to set this even if we have no intention of listening since otherwise
            // the peer will drop our connection
            Some(24567),
            PeerChainInfoV2 {
                genesis_id: GenesisId { chain_id: chain_id.to_string(), hash: genesis_hash },
                height: head_height,
                tracked_shards: vec![0],
                archival: false,
            },
            PartialEdgeInfo::new(&self.my_peer_id, &self.peer_id, 1, &self.secret_key),
        ));

        self.write_message(&handshake).await?;

        let start = Instant::now();

        let (message, timestamp) = self.recv_message().await?;

        match message {
            // TODO: maybe check the handshake for sanity
            PeerMessage::Handshake(_) => {
                tracing::info!(target: "network", "handshake latency: {:?}", timestamp - start);
            }
            PeerMessage::HandshakeFailure(_peer_info, reason) => {
                return Err(ConnectError::HandshakeFailure(reason))
            }
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "First message received from {:?} is not a handshake: {:?}",
                        self, &message
                    ),
                )
                .into())
            }
        };

        Ok(())
    }

    async fn write_message(&mut self, msg: &PeerMessage) -> io::Result<()> {
        let mut msg = msg.serialize(Encoding::Proto);
        let mut buf = (msg.len() as u32).to_le_bytes().to_vec();
        buf.append(&mut msg);
        self.stream.write_all(&buf).await
    }

    // panics if there's not at least `len` + 4 bytes available
    fn extract_msg(&mut self, len: usize) -> io::Result<PeerMessage> {
        self.buf.advance(4);
        let msg = PeerMessage::deserialize(Encoding::Proto, &self.buf[..len]);
        self.buf.advance(len);
        msg.map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("error parsing protobuf of length {}", len),
            )
        })
    }

    async fn do_read(&mut self) -> io::Result<()> {
        let n = tokio::time::timeout(Duration::from_secs(5), self.stream.read_buf(&mut self.buf))
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

    // drains anything still available to be read without blocking and
    // returns a vec of message lengths currently ready
    fn read_remaining_messages(
        &mut self,
        first_msg_length: usize,
    ) -> io::Result<Vec<(usize, Instant)>> {
        let mut pos = first_msg_length + 4;
        let mut lengths = Vec::new();
        let mut pending_msg_length = None;

        loop {
            match self.stream.try_read_buf(&mut self.buf) {
                Ok(n) => {
                    if n == 0 {
                        return Err(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            "no more bytes available, but expected to receive a message",
                        ));
                    }
                    let timestamp = Instant::now();
                    tracing::trace!(target: "network", "Read {} bytes from {:?} non-blocking", n, self.stream.peer_addr());
                    loop {
                        if pending_msg_length.is_none() && self.buf.remaining() - pos >= 4 {
                            let len =
                                u32::from_le_bytes(self.buf[pos..pos + 4].try_into().unwrap());
                            pending_msg_length = Some(len as usize);
                        }
                        if let Some(l) = pending_msg_length {
                            if self.buf.remaining() - pos >= l + 4 {
                                lengths.push((l, timestamp));
                                pos += l + 4;
                                pending_msg_length = None;
                            } else {
                                break;
                            }
                        } else {
                            break;
                        }
                    }
                }
                Err(e) => match e.kind() {
                    io::ErrorKind::WouldBlock => {
                        break;
                    }
                    _ => return Err(e),
                },
            };
        }
        Ok(lengths)
    }

    // Reads from the socket until there is at least one full PeerMessage available.
    // After that point, continues to read any bytes available to be read without blocking
    // and appends any extra messages to the returned Vec. Usually there is only one in there
    async fn recv_messages(&mut self) -> io::Result<Vec<(PeerMessage, Instant)>> {
        let mut messages = Vec::new();
        let (msg_length, first_byte_time) = self.read_msg_length().await?;

        while self.buf.remaining() < msg_length + 4 {
            // TODO: measure time to last byte here
            self.do_read().await?;
        }

        let more_messages = self.read_remaining_messages(msg_length)?;

        let msg = self.extract_msg(msg_length)?;
        tracing::debug!(target: "network", "received PeerMessage::{} len: {}", msg, msg_length);
        messages.push((msg, first_byte_time));

        for (len, timestamp) in more_messages {
            let msg = self.extract_msg(len)?;
            tracing::debug!(target: "network", "received PeerMessage::{} len: {}", msg, len);
            messages.push((msg, timestamp));
        }

        // make sure we can probably read the next message in one syscall next time
        let max_len_after_next_read = self.buf.chunk_mut().len() + self.buf.remaining();
        if max_len_after_next_read < 512 {
            self.buf.reserve(512 - max_len_after_next_read);
        }
        Ok(messages)
    }

    async fn recv_message(&mut self) -> io::Result<(PeerMessage, Instant)> {
        let (msg_length, first_byte_time) = self.read_msg_length().await?;

        while self.buf.remaining() < msg_length + 4 {
            self.do_read().await?;
        }
        Ok((self.extract_msg(msg_length)?, first_byte_time))
    }

    pub async fn recv(&mut self) -> io::Result<Vec<(Message, Instant)>> {
        loop {
            let msgs = self.recv_messages().await?;
            let mut ret = Vec::new();

            for (msg, timestamp) in msgs {
                if let Some(m) = Message::from_peer_message(msg) {
                    ret.push((m, timestamp));
                }
            }
            if !ret.is_empty() {
                return Ok(ret);
            }
        }
    }

    pub async fn send_ping(&mut self, target: &PeerId, nonce: u64, ttl: u8) -> anyhow::Result<()> {
        let body = RoutedMessageBody::Ping(Ping { nonce, source: self.my_peer_id.clone() });
        let msg = RawRoutedMessage { target: AccountOrPeerIdOrHash::PeerId(target.clone()), body }
            .sign(self.my_peer_id.clone(), &self.secret_key, ttl, Some(Utc::now_utc()));

        self.write_message(&PeerMessage::Routed(Box::new(msg))).await?;

        Ok(())
    }
}
