//! Stream wrapper, which allows for custom interactions with the network protocol.
//! We might want to turn it into a fuzz testing framework for the network protocol.
use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::tcp;
use crate::network_protocol::{Encoding, PeerMessage};

pub struct Stream {
    stream: tcp::Stream,
    force_encoding: Option<Encoding>,
    protocol_buffers_supported: bool,
}

impl Stream {
    pub fn new(force_encoding: Option<Encoding>, stream: tcp::Stream) -> Self {
        Self {
            stream,
            force_encoding,
            protocol_buffers_supported: false,
        }
    }

    fn encoding(&self) -> Option<Encoding> {
        if self.force_encoding.is_some() {
            return self.force_encoding;
        }
        if self.protocol_buffers_supported {
            return Some(Encoding::Proto);
        }
        return None;
    }

    pub async fn read(&mut self) -> PeerMessage {
        'read: loop {
            let n = self.stream.stream.read_u32_le().await.unwrap() as usize;
            let mut buf = BytesMut::new();
            buf.resize(n, 0);
            self.stream.stream.read_exact(&mut buf[..]).await.unwrap();
            for enc in [Encoding::Proto, Encoding::Borsh] {
                if let Ok(msg) = PeerMessage::deserialize(enc, &buf[..]) {
                    // If deserialize() succeeded but we expected different encoding, ignore the
                    // message.
                    if self.encoding().unwrap_or(enc) != enc {
                        println!("unexpected encoding, ignoring message");
                        continue 'read;
                    }
                    if enc == Encoding::Proto {
                        self.protocol_buffers_supported = true;
                    }
                    return msg;
                }
            }
            panic!("unknown encoding");
        }
    }

    pub async fn write(&mut self, msg: &PeerMessage) {
        if let Some(enc) = self.encoding() {
            self.write_encoded(&msg.serialize(enc)).await;
        } else {
            self.write_encoded(&msg.serialize(Encoding::Proto)).await;
            self.write_encoded(&msg.serialize(Encoding::Borsh)).await;
        }
    }

    async fn write_encoded(&mut self, msg: &[u8]) {
        self.stream.stream.write_u32_le(msg.len() as u32).await.unwrap();
        self.stream.stream.write_all(msg).await.unwrap();
        self.stream.stream.flush().await.unwrap();
    }
}

/*
// The following is partial reimplementation of the handshake protocol.
// It is a stub which eventually evolve to be a fully fledged set of interactions
// suitable for fuzz testing the network protocol.

async fn write_msg(stream: &mut tcp::Stream, msg:&PeerMessage) -> anyhow::Result<()> {
    let buf = msg.serialize(Encoding::Proto);
    stream.stream.write_u32_le(buf.size()).await?;
    stream.stream.write_exact(&buf[..]).await?;
    Ok(())
}

async fn read_msg(stream: &mut tcp::Stream) -> anyhow::Result<PeerMessage> {
    let n = stream.stream.read_u32_le().await?;
    let mut buf = vec![0; n];
    stream.stream.read_exact(&mut buf[..]).await?;
    Ok(PeerMessage::deserialize(Encoding::Proto, buf)?)
}

pub(crate) async fn handshake(state: &NetworkState, stream: &mut tcp::Stream) -> anyhow::Result<()> {
    match stream.type_ {
        tcp::StreamType::Inbound => {
            let h = match read_msg(stream).await? {
                PeerMessage::Handshake(h) => h,
                msg => bail!("unexpected message {msg:?}"),
            };
            
            Handshake {
                protocol_version: PROTOCOL_VERSION,
                oldest_supported_version: MIN_SUPPORTED_PROTOCOL_VERSION,
                sender_peer_id: self.id,
                target_peer_id: h.sender_peer_id,
                Sender
            }
        }
        tcp::StreamType::Outbound{peer_id} => {
            let nonce = 1;
            Handshake{
                protocol_version: PROTOCOL_VERSION,
                oldest_supported_version: MIN_SUPPORTED_PROTOCOL_VERSION,
                sender_peer_id: self.id,
                target_peer_id: peer_id,
                sender_listen_port: None,
                sender_chain_info: chain.chain_info(),
                partial_edge_info: PartialEdgeInfo::new(&self.config.node_id(), peer_id, nonce, &self.config.node_key)
            }
        }
    }
}*/
