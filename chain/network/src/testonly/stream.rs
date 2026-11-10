//! Stream wrapper, which allows for custom interactions with the network protocol.
//! We might want to turn it into a fuzz testing framework for the network protocol.
use crate::network_protocol::{Encoding, PeerMessage, PeerMessageFrames, PeerMessageHeader};
use crate::tcp;
use borsh::BorshDeserialize;
use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub struct Stream {
    stream: tcp::Stream,
    encoding: Encoding,
    first_message: bool,
}

impl Stream {
    /// Creates a stream which speaks `encoding`. For `Encoding::Borsh` it behaves like a node
    /// configured with `PreferBorsh`: the initial handshake is sent in both encodings.
    pub fn new_with_encoding(stream: tcp::Stream, encoding: Encoding) -> Self {
        Self { stream, encoding, first_message: true }
    }

    pub async fn read(&mut self) -> Result<PeerMessage, std::io::Error> {
        let frame = self.read_frame().await?;
        let frames = match self.encoding {
            Encoding::Proto => PeerMessageFrames::proto(frame),
            Encoding::Borsh => {
                let kind = PeerMessageHeader::try_from_slice(&frame).map_err(invalid_data)?.kind();
                let body = self.read_frame().await?;
                if body.len() > kind.max_size() {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("body of a {kind:?} message is too large: {}", body.len()),
                    ));
                }
                PeerMessageFrames { encoding: Encoding::Borsh, header: Some(frame), body }
            }
        };
        PeerMessage::deserialize_frames(&frames).map(|(_, msg)| msg).map_err(invalid_data)
    }

    pub async fn write(&mut self, msg: &PeerMessage) {
        if self.first_message {
            self.first_message = false;
            if self.encoding == Encoding::Borsh && is_handshake(msg) {
                // Like a node with `PreferBorsh`, send the initial handshake in both encodings.
                self.write_frames(msg.serialize_with_encoding(Encoding::Borsh)).await;
                self.write_frames(msg.serialize_with_encoding(Encoding::Proto)).await;
                return;
            }
        }
        self.write_frames(msg.serialize_with_encoding(self.encoding)).await;
    }

    async fn write_frames(&mut self, frames: PeerMessageFrames) {
        if let Some(header) = frames.header {
            self.write_encoded(&header).await;
        }
        self.write_encoded(&frames.body).await;
    }

    async fn read_frame(&mut self) -> Result<Vec<u8>, std::io::Error> {
        let n = self.stream.stream.read_u32_le().await? as usize;
        let mut buf = BytesMut::new();
        buf.resize(n, 0);
        self.stream.stream.read_exact(&mut buf[..]).await?;
        Ok(buf.to_vec())
    }

    async fn write_encoded(&mut self, msg: &[u8]) {
        self.stream.stream.write_u32_le(msg.len() as u32).await.unwrap();
        self.stream.stream.write_all(msg).await.unwrap();
        self.stream.stream.flush().await.unwrap();
    }
}

fn is_handshake(msg: &PeerMessage) -> bool {
    matches!(
        msg,
        PeerMessage::Tier1Handshake(_)
            | PeerMessage::Tier2Handshake(_)
            | PeerMessage::Tier3Handshake(_)
    )
}

fn invalid_data(err: impl std::fmt::Display) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, format!("Failed to deserialize: {err}"))
}
