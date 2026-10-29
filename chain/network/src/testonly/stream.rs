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
}

impl Stream {
    /// Creates a stream which speaks protobuf.
    pub fn new(stream: tcp::Stream) -> Self {
        Self::new_with_encoding(stream, Encoding::Proto)
    }

    /// Creates a stream which speaks `encoding`.
    pub fn new_with_encoding(stream: tcp::Stream, encoding: Encoding) -> Self {
        Self { stream, encoding }
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
        let frames = msg.serialize_with_encoding(self.encoding);
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

fn invalid_data(err: impl std::fmt::Display) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, format!("Failed to deserialize: {err}"))
}
