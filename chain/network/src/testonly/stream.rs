//! Stream wrapper, which allows for custom interactions with the network protocol.
//! We might want to turn it into a fuzz testing framework for the network protocol.
use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::network_protocol::PeerMessage;
use crate::tcp;

pub struct Stream {
    stream: tcp::Stream,
}

impl Stream {
    pub fn new(stream: tcp::Stream) -> Self {
        Self { stream }
    }

    pub async fn read(&mut self) -> Result<PeerMessage, std::io::Error> {
        loop {
            // TODO: simplify / remove this loop
            let n = self.stream.stream.read_u32_le().await? as usize;
            let mut buf = BytesMut::new();
            buf.resize(n, 0);
            self.stream.stream.read_exact(&mut buf[..]).await?;
            if let Ok(msg) = PeerMessage::deserialize(&buf[..]) {
                return Ok(msg);
            }
            panic!("unknown encoding");
        }
    }

    pub async fn write(&mut self, msg: &PeerMessage) {
        self.write_encoded(&msg.serialize()).await;
    }

    async fn write_encoded(&mut self, msg: &[u8]) {
        self.stream.stream.write_u32_le(msg.len() as u32).await.unwrap();
        self.stream.stream.write_all(msg).await.unwrap();
        self.stream.stream.flush().await.unwrap();
    }
}
