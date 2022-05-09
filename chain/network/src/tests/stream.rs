/// Stream wraps TcpStream, allowing for sending & receiving PeerMessages.
/// Currently just used in tests, but eventually will replace actix-driven communication.
use bytes::BytesMut;
use tokio::io;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net;
use tokio::sync::Mutex as AsyncMutex;

use crate::network_protocol::{Encoding, PeerMessage};

pub struct Stream {
    pub local_addr: std::net::SocketAddr,
    pub peer_addr: std::net::SocketAddr,
    force_encoding: Option<Encoding>,
    protocol_buffers_supported: bool,
    reader: AsyncMutex<io::BufReader<io::ReadHalf<net::TcpStream>>>,
    writer: AsyncMutex<io::BufWriter<io::WriteHalf<net::TcpStream>>>,
}

impl Stream {
    pub fn new(force_encoding: Option<Encoding>, stream: net::TcpStream) -> Self {
        let local_addr = stream.local_addr().unwrap();
        let peer_addr = stream.peer_addr().unwrap();
        let (reader, writer) = io::split(stream);
        Self {
            local_addr,
            peer_addr,
            force_encoding,
            protocol_buffers_supported: false,
            reader: AsyncMutex::new(io::BufReader::new(reader)),
            writer: AsyncMutex::new(io::BufWriter::new(writer)),
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
        let mut reader = self.reader.lock().await;
        'read: loop {
            let n = reader.read_u32_le().await.unwrap() as usize;
            let mut buf = BytesMut::new();
            buf.resize(n, 0);
            reader.read_exact(&mut buf[..]).await.unwrap();
            for enc in [Encoding::Proto, Encoding::Borsh] {
                if let Ok(msg) = PeerMessage::deserialize(enc, &buf[..]) {
                    // If deserialize() succeeded but we expected different encoding, ignore the
                    // message.
                    if !self.encoding().map(|want| want == enc).unwrap_or(true) {
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

    pub async fn write(&self, msg: &PeerMessage) {
        if let Some(enc) = self.encoding() {
            self.write_encoded(&msg.serialize(enc)).await;
        } else {
            self.write_encoded(&msg.serialize(Encoding::Proto)).await;
            self.write_encoded(&msg.serialize(Encoding::Borsh)).await;
        }
    }

    async fn write_encoded(&self, msg: &[u8]) {
        let mut writer = self.writer.lock().await;
        writer.write_u32_le(msg.len() as u32).await.unwrap();
        writer.write_all(msg).await.unwrap();
        writer.flush().await.unwrap();
    }
}
