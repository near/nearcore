use async_trait::async_trait;
use bytes::buf::{Buf, BufMut};
use bytes::BytesMut;
use near_crypto::SecretKey;
use near_network::types::{Encoding, PeerMessage};
use near_network_primitives::types::{
    AccountOrPeerIdOrHash, RawRoutedMessage, RoutedMessageBody, TcpListener, TcpStream, TcpSys,
};
use near_primitives::network::PeerId;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context, Poll, Waker};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use crate::{MockNet, MockPeer};

struct ReadBuffer {
    buf: BytesMut,
    reader: Option<Waker>,
}

struct MockTcpStreamInner {
    local_addr: SocketAddr,
    peer_addr: SocketAddr,
    response_delay: Duration,
    read_buf: Arc<Mutex<ReadBuffer>>,
    peer: Arc<Mutex<MockPeer>>,
    net: MockNet,
    shutdown: AtomicBool,
}

pub(crate) struct MockTcpStream(Arc<MockTcpStreamInner>);

// the point of this is so that there's only one MockTcpStream around,
// owned by neard. When that is dropped, MockTcpStreamInner will be dropped
// and we can close the connection
#[derive(Clone)]
pub(crate) struct MockTcpStreamHandle(Weak<MockTcpStreamInner>);

impl MockTcpStream {
    fn new(net: MockNet, peer: Arc<Mutex<MockPeer>>) -> io::Result<Self> {
        Ok(Self(Arc::new(MockTcpStreamInner::new(net, peer)?)))
    }

    fn handle(&self) -> MockTcpStreamHandle {
        MockTcpStreamHandle(Arc::downgrade(&self.0))
    }

    pub(crate) fn send_message(&self, msg: &PeerMessage) {
        self.0.send_message(msg);
    }

    pub(crate) fn send_routed_message(
        &self,
        msg: RoutedMessageBody,
        target: PeerId,
        key: &SecretKey,
    ) {
        self.0.send_routed_message(msg, target, key);
    }

    fn handle_frame(&self, frame: &[u8]) {
        tracing::trace_span!(target: "mock-node", "handle_frame");
        self.0.peer.lock().unwrap().recv(self, PeerMessage::deserialize(Encoding::Proto, frame));
    }

    fn read(&self, src: &[u8]) -> io::Result<usize> {
        let len = src.len();
        let mut pos = 0;

        if self.0.shutdown.load(Ordering::Acquire) {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "mock socket has been shutdown()",
            ));
        }
        // TODO: handle the case of incomplete frames by saving the data for later in a BytesMut or something.
        // with the current code it doesn't look possible that we get an incomplete frame here but the mock
        // code ideally would not assume that
        while pos < len {
            if len - pos <= 4 {
                panic!(
                    "mock poll_write() received a partial frame: {} bytes remaining. \
                    Don't know how to correctly continue",
                    len - pos
                );
            }
            let frame_len = u32::from_le_bytes(src[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4;
            if len - pos < frame_len {
                panic!(
                    "mock poll_write() received a partial frame: {} bytes remaining. \
                    Don't know how to correctly continue",
                    len - pos
                );
            }
            self.handle_frame(&src[pos..pos + frame_len]);
            pos += frame_len;
        }
        Ok(len)
    }
}

impl MockTcpStreamHandle {
    pub(crate) fn get(&self) -> Option<MockTcpStream> {
        self.0.upgrade().map(MockTcpStream)
    }

    pub(crate) fn alive(&self) -> bool {
        self.0.upgrade().is_some()
    }
}

impl MockTcpStreamInner {
    fn new(net: MockNet, peer: Arc<Mutex<MockPeer>>) -> io::Result<Self> {
        let (peer_addr, response_delay) = {
            let p = peer.lock().unwrap();
            if p.is_connected() {
                // Actually it would connect but let's just not handle that case for now.
                return Err(io::Error::new(
                    io::ErrorKind::ConnectionRefused,
                    "already connected to mock peer at this address",
                ));
            }
            (p.addr, p.response_delay)
        };

        Ok(Self {
            local_addr: SocketAddr::new("127.0.0.1".parse().unwrap(), net.get_port()?),
            peer_addr,
            response_delay,
            read_buf: Arc::new(Mutex::new(ReadBuffer { buf: BytesMut::new(), reader: None })),
            peer,
            net,
            shutdown: AtomicBool::new(false),
        })
    }

    fn is_full(&self) -> bool {
        // 4GB max
        self.read_buf.lock().unwrap().buf.remaining() > 1 << 32
    }

    fn send_message(&self, msg: &PeerMessage) {
        if self.is_full() {
            // Should not get here. But could happen if we have a buggy neard that
            // is not reading for some reason.
            tracing::warn!(target: "mock-node", "Can't write to mock TCP stream. Already full (> 4GB buffered). is neard reading from its sockets?");
            return;
        }
        let bytes = msg.serialize(Encoding::Proto);
        let delay = self.response_delay;
        let buf = self.read_buf.clone();

        tracing::trace!(target: "mock-node", "sending {} with delay {:?}", msg, delay);
        // TODO: keep the join handle and abort it on drop. probably a good idea for is_full()
        // to look at how many outstanding send_message() tasks we have.
        actix::spawn(async move {
            tokio::time::sleep(delay).await;
            tracing::trace!(target: "mock-node", "copying {} bytes into read buffer", bytes.len() + 4);

            let mut buf = buf.lock().unwrap();
            buf.buf.put_u32_le(bytes.len() as u32);
            buf.buf.put_slice(&bytes);
            if let Some(reader) = &buf.reader {
                reader.wake_by_ref();
            }
        });
    }

    fn send_routed_message(&self, msg: RoutedMessageBody, target: PeerId, key: &SecretKey) {
        let msg = RawRoutedMessage { target: AccountOrPeerIdOrHash::PeerId(target), body: msg }
            .sign(PeerId::new(key.public_key()), key, 100);
        self.send_message(&PeerMessage::Routed(msg));
    }

    fn write(&self, dst: &mut ReadBuf<'_>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let mut buf = self.read_buf.lock().unwrap();
        if buf.buf.has_remaining() {
            let n = std::cmp::min(buf.buf.remaining(), dst.remaining());
            tracing::trace!(target: "mock-node", "writing {} bytes out of {} available", n, buf.buf.remaining());
            dst.put_slice(&buf.buf[..n]);
            buf.buf.advance(n);
            Poll::Ready(Ok(()))
        } else {
            match &buf.reader {
                Some(w) => {
                    if !w.will_wake(cx.waker()) {
                        buf.reader = Some(cx.waker().clone());
                    }
                }
                None => {
                    buf.reader = Some(cx.waker().clone());
                }
            }
            Poll::Pending
        }
    }
}

impl Drop for MockTcpStreamInner {
    fn drop(&mut self) {
        self.net.release_port(self.local_addr.port());
    }
}

impl AsyncRead for MockTcpStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        tracing::trace_span!(target: "mock-node", "poll_read", capacity = buf.remaining());
        self.0.write(buf, cx)
    }
}

impl AsyncWrite for MockTcpStream {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        src: &[u8],
    ) -> Poll<io::Result<usize>> {
        tracing::trace_span!(target: "mock-node", "poll_write", len = src.len());
        Poll::Ready(self.read(src))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        self.0.shutdown.store(true, Ordering::Release);
        Poll::Ready(Ok(()))
    }
}

impl TcpStream for MockTcpStream {
    fn local_addr(&self) -> io::Result<SocketAddr> {
        Ok(self.0.local_addr)
    }

    fn peer_addr(&self) -> io::Result<SocketAddr> {
        Ok(self.0.peer_addr)
    }
}

struct MockTcpListener;

#[async_trait]
impl TcpListener for MockTcpListener {
    async fn accept(&self) -> io::Result<(Box<dyn TcpStream>, SocketAddr)> {
        // does nothing for now but in the future we could mock incoming connections
        std::future::pending().await
    }
}

#[async_trait]
impl TcpSys for MockNet {
    fn sys(&self) -> Box<dyn TcpSys> {
        Box::new(self.clone())
    }

    async fn bind(&mut self, addr: SocketAddr) -> io::Result<Box<dyn TcpListener>> {
        tracing::debug!(target: "mock-node", "bind() to {:?}", &addr);
        Ok(Box::new(MockTcpListener {}))
    }

    async fn connect(&mut self, addr: SocketAddr) -> io::Result<Box<dyn TcpStream>> {
        tracing::debug!(target: "mock-node", "connect() to {:?}", &addr);
        for peer in self.peers.iter() {
            if peer.lock().unwrap().addr == addr {
                let s = MockTcpStream::new(self.clone(), peer.clone())?;
                peer.lock().unwrap().connected(s.handle());
                return Ok(Box::new(s));
            }
        }
        Err(io::Error::new(
            io::ErrorKind::ConnectionRefused,
            "given IP addr : TCP port not being mocked",
        ))
    }
}
