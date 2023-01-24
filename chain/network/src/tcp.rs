use crate::network_protocol::PeerInfo;
use anyhow::{anyhow, Context as _};
use near_primitives::network::PeerId;
use std::sync::{Arc,Mutex};
use std::fmt;

/// TCP connections established by a node belong to different logical networks (aka tiers),
/// which serve different purpose.
// TODO(gprusak): add a link to the design on github docs (but first write those docs).
#[derive(Clone, Copy, Debug, PartialEq, Eq, strum::AsRefStr)]
pub enum Tier {
    /// Tier1 connections are established between the BFT consensus participants (or their proxies)
    /// and are reserved exclusively for exchanging BFT consensus messages.
    T1,
    /// Tier2 connections form a P2P gossip network, which is used for everything, except the BFT
    /// consensus messages. Also, Tier1 peer discovery actually happens on Tier2 network, i.e.
    /// Tier2 network is necessary to bootstrap Tier1 connections.
    T2,
}

#[derive(Clone, Debug)]
pub(crate) enum StreamType {
    Inbound,
    Outbound { peer_id: PeerId, tier: Tier },
}

#[derive(Debug)]
pub struct Stream {
    pub(crate) stream: tokio::net::TcpStream,
    pub(crate) type_: StreamType,
    /// cached stream.local_addr()
    pub(crate) local_addr: std::net::SocketAddr,
    /// cached peer_addr.local_addr()
    pub(crate) peer_addr: std::net::SocketAddr,
}

/// TEST-ONLY. Used to identify events relevant to a specific TCP connection in unit tests.
/// Every outbound TCP connection has a unique TCP port (while inbound TCP connections
/// have the same port as the TCP listen socket).
/// We are assuming here that the unit test is executed on a single machine on the loopback
/// network interface, so that both inbound and outbound IP is always 127.0.0.1.
/// To create a reliable StreamId for a distributed, we would have to transmit it over the connection itself,
/// which is doable, but not yet needed in our testing framework.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct StreamId {
    inbound: std::net::SocketAddr,
    outbound: std::net::SocketAddr,
}

#[cfg(test)]
pub(crate) struct Socket(tokio::net::TcpSocket);

#[cfg(test)]
impl Socket {
    pub fn bind_v4() -> Self {
        let socket = tokio::net::TcpSocket::new_v4().unwrap();
        socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
        Self(socket)
    }

    pub async fn connect(self, peer_info: &PeerInfo, tier: Tier) -> Stream {
        // TODO(gprusak): this could replace Stream::connect,
        // however this means that we will have to replicate everything
        // that tokio::net::TcpStream sets on the socket.
        // As long as Socket::connect is test-only we may ignore that.
        let stream = self.0.connect(peer_info.addr.unwrap()).await.unwrap();
        Stream::new(stream, StreamType::Outbound { peer_id: peer_info.id.clone(), tier }).unwrap()
    }
}

impl Stream {
    fn new(stream: tokio::net::TcpStream, type_: StreamType) -> std::io::Result<Self> {
        Ok(Self { peer_addr: stream.peer_addr()?, local_addr: stream.local_addr()?, stream, type_ })
    }

    pub async fn connect(peer_info: &PeerInfo, tier: Tier) -> anyhow::Result<Stream> {
        let addr =
            peer_info.addr.ok_or(anyhow!("Trying to connect to peer with no public address"))?;
        // The `connect` may take several minutes. This happens when the
        // `SYN` packet for establishing a TCP connection gets silently
        // dropped, in which case the default TCP timeout is applied. That's
        // too long for us, so we shorten it to one second.
        //
        // Why exactly a second? It was hard-coded in a library we used
        // before, so we keep it to preserve behavior. Removing the timeout
        // completely was observed to break stuff for real on the testnet.
        let stream = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            tokio::net::TcpStream::connect(addr),
        )
        .await?
        .context("TcpStream::connect()")?;
        Ok(Stream::new(stream, StreamType::Outbound { peer_id: peer_info.id.clone(), tier })?)
    }

    /// Establishes a loopback TCP connection to localhost with random ports.
    /// Returns a pair of streams: (outbound,inbound).
    #[cfg(test)]
    pub async fn loopback(peer_id: PeerId, tier: Tier) -> (Stream, Stream) {
        let listener_addr = ListenerAddr::new_localhost();
        let peer_info = PeerInfo {
            id: peer_id,
            addr: Some(*listener_addr.as_ref()),
            account_id: None,
        };
        let mut listener = listener_addr.listener().unwrap();
        let (outbound, inbound) =
            tokio::join!(Stream::connect(&peer_info, tier), listener.accept());
        (outbound.unwrap(), inbound.unwrap())
    }

    // TEST-ONLY used in reporting test events.
    pub(crate) fn id(&self) -> StreamId {
        match self.type_ {
            StreamType::Inbound => StreamId { inbound: self.local_addr, outbound: self.peer_addr },
            StreamType::Outbound { .. } => {
                StreamId { inbound: self.peer_addr, outbound: self.local_addr }
            }
        }
    }
}

/// ListenerAddr is a cloneable owner of a TCP socket, which can be turned into a
/// Listener. Since it actually keeps ownership of a system resource - TCP socket bound to a
/// specific local port, it allows to avoid a race condition in tests where multiple tests would
/// try to use the same port.
///
/// We make it cloneable, so that it can be included in a cloneable config and reused when a node
/// instance is being restarted in test.
/// TODO(gprusak): we lose the compilation-time check that would ensure that only one node instance
/// is using the given socket - expressing that would require the node to return the socket
/// ownership during shutdown and that would be ugly and hard to maintain. However we could rather
/// cheaply implement a runtime check which would panic in case multiple nodes were trying to start
/// listening on the same socket.
#[derive(Clone)]
pub struct ListenerAddr{
    raw: Arc<Mutex<Option<PlaceholderSocket>>>,
    addr: std::net::SocketAddr,
}

impl fmt::Debug for ListenerAddr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.addr.fmt(f)
    }
}

impl AsRef<std::net::SocketAddr> for ListenerAddr {
    fn as_ref(&self) -> &std::net::SocketAddr { &self.addr }
}

struct PlaceholderSocket(rustix::fd::OwnedFd);

impl PlaceholderSocket {
    fn new(addr: std::net::SocketAddr) -> std::io::Result<Self> {
        let f = match &addr {
            std::net::SocketAddr::V4(_) => rustix::net::AddressFamily::INET,
            std::net::SocketAddr::V6(_) => rustix::net::AddressFamily::INET6,
        };
        let t = rustix::net::SocketType::STREAM;
        let p = rustix::net::Protocol::TCP;
        let socket = rustix::net::socket(f,t,p)?;
        rustix::net::sockopt::set_socket_reuseaddr(&socket,true)?;
        rustix::net::bind(&socket,&addr)?;
        Ok(Self(socket))
    }

    fn local_addr(&self) -> std::io::Result<std::net::SocketAddr> {
        Ok(match rustix::net::getsockname(&self.0)? {
            rustix::net::SocketAddrAny::V4(addr) => std::net::SocketAddr::V4(addr),
            rustix::net::SocketAddrAny::V6(addr) => std::net::SocketAddr::V6(addr),
            _ => panic!("unexpected socket addr"),
        })
    }
}

impl ListenerAddr {
    pub fn new(addr: std::net::SocketAddr) -> std::io::Result<Self> {
        let socket = PlaceholderSocket::new(addr)?;
        Ok(Self{
            // We fetch local_addr(), because addr.port() could have been 0, in which case an
            // arbitrary free port will be allocated.
            addr: socket.local_addr()?,
            raw: Arc::new(Mutex::new(Some(socket))),
        })
    }

    #[cfg(test)]
    pub(crate) fn new_localhost() -> Self {
        Self::new("127.0.0.1:0".parse().unwrap()).unwrap()
    }

    pub(crate) fn listener(&self) -> std::io::Result<Listener> {
        drop(self.clone().raw.lock().unwrap().take());
        let inner = std::net::TcpListener::bind(self.addr)?;
        // tokio::net::TcpListener::from_std() assumes that the socket is nonblocking.
        // We need to configure it manually here.
        inner.set_nonblocking(true)?;
        Ok(Listener {
            inner: Some(tokio::net::TcpListener::from_std(inner)?),
            addr: self.clone(),   
        })
    }
}

pub(crate) struct Listener {
    addr: ListenerAddr,
    inner: Option<tokio::net::TcpListener>,
}

impl Drop for Listener {
    fn drop(&mut self) {
        drop(self.inner.take());
        match PlaceholderSocket::new(self.addr.addr) {
            Ok(socket) => { *self.addr.raw.lock().unwrap() = Some(socket); }
            Err(err) => { near_o11y::log_assert!(false,"failed to guard port {} after closing TCP listener: {err}",self.addr.addr); }
        }
    }
}

impl Listener {
    pub async fn accept(&mut self) -> std::io::Result<Stream> {
        let (stream, _) = self.inner.as_mut().unwrap().accept().await?;
        Stream::new(stream, StreamType::Inbound)
    }
}
