use crate::network_protocol::PeerInfo;
use anyhow::{anyhow, Context as _};
use near_primitives::network::PeerId;
use std::fmt;
use std::sync::Arc;

const LISTENER_BACKLOG: u32 = 128;

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
    pub fn bind() -> Self {
        let socket = tokio::net::TcpSocket::new_v6().unwrap();
        socket.bind("[::1]:0".parse().unwrap()).unwrap();
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
        let listener_addr = ListenerAddr::new_for_test();
        let peer_info = PeerInfo { id: peer_id, addr: Some(*listener_addr), account_id: None };
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

/// In production code ListenerAddr is isomorphic to std::net::SocketAddr.
///
/// In tests it additionally allows to "reserve" a random unused TCP port:
/// * it allows to avoid race conditions in tests which require a dedicated TCP port to spawn a
///   node on (and potentially restart it every now and then).
/// * it is implemented by usign SO_REUSEPORT socket option (do not confuse with SO_REUSEADDR),
///   which allows multiple sockets to share a port. new_for_test() creates a socket and binds
///   it to a random unused local port (without starting a TCP listener).
///   This socket won't be used for anything but telling the OS that the given TCP port is in use.
///   However thanks to SO_REUSEPORT we can create another socket bind it to the same port
///   and make it a listener.
/// * The drawback of this solution that it is hard to detect a situation in which multiple
///   listener sockets in test are bound to the same port. TODO(gprusak): we can prevent creating
///   multiple listeners for a single port within the same process by implementing a global register
///   of ports in use.
#[derive(Clone)]
pub struct ListenerAddr {
    /// A test-only guard ensuring that OS considers the given TCP port to be in use (in prod,
    /// guard should be always None).
    guard: Arc<Option<tokio::net::TcpSocket>>,
    addr: std::net::SocketAddr,
}

impl fmt::Debug for ListenerAddr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ListenerAddr")
            .field("addr", &self.addr)
            .field("has_guard", &self.guard.is_some())
            .finish()
    }
}

impl std::ops::Deref for ListenerAddr {
    type Target = std::net::SocketAddr;
    fn deref(&self) -> &Self::Target {
        &self.addr
    }
}

impl ListenerAddr {
    pub fn new(addr: std::net::SocketAddr) -> Self {
        assert!(
            addr.port() != 0,
            "using an arbitrary port for the tcp::Listener is allowed only \
             in tests and only via new_for_test() method"
        );
        Self { addr, guard: Arc::new(None) }
    }

    /// TEST-ONLY: constructs a ListenerAddr owning a random port on localhost.
    pub fn new_for_test() -> Self {
        let guard = tokio::net::TcpSocket::new_v6().unwrap();
        guard.set_reuseaddr(true).unwrap();
        guard.set_reuseport(true).unwrap();
        guard.bind("[::1]:0".parse().unwrap()).unwrap();
        Self { addr: guard.local_addr().unwrap(), guard: Arc::new(Some(guard)) }
    }

    /// Constructs a Listener out of ListenerAddr.
    pub(crate) fn listener(&self) -> std::io::Result<Listener> {
        let socket = match self.addr {
            std::net::SocketAddr::V4(_) => tokio::net::TcpSocket::new_v4()?,
            std::net::SocketAddr::V6(_) => tokio::net::TcpSocket::new_v6()?,
        };
        if self.guard.is_some() {
            socket.set_reuseport(true)?;
        }
        socket.set_reuseaddr(true)?;
        socket.bind(self.addr)?;
        Ok(Listener(socket.listen(LISTENER_BACKLOG)?))
    }
}

pub(crate) struct Listener(tokio::net::TcpListener);

impl Listener {
    pub async fn accept(&mut self) -> std::io::Result<Stream> {
        let (stream, _) = self.0.accept().await?;
        Stream::new(stream, StreamType::Inbound)
    }
}
