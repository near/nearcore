use crate::config::SocketOptions;
use crate::network_protocol::PeerInfo;
use anyhow::{Context as _, anyhow};
use named_lock::NamedLockGuard;
use near_primitives::network::PeerId;
use parking_lot::Mutex;
use rand::Rng;
use std::fmt;
use std::net::SocketAddr;

const LISTENER_BACKLOG: u32 = 128;

/// TEST-ONLY: guards ensuring that OS considers the given TCP listener port to be in use until
/// this OS process is terminated.
pub(crate) static RESERVED_PORT_LOCKS: std::sync::LazyLock<Mutex<Vec<NamedLockGuard>>> =
    std::sync::LazyLock::new(|| Mutex::new(Vec::new()));

/// TCP connections established by a node belong to different logical networks (aka tiers),
/// which serve different purpose.
// TODO(gprusak): add a link to the design on github docs (but first write those docs).
#[derive(Clone, Copy, Debug, PartialEq, Eq, strum::AsRefStr, strum::IntoStaticStr)]
pub enum Tier {
    /// Tier1 connections are established between the BFT consensus participants (or their proxies)
    /// and are reserved exclusively for exchanging BFT consensus messages.
    T1,
    /// Tier2 connections form a P2P gossip network, which is used for everything, except the BFT
    /// consensus messages. Also, Tier1 peer discovery actually happens on Tier2 network, i.e.
    /// Tier2 network is necessary to bootstrap Tier1 connections.
    T2,
    /// Tier3 connections are created ad hoc to directly transfer large messages, e.g. state parts.
    /// Requests for state parts are routed over Tier2. A node receiving such a request initiates a
    /// direct Tier3 connections to send the response. By sending large responses over dedicated
    /// connections we avoid delaying other messages and we minimize network bandwidth usage.
    T3,
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
        if let Err(err) = stream.set_nodelay(true) {
            tracing::warn!(target: "network", "Failed to set TCP_NODELAY: {}", err);
        }
        Ok(Self { peer_addr: stream.peer_addr()?, local_addr: stream.local_addr()?, stream, type_ })
    }

    pub async fn connect(
        peer_info: &PeerInfo,
        tier: Tier,
        socket_options: &SocketOptions,
    ) -> anyhow::Result<Stream> {
        let addr = peer_info
            .addr
            .ok_or_else(|| anyhow!("Trying to connect to peer with no public address"))?;

        let socket = match addr {
            std::net::SocketAddr::V4(_) => tokio::net::TcpSocket::new_v4()?,
            std::net::SocketAddr::V6(_) => tokio::net::TcpSocket::new_v6()?,
        };

        // Avoid setting the buffer sizes for T1 connections, which are numerous and lightweight.
        match tier {
            Tier::T2 => {
                if let Some(so_recv_buffer) = socket_options.recv_buffer_size {
                    socket.set_recv_buffer_size(so_recv_buffer)?;
                    tracing::debug!(target: "network", "so_recv_buffer wanted {} got {:?}", so_recv_buffer, socket.recv_buffer_size());
                }

                if let Some(so_send_buffer) = socket_options.send_buffer_size {
                    socket.set_send_buffer_size(so_send_buffer)?;
                    tracing::debug!(target: "network", "so_send_buffer wanted {} got {:?}", so_send_buffer, socket.send_buffer_size());
                }
            }
            _ => {}
        };

        // The `connect` may take several minutes. This happens when the
        // `SYN` packet for establishing a TCP connection gets silently
        // dropped, in which case the default TCP timeout is applied. That's
        // too long for us, so we shorten it to one second.
        //
        // Why exactly a second? It was hard-coded in a library we used
        // before, so we keep it to preserve behavior. Removing the timeout
        // completely was observed to break stuff for real on the testnet.
        let stream = tokio::time::timeout(std::time::Duration::from_secs(1), socket.connect(addr))
            .await?
            .context("TcpStream::connect()")?;
        Ok(Stream::new(stream, StreamType::Outbound { peer_id: peer_info.id.clone(), tier })?)
    }

    /// Establishes a loopback TCP connection to localhost with random ports.
    /// Returns a pair of streams: (outbound,inbound).
    #[cfg(test)]
    pub async fn loopback(peer_id: PeerId, tier: Tier) -> (Stream, Stream) {
        let listener_addr = ListenerAddr::reserve_for_test();
        let peer_info = PeerInfo { id: peer_id, addr: Some(*listener_addr), account_id: None };
        let socket_options = SocketOptions::default();
        let listener = listener_addr.listener().unwrap();
        let (outbound, inbound) =
            tokio::join!(Stream::connect(&peer_info, tier, &socket_options), listener.accept());
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

/// ListenerAddr is isomorphic to std::net::SocketAddr, but it should be used
/// solely for opening a TCP listener socket on it.
///
/// In tests it additionally allows to "reserve" a random unused TCP port:
/// * it allows to avoid race conditions in tests which require a dedicated TCP port to spawn a
///   node on (and potentially restart it every now and then).
/// * it is implemented by unsigned SO_REUSEPORT socket option (do not confuse with SO_REUSEADDR),
///   which allows multiple sockets to share a port. reserve_for_test() creates a socket and binds
///   it to a random unused local port (without starting a TCP listener).
///   This socket won't be used for anything but telling the OS that the given TCP port is in use.
///   However thanks to SO_REUSEPORT we can create another socket bind it to the same port
///   and make it a listener.
/// * The reserved port stays reserved until the process terminates - hence during a process
///   lifetime reserve_for_test() should be called a small amount of times (~1000 should be fine,
///   there are only 2^16 ports on a network interface). TODO(gprusak): we may want to track the
///   lifecycle of ListenerAddr (for example via reference counter), so that we can reuse the port
///   after all the references are dropped.
/// * The drawback of this solution that it is hard to detect a situation in which multiple
///   listener sockets in test are bound to the same port. TODO(gprusak): we can prevent creating
///   multiple listeners for a single port within the same process by adding a mutex to the port
///   guard.
#[derive(serde::Serialize, serde::Deserialize, Clone, Copy, PartialEq, Eq)]
pub struct ListenerAddr(std::net::SocketAddr);

impl fmt::Debug for ListenerAddr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl fmt::Display for ListenerAddr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl std::ops::Deref for ListenerAddr {
    type Target = std::net::SocketAddr;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl ListenerAddr {
    pub fn new(addr: std::net::SocketAddr) -> Self {
        assert!(
            addr.port() != 0,
            "using an any port (i.e. 0) for the tcp::ListenerAddr is allowed only \
             in tests and only via reserve_for_test() method"
        );
        Self(addr)
    }

    /// TEST-ONLY: reserves a random port on localhost for a TCP listener.
    /// cspell:ignore REUSEADDR REUSEPORT
    pub fn reserve_for_test() -> Self {
        for _ in 0..1000 {
            // Randomize a port and then first check the named lock. We need a named lock instead of a mutex because
            // tests are run in multiple processes. Once we obtain the lock, we try the port to see if we can listen on
            // it. If we can't, then try again - the system may be using the port for other purposes. If we can, then
            // we release the port and then use this port as the testing port.
            // Why do we lock first and then check the port? This is because after we reserve a port, the test may not
            // use it right away, and during this time, someone else may check the port and listen on it for a brief
            // moment (before realizing the lock isn't available), but that brief moment may cause the test to fail if
            // the listener cannot listen on that port.
            //
            // Note: an old implementation asked the OS for a port (by listening on port 0) and then held the socket
            // without releasing it, and then allow the test to re-use the port by specifying set_reuseport. However,
            // somehow this leads to flaky tests. It's not clear why, but the port reuse trick feels fragile.
            let port = rand::thread_rng().gen_range(20000..65536);
            let lock =
                named_lock::NamedLock::create(&format!("nearcore_test_reserved_port_{}", port))
                    .unwrap();
            let try_lock = lock.try_lock();
            let guard = match try_lock {
                Ok(guard) => guard,
                Err(named_lock::Error::WouldBlock) => {
                    // this port is already reserved, try another one
                    tracing::trace!(target: "network", "Port {} is already reserved, trying another one", port);
                    continue;
                }
                Err(err) => {
                    panic!("Failed to create a named lock for port {}: {}", port, err);
                }
            };
            let addr: SocketAddr = format!("[::1]:{}", port).parse().unwrap();
            let tcp_socket = std::net::TcpListener::bind(addr);
            if tcp_socket.is_err() {
                // this port is already in use, try another one
                tracing::trace!(target: "network", "Port {} is already in use, trying another one", port);
                continue;
            }
            RESERVED_PORT_LOCKS.lock().push(guard);
            return Self(addr);
        }
        panic!("Failed to reserve a TCP port for a listener after 1000 attempts");
    }

    /// Constructs a std::net::TcpListener, for usage outside of near_network.
    pub fn std_listener(&self) -> std::io::Result<std::net::TcpListener> {
        self.listener()?.0.into_std()
    }

    /// Constructs a Listener out of ListenerAddr.
    pub(crate) fn listener(&self) -> std::io::Result<Listener> {
        let socket = match &self.0 {
            std::net::SocketAddr::V4(_) => tokio::net::TcpSocket::new_v4()?,
            std::net::SocketAddr::V6(_) => tokio::net::TcpSocket::new_v6()?,
        };
        socket.set_reuseaddr(true)?;
        socket.bind(self.0)?;
        Ok(Listener(socket.listen(LISTENER_BACKLOG)?))
    }

    pub(crate) fn is_ipv4(&self) -> bool {
        self.0.is_ipv4()
    }
}

pub(crate) struct Listener(tokio::net::TcpListener);

impl Listener {
    pub async fn accept(&self) -> std::io::Result<Stream> {
        let (stream, _) = self.0.accept().await?;
        Stream::new(stream, StreamType::Inbound)
    }
}
