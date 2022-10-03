use near_primitives::network::PeerId;
use crate::network_protocol::PeerInfo;
use anyhow::{Context as _,anyhow};

#[derive(Clone,Debug)]
pub(crate) enum StreamType {
    Inbound,
    Outbound { peer_id: PeerId },
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
#[derive(Debug,Clone,Copy,PartialEq,Eq)]
pub(crate) struct StreamId {
    inbound: std::net::SocketAddr,
    outbound: std::net::SocketAddr,
}

impl Stream {
    fn new(stream: tokio::net::TcpStream, type_: StreamType) -> std::io::Result<Self> {
        Ok(Self {
            peer_addr: stream.peer_addr()?,
            local_addr: stream.local_addr()?,
            stream,
            type_,
        })
    }

    pub async fn connect(peer_info:&PeerInfo) -> anyhow::Result<Stream> {
        let addr = peer_info.addr.ok_or(anyhow!("Trying to connect to peer with no public address"))?;
        // The `connect` may take several minutes. This happens when the
        // `SYN` packet for establishing a TCP connection gets silently
        // dropped, in which case the default TCP timeout is applied. That's
        // too long for us, so we shorten it to one second.
        //
        // Why exactly a second? It was hard-coded in a library we used
        // before, so we keep it to preserve behavior. Removing the timeout
        // completely was observed to break stuff for real on the testnet.
        let stream = tokio::time::timeout(std::time::Duration::from_secs(1), tokio::net::TcpStream::connect(addr)).await?.context("TcpStream::connect()")?;
        Ok(Stream::new(stream,StreamType::Outbound{ peer_id: peer_info.id.clone() })?)
    }

    /// Establishes a loopback TCP connection to localhost with random ports.
    /// Returns a pair of streams: (outbound,inbound).
    #[cfg(test)]
    pub async fn loopback(peer_id:PeerId) -> (Stream,Stream) {
        let localhost = std::net::SocketAddr::new(std::net::Ipv4Addr::LOCALHOST.into(),0);
        let mut listener = Listener::bind(localhost).await.unwrap();
        let peer_info = PeerInfo {
            id: peer_id,
            addr: Some(listener.0.local_addr().unwrap()),
            account_id: None,
        };
        let (outbound,inbound) = tokio::join!(
            Stream::connect(&peer_info),
            listener.accept(),
        );
        (outbound.unwrap(),inbound.unwrap())
    }

    // TEST-ONLY used in reporting test events.
    pub(crate) fn id(&self) -> StreamId {
        match self.type_ {
            StreamType::Inbound => StreamId {
                inbound: self.local_addr,
                outbound: self.peer_addr,
            },
            StreamType::Outbound{..} => StreamId {
                inbound: self.peer_addr,
                outbound: self.local_addr,
            },
        }
    }
}

pub(crate) struct Listener(tokio::net::TcpListener);

impl Listener {
    // TODO(gprusak): this shouldn't be async. It is only
    // because TcpListener accepts anything that asynchronously resolves to SocketAddr.
    pub async fn bind(addr: std::net::SocketAddr) -> std::io::Result<Self> {
        Ok(Self(tokio::net::TcpListener::bind(addr).await?))
    }

    pub async fn accept(&mut self) -> std::io::Result<Stream> {
        let (stream,_) = self.0.accept().await?;
        Stream::new(stream,StreamType::Inbound)
    }
}
