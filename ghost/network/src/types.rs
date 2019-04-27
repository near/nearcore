use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};

#[derive(Clone, Debug)]
pub struct PeerAddr(pub SocketAddr);

#[derive(Clone, Debug)]
pub struct PeerInfo {
    pub addr: PeerAddr,
}
