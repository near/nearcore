/// Conversion functions for the messages representing network primitives.
use super::*;

use crate::network_protocol::proto;
use crate::network_protocol::PeerAddr;
use crate::network_protocol::{Edge, PartialEdgeInfo, PeerInfo};
use borsh::{BorshDeserialize as _, BorshSerialize as _};
use near_primitives::network::AnnounceAccount;
use protobuf::MessageField as MF;
use std::net::{IpAddr, SocketAddr};

#[derive(thiserror::Error, Debug)]
pub enum ParseSocketAddrError {
    #[error("invalid IP")]
    InvalidIP,
    #[error("invalid port")]
    InvalidPort,
}

impl From<&SocketAddr> for proto::SocketAddr {
    fn from(x: &SocketAddr) -> Self {
        Self {
            ip: match x.ip() {
                IpAddr::V4(ip) => ip.octets().to_vec(),
                IpAddr::V6(ip) => ip.octets().to_vec(),
            },
            port: x.port() as u32,
            ..Self::default()
        }
    }
}

impl TryFrom<&proto::SocketAddr> for SocketAddr {
    type Error = ParseSocketAddrError;
    fn try_from(x: &proto::SocketAddr) -> Result<Self, Self::Error> {
        let ip = match x.ip.len() {
            4 => IpAddr::from(<[u8; 4]>::try_from(&x.ip[..]).unwrap()),
            16 => IpAddr::from(<[u8; 16]>::try_from(&x.ip[..]).unwrap()),
            _ => return Err(Self::Error::InvalidIP),
        };
        let port = u16::try_from(x.port).map_err(|_| Self::Error::InvalidPort)?;
        Ok(SocketAddr::new(ip, port))
    }
}

////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum ParsePeerAddrError {
    #[error("addr: {0}")]
    Addr(ParseRequiredError<ParseSocketAddrError>),
    #[error("peer_id: {0}")]
    PeerId(ParseRequiredError<ParsePublicKeyError>),
}

impl From<&PeerAddr> for proto::PeerAddr {
    fn from(x: &PeerAddr) -> Self {
        Self {
            addr: MF::some((&x.addr).into()),
            peer_id: MF::some((&x.peer_id).into()),
            ..Self::default()
        }
    }
}

impl TryFrom<&proto::PeerAddr> for PeerAddr {
    type Error = ParsePeerAddrError;
    fn try_from(x: &proto::PeerAddr) -> Result<Self, Self::Error> {
        Ok(Self {
            addr: try_from_required(&x.addr).map_err(Self::Error::Addr)?,
            peer_id: try_from_required(&x.peer_id).map_err(Self::Error::PeerId)?,
        })
    }
}

////////////////////////////////////////

impl From<&PeerInfo> for proto::PeerInfo {
    fn from(x: &PeerInfo) -> Self {
        Self { borsh: x.try_to_vec().unwrap(), ..Self::default() }
    }
}

pub type ParsePeerInfoError = borsh::maybestd::io::Error;

impl TryFrom<&proto::PeerInfo> for PeerInfo {
    type Error = ParsePeerInfoError;
    fn try_from(x: &proto::PeerInfo) -> Result<Self, Self::Error> {
        Self::try_from_slice(&x.borsh)
    }
}

////////////////////////////////////////

pub type ParsePartialEdgeInfoError = borsh::maybestd::io::Error;

impl From<&PartialEdgeInfo> for proto::PartialEdgeInfo {
    fn from(x: &PartialEdgeInfo) -> Self {
        Self { borsh: x.try_to_vec().unwrap(), ..Self::default() }
    }
}

impl TryFrom<&proto::PartialEdgeInfo> for PartialEdgeInfo {
    type Error = ParsePartialEdgeInfoError;
    fn try_from(p: &proto::PartialEdgeInfo) -> Result<Self, Self::Error> {
        Self::try_from_slice(&p.borsh)
    }
}

////////////////////////////////////////

pub type ParseEdgeError = borsh::maybestd::io::Error;

impl From<&Edge> for proto::Edge {
    fn from(x: &Edge) -> Self {
        Self { borsh: x.try_to_vec().unwrap(), ..Self::default() }
    }
}

impl TryFrom<&proto::Edge> for Edge {
    type Error = ParseEdgeError;
    fn try_from(x: &proto::Edge) -> Result<Self, Self::Error> {
        Self::try_from_slice(&x.borsh)
    }
}

////////////////////////////////////////

pub type ParseAnnounceAccountError = borsh::maybestd::io::Error;

impl From<&AnnounceAccount> for proto::AnnounceAccount {
    fn from(x: &AnnounceAccount) -> Self {
        Self { borsh: x.try_to_vec().unwrap(), ..Self::default() }
    }
}

impl TryFrom<&proto::AnnounceAccount> for AnnounceAccount {
    type Error = ParseAnnounceAccountError;
    fn try_from(x: &proto::AnnounceAccount) -> Result<Self, Self::Error> {
        Self::try_from_slice(&x.borsh)
    }
}
