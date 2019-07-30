use near_protos::network as network_proto;
use protobuf::well_known_types::UInt32Value;
use protobuf::{RepeatedField, SingularPtrField};
use std::borrow::Borrow;
use std::convert::{Into, TryFrom, TryInto};
use std::fmt;
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::iter::FromIterator;
use std::net::SocketAddr;

use crate::hash::CryptoHash;
use crate::types::{AccountId, PeerId};
use crate::utils::to_string_value;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PeerAddr {
    pub id: PeerId,
    pub addr: SocketAddr,
}

impl PeerAddr {
    pub fn parse(addr_id: &str) -> Result<Self, Box<std::error::Error>> {
        let addr_id: Vec<_> = addr_id.split('/').collect();
        let (addr, id) = (addr_id[0], addr_id[1]);
        Ok(PeerAddr {
            id: id.to_string().try_into()?,
            addr: addr
                .parse::<SocketAddr>()
                .map_err(|e| format!("Error parsing address {:?}: {:?}", addr, e))?,
        })
    }
}

impl Display for PeerAddr {
    fn fmt(&self, f: &mut Formatter) -> Result<(), std::fmt::Error> {
        write!(f, "{}/{}", self.addr, self.id)
    }
}

impl TryFrom<PeerInfo> for PeerAddr {
    type Error = Box<std::error::Error>;

    fn try_from(peer_info: PeerInfo) -> Result<Self, Self::Error> {
        match peer_info.addr {
            Some(addr) => Ok(PeerAddr { id: peer_info.id, addr }),
            None => Err(format!("PeerInfo {:?} doesn't have an address", peer_info).into()),
        }
    }
}

/// Info about the peer. If peer is an authority then we also know its account id.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PeerInfo {
    pub id: PeerId,
    pub addr: Option<SocketAddr>,
    pub account_id: Option<AccountId>,
}

impl PeerInfo {
    pub fn addr_port(&self) -> Option<u16> {
        self.addr.map(|addr| addr.port())
    }
}

impl PartialEq for PeerInfo {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Hash for PeerInfo {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl Eq for PeerInfo {}

impl fmt::Display for PeerInfo {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if let Some(acc) = self.account_id.as_ref() {
            write!(f, "({}, {:?}, {})", self.id, self.addr, acc)
        } else {
            write!(f, "({}, {:?})", self.id, self.addr)
        }
    }
}

impl Borrow<PeerId> for PeerInfo {
    fn borrow(&self) -> &PeerId {
        &self.id
    }
}

impl From<PeerAddr> for PeerInfo {
    fn from(node_addr: PeerAddr) -> Self {
        PeerInfo { id: node_addr.id, addr: Some(node_addr.addr), account_id: None }
    }
}

impl TryFrom<network_proto::PeerInfo> for PeerInfo {
    type Error = Box<std::error::Error>;

    fn try_from(proto: network_proto::PeerInfo) -> Result<Self, Self::Error> {
        let addr = proto.addr.into_option().and_then(|s| s.value.parse::<SocketAddr>().ok());
        let account_id = proto.account_id.into_option().map(|s| s.value);
        Ok(PeerInfo { id: CryptoHash::try_from(proto.id)?, addr, account_id })
    }
}

impl From<PeerInfo> for network_proto::PeerInfo {
    fn from(peer_info: PeerInfo) -> network_proto::PeerInfo {
        let id = peer_info.id;
        let addr = SingularPtrField::from_option(
            peer_info.addr.map(|s| to_string_value(format!("{}", s))),
        );
        let account_id = SingularPtrField::from_option(peer_info.account_id.map(to_string_value));
        network_proto::PeerInfo { id: id.into(), addr, account_id, ..Default::default() }
    }
}

pub type PeersInfo = Vec<PeerInfo>;

#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
pub struct Handshake {
    /// Protocol version.
    pub version: u32,
    /// Sender's peer id.
    pub peer_id: PeerId,
    /// Sender's listening addr.
    pub listen_port: Option<u16>,
    /// Sender's information about known peers.
    pub peers_info: PeersInfo,
}

impl TryFrom<network_proto::Handshake> for Handshake {
    type Error = Box<std::error::Error>;

    fn try_from(proto: network_proto::Handshake) -> Result<Self, Self::Error> {
        let account_id = proto.account_id.into_option().map(|s| s.value);
        let listen_port = proto.listen_port.into_option().map(|v| v.value as u16);
        let peers_info =
            proto.peers_info.into_iter().map(TryInto::try_into).collect::<Result<Vec<_>, _>>()?;
        Ok(Handshake {
            version: proto.version,
            peer_id: proto.peer_id.try_into()?,
            listen_port,
            peers_info,
        })
    }
}

impl From<Handshake> for network_proto::Handshake {
    fn from(hand_shake: Handshake) -> network_proto::Handshake {
        let account_id = SingularPtrField::from_option(hand_shake.account_id.map(to_string_value));
        let listen_port = SingularPtrField::from_option(hand_shake.listen_port.map(|v| {
            let mut res = UInt32Value::new();
            res.set_value(u32::from(v));
            res
        }));
        network_proto::Handshake {
            version: hand_shake.version,
            peer_id: hand_shake.peer_id.into(),
            peers_info: RepeatedField::from_iter(
                hand_shake.peers_info.into_iter().map(std::convert::Into::into),
            ),
            account_id,
            listen_port,
            ..Default::default()
        }
    }
}
