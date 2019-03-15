use crate::types::{AccountId, PeerId};
use crate::hash::CryptoHash;
use crate::chain::ChainState;
use std::borrow::Borrow;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::net::SocketAddr;
use std::convert::TryFrom;
use std::iter::FromIterator;
use near_protos::network as network_proto;
use protobuf::{SingularPtrField, RepeatedField};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PeerAddr {
    pub id: PeerId,
    pub addr: SocketAddr,
}

impl PeerAddr {
    pub fn parse(addr_id: &str) -> Result<Self, String> {
        let addr_id: Vec<_> = addr_id.split('/').collect();
        let (addr, id) = (addr_id[0], addr_id[1]);
        Ok(PeerAddr {
            id: String::into(id.to_string()),
            addr: addr.parse::<SocketAddr>().map_err(|e| format!("Error parsing address {:?}: {:?}", addr, e))?,
        })
    }
}

impl TryFrom<PeerInfo> for PeerAddr {
    type Error = String;

    fn try_from(peer_info: PeerInfo) -> Result<Self, Self::Error> {
        match peer_info.addr {
            Some(addr) => Ok(PeerAddr { id: peer_info.id, addr }),
            None => Err(format!("PeerInfo {:?} doesn't have an address", peer_info))
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

impl From<network_proto::PeerInfo> for PeerInfo {
    fn from(proto: network_proto::PeerInfo) -> Self {
        let addr = match &proto.addr {
            Some(network_proto::PeerInfo_oneof_addr::no_addr(_)) => None,
            Some(network_proto::PeerInfo_oneof_addr::some_addr(s)) => {
                s.parse::<SocketAddr>().ok()
            }
            None => unreachable!()
        };
        let account_id = match &proto.account_id {
            Some(network_proto::PeerInfo_oneof_account_id::no_account_id(_)) => None,
            Some(network_proto::PeerInfo_oneof_account_id::some_account_id(id)) => {
                Some(id.clone())
            }
            None => unreachable!()
        };
        PeerInfo {
            id: CryptoHash::from(proto.id),
            addr,
            account_id,
        }
    }
}

impl Into<network_proto::PeerInfo> for PeerInfo {
    fn into(self) -> network_proto::PeerInfo {
        let id = self.id;
        let addr = match self.addr {
            Some(addr) => network_proto::PeerInfo_oneof_addr::some_addr(format!("{}", addr)),
            None => network_proto::PeerInfo_oneof_addr::no_addr(true),
        };
        let account_id = match self.account_id {
            Some(account_id) => network_proto::PeerInfo_oneof_account_id::some_account_id(account_id),
            None => network_proto::PeerInfo_oneof_account_id::no_account_id(true)
        };
        network_proto::PeerInfo {
            id: id.into(),
            addr: Some(addr),
            account_id: Some(account_id),
            unknown_fields: Default::default(),
            cached_size: Default::default(),
        }
    }
}

pub type PeersInfo = Vec<PeerInfo>;

#[derive(PartialEq, Eq, Debug, Serialize, Deserialize, Clone)]
pub struct ConnectedInfo {
    pub chain_state: ChainState,
}

impl From<network_proto::ConnectedInfo> for ConnectedInfo {
    fn from(proto: network_proto::ConnectedInfo) -> Self {
        ConnectedInfo {
            chain_state: proto.chain_state.unwrap().into()
        }
    }
}

impl Into<network_proto::ConnectedInfo> for ConnectedInfo {
    fn into(self) -> network_proto::ConnectedInfo {
        network_proto::ConnectedInfo {
            chain_state: SingularPtrField::some(self.chain_state.into()),
            unknown_fields: Default::default(),
            cached_size: Default::default(),
        }
    }
}

#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
pub struct Handshake {
    /// Protocol version.
    pub version: u32,
    /// Sender's peer id.
    pub peer_id: PeerId,
    /// Sender's account id, if present.
    pub account_id: Option<AccountId>,
    /// Sender's listening addr.
    pub listen_port: Option<u16>,
    /// Sender's information about known peers.
    pub peers_info: PeersInfo,
    /// Connected info message that peer receives.
    pub connected_info: ConnectedInfo,
}

impl From<network_proto::HandShake> for Handshake {
    fn from(proto: network_proto::HandShake) -> Self {
        let account_id = match &proto.account_id {
            Some(network_proto::HandShake_oneof_account_id::no_account_id(_)) => None,
            Some(network_proto::HandShake_oneof_account_id::some_account_id(id)) => {
                Some(id.clone())
            }
            None => unreachable!()
        };
        let listen_port = match &proto.listen_port {
            Some(network_proto::HandShake_oneof_listen_port::no_listen_port(_)) => None,
            Some(network_proto::HandShake_oneof_listen_port::some_listen_port(port)) => {
                Some(*port as u16)
            }
            None => unreachable!()
        };
        Handshake {
            version: proto.version,
            peer_id: proto.peer_id.into(),
            account_id,
            listen_port,
            peers_info: proto.peers_info.into_iter().map(std::convert::Into::into).collect(),
            connected_info: proto.connected_info.unwrap().into(),
        }
    }
}

impl Into<network_proto::HandShake> for Handshake {
    fn into(self) -> network_proto::HandShake {
        let account_id = match self.account_id {
            Some(account_id) => network_proto::HandShake_oneof_account_id::some_account_id(account_id),
            None => network_proto::HandShake_oneof_account_id::no_account_id(true),
        };
        let listen_port = match self.listen_port {
            Some(port) => network_proto::HandShake_oneof_listen_port::some_listen_port(u32::from(port)),
            None => network_proto::HandShake_oneof_listen_port::no_listen_port(true),
        };
        network_proto::HandShake {
            version: self.version,
            peer_id: self.peer_id.into(),
            peers_info: RepeatedField::from_iter(
                self.peers_info.into_iter().map(std::convert::Into::into)
            ),
            connected_info: SingularPtrField::some(self.connected_info.into()),
            account_id: Some(account_id),
            listen_port: Some(listen_port),
            unknown_fields: Default::default(),
            cached_size: Default::default(),
        }
    }
}

#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
pub enum PeerMessage {
    Handshake(Handshake),
    InfoGossip(PeersInfo),
    Message(Vec<u8>),
}


impl From<network_proto::PeerMessage> for PeerMessage {
    fn from(proto: network_proto::PeerMessage) -> Self {
        match proto.m {
            Some(network_proto::PeerMessage_oneof_m::hand_shake(hand_shake)) => {
                PeerMessage::Handshake(hand_shake.into())
            }
            Some(network_proto::PeerMessage_oneof_m::info_gossip(gossip)) => {
                let peer_info = gossip.info_gossip.into_iter().map(std::convert::Into::into).collect();
                PeerMessage::InfoGossip(peer_info)
            }
            Some(network_proto::PeerMessage_oneof_m::message(message)) => {
                PeerMessage::Message(message)
            }
            None => unreachable!()
        }
    }
}

impl Into<network_proto::PeerMessage> for PeerMessage {
    fn into(self) -> network_proto::PeerMessage {
        let m = match self {
            PeerMessage::Handshake(hand_shake) => {
                Some(network_proto::PeerMessage_oneof_m::hand_shake(hand_shake.into()))
            }
            PeerMessage::InfoGossip(peers_info) => {
                let gossip = network_proto::InfoGossip {
                    info_gossip: RepeatedField::from_iter(peers_info.into_iter().map(std::convert::Into::into)),
                    unknown_fields: Default::default(),
                    cached_size: Default::default(),
                };
                Some(network_proto::PeerMessage_oneof_m::info_gossip(gossip))
            }
            PeerMessage::Message(message) => {
                Some(network_proto::PeerMessage_oneof_m::message(message))
            }
        };
        network_proto::PeerMessage {
            m,
            unknown_fields: Default::default(),
            cached_size: Default::default(),
        }
    }
}