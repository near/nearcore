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
use protobuf::well_known_types::{StringValue, UInt32Value};

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
        let addr = proto.addr
            .into_option()
            .and_then(|s| s.value.parse::<SocketAddr>().ok());
        let account_id = proto.account_id.into_option().map(|s| s.value);
        PeerInfo {
            id: CryptoHash::from(proto.id),
            addr,
            account_id,
        }
    }
}

fn to_string_value(s: String) -> StringValue {
    let mut res = StringValue::new();
    res.set_value(s);
    res
}

impl From<PeerInfo> for network_proto::PeerInfo {
    fn from(peer_info: PeerInfo) -> network_proto::PeerInfo {
        let id = peer_info.id;
        let addr = SingularPtrField::from_option(peer_info.addr.map(|s| {
            to_string_value(format!("{}", s))
        }));
        let account_id = SingularPtrField::from_option(
            peer_info.account_id.map(to_string_value)
        );
        network_proto::PeerInfo {
            id: id.into(),
            addr,
            account_id,
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

impl From<ConnectedInfo> for network_proto::ConnectedInfo {
    fn from(connected_info: ConnectedInfo) -> network_proto::ConnectedInfo {
        network_proto::ConnectedInfo {
            chain_state: SingularPtrField::some(connected_info.chain_state.into()),
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
        let account_id = proto.account_id.into_option().map(|s| s.value);
        let listen_port = proto.listen_port.into_option().map(|v| v.value as u16);
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

impl From<Handshake> for network_proto::HandShake {
    fn from(hand_shake: Handshake) -> network_proto::HandShake {
        let account_id = SingularPtrField::from_option(
            hand_shake.account_id.map(to_string_value)
        );
        let listen_port = SingularPtrField::from_option(hand_shake.listen_port.map(|v| {
            let mut res = UInt32Value::new();
            res.set_value(u32::from(v));
            res
        }));
        network_proto::HandShake {
            version: hand_shake.version,
            peer_id: hand_shake.peer_id.into(),
            peers_info: RepeatedField::from_iter(
                hand_shake.peers_info.into_iter().map(std::convert::Into::into)
            ),
            connected_info: SingularPtrField::some(hand_shake.connected_info.into()),
            account_id,
            listen_port,
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
        match proto.message_type {
            Some(network_proto::PeerMessage_oneof_message_type::hand_shake(hand_shake)) => {
                PeerMessage::Handshake(hand_shake.into())
            }
            Some(network_proto::PeerMessage_oneof_message_type::info_gossip(gossip)) => {
                let peer_info = gossip.info_gossip.into_iter().map(std::convert::Into::into).collect();
                PeerMessage::InfoGossip(peer_info)
            }
            Some(network_proto::PeerMessage_oneof_message_type::message(message)) => {
                PeerMessage::Message(message)
            }
            None => unreachable!()
        }
    }
}

impl From<PeerMessage> for network_proto::PeerMessage {
    fn from(message: PeerMessage) -> network_proto::PeerMessage {
        let message_type = match message {
            PeerMessage::Handshake(hand_shake) => {
                Some(network_proto::PeerMessage_oneof_message_type::hand_shake(hand_shake.into()))
            }
            PeerMessage::InfoGossip(peers_info) => {
                let gossip = network_proto::InfoGossip {
                    info_gossip: RepeatedField::from_iter(
                        peers_info.into_iter().map(std::convert::Into::into)
                    ),
                    unknown_fields: Default::default(),
                    cached_size: Default::default(),
                };
                Some(network_proto::PeerMessage_oneof_message_type::info_gossip(gossip))
            }
            PeerMessage::Message(message) => {
                Some(network_proto::PeerMessage_oneof_message_type::message(message))
            }
        };
        network_proto::PeerMessage {
            message_type,
            unknown_fields: Default::default(),
            cached_size: Default::default(),
        }
    }
}