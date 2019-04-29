use std::convert::{Into, TryFrom, TryInto};
use std::fmt;
use std::iter::FromIterator;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};
use std::time::Duration;

use actix::{Actor, Message, Recipient};
use chrono::{DateTime, Utc};
use protobuf::well_known_types::UInt32Value;
use protobuf::{RepeatedField, SingularPtrField};
use serde_derive::{Deserialize, Serialize};
use tokio::net::{TcpListener, TcpStream};

use near_chain::{Block, BlockHeader};
use near_protos::network as network_proto;
use primitives::crypto::signature::{PublicKey, SecretKey};
use primitives::hash::CryptoHash;
use primitives::logging::pretty_str;
use primitives::transaction::SignedTransaction;
use primitives::types::AccountId;
use primitives::utils::{proto_to_result, proto_to_type, to_string_value};

/// Current latest version of the protocol
pub const PROTOCOL_VERSION: u32 = 1;

use crate::peer::Peer;
use actix::dev::{MessageResponse, ResponseChannel};
use primitives::traits::Base58Encoded;
use std::hash::{Hash, Hasher};

/// Peer id is the public key.
#[derive(Copy, Clone, Eq, PartialOrd, Ord, PartialEq, Serialize, Deserialize, Debug)]
pub struct PeerId(PublicKey);

impl From<PeerId> for Vec<u8> {
    fn from(peer_id: PeerId) -> Vec<u8> {
        peer_id.0.into()
    }
}

impl From<PublicKey> for PeerId {
    fn from(public_key: PublicKey) -> PeerId {
        PeerId(public_key)
    }
}

impl Hash for PeerId {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write(self.0.as_ref());
    }
}

impl fmt::Display for PeerId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", pretty_str(&self.0.to_base58(), 4))
    }
}

/// Peer information.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
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

impl PeerInfo {
    pub fn new(id: PeerId, addr: SocketAddr) -> Self {
        PeerInfo { id, addr: Some(addr), account_id: None }
    }
}

impl fmt::Display for PeerInfo {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if let Some(acc) = self.account_id.as_ref() {
            write!(f, "({}, {:?}, {})", self.id, self.addr, acc)
        } else {
            write!(f, "({}, {:?})", self.id, self.addr)
        }
    }
}

impl TryFrom<network_proto::PeerInfo> for PeerInfo {
    type Error = String;

    fn try_from(proto: network_proto::PeerInfo) -> Result<Self, Self::Error> {
        let addr = proto.addr.into_option().and_then(|s| s.value.parse::<SocketAddr>().ok());
        let account_id = proto.account_id.into_option().map(|s| s.value);
        Ok(PeerInfo { id: PublicKey::try_from(proto.id)?.into(), addr, account_id })
    }
}

impl From<PeerInfo> for network_proto::PeerInfo {
    fn from(peer_info: PeerInfo) -> network_proto::PeerInfo {
        let id = peer_info.id;
        let addr = SingularPtrField::from_option(
            peer_info.addr.map(|s| to_string_value(format!("{}", s))),
        );
        let account_id = SingularPtrField::from_option(peer_info.account_id.map(to_string_value));
        network_proto::PeerInfo { id: id.0.into(), addr, account_id, ..Default::default() }
    }
}

/// Peer type.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum PeerType {
    /// Inbound session
    Inbound,
    /// Outbound session
    Outbound,
}

/// Peer status.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum PeerStatus {
    /// Waiting for handshake.
    Connecting,
    /// Ready to go.
    Ready,
}

#[derive(PartialEq, Eq, Clone, Debug)]
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
}

impl Handshake {
    pub fn new(peer_id: PeerId, account_id: Option<AccountId>, listen_port: Option<u16>) -> Self {
        Handshake {
            version: PROTOCOL_VERSION,
            peer_id,
            account_id,
            listen_port,
            peers_info: vec![],
        }
    }
}

impl TryFrom<network_proto::HandShake> for Handshake {
    type Error = String;

    fn try_from(proto: network_proto::HandShake) -> Result<Self, Self::Error> {
        let account_id = proto.account_id.into_option().map(|s| s.value);
        let listen_port = proto.listen_port.into_option().map(|v| v.value as u16);
        let peers_info =
            proto.peers_info.into_iter().map(TryInto::try_into).collect::<Result<Vec<_>, _>>()?;
        let peer_id: PublicKey = proto.peer_id.try_into().map_err(|e| format!("{}", e))?;
        // let connected_info = proto_to_type(proto.connected_info)?;
        Ok(Handshake {
            version: proto.version,
            peer_id: peer_id.into(),
            account_id,
            listen_port,
            peers_info,
        })
    }
}

impl From<Handshake> for network_proto::HandShake {
    fn from(hand_shake: Handshake) -> network_proto::HandShake {
        let account_id = SingularPtrField::from_option(hand_shake.account_id.map(to_string_value));
        let listen_port = SingularPtrField::from_option(hand_shake.listen_port.map(|v| {
            let mut res = UInt32Value::new();
            res.set_value(u32::from(v));
            res
        }));
        network_proto::HandShake {
            version: hand_shake.version,
            peer_id: hand_shake.peer_id.into(),
            peers_info: RepeatedField::from_iter(
                hand_shake.peers_info.into_iter().map(std::convert::Into::into),
            ),
            connected_info: SingularPtrField::none(), //SingularPtrField::some(hand_shake.connected_info.into()),
            account_id,
            listen_port,
            ..Default::default()
        }
    }
}

pub type PeersInfo = Vec<PeerInfo>;

#[derive(PartialEq, Eq, Clone, Debug)]
pub enum PeerMessage {
    Handshake(Handshake),
    InfoGossip(PeersInfo),
    Message(Vec<u8>),
}

impl fmt::Display for PeerMessage {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            PeerMessage::Handshake(_) => f.write_str("Handshake"),
            PeerMessage::InfoGossip(_) => f.write_str("InfoGossip"),
            PeerMessage::Message(_) => f.write_str("Message"),
        }
    }
}

impl TryFrom<network_proto::PeerMessage> for PeerMessage {
    type Error = String;

    fn try_from(proto: network_proto::PeerMessage) -> Result<Self, Self::Error> {
        match proto.message_type {
            Some(network_proto::PeerMessage_oneof_message_type::hand_shake(hand_shake)) => {
                hand_shake.try_into().map(PeerMessage::Handshake)
            }
            Some(network_proto::PeerMessage_oneof_message_type::info_gossip(gossip)) => {
                let peer_info = gossip
                    .info_gossip
                    .into_iter()
                    .map(TryInto::try_into)
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(PeerMessage::InfoGossip(peer_info))
            }
            Some(network_proto::PeerMessage_oneof_message_type::message(message)) => {
                Ok(PeerMessage::Message(message))
            }
            None => unreachable!(),
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
                        peers_info.into_iter().map(std::convert::Into::into),
                    ),
                    ..Default::default()
                };
                Some(network_proto::PeerMessage_oneof_message_type::info_gossip(gossip))
            }
            PeerMessage::Message(message) => {
                Some(network_proto::PeerMessage_oneof_message_type::message(message))
            }
        };
        network_proto::PeerMessage { message_type, ..Default::default() }
    }
}

/// Configuration for the peer-to-peer manager.
pub struct NetworkConfig {
    pub public_key: PublicKey,
    pub private_key: SecretKey,
    pub addr: Option<SocketAddr>,
    pub boot_nodes: Vec<PeerInfo>,
    pub handshake_timeout: Duration,
    pub reconnect_delay: Duration,
    pub bootstrap_peers_period: Duration,
    pub peer_max_count: u32,
}

/// Status of the known peers.
#[derive(Serialize, Deserialize, Eq, PartialEq)]
pub enum KnownPeerStatus {
    Unknown,
    NotConnected,
    Connected,
    Banned,
}

/// Information node stores about known peers.
#[derive(Serialize, Deserialize)]
pub struct KnownPeerState {
    pub peer_info: PeerInfo,
    pub status: KnownPeerStatus,
    pub first_seen: DateTime<Utc>,
    pub last_seen: DateTime<Utc>,
}

impl KnownPeerState {
    pub fn new(peer_info: PeerInfo) -> Self {
        KnownPeerState {
            peer_info,
            status: KnownPeerStatus::Unknown,
            first_seen: Utc::now(),
            last_seen: Utc::now(),
        }
    }
}

/// Message passed over the network from peer to peer.
/// Box's are used when message is significantly larger than other enum members.
#[derive(PartialEq, Debug, Clone)]
#[allow(clippy::large_enum_variant)]
pub enum ProtocolMessage {
    /// Incoming transaction.
    Transaction(Box<SignedTransaction>),
}

impl TryFrom<network_proto::Message> for ProtocolMessage {
    type Error = String;

    fn try_from(proto: network_proto::Message) -> Result<Self, Self::Error> {
        match proto.message_type {
            //            Some(network_proto::Message_oneof_message_type::connected_info(info)) => {
            //                info.try_into().map(Message::Connected)
            //            }
            Some(network_proto::Message_oneof_message_type::transaction(tx)) => {
                tx.try_into().map(|tx| ProtocolMessage::Transaction(Box::new(tx)))
            }
            //            Some(network_proto::Message_oneof_message_type::receipt(receipt)) => {
            //                receipt.try_into().map(|receipt| Message::Receipt(Box::new(receipt)))
            //            }
            //            Some(network_proto::Message_oneof_message_type::block_announce(ann)) => {
            //                match (proto_to_type(ann.beacon_block), proto_to_type(ann.shard_block)) {
            //                    (Ok(beacon), Ok(shard)) => {
            //                        Ok(Message::BlockAnnounce(Box::new((beacon, shard))))
            //                    }
            //                    _ => Err(PROTO_ERROR.to_string()),
            //                }
            //            }
            //            Some(network_proto::Message_oneof_message_type::block_fetch_request(request)) => {
            //                Ok(Message::BlockFetchRequest(request.request_id, request.from, request.to))
            //            }
            //            Some(network_proto::Message_oneof_message_type::block_response(response)) => {
            //                let blocks: Result<Vec<_>, _> = response
            //                    .response
            //                    .into_iter()
            //                    .map(|coupled| {
            //                        match (
            //                            proto_to_type(coupled.beacon_block),
            //                            proto_to_type(coupled.shard_block),
            //                        ) {
            //                            (Ok(beacon), Ok(shard)) => Ok((beacon, shard)),
            //                            _ => Err(PROTO_ERROR.to_string()),
            //                        }
            //                    })
            //                    .collect();
            //                match blocks {
            //                    Ok(blocks) => {
            //                        Ok(Message::BlockResponse(response.request_id, blocks, response.best_index))
            //                    }
            //                    Err(e) => Err(e),
            //                }
            //            }
            //            Some(network_proto::Message_oneof_message_type::gossip(gossip)) => {
            //                gossip.try_into().map(|g| Message::Gossip(Box::new(g)))
            //            }
            //            Some(network_proto::Message_oneof_message_type::payload_gossip(payload_gossip)) => {
            //                payload_gossip.try_into().map(|g| Message::PayloadGossip(Box::new(g)))
            //            }
            //            Some(network_proto::Message_oneof_message_type::payload_request(request)) => {
            //                let payload_request = proto_to_type(request.payload)?;
            //                Ok(Message::PayloadRequest(request.request_id, payload_request))
            //            }
            //            Some(network_proto::Message_oneof_message_type::payload_snapshot_request(request)) => {
            //                Ok(Message::PayloadSnapshotRequest(
            //                    request.request_id,
            //                    request.snapshot_hash.try_into()?,
            //                ))
            //            }
            //            Some(network_proto::Message_oneof_message_type::payload_response(response)) => {
            //                match proto_to_type(response.payload) {
            //                    Ok(payload) => Ok(Message::PayloadResponse(response.request_id, payload)),
            //                    Err(e) => Err(e),
            //                }
            //            }
            //            Some(network_proto::Message_oneof_message_type::payload_snapshot_response(
            //                     response,
            //                 )) => {
            //                let snapshot = proto_to_type(response.snapshot)?;
            //                Ok(Message::PayloadSnapshotResponse(response.request_id, snapshot))
            //            }
            //            Some(network_proto::Message_oneof_message_type::joint_block_bls(joint)) => {
            //                match joint.field_type {
            //                    Some(network_proto::Message_JointBlockBLS_oneof_type::general(general)) => {
            //                        let beacon_sig = Base58Encoded::from_base58(&general.beacon_sig)
            //                            .map_err(|e| format!("cannot decode signature: {:?}", e))?;
            //                        let shard_sig = Base58Encoded::from_base58(&general.shard_sig)
            //                            .map_err(|e| format!("cannot deocde signature: {:?}", e))?;
            //                        Ok(Message::JointBlockBLS(JointBlockBLS::General {
            //                            sender_id: general.sender_id as AuthorityId,
            //                            receiver_id: general.receiver_id as AuthorityId,
            //                            beacon_hash: general.beacon_hash.try_into()?,
            //                            shard_hash: general.shard_hash.try_into()?,
            //                            beacon_sig,
            //                            shard_sig,
            //                        }))
            //                    }
            //                    Some(network_proto::Message_JointBlockBLS_oneof_type::request(request)) => {
            //                        Ok(Message::JointBlockBLS(JointBlockBLS::Request {
            //                            sender_id: request.sender_id as AuthorityId,
            //                            receiver_id: request.receiver_id as AuthorityId,
            //                            beacon_hash: request.beacon_hash.try_into()?,
            //                            shard_hash: request.shard_hash.try_into()?,
            //                        }))
            //                    }
            //                    None => unreachable!(),
            //                }
            //            }
            _ => panic!("!!"),
            None => unreachable!(),
        }
    }
}

impl From<ProtocolMessage> for network_proto::Message {
    fn from(message: ProtocolMessage) -> Self {
        let message_type = match message {
//            Message::Connected(connected_info) => {
//                network_proto::Message_oneof_message_type::connected_info(connected_info.into())
//            }
            ProtocolMessage::Transaction(tx) => {
                network_proto::Message_oneof_message_type::transaction((*tx).into())
            }
//            Message::Receipt(receipt) => {
//                network_proto::Message_oneof_message_type::receipt((*receipt).into())
//            }
//            Message::BlockAnnounce(ann) => {
//                let blocks = to_coupled_block(*ann);
//                network_proto::Message_oneof_message_type::block_announce(blocks)
//            }
//            Message::BlockFetchRequest(request_id, from, to) => {
//                let request = network_proto::Message_BlockFetchRequest {
//                    request_id,
//                    from,
//                    to,
//                    ..Default::default()
//                };
//                network_proto::Message_oneof_message_type::block_fetch_request(request)
//            }
//            Message::BlockResponse(request_id, blocks, best_index) => {
//                let response = network_proto::Message_BlockResponse {
//                    request_id,
//                    response: RepeatedField::from_iter(blocks.into_iter().map(to_coupled_block)),
//                    best_index,
//                    ..Default::default()
//                };
//                network_proto::Message_oneof_message_type::block_response(response)
//            }
//            Message::Gossip(gossip) => {
//                network_proto::Message_oneof_message_type::gossip((*gossip).into())
//            }
//            Message::PayloadGossip(payload_gossip) => {
//                network_proto::Message_oneof_message_type::payload_gossip((*payload_gossip).into())
//            }
//            Message::PayloadRequest(request_id, request) => {
//                let request = network_proto::Message_PayloadRequest {
//                    request_id,
//                    payload: SingularPtrField::some(request.into()),
//                    ..Default::default()
//                };
//                network_proto::Message_oneof_message_type::payload_request(request)
//            }
//            Message::PayloadSnapshotRequest(request_id, snapshot_hash) => {
//                let snapshot_request = network_proto::Message_PayloadSnapshotRequest {
//                    request_id,
//                    snapshot_hash: snapshot_hash.into(),
//                    ..Default::default()
//                };
//                network_proto::Message_oneof_message_type::payload_snapshot_request(
//                    snapshot_request,
//                )
//            }
//            Message::PayloadResponse(request_id, payload) => {
//                let response = network_proto::Message_PayloadResponse {
//                    request_id,
//                    payload: SingularPtrField::some(payload.into()),
//                    ..Default::default()
//                };
//                network_proto::Message_oneof_message_type::payload_response(response)
//            }
//            Message::PayloadSnapshotResponse(request_id, snapshot) => {
//                let response = network_proto::Message_PayloadSnapshotResponse {
//                    request_id,
//                    snapshot: SingularPtrField::some(snapshot.into()),
//                    ..Default::default()
//                };
//                network_proto::Message_oneof_message_type::payload_snapshot_response(response)
//            }
//            Message::JointBlockBLS(joint_bls) => match joint_bls {
//                JointBlockBLS::General {
//                    sender_id,
//                    receiver_id,
//                    beacon_hash,
//                    shard_hash,
//                    beacon_sig,
//                    shard_sig,
//                } => {
//                    let proto = network_proto::Message_JointBlockBLS_General {
//                        sender_id: sender_id as u64,
//                        receiver_id: receiver_id as u64,
//                        beacon_hash: beacon_hash.into(),
//                        shard_hash: shard_hash.into(),
//                        beacon_sig: beacon_sig.to_base58(),
//                        shard_sig: shard_sig.to_base58(),
//                        ..Default::default()
//                    };
//                    let bls_proto = network_proto::Message_JointBlockBLS {
//                        field_type: Some(network_proto::Message_JointBlockBLS_oneof_type::general(
//                            proto,
//                        )),
//                        ..Default::default()
//                    };
//                    network_proto::Message_oneof_message_type::joint_block_bls(bls_proto)
//                }
//                JointBlockBLS::Request { sender_id, receiver_id, beacon_hash, shard_hash } => {
//                    let proto = network_proto::Message_JointBlockBLS_Request {
//                        sender_id: sender_id as u64,
//                        receiver_id: receiver_id as u64,
//                        beacon_hash: beacon_hash.into(),
//                        shard_hash: shard_hash.into(),
//                        unknown_fields: Default::default(),
//                        cached_size: Default::default(),
//                    };
//                    let bls_proto = network_proto::Message_JointBlockBLS {
//                        field_type: Some(network_proto::Message_JointBlockBLS_oneof_type::request(
//                            proto,
//                        )),
//                        unknown_fields: Default::default(),
//                        cached_size: Default::default(),
//                    };
//                    network_proto::Message_oneof_message_type::joint_block_bls(bls_proto)
//                }
//            },
        };
        network_proto::Message { message_type: Some(message_type), ..Default::default() }
    }
}

/// Actor message that holds the TCP stream from an inbound TCP connection
#[derive(Message)]
pub struct InboundTcpConnect {
    /// Tcp stream of the inbound connections
    pub stream: TcpStream,
}

impl InboundTcpConnect {
    /// Method to create a new InboundTcpConnect message from a TCP stream
    pub fn new(stream: TcpStream) -> InboundTcpConnect {
        InboundTcpConnect { stream }
    }
}

/// Actor message to request the creation of an outbound TCP connection to a peer.
#[derive(Message)]
pub struct OutboundTcpConnect {
    /// Peer information of the outbound connection
    pub peer_info: PeerInfo,
}

#[derive(Message)]
pub struct SendMessage {
    pub message: ProtocolMessage,
}

/// Actor message to consolidate potential new peer.
/// Returns if connection should be kept or dropped.
pub struct Consolidate {
    pub actor: Recipient<SendMessage>,
    pub peer_info: PeerInfo,
    pub peer_type: PeerType,
}

impl Message for Consolidate {
    type Result = bool;
}

#[derive(Message)]
pub struct Unregister {
    pub peer_id: PeerId,
}

#[derive(Debug)]
pub enum NetworkRequests {
    FetchInfo,
    BlockAnnounce { block: Block },
    BlockHeaderAnnounce { header: BlockHeader },
    BlockRequest { hash: CryptoHash, peer_info: PeerInfo },
}

pub enum NetworkResponses {
    NoResponse,
    Info { num_active_peers: usize, peer_max_count: u32 },
}

impl<A, M> MessageResponse<A, M> for NetworkResponses
where
    A: Actor,
    M: Message<Result = NetworkResponses>,
{
    fn handle<R: ResponseChannel<M>>(self, _: &mut A::Context, tx: Option<R>) {
        if let Some(tx) = tx {
            tx.send(self)
        }
    }
}

impl Message for NetworkRequests {
    type Result = NetworkResponses;
}
