use std::convert::From;
use std::convert::{Into, TryFrom, TryInto};
use std::fmt;
use std::hash::{Hash, Hasher};
use std::iter::FromIterator;
use std::net::SocketAddr;
use std::time::Duration;

use actix::dev::{MessageResponse, ResponseChannel};
use actix::{Actor, Addr, Message};
use chrono::{DateTime, Utc};
use protobuf::well_known_types::UInt32Value;
use protobuf::{RepeatedField, SingularPtrField};
use reed_solomon_erasure::Shard;
use serde_derive::{Deserialize, Serialize};
use tokio::net::TcpStream;

use near_chain::{Block, BlockApproval, BlockHeader, Weight};
use near_primitives::crypto::signature::{sign, PublicKey, SecretKey, Signature};
use near_primitives::hash::{hash, CryptoHash};
use near_primitives::logging::pretty_str;
use near_primitives::merkle::MerklePath;
use near_primitives::serialize::{BaseEncode, Decode, Encode};
use near_primitives::sharding::{ChunkHash, ChunkOnePart};
use near_primitives::transaction::{ReceiptTransaction, SignedTransaction};
use near_primitives::types::{AccountId, BlockIndex, ChunkExtra, EpochId, ShardId};
use near_primitives::utils::{proto_to_type, to_string_value};
use near_protos::network as network_proto;

use crate::peer::Peer;

/// Current latest version of the protocol
pub const PROTOCOL_VERSION: u32 = 2;

/// Peer id is the public key.
#[derive(Copy, Clone, Eq, PartialOrd, Ord, PartialEq, Serialize, Deserialize)]
pub struct PeerId(PublicKey);

impl PeerId {
    pub fn public_key(&self) -> PublicKey {
        self.0
    }
}

impl From<PeerId> for Vec<u8> {
    fn from(peer_id: PeerId) -> Vec<u8> {
        (&peer_id.0).into()
    }
}

impl From<PublicKey> for PeerId {
    fn from(public_key: PublicKey) -> PeerId {
        PeerId(public_key)
    }
}

impl TryFrom<Vec<u8>> for PeerId {
    type Error = Box<dyn std::error::Error>;

    fn try_from(bytes: Vec<u8>) -> Result<PeerId, Self::Error> {
        Ok(PeerId(bytes.try_into()?))
    }
}

impl std::convert::AsRef<[u8]> for PeerId {
    fn as_ref(&self) -> &[u8] {
        &(self.0).0[..]
    }
}

impl Hash for PeerId {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write(self.0.as_ref());
    }
}

impl fmt::Display for PeerId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl fmt::Debug for PeerId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", pretty_str(&self.0.to_base(), 4))
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

impl TryFrom<&str> for PeerInfo {
    type Error = Box<dyn std::error::Error>;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        let chunks: Vec<_> = s.split("@").collect();
        if chunks.len() != 2 {
            return Err(format!("Invalid peer info format, got {}, must be id@ip_addr", s).into());
        }
        Ok(PeerInfo {
            id: PublicKey::try_from(chunks[0])?.into(),
            addr: Some(
                chunks[1].parse().map_err(|err| {
                    format!("Invalid ip address format for {}: {}", chunks[1], err)
                })?,
            ),
            account_id: None,
        })
    }
}

impl TryFrom<network_proto::PeerInfo> for PeerInfo {
    type Error = Box<dyn std::error::Error>;

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
        network_proto::PeerInfo {
            id: (&id.0).into(),
            addr,
            account_id,
            cached_size: Default::default(),
            unknown_fields: Default::default(),
        }
    }
}

/// Peer chain information.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Default)]
pub struct PeerChainInfo {
    /// Genesis hash.
    pub genesis: CryptoHash,
    /// Last known chain height of the peer.
    pub height: BlockIndex,
    /// Last known chain weight of the peer.
    pub total_weight: Weight,
}

impl TryFrom<network_proto::PeerChainInfo> for PeerChainInfo {
    type Error = Box<dyn std::error::Error>;

    fn try_from(proto: network_proto::PeerChainInfo) -> Result<Self, Self::Error> {
        Ok(PeerChainInfo {
            genesis: proto.genesis.try_into()?,
            height: proto.height,
            total_weight: proto.total_weight.into(),
        })
    }
}

impl From<PeerChainInfo> for network_proto::PeerChainInfo {
    fn from(chain_peer_info: PeerChainInfo) -> network_proto::PeerChainInfo {
        network_proto::PeerChainInfo {
            genesis: chain_peer_info.genesis.into(),
            height: chain_peer_info.height,
            total_weight: chain_peer_info.total_weight.to_num(),
            cached_size: Default::default(),
            unknown_fields: Default::default(),
        }
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
    /// Banned, should shutdown this peer.
    Banned(ReasonForBan),
}

#[derive(PartialEq, Eq, Clone, Debug)]
pub struct Handshake {
    /// Protocol version.
    pub version: u32,
    /// Sender's peer id.
    pub peer_id: PeerId,
    /// Sender's listening addr.
    pub listen_port: Option<u16>,
    /// Peer's chain information.
    pub chain_info: PeerChainInfo,
}

impl Handshake {
    pub fn new(peer_id: PeerId, listen_port: Option<u16>, chain_info: PeerChainInfo) -> Self {
        Handshake { version: PROTOCOL_VERSION, peer_id, listen_port, chain_info }
    }
}

impl TryFrom<network_proto::Handshake> for Handshake {
    type Error = Box<dyn std::error::Error>;

    fn try_from(proto: network_proto::Handshake) -> Result<Self, Self::Error> {
        let listen_port = proto.listen_port.into_option().map(|v| v.value as u16);
        let peer_id: PublicKey = proto.peer_id.try_into().map_err(|e| format!("{}", e))?;
        let chain_info = proto_to_type(proto.chain_info)?;
        Ok(Handshake { version: proto.version, peer_id: peer_id.into(), listen_port, chain_info })
    }
}

impl From<Handshake> for network_proto::Handshake {
    fn from(handshake: Handshake) -> network_proto::Handshake {
        let listen_port = SingularPtrField::from_option(handshake.listen_port.map(|v| {
            let mut res = UInt32Value::new();
            res.set_value(u32::from(v));
            res
        }));
        network_proto::Handshake {
            version: handshake.version,
            peer_id: handshake.peer_id.into(),
            listen_port,
            chain_info: SingularPtrField::some(handshake.chain_info.into()),
            cached_size: Default::default(),
            unknown_fields: Default::default(),
        }
    }
}

/// Account route description
#[derive(PartialEq, Eq, Clone, Debug)]
pub struct AnnounceAccountRoute {
    pub peer_id: PeerId,
    pub hash: CryptoHash,
    pub signature: Signature,
}

impl TryFrom<network_proto::AnnounceAccountRoute> for AnnounceAccountRoute {
    type Error = Box<dyn std::error::Error>;

    fn try_from(proto: network_proto::AnnounceAccountRoute) -> Result<Self, Self::Error> {
        let peer_id: PeerId = proto.peer_id.try_into().map_err(|e| format!("{}", e))?;
        let hash: CryptoHash = proto.hash.try_into().map_err(|e| format!("{}", e))?;
        let signature: Signature = proto.signature.try_into().map_err(|e| format!("{}", e))?;
        Ok(AnnounceAccountRoute { peer_id, hash, signature })
    }
}

impl From<AnnounceAccountRoute> for network_proto::AnnounceAccountRoute {
    fn from(announce_account_route: AnnounceAccountRoute) -> network_proto::AnnounceAccountRoute {
        network_proto::AnnounceAccountRoute {
            peer_id: announce_account_route.peer_id.into(),
            hash: announce_account_route.hash.into(),
            signature: announce_account_route.signature.into(),
            cached_size: Default::default(),
            unknown_fields: Default::default(),
        }
    }
}

/// Account announcement information
#[derive(PartialEq, Eq, Clone, Debug)]
pub struct AnnounceAccount {
    /// AccountId to be announced
    pub account_id: AccountId,
    /// This announcement is only valid for this `epoch`
    pub epoch_id: EpochId,
    /// Complete route description to account id
    /// First element of the route (header) contains:
    ///     peer_id owner of the account_id
    ///     hash of the announcement
    ///     signature with account id secret key
    /// Subsequent elements of the route contain:
    ///     peer_id of intermediates hop in the route
    ///     hash built using previous hash and peer_id
    ///     signature with peer id secret key
    pub route: Vec<AnnounceAccountRoute>,
}

impl AnnounceAccount {
    pub fn new(
        account_id: AccountId,
        epoch_id: EpochId,
        peer_id: PeerId,
        hash: CryptoHash,
        signature: Signature,
    ) -> Self {
        let route = vec![AnnounceAccountRoute { peer_id, hash, signature }];
        Self { account_id, epoch_id, route }
    }

    pub fn build_header_hash(
        account_id: &AccountId,
        peer_id: &PeerId,
        epoch_id: &EpochId,
    ) -> CryptoHash {
        hash([account_id.as_bytes(), peer_id.as_ref(), epoch_id.as_ref()].concat().as_slice())
    }

    pub fn header_hash(&self) -> CryptoHash {
        AnnounceAccount::build_header_hash(
            &self.account_id,
            &self.route.first().unwrap().peer_id,
            &self.epoch_id,
        )
    }

    pub fn header(&self) -> &AnnounceAccountRoute {
        self.route.first().unwrap()
    }

    pub fn peer_id_sender(&self) -> PeerId {
        self.route.last().unwrap().peer_id
    }

    pub fn num_hops(&self) -> usize {
        self.route.len() - 1
    }

    pub fn extend(&mut self, peer_id: PeerId, secret_key: &SecretKey) {
        let last_hash = self.route.last().unwrap().hash;
        let new_hash = hash([last_hash.as_ref(), peer_id.as_ref()].concat().as_slice());
        let signature = sign(new_hash.as_ref(), secret_key);
        self.route.push(AnnounceAccountRoute { peer_id, hash: new_hash, signature })
    }
}

impl TryFrom<network_proto::AnnounceAccount> for AnnounceAccount {
    type Error = Box<dyn std::error::Error>;

    fn try_from(proto: network_proto::AnnounceAccount) -> Result<Self, Self::Error> {
        let epoch_id: CryptoHash = proto.epoch.try_into().map_err(|e| format!("{}", e))?;
        Ok(AnnounceAccount {
            account_id: proto.account_id,
            epoch_id: EpochId(epoch_id),
            route: proto
                .route
                .into_iter()
                .filter_map(|hop| match hop.try_into() {
                    Ok(hop) => Some(hop),
                    Err(_) => None,
                })
                .collect(),
        })
    }
}

impl From<AnnounceAccount> for network_proto::AnnounceAccount {
    fn from(announce_account: AnnounceAccount) -> network_proto::AnnounceAccount {
        network_proto::AnnounceAccount {
            account_id: announce_account.account_id,
            epoch: announce_account.epoch_id.0.into(),
            route: RepeatedField::from_iter(
                announce_account.route.into_iter().map(|hop| hop.into()),
            ),
            cached_size: Default::default(),
            unknown_fields: Default::default(),
        }
    }
}

#[derive(PartialEq, Eq, Clone, Debug)]
pub enum PeerMessage {
    Handshake(Handshake),

    PeersRequest,
    PeersResponse(Vec<PeerInfo>),

    BlockHeadersRequest(Vec<CryptoHash>),
    BlockHeaders(Vec<BlockHeader>),
    BlockHeaderAnnounce(BlockHeader),

    BlockRequest(CryptoHash),
    Block(Block),
    BlockApproval(AccountId, CryptoHash, Signature),

    Transaction(SignedTransaction),

    StateRequest(ShardId, CryptoHash),
    StateResponse(StateResponseInfo),
    AnnounceAccount(AnnounceAccount),

    ChunkPartRequest(ChunkPartRequestMsg),
    ChunkOnePartRequest(ChunkPartRequestMsg),
    ChunkPart(ChunkPartMsg),
    ChunkOnePart(ChunkOnePart),
}

impl fmt::Display for PeerMessage {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            PeerMessage::Handshake(_) => f.write_str("Handshake"),
            PeerMessage::PeersRequest => f.write_str("PeersRequest"),
            PeerMessage::PeersResponse(_) => f.write_str("PeersResponse"),
            PeerMessage::BlockHeadersRequest(_) => f.write_str("BlockHeaderRequest"),
            PeerMessage::BlockHeaders(_) => f.write_str("BlockHeaders"),
            PeerMessage::BlockHeaderAnnounce(_) => f.write_str("BlockHeaderAnnounce"),
            PeerMessage::BlockRequest(_) => f.write_str("BlockRequest"),
            PeerMessage::Block(_) => f.write_str("Block"),
            PeerMessage::BlockApproval(_, _, _) => f.write_str("BlockApproval"),
            PeerMessage::Transaction(_) => f.write_str("Transaction"),
            PeerMessage::StateRequest(_, _) => f.write_str("StateRequest"),
            PeerMessage::StateResponse(_) => f.write_str("StateResponse"),
            PeerMessage::AnnounceAccount(_) => f.write_str("AnnounceAccount"),
            PeerMessage::ChunkPartRequest(_) => f.write_str("ChunkPartRequest"),
            PeerMessage::ChunkOnePartRequest(_) => f.write_str("ChunkOnePartRequest"),
            PeerMessage::ChunkPart(_) => f.write_str("ChunkPart"),
            PeerMessage::ChunkOnePart(_) => f.write_str("ChunkOnePart"),
        }
    }
}

impl TryFrom<network_proto::PeerMessage> for PeerMessage {
    type Error = Box<dyn std::error::Error>;

    fn try_from(proto: network_proto::PeerMessage) -> Result<Self, Self::Error> {
        match proto.message_type {
            Some(network_proto::PeerMessage_oneof_message_type::hand_shake(hand_shake)) => {
                hand_shake.try_into().map(PeerMessage::Handshake)
            }
            Some(network_proto::PeerMessage_oneof_message_type::peers_request(_)) => {
                Ok(PeerMessage::PeersRequest)
            }
            Some(network_proto::PeerMessage_oneof_message_type::peers_response(peers_response)) => {
                let peers_response = peers_response
                    .peers
                    .into_iter()
                    .map(TryInto::try_into)
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(PeerMessage::PeersResponse(peers_response))
            }
            Some(network_proto::PeerMessage_oneof_message_type::block(block)) => {
                Ok(PeerMessage::Block(block.try_into()?))
            }
            Some(network_proto::PeerMessage_oneof_message_type::block_header_announce(header)) => {
                Ok(PeerMessage::BlockHeaderAnnounce(header.try_into()?))
            }
            Some(network_proto::PeerMessage_oneof_message_type::transaction(transaction)) => {
                Ok(PeerMessage::Transaction(transaction.try_into()?))
            }
            Some(network_proto::PeerMessage_oneof_message_type::block_approval(block_approval)) => {
                Ok(PeerMessage::BlockApproval(
                    block_approval.account_id,
                    block_approval.hash.try_into()?,
                    block_approval.signature.try_into()?,
                ))
            }
            Some(network_proto::PeerMessage_oneof_message_type::block_request(block_request)) => {
                Ok(PeerMessage::BlockRequest(block_request.try_into()?))
            }
            Some(network_proto::PeerMessage_oneof_message_type::block_headers_request(
                block_headers_request,
            )) => Ok(PeerMessage::BlockHeadersRequest(
                block_headers_request
                    .hashes
                    .into_iter()
                    .map(TryInto::try_into)
                    .collect::<Result<Vec<_>, _>>()?,
            )),
            Some(network_proto::PeerMessage_oneof_message_type::block_headers(block_headers)) => {
                Ok(PeerMessage::BlockHeaders(
                    block_headers
                        .headers
                        .into_iter()
                        .map(TryInto::try_into)
                        .collect::<Result<Vec<_>, _>>()?,
                ))
            }
            Some(network_proto::PeerMessage_oneof_message_type::state_request(state_request)) => {
                Ok(PeerMessage::StateRequest(
                    state_request.shard_id,
                    state_request.hash.try_into()?,
                ))
            }
            Some(network_proto::PeerMessage_oneof_message_type::state_response(state_response)) => {
                let outgoing_receipts_proto =
                    state_response
                        .outgoing_receipts
                        .into_option()
                        .ok_or::<Self::Error>("missing outgoing_receipts".into())?;
                Ok(PeerMessage::StateResponse(StateResponseInfo {
                    shard_id: state_response.shard_id,
                    hash: state_response.hash.try_into()?,
                    prev_chunk_hash: ChunkHash(state_response.prev_chunk_hash.try_into()?),
                    prev_chunk_extra: ChunkExtra {
                        state_root: state_response.prev_state_root.try_into()?,
                        validator_proposals: state_response
                            .validator_proposals
                            .into_iter()
                            .map(TryInto::try_into)
                            .collect::<Result<Vec<_>, _>>()?,
                        gas_used: state_response.prev_gas_used.try_into()?,
                        gas_limit: state_response.prev_gas_limit.try_into()?,
                    },
                    payload: state_response.payload,
                    outgoing_receipts: (
                        outgoing_receipts_proto.hash.try_into()?,
                        outgoing_receipts_proto
                            .receipts
                            .into_iter()
                            .map(TryInto::try_into)
                            .collect::<Result<Vec<_>, _>>()?,
                    ),
                    incoming_receipts: state_response
                        .incoming_receipts
                        .into_iter()
                        .map(|receipt| {
                            Ok((
                                receipt.hash.try_into()?,
                                receipt
                                    .receipts
                                    .into_iter()
                                    .map(TryInto::try_into)
                                    .collect::<Result<Vec<_>, _>>()?,
                            ))
                        })
                        .collect::<Result<Vec<(CryptoHash, Vec<ReceiptTransaction>)>, Self::Error>>(
                        )?,
                }))
            }
            Some(network_proto::PeerMessage_oneof_message_type::chunk_part_request(
                chunk_part_request,
            )) => Ok(PeerMessage::ChunkPartRequest(ChunkPartRequestMsg {
                shard_id: chunk_part_request.shard_id,
                chunk_hash: ChunkHash(chunk_part_request.chunk_hash.try_into()?),
                height: chunk_part_request.height,
                part_id: chunk_part_request.part_id,
            })),
            Some(network_proto::PeerMessage_oneof_message_type::chunk_one_part_request(
                chunk_part_request,
            )) => Ok(PeerMessage::ChunkOnePartRequest(ChunkPartRequestMsg {
                shard_id: chunk_part_request.shard_id,
                chunk_hash: ChunkHash(chunk_part_request.chunk_hash.try_into()?),
                height: chunk_part_request.height,
                part_id: chunk_part_request.part_id,
            })),
            Some(network_proto::PeerMessage_oneof_message_type::chunk_part(chunk_part)) => {
                Ok(PeerMessage::ChunkPart(ChunkPartMsg {
                    shard_id: chunk_part.shard_id,
                    chunk_hash: ChunkHash(chunk_part.chunk_hash.try_into()?),
                    part_id: chunk_part.part_id,
                    part: chunk_part.part.into_boxed_slice(),
                    merkle_path: MerklePath::decode(chunk_part.merkle_path.as_slice())?,
                }))
            }
            Some(network_proto::PeerMessage_oneof_message_type::chunk_header_and_part(
                chunk_header_and_part,
            )) => Ok(PeerMessage::ChunkOnePart(ChunkOnePart {
                shard_id: chunk_header_and_part.shard_id,
                chunk_hash: ChunkHash(chunk_header_and_part.chunk_hash.try_into()?),
                header: proto_to_type(chunk_header_and_part.header)?,
                part_id: chunk_header_and_part.part_id,
                part: chunk_header_and_part.part.into_boxed_slice(),
                receipts: chunk_header_and_part
                    .receipts
                    .into_iter()
                    .map(TryInto::try_into)
                    .collect::<Result<Vec<_>, _>>()?,
                merkle_path: MerklePath::decode(chunk_header_and_part.merkle_path.as_slice())?,
            })),
            Some(network_proto::PeerMessage_oneof_message_type::announce_account(
                announce_account,
            )) => announce_account.try_into().map(PeerMessage::AnnounceAccount),
            None => Err(format!("Unexpected empty message body").into()),
        }
    }
}

impl From<PeerMessage> for network_proto::PeerMessage {
    fn from(message: PeerMessage) -> network_proto::PeerMessage {
        let message_type = match message {
            PeerMessage::Handshake(hand_shake) => {
                Some(network_proto::PeerMessage_oneof_message_type::hand_shake(hand_shake.into()))
            }
            PeerMessage::PeersRequest => {
                Some(network_proto::PeerMessage_oneof_message_type::peers_request(true))
            }
            PeerMessage::PeersResponse(peers_response) => {
                let peers_response = network_proto::PeersResponse {
                    peers: RepeatedField::from_iter(
                        peers_response.into_iter().map(std::convert::Into::into),
                    ),
                    cached_size: Default::default(),
                    unknown_fields: Default::default(),
                };
                Some(network_proto::PeerMessage_oneof_message_type::peers_response(peers_response))
            }
            PeerMessage::Block(block) => {
                Some(network_proto::PeerMessage_oneof_message_type::block(block.into()))
            }
            PeerMessage::BlockHeaderAnnounce(header) => Some(
                network_proto::PeerMessage_oneof_message_type::block_header_announce(header.into()),
            ),
            PeerMessage::Transaction(transaction) => {
                Some(network_proto::PeerMessage_oneof_message_type::transaction(transaction.into()))
            }
            PeerMessage::BlockApproval(account_id, hash, signature) => {
                let block_approval = network_proto::BlockApproval {
                    account_id,
                    hash: hash.into(),
                    signature: signature.into(),
                    cached_size: Default::default(),
                    unknown_fields: Default::default(),
                };
                Some(network_proto::PeerMessage_oneof_message_type::block_approval(block_approval))
            }
            PeerMessage::BlockRequest(hash) => {
                Some(network_proto::PeerMessage_oneof_message_type::block_request(hash.into()))
            }
            PeerMessage::BlockHeadersRequest(hashes) => {
                let request = network_proto::BlockHeaderRequest {
                    hashes: RepeatedField::from_iter(
                        hashes.into_iter().map(std::convert::Into::into),
                    ),
                    cached_size: Default::default(),
                    unknown_fields: Default::default(),
                };
                Some(network_proto::PeerMessage_oneof_message_type::block_headers_request(request))
            }
            PeerMessage::BlockHeaders(headers) => {
                let block_headers = network_proto::BlockHeaders {
                    headers: RepeatedField::from_iter(
                        headers.into_iter().map(std::convert::Into::into),
                    ),
                    cached_size: Default::default(),
                    unknown_fields: Default::default(),
                };
                Some(network_proto::PeerMessage_oneof_message_type::block_headers(block_headers))
            }
            PeerMessage::StateRequest(shard_id, hash) => {
                let state_request = network_proto::StateRequest {
                    shard_id,
                    hash: hash.into(),
                    cached_size: Default::default(),
                    unknown_fields: Default::default(),
                };
                Some(network_proto::PeerMessage_oneof_message_type::state_request(state_request))
            }
            PeerMessage::StateResponse(StateResponseInfo {
                shard_id,
                hash,
                prev_chunk_hash,
                prev_chunk_extra,
                payload,
                outgoing_receipts,
                incoming_receipts,
            }) => {
                let state_response = network_proto::StateResponse {
                    shard_id,
                    hash: hash.into(),
                    prev_chunk_hash: prev_chunk_hash.0.into(),
                    prev_state_root: prev_chunk_extra.state_root.into(),
                    validator_proposals: RepeatedField::from_iter(
                        prev_chunk_extra
                            .validator_proposals
                            .into_iter()
                            .map(std::convert::Into::into),
                    ),
                    prev_gas_used: prev_chunk_extra.gas_used.into(),
                    prev_gas_limit: prev_chunk_extra.gas_limit.into(),
                    payload,
                    outgoing_receipts: SingularPtrField::some(
                        network_proto::StateResponseReceipts {
                            hash: outgoing_receipts.0.into(),
                            receipts: RepeatedField::from_iter(
                                outgoing_receipts.1.into_iter().map(std::convert::Into::into),
                            ),
                            cached_size: Default::default(),
                            unknown_fields: Default::default(),
                        },
                    ),
                    incoming_receipts: RepeatedField::from_iter(incoming_receipts.into_iter().map(
                        |(hash, receipts)| {
                            (network_proto::StateResponseReceipts {
                                hash: hash.into(),
                                receipts: RepeatedField::from_iter(
                                    receipts.into_iter().map(std::convert::Into::into),
                                ),
                                cached_size: Default::default(),
                                unknown_fields: Default::default(),
                            })
                        },
                    )),
                    cached_size: Default::default(),
                    unknown_fields: Default::default(),
                };
                Some(network_proto::PeerMessage_oneof_message_type::state_response(state_response))
            }
            PeerMessage::ChunkPartRequest(chunk_part_request) => {
                let chunk_part_request = network_proto::ChunkPartRequest {
                    shard_id: chunk_part_request.shard_id,
                    chunk_hash: chunk_part_request.chunk_hash.0.into(),
                    height: chunk_part_request.height,
                    part_id: chunk_part_request.part_id,
                    ..Default::default()
                };
                Some(network_proto::PeerMessage_oneof_message_type::chunk_part_request(
                    chunk_part_request,
                ))
            }
            PeerMessage::ChunkOnePartRequest(chunk_part_request) => {
                let chunk_part_request = network_proto::ChunkPartRequest {
                    shard_id: chunk_part_request.shard_id,
                    chunk_hash: chunk_part_request.chunk_hash.0.into(),
                    height: chunk_part_request.height,
                    part_id: chunk_part_request.part_id,
                    ..Default::default()
                };
                Some(network_proto::PeerMessage_oneof_message_type::chunk_one_part_request(
                    chunk_part_request,
                ))
            }
            PeerMessage::ChunkPart(chunk_part) => {
                let chunk_part = network_proto::ChunkPart {
                    shard_id: chunk_part.shard_id,
                    chunk_hash: chunk_part.chunk_hash.0.into(),
                    part_id: chunk_part.part_id,
                    part: (*chunk_part.part).to_vec(),
                    merkle_path: MerklePath::encode(&chunk_part.merkle_path).unwrap(),
                    ..Default::default()
                };
                Some(network_proto::PeerMessage_oneof_message_type::chunk_part(chunk_part))
            }
            PeerMessage::ChunkOnePart(chunk_header_and_part) => {
                let chunk_header_and_part = network_proto::ChunkOnePart {
                    shard_id: chunk_header_and_part.shard_id,
                    chunk_hash: chunk_header_and_part.chunk_hash.0.into(),
                    header: SingularPtrField::some(chunk_header_and_part.header.into()),
                    part_id: chunk_header_and_part.part_id,
                    part: (*chunk_header_and_part.part).to_vec(),
                    merkle_path: MerklePath::encode(&chunk_header_and_part.merkle_path).unwrap(),
                    ..Default::default()
                };
                Some(network_proto::PeerMessage_oneof_message_type::chunk_header_and_part(
                    chunk_header_and_part,
                ))
            }
            PeerMessage::AnnounceAccount(announce_account) => {
                Some(network_proto::PeerMessage_oneof_message_type::announce_account(
                    announce_account.into(),
                ))
            }
        };
        network_proto::PeerMessage {
            message_type,
            cached_size: Default::default(),
            unknown_fields: Default::default(),
        }
    }
}

/// Configuration for the peer-to-peer manager.
#[derive(Clone)]
pub struct NetworkConfig {
    pub public_key: PublicKey,
    pub secret_key: SecretKey,
    pub account_id: Option<AccountId>,
    pub addr: Option<SocketAddr>,
    pub boot_nodes: Vec<PeerInfo>,
    pub handshake_timeout: Duration,
    pub reconnect_delay: Duration,
    pub bootstrap_peers_period: Duration,
    pub peer_max_count: u32,
    /// Duration of the ban for misbehaving peers.
    pub ban_window: Duration,
    /// Remove expired peers.
    pub peer_expiration_duration: Duration,
    /// Maximum number of peer addresses we should ever send.
    pub max_send_peers: u32,
    /// Duration for checking on stats from the peers.
    pub peer_stats_period: Duration,
}

/// Status of the known peers.
#[derive(Serialize, Deserialize, Eq, PartialEq, Debug)]
pub enum KnownPeerStatus {
    Unknown,
    NotConnected,
    Connected,
    Banned(ReasonForBan, DateTime<Utc>),
}

/// Information node stores about known peers.
#[derive(Serialize, Deserialize, Debug)]
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

impl TryFrom<Vec<u8>> for KnownPeerState {
    type Error = Box<dyn std::error::Error>;

    fn try_from(bytes: Vec<u8>) -> Result<KnownPeerState, Self::Error> {
        Decode::decode(&bytes).map_err(|err| err.into())
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

#[derive(Message, Clone, Debug)]
pub struct SendMessage {
    pub message: PeerMessage,
}

/// Actor message to consolidate potential new peer.
/// Returns if connection should be kept or dropped.
pub struct Consolidate {
    pub actor: Addr<Peer>,
    pub peer_info: PeerInfo,
    pub peer_type: PeerType,
    pub chain_info: PeerChainInfo,
}

impl Message for Consolidate {
    type Result = bool;
}

/// Unregister message from Peer to PeerManager.
#[derive(Message)]
pub struct Unregister {
    pub peer_id: PeerId,
}

pub struct PeerList {
    pub peers: Vec<PeerInfo>,
}

/// Requesting peers from peer manager to communicate to a peer.
pub struct PeersRequest {}

impl Message for PeersRequest {
    type Result = PeerList;
}

/// Received new peers from another peer.
#[derive(Message)]
pub struct PeersResponse {
    pub peers: Vec<PeerInfo>,
}

impl<A, M> MessageResponse<A, M> for PeerList
where
    A: Actor,
    M: Message<Result = PeerList>,
{
    fn handle<R: ResponseChannel<M>>(self, _: &mut A::Context, tx: Option<R>) {
        if let Some(tx) = tx {
            tx.send(self)
        }
    }
}

/// Ban reason.
#[derive(Debug, Clone, PartialEq, Eq, Copy, Serialize, Deserialize)]
pub enum ReasonForBan {
    None = 0,
    BadBlock = 1,
    BadBlockHeader = 2,
    HeightFraud = 3,
    BadHandshake = 4,
    BadBlockApproval = 5,
    Abusive = 6,
    InvalidSignature = 7,
    InvalidPeerId = 8,
    InvalidHash = 9,
}

#[derive(Message)]
pub struct Ban {
    pub peer_id: PeerId,
    pub ban_reason: ReasonForBan,
}

#[derive(Debug)]
pub enum NetworkRequests {
    /// Fetch information from the network.
    FetchInfo,
    /// Sends block, either when block was just produced or when requested.
    Block { block: Block },
    /// Sends block header announcement, with possibly attaching approval for this block if
    /// participating in this epoch.
    BlockHeaderAnnounce { header: BlockHeader, approval: Option<BlockApproval> },
    /// Request block with given hash from given peer.
    BlockRequest { hash: CryptoHash, peer_id: PeerId },
    /// Request given block headers.
    BlockHeadersRequest { hashes: Vec<CryptoHash>, peer_id: PeerId },
    /// Request state for given shard at given state root.
    StateRequest { shard_id: ShardId, hash: CryptoHash, account_id: AccountId },
    /// Ban given peer.
    BanPeer { peer_id: PeerId, ban_reason: ReasonForBan },
    /// Announce account
    AnnounceAccount(AnnounceAccount),

    /// Request chunk part
    ChunkPartRequest { account_id: AccountId, part_request: ChunkPartRequestMsg },
    /// Request chunk part and receipts
    ChunkOnePartRequest { account_id: AccountId, part_request: ChunkPartRequestMsg },
    /// Response to a peer with chunk part and receipts.
    ChunkOnePartResponse { peer_id: PeerId, header_and_part: ChunkOnePart },
    /// A chunk header and one part for another validator.
    ChunkOnePartMessage { account_id: AccountId, header_and_part: ChunkOnePart },
    /// A chunk part
    ChunkPart { peer_id: PeerId, part: ChunkPartMsg },
}

/// Combines peer address info and chain information.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct FullPeerInfo {
    pub peer_info: PeerInfo,
    pub chain_info: PeerChainInfo,
}

#[derive(Debug)]
pub struct NetworkInfo {
    pub num_active_peers: usize,
    pub peer_max_count: u32,
    pub most_weight_peers: Vec<FullPeerInfo>,
    pub sent_bytes_per_sec: u64,
    pub received_bytes_per_sec: u64,
    /// Accounts of known block and chunk producers from routing table.  
    pub known_producers: Vec<AccountId>,
}

#[derive(Debug)]
pub enum NetworkResponses {
    NoResponse,
    Info(NetworkInfo),
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

#[derive(PartialEq, Eq, Clone, Debug)]
pub struct StateResponseInfo {
    pub shard_id: ShardId,
    pub hash: CryptoHash,
    pub prev_chunk_hash: ChunkHash,
    pub prev_chunk_extra: ChunkExtra,
    pub payload: Vec<u8>,
    pub outgoing_receipts: (CryptoHash, Vec<ReceiptTransaction>),
    pub incoming_receipts: Vec<(CryptoHash, Vec<ReceiptTransaction>)>,
}

#[derive(Debug)]
pub enum NetworkClientMessages {
    /// Received transaction.
    Transaction(SignedTransaction),
    /// Received block header.
    BlockHeader(BlockHeader, PeerId),
    /// Received block, possibly requested.
    Block(Block, PeerId, bool),
    /// Received list of headers for syncing.
    BlockHeaders(Vec<BlockHeader>, PeerId),
    /// Get Chain information from Client.
    GetChainInfo,
    /// Block approval.
    BlockApproval(AccountId, CryptoHash, Signature),
    /// Request headers.
    BlockHeadersRequest(Vec<CryptoHash>),
    /// Request a block.
    BlockRequest(CryptoHash),
    /// State request.
    StateRequest(ShardId, CryptoHash),
    /// State response.
    StateResponse(StateResponseInfo),
    /// Account announcement that needs to be validated before being processed
    AnnounceAccount(AnnounceAccount),

    /// Request chunk part
    ChunkPartRequest(ChunkPartRequestMsg, PeerId),
    /// Request chunk part
    ChunkOnePartRequest(ChunkPartRequestMsg, PeerId),
    /// A chunk part
    ChunkPart(ChunkPartMsg),
    /// A chunk header and one part
    ChunkOnePart(ChunkOnePart),
}

pub enum NetworkClientResponses {
    /// No response.
    NoResponse,
    /// Valid transaction inserted into mempool as response to Transaction.
    ValidTx,
    /// Invalid transaction inserted into mempool as response to Transaction.
    InvalidTx(String),
    /// Ban peer for malicious behaviour.
    Ban { ban_reason: ReasonForBan },
    /// Chain information.
    ChainInfo { genesis: CryptoHash, height: BlockIndex, total_weight: Weight },
    /// Block response.
    Block(Block),
    /// Headers response.
    BlockHeaders(Vec<BlockHeader>),
    /// Response to state request.
    StateResponse(StateResponseInfo),
}

impl<A, M> MessageResponse<A, M> for NetworkClientResponses
where
    A: Actor,
    M: Message<Result = NetworkClientResponses>,
{
    fn handle<R: ResponseChannel<M>>(self, _: &mut A::Context, tx: Option<R>) {
        if let Some(tx) = tx {
            tx.send(self)
        }
    }
}

impl Message for NetworkClientMessages {
    type Result = NetworkClientResponses;
}

/// Peer stats query.
pub struct QueryPeerStats {}

/// Peer stats result
#[derive(Debug)]
pub struct PeerStatsResult {
    /// Chain info.
    pub chain_info: PeerChainInfo,
    /// Number of bytes we've received from the peer.
    pub received_bytes_per_sec: u64,
    /// Number of bytes we've sent to the peer.
    pub sent_bytes_per_sec: u64,
    /// Returns if this peer is abusive and should be banned.
    pub is_abusive: bool,
    /// Counts of incoming/outgoing messages from given peer.
    pub message_counts: (u64, u64),
}

impl<A, M> MessageResponse<A, M> for PeerStatsResult
where
    A: Actor,
    M: Message<Result = PeerStatsResult>,
{
    fn handle<R: ResponseChannel<M>>(self, _: &mut A::Context, tx: Option<R>) {
        if let Some(tx) = tx {
            tx.send(self)
        }
    }
}

impl Message for QueryPeerStats {
    type Result = PeerStatsResult;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChunkPartRequestMsg {
    pub shard_id: u64,
    pub chunk_hash: ChunkHash,
    pub height: BlockIndex,
    pub part_id: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChunkPartMsg {
    pub shard_id: u64,
    pub chunk_hash: ChunkHash,
    pub part_id: u64,
    pub part: Shard,
    pub merkle_path: MerklePath,
}
