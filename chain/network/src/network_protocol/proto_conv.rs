/// Contains protobuf <-> network_protocol conversions.
use crate::network_protocol::proto;
use crate::network_protocol::proto::peer_message::Message_type as ProtoMT;
use crate::network_protocol::{
    Handshake, HandshakeFailureReason, PeerMessage, RoutingSyncV2, RoutingTableUpdate,
};
use borsh::{BorshDeserialize as _, BorshSerialize as _};
use near_network_primitives::types::{
    Edge, PartialEdgeInfo, PeerChainInfoV2, PeerInfo, RoutedMessage,
};
use near_primitives::block::{Block, BlockHeader, GenesisId};
use near_primitives::challenge::Challenge;
use near_primitives::hash::CryptoHash;
use near_primitives::network::{AnnounceAccount, PeerId};
use near_primitives::syncing::{EpochSyncFinalizationResponse, EpochSyncResponse};
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::EpochId;
use protobuf::MessageField as MF;
use thiserror::Error;

#[derive(Error, Debug)]
#[error("[{idx}]: {source}")]
pub struct ParseVecError<E> {
    idx: usize,
    #[source]
    source: E,
}

fn try_from_vec<'a, X, Y: TryFrom<&'a X>>(
    xs: &'a Vec<X>,
) -> Result<Vec<Y>, ParseVecError<Y::Error>> {
    let mut ys = vec![];
    for (idx, x) in xs.iter().enumerate() {
        ys.push(x.try_into().map_err(|source| ParseVecError { idx, source })?);
    }
    Ok(ys)
}

#[derive(Error, Debug)]
pub enum ParseRequiredError<E> {
    #[error("missing, while required")]
    Missing,
    #[error(transparent)]
    Other(E),
}

fn try_from_required<'a, X, Y: TryFrom<&'a X>>(
    x: &'a MF<X>,
) -> Result<Y, ParseRequiredError<Y::Error>> {
    x.as_ref().ok_or(ParseRequiredError::Missing)?.try_into().map_err(ParseRequiredError::Other)
}

impl From<&CryptoHash> for proto::CryptoHash {
    fn from(x: &CryptoHash) -> Self {
        let mut y = Self::new();
        y.hash = x.0.into();
        y
    }
}

pub type ParseCryptoHashError = Box<dyn std::error::Error + Send + Sync>;

impl TryFrom<&proto::CryptoHash> for CryptoHash {
    type Error = ParseCryptoHashError;
    fn try_from(p: &proto::CryptoHash) -> Result<Self, Self::Error> {
        CryptoHash::try_from(&p.hash[..])
    }
}

//////////////////////////////////////////

impl From<&GenesisId> for proto::GenesisId {
    fn from(x: &GenesisId) -> Self {
        Self { chain_id: x.chain_id.clone(), hash: MF::some((&x.hash).into()), ..Self::default() }
    }
}

#[derive(Error, Debug)]
pub enum ParseGenesisIdError {
    #[error("hash: {0}")]
    Hash(ParseRequiredError<ParseCryptoHashError>),
}

impl TryFrom<&proto::GenesisId> for GenesisId {
    type Error = ParseGenesisIdError;
    fn try_from(p: &proto::GenesisId) -> Result<Self, Self::Error> {
        Ok(Self {
            chain_id: p.chain_id.clone(),
            hash: try_from_required(&p.hash).map_err(Self::Error::Hash)?,
        })
    }
}

//////////////////////////////////////////

impl From<&PeerChainInfoV2> for proto::PeerChainInfo {
    fn from(x: &PeerChainInfoV2) -> Self {
        Self {
            genesis_id: MF::some((&x.genesis_id).into()),
            height: x.height,
            tracked_shards: x.tracked_shards.clone(),
            archival: x.archival,
            ..Self::default()
        }
    }
}

#[derive(Error, Debug)]
pub enum ParsePeerChainInfoV2Error {
    #[error("genesis_id {0}")]
    GenesisId(ParseRequiredError<ParseGenesisIdError>),
}

impl TryFrom<&proto::PeerChainInfo> for PeerChainInfoV2 {
    type Error = ParsePeerChainInfoV2Error;
    fn try_from(p: &proto::PeerChainInfo) -> Result<Self, Self::Error> {
        Ok(Self {
            genesis_id: try_from_required(&p.genesis_id).map_err(Self::Error::GenesisId)?,
            height: p.height,
            tracked_shards: p.tracked_shards.clone(),
            archival: p.archival,
        })
    }
}

//////////////////////////////////////////

impl From<&PeerId> for proto::PublicKey {
    fn from(x: &PeerId) -> Self {
        Self { borsh: x.try_to_vec().unwrap(), ..Self::default() }
    }
}

pub type ParsePeerIdError = borsh::maybestd::io::Error;

impl TryFrom<&proto::PublicKey> for PeerId {
    type Error = ParsePeerIdError;
    fn try_from(p: &proto::PublicKey) -> Result<Self, Self::Error> {
        Self::try_from_slice(&p.borsh)
    }
}

//////////////////////////////////////////

impl From<&PartialEdgeInfo> for proto::PartialEdgeInfo {
    fn from(x: &PartialEdgeInfo) -> Self {
        Self { borsh: x.try_to_vec().unwrap(), ..Self::default() }
    }
}

pub type ParsePartialEdgeInfoError = borsh::maybestd::io::Error;

impl TryFrom<&proto::PartialEdgeInfo> for PartialEdgeInfo {
    type Error = ParsePartialEdgeInfoError;
    fn try_from(p: &proto::PartialEdgeInfo) -> Result<Self, Self::Error> {
        Self::try_from_slice(&p.borsh)
    }
}

//////////////////////////////////////////

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

//////////////////////////////////////////

impl From<&Handshake> for proto::Handshake {
    fn from(x: &Handshake) -> Self {
        Self {
            protocol_version: x.protocol_version,
            oldest_supported_version: x.oldest_supported_version,
            sender_peer_id: MF::some((&x.sender_peer_id).into()),
            target_peer_id: MF::some((&x.target_peer_id).into()),
            sender_listen_port: x.sender_listen_port.unwrap_or(0).into(),
            sender_chain_info: MF::some((&x.sender_chain_info).into()),
            partial_edge_info: MF::some((&x.partial_edge_info).into()),
            ..Self::default()
        }
    }
}

#[derive(Error, Debug)]
pub enum ParseHandshakeError {
    #[error("sender_peer_id {0}")]
    SenderPeerId(ParseRequiredError<ParsePeerIdError>),
    #[error("target_peer_id {0}")]
    TargetPeerId(ParseRequiredError<ParsePeerIdError>),
    #[error("sender_listen_port {0}")]
    SenderListenPort(std::num::TryFromIntError),
    #[error("sender_chain_info {0}")]
    SenderChainInfo(ParseRequiredError<ParsePeerChainInfoV2Error>),
    #[error("partial_edge_info {0}")]
    PartialEdgeInfo(ParseRequiredError<ParsePartialEdgeInfoError>),
}

impl TryFrom<&proto::Handshake> for Handshake {
    type Error = ParseHandshakeError;
    fn try_from(p: &proto::Handshake) -> Result<Self, Self::Error> {
        Ok(Self {
            protocol_version: p.protocol_version,
            oldest_supported_version: p.oldest_supported_version,
            sender_peer_id: try_from_required(&p.sender_peer_id)
                .map_err(Self::Error::SenderPeerId)?,
            target_peer_id: try_from_required(&p.target_peer_id)
                .map_err(Self::Error::TargetPeerId)?,
            sender_listen_port: {
                let port =
                    u16::try_from(p.sender_listen_port).map_err(Self::Error::SenderListenPort)?;
                if port == 0 {
                    None
                } else {
                    Some(port)
                }
            },
            sender_chain_info: try_from_required(&p.sender_chain_info)
                .map_err(Self::Error::SenderChainInfo)?,
            partial_edge_info: try_from_required(&p.partial_edge_info)
                .map_err(Self::Error::PartialEdgeInfo)?,
        })
    }
}

//////////////////////////////////////////

impl From<(&PeerInfo, &HandshakeFailureReason)> for proto::HandshakeFailure {
    fn from((pi, hfr): (&PeerInfo, &HandshakeFailureReason)) -> Self {
        match hfr {
            HandshakeFailureReason::ProtocolVersionMismatch {
                version,
                oldest_supported_version,
            } => Self {
                peer_info: MF::some(pi.into()),
                reason: proto::handshake_failure::Reason::ProtocolVersionMismatch.into(),
                version: *version,
                oldest_supported_version: *oldest_supported_version,
                ..Default::default()
            },
            HandshakeFailureReason::GenesisMismatch(genesis_id) => Self {
                peer_info: MF::some(pi.into()),
                reason: proto::handshake_failure::Reason::GenesisMismatch.into(),
                genesis_id: MF::some(genesis_id.into()),
                ..Default::default()
            },
            HandshakeFailureReason::InvalidTarget => Self {
                peer_info: MF::some(pi.into()),
                reason: proto::handshake_failure::Reason::InvalidTarget.into(),
                ..Default::default()
            },
        }
    }
}

#[derive(Error, Debug)]
pub enum ParseHandshakeFailureError {
    #[error("peer_info: {0}")]
    PeerInfo(ParseRequiredError<ParsePeerInfoError>),
    #[error("genesis_id: {0}")]
    GenesisId(ParseRequiredError<ParseGenesisIdError>),
    #[error("reason: unknown")]
    UnknownReason,
}

impl TryFrom<&proto::HandshakeFailure> for (PeerInfo, HandshakeFailureReason) {
    type Error = ParseHandshakeFailureError;
    fn try_from(x: &proto::HandshakeFailure) -> Result<Self, Self::Error> {
        let pi = try_from_required(&x.peer_info).map_err(Self::Error::PeerInfo)?;
        let hfr = match x.reason.enum_value_or_default() {
            proto::handshake_failure::Reason::ProtocolVersionMismatch => {
                HandshakeFailureReason::ProtocolVersionMismatch {
                    version: x.version,
                    oldest_supported_version: x.oldest_supported_version,
                }
            }
            proto::handshake_failure::Reason::GenesisMismatch => {
                HandshakeFailureReason::GenesisMismatch(
                    try_from_required(&x.genesis_id).map_err(Self::Error::GenesisId)?,
                )
            }
            proto::handshake_failure::Reason::InvalidTarget => {
                HandshakeFailureReason::InvalidTarget
            }
            proto::handshake_failure::Reason::UNKNOWN => return Err(Self::Error::UnknownReason),
        };
        Ok((pi, hfr))
    }
}

//////////////////////////////////////////

impl From<&Edge> for proto::Edge {
    fn from(x: &Edge) -> Self {
        Self { borsh: x.try_to_vec().unwrap(), ..Self::default() }
    }
}

pub type ParseEdgeError = borsh::maybestd::io::Error;

impl TryFrom<&proto::Edge> for Edge {
    type Error = ParseEdgeError;
    fn try_from(x: &proto::Edge) -> Result<Self, Self::Error> {
        Self::try_from_slice(&x.borsh)
    }
}

//////////////////////////////////////////

impl From<&AnnounceAccount> for proto::AnnounceAccount {
    fn from(x: &AnnounceAccount) -> Self {
        Self { borsh: x.try_to_vec().unwrap(), ..Self::default() }
    }
}

pub type ParseAnnounceAccountError = borsh::maybestd::io::Error;

impl TryFrom<&proto::AnnounceAccount> for AnnounceAccount {
    type Error = ParseAnnounceAccountError;
    fn try_from(x: &proto::AnnounceAccount) -> Result<Self, Self::Error> {
        Self::try_from_slice(&x.borsh)
    }
}

//////////////////////////////////////////

impl From<&RoutingTableUpdate> for proto::RoutingTableUpdate {
    fn from(x: &RoutingTableUpdate) -> Self {
        Self {
            edges: x.edges.iter().map(Into::into).collect(),
            accounts: x.accounts.iter().map(Into::into).collect(),
            ..Self::default()
        }
    }
}

#[derive(Error, Debug)]
pub enum ParseRoutingTableUpdateError {
    #[error("edges {0}")]
    Edges(ParseVecError<ParseEdgeError>),
    #[error("accounts {0}")]
    Accounts(ParseVecError<ParseAnnounceAccountError>),
}

impl TryFrom<&proto::RoutingTableUpdate> for RoutingTableUpdate {
    type Error = ParseRoutingTableUpdateError;
    fn try_from(x: &proto::RoutingTableUpdate) -> Result<Self, Self::Error> {
        Ok(Self {
            edges: try_from_vec(&x.edges).map_err(Self::Error::Edges)?,
            accounts: try_from_vec(&x.accounts).map_err(Self::Error::Accounts)?,
        })
    }
}

//////////////////////////////////////////

impl From<&BlockHeader> for proto::BlockHeader {
    fn from(x: &BlockHeader) -> Self {
        Self { borsh: x.try_to_vec().unwrap(), ..Self::default() }
    }
}

pub type ParseBlockHeaderError = borsh::maybestd::io::Error;

impl TryFrom<&proto::BlockHeader> for BlockHeader {
    type Error = ParseBlockHeaderError;
    fn try_from(x: &proto::BlockHeader) -> Result<Self, Self::Error> {
        Self::try_from_slice(&x.borsh)
    }
}

//////////////////////////////////////////

impl From<&Block> for proto::Block {
    fn from(x: &Block) -> Self {
        Self { borsh: x.try_to_vec().unwrap(), ..Self::default() }
    }
}

pub type ParseBlockError = borsh::maybestd::io::Error;

impl TryFrom<&proto::Block> for Block {
    type Error = ParseBlockError;
    fn try_from(x: &proto::Block) -> Result<Self, Self::Error> {
        Self::try_from_slice(&x.borsh)
    }
}

//////////////////////////////////////////

impl From<&PeerMessage> for proto::PeerMessage {
    fn from(x: &PeerMessage) -> Self {
        Self {
            message_type: Some(match x {
                PeerMessage::Handshake(h) => ProtoMT::Handshake(h.into()),
                PeerMessage::HandshakeFailure(pi, hfr) => {
                    ProtoMT::HandshakeFailure((pi, hfr).into())
                }
                PeerMessage::LastEdge(e) => ProtoMT::LastEdge(proto::LastEdge {
                    edge: MF::some(e.into()),
                    ..Default::default()
                }),
                PeerMessage::SyncRoutingTable(rtu) => ProtoMT::SyncRoutingTable(rtu.into()),
                PeerMessage::RequestUpdateNonce(pei) => {
                    ProtoMT::UpdateNonceRequest(proto::UpdateNonceRequest {
                        partial_edge_info: MF::some(pei.into()),
                        ..Default::default()
                    })
                }
                PeerMessage::ResponseUpdateNonce(e) => {
                    ProtoMT::UpdateNonceResponse(proto::UpdateNonceResponse {
                        edge: MF::some(e.into()),
                        ..Default::default()
                    })
                }
                PeerMessage::PeersRequest => ProtoMT::PeersRequest(proto::PeersRequest::new()),
                PeerMessage::PeersResponse(pis) => ProtoMT::PeersResponse(proto::PeersResponse {
                    peers: pis.iter().map(Into::into).collect(),
                    ..Default::default()
                }),
                PeerMessage::BlockHeadersRequest(bhs) => {
                    ProtoMT::BlockHeadersRequest(proto::BlockHeadersRequest {
                        block_hashes: bhs.iter().map(Into::into).collect(),
                        ..Default::default()
                    })
                }
                PeerMessage::BlockHeaders(bhs) => {
                    ProtoMT::BlockHeadersResponse(proto::BlockHeadersResponse {
                        block_headers: bhs.iter().map(Into::into).collect(),
                        ..Default::default()
                    })
                }
                PeerMessage::BlockRequest(bh) => ProtoMT::BlockRequest(proto::BlockRequest {
                    block_hash: MF::some(bh.into()),
                    ..Default::default()
                }),
                PeerMessage::Block(b) => ProtoMT::BlockResponse(proto::BlockResponse {
                    block: MF::some(b.into()),
                    ..Default::default()
                }),
                PeerMessage::Transaction(t) => ProtoMT::Transaction(proto::SignedTransaction {
                    borsh: t.try_to_vec().unwrap(),
                    ..Default::default()
                }),
                PeerMessage::Routed(r) => ProtoMT::Routed(proto::RoutedMessage {
                    borsh: r.try_to_vec().unwrap(),
                    ..Default::default()
                }),
                PeerMessage::Disconnect => ProtoMT::Disconnect(proto::Disconnect::new()),
                PeerMessage::Challenge(r) => ProtoMT::Challenge(proto::Challenge {
                    borsh: r.try_to_vec().unwrap(),
                    ..Default::default()
                }),
                PeerMessage::EpochSyncRequest(epoch_id) => {
                    ProtoMT::EpochSyncRequest(proto::EpochSyncRequest {
                        epoch_id: MF::some((&epoch_id.0).into()),
                        ..Default::default()
                    })
                }
                PeerMessage::EpochSyncResponse(esr) => {
                    ProtoMT::EpochSyncResponse(proto::EpochSyncResponse {
                        borsh: esr.try_to_vec().unwrap(),
                        ..Default::default()
                    })
                }
                PeerMessage::EpochSyncFinalizationRequest(epoch_id) => {
                    ProtoMT::EpochSyncFinalizationRequest(proto::EpochSyncFinalizationRequest {
                        epoch_id: MF::some((&epoch_id.0).into()),
                        ..Default::default()
                    })
                }
                PeerMessage::EpochSyncFinalizationResponse(esfr) => {
                    ProtoMT::EpochSyncFinalizationResponse(proto::EpochSyncFinalizationResponse {
                        borsh: esfr.try_to_vec().unwrap(),
                        ..Default::default()
                    })
                }
                PeerMessage::RoutingTableSyncV2(rs) => {
                    ProtoMT::RoutingTableSyncV2(proto::RoutingSyncV2 {
                        borsh: rs.try_to_vec().unwrap(),
                        ..Default::default()
                    })
                }
            }),
            ..Default::default()
        }
    }
}

pub type ParseTransactionError = borsh::maybestd::io::Error;
pub type ParseRoutedError = borsh::maybestd::io::Error;
pub type ParseChallengeError = borsh::maybestd::io::Error;
pub type ParseEpochSyncResponseError = borsh::maybestd::io::Error;
pub type ParseEpochSyncFinalizationResponseError = borsh::maybestd::io::Error;
pub type ParseRoutingTableSyncV2Error = borsh::maybestd::io::Error;

#[derive(Error, Debug)]
pub enum ParsePeerMessageError {
    #[error("empty message")]
    Empty,
    #[error("handshake: {0}")]
    Handshake(ParseHandshakeError),
    #[error("handshake_failure: {0}")]
    HandshakeFailure(ParseHandshakeFailureError),
    #[error("last_edge: {0}")]
    LastEdge(ParseRequiredError<ParseEdgeError>),
    #[error("sync_routing_table: {0}")]
    SyncRoutingTable(ParseRoutingTableUpdateError),
    #[error("update_nonce_requrest: {0}")]
    UpdateNonceRequest(ParseRequiredError<ParsePartialEdgeInfoError>),
    #[error("update_nonce_response: {0}")]
    UpdateNonceResponse(ParseRequiredError<ParseEdgeError>),
    #[error("peers_response: {0}")]
    PeersResponse(ParseVecError<ParsePeerInfoError>),
    #[error("block_headers_request: {0}")]
    BlockHeadersRequest(ParseVecError<ParseCryptoHashError>),
    #[error("block_headers_response: {0}")]
    BlockHeadersResponse(ParseVecError<ParseBlockHeaderError>),
    #[error("block_request: {0}")]
    BlockRequest(ParseRequiredError<ParseCryptoHashError>),
    #[error("block_response: {0}")]
    BlockResponse(ParseRequiredError<ParseBlockError>),
    #[error("transaction: {0}")]
    Transaction(ParseTransactionError),
    #[error("routed: {0}")]
    Routed(ParseRoutedError),
    #[error("challenge: {0}")]
    Challenge(ParseChallengeError),
    #[error("epoch_sync_request: {0}")]
    EpochSyncRequest(ParseRequiredError<ParseCryptoHashError>),
    #[error("epoch_sync_response: {0}")]
    EpochSyncResponse(ParseEpochSyncResponseError),
    #[error("epoch_sync_finalization_request: {0}")]
    EpochSyncFinalizationRequest(ParseRequiredError<ParseCryptoHashError>),
    #[error("epoch_sync_finalization_response: {0}")]
    EpochSyncFinalizationResponse(ParseEpochSyncFinalizationResponseError),
    #[error("routing_table_sync_v2")]
    RoutingTableSyncV2(ParseRoutingTableSyncV2Error),
}

impl TryFrom<&proto::PeerMessage> for PeerMessage {
    type Error = ParsePeerMessageError;
    fn try_from(x: &proto::PeerMessage) -> Result<Self, Self::Error> {
        Ok(match x.message_type.as_ref().ok_or(Self::Error::Empty)? {
            ProtoMT::Handshake(h) => {
                PeerMessage::Handshake(h.try_into().map_err(Self::Error::Handshake)?)
            }
            ProtoMT::HandshakeFailure(hf) => {
                let (pi, hfr) = hf.try_into().map_err(Self::Error::HandshakeFailure)?;
                PeerMessage::HandshakeFailure(pi, hfr)
            }
            ProtoMT::LastEdge(le) => {
                PeerMessage::LastEdge(try_from_required(&le.edge).map_err(Self::Error::LastEdge)?)
            }
            ProtoMT::SyncRoutingTable(rtu) => PeerMessage::SyncRoutingTable(
                rtu.try_into().map_err(Self::Error::SyncRoutingTable)?,
            ),
            ProtoMT::UpdateNonceRequest(unr) => PeerMessage::RequestUpdateNonce(
                try_from_required(&unr.partial_edge_info)
                    .map_err(Self::Error::UpdateNonceRequest)?,
            ),
            ProtoMT::UpdateNonceResponse(unr) => PeerMessage::ResponseUpdateNonce(
                try_from_required(&unr.edge).map_err(Self::Error::UpdateNonceResponse)?,
            ),
            ProtoMT::PeersRequest(_) => PeerMessage::PeersRequest,
            ProtoMT::PeersResponse(pr) => PeerMessage::PeersResponse(
                try_from_vec(&pr.peers).map_err(Self::Error::PeersResponse)?,
            ),
            ProtoMT::BlockHeadersRequest(bhr) => PeerMessage::BlockHeadersRequest(
                try_from_vec(&bhr.block_hashes).map_err(Self::Error::BlockHeadersRequest)?,
            ),
            ProtoMT::BlockHeadersResponse(bhr) => PeerMessage::BlockHeaders(
                try_from_vec(&bhr.block_headers).map_err(Self::Error::BlockHeadersResponse)?,
            ),
            ProtoMT::BlockRequest(br) => PeerMessage::BlockRequest(
                try_from_required(&br.block_hash).map_err(Self::Error::BlockRequest)?,
            ),
            ProtoMT::BlockResponse(br) => PeerMessage::Block(
                try_from_required(&br.block).map_err(Self::Error::BlockResponse)?,
            ),
            ProtoMT::Transaction(t) => PeerMessage::Transaction(
                SignedTransaction::try_from_slice(&t.borsh).map_err(Self::Error::Transaction)?,
            ),
            ProtoMT::Routed(r) => PeerMessage::Routed(Box::new(
                RoutedMessage::try_from_slice(&r.borsh).map_err(Self::Error::Routed)?,
            )),
            ProtoMT::Disconnect(_) => PeerMessage::Disconnect,
            ProtoMT::Challenge(c) => PeerMessage::Challenge(
                Challenge::try_from_slice(&c.borsh).map_err(Self::Error::Challenge)?,
            ),
            ProtoMT::EpochSyncRequest(esr) => PeerMessage::EpochSyncRequest(EpochId(
                try_from_required(&esr.epoch_id).map_err(Self::Error::EpochSyncRequest)?,
            )),
            ProtoMT::EpochSyncResponse(esr) => PeerMessage::EpochSyncResponse(Box::new(
                EpochSyncResponse::try_from_slice(&esr.borsh)
                    .map_err(Self::Error::EpochSyncResponse)?,
            )),
            ProtoMT::EpochSyncFinalizationRequest(esr) => {
                PeerMessage::EpochSyncFinalizationRequest(EpochId(
                    try_from_required(&esr.epoch_id)
                        .map_err(Self::Error::EpochSyncFinalizationRequest)?,
                ))
            }
            ProtoMT::EpochSyncFinalizationResponse(esr) => {
                PeerMessage::EpochSyncFinalizationResponse(Box::new(
                    EpochSyncFinalizationResponse::try_from_slice(&esr.borsh)
                        .map_err(Self::Error::EpochSyncFinalizationResponse)?,
                ))
            }
            ProtoMT::RoutingTableSyncV2(rts) => PeerMessage::RoutingTableSyncV2(
                RoutingSyncV2::try_from_slice(&rts.borsh)
                    .map_err(Self::Error::RoutingTableSyncV2)?,
            ),
        })
    }
}
