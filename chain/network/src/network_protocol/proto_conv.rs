/// Contains protobuf <-> network_protocol conversions.
use crate::network_protocol::proto;
use crate::network_protocol::proto::peer_message::MessageType as ProtoMT;
use crate::network_protocol::{
    Handshake, HandshakeFailureReason, PeerMessage, RoutingSyncV2, RoutingTableUpdate,
};
use anyhow::{bail, Context};
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

fn try_from_vec<'a, X, Y: TryFrom<&'a X>>(xs: &'a Vec<X>) -> Result<Vec<Y>, Y::Error> {
    let mut ys = vec![];
    for x in xs {
        ys.push(x.try_into()?);
    }
    Ok(ys)
}

fn try_from_required<'a, X, Y: TryFrom<&'a X, Error = anyhow::Error>>(
    x: &'a Option<X>,
) -> anyhow::Result<Y> {
    x.as_ref().context("missing")?.try_into()
}

impl From<&CryptoHash> for proto::CryptoHash {
    fn from(x: &CryptoHash) -> Self {
        Self { hash: x.0.into() }
    }
}

impl TryFrom<&proto::CryptoHash> for CryptoHash {
    type Error = anyhow::Error;
    fn try_from(p: &proto::CryptoHash) -> anyhow::Result<Self> {
        CryptoHash::try_from(&p.hash[..]).map_err(|err| anyhow::Error::msg(err.to_string()))
    }
}

//////////////////////////////////////////

impl From<&GenesisId> for proto::GenesisId {
    fn from(x: &GenesisId) -> Self {
        Self { chain_id: x.chain_id.clone(), hash: Some((&x.hash).into()) }
    }
}

impl TryFrom<&proto::GenesisId> for GenesisId {
    type Error = anyhow::Error;
    fn try_from(p: &proto::GenesisId) -> anyhow::Result<Self> {
        Ok(Self { chain_id: p.chain_id.clone(), hash: try_from_required(&p.hash).context("hash")? })
    }
}

//////////////////////////////////////////

impl From<&PeerChainInfoV2> for proto::PeerChainInfo {
    fn from(x: &PeerChainInfoV2) -> Self {
        Self {
            genesis_id: Some((&x.genesis_id).into()),
            height: x.height,
            tracked_shards: x.tracked_shards.clone(),
            archival: x.archival,
        }
    }
}

impl TryFrom<&proto::PeerChainInfo> for PeerChainInfoV2 {
    type Error = anyhow::Error;
    fn try_from(p: &proto::PeerChainInfo) -> anyhow::Result<Self> {
        Ok(Self {
            genesis_id: try_from_required(&p.genesis_id).context("genesis_id")?,
            height: p.height,
            tracked_shards: p.tracked_shards.clone(),
            archival: p.archival,
        })
    }
}

//////////////////////////////////////////

impl From<&PeerId> for proto::PublicKey {
    fn from(x: &PeerId) -> Self {
        Self { borsh: x.try_to_vec().unwrap() }
    }
}

impl TryFrom<&proto::PublicKey> for PeerId {
    type Error = anyhow::Error;
    fn try_from(p: &proto::PublicKey) -> anyhow::Result<Self> {
        Ok(Self::try_from_slice(&p.borsh)?)
    }
}

//////////////////////////////////////////

impl From<&PartialEdgeInfo> for proto::PartialEdgeInfo {
    fn from(x: &PartialEdgeInfo) -> Self {
        Self { borsh: x.try_to_vec().unwrap() }
    }
}

impl TryFrom<&proto::PartialEdgeInfo> for PartialEdgeInfo {
    type Error = anyhow::Error;
    fn try_from(p: &proto::PartialEdgeInfo) -> anyhow::Result<Self> {
        Ok(Self::try_from_slice(&p.borsh)?)
    }
}

//////////////////////////////////////////

impl From<&PeerInfo> for proto::PeerInfo {
    fn from(x: &PeerInfo) -> Self {
        Self { borsh: x.try_to_vec().unwrap() }
    }
}

impl TryFrom<&proto::PeerInfo> for PeerInfo {
    type Error = anyhow::Error;
    fn try_from(x: &proto::PeerInfo) -> anyhow::Result<Self> {
        Ok(Self::try_from_slice(&x.borsh)?)
    }
}

//////////////////////////////////////////

impl From<&Handshake> for proto::Handshake {
    fn from(x: &Handshake) -> Self {
        Self {
            protocol_version: x.protocol_version,
            oldest_supported_version: x.oldest_supported_version,
            sender_peer_id: Some((&x.sender_peer_id).into()),
            target_peer_id: Some((&x.target_peer_id).into()),
            sender_listen_port: x.sender_listen_port.unwrap_or(0).into(),
            sender_chain_info: Some((&x.sender_chain_info).into()),
            partial_edge_info: Some((&x.partial_edge_info).into()),
        }
    }
}

impl TryFrom<&proto::Handshake> for Handshake {
    type Error = anyhow::Error;
    fn try_from(p: &proto::Handshake) -> anyhow::Result<Self> {
        Ok(Self {
            protocol_version: p.protocol_version,
            oldest_supported_version: p.oldest_supported_version,
            sender_peer_id: try_from_required(&p.sender_peer_id).context("sender_peer_id")?,
            target_peer_id: try_from_required(&p.target_peer_id).context("target_peer_id")?,
            sender_listen_port: {
                let port = u16::try_from(p.sender_listen_port).context("sender_listen_port")?;
                if port == 0 {
                    None
                } else {
                    Some(port)
                }
            },
            sender_chain_info: try_from_required(&p.sender_chain_info)
                .context("sender_chain_info")?,
            partial_edge_info: try_from_required(&p.partial_edge_info)
                .context("partial_edge_info")?,
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
                peer_info: Some(pi.into()),
                reason: proto::handshake_failure::Reason::ProtocolVersionMismatch.into(),
                version: *version,
                oldest_supported_version: *oldest_supported_version,
                ..Default::default()
            },
            HandshakeFailureReason::GenesisMismatch(genesis_id) => Self {
                peer_info: Some(pi.into()),
                reason: proto::handshake_failure::Reason::GenesisMismatch.into(),
                genesis_id: Some(genesis_id.into()),
                ..Default::default()
            },
            HandshakeFailureReason::InvalidTarget => Self {
                peer_info: Some(pi.into()),
                reason: proto::handshake_failure::Reason::InvalidTarget.into(),
                ..Default::default()
            },
        }
    }
}

impl TryFrom<&proto::HandshakeFailure> for (PeerInfo, HandshakeFailureReason) {
    type Error = anyhow::Error;
    fn try_from(x: &proto::HandshakeFailure) -> anyhow::Result<Self> {
        let pi = try_from_required(&x.peer_info).context("peer_info")?;
        let hfr =
            match proto::handshake_failure::Reason::from_i32(x.reason).context("unknown reason")? {
                proto::handshake_failure::Reason::ProtocolVersionMismatch => {
                    HandshakeFailureReason::ProtocolVersionMismatch {
                        version: x.version,
                        oldest_supported_version: x.oldest_supported_version,
                    }
                }
                proto::handshake_failure::Reason::GenesisMismatch => {
                    HandshakeFailureReason::GenesisMismatch(
                        try_from_required(&x.genesis_id).context("genesis_id")?,
                    )
                }
                proto::handshake_failure::Reason::InvalidTarget => {
                    HandshakeFailureReason::InvalidTarget
                }
                proto::handshake_failure::Reason::Unknown => bail!("unknown reason"),
            };
        Ok((pi, hfr))
    }
}

//////////////////////////////////////////

impl From<&Edge> for proto::Edge {
    fn from(x: &Edge) -> Self {
        Self { borsh: x.try_to_vec().unwrap() }
    }
}

impl TryFrom<&proto::Edge> for Edge {
    type Error = anyhow::Error;
    fn try_from(x: &proto::Edge) -> anyhow::Result<Self> {
        Ok(Self::try_from_slice(&x.borsh)?)
    }
}

//////////////////////////////////////////

impl From<&AnnounceAccount> for proto::AnnounceAccount {
    fn from(x: &AnnounceAccount) -> Self {
        Self { borsh: x.try_to_vec().unwrap() }
    }
}

impl TryFrom<&proto::AnnounceAccount> for AnnounceAccount {
    type Error = anyhow::Error;
    fn try_from(x: &proto::AnnounceAccount) -> anyhow::Result<Self> {
        Ok(Self::try_from_slice(&x.borsh)?)
    }
}

//////////////////////////////////////////

impl From<&RoutingTableUpdate> for proto::RoutingTableUpdate {
    fn from(x: &RoutingTableUpdate) -> Self {
        Self {
            edges: x.edges.iter().map(Into::into).collect(),
            accounts: x.accounts.iter().map(Into::into).collect(),
        }
    }
}

impl TryFrom<&proto::RoutingTableUpdate> for RoutingTableUpdate {
    type Error = anyhow::Error;
    fn try_from(x: &proto::RoutingTableUpdate) -> anyhow::Result<Self> {
        Ok(Self {
            edges: try_from_vec(&x.edges).context("edges")?,
            accounts: try_from_vec(&x.accounts).context("accounts")?,
        })
    }
}

//////////////////////////////////////////

impl From<&BlockHeader> for proto::BlockHeader {
    fn from(x: &BlockHeader) -> Self {
        Self { borsh: x.try_to_vec().unwrap() }
    }
}

impl TryFrom<&proto::BlockHeader> for BlockHeader {
    type Error = anyhow::Error;
    fn try_from(x: &proto::BlockHeader) -> anyhow::Result<Self> {
        Ok(Self::try_from_slice(&x.borsh)?)
    }
}

//////////////////////////////////////////

impl From<&Block> for proto::Block {
    fn from(x: &Block) -> Self {
        Self { borsh: x.try_to_vec().unwrap() }
    }
}

impl TryFrom<&proto::Block> for Block {
    type Error = anyhow::Error;
    fn try_from(x: &proto::Block) -> anyhow::Result<Self> {
        Ok(Self::try_from_slice(&x.borsh)?)
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
                PeerMessage::LastEdge(e) => {
                    ProtoMT::LastEdge(proto::LastEdge { edge: Some(e.into()) })
                }
                PeerMessage::SyncRoutingTable(rtu) => ProtoMT::SyncRoutingTable(rtu.into()),
                PeerMessage::RequestUpdateNonce(pei) => {
                    ProtoMT::UpdateNonceRequest(proto::UpdateNonceRequest {
                        partial_edge_info: Some(pei.into()),
                    })
                }
                PeerMessage::ResponseUpdateNonce(e) => {
                    ProtoMT::UpdateNonceResponse(proto::UpdateNonceResponse {
                        edge: Some(e.into()),
                    })
                }
                PeerMessage::PeersRequest => ProtoMT::PeersRequest(proto::PeersRequest {}),
                PeerMessage::PeersResponse(pis) => ProtoMT::PeersResponse(proto::PeersResponse {
                    peers: pis.iter().map(Into::into).collect(),
                }),
                PeerMessage::BlockHeadersRequest(bhs) => {
                    ProtoMT::BlockHeadersRequest(proto::BlockHeadersRequest {
                        block_hashes: bhs.iter().map(Into::into).collect(),
                    })
                }
                PeerMessage::BlockHeaders(bhs) => {
                    ProtoMT::BlockHeadersResponse(proto::BlockHeadersResponse {
                        block_headers: bhs.iter().map(Into::into).collect(),
                    })
                }
                PeerMessage::BlockRequest(bh) => {
                    ProtoMT::BlockRequest(proto::BlockRequest { block_hash: Some(bh.into()) })
                }
                PeerMessage::Block(b) => {
                    ProtoMT::BlockResponse(proto::BlockResponse { block: Some(b.into()) })
                }
                PeerMessage::Transaction(t) => ProtoMT::Transaction(proto::SignedTransaction {
                    borsh: t.try_to_vec().unwrap(),
                }),
                PeerMessage::Routed(r) => {
                    ProtoMT::Routed(proto::RoutedMessage { borsh: r.try_to_vec().unwrap() })
                }
                PeerMessage::Disconnect => ProtoMT::Disconnect(proto::Disconnect {}),
                PeerMessage::Challenge(r) => {
                    ProtoMT::Challenge(proto::Challenge { borsh: r.try_to_vec().unwrap() })
                }
                PeerMessage::EpochSyncRequest(epoch_id) => {
                    ProtoMT::EpochSyncRequest(proto::EpochSyncRequest {
                        epoch_id: Some((&epoch_id.0).into()),
                    })
                }
                PeerMessage::EpochSyncResponse(esr) => {
                    ProtoMT::EpochSyncResponse(proto::EpochSyncResponse {
                        borsh: esr.try_to_vec().unwrap(),
                    })
                }
                PeerMessage::EpochSyncFinalizationRequest(epoch_id) => {
                    ProtoMT::EpochSyncFinalizationRequest(proto::EpochSyncFinalizationRequest {
                        epoch_id: Some((&epoch_id.0).into()),
                    })
                }
                PeerMessage::EpochSyncFinalizationResponse(esfr) => {
                    ProtoMT::EpochSyncFinalizationResponse(proto::EpochSyncFinalizationResponse {
                        borsh: esfr.try_to_vec().unwrap(),
                    })
                }
                PeerMessage::RoutingTableSyncV2(rs) => {
                    ProtoMT::RoutingTableSyncV2(proto::RoutingSyncV2 {
                        borsh: rs.try_to_vec().unwrap(),
                    })
                }
            }),
        }
    }
}

impl TryFrom<&proto::PeerMessage> for PeerMessage {
    type Error = anyhow::Error;
    fn try_from(x: &proto::PeerMessage) -> anyhow::Result<Self> {
        Ok(match x.message_type.as_ref().context("empty or unknown")? {
            ProtoMT::Handshake(h) => PeerMessage::Handshake(h.try_into().context("Handshake")?),
            ProtoMT::HandshakeFailure(hf) => {
                let (pi, hfr) = hf.try_into().context("HandshakeFailure")?;
                PeerMessage::HandshakeFailure(pi, hfr)
            }
            ProtoMT::LastEdge(le) => {
                PeerMessage::LastEdge(try_from_required(&le.edge).context("LastEdge")?)
            }
            ProtoMT::SyncRoutingTable(rtu) => {
                PeerMessage::SyncRoutingTable(rtu.try_into().context("SyncRoutingTable")?)
            }
            ProtoMT::UpdateNonceRequest(unr) => PeerMessage::RequestUpdateNonce(
                try_from_required(&unr.partial_edge_info).context("UpdateNonceRequest")?,
            ),
            ProtoMT::UpdateNonceResponse(unr) => PeerMessage::ResponseUpdateNonce(
                try_from_required(&unr.edge).context("UpdateNonceResponse")?,
            ),
            ProtoMT::PeersRequest(_) => PeerMessage::PeersRequest,
            ProtoMT::PeersResponse(pr) => {
                PeerMessage::PeersResponse(try_from_vec(&pr.peers).context("PeersResponse")?)
            }
            ProtoMT::BlockHeadersRequest(bhr) => PeerMessage::BlockHeadersRequest(
                try_from_vec(&bhr.block_hashes).context("BlockHeadersRequest")?,
            ),
            ProtoMT::BlockHeadersResponse(bhr) => PeerMessage::BlockHeaders(
                try_from_vec(&bhr.block_headers).context("BlockHeadersResponse")?,
            ),
            ProtoMT::BlockRequest(br) => PeerMessage::BlockRequest(
                try_from_required(&br.block_hash).context("BlockRequest")?,
            ),
            ProtoMT::BlockResponse(br) => {
                PeerMessage::Block(try_from_required(&br.block).context("BlockResponse")?)
            }
            ProtoMT::Transaction(t) => PeerMessage::Transaction(
                SignedTransaction::try_from_slice(&t.borsh).context("Transaction")?,
            ),
            ProtoMT::Routed(r) => PeerMessage::Routed(Box::new(
                RoutedMessage::try_from_slice(&r.borsh).context("Routed")?,
            )),
            ProtoMT::Disconnect(_) => PeerMessage::Disconnect,
            ProtoMT::Challenge(c) => {
                PeerMessage::Challenge(Challenge::try_from_slice(&c.borsh).context("Challenge")?)
            }
            ProtoMT::EpochSyncRequest(esr) => PeerMessage::EpochSyncRequest(EpochId(
                try_from_required(&esr.epoch_id).context("EpochSyncRequest")?,
            )),
            ProtoMT::EpochSyncResponse(esr) => PeerMessage::EpochSyncResponse(Box::new(
                EpochSyncResponse::try_from_slice(&esr.borsh).context("EpochSyncResponse")?,
            )),
            ProtoMT::EpochSyncFinalizationRequest(esr) => {
                PeerMessage::EpochSyncFinalizationRequest(EpochId(
                    try_from_required(&esr.epoch_id).context("EpochSyncFinalizationRequest")?,
                ))
            }
            ProtoMT::EpochSyncFinalizationResponse(esr) => {
                PeerMessage::EpochSyncFinalizationResponse(Box::new(
                    EpochSyncFinalizationResponse::try_from_slice(&esr.borsh)
                        .context("EpochSyncFinalizationResponse")?,
                ))
            }
            ProtoMT::RoutingTableSyncV2(rts) => PeerMessage::RoutingTableSyncV2(
                RoutingSyncV2::try_from_slice(&rts.borsh).context("RoutingTableSyncV2")?,
            ),
        })
    }
}
