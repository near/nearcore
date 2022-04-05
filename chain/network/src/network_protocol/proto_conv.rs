use crate::network_protocol::proto;
use crate::network_protocol::proto::peer_message::MessageType as ProtoMT;
use crate::network_protocol::{
    Handshake, HandshakeFailureReason, PeerMessage, RoutingSyncV2, RoutingTableUpdate,
};
use anyhow::{bail, Context};
/// Contains protobuf <-> network_protocol conversions.
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

impl TryFrom<&proto::CryptoHash> for CryptoHash {
    type Error = anyhow::Error;
    fn try_from(p: &proto::CryptoHash) -> anyhow::Result<Self> {
        CryptoHash::try_from(&p.hash[..]).map_err(|err| anyhow::Error::msg(err.to_string()))
    }
}

impl TryFrom<&CryptoHash> for proto::CryptoHash {
    type Error = anyhow::Error;
    fn try_from(x: &CryptoHash) -> anyhow::Result<Self> {
        Ok(Self { hash: x.0.into() })
    }
}

//////////////////////////////////////////

impl TryFrom<&proto::GenesisId> for GenesisId {
    type Error = anyhow::Error;
    fn try_from(p: &proto::GenesisId) -> anyhow::Result<Self> {
        Ok(Self {
            chain_id: p.chain_id.clone(),
            hash: (|| CryptoHash::try_from(p.hash.as_ref().context("missing")?))()
                .context("hash")?,
        })
    }
}

impl TryFrom<&GenesisId> for proto::GenesisId {
    type Error = anyhow::Error;
    fn try_from(x: &GenesisId) -> anyhow::Result<Self> {
        Ok(Self { chain_id: x.chain_id.clone(), hash: Some((&x.hash).try_into().context("hash")?) })
    }
}

//////////////////////////////////////////

impl TryFrom<&proto::PeerChainInfo> for PeerChainInfoV2 {
    type Error = anyhow::Error;
    fn try_from(p: &proto::PeerChainInfo) -> anyhow::Result<Self> {
        Ok(Self {
            genesis_id: (|| GenesisId::try_from(p.genesis_id.as_ref().context("missing")?))()
                .context("genesis_id")?,
            height: p.height,
            tracked_shards: p.tracked_shards.clone(),
            archival: p.archival,
        })
    }
}

impl TryFrom<&PeerChainInfoV2> for proto::PeerChainInfo {
    type Error = anyhow::Error;
    fn try_from(x: &PeerChainInfoV2) -> anyhow::Result<Self> {
        Ok(Self {
            genesis_id: Some((&x.genesis_id).try_into().context("genesis_id")?),
            height: x.height,
            tracked_shards: x.tracked_shards.clone(),
            archival: x.archival,
        })
    }
}

//////////////////////////////////////////

impl TryFrom<&proto::PublicKey> for PeerId {
    type Error = anyhow::Error;
    fn try_from(p: &proto::PublicKey) -> anyhow::Result<Self> {
        Ok(Self::try_from_slice(&p.borsh)?)
    }
}

impl TryFrom<&PeerId> for proto::PublicKey {
    type Error = anyhow::Error;
    fn try_from(x: &PeerId) -> anyhow::Result<Self> {
        Ok(Self { borsh: x.try_to_vec()? })
    }
}

//////////////////////////////////////////

impl TryFrom<&proto::PartialEdgeInfo> for PartialEdgeInfo {
    type Error = anyhow::Error;
    fn try_from(p: &proto::PartialEdgeInfo) -> anyhow::Result<Self> {
        Ok(Self::try_from_slice(&p.borsh)?)
    }
}

impl TryFrom<&PartialEdgeInfo> for proto::PartialEdgeInfo {
    type Error = anyhow::Error;
    fn try_from(x: &PartialEdgeInfo) -> anyhow::Result<Self> {
        Ok(Self { borsh: x.try_to_vec()? })
    }
}

//////////////////////////////////////////

impl TryFrom<&PeerInfo> for proto::PeerInfo {
    type Error = anyhow::Error;
    fn try_from(x: &PeerInfo) -> anyhow::Result<Self> {
        Ok(Self { borsh: x.try_to_vec()? })
    }
}

impl TryFrom<&proto::PeerInfo> for PeerInfo {
    type Error = anyhow::Error;
    fn try_from(x: &proto::PeerInfo) -> anyhow::Result<Self> {
        Ok(Self::try_from_slice(&x.borsh)?)
    }
}

//////////////////////////////////////////

impl TryFrom<&proto::Handshake> for Handshake {
    type Error = anyhow::Error;
    fn try_from(p: &proto::Handshake) -> anyhow::Result<Self> {
        Ok(Self {
            protocol_version: p.protocol_version,
            oldest_supported_version: p.oldest_supported_version,
            sender_peer_id: (|| PeerId::try_from(p.sender_peer_id.as_ref().context("missing")?))()
                .context("sender_peer_id")?,
            target_peer_id: (|| PeerId::try_from(p.target_peer_id.as_ref().context("missing")?))()
                .context("target_peer_id")?,
            sender_listen_port: {
                let port = u16::try_from(p.sender_listen_port).context("sender_listen_port")?;
                if port == 0 {
                    None
                } else {
                    Some(port)
                }
            },
            sender_chain_info: (|| {
                PeerChainInfoV2::try_from(p.sender_chain_info.as_ref().context("missing")?)
            })()
            .context("sender_chain_info")?,
            partial_edge_info: (|| {
                PartialEdgeInfo::try_from(p.partial_edge_info.as_ref().context("missing")?)
            })()
            .context("partial_edge_info")?,
        })
    }
}

impl TryFrom<&Handshake> for proto::Handshake {
    type Error = anyhow::Error;
    fn try_from(x: &Handshake) -> anyhow::Result<Self> {
        Ok(Self {
            protocol_version: x.protocol_version,
            oldest_supported_version: x.oldest_supported_version,
            sender_peer_id: Some((&x.sender_peer_id).try_into().context("sender_peer_id")?),
            target_peer_id: Some((&x.target_peer_id).try_into().context("target_peer_id")?),
            sender_listen_port: x.sender_listen_port.unwrap_or(0).into(),
            sender_chain_info: Some(
                (&x.sender_chain_info).try_into().context("sender_chain_info")?,
            ),
            partial_edge_info: Some(
                (&x.partial_edge_info).try_into().context("partial_edge_info")?,
            ),
        })
    }
}

//////////////////////////////////////////

impl TryFrom<(&PeerInfo, &HandshakeFailureReason)> for proto::HandshakeFailure {
    type Error = anyhow::Error;
    fn try_from((pi, hfr): (&PeerInfo, &HandshakeFailureReason)) -> anyhow::Result<Self> {
        Ok(match hfr {
            HandshakeFailureReason::ProtocolVersionMismatch {
                version,
                oldest_supported_version,
            } => Self {
                peer_info: Some(pi.try_into()?),
                reason: proto::handshake_failure::Reason::ProtocolVersionMismatch.into(),
                version: *version,
                oldest_supported_version: *oldest_supported_version,
                ..Default::default()
            },
            HandshakeFailureReason::GenesisMismatch(genesis_id) => Self {
                peer_info: Some(pi.try_into()?),
                reason: proto::handshake_failure::Reason::GenesisMismatch.into(),
                genesis_id: Some(genesis_id.try_into()?),
                ..Default::default()
            },
            HandshakeFailureReason::InvalidTarget => Self {
                peer_info: Some(pi.try_into()?),
                reason: proto::handshake_failure::Reason::InvalidTarget.into(),
                ..Default::default()
            },
        })
    }
}

impl TryFrom<&proto::HandshakeFailure> for (PeerInfo, HandshakeFailureReason) {
    type Error = anyhow::Error;
    fn try_from(x: &proto::HandshakeFailure) -> anyhow::Result<Self> {
        let pi = (|| PeerInfo::try_from(x.peer_info.as_ref().context("missing")?))()
            .context("peer_info")?;
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
                        (|| GenesisId::try_from(x.genesis_id.as_ref().context("missing")?))()
                            .context("genesis_id")?,
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

impl TryFrom<&Edge> for proto::Edge {
    type Error = anyhow::Error;
    fn try_from(x: &Edge) -> anyhow::Result<Self> {
        Ok(Self { borsh: x.try_to_vec()? })
    }
}

impl TryFrom<&proto::Edge> for Edge {
    type Error = anyhow::Error;
    fn try_from(x: &proto::Edge) -> anyhow::Result<Self> {
        Ok(Self::try_from_slice(&x.borsh)?)
    }
}

//////////////////////////////////////////

impl TryFrom<&AnnounceAccount> for proto::AnnounceAccount {
    type Error = anyhow::Error;
    fn try_from(x: &AnnounceAccount) -> anyhow::Result<Self> {
        Ok(Self { borsh: x.try_to_vec()? })
    }
}

impl TryFrom<&proto::AnnounceAccount> for AnnounceAccount {
    type Error = anyhow::Error;
    fn try_from(x: &proto::AnnounceAccount) -> anyhow::Result<Self> {
        Ok(Self::try_from_slice(&x.borsh)?)
    }
}

//////////////////////////////////////////

impl TryFrom<&RoutingTableUpdate> for proto::RoutingTableUpdate {
    type Error = anyhow::Error;
    fn try_from(x: &RoutingTableUpdate) -> anyhow::Result<Self> {
        let mut p = Self::default();
        for e in &x.edges {
            p.edges.push(e.try_into().context("edges")?);
        }
        for a in &x.accounts {
            p.accounts.push(a.try_into().context("accounts")?);
        }
        Ok(p)
    }
}

impl TryFrom<&proto::RoutingTableUpdate> for RoutingTableUpdate {
    type Error = anyhow::Error;
    fn try_from(x: &proto::RoutingTableUpdate) -> anyhow::Result<Self> {
        let mut p = Self { edges: vec![], accounts: vec![] };
        for e in &x.edges {
            p.edges.push(e.try_into().context("edges")?);
        }
        for a in &x.accounts {
            p.accounts.push(a.try_into().context("accounts")?);
        }
        Ok(p)
    }
}

//////////////////////////////////////////

impl TryFrom<&proto::BlockHeader> for BlockHeader {
    type Error = anyhow::Error;
    fn try_from(x: &proto::BlockHeader) -> anyhow::Result<Self> {
        Ok(Self::try_from_slice(&x.borsh)?)
    }
}

impl TryFrom<&BlockHeader> for proto::BlockHeader {
    type Error = anyhow::Error;
    fn try_from(x: &BlockHeader) -> anyhow::Result<Self> {
        Ok(Self { borsh: x.try_to_vec()? })
    }
}

//////////////////////////////////////////

impl TryFrom<&proto::Block> for Block {
    type Error = anyhow::Error;
    fn try_from(x: &proto::Block) -> anyhow::Result<Self> {
        Ok(Self::try_from_slice(&x.borsh)?)
    }
}

impl TryFrom<&Block> for proto::Block {
    type Error = anyhow::Error;
    fn try_from(x: &Block) -> anyhow::Result<Self> {
        Ok(Self { borsh: x.try_to_vec()? })
    }
}

//////////////////////////////////////////

impl TryFrom<&PeerMessage> for proto::PeerMessage {
    type Error = anyhow::Error;
    fn try_from(x: &PeerMessage) -> anyhow::Result<Self> {
        Ok(Self {
            message_type: Some(match x {
                PeerMessage::Handshake(h) => ProtoMT::Handshake(h.try_into().context("Handshake")?),
                PeerMessage::HandshakeFailure(pi, hfr) => {
                    ProtoMT::HandshakeFailure((pi, hfr).try_into().context("HandshakeFailure")?)
                }
                PeerMessage::LastEdge(e) => ProtoMT::LastEdge(proto::LastEdge {
                    edge: Some(e.try_into().context("LastEdge")?),
                }),
                PeerMessage::SyncRoutingTable(rtu) => {
                    ProtoMT::SyncRoutingTable(rtu.try_into().context("SyncRoutingTable")?)
                }
                PeerMessage::RequestUpdateNonce(pei) => {
                    ProtoMT::UpdateNonceRequest(proto::UpdateNonceRequest {
                        partial_edge_info: Some(pei.try_into().context("UpdateNonceRequest")?),
                    })
                }
                PeerMessage::ResponseUpdateNonce(e) => {
                    ProtoMT::UpdateNonceResponse(proto::UpdateNonceResponse {
                        edge: Some(e.try_into().context("UpdateNonceResponse")?),
                    })
                }
                PeerMessage::PeersRequest => ProtoMT::PeersRequest(proto::PeersRequest {}),
                PeerMessage::PeersResponse(pis) => ProtoMT::PeersResponse(proto::PeersResponse {
                    peers: {
                        let mut peers = vec![];
                        for pi in pis {
                            peers.push(pi.try_into().context("PeersResponse")?);
                        }
                        peers
                    },
                }),
                PeerMessage::BlockHeadersRequest(bhs) => {
                    ProtoMT::BlockHeadersRequest(proto::BlockHeadersRequest {
                        block_hashes: {
                            let mut block_hashes = vec![];
                            for bh in bhs {
                                block_hashes.push(bh.try_into().context("BlockHeadersRequest")?);
                            }
                            block_hashes
                        },
                    })
                }
                PeerMessage::BlockHeaders(bhs) => {
                    ProtoMT::BlockHeadersResponse(proto::BlockHeadersResponse {
                        block_headers: {
                            let mut block_headers = vec![];
                            for bh in bhs {
                                block_headers.push(bh.try_into().context("BlockHeadersResponse")?);
                            }
                            block_headers
                        },
                    })
                }
                PeerMessage::BlockRequest(bh) => ProtoMT::BlockRequest(proto::BlockRequest {
                    block_hash: Some(bh.try_into().context("BlockRequest")?),
                }),
                PeerMessage::Block(b) => ProtoMT::BlockResponse(proto::BlockResponse {
                    block: Some(b.try_into().context("BlockResponse")?),
                }),
                PeerMessage::Transaction(t) => ProtoMT::Transaction(proto::SignedTransaction {
                    borsh: t.try_to_vec().context("Transaction")?,
                }),
                PeerMessage::Routed(r) => ProtoMT::Routed(proto::RoutedMessage {
                    borsh: r.try_to_vec().context("Routed")?,
                }),
                PeerMessage::Disconnect => ProtoMT::Disconnect(proto::Disconnect {}),
                PeerMessage::Challenge(r) => ProtoMT::Challenge(proto::Challenge {
                    borsh: r.try_to_vec().context("Challenge")?,
                }),
                PeerMessage::EpochSyncRequest(epoch_id) => {
                    ProtoMT::EpochSyncRequest(proto::EpochSyncRequest {
                        epoch_id: Some((&epoch_id.0).try_into().context("EpochSyncRequest")?),
                    })
                }
                PeerMessage::EpochSyncResponse(esr) => {
                    ProtoMT::EpochSyncResponse(proto::EpochSyncResponse {
                        borsh: esr.try_to_vec().context("EpochSyncResponse")?,
                    })
                }
                PeerMessage::EpochSyncFinalizationRequest(epoch_id) => {
                    ProtoMT::EpochSyncFinalizationRequest(proto::EpochSyncFinalizationRequest {
                        epoch_id: Some(
                            (&epoch_id.0).try_into().context("EpochSyncFinalizationRequest")?,
                        ),
                    })
                }
                PeerMessage::EpochSyncFinalizationResponse(esfr) => {
                    ProtoMT::EpochSyncFinalizationResponse(proto::EpochSyncFinalizationResponse {
                        borsh: esfr.try_to_vec().context("EpochSyncFinalizationResponse")?,
                    })
                }
                PeerMessage::RoutingTableSyncV2(rs) => {
                    ProtoMT::RoutingTableSyncV2(proto::RoutingSyncV2 {
                        borsh: rs.try_to_vec().context("RoutingTableSyncV2")?,
                    })
                }
            }),
        })
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
            ProtoMT::LastEdge(le) => PeerMessage::LastEdge(
                (|| Edge::try_from(le.edge.as_ref().context("missing")?))().context("LastEdge")?,
            ),
            ProtoMT::SyncRoutingTable(rtu) => {
                PeerMessage::SyncRoutingTable(rtu.try_into().context("SyncRoutingTable")?)
            }
            ProtoMT::UpdateNonceRequest(unr) => PeerMessage::RequestUpdateNonce(
                (|| PartialEdgeInfo::try_from(unr.partial_edge_info.as_ref().context("missing")?))(
                )
                .context("UpdateNonceRequest")?,
            ),
            ProtoMT::UpdateNonceResponse(unr) => PeerMessage::ResponseUpdateNonce(
                (|| Edge::try_from(unr.edge.as_ref().context("missing")?))()
                    .context("UpdateNonceResponse")?,
            ),
            ProtoMT::PeersRequest(_) => PeerMessage::PeersRequest,
            ProtoMT::PeersResponse(pr) => PeerMessage::PeersResponse({
                let mut pis = vec![];
                for pi in &pr.peers {
                    pis.push(pi.try_into().context("PeersResponse")?);
                }
                pis
            }),
            ProtoMT::BlockHeadersRequest(bhr) => PeerMessage::BlockHeadersRequest({
                let mut bhs = vec![];
                for bh in &bhr.block_hashes {
                    bhs.push(bh.try_into().context("BlockHeadersRequest")?);
                }
                bhs
            }),
            ProtoMT::BlockHeadersResponse(bhr) => PeerMessage::BlockHeaders({
                let mut bhs = vec![];
                for bh in &bhr.block_headers {
                    bhs.push(bh.try_into().context("BlockHeadersResponse")?);
                }
                bhs
            }),
            ProtoMT::BlockRequest(br) => PeerMessage::BlockRequest(
                (|| CryptoHash::try_from(br.block_hash.as_ref().context("missing")?))()
                    .context("BlockRequest")?,
            ),
            ProtoMT::BlockResponse(br) => PeerMessage::Block(
                (|| Block::try_from(br.block.as_ref().context("missing")?))()
                    .context("BlockResponse")?,
            ),
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
                (|| CryptoHash::try_from(esr.epoch_id.as_ref().context("missing")?))()
                    .context("EpochSyncRequest")?,
            )),
            ProtoMT::EpochSyncResponse(esr) => PeerMessage::EpochSyncResponse(Box::new(
                EpochSyncResponse::try_from_slice(&esr.borsh).context("EpochSyncResponse")?,
            )),
            ProtoMT::EpochSyncFinalizationRequest(esr) => {
                PeerMessage::EpochSyncFinalizationRequest(EpochId(
                    (|| CryptoHash::try_from(esr.epoch_id.as_ref().context("missing")?))()
                        .context("EpochSyncFinalizationRequest")?,
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
