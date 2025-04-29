/// Conversion functions for PeerMessage - the top-level message for the NEAR P2P protocol format.
use super::*;
use crate::network_protocol::proto::peer_message::Message_type as ProtoMT;
use crate::network_protocol::proto::{self};
use crate::network_protocol::state_sync::{SnapshotHostInfo, SyncSnapshotHosts};
use crate::network_protocol::{
    AdvertisedPeerDistance, Disconnect, DistanceVector, PeerMessage, PeersRequest, PeersResponse,
    RoutingTableUpdate, SyncAccountsData,
};
use crate::network_protocol::{RoutedMessage, RoutedMessageV2};
use crate::types::StateResponseInfo;
use borsh::BorshDeserialize as _;
use near_async::time::error::ComponentRange;
use near_primitives::block::{Block, BlockHeader};
use near_primitives::challenge::Challenge;
use near_primitives::optimistic_block::{OptimisticBlock, OptimisticBlockInner};
use near_primitives::transaction::SignedTransaction;
use near_primitives::utils::compression::CompressedData;
use protobuf::MessageField as MF;
use std::sync::Arc;

#[derive(thiserror::Error, Debug)]
pub enum ParseRoutingTableUpdateError {
    #[error("edges {0}")]
    Edges(ParseVecError<ParseEdgeError>),
    #[error("accounts {0}")]
    Accounts(ParseVecError<ParseAnnounceAccountError>),
}

impl From<&RoutingTableUpdate> for proto::RoutingTableUpdate {
    fn from(x: &RoutingTableUpdate) -> Self {
        Self {
            edges: x.edges.iter().map(Into::into).collect(),
            accounts: x.accounts.iter().map(Into::into).collect(),
            ..Default::default()
        }
    }
}

impl TryFrom<&proto::RoutingTableUpdate> for RoutingTableUpdate {
    type Error = ParseRoutingTableUpdateError;
    fn try_from(x: &proto::RoutingTableUpdate) -> Result<Self, Self::Error> {
        Ok(Self {
            edges: try_from_slice(&x.edges).map_err(Self::Error::Edges)?,
            accounts: try_from_slice(&x.accounts).map_err(Self::Error::Accounts)?,
        })
    }
}

//////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum ParseAdvertisedPeerDistanceError {
    #[error("destination {0}")]
    Destination(ParseRequiredError<ParsePublicKeyError>),
}

impl From<&AdvertisedPeerDistance> for proto::AdvertisedPeerDistance {
    fn from(x: &AdvertisedPeerDistance) -> Self {
        Self {
            destination: MF::some((&x.destination).into()),
            distance: x.distance,
            ..Default::default()
        }
    }
}

impl TryFrom<&proto::AdvertisedPeerDistance> for AdvertisedPeerDistance {
    type Error = ParseAdvertisedPeerDistanceError;
    fn try_from(x: &proto::AdvertisedPeerDistance) -> Result<Self, Self::Error> {
        Ok(Self {
            destination: try_from_required(&x.destination).map_err(Self::Error::Destination)?,
            distance: x.distance,
        })
    }
}

//////////////////////////////////////////

//////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum ParseDistanceVectorError {
    #[error("root {0}")]
    Root(ParseRequiredError<ParsePublicKeyError>),
    #[error("distances {0}")]
    Distances(ParseVecError<ParseAdvertisedPeerDistanceError>),
    #[error("edges {0}")]
    Edges(ParseVecError<ParseEdgeError>),
}

impl From<&DistanceVector> for proto::DistanceVector {
    fn from(x: &DistanceVector) -> Self {
        Self {
            root: MF::some((&x.root).into()),
            distances: x.distances.iter().map(Into::into).collect(),
            edges: x.edges.iter().map(Into::into).collect(),
            ..Default::default()
        }
    }
}

impl TryFrom<&proto::DistanceVector> for DistanceVector {
    type Error = ParseDistanceVectorError;
    fn try_from(x: &proto::DistanceVector) -> Result<Self, Self::Error> {
        Ok(Self {
            root: try_from_required(&x.root).map_err(Self::Error::Root)?,
            distances: try_from_slice(&x.distances).map_err(Self::Error::Distances)?,
            edges: try_from_slice(&x.edges).map_err(Self::Error::Edges)?,
        })
    }
}

//////////////////////////////////////////

impl From<&BlockHeader> for proto::BlockHeader {
    fn from(x: &BlockHeader) -> Self {
        Self { borsh: borsh::to_vec(&x).unwrap(), ..Default::default() }
    }
}

pub type ParseBlockHeaderError = std::io::Error;

impl TryFrom<&proto::BlockHeader> for BlockHeader {
    type Error = ParseBlockHeaderError;
    fn try_from(x: &proto::BlockHeader) -> Result<Self, Self::Error> {
        Self::try_from_slice(&x.borsh)
    }
}

//////////////////////////////////////////

impl From<&Block> for proto::Block {
    fn from(x: &Block) -> Self {
        Self { borsh: borsh::to_vec(&x).unwrap(), ..Default::default() }
    }
}

pub type ParseBlockError = std::io::Error;

impl TryFrom<&proto::Block> for Block {
    type Error = ParseBlockError;
    fn try_from(x: &proto::Block) -> Result<Self, Self::Error> {
        Self::try_from_slice(&x.borsh)
    }
}

//////////////////////////////////////////

impl From<&StateResponseInfo> for proto::StateResponseInfo {
    fn from(x: &StateResponseInfo) -> Self {
        Self { borsh: borsh::to_vec(&x).unwrap(), ..Default::default() }
    }
}

pub type ParseStateInfoError = std::io::Error;

impl TryFrom<&proto::StateResponseInfo> for StateResponseInfo {
    type Error = ParseStateInfoError;
    fn try_from(x: &proto::StateResponseInfo) -> Result<Self, Self::Error> {
        Self::try_from_slice(&x.borsh)
    }
}

//////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum ParseSnapshotHostInfoError {
    #[error("peer_id {0}")]
    PeerId(ParseRequiredError<ParsePublicKeyError>),
    #[error("sync_hash {0}")]
    SyncHash(ParseRequiredError<ParseCryptoHashError>),
    #[error("signature {0}")]
    Signature(ParseRequiredError<ParseSignatureError>),
}

impl From<&SnapshotHostInfo> for proto::SnapshotHostInfo {
    fn from(x: &SnapshotHostInfo) -> Self {
        Self {
            peer_id: MF::some((&x.peer_id).into()),
            sync_hash: MF::some((&x.sync_hash).into()),
            epoch_height: x.epoch_height,
            shards: x.shards.clone().into_iter().map(Into::into).collect(),
            signature: MF::some((&x.signature).into()),
            ..Default::default()
        }
    }
}

impl TryFrom<&proto::SnapshotHostInfo> for SnapshotHostInfo {
    type Error = ParseSnapshotHostInfoError;
    fn try_from(x: &proto::SnapshotHostInfo) -> Result<Self, Self::Error> {
        Ok(Self {
            peer_id: try_from_required(&x.peer_id).map_err(Self::Error::PeerId)?,
            sync_hash: try_from_required(&x.sync_hash).map_err(Self::Error::SyncHash)?,
            epoch_height: x.epoch_height,
            shards: x.shards.clone().into_iter().map(Into::into).collect(),
            signature: try_from_required(&x.signature).map_err(Self::Error::Signature)?,
        })
    }
}

//////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum ParseOptimisticBlockError {
    #[error("inner")]
    Inner(std::io::Error),
    #[error("sync_hash {0}")]
    Hash(ParseRequiredError<ParseCryptoHashError>),
    #[error("signature {0}")]
    Signature(ParseRequiredError<ParseSignatureError>),
}

impl From<&OptimisticBlock> for proto::OptimisticBlock {
    fn from(ob: &OptimisticBlock) -> Self {
        Self {
            inner: borsh::to_vec(&ob.inner).unwrap(),
            signature: MF::some((&ob.signature).into()),
            hash: MF::some((&ob.hash).into()),
            ..Default::default()
        }
    }
}

impl TryFrom<&proto::OptimisticBlock> for OptimisticBlock {
    type Error = ParseOptimisticBlockError;
    fn try_from(p_ob: &proto::OptimisticBlock) -> Result<Self, Self::Error> {
        Ok(Self {
            inner: OptimisticBlockInner::try_from_slice(&p_ob.inner).map_err(Self::Error::Inner)?,
            signature: try_from_required(&p_ob.signature).map_err(Self::Error::Signature)?,
            hash: try_from_required(&p_ob.hash).map_err(Self::Error::Hash)?,
        })
    }
}

//////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum ParseSyncSnapshotHostsError {
    #[error("hosts {0}")]
    Hosts(ParseVecError<ParseSnapshotHostInfoError>),
}

impl From<&SyncSnapshotHosts> for proto::SyncSnapshotHosts {
    fn from(x: &SyncSnapshotHosts) -> Self {
        Self { hosts: x.hosts.iter().map(|d| d.as_ref().into()).collect(), ..Default::default() }
    }
}

impl TryFrom<&proto::SyncSnapshotHosts> for SyncSnapshotHosts {
    type Error = ParseSyncSnapshotHostsError;
    fn try_from(x: &proto::SyncSnapshotHosts) -> Result<Self, Self::Error> {
        Ok(Self {
            hosts: try_from_slice(&x.hosts)
                .map_err(Self::Error::Hosts)?
                .into_iter()
                .map(Arc::new)
                .collect(),
        })
    }
}

//////////////////////////////////////////

impl From<&PeerMessage> for proto::PeerMessage {
    fn from(x: &PeerMessage) -> Self {
        Self {
            message_type: Some(match x {
                PeerMessage::Tier1Handshake(h) => ProtoMT::Tier1Handshake(h.into()),
                PeerMessage::Tier2Handshake(h) => ProtoMT::Tier2Handshake(h.into()),
                PeerMessage::Tier3Handshake(h) => ProtoMT::Tier3Handshake(h.into()),
                PeerMessage::HandshakeFailure(pi, hfr) => {
                    ProtoMT::HandshakeFailure((pi, hfr).into())
                }
                PeerMessage::LastEdge(e) => ProtoMT::LastEdge(proto::LastEdge {
                    edge: MF::some(e.into()),
                    ..Default::default()
                }),
                PeerMessage::SyncRoutingTable(rtu) => ProtoMT::SyncRoutingTable(rtu.into()),
                PeerMessage::DistanceVector(spt) => ProtoMT::DistanceVector(spt.into()),
                PeerMessage::RequestUpdateNonce(pei) => {
                    ProtoMT::UpdateNonceRequest(proto::UpdateNonceRequest {
                        partial_edge_info: MF::some(pei.into()),
                        ..Default::default()
                    })
                }
                PeerMessage::SyncAccountsData(msg) => {
                    ProtoMT::SyncAccountsData(proto::SyncAccountsData {
                        accounts_data: msg
                            .accounts_data
                            .iter()
                            .map(|d| d.as_ref().into())
                            .collect(),
                        incremental: msg.incremental,
                        requesting_full_sync: msg.requesting_full_sync,
                        ..Default::default()
                    })
                }
                PeerMessage::PeersRequest(pr) => ProtoMT::PeersRequest(proto::PeersRequest {
                    max_peers: pr.max_peers,
                    max_direct_peers: pr.max_direct_peers,
                    ..Default::default()
                }),
                PeerMessage::PeersResponse(pr) => ProtoMT::PeersResponse(proto::PeersResponse {
                    peers: pr.peers.iter().map(Into::into).collect(),
                    direct_peers: pr.direct_peers.iter().map(Into::into).collect(),
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
                PeerMessage::OptimisticBlock(ob) => ProtoMT::OptimisticBlock(ob.into()),
                PeerMessage::Transaction(t) => ProtoMT::Transaction(proto::SignedTransaction {
                    borsh: borsh::to_vec(&t).unwrap(),
                    ..Default::default()
                }),
                PeerMessage::Routed(r) => ProtoMT::Routed(proto::RoutedMessage {
                    borsh: borsh::to_vec(&r.msg).unwrap(),
                    created_at: MF::from_option(r.created_at.as_ref().map(utc_to_proto)),
                    num_hops: r.num_hops,
                    ..Default::default()
                }),
                PeerMessage::Disconnect(r) => ProtoMT::Disconnect(proto::Disconnect {
                    remove_from_connection_store: r.remove_from_connection_store,
                    ..Default::default()
                }),
                PeerMessage::Challenge(r) => ProtoMT::Challenge(proto::Challenge {
                    borsh: borsh::to_vec(&r).unwrap(),
                    ..Default::default()
                }),
                PeerMessage::SyncSnapshotHosts(ssh) => ProtoMT::SyncSnapshotHosts(ssh.into()),
                PeerMessage::StateRequestHeader(shard_id, sync_hash) => {
                    ProtoMT::StateRequestHeader(proto::StateRequestHeader {
                        shard_id: (*shard_id).into(),
                        sync_hash: MF::some(sync_hash.into()),
                        ..Default::default()
                    })
                }
                PeerMessage::StateRequestPart(shard_id, sync_hash, part_id) => {
                    ProtoMT::StateRequestPart(proto::StateRequestPart {
                        shard_id: (*shard_id).into(),
                        sync_hash: MF::some(sync_hash.into()),
                        part_id: *part_id,
                        ..Default::default()
                    })
                }
                PeerMessage::VersionedStateResponse(sri) => {
                    ProtoMT::StateResponse(proto::StateResponse {
                        state_response_info: MF::some(sri.into()),
                        ..Default::default()
                    })
                }
                PeerMessage::EpochSyncRequest => {
                    ProtoMT::EpochSyncRequest(proto::EpochSyncRequest { ..Default::default() })
                }
                PeerMessage::EpochSyncResponse(esp) => {
                    ProtoMT::EpochSyncResponse(proto::EpochSyncResponse {
                        compressed_proof: esp.as_slice().to_vec(),
                        ..Default::default()
                    })
                }
            }),
            ..Default::default()
        }
    }
}

pub type ParsePeersRequestError = std::io::Error;
pub type ParseTransactionError = std::io::Error;
pub type ParseRoutedError = std::io::Error;

#[derive(thiserror::Error, Debug)]
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
    #[error("shortest_path_tree: {0}")]
    DistanceVector(ParseDistanceVectorError),
    #[error("update_nonce_request: {0}")]
    UpdateNonceRequest(ParseRequiredError<ParsePartialEdgeInfoError>),
    #[error("update_nonce_response: {0}")]
    UpdateNonceResponse(ParseRequiredError<ParseEdgeError>),
    #[error("peers_request: {0}")]
    PeersRequest(ParsePeersRequestError),
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
    Challenge(std::io::Error),
    #[error("routed_created_at: {0}")]
    RoutedCreatedAtTimestamp(ComponentRange),
    #[error("sync_accounts_data: {0}")]
    SyncAccountsData(ParseVecError<ParseSignedAccountDataError>),
    #[error("state_response: {0}")]
    StateResponse(ParseRequiredError<ParseStateInfoError>),
    #[error("sync_snapshot_hosts: {0}")]
    SyncSnapshotHosts(ParseSyncSnapshotHostsError),
    #[error("optimistic_block: {0}")]
    OptimisticBlock(ParseOptimisticBlockError),
}

impl TryFrom<&proto::PeerMessage> for PeerMessage {
    type Error = ParsePeerMessageError;
    fn try_from(x: &proto::PeerMessage) -> Result<Self, Self::Error> {
        Ok(match x.message_type.as_ref().ok_or(Self::Error::Empty)? {
            ProtoMT::Tier1Handshake(h) => {
                PeerMessage::Tier1Handshake(h.try_into().map_err(Self::Error::Handshake)?)
            }
            ProtoMT::Tier2Handshake(h) => {
                PeerMessage::Tier2Handshake(h.try_into().map_err(Self::Error::Handshake)?)
            }
            ProtoMT::Tier3Handshake(h) => {
                PeerMessage::Tier3Handshake(h.try_into().map_err(Self::Error::Handshake)?)
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
            ProtoMT::DistanceVector(spt) => {
                PeerMessage::DistanceVector(spt.try_into().map_err(Self::Error::DistanceVector)?)
            }
            ProtoMT::UpdateNonceRequest(unr) => PeerMessage::RequestUpdateNonce(
                try_from_required(&unr.partial_edge_info)
                    .map_err(Self::Error::UpdateNonceRequest)?,
            ),
            ProtoMT::UpdateNonceResponse(unr) => {
                PeerMessage::SyncRoutingTable(RoutingTableUpdate {
                    edges: vec![
                        try_from_required(&unr.edge).map_err(Self::Error::UpdateNonceResponse)?,
                    ],
                    accounts: vec![],
                })
            }
            ProtoMT::SyncAccountsData(msg) => PeerMessage::SyncAccountsData(SyncAccountsData {
                accounts_data: try_from_slice(&msg.accounts_data)
                    .map_err(Self::Error::SyncAccountsData)?
                    .into_iter()
                    .map(Arc::new)
                    .collect(),
                incremental: msg.incremental,
                requesting_full_sync: msg.requesting_full_sync,
            }),
            ProtoMT::PeersRequest(pr) => PeerMessage::PeersRequest(PeersRequest {
                max_peers: pr.max_peers,
                max_direct_peers: pr.max_direct_peers,
            }),
            ProtoMT::PeersResponse(pr) => PeerMessage::PeersResponse(PeersResponse {
                peers: try_from_slice(&pr.peers).map_err(Self::Error::PeersResponse)?,
                direct_peers: try_from_slice(&pr.direct_peers)
                    .map_err(Self::Error::PeersResponse)?,
            }),
            ProtoMT::BlockHeadersRequest(bhr) => PeerMessage::BlockHeadersRequest(
                try_from_slice(&bhr.block_hashes).map_err(Self::Error::BlockHeadersRequest)?,
            ),
            ProtoMT::BlockHeadersResponse(bhr) => PeerMessage::BlockHeaders(
                try_from_slice(&bhr.block_headers).map_err(Self::Error::BlockHeadersResponse)?,
            ),
            ProtoMT::BlockRequest(br) => PeerMessage::BlockRequest(
                try_from_required(&br.block_hash).map_err(Self::Error::BlockRequest)?,
            ),
            ProtoMT::BlockResponse(br) => PeerMessage::Block(
                try_from_required(&br.block).map_err(Self::Error::BlockResponse)?,
            ),
            ProtoMT::OptimisticBlock(ob) => {
                PeerMessage::OptimisticBlock(ob.try_into().map_err(Self::Error::OptimisticBlock)?)
            }
            ProtoMT::Transaction(t) => PeerMessage::Transaction(
                SignedTransaction::try_from_slice(&t.borsh).map_err(Self::Error::Transaction)?,
            ),
            ProtoMT::Routed(r) => PeerMessage::Routed(Box::new(RoutedMessageV2 {
                msg: RoutedMessage::try_from_slice(&r.borsh).map_err(Self::Error::Routed)?,
                created_at: r
                    .created_at
                    .as_ref()
                    .map(utc_from_proto)
                    .transpose()
                    .map_err(Self::Error::RoutedCreatedAtTimestamp)?,
                num_hops: r.num_hops,
            })),
            ProtoMT::Disconnect(d) => PeerMessage::Disconnect(Disconnect {
                remove_from_connection_store: d.remove_from_connection_store,
            }),
            ProtoMT::Challenge(c) => PeerMessage::Challenge(Box::new(
                Challenge::try_from_slice(&c.borsh).map_err(Self::Error::Challenge)?,
            )),
            ProtoMT::StateRequestHeader(srh) => PeerMessage::StateRequestHeader(
                srh.shard_id.into(),
                try_from_required(&srh.sync_hash).map_err(Self::Error::BlockRequest)?,
            ),
            ProtoMT::StateRequestPart(srp) => PeerMessage::StateRequestPart(
                srp.shard_id.into(),
                try_from_required(&srp.sync_hash).map_err(Self::Error::BlockRequest)?,
                srp.part_id,
            ),
            ProtoMT::StateResponse(t) => PeerMessage::VersionedStateResponse(
                try_from_required(&t.state_response_info).map_err(Self::Error::StateResponse)?,
            ),
            ProtoMT::SyncSnapshotHosts(srh) => PeerMessage::SyncSnapshotHosts(
                srh.try_into().map_err(Self::Error::SyncSnapshotHosts)?,
            ),
            ProtoMT::EpochSyncRequest(_) => PeerMessage::EpochSyncRequest,
            ProtoMT::EpochSyncResponse(esr) => PeerMessage::EpochSyncResponse(
                CompressedData::from_boxed_slice(esr.compressed_proof.clone().into_boxed_slice()),
            ),
        })
    }
}
