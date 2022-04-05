use crate::network_protocol as mem;
/// Contains borsh <-> network_protocol conversions.
use crate::network_protocol::borsh as net;
use anyhow::bail;

impl TryFrom<&net::Handshake> for mem::Handshake {
    type Error = anyhow::Error;
    fn try_from(x: &net::Handshake) -> anyhow::Result<Self> {
        Ok(Self {
            protocol_version: x.protocol_version,
            oldest_supported_version: x.oldest_supported_version,
            sender_peer_id: x.sender_peer_id.clone(),
            target_peer_id: x.target_peer_id.clone(),
            sender_listen_port: x.sender_listen_port,
            sender_chain_info: x.sender_chain_info.clone(),
            partial_edge_info: x.partial_edge_info.clone(),
        })
    }
}

impl TryFrom<&mem::Handshake> for net::Handshake {
    type Error = anyhow::Error;
    fn try_from(x: &mem::Handshake) -> anyhow::Result<Self> {
        Ok(Self {
            protocol_version: x.protocol_version,
            oldest_supported_version: x.oldest_supported_version,
            sender_peer_id: x.sender_peer_id.clone(),
            target_peer_id: x.target_peer_id.clone(),
            sender_listen_port: x.sender_listen_port,
            sender_chain_info: x.sender_chain_info.clone(),
            partial_edge_info: x.partial_edge_info.clone(),
        })
    }
}

//////////////////////////////////////////

impl TryFrom<&net::HandshakeFailureReason> for mem::HandshakeFailureReason {
    type Error = anyhow::Error;
    fn try_from(x: &net::HandshakeFailureReason) -> anyhow::Result<Self> {
        Ok(match x {
            net::HandshakeFailureReason::ProtocolVersionMismatch {
                version,
                oldest_supported_version,
            } => mem::HandshakeFailureReason::ProtocolVersionMismatch {
                version: *version,
                oldest_supported_version: *oldest_supported_version,
            },
            net::HandshakeFailureReason::GenesisMismatch(genesis_id) => {
                mem::HandshakeFailureReason::GenesisMismatch(genesis_id.clone())
            }
            net::HandshakeFailureReason::InvalidTarget => {
                mem::HandshakeFailureReason::InvalidTarget
            }
        })
    }
}

impl TryFrom<&mem::HandshakeFailureReason> for net::HandshakeFailureReason {
    type Error = anyhow::Error;
    fn try_from(x: &mem::HandshakeFailureReason) -> anyhow::Result<Self> {
        Ok(match x {
            mem::HandshakeFailureReason::ProtocolVersionMismatch {
                version,
                oldest_supported_version,
            } => net::HandshakeFailureReason::ProtocolVersionMismatch {
                version: *version,
                oldest_supported_version: *oldest_supported_version,
            },
            mem::HandshakeFailureReason::GenesisMismatch(genesis_id) => {
                net::HandshakeFailureReason::GenesisMismatch(genesis_id.clone())
            }
            mem::HandshakeFailureReason::InvalidTarget => {
                net::HandshakeFailureReason::InvalidTarget
            }
        })
    }
}

//////////////////////////////////////////

impl TryFrom<&net::PeerMessage> for mem::PeerMessage {
    type Error = anyhow::Error;
    fn try_from(x: &net::PeerMessage) -> anyhow::Result<Self> {
        Ok(match x.clone() {
            net::PeerMessage::Handshake(h) => mem::PeerMessage::Handshake((&h).try_into()?),
            net::PeerMessage::HandshakeFailure(pi, hfr) => {
                mem::PeerMessage::HandshakeFailure(pi, (&hfr).try_into()?)
            }
            net::PeerMessage::LastEdge(e) => mem::PeerMessage::LastEdge(e),
            net::PeerMessage::SyncRoutingTable(rtu) => mem::PeerMessage::SyncRoutingTable(rtu),
            net::PeerMessage::RequestUpdateNonce(e) => mem::PeerMessage::RequestUpdateNonce(e),
            net::PeerMessage::ResponseUpdateNonce(e) => mem::PeerMessage::ResponseUpdateNonce(e),
            net::PeerMessage::PeersRequest => mem::PeerMessage::PeersRequest,
            net::PeerMessage::PeersResponse(pis) => mem::PeerMessage::PeersResponse(pis),
            net::PeerMessage::BlockHeadersRequest(bhs) => {
                mem::PeerMessage::BlockHeadersRequest(bhs)
            }
            net::PeerMessage::BlockHeaders(bhs) => mem::PeerMessage::BlockHeaders(bhs),
            net::PeerMessage::BlockRequest(bh) => mem::PeerMessage::BlockRequest(bh),
            net::PeerMessage::Block(b) => mem::PeerMessage::Block(b),
            net::PeerMessage::Transaction(t) => mem::PeerMessage::Transaction(t),
            net::PeerMessage::Routed(r) => mem::PeerMessage::Routed(r),
            net::PeerMessage::Disconnect => mem::PeerMessage::Disconnect,
            net::PeerMessage::Challenge(c) => mem::PeerMessage::Challenge(c),
            net::PeerMessage::_HandshakeV2 => bail!("HandshakeV2 is deprecated"),
            net::PeerMessage::EpochSyncRequest(epoch_id) => {
                mem::PeerMessage::EpochSyncRequest(epoch_id)
            }
            net::PeerMessage::EpochSyncResponse(esr) => mem::PeerMessage::EpochSyncResponse(esr),
            net::PeerMessage::EpochSyncFinalizationRequest(epoch_id) => {
                mem::PeerMessage::EpochSyncFinalizationRequest(epoch_id)
            }
            net::PeerMessage::EpochSyncFinalizationResponse(esfr) => {
                mem::PeerMessage::EpochSyncFinalizationResponse(esfr)
            }
            net::PeerMessage::RoutingTableSyncV2(rs) => mem::PeerMessage::RoutingTableSyncV2(rs),
        })
    }
}

impl TryFrom<&mem::PeerMessage> for net::PeerMessage {
    type Error = anyhow::Error;
    fn try_from(x: &mem::PeerMessage) -> anyhow::Result<Self> {
        Ok(match x.clone() {
            mem::PeerMessage::Handshake(h) => net::PeerMessage::Handshake((&h).try_into()?),
            mem::PeerMessage::HandshakeFailure(pi, hfr) => {
                net::PeerMessage::HandshakeFailure(pi, (&hfr).try_into()?)
            }
            mem::PeerMessage::LastEdge(e) => net::PeerMessage::LastEdge(e),
            mem::PeerMessage::SyncRoutingTable(rtu) => net::PeerMessage::SyncRoutingTable(rtu),
            mem::PeerMessage::RequestUpdateNonce(e) => net::PeerMessage::RequestUpdateNonce(e),
            mem::PeerMessage::ResponseUpdateNonce(e) => net::PeerMessage::ResponseUpdateNonce(e),
            mem::PeerMessage::PeersRequest => net::PeerMessage::PeersRequest,
            mem::PeerMessage::PeersResponse(pis) => net::PeerMessage::PeersResponse(pis),
            mem::PeerMessage::BlockHeadersRequest(bhs) => {
                net::PeerMessage::BlockHeadersRequest(bhs)
            }
            mem::PeerMessage::BlockHeaders(bhs) => net::PeerMessage::BlockHeaders(bhs),
            mem::PeerMessage::BlockRequest(bh) => net::PeerMessage::BlockRequest(bh),
            mem::PeerMessage::Block(b) => net::PeerMessage::Block(b),
            mem::PeerMessage::Transaction(t) => net::PeerMessage::Transaction(t),
            mem::PeerMessage::Routed(r) => net::PeerMessage::Routed(r),
            mem::PeerMessage::Disconnect => net::PeerMessage::Disconnect,
            mem::PeerMessage::Challenge(c) => net::PeerMessage::Challenge(c),
            mem::PeerMessage::EpochSyncRequest(epoch_id) => {
                net::PeerMessage::EpochSyncRequest(epoch_id)
            }
            mem::PeerMessage::EpochSyncResponse(esr) => net::PeerMessage::EpochSyncResponse(esr),
            mem::PeerMessage::EpochSyncFinalizationRequest(epoch_id) => {
                net::PeerMessage::EpochSyncFinalizationRequest(epoch_id)
            }
            mem::PeerMessage::EpochSyncFinalizationResponse(esfr) => {
                net::PeerMessage::EpochSyncFinalizationResponse(esfr)
            }
            mem::PeerMessage::RoutingTableSyncV2(rs) => net::PeerMessage::RoutingTableSyncV2(rs),
        })
    }
}
