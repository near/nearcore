/// Conversion functions for `Handshake` messages.
use super::*;

use crate::network_protocol::proto;
use crate::network_protocol::{Handshake, HandshakeFailureReason};
use crate::network_protocol::{PeerChainInfoV2, PeerInfo};
use near_primitives::block::GenesisId;
use protobuf::MessageField as MF;

impl From<&GenesisId> for proto::GenesisId {
    fn from(x: &GenesisId) -> Self {
        Self { chain_id: x.chain_id.clone(), hash: MF::some((&x.hash).into()), ..Self::default() }
    }
}

#[derive(thiserror::Error, Debug)]
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

#[derive(thiserror::Error, Debug)]
pub enum ParsePeerChainInfoV2Error {
    #[error("genesis_id {0}")]
    GenesisId(ParseRequiredError<ParseGenesisIdError>),
}

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

#[derive(thiserror::Error, Debug)]
pub enum ParseHandshakeError {
    #[error("sender_peer_id {0}")]
    SenderPeerId(ParseRequiredError<ParsePublicKeyError>),
    #[error("target_peer_id {0}")]
    TargetPeerId(ParseRequiredError<ParsePublicKeyError>),
    #[error("sender_listen_port {0}")]
    SenderListenPort(std::num::TryFromIntError),
    #[error("sender_chain_info {0}")]
    SenderChainInfo(ParseRequiredError<ParsePeerChainInfoV2Error>),
    #[error("partial_edge_info {0}")]
    PartialEdgeInfo(ParseRequiredError<ParsePartialEdgeInfoError>),
    #[error("owned_account {0}")]
    OwnedAccount(ParseSignedOwnedAccountError),
}

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
            owned_account: x.owned_account.as_ref().map(Into::into).into(),
            ..Self::default()
        }
    }
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
            owned_account: try_from_optional(&p.owned_account)
                .map_err(Self::Error::OwnedAccount)?,
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
                ..Self::default()
            },
            HandshakeFailureReason::GenesisMismatch(genesis_id) => Self {
                peer_info: MF::some(pi.into()),
                reason: proto::handshake_failure::Reason::GenesisMismatch.into(),
                genesis_id: MF::some(genesis_id.into()),
                ..Self::default()
            },
            HandshakeFailureReason::InvalidTarget => Self {
                peer_info: MF::some(pi.into()),
                reason: proto::handshake_failure::Reason::InvalidTarget.into(),
                ..Self::default()
            },
        }
    }
}

#[derive(thiserror::Error, Debug)]
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
