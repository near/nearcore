/// Conversion functions for payloads signable by an account key.
use super::*;

use crate::network_protocol::proto;
use crate::network_protocol::proto::account_key_payload::Payload_type as ProtoPT;
use crate::network_protocol::{
    AccountData, AccountKeySignedPayload, OwnedAccount, SignedAccountData, SignedOwnedAccount,
    VersionedAccountData,
};
use protobuf::{Message as _, MessageField as MF};

#[derive(thiserror::Error, Debug)]
pub enum ParseAccountDataError {
    #[error("bad payload type")]
    BadPayloadType,
    #[error("peer_id: {0}")]
    PeerId(ParseRequiredError<ParsePublicKeyError>),
    #[error("account_key: {0}")]
    AccountKey(ParseRequiredError<ParsePublicKeyError>),
    #[error("peers: {0}")]
    Peers(ParseVecError<ParsePeerAddrError>),
    #[error("timestamp: {0}")]
    Timestamp(ParseRequiredError<ParseTimestampError>),
}

// TODO: consider whether to introduce an intermediate AccountKeyPayload enum.
impl From<&VersionedAccountData> for proto::AccountKeyPayload {
    fn from(x: &VersionedAccountData) -> Self {
        Self {
            payload_type: Some(ProtoPT::AccountData(proto::AccountData {
                peer_id: MF::some((&x.peer_id).into()),
                account_key: MF::some((&x.account_key).into()),
                proxies: x.proxies.iter().map(Into::into).collect(),
                version: x.version,
                timestamp: MF::some(utc_to_proto(&x.timestamp)),
                ..Default::default()
            })),
            ..Self::default()
        }
    }
}

impl TryFrom<&proto::AccountKeyPayload> for VersionedAccountData {
    type Error = ParseAccountDataError;
    fn try_from(x: &proto::AccountKeyPayload) -> Result<Self, Self::Error> {
        let x = match x.payload_type.as_ref().ok_or(Self::Error::BadPayloadType)? {
            ProtoPT::AccountData(a) => a,
            _ => return Err(Self::Error::BadPayloadType),
        };
        Ok(Self {
            data: AccountData {
                peer_id: try_from_required(&x.peer_id).map_err(Self::Error::PeerId)?,
                proxies: try_from_slice(&x.proxies).map_err(Self::Error::Peers)?,
            },
            account_key: try_from_required(&x.account_key).map_err(Self::Error::AccountKey)?,
            version: x.version,
            timestamp: map_from_required(&x.timestamp, utc_from_proto)
                .map_err(Self::Error::Timestamp)?,
        })
    }
}

//////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum ParseSignedAccountDataError {
    #[error("decode: {0}")]
    Decode(protobuf::Error),
    #[error("validator: {0}")]
    AccountData(ParseAccountDataError),
    #[error("signature: {0}")]
    Signature(ParseRequiredError<ParseSignatureError>),
}

impl From<&SignedAccountData> for proto::AccountKeySignedPayload {
    fn from(x: &SignedAccountData) -> Self {
        Self {
            payload: (&x.payload.payload).clone(),
            signature: MF::some((&x.payload.signature).into()),
            ..Self::default()
        }
    }
}

impl TryFrom<&proto::AccountKeySignedPayload> for SignedAccountData {
    type Error = ParseSignedAccountDataError;
    fn try_from(x: &proto::AccountKeySignedPayload) -> Result<Self, Self::Error> {
        let account_data =
            proto::AccountKeyPayload::parse_from_bytes(&x.payload).map_err(Self::Error::Decode)?;
        Ok(Self {
            account_data: (&account_data).try_into().map_err(Self::Error::AccountData)?,
            payload: AccountKeySignedPayload {
                payload: x.payload.clone(),
                signature: try_from_required(&x.signature).map_err(Self::Error::Signature)?,
            },
        })
    }
}

//////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum ParseOwnedAccountError {
    #[error("bad payload type")]
    BadPayloadType,
    #[error("peer_id: {0}")]
    PeerId(ParseRequiredError<ParsePublicKeyError>),
    #[error("account_key: {0}")]
    AccountKey(ParseRequiredError<ParsePublicKeyError>),
    #[error("timestamp: {0}")]
    Timestamp(ParseRequiredError<ParseTimestampError>),
}

impl From<&OwnedAccount> for proto::AccountKeyPayload {
    fn from(x: &OwnedAccount) -> Self {
        Self {
            payload_type: Some(ProtoPT::OwnedAccount(proto::OwnedAccount {
                account_key: MF::some((&x.account_key).into()),
                peer_id: MF::some((&x.peer_id).into()),
                timestamp: MF::some(utc_to_proto(&x.timestamp)),
                ..Default::default()
            })),
            ..Self::default()
        }
    }
}

impl TryFrom<&proto::AccountKeyPayload> for OwnedAccount {
    type Error = ParseOwnedAccountError;
    fn try_from(x: &proto::AccountKeyPayload) -> Result<Self, Self::Error> {
        let x = match x.payload_type.as_ref().ok_or(Self::Error::BadPayloadType)? {
            ProtoPT::OwnedAccount(a) => a,
            _ => return Err(Self::Error::BadPayloadType),
        };
        Ok(Self {
            account_key: try_from_required(&x.account_key).map_err(Self::Error::AccountKey)?,
            peer_id: try_from_required(&x.peer_id).map_err(Self::Error::PeerId)?,
            timestamp: map_from_required(&x.timestamp, utc_from_proto)
                .map_err(Self::Error::Timestamp)?,
        })
    }
}

//////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum ParseSignedOwnedAccountError {
    #[error("decode: {0}")]
    Decode(protobuf::Error),
    #[error("owned_account: {0}")]
    OwnedAccount(ParseOwnedAccountError),
    #[error("signature: {0}")]
    Signature(ParseRequiredError<ParseSignatureError>),
}

impl From<&SignedOwnedAccount> for proto::AccountKeySignedPayload {
    fn from(x: &SignedOwnedAccount) -> Self {
        Self {
            payload: (&x.payload.payload).clone(),
            signature: MF::some((&x.payload.signature).into()),
            ..Self::default()
        }
    }
}

impl TryFrom<&proto::AccountKeySignedPayload> for SignedOwnedAccount {
    type Error = ParseSignedOwnedAccountError;
    fn try_from(x: &proto::AccountKeySignedPayload) -> Result<Self, Self::Error> {
        let owned_account =
            proto::AccountKeyPayload::parse_from_bytes(&x.payload).map_err(Self::Error::Decode)?;
        Ok(Self {
            owned_account: (&owned_account).try_into().map_err(Self::Error::OwnedAccount)?,
            payload: AccountKeySignedPayload {
                payload: x.payload.clone(),
                signature: try_from_required(&x.signature).map_err(Self::Error::Signature)?,
            },
        })
    }
}
