/// Conversion functions for payloads signable by an account key.
use super::*;

use crate::network_protocol::proto;
use crate::network_protocol::proto::account_key_payload::Payload_type as ProtoPT;
use near_primitives::account::id::{ParseAccountError};
use crate::network_protocol::{
    AccountKeySignedPayload, SignedValidator, Validator,
};
use near_primitives::types::EpochId;
use protobuf::{Message as _, MessageField as MF};

#[derive(thiserror::Error,Debug)]
pub enum ParseValidatorError {
    #[error("bad payload type")]
    BadPayloadType,
    #[error("account_id: {0}")]
    AccountId(ParseAccountError),
    #[error("peers: {0}")]
    Peers(ParseVecError<ParsePeerAddrError>),
    #[error("epoch_id: {0}")]
    EpochId(ParseRequiredError<ParseCryptoHashError>),
    #[error("timestamp: {0}")]
    Timestamp(ParseRequiredError<ParseTimestampError>),
}

// TODO: currently a direct conversion Validator <-> proto::AccountKeyPayload is implemented.
// When more variants are available, consider whether to introduce an intermediate
// AccountKeyPayload enum.
impl From<&Validator> for proto::AccountKeyPayload {
    fn from(x:&Validator) -> Self {
        Self {
            payload_type: Some(ProtoPT::Validator(proto::Validator{
                account_id: x.account_id.to_string(),
                peers: x.peers.iter().map(Into::into).collect(), 
                epoch_id: MF::some((&x.epoch_id.0).into()),
                timestamp: MF::some(utc_to_proto(&x.timestamp)),
                ..Default::default()
            })),
            ..Self::default()
        }
    }
}

impl TryFrom<&proto::AccountKeyPayload> for Validator {
    type Error = ParseValidatorError;
    fn try_from(x:&proto::AccountKeyPayload) -> Result<Self,Self::Error> {
        let x = match x.payload_type.as_ref().ok_or(Self::Error::BadPayloadType)? {
            ProtoPT::Validator(v) => v,
            #[allow(unreachable_patterns)]
            _ => { return Err(Self::Error::BadPayloadType) },
        };
        Ok(Self{
            account_id: x.account_id.clone().try_into().map_err(Self::Error::AccountId)?,
            peers: try_from_vec(&x.peers).map_err(Self::Error::Peers)?,
            epoch_id: EpochId(try_from_required(&x.epoch_id).map_err(Self::Error::EpochId)?),
            timestamp: map_from_required(&x.timestamp,utc_from_proto).map_err(Self::Error::Timestamp)?,
        })
    }
}

//////////////////////////////////////////

// TODO: I took this number out of thin air,
// determine a reasonable limit later.
const VALIDATOR_PAYLOAD_MAX_BYTES : usize = 10000;

#[derive(thiserror::Error,Debug)]
pub enum ParseSignedValidatorError {
    #[error("payload too large: {0}B")]
    PayloadTooLarge(usize),
    #[error("decode: {0}")]
    Decode(protobuf::Error),
    #[error("validator: {0}")]
    Validator(ParseValidatorError),
    #[error("signature: {0}")]
    Signature(ParseRequiredError<ParseSignatureError>),
}

impl From<&SignedValidator> for proto::AccountKeySignedPayload {
    fn from(x:&SignedValidator) -> Self {
        Self{
            payload: (&x.payload.payload).clone(),
            signature: MF::some((&x.payload.signature).into()),
            ..Self::default()
        }
    }
}

impl TryFrom<&proto::AccountKeySignedPayload> for SignedValidator {
    type Error = ParseSignedValidatorError;
    fn try_from(x:&proto::AccountKeySignedPayload) -> Result<Self,Self::Error> {
        // We definitely should tolerate unknown fields, so that we can do
        // backward compatible changes. We also need to limit the total
        // size of the payload, to prevent large message attacks.
        // TODO: is this the right place to do this check? Should we do the same while encoding?
        // An alternative would be to do this check in the business logic of PeerManagerActor,
        // probably together with signature validation. The amount of memory a node
        // maintains per vaidator should be bounded.
        if x.payload.len() > VALIDATOR_PAYLOAD_MAX_BYTES {
            return Err(Self::Error::PayloadTooLarge(x.payload.len()));
        }
        let validator = proto::AccountKeyPayload::parse_from_bytes(&x.payload).map_err(Self::Error::Decode)?;
        Ok(Self {
            validator: (&validator).try_into().map_err(Self::Error::Validator)?,
            payload: AccountKeySignedPayload {
                payload: x.payload.clone(),
                signature: try_from_required(&x.signature).map_err(Self::Error::Signature)?,
            },
        })
    }
}
