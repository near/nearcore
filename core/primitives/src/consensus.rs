use std::fmt::Debug;
use std::hash::Hash;

pub use super::serialize::{Decode, Encode};
use crate::hash::CryptoHash;
use crate::types::AuthorityId;
use crate::types::PartialSignature;
use crate::traits::ToBytes;
use crate::traits::FromBytes;

pub type GenericResult = Result<(), &'static str>;

/// General payload that can be stored on TxFlow. Should either not have references,
/// or the references should live for static lifetime.
pub trait Payload: Clone + Send + Hash + Debug + Encode + Decode + 'static {
    fn verify(&self) -> GenericResult;
    // Merge content from another payload into this one.
    fn union_update(&mut self, other: Self);
    fn is_empty(&self) -> bool;
    // Creates empty payload.
    fn new() -> Self;
}

/// Partial BLS for the beacon and shard blocks.
#[derive(PartialEq, Debug)]
pub enum JointBlockBLS{
    Request {
        sender_id: AuthorityId,
        receiver_id: AuthorityId,
        beacon_hash: CryptoHash,
        shard_hash: CryptoHash,
    },
    General {
        sender_id: AuthorityId,
        receiver_id: AuthorityId,
        beacon_hash: CryptoHash,
        shard_hash: CryptoHash,
        beacon_sig: PartialSignature,
        shard_sig: PartialSignature,
    },
}

#[derive(PartialEq, Debug, Serialize, Deserialize)]
pub enum JointBlockBLSEncoded {
    Request {
        sender_id: AuthorityId,
        receiver_id: AuthorityId,
        beacon_hash: CryptoHash,
        shard_hash: CryptoHash,
    },
    General {
        sender_id: AuthorityId,
        receiver_id: AuthorityId,
        beacon_hash: CryptoHash,
        shard_hash: CryptoHash,
        beacon_sig: Vec<u8>,
        shard_sig: Vec<u8>,
    },
}


// TODO: Remove it once PartialSignature implements Serialize/Deserialize.
impl From<JointBlockBLSEncoded> for JointBlockBLS {
    fn from(other: JointBlockBLSEncoded) -> Self {
        match other {
            JointBlockBLSEncoded::Request {
                sender_id,
                receiver_id,
                beacon_hash,
                shard_hash,
            } => JointBlockBLS::Request { sender_id, receiver_id, beacon_hash, shard_hash },
            JointBlockBLSEncoded::General {
                sender_id,
                receiver_id,
                beacon_hash,
                shard_hash,
                beacon_sig,
                shard_sig,
            } => {
                let beacon_sig = PartialSignature::from_bytes(&beacon_sig).unwrap();
                let shard_sig = PartialSignature::from_bytes(&shard_sig).unwrap();
                JointBlockBLS::General {
                    sender_id,
                    receiver_id,
                    beacon_hash,
                    shard_hash,
                    beacon_sig,
                    shard_sig,
                }
            }
        }
    }
}

impl From<JointBlockBLS> for JointBlockBLSEncoded {
    fn from(other: JointBlockBLS) -> Self {
        match other {
            JointBlockBLS::Request {
                sender_id,
                receiver_id,
                beacon_hash,
                shard_hash,
            } => JointBlockBLSEncoded::Request { sender_id, receiver_id, beacon_hash, shard_hash },
            JointBlockBLS::General {
                sender_id,
                receiver_id,
                beacon_hash,
                shard_hash,
                beacon_sig,
                shard_sig,
            } => {
                let beacon_sig = beacon_sig.to_bytes();
                let shard_sig = shard_sig.to_bytes();
                JointBlockBLSEncoded::General {
                    sender_id,
                    receiver_id,
                    beacon_hash,
                    shard_hash,
                    beacon_sig,
                    shard_sig,
                }
            }
        }
    }
}
