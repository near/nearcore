use std::fmt::Debug;
use std::hash::Hash;

pub use crate::serialize::{Decode, Encode};
use crate::hash::CryptoHash;
use crate::crypto::signature::bs58_serializer;
use crate::types::{AuthorityId, PartialSignature};

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
#[derive(PartialEq, Debug, Serialize, Deserialize, Clone)]
#[allow(clippy::large_enum_variant)]
pub enum JointBlockBLS {
    Request {
        sender_id: AuthorityId,
        receiver_id: AuthorityId,
        // TODO: consider replacing beacon_hash / shard_hash with block_index.
        beacon_hash: CryptoHash,
        shard_hash: CryptoHash,
    },
    General {
        sender_id: AuthorityId,
        receiver_id: AuthorityId,
        beacon_hash: CryptoHash,
        shard_hash: CryptoHash,
        #[serde(with = "bs58_serializer")]
        beacon_sig: PartialSignature,
        #[serde(with = "bs58_serializer")]
        shard_sig: PartialSignature,
    },
}
