use std::collections::HashSet;
use std::fmt::Debug;
use std::hash::Hash;

use crate::hash::CryptoHash;

use super::aggregate_signature;
use super::types;
pub use super::serialize::{Encode, Decode};

/// Trait to abstract the way signing happens.
/// Can be used to not keep private key in the given binary via cross-process communication.
pub trait Signer: Sync + Send {
    fn public_key(&self) -> aggregate_signature::BlsPublicKey;
    fn sign(&self, hash: &CryptoHash) -> types::PartialSignature;
    fn account_id(&self) -> types::AccountId;
}

pub trait WitnessSelector: 'static {
    fn epoch_witnesses(&self, epoch: u64) -> &HashSet<types::UID>;
    fn epoch_leader(&self, epoch: u64) -> types::UID;
    /// Random sample of witnesses. Should exclude the current witness.
    fn random_witnesses(&self, epoch: u64, sample_size: usize) -> HashSet<types::UID>;
}

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

pub trait TxFlow<P: Payload> {
    /// Tells TxFlow to process the given TxFlow message received from another peer. TxFlow signals
    /// when the message is processed.
    fn process_message(
        &mut self,
        message: types::SignedMessageData<P>,
        callback: &Fn() -> GenericResult,
    ) -> GenericResult;
    /// Tells TxFlow to process a payload, e.g. for in-shard TxFlow it is a transaction received
    /// from a client. TxFlow signals when the payload is accepted.
    fn process_payload(&mut self, payload: P, callback: &Fn() -> GenericResult) -> GenericResult;
    /// Subscribes to the messages produced by TxFlow. These messages indicate the receiver that
    /// they have to be relayed to.
    fn subscribe_to_messages(
        &mut self,
        subscriber: &Fn(types::UID, &types::SignedMessageData<P>) -> GenericResult,
    ) -> GenericResult;
    /// Subscribes to the consensus blocks produced by TxFlow. The consensus blocks contain messages
    /// with payload + some content specific to whether TxFlow is used on the Beacon Chain or in
    /// the shard.
    fn subscribe_to_consensus_blocks(
        &mut self,
        subscriber: &Fn(types::ConsensusBlockBody<P>),
    ) -> GenericResult;
}

/// FromBytes is like TryFrom<Vec<u8>>
pub trait FromBytes: Sized {
    fn from_bytes(bytes: &Vec<u8>) -> Result<Self, Box<std::error::Error>>;
}

/// ToBytes is like Into<Vec<u8>>, but doesn't consume self
pub trait ToBytes: Sized {
    fn to_bytes(&self) -> Vec<u8>;
}

pub trait Base58Encoded : FromBytes + ToBytes {
    fn from_base58(s: &String) -> Result<Self, Box<std::error::Error>> {
        let bytes = bs58::decode(s).into_vec()?;
        Self::from_bytes(&bytes)
    }

    fn to_base58(&self) -> String {
        let bytes = self.to_bytes();
        bs58::encode(bytes).into_string()
    }
}
