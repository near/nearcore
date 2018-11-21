use serde::{de::DeserializeOwned, Serialize};
use std::collections::HashSet;
use std::fmt::Debug;
use std::hash::Hash;

use super::types;
use hash::CryptoHash;

// encode a type to byte array
pub trait Encode {
    fn encode(&self) -> Option<Vec<u8>>;
}

// decode from byte array
pub trait Decode: Sized {
    fn decode(data: &[u8]) -> Option<Self>;
}

impl<T> Encode for T
where
    T: Serialize,
{
    fn encode(&self) -> Option<Vec<u8>> {
        bincode::serialize(&self).ok()
    }
}

impl<T> Decode for T
where
    T: DeserializeOwned,
{
    fn decode(data: &[u8]) -> Option<Self> {
        bincode::deserialize(data).ok()
    }
}

/// trait that abstracts ``Header"
pub trait Header:
    Debug + Clone + Send + Sync + Serialize + DeserializeOwned + Eq + 'static
{
    // TODO: add methods
    fn hash(&self) -> CryptoHash;

    // get block index
    fn number(&self) -> u64;
}

/// trait that abstracts ``block", ideally could be used for both beacon-chain blocks
/// and shard-chain blocks
pub trait Block: Debug + Clone + Send + Sync + Serialize + DeserializeOwned + Eq + 'static {
    type Header: Header;
    type Body;

    fn header(&self) -> &Self::Header;
    fn body(&self) -> &Self::Body;
    fn deconstruct(self) -> (Self::Header, Self::Body);
    fn new(header: Self::Header, body: Self::Body) -> Self;
    fn hash(&self) -> CryptoHash;
}

pub trait Verifier {
    fn compute_state(&mut self, transactions: &[types::SignedTransaction]) -> types::State;
}

pub trait WitnessSelector {
    fn epoch_witnesses(&self, epoch: u64) -> &HashSet<types::UID>;
    fn epoch_leader(&self, epoch: u64) -> types::UID;
}

pub type GenericResult = Result<(), &'static str>;

pub trait Payload: Hash {
    fn verify(&self) -> GenericResult;
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
    fn subscribe_to_consensus_blocks<C>(
        &mut self,
        subscriber: &Fn(types::ConsensusBlockBody<P, C>),
    ) -> GenericResult;
}
