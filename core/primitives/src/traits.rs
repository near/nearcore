use std::collections::HashSet;
use std::hash::Hash;

use super::types;
use hash::HashValue;

pub trait VerifierLike {
   fn compute_state(&mut self, transactions: &[types::StatedTransaction]) -> types::State;
}

// encode a type to byte array
pub trait Encode {
    fn encode(&self) -> Option<Vec<u8>>;
}

// decode from byte array
pub trait Decode: Sized {
    fn decode(data: &[u8]) -> Option<Self>;
}

/// trait that abstracts ``Header"
pub trait Header: Clone + Send + Sync + Encode + Decode + Eq + 'static {
    // TODO: add methods
    fn hash(&self) -> HashValue;
}

/// trait that abstracts ``block", ideally could be used for both beacon-chain blocks
/// and shard-chain blocks
pub trait Block: Clone + Send + Sync + Encode + Decode + Eq + 'static {
    type Header;
    type Body;

    fn header(&self) -> &Self::Header;
    fn body(&self) -> &Self::Body;
    fn deconstruct(self) -> (Self::Header, Self::Body);
    fn new(header: Self::Header, body: Self::Body) -> Self;
    fn hash(&self) -> HashValue;
}

pub trait Verifier {
    fn compute_state(&mut self, transactions: &[types::StatedTransaction]) -> types::State;
}

pub trait WitnessSelector {
    fn epoch_witnesses(&self, epoch: u64) -> &HashSet<types::UID>;
    fn epoch_leader(&self, epoch: u64) -> types::UID;
}

pub type GenericResult = Result<(), &'static str>;

pub trait Payload: Hash {
    fn verify(&self) -> GenericResult;
}

pub trait TxFlow<P: Payload,
    C,
    MessageCallback: Fn() -> GenericResult,
    PayloadCallback: Fn() -> GenericResult,
    MessageSubscriber: Fn(types::UID, &types::SignedMessageData<P>) -> GenericResult,
    ConsensusSubscriber: Fn(types::ConsensusBlockBody<P, C>),
>{
    /// Tells TxFlow to process the given TxFlow message received from another peer. TxFlow signals
    /// when the message is processed.
    fn process_message(message: types::SignedMessageData<P>, callback: MessageCallback) -> GenericResult;
    /// Tells TxFlow to process a payload, e.g. for in-shard TxFlow it is a transaction received
    /// from a client. TxFlow signals when the payload is accepted.
    fn process_payload(payload: P, callback: PayloadCallback) -> GenericResult;
    /// Subscribes to the messages produced by TxFlow. These messages indicate the receiver that
    /// they have to be relayed to.
    fn subscribe_to_messages(subscriber: MessageSubscriber) -> GenericResult;
    /// Subscribes to the consensus blocks produced by TxFlow. The consensus blocks contain messages
    /// with payload + some content specific to whether TxFlow is used on the Beacon Chain or in
    /// the shard.
    fn subscribe_to_consensus_blocks(subscriber: ConsensusSubscriber) -> GenericResult;
}
