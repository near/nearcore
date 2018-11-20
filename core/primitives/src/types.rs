use std::borrow::Borrow;
use std::hash::{Hash, Hasher};

/// User identifier. Currently derived from the user's public key.
pub type UID = u64;
// TODO: Separate cryptographic hash from the hashmap hash.
/// Hash of a struct that can be used to verify the signature on the struct. Not to be confused with
/// the hash used in the container like HashMap. Currently we conflate them.
pub type StructHash = u64;
/// Signature of a struct, i.e. signature of the struct's hash. It is a simple signature, not to be
/// confused with the multisig.
pub type StructSignature = u128;
/// Hash used by a struct implementing the Merkle tree.
pub type MerkleHash = u64;
/// Part of the BLS signature.
pub type BLSSignature = u128;


// 1. Transaction structs.

#[derive(Hash, Serialize, Deserialize, PartialEq, Eq, Debug)]
pub struct TransactionBody {
    nonce: u64,
    sender_uid: UID,
    receiver_uid: UID,
    amount: u64,
}

impl TransactionBody {
    pub fn new(nonce: u64, sender_uid: UID, receiver_uid: UID, amount: u64) -> TransactionBody {
        TransactionBody {
            nonce,
            sender_uid,
            receiver_uid,
            amount,
        }
    }
}

#[derive(Hash, Serialize, Deserialize, PartialEq, Eq, Debug)]
pub struct SignedTransaction {
    sender_sig: StructSignature,
    hash: StructHash,
    pub body: TransactionBody,
}

impl SignedTransaction {
    pub fn new(sender_sig: StructSignature, hash: StructHash, body: TransactionBody) -> SignedTransaction {
        SignedTransaction {
            sender_sig,
            hash,
            body,
        }
    }
}

#[derive(Hash, Debug, Serialize, Deserialize)]
pub enum TransactionState {
    Sender,
    Receiver,
    SenderRollback,
    Cancelled,
}

#[derive(Hash, Debug, Serialize, Deserialize)]
pub struct StatedTransaction {
    transaction: SignedTransaction,
    state: TransactionState,
}

// 2. State structs.

#[derive(Hash, Debug)]
pub struct State {
    // TODO: Fill in.
}

// 3. Epoch blocks produced by verifiers running inside a shard.

#[derive(Hash, Debug, Serialize, Deserialize)]
pub struct EpochBlockHeader {
    pub shard_id: u32,
    pub verifier_epoch: u64,
    pub txflow_epoch: u64,
    pub prev_header_hash: StructHash,

    pub states_merkle_root: MerkleHash,
    pub new_transactions_merkle_root: MerkleHash,
    pub cancelled_transactions_merkle_root: MerkleHash,
}

#[derive(Hash, Debug)]
pub struct SignedEpochBlockHeader {
    pub bls_sig: BLSSignature,
    pub epoch_block_header: EpochBlockHeader,
}

#[derive(Hash, Debug)]
pub struct FullEpochBlockBody {
    states: Vec<State>,
    new_transactions: Vec<StatedTransaction>,
    cancelled_transactions: Vec<StatedTransaction>,
}

#[derive(Hash, Debug)]
pub enum MerkleStateNode {
    Hash(MerkleHash),
    State(State),
}

#[derive(Hash, Debug)]
pub enum MerkleStatedTransactionNode {
    Hash(MerkleHash),
    StatedTransaction(StatedTransaction),
}

#[derive(Hash, Debug)]
pub struct ShardedEpochBlockBody {
    states_subtree: Vec<MerkleStateNode>,
    new_transactions_subtree: Vec<MerkleStatedTransactionNode>,
    cancelled_transactions_subtree: Vec<MerkleStatedTransactionNode>,
}

// 4. TxFlow-specific structs.

// 4.1 DAG-specific structs.

/// Endorsement of a representative message. Includes the epoch of the message that it endorses as
/// well as the BLS signature part. The leader should also include such self-endorsement upon
/// creation of the representative message.
#[derive(Hash, Debug, Clone)]
pub struct Endorsement {
    pub epoch: u64,
    pub signature: BLSSignature
}

#[derive(Hash, Debug)]
pub struct InShardPayload {
    pub transactions: Vec<SignedTransaction>,
    pub epoch_block_header: Option<SignedEpochBlockHeader>,
}


#[derive(Hash, Debug)]
pub struct BeaconChainPayload {

}

#[derive(Hash, Debug, Clone)]
/// Not signed data representing TxFlow message.
pub struct MessageDataBody<P> {
    pub owner_uid: UID,
    pub parents: Vec<StructHash>,
    pub epoch: u64,
    pub payload: P,
    /// Optional endorsement of this or other representative block.
    pub endorsements: Vec<Endorsement>,
}

#[derive(Debug, Clone)]
pub struct SignedMessageData<P> {
    /// Signature of the hash.
    pub owner_sig: StructSignature,
    /// Hash of the body.
    pub hash: StructHash,
    pub body: MessageDataBody<P>,
}

impl<P> Hash for SignedMessageData<P> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(self.hash);
    }
}

impl<P> Borrow<StructHash> for SignedMessageData<P> {
    fn borrow(&self) -> &StructHash {
        &self.hash
    }
}

impl<P> PartialEq for SignedMessageData<P> {
    fn eq(&self, other: &Self) -> bool {
        self.hash == other.hash
    }
}

impl<P> Eq for SignedMessageData<P> {}

#[derive(Hash, Debug)]
pub struct ConsensusBlockHeader {
    pub body_hash: StructHash,
    pub prev_block_body_hash: StructHash,
}

#[derive(Hash, Debug)]
pub struct ConsensusBlockBody<P, C> {
    /// TxFlow messages that constitute that consensus block together with the endorsements.
    pub messages: Vec<SignedMessageData<P>>,

    /// The content specific to where the TxFlow is used: in shard or in beacon chain.
    pub content: C,
}

// 4.2 Gossip-specific structs.
#[derive(Hash, Debug)]
pub enum GossipBody<P> {
    /// A gossip with a single `SignedMessageData` that one participant decided to share with another.
    Unsolicited(SignedMessageData<P>),
    /// A reply to an unsolicited gossip with the `SignedMessageData`.
    UnsolicitedReply(SignedMessageData<P>),
    /// A request to provide a list of `SignedMessageData`'s with the following hashes.
    Fetch(Vec<StructHash>),
    /// A response to the fetch request providing the requested messages.
    FetchReply(Vec<SignedMessageData<P>>),
}

/// A single unit of communication between the TxFlow participants.
#[derive(Hash, Debug)]
pub struct Gossip<P> {
    pub sender_uid: UID,
    pub receiver_uid: UID,
    pub sender_sig: StructSignature,
    pub body: GossipBody<P>,
}
