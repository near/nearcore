use hash::{hash_struct, CryptoHash};
use std::borrow::Borrow;
use std::hash::{Hash, Hasher};

/// User identifier. Currently derived from the user's public key.
pub type UID = u64;
/// Account identifier. Provides access to user's state.
pub type AccountId = u64;
// TODO: Separate cryptographic hash from the hashmap hash.
/// Signature of a struct, i.e. signature of the struct's hash. It is a simple signature, not to be
/// confused with the multisig.
pub type StructSignature = u128;
/// Hash used by a struct implementing the Merkle tree.
pub type MerkleHash = CryptoHash;
/// Part of the BLS signature.
pub type BLSSignature = u128;
/// Database record type.
pub type DBValue = Vec<u8>;

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub enum BlockId {
    Number(u64),
    Hash(CryptoHash),
}

// 1. Transaction structs.

/// Call view function in the contracts.
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
pub struct ViewCall {
    pub account: AccountId,
}

/// Result of view call.
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
pub struct ViewCallResult {
    pub account: AccountId,
    pub amount: u64,
}

/// TODO: Call non-view function in the contracts.
#[derive(Hash, Serialize, Deserialize, PartialEq, Eq, Debug, Clone)]
pub struct TransactionBody {
    pub nonce: u64,
    pub sender: AccountId,
    pub receiver: AccountId,
    pub amount: u64,
}

impl TransactionBody {
    pub fn new(nonce: u64, sender: AccountId, receiver: AccountId, amount: u64) -> TransactionBody {
        TransactionBody {
            nonce,
            sender,
            receiver,
            amount,
        }
    }
}

impl Default for TransactionBody {
    fn default() -> Self {
        TransactionBody::new(0, 0, 0, 0)
    }
}

#[derive(Hash, Serialize, Deserialize, PartialEq, Eq, Debug, Clone)]
pub struct SignedTransaction {
    sender_sig: StructSignature,
    hash: CryptoHash,
    pub body: TransactionBody,
}

impl SignedTransaction {
    pub fn new(sender_sig: StructSignature, body: TransactionBody) -> SignedTransaction {
        SignedTransaction {
            sender_sig,
            hash: hash_struct(&body),
            body,
        }
    }
}

impl Default for SignedTransaction {
    fn default() -> Self {
        SignedTransaction::new(0, TransactionBody::default())
    }
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
    pub prev_header_hash: CryptoHash,

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
    new_transactions: Vec<SignedTransaction>,
    cancelled_transactions: Vec<SignedTransaction>,
}

#[derive(Hash, Debug)]
pub enum MerkleStateNode {
    Hash(MerkleHash),
    State(State),
}

#[derive(Hash, Debug)]
pub enum MerkleSignedTransactionNode {
    Hash(MerkleHash),
    SignedTransaction(SignedTransaction),
}

#[derive(Hash, Debug)]
pub struct ShardedEpochBlockBody {
    states_subtree: Vec<MerkleStateNode>,
    new_transactions_subtree: Vec<MerkleSignedTransactionNode>,
    cancelled_transactions_subtree: Vec<MerkleSignedTransactionNode>,
}

// 4. TxFlow-specific structs.

pub type TxFlowHash = u64;

// 4.1 DAG-specific structs.

/// Endorsement of a representative message. Includes the epoch of the message that it endorses as
/// well as the BLS signature part. The leader should also include such self-endorsement upon
/// creation of the representative message.
#[derive(Hash, Debug, Clone)]
pub struct Endorsement {
    pub epoch: u64,
    pub signature: BLSSignature,
}

#[derive(Hash, Debug)]
pub struct InShardPayload {
    pub transactions: Vec<SignedTransaction>,
    pub epoch_block_header: Option<SignedEpochBlockHeader>,
}

#[derive(Hash, Debug)]
pub struct BeaconChainPayload {}

#[derive(Hash, Debug, Clone)]
/// Not signed data representing TxFlow message.
pub struct MessageDataBody<P> {
    pub owner_uid: UID,
    pub parents: Vec<TxFlowHash>,
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
    pub hash: TxFlowHash,
    pub body: MessageDataBody<P>,
}

impl<P> Hash for SignedMessageData<P> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(self.hash);
    }
}

impl<P> Borrow<TxFlowHash> for SignedMessageData<P> {
    fn borrow(&self) -> &TxFlowHash {
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
    pub body_hash: CryptoHash,
    pub prev_block_body_hash: CryptoHash,
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
    Fetch(Vec<TxFlowHash>),
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
