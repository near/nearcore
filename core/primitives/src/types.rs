use std::hash::{Hash, Hasher};
use hash::{CryptoHash, hash_struct};

/// User identifier. Currently derived from the user's public key.
pub type UID = u64;
/// Account identifier. Provides access to user's state.
pub type AccountId = u64;
// TODO: Separate cryptographic hash from the hashmap hash.
/// Signature of a struct, i.e. signature of the struct's hash. It is a simple signature, not to be
/// confused with the multisig.
pub type StructSignature = u128;
/// Hash used by a struct implementing the Merkle tree.
pub type MerkleHash = u64;
/// Part of the BLS signature.
pub type BLSSignature = u128;
/// Database record type.
pub type DBValue = Vec<u8>;

// 1. Transaction structs.

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

#[derive(Hash, Debug, Serialize, Deserialize, Clone)]
pub enum TransactionState {
    Sender,
    Receiver,
    SenderRollback,
    Cancelled,
}

#[derive(Hash, Debug, Serialize, Deserialize, Clone)]
pub struct StatedTransaction {
    pub transaction: SignedTransaction,
    pub state: TransactionState,
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

pub type TxFlowHash = u64;

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
