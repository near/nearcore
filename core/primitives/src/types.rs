use std::hash::{Hash, Hasher};
// 1. Transaction structs.

#[derive(Hash, Serialize, Deserialize, PartialEq, Eq, Debug)]
pub struct TransactionBody {
    nonce: u64,
    sender_uid: u64,
    receiver_uid: u64,
    amount: u64
}

impl TransactionBody {
    pub fn new(nonce: u64, sender_uid: u64, receiver_uid: u64, amount: u64) -> TransactionBody {
        TransactionBody {
            nonce,
            sender_uid,
            receiver_uid,
            amount
        }
    }
}

#[derive(Hash, Serialize, Deserialize, PartialEq, Eq, Debug)]
pub struct SignedTransaction {
    sender_sig: u128,
    hash: u64,
    pub body: TransactionBody
}

impl SignedTransaction {
    pub fn new(sender_sig: u128, hash: u64, body: TransactionBody) -> SignedTransaction {
        SignedTransaction {
            sender_sig,
            hash,
            body
        }
    }
}

#[derive(Hash, Debug, Serialize, Deserialize)]
pub enum TransactionState {
    Sender,
    Receiver,
    SenderRollback,
    Cancelled
}

#[derive(Hash, Debug, Serialize, Deserialize)]
pub struct StatedTransaction {
    transaction: SignedTransaction,
    state: TransactionState
}

// 2. State structs.

#[derive(Hash, Debug)]
pub struct State {

}

// 3. Epoch blocks produced by verifiers running inside a shard.

#[derive(Hash, Debug, Serialize, Hash)]
pub struct EpochBlockHeader {
    pub shard_id: u32,
    pub verifier_epoch: u64,
    pub txflow_epoch: u64,
    pub prev_header_hash: u64,

    pub states_merkle_root: u64,
    pub new_transactions_merkle_root: u64,
    pub cancelled_transactions_merkle_root: u64
}

#[derive(Hash, Debug)]
pub struct SignedEpochBlockHeader {
    pub bls_sig: u128,
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
    Hash(u64),
    State(State),
}

#[derive(Hash, Debug)]
pub enum MerkleStatedTransactionNode {
    Hash(u64),
    StatedTransaction(StatedTransaction),
}

#[derive(Hash, Debug)]
pub struct ShardedEpochBlockBody {
    states_subtree: Vec<MerkleStateNode>,
    new_transactions_subtree: Vec<MerkleStatedTransactionNode>,
    cancelled_transactions_subtree: Vec<MerkleStatedTransactionNode>,
}

// 4. TxFlow-specific structs.

/// Endorsement of a representative message. Includes the epoch of the message that it endorses as
/// well as the BLS signature part. The leader should also include such self-endorsement upon
/// creation of the representative message.
#[derive(Hash, Debug, Clone)]
pub struct Endorsement {
    pub epoch: u64,
    pub signature: u128
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
    pub owner_uid: u64,
    pub parents: Vec<u64>,
    pub epoch: u64,
    pub payload: P,
    /// Optional endorsement of this or other representative block.
    pub endorsements: Vec<Endorsement>,
}

#[derive(Debug, Clone)]
pub struct SignedMessageData<P> {
    /// Signature of the hash.
    pub owner_sig: u128,
    /// Hash of the body.
    pub hash: u64,
    pub body: MessageDataBody<P>,
}

impl<P> Hash for SignedMessageData<P> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(self.hash);
    }
}


#[derive(Hash, Debug)]
pub struct ConsensusBlockHeader {
    pub body_hash: u64,
    pub prev_block_body_hash: u64,
}

#[derive(Hash, Debug)]
pub struct ConsensusBlockBody<P, C> {
    /// TxFlow messages that constitute that consensus block together with the endorsements.
    pub messages: Vec<SignedMessageData<P>>,

    /// The content specific to where the TxFlow is used: in shard or in beacon chain.
    pub content: C,
}
