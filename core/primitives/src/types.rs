// 1. Transaction structs.

#[derive(Hash, Serialize, Deserialize, PartialEq, Debug)]
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

#[derive(Hash, Serialize, Deserialize, PartialEq, Debug)]
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

#[derive(Hash, Serialize, Deserialize)]
pub enum TransactionState {
    Sender,
    Receiver,
    SenderRollback,
    Cancelled
}

#[derive(Hash, Serialize, Deserialize)]
pub struct StatedTransaction {
    transaction: SignedTransaction,
    state: TransactionState
}

// 2. State structs.

#[derive(Hash)]
pub struct State {

}

// 3. Epoch block structs.

#[derive(Hash)]
pub struct EpochBlockHeader {
    shard_id: u32,
    verifier_epoch: u64,
    txflow_epoch: u64,
    prev_header_hash: u64,

    states_merkle_root: u64,
    new_transactions_merkle_root: u64,
    cancelled_transactions_merkle_root: u64
}

#[derive(Hash)]
pub struct SignedEpochBlockHeader {
    pub bls_sig: u128,
    pub epoch_block_header: EpochBlockHeader,
}

#[derive(Hash)]
pub struct FullEpochBlockBody {
    states: Vec<State>,
    new_transactions: Vec<StatedTransaction>,
    cancelled_transactions: Vec<StatedTransaction>,
}

#[derive(Hash)]
pub enum MerkleStateNode {
    Hash(u64),
    State(State),
}

#[derive(Hash)]
pub enum MerkleStatedTransactionNode {
    Hash(u64),
    StatedTransaction(StatedTransaction),
}

#[derive(Hash)]
pub struct ShardedEpochBlockBody {
    states_subtree: Vec<MerkleStateNode>,
    new_transactions_subtree: Vec<MerkleStatedTransactionNode>,
    cancelled_transactions_subtree: Vec<MerkleStatedTransactionNode>,
}
