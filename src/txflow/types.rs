// 1. Transaction structs.

#[derive(Hash)]
pub struct TransactionBody {
    nonce: u64,
    sender_uid: u64,
    receiver_uid: u64,
    amount: u64
}

#[derive(Hash)]
pub struct SignedTransaction {
    sender_sig: u128,
    hash: u64,
    pub body: TransactionBody
}

#[derive(Hash)]
pub enum TransactionState {
    Sender,
    Receiver,
    SenderRollback,
    Cancelled
}

#[derive(Hash)]
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

#[derive(Hash)]
pub struct MessageBody {
    pub owner_uid: u64,
    // Hashes of the parents.
    pub parents: Vec<u64>,
    pub epoch: u64,
    pub is_representative: bool,
    pub is_endorsement: bool,
    pub transactions: Vec<SignedTransaction>,
    pub epoch_block_header: Option<SignedEpochBlockHeader>,
}

pub struct SignedMessage {
    pub owner_sig: u128,  // Signature of the hash.
    pub hash: u64,  // Hash of the body.
    pub body: MessageBody
}
