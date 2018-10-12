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
pub enum Payload {
    None,
    Transactions(Vec<SignedTransaction>),
}

#[derive(Hash)]
pub struct MessageBody {
    pub owner_uid: u64,
    pub parents: Vec<u64>,  // Hashes of the parents.
    pub epoch: u64,
    pub is_commit: bool,
    pub payload: Payload
}

pub struct SignedMessage {
    pub owner_sig: u128,  // Signature of the hash.
    pub hash: u64,  // Hash of the body.
    pub body: MessageBody
}
