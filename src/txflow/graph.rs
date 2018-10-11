#[derive(Hash)]
struct TransactionBody {
    nonce: u64,
    sender_uid: u64,
    receiver_uid: u64,
    amount: u64
}

#[derive(Hash)]
struct SignedTransaction {
    sender_sig: u128,
    hash: u64,
    body: TransactionBody
}

#[derive(Hash)]
enum Payload {
    None,
    Transactions(Vec<SignedTransaction>),
}

#[derive(Hash)]
struct MessageBody {
    owner_uid: u64,
    parents: Vec<u64>,  // Hashes of the parents.
    epoch: u64,
    is_commit: bool,
    payload: Payload
}

struct SignedMessage {
    owner_sig: u128,  // Signature of the hash.
    hash: u64,  // Hash of the body.
    body: MessageBody
}