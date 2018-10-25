use primitives::types::{SignedTransaction, SignedEpochBlockHeader};

#[derive(Hash)]
pub struct MessageDataBody {
    pub owner_uid: u64,
    // Hashes of the parents.
    pub parents: Vec<u64>,
    pub epoch: u64,
    pub is_representative: bool,
    pub is_endorsement: bool,
    pub transactions: Vec<SignedTransaction>,
    pub epoch_block_header: Option<SignedEpochBlockHeader>,
}

pub struct SignedMessageData {
    pub owner_sig: u128,  // Signature of the hash.
    pub hash: u64,  // Hash of the body.
    pub body: MessageDataBody
}
