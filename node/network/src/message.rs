use beacon::types::SignedBeaconBlock;
use primitives::hash::CryptoHash;
use primitives::types::{AccountId, Gossip};
use transaction::{SignedTransaction, ReceiptTransaction, ChainPayload};
use shard::SignedShardBlock;

pub type RequestId = u64;

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
pub enum Message {
    // Box is used here because SignedTransaction
    // is significantly larger than other enum members
    Transaction(Box<SignedTransaction>),
    Receipt(Box<ReceiptTransaction>),
    Status(Status),
    BlockAnnounce(Box<(SignedBeaconBlock, SignedShardBlock)>),
    Gossip(Box<Gossip<ChainPayload>>),
}

/// status sent on connection
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Status {
    /// Protocol version.
    pub version: u32,
    /// Best block index.
    pub best_index: u64,
    /// Best block hash.
    pub best_hash: CryptoHash,
    /// Genesis hash.
    pub genesis_hash: CryptoHash,
    /// Account id.
    pub account_id: Option<AccountId>,
}
