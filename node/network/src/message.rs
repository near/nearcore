use primitives::beacon::SignedBeaconBlock;
use primitives::chain::ChainPayload;
use primitives::chain::ReceiptBlock;
use primitives::chain::SignedShardBlock;
use primitives::hash::CryptoHash;
use primitives::types::{AccountId, BlockId, Gossip};
use serde_derive::{Deserialize, Serialize};

pub type RequestId = u64;
pub type CoupledBlock = (SignedBeaconBlock, SignedShardBlock);

#[derive(PartialEq, Debug, Serialize, Deserialize)]
pub enum Message {
    // Box is used here because SignedTransaction
    // is significantly larger than other enum members
    Receipt(Box<ReceiptBlock>),
    Status(Status),

    BlockAnnounce(Box<CoupledBlock>),
    BlockFetchRequest(RequestId, Vec<CryptoHash>),
    BlockFetchResponse(RequestId, Vec<CoupledBlock>),

    Gossip(Box<Gossip<ChainPayload>>),
    PayloadRequest(RequestId, Vec<CryptoHash>, Vec<CryptoHash>),
    PayloadResponse(RequestId, Vec<ChainPayload>)
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
