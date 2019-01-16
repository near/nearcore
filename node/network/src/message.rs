use beacon::types::{SignedBeaconBlock, SignedBeaconBlockHeader};
use primitives::hash::CryptoHash;
use primitives::types::{
    AccountId, BlockId, ChainPayload, Gossip, ReceiptTransaction, SignedTransaction,
};

pub type RequestId = u64;

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
pub enum Message {
    // Box is used here because SignedTransaction
    // is significantly larger than other enum members
    Transaction(Box<SignedTransaction>),
    Receipt(Box<ReceiptTransaction>),
    Status(Status),
    BlockRequest(BlockRequest),
    BlockResponse(BlockResponse<SignedBeaconBlock>),
    BlockAnnounce(BlockAnnounce<SignedBeaconBlock, SignedBeaconBlockHeader>),
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

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
pub struct BlockRequest {
    /// request id
    pub id: RequestId,
    /// starting from this id
    pub from: BlockId,
    /// ending at this id,
    pub to: Option<BlockId>,
    /// max number of blocks requested
    pub max: Option<u64>,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockResponse<Block> {
    // request id that the response is responding to
    pub id: RequestId,
    // block data
    pub blocks: Vec<Block>,
}

#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub enum BlockAnnounce<B, H> {
    // Announce either header or the entire block
    Header(H),
    Block(B),
}
