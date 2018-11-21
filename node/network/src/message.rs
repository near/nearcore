use primitives::hash::CryptoHash;
use primitives::types::BlockId;

pub type RequestId = u64;

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
pub enum MessageBody<T, B, H> {
    //TODO: add different types of messages here
    Transaction(T),
    Status(Status),
    BlockRequest(BlockRequest),
    BlockResponse(BlockResponse<B>),
    BlockAnnounce(BlockAnnounce<H>),
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
pub struct Message<T, B, H> {
    pub body: MessageBody<T, B, H>,
}

impl<T, B, H> Message<T, B, H> {
    pub fn new(body: MessageBody<T, B, H>) -> Message<T, B, H> {
        Message { body }
    }
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
}

impl Default for Status {
    fn default() -> Self {
        Status {
            version: 1,
            best_index: 0,
            best_hash: CryptoHash::default(),
            genesis_hash: CryptoHash::default(),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockRequest {
    // request id
    pub id: RequestId,
    // starting from this id
    pub from: BlockId,
    // ending at this id,
    pub to: Option<BlockId>,
    // max number of blocks requested
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
pub struct BlockAnnounce<H> {
    // New block header
    pub header: H,
}
