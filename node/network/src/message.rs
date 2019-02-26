use primitives::beacon::SignedBeaconBlock;
use primitives::chain::ChainPayload;
use primitives::chain::ReceiptBlock;
use primitives::chain::SignedShardBlock;
use primitives::hash::CryptoHash;
use primitives::types::Gossip;
use serde_derive::{Deserialize, Serialize};

pub type RequestId = u64;
pub type CoupledBlock = (SignedBeaconBlock, SignedShardBlock);

/// Current latest version of the protocol
pub const PROTOCOL_VERSION: u32 = 1;

#[derive(PartialEq, Eq, Debug, Serialize, Deserialize, Clone)]
pub struct ChainState {
    pub genesis_hash: CryptoHash,
    pub last_index: u64,
}

#[derive(PartialEq, Debug, Serialize, Deserialize)]
pub struct ConnectedInfo {
    pub chain_state: ChainState
}

#[derive(PartialEq, Debug, Serialize, Deserialize)]
pub enum Message {
    Connected(ConnectedInfo),
    // Box is used here because SignedTransaction
    // is significantly larger than other enum members
    Receipt(Box<ReceiptBlock>),

    BlockAnnounce(Box<CoupledBlock>),
    BlockFetchRequest(RequestId, Vec<CryptoHash>),
    BlockFetchResponse(RequestId, Vec<CoupledBlock>),

    Gossip(Box<Gossip<ChainPayload>>),
    PayloadRequest(RequestId, Vec<CryptoHash>, Vec<CryptoHash>),
    PayloadResponse(RequestId, ChainPayload)
}
