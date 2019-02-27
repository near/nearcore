use serde_derive::{Deserialize, Serialize};

use primitives::beacon::SignedBeaconBlock;
use primitives::chain::{ChainPayload, ReceiptBlock, SignedShardBlock};
use primitives::hash::CryptoHash;
use primitives::transaction::SignedTransaction;
use primitives::types::Gossip;

pub type RequestId = u64;
pub type CoupledBlock = (SignedBeaconBlock, SignedShardBlock);

/// Current latest version of the protocol
pub const PROTOCOL_VERSION: u32 = 1;

#[derive(PartialEq, Eq, Debug, Serialize, Deserialize, Clone)]
pub struct ChainState {
    pub genesis_hash: CryptoHash,
    pub last_index: u64,
}

#[derive(PartialEq, Eq, Debug, Serialize, Deserialize, Clone)]
pub struct ConnectedInfo {
    pub chain_state: ChainState,
}

/// Message passed over the network from peer to peer.
/// Box's are used when message is significantly larger than other enum members.
#[derive(PartialEq, Debug, Serialize, Deserialize)]
pub enum Message {
    Connected(ConnectedInfo),
    Transaction(Box<SignedTransaction>),
    Receipt(Box<ReceiptBlock>),

    BlockAnnounce(Box<CoupledBlock>),
    BlockRequest(RequestId, Vec<CryptoHash>),
    BlockResponse(RequestId, Vec<CoupledBlock>),
    BlockFetchRequest(RequestId, u64, u64),

    Gossip(Box<Gossip<ChainPayload>>),
    PayloadRequest(RequestId, Vec<CryptoHash>, Vec<CryptoHash>),
    PayloadResponse(RequestId, ChainPayload),
}
