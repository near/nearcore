use serde_derive::{Deserialize, Serialize};

use nightshade::nightshade_task::Gossip;
use primitives::beacon::SignedBeaconBlock;
use primitives::chain::{ChainPayload, ReceiptBlock, SignedShardBlock};
use primitives::hash::CryptoHash;
use primitives::transaction::SignedTransaction;

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
    /// On peer connected, information about their chain.
    Connected(ConnectedInfo),
    /// Incoming transaction.
    Transaction(Box<SignedTransaction>),
    /// Incoming receipt block.
    Receipt(Box<ReceiptBlock>),

    /// Announce of new block.
    BlockAnnounce(Box<CoupledBlock>),
    /// Request for list of blocks.
    BlockRequest(RequestId, Vec<CryptoHash>),
    /// Fetch range of blocks by index.
    BlockFetchRequest(RequestId, u64, u64),
    /// Response with list of blocks.
    BlockResponse(RequestId, Vec<CoupledBlock>),

    /// Nightshade gossip.
    Gossip(Box<Gossip>),
    /// Announce of tx/receipts between authorities.
    PayloadAnnounce(ChainPayload),
    /// Request specific tx/receipts.
    PayloadRequest(RequestId, Vec<CryptoHash>, Vec<CryptoHash>),
    /// Request payload snapshot diff.
    PayloadSnapshotRequest(RequestId, CryptoHash),
    /// Response with payload for request.
    PayloadResponse(RequestId, ChainPayload),
}
