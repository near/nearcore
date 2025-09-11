use near_primitives::hash::CryptoHash;
use near_primitives::merkle::MerklePath;
use near_primitives::types::{AccountId, MerkleHash, ShardId};

#[derive(actix::Message, Debug, Clone)]
#[rtype(result = "()")]
pub struct SpiceIncomingPartialData {
    pub data: SpicePartialData,
    pub sender: AccountId,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SpiceDataIdentifier {
    ReceiptProof { block_hash: CryptoHash, from_shard_id: ShardId, to_shard_id: ShardId },
    Witness { block_hash: CryptoHash, shard_id: ShardId },
}

impl SpiceDataIdentifier {
    pub fn block_hash(&self) -> &CryptoHash {
        match self {
            SpiceDataIdentifier::ReceiptProof { block_hash, .. } => block_hash,
            SpiceDataIdentifier::Witness { block_hash, .. } => block_hash,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SpiceDataCommitment {
    pub hash: CryptoHash,
    pub root: MerkleHash,
    pub encoded_length: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpiceDataPart {
    pub part_ord: u64,
    pub part: Box<[u8]>,
    pub merkle_proof: MerklePath,
}

// TODO(spice): Version this struct since it is sent over the network.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpicePartialData {
    // We include id to allow finding recipients and producers when receiving the data.
    pub id: SpiceDataIdentifier,
    pub commitment: SpiceDataCommitment,
    pub parts: Vec<SpiceDataPart>,
}
