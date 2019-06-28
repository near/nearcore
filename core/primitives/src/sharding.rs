use crate::crypto::group_signature::GroupSignature;
use crate::crypto::signature::{Signature, DEFAULT_SIGNATURE};
use crate::crypto::signer::EDSigner;
use crate::hash::{hash_struct, CryptoHash};
use crate::merkle::{merklize, MerklePath};
use crate::transaction::{ReceiptTransaction, SignedTransaction};
use crate::types::{MerkleHash, ShardId};
use near_protos::chain as chain_proto;
use reed_solomon_erasure::{ReedSolomon, Shard};
use std::convert::{TryFrom, TryInto};
use std::sync::Arc;

pub struct MainChainBlockHeader {
    pub prev_block_hash: CryptoHash,
    pub height: u64,
    pub signature: GroupSignature,
}

pub struct MainChainBlockBody {
    pub shard_blocks: Vec<ShardChunkHeader>,
}

pub struct MainChainLocalBlock {
    pub header: MainChainBlockHeader,
    pub body: Option<MainChainBlockBody>,
}

#[derive(Hash, Eq, PartialEq, Clone, Debug, Serialize, Deserialize)]
pub struct ChunkHash(pub CryptoHash);

impl AsRef<[u8]> for ChunkHash {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub struct ShardChunkHeader {
    pub prev_block_hash: CryptoHash,
    pub prev_state_root: CryptoHash,
    pub encoded_merkle_root: CryptoHash,
    pub encoded_length: u64,
    pub height_created: u64,
    pub height_included: u64,
    pub shard_id: ShardId,

    /// Signature of the chunk producer.
    pub signature: Signature,
}

impl ShardChunkHeader {
    pub fn chunk_hash(&self) -> ChunkHash {
        // Exclude height_included and signature
        ChunkHash(hash_struct(&(
            self.prev_block_hash,
            self.prev_state_root,
            self.encoded_merkle_root,
            self.encoded_length,
            self.height_created,
            self.shard_id,
        )))
    }
}

impl TryFrom<chain_proto::ShardChunkHeader> for ShardChunkHeader {
    type Error = Box<dyn std::error::Error>;

    fn try_from(proto: chain_proto::ShardChunkHeader) -> Result<Self, Self::Error> {
        Ok(ShardChunkHeader {
            prev_block_hash: proto.prev_block_hash.try_into()?,
            prev_state_root: proto.prev_state_root.try_into()?,
            encoded_merkle_root: proto.encoded_merkle_root.try_into()?,
            encoded_length: proto.encoded_length.try_into()?,
            height_created: proto.height_created,
            height_included: proto.height_included,
            shard_id: proto.shard_id,
            signature: proto.signature.try_into()?,
        })
    }
}

impl From<ShardChunkHeader> for chain_proto::ShardChunkHeader {
    fn from(header: ShardChunkHeader) -> Self {
        chain_proto::ShardChunkHeader {
            prev_block_hash: header.prev_block_hash.into(),
            prev_state_root: header.prev_state_root.into(),
            encoded_merkle_root: header.encoded_merkle_root.into(),
            encoded_length: header.encoded_length.into(),
            height_created: header.height_created,
            height_included: header.height_included,
            shard_id: header.shard_id,
            signature: header.signature.into(),
            ..Default::default()
        }
    }
}

#[derive(Default, Serialize)]
pub struct EncodedShardChunkBody {
    pub parts: Vec<Option<Shard>>,
}

#[derive(Serialize)]
pub struct EncodedShardChunk {
    pub header: ShardChunkHeader,
    pub content: EncodedShardChunkBody,
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct ChunkOnePart {
    pub shard_id: u64,
    pub chunk_hash: ChunkHash,
    pub header: ShardChunkHeader,
    pub part_id: u64,
    pub part: Box<[u8]>,
    pub receipts: Vec<ReceiptTransaction>,
    pub merkle_path: MerklePath,
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct ShardChunk {
    pub chunk_hash: ChunkHash,
    pub header: ShardChunkHeader,
    pub transactions: Vec<SignedTransaction>,
    pub receipts: Vec<ReceiptTransaction>,
}

impl EncodedShardChunkBody {
    pub fn num_fetched_parts(&self) -> usize {
        let mut fetched_parts: usize = 0;

        for part in self.parts.iter() {
            if part.is_some() {
                fetched_parts += 1;
            }
        }

        return fetched_parts;
    }

    pub fn reconstruct(&mut self, data_shards: usize, parity_shards: usize) {
        let rs = ReedSolomon::new(data_shards, parity_shards).unwrap();
        rs.reconstruct_shards(self.parts.as_mut_slice()).unwrap();
    }

    pub fn get_merkle_hash_and_paths(&self) -> (MerkleHash, Vec<MerklePath>) {
        merklize(&self.parts.iter().map(|x| x.as_ref().unwrap()).collect::<Vec<_>>())
    }
}

impl EncodedShardChunk {
    pub fn from_header(header: ShardChunkHeader, total_parts: usize) -> Self {
        Self { header, content: EncodedShardChunkBody { parts: vec![None; total_parts] } }
    }

    pub fn from_parts_and_metadata(
        prev_block_hash: CryptoHash,
        prev_state_root: CryptoHash,
        height: u64,
        shard_id: ShardId,
        encoded_length: u64,
        parts: Vec<Option<Shard>>,

        data_shards: usize,
        parity_shards: usize,

        signer: Arc<dyn EDSigner>,
    ) -> (Self, Vec<MerklePath>) {
        let mut content = EncodedShardChunkBody { parts };
        content.reconstruct(data_shards, parity_shards);
        let (encoded_merkle_root, merkle_paths) = content.get_merkle_hash_and_paths();
        let mut header = ShardChunkHeader {
            prev_block_hash,
            prev_state_root,
            encoded_merkle_root,
            encoded_length,
            height_created: height,
            height_included: 0,
            shard_id,
            signature: DEFAULT_SIGNATURE,
        };

        header.signature = signer.sign(header.chunk_hash().0.as_ref());

        (Self { header, content }, merkle_paths)
    }

    pub fn chunk_hash(&self) -> ChunkHash {
        self.header.chunk_hash()
    }
}
