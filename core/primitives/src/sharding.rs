use crate::crypto::group_signature::GroupSignature;
use crate::hash::{hash_struct, CryptoHash};
use crate::merkle::{merklize, MerklePath};
use crate::types::MerkleHash;
use reed_solomon_erasure::{ReedSolomon, Shard};

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

#[derive(Serialize, Clone)]
pub struct ShardChunkHeader {
    pub prev_block_hash: CryptoHash,
    pub encoded_merkle_root: CryptoHash,
    pub height: u64,
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
    pub fn from_header(header: ShardChunkHeader, total_shards: usize) -> Self {
        Self { header, content: EncodedShardChunkBody { parts: vec![None; total_shards] } }
    }

    pub fn from_parts_and_metadata(
        prev_block_hash: CryptoHash,
        height: u64,
        parts: Vec<Option<Shard>>,

        data_shards: usize,
        parity_shards: usize,
    ) -> Self {
        let mut content = EncodedShardChunkBody { parts };
        content.reconstruct(data_shards, parity_shards);
        let (encoded_merkle_root, _) = content.get_merkle_hash_and_paths();
        let header = ShardChunkHeader { prev_block_hash, encoded_merkle_root, height };

        Self { header, content }
    }

    pub fn chunk_hash(&self) -> CryptoHash {
        hash_struct(self)
    }
}
