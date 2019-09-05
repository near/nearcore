use std::sync::Arc;

use reed_solomon_erasure::{ReedSolomon, Shard};

use borsh::{BorshDeserialize, BorshSerialize};
use near_crypto::{Signature, Signer};

use crate::hash::{hash, CryptoHash};
use crate::merkle::{merklize, MerklePath};
use crate::receipt::Receipt;
use crate::transaction::SignedTransaction;
use crate::types::{BlockIndex, Gas, MerkleHash, ShardId, ValidatorStake};

#[derive(BorshSerialize, BorshDeserialize, Hash, Eq, PartialEq, Clone, Debug, Default)]
pub struct ChunkHash(pub CryptoHash);

impl AsRef<[u8]> for ChunkHash {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

#[derive(BorshSerialize, BorshDeserialize, Clone, PartialEq, Eq, Debug)]
pub struct ShardChunkHeaderInner {
    /// Previous block hash.
    pub prev_block_hash: CryptoHash,
    pub prev_state_root: CryptoHash,
    pub encoded_merkle_root: CryptoHash,
    pub encoded_length: u64,
    pub height_created: BlockIndex,
    /// Shard index.
    pub shard_id: ShardId,
    /// Gas used in this chunk.
    pub gas_used: Gas,
    /// Gas limit voted by validators.
    pub gas_limit: Gas,
    /// Receipts merkle root.
    pub receipts_root: CryptoHash,
    /// Validator proposals.
    pub validator_proposals: Vec<ValidatorStake>,
}

#[derive(BorshSerialize, BorshDeserialize, Clone, PartialEq, Eq, Debug)]
#[borsh_init(init)]
pub struct ShardChunkHeader {
    pub inner: ShardChunkHeaderInner,

    pub height_included: BlockIndex,

    /// Signature of the chunk producer.
    pub signature: Signature,

    #[borsh_skip]
    pub hash: ChunkHash,
}

impl ShardChunkHeader {
    pub fn init(&mut self) {
        self.hash = ChunkHash(hash(&self.inner.try_to_vec().expect("Failed to serialize")));
    }

    pub fn chunk_hash(&self) -> ChunkHash {
        self.hash.clone()
    }

    pub fn new(
        prev_block_hash: CryptoHash,
        prev_state_root: CryptoHash,
        encoded_merkle_root: CryptoHash,
        encoded_length: u64,
        height: BlockIndex,
        shard_id: ShardId,
        gas_used: Gas,
        gas_limit: Gas,
        receipts_root: CryptoHash,
        validator_proposals: Vec<ValidatorStake>,
        signer: Arc<dyn Signer>,
    ) -> Self {
        let inner = ShardChunkHeaderInner {
            prev_block_hash,
            prev_state_root,
            encoded_merkle_root,
            encoded_length,
            height_created: height,
            shard_id,
            gas_used,
            gas_limit,
            receipts_root,
            validator_proposals,
        };
        let hash = ChunkHash(hash(&inner.try_to_vec().expect("Failed to serialize")));
        let signature = signer.sign(hash.as_ref());
        Self { inner, height_included: 0, signature, hash }
    }
}

#[derive(Default, BorshSerialize, BorshDeserialize)]
pub struct EncodedShardChunkBody {
    pub parts: Vec<Option<Shard>>,
}

#[derive(BorshSerialize, BorshDeserialize)]
pub struct EncodedShardChunk {
    pub header: ShardChunkHeader,
    pub content: EncodedShardChunkBody,
}

#[derive(BorshSerialize, BorshDeserialize, Debug, Clone, Eq, PartialEq)]
pub struct ChunkOnePart {
    pub shard_id: u64,
    pub chunk_hash: ChunkHash,
    pub header: ShardChunkHeader,
    pub part_id: u64,
    pub part: Box<[u8]>,
    pub receipts: Vec<Receipt>,
    pub receipts_proofs: Vec<MerklePath>,
    pub merkle_path: MerklePath,
}

#[derive(BorshSerialize, BorshDeserialize, Debug, Clone, Eq, PartialEq)]
pub struct ShardChunk {
    pub chunk_hash: ChunkHash,
    pub header: ShardChunkHeader,
    pub transactions: Vec<SignedTransaction>,
    pub receipts: Vec<Receipt>,
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
        merklize(&self.parts.iter().map(|x| x.as_ref().unwrap().clone()).collect::<Vec<_>>())
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
        gas_used: Gas,
        gas_limit: Gas,
        receipts_root: CryptoHash,
        validator_proposal: Vec<ValidatorStake>,

        encoded_length: u64,
        parts: Vec<Option<Shard>>,

        data_shards: usize,
        parity_shards: usize,

        signer: Arc<dyn Signer>,
    ) -> (Self, Vec<MerklePath>) {
        let mut content = EncodedShardChunkBody { parts };
        content.reconstruct(data_shards, parity_shards);
        let (encoded_merkle_root, merkle_paths) = content.get_merkle_hash_and_paths();
        let header = ShardChunkHeader::new(
            prev_block_hash,
            prev_state_root,
            encoded_merkle_root,
            encoded_length,
            height,
            shard_id,
            gas_used,
            gas_limit,
            receipts_root,
            validator_proposal,
            signer,
        );

        (Self { header, content }, merkle_paths)
    }

    pub fn chunk_hash(&self) -> ChunkHash {
        self.header.chunk_hash()
    }
}
