use std::sync::Arc;

use reed_solomon_erasure::{option_shards_into_shards, ReedSolomon, Shard};

use borsh::{BorshDeserialize, BorshSerialize};
use near_crypto::{BlsSignature, BlsSigner};

use crate::hash::{hash, CryptoHash};
use crate::merkle::{merklize, MerklePath};
use crate::receipt::Receipt;
use crate::transaction::SignedTransaction;
use crate::types::{Balance, BlockIndex, Gas, MerkleHash, ShardId, StateRoot, ValidatorStake};

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
    pub prev_state_root: StateRoot,
    pub prev_state_num_parts: u64,
    pub encoded_merkle_root: CryptoHash,
    pub encoded_length: u64,
    pub height_created: BlockIndex,
    /// Shard index.
    pub shard_id: ShardId,
    /// Gas used in this chunk.
    pub gas_used: Gas,
    /// Gas limit voted by validators.
    pub gas_limit: Gas,
    /// Rent paid in the previous chunk
    pub rent_paid: Balance,
    /// Outgoing receipts merkle root.
    pub outgoing_receipts_root: CryptoHash,
    /// Tx merkle root.
    pub tx_root: CryptoHash,
    /// Validator proposals.
    pub validator_proposals: Vec<ValidatorStake>,
}

#[derive(BorshSerialize, BorshDeserialize, Clone, PartialEq, Eq, Debug)]
#[borsh_init(init)]
pub struct ShardChunkHeader {
    pub inner: ShardChunkHeaderInner,

    pub height_included: BlockIndex,

    /// Signature of the chunk producer.
    pub signature: BlsSignature,

    #[borsh_skip]
    pub hash: ChunkHash,
}

#[derive(BorshSerialize, BorshDeserialize, Hash, Eq, PartialEq, Clone, Debug, Default)]
pub struct ChunkHashHeight(pub ChunkHash, pub BlockIndex);

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
        prev_state_num_parts: u64,
        encoded_merkle_root: CryptoHash,
        encoded_length: u64,
        height: BlockIndex,
        shard_id: ShardId,
        gas_used: Gas,
        gas_limit: Gas,
        rent_paid: Balance,
        outgoing_receipts_root: CryptoHash,
        tx_root: CryptoHash,
        validator_proposals: Vec<ValidatorStake>,
        signer: Arc<dyn BlsSigner>,
    ) -> Self {
        let inner = ShardChunkHeaderInner {
            prev_block_hash,
            prev_state_root,
            prev_state_num_parts,
            encoded_merkle_root,
            encoded_length,
            height_created: height,
            shard_id,
            gas_used,
            gas_limit,
            rent_paid,
            outgoing_receipts_root,
            tx_root,
            validator_proposals,
        };
        let hash = ChunkHash(hash(&inner.try_to_vec().expect("Failed to serialize")));
        let signature = signer.sign(hash.as_ref());
        Self { inner, height_included: 0, signature, hash }
    }
}

#[derive(BorshSerialize, BorshDeserialize, Debug, Clone, Eq, PartialEq)]
pub struct ChunkOnePart {
    pub shard_id: u64,
    pub chunk_hash: ChunkHash,
    pub header: ShardChunkHeader,
    pub part_id: u64,
    pub part: Box<[u8]>,
    pub receipt_proofs: Vec<ReceiptProof>,
    pub merkle_path: MerklePath,
}

#[derive(BorshSerialize, BorshDeserialize, Debug, Clone, Eq, PartialEq)]
pub struct ShardProof {
    pub from_shard_id: ShardId,
    pub to_shard_id: ShardId,
    pub proof: MerklePath,
}

#[derive(BorshSerialize, BorshDeserialize, Debug, Clone, Eq, PartialEq)]
// For each Merkle proof there is a subset of receipts which may be proven.
pub struct ReceiptProof(pub Vec<Receipt>, pub ShardProof);

#[derive(BorshSerialize, BorshDeserialize, Debug, Clone, Eq, PartialEq)]
pub struct ShardChunk {
    pub chunk_hash: ChunkHash,
    pub header: ShardChunkHeader,
    pub transactions: Vec<SignedTransaction>,
    pub receipts: Vec<Receipt>,
}

#[derive(Clone, Debug, Eq, PartialEq, BorshSerialize, BorshDeserialize)]
pub struct ChunkPartMsg {
    pub shard_id: u64,
    pub chunk_hash: ChunkHash,
    pub part_id: u64,
    pub part: Shard,
    pub merkle_path: MerklePath,
}

#[derive(Default, BorshSerialize, BorshDeserialize, Debug)]
pub struct EncodedShardChunkBody {
    pub parts: Vec<Option<Shard>>,
}

impl EncodedShardChunkBody {
    pub fn num_fetched_parts(&self) -> usize {
        let mut fetched_parts: usize = 0;

        for part in self.parts.iter() {
            if part.is_some() {
                fetched_parts += 1;
            }
        }

        fetched_parts
    }

    pub fn reconstruct(&mut self, data_shards: usize, parity_shards: usize) {
        let rs = ReedSolomon::new(data_shards, parity_shards).unwrap();
        rs.reconstruct_shards(self.parts.as_mut_slice()).unwrap();
    }

    pub fn get_merkle_hash_and_paths(&self) -> (MerkleHash, Vec<MerklePath>) {
        merklize(&self.parts.iter().map(|x| x.as_ref().unwrap().clone()).collect::<Vec<_>>())
    }
}

#[derive(BorshSerialize, BorshDeserialize)]
struct TransactionReceipt(Vec<SignedTransaction>, Vec<Receipt>);

#[derive(BorshSerialize, BorshDeserialize, Debug)]
pub struct EncodedShardChunk {
    pub header: ShardChunkHeader,
    pub content: EncodedShardChunkBody,
}

impl EncodedShardChunk {
    pub fn from_header(header: ShardChunkHeader, total_parts: usize) -> Self {
        Self { header, content: EncodedShardChunkBody { parts: vec![None; total_parts] } }
    }

    pub fn new(
        prev_block_hash: CryptoHash,
        prev_state_root: CryptoHash,
        prev_state_num_parts: u64,
        height: u64,
        shard_id: ShardId,
        total_parts: usize,
        data_parts: usize,
        gas_used: Gas,
        gas_limit: Gas,
        rent_paid: Balance,
        tx_root: CryptoHash,
        validator_proposals: Vec<ValidatorStake>,
        transactions: &Vec<SignedTransaction>,
        outgoing_receipts: &Vec<Receipt>,
        outgoing_receipts_root: CryptoHash,
        signer: Arc<dyn BlsSigner>,
    ) -> Result<(EncodedShardChunk, Vec<MerklePath>), std::io::Error> {
        let mut bytes =
            TransactionReceipt(transactions.to_vec(), outgoing_receipts.to_vec()).try_to_vec()?;
        let parity_parts = total_parts - data_parts;

        let mut parts = vec![];
        let encoded_length = bytes.len();

        if bytes.len() % data_parts != 0 {
            bytes.extend((bytes.len() % data_parts..data_parts).map(|_| 0));
        }
        let shard_length = (encoded_length + data_parts - 1) / data_parts;
        assert_eq!(bytes.len(), shard_length * data_parts);

        for i in 0..data_parts {
            parts.push(Some(
                bytes[i * shard_length..(i + 1) * shard_length].to_vec().into_boxed_slice()
                    as Box<[u8]>,
            ));
        }
        for _ in data_parts..total_parts {
            parts.push(None);
        }

        let (new_chunk, merkle_paths) = EncodedShardChunk::from_parts_and_metadata(
            prev_block_hash,
            prev_state_root,
            prev_state_num_parts,
            height,
            shard_id,
            gas_used,
            gas_limit,
            rent_paid,
            outgoing_receipts_root,
            tx_root,
            validator_proposals,
            encoded_length as u64,
            parts,
            data_parts,
            parity_parts,
            signer,
        );
        Ok((new_chunk, merkle_paths))
    }

    pub fn from_parts_and_metadata(
        prev_block_hash: CryptoHash,
        prev_state_root: CryptoHash,
        prev_state_num_parts: u64,
        height: u64,
        shard_id: ShardId,
        gas_used: Gas,
        gas_limit: Gas,
        rent_paid: Balance,
        outgoing_receipts_root: CryptoHash,
        tx_root: CryptoHash,
        validator_proposals: Vec<ValidatorStake>,

        encoded_length: u64,
        parts: Vec<Option<Shard>>,

        data_shards: usize,
        parity_shards: usize,

        signer: Arc<dyn BlsSigner>,
    ) -> (Self, Vec<MerklePath>) {
        let mut content = EncodedShardChunkBody { parts };
        content.reconstruct(data_shards, parity_shards);
        let (encoded_merkle_root, merkle_paths) = content.get_merkle_hash_and_paths();
        let header = ShardChunkHeader::new(
            prev_block_hash,
            prev_state_root,
            prev_state_num_parts,
            encoded_merkle_root,
            encoded_length,
            height,
            shard_id,
            gas_used,
            gas_limit,
            rent_paid,
            outgoing_receipts_root,
            tx_root,
            validator_proposals,
            signer,
        );

        (Self { header, content }, merkle_paths)
    }

    pub fn chunk_hash(&self) -> ChunkHash {
        self.header.chunk_hash()
    }

    pub fn create_chunk_one_part(
        &self,
        part_id: u64,
        receipt_proofs: Vec<ReceiptProof>,
        merkle_path: MerklePath,
    ) -> ChunkOnePart {
        ChunkOnePart {
            shard_id: self.header.inner.shard_id,
            chunk_hash: self.header.chunk_hash(),
            header: self.header.clone(),
            part_id,
            part: self.content.parts[part_id as usize].clone().unwrap(),
            receipt_proofs,
            merkle_path,
        }
    }

    pub fn create_chunk_part_msg(&self, part_id: u64, merkle_path: MerklePath) -> ChunkPartMsg {
        ChunkPartMsg {
            shard_id: self.header.inner.shard_id,
            chunk_hash: self.header.chunk_hash(),
            part_id,
            part: self.content.parts[part_id as usize].clone().unwrap(),
            merkle_path,
        }
    }

    pub fn decode_chunk(&self, data_parts: usize) -> Result<ShardChunk, std::io::Error> {
        let encoded_data = option_shards_into_shards(self.content.parts[0..data_parts].to_vec())
            .iter()
            .map(|boxed| boxed.iter())
            .flatten()
            .cloned()
            .collect::<Vec<u8>>()[0..self.header.inner.encoded_length as usize]
            .to_vec();

        let transaction_receipts = TransactionReceipt::try_from_slice(&encoded_data)?;

        Ok(ShardChunk {
            chunk_hash: self.chunk_hash(),
            header: self.header.clone(),
            transactions: transaction_receipts.0,
            receipts: transaction_receipts.1,
        })
    }
}
