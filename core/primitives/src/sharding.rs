use borsh::{BorshDeserialize, BorshSerialize};
use reed_solomon_erasure::galois_8::ReedSolomon;

use near_crypto::Signature;

use crate::hash::{hash, CryptoHash};
use crate::merkle::{merklize, MerklePath};
use crate::receipt::Receipt;
use crate::transaction::SignedTransaction;
use crate::types::{Balance, BlockHeight, Gas, MerkleHash, ShardId, StateRoot, ValidatorStake};
use crate::validator_signer::ValidatorSigner;

#[derive(
    BorshSerialize, BorshDeserialize, Serialize, Hash, Eq, PartialEq, Clone, Debug, Default,
)]
pub struct ChunkHash(pub CryptoHash);

impl AsRef<[u8]> for ChunkHash {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl From<ChunkHash> for Vec<u8> {
    fn from(chunk_hash: ChunkHash) -> Self {
        chunk_hash.0.into()
    }
}

impl From<CryptoHash> for ChunkHash {
    fn from(crypto_hash: CryptoHash) -> Self {
        Self(crypto_hash)
    }
}

#[derive(BorshSerialize, BorshDeserialize, Serialize, Clone, PartialEq, Eq, Debug)]
pub struct ShardChunkHeaderInner {
    /// Previous block hash.
    pub prev_block_hash: CryptoHash,
    pub prev_state_root: StateRoot,
    /// Root of the outcomes from execution transactions and results.
    pub outcome_root: CryptoHash,
    pub encoded_merkle_root: CryptoHash,
    pub encoded_length: u64,
    pub height_created: BlockHeight,
    /// Shard index.
    pub shard_id: ShardId,
    /// Gas used in this chunk.
    pub gas_used: Gas,
    /// Gas limit voted by validators.
    pub gas_limit: Gas,
    /// Rent paid in the previous chunk
    pub rent_paid: Balance,
    /// Total validator reward in previous chunk
    pub validator_reward: Balance,
    /// Total balance burnt in previous chunk
    pub balance_burnt: Balance,
    /// Outgoing receipts merkle root.
    pub outgoing_receipts_root: CryptoHash,
    /// Tx merkle root.
    pub tx_root: CryptoHash,
    /// Validator proposals.
    pub validator_proposals: Vec<ValidatorStake>,
}

#[derive(BorshSerialize, BorshDeserialize, Serialize, Clone, PartialEq, Eq, Debug)]
#[borsh_init(init)]
pub struct ShardChunkHeader {
    pub inner: ShardChunkHeaderInner,

    pub height_included: BlockHeight,

    /// Signature of the chunk producer.
    pub signature: Signature,

    #[borsh_skip]
    pub hash: ChunkHash,
}

#[derive(
    BorshSerialize, BorshDeserialize, Serialize, Hash, Eq, PartialEq, Clone, Debug, Default,
)]
pub struct ChunkHashHeight(pub ChunkHash, pub BlockHeight);

impl ShardChunkHeader {
    pub fn init(&mut self) {
        self.hash = ChunkHash(hash(&self.inner.try_to_vec().expect("Failed to serialize")));
    }

    pub fn chunk_hash(&self) -> ChunkHash {
        self.hash.clone()
    }

    pub fn new(
        prev_block_hash: CryptoHash,
        prev_state_root: StateRoot,
        outcome_root: CryptoHash,
        encoded_merkle_root: CryptoHash,
        encoded_length: u64,
        height: BlockHeight,
        shard_id: ShardId,
        gas_used: Gas,
        gas_limit: Gas,
        rent_paid: Balance,
        validator_reward: Balance,
        balance_burnt: Balance,
        outgoing_receipts_root: CryptoHash,
        tx_root: CryptoHash,
        validator_proposals: Vec<ValidatorStake>,
        signer: &dyn ValidatorSigner,
    ) -> Self {
        let inner = ShardChunkHeaderInner {
            prev_block_hash,
            prev_state_root,
            outcome_root,
            encoded_merkle_root,
            encoded_length,
            height_created: height,
            shard_id,
            gas_used,
            gas_limit,
            rent_paid,
            validator_reward,
            balance_burnt,
            outgoing_receipts_root,
            tx_root,
            validator_proposals,
        };
        let (hash, signature) = signer.sign_chunk_header_inner(&inner);
        Self { inner, height_included: 0, signature, hash }
    }
}

#[derive(BorshSerialize, BorshDeserialize, Serialize, Debug, Clone, Eq, PartialEq)]
pub struct PartialEncodedChunk {
    pub shard_id: u64,
    pub chunk_hash: ChunkHash,
    pub header: Option<ShardChunkHeader>,
    pub parts: Vec<PartialEncodedChunkPart>,
    pub receipts: Vec<ReceiptProof>,
}

#[derive(BorshSerialize, BorshDeserialize, Serialize, Debug, Clone, Eq, PartialEq)]
pub struct ShardProof {
    pub from_shard_id: ShardId,
    pub to_shard_id: ShardId,
    pub proof: MerklePath,
}

#[derive(BorshSerialize, BorshDeserialize, Serialize, Debug, Clone, Eq, PartialEq)]
/// For each Merkle proof there is a subset of receipts which may be proven.
pub struct ReceiptProof(pub Vec<Receipt>, pub ShardProof);

#[derive(BorshSerialize, BorshDeserialize, Serialize, Debug, Clone, Eq, PartialEq)]
pub struct PartialEncodedChunkPart {
    pub part_ord: u64,
    pub part: Box<[u8]>,
    pub merkle_proof: MerklePath,
}

#[derive(BorshSerialize, BorshDeserialize, Serialize, Debug, Clone, Eq, PartialEq)]
pub struct ShardChunk {
    pub chunk_hash: ChunkHash,
    pub header: ShardChunkHeader,
    pub transactions: Vec<SignedTransaction>,
    pub receipts: Vec<Receipt>,
}

#[derive(Default, BorshSerialize, BorshDeserialize, Serialize, Debug, Clone, PartialEq, Eq)]
pub struct EncodedShardChunkBody {
    pub parts: Vec<Option<Box<[u8]>>>,
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

    /// Returns true if reconstruction was successful
    pub fn reconstruct(&mut self, rs: &ReedSolomon) -> Result<(), reed_solomon_erasure::Error> {
        rs.reconstruct(self.parts.as_mut_slice())
    }

    pub fn get_merkle_hash_and_paths(&self) -> (MerkleHash, Vec<MerklePath>) {
        merklize(&self.parts.iter().map(|x| x.as_ref().unwrap().clone()).collect::<Vec<_>>())
    }
}

#[derive(BorshSerialize, BorshDeserialize, Serialize)]
struct TransactionReceipt(Vec<SignedTransaction>, Vec<Receipt>);

#[derive(BorshSerialize, BorshDeserialize, Serialize, Debug, Clone, PartialEq, Eq)]
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
        prev_state_root: StateRoot,
        outcome_root: CryptoHash,
        height: BlockHeight,
        shard_id: ShardId,
        rs: &ReedSolomon,
        gas_used: Gas,
        gas_limit: Gas,
        rent_paid: Balance,
        validator_reward: Balance,
        balance_burnt: Balance,

        tx_root: CryptoHash,
        validator_proposals: Vec<ValidatorStake>,
        transactions: Vec<SignedTransaction>,
        outgoing_receipts: &Vec<Receipt>,
        outgoing_receipts_root: CryptoHash,
        signer: &dyn ValidatorSigner,
    ) -> Result<(EncodedShardChunk, Vec<MerklePath>), std::io::Error> {
        let mut bytes = TransactionReceipt(transactions, outgoing_receipts.clone()).try_to_vec()?;

        let mut parts = vec![];
        let data_parts = rs.data_shard_count();
        let total_parts = rs.total_shard_count();
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
            outcome_root,
            height,
            shard_id,
            gas_used,
            gas_limit,
            rent_paid,
            validator_reward,
            balance_burnt,
            outgoing_receipts_root,
            tx_root,
            validator_proposals,
            encoded_length as u64,
            parts,
            rs,
            signer,
        );
        Ok((new_chunk, merkle_paths))
    }

    pub fn from_parts_and_metadata(
        prev_block_hash: CryptoHash,
        prev_state_root: StateRoot,
        outcome_root: CryptoHash,
        height: BlockHeight,
        shard_id: ShardId,
        gas_used: Gas,
        gas_limit: Gas,
        rent_paid: Balance,
        validator_reward: Balance,
        balance_burnt: Balance,
        outgoing_receipts_root: CryptoHash,
        tx_root: CryptoHash,
        validator_proposals: Vec<ValidatorStake>,

        encoded_length: u64,
        parts: Vec<Option<Box<[u8]>>>,

        rs: &ReedSolomon,

        signer: &dyn ValidatorSigner,
    ) -> (Self, Vec<MerklePath>) {
        let mut content = EncodedShardChunkBody { parts };
        content.reconstruct(rs).unwrap();
        let (encoded_merkle_root, merkle_paths) = content.get_merkle_hash_and_paths();
        let header = ShardChunkHeader::new(
            prev_block_hash,
            prev_state_root,
            outcome_root,
            encoded_merkle_root,
            encoded_length,
            height,
            shard_id,
            gas_used,
            gas_limit,
            rent_paid,
            validator_reward,
            balance_burnt,
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

    pub fn create_partial_encoded_chunk(
        &self,
        part_ords: Vec<u64>,
        include_header: bool,
        receipts: Vec<ReceiptProof>,
        merkle_paths: &[MerklePath],
    ) -> PartialEncodedChunk {
        let parts = part_ords
            .iter()
            .map(|part_ord| PartialEncodedChunkPart {
                part_ord: *part_ord,
                part: self.content.parts[*part_ord as usize].clone().unwrap(),
                merkle_proof: merkle_paths[*part_ord as usize].clone(),
            })
            .collect();

        PartialEncodedChunk {
            shard_id: self.header.inner.shard_id,
            chunk_hash: self.header.chunk_hash(),
            header: if include_header { Some(self.header.clone()) } else { None },
            parts,
            receipts,
        }
    }

    pub fn decode_chunk(&self, data_parts: usize) -> Result<ShardChunk, std::io::Error> {
        let unwrapped = self.content.parts[0..data_parts]
            .iter()
            .map(|option| option.as_ref().expect("Missing shard"));
        let encoded_data = unwrapped
            .map(|boxed| boxed.iter())
            .flatten()
            .cloned()
            .take(self.header.inner.encoded_length as usize)
            .collect::<Vec<u8>>();

        let transaction_receipts = TransactionReceipt::try_from_slice(&encoded_data)?;

        Ok(ShardChunk {
            chunk_hash: self.chunk_hash(),
            header: self.header.clone(),
            transactions: transaction_receipts.0,
            receipts: transaction_receipts.1,
        })
    }
}
