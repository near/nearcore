use std::collections::HashMap;
use std::convert::{TryFrom, TryInto};
use std::iter::FromIterator;
use std::sync::Arc;

use chrono::prelude::{DateTime, NaiveDateTime, Utc};
use chrono::serde::ts_nanoseconds;
use protobuf::{Message as ProtoMessage, RepeatedField, SingularPtrField};

use near_protos::chain as chain_proto;

use crate::crypto::signature::{verify, PublicKey, Signature, DEFAULT_SIGNATURE};
use crate::crypto::signer::EDSigner;
use crate::hash::{hash, CryptoHash};
use crate::merkle::merklize;
use crate::serialize::{base_format, vec_base_format};
use crate::sharding::ShardChunkHeader;
use crate::transaction::SignedTransaction;
use crate::types::{Balance, BlockIndex, GasUsage, MerkleHash, ShardId, ValidatorStake};
use crate::utils::proto_to_type;

/// Number of nano seconds in one second.
const NS_IN_SECOND: u64 = 1_000_000_000;

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct BlockHeader {
    /// Height of this block since the genesis block (height 0).
    pub height: BlockIndex,
    /// Hash of the block previous to this in the chain.
    #[serde(with = "base_format")]
    pub prev_hash: CryptoHash,
    /// Root hash of the state at the previous block.
    #[serde(with = "base_format")]
    pub prev_state_root: MerkleHash,
    /// Root hash of the transactions in the given block.
    #[serde(with = "base_format")]
    pub tx_root: MerkleHash,
    /// Timestamp at which the block was built.
    #[serde(with = "ts_nanoseconds")]
    pub timestamp: DateTime<Utc>,
    /// Approval mask, given current block producers.
    pub approval_mask: Vec<bool>,
    /// Approval signatures.
    #[serde(with = "vec_base_format")]
    pub approval_sigs: Vec<Signature>,
    /// Total weight.
    pub total_weight: Weight,
    /// Validator proposals.
    pub validator_proposal: Vec<ValidatorStake>,
    /// Epoch start hash of the previous epoch.
    /// Used for retrieving validator information
    pub epoch_hash: CryptoHash,
    /// Sum of gas used across all chunks.
    pub gas_used: GasUsage,
    /// Gas limit. Same for all chunks.
    pub gas_limit: GasUsage,
    /// Gas price. Same for all chunks
    pub gas_price: Balance,

    /// Signature of the block producer.
    #[serde(with = "base_format")]
    pub signature: Signature,

    /// Cached value of hash for this block.
    #[serde(with = "base_format")]
    hash: CryptoHash,
}

impl BlockHeader {
    fn header_body(
        height: BlockIndex,
        prev_hash: CryptoHash,
        prev_state_root: MerkleHash,
        tx_root: MerkleHash,
        timestamp: DateTime<Utc>,
        approval_mask: Vec<bool>,
        approval_sigs: Vec<Signature>,
        total_weight: Weight,
        mut validator_proposal: Vec<ValidatorStake>,
        epoch_hash: CryptoHash,
        gas_used: GasUsage,
        gas_limit: GasUsage,
        gas_price: Balance,
    ) -> chain_proto::BlockHeaderBody {
        chain_proto::BlockHeaderBody {
            height,
            prev_hash: prev_hash.into(),
            prev_state_root: prev_state_root.into(),
            tx_root: tx_root.into(),
            timestamp: timestamp.timestamp_nanos() as u64,
            approval_mask,
            approval_sigs: RepeatedField::from_iter(
                approval_sigs.iter().map(std::convert::Into::into),
            ),
            total_weight: total_weight.to_num(),
            validator_proposal: RepeatedField::from_iter(
                validator_proposal.drain(..).map(std::convert::Into::into),
            ),
            epoch_hash: epoch_hash.into(),
            gas_used,
            gas_limit,
            gas_price: SingularPtrField::some(gas_price.into()),
            cached_size: Default::default(),
            unknown_fields: Default::default(),
        }
    }

    pub fn new(
        height: BlockIndex,
        prev_hash: CryptoHash,
        prev_state_root: MerkleHash,
        tx_root: MerkleHash,
        timestamp: DateTime<Utc>,
        approval_mask: Vec<bool>,
        approval_sigs: Vec<Signature>,
        total_weight: Weight,
        validator_proposal: Vec<ValidatorStake>,
        epoch_hash: CryptoHash,
        gas_used: GasUsage,
        gas_limit: GasUsage,
        gas_price: Balance,
        signer: Arc<dyn EDSigner>,
    ) -> Self {
        let hb = Self::header_body(
            height,
            prev_hash,
            prev_state_root,
            tx_root,
            timestamp,
            approval_mask,
            approval_sigs,
            total_weight,
            validator_proposal,
            epoch_hash,
            gas_used,
            gas_limit,
            gas_price,
        );
        let bytes = hb.write_to_bytes().expect("Failed to serialize");
        let hash = hash(&bytes);
        let h = chain_proto::BlockHeader {
            body: SingularPtrField::some(hb),
            signature: signer.sign(hash.as_ref()).into(),
            cached_size: Default::default(),
            unknown_fields: Default::default(),
        };
        h.try_into().expect("Failed to parse just created header")
    }

    pub fn genesis(
        state_root: MerkleHash,
        timestamp: DateTime<Utc>,
        initial_gas_limit: GasUsage,
        initial_gas_price: Balance,
    ) -> Self {
        let header_body = Self::header_body(
            0,
            CryptoHash::default(),
            state_root,
            MerkleHash::default(),
            timestamp,
            vec![],
            vec![],
            0.into(),
            vec![],
            CryptoHash::default(),
            0,
            initial_gas_limit,
            initial_gas_price,
        );
        chain_proto::BlockHeader {
            body: SingularPtrField::some(header_body),
            signature: DEFAULT_SIGNATURE.into(),
            cached_size: Default::default(),
            unknown_fields: Default::default(),
        }
        .try_into()
        .expect("Failed to parse just created header")
    }

    pub fn hash(&self) -> CryptoHash {
        self.hash
    }

    /// Verifies that given public key produced the block.
    pub fn verify_block_producer(&self, public_key: &PublicKey) -> bool {
        verify(self.hash.as_ref(), &self.signature, public_key)
    }
}

impl TryFrom<chain_proto::BlockHeader> for BlockHeader {
    type Error = Box<dyn std::error::Error>;

    fn try_from(proto: chain_proto::BlockHeader) -> Result<Self, Self::Error> {
        let body = proto.body.into_option().ok_or("Missing Header body")?;
        let bytes = body.write_to_bytes().map_err(|err| err.to_string())?;
        let hash = hash(&bytes);
        let height = body.height;
        let prev_hash = body.prev_hash.try_into()?;
        let prev_state_root = body.prev_state_root.try_into()?;
        let tx_root = body.tx_root.try_into()?;
        let timestamp = DateTime::from_utc(
            NaiveDateTime::from_timestamp(
                (body.timestamp / NS_IN_SECOND) as i64,
                (body.timestamp % NS_IN_SECOND) as u32,
            ),
            Utc,
        );
        let approval_mask = body.approval_mask;
        let approval_sigs =
            body.approval_sigs.into_iter().map(TryInto::try_into).collect::<Result<Vec<_>, _>>()?;
        let total_weight = body.total_weight.into();
        let signature = proto.signature.try_into()?;
        let validator_proposal = body
            .validator_proposal
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<_>, _>>()?;
        let epoch_hash = body.epoch_hash.try_into()?;
        Ok(BlockHeader {
            height,
            prev_hash,
            prev_state_root,
            tx_root,
            timestamp,
            approval_mask,
            approval_sigs,
            total_weight,
            validator_proposal,
            epoch_hash,
            gas_used: body.gas_used,
            gas_limit: body.gas_limit,
            gas_price: proto_to_type(body.gas_price)?,
            signature,
            hash,
        })
    }
}

impl From<BlockHeader> for chain_proto::BlockHeader {
    fn from(mut header: BlockHeader) -> Self {
        chain_proto::BlockHeader {
            body: SingularPtrField::some(chain_proto::BlockHeaderBody {
                height: header.height,
                prev_hash: header.prev_hash.into(),
                prev_state_root: header.prev_state_root.into(),
                tx_root: header.tx_root.into(),
                timestamp: header.timestamp.timestamp_nanos() as u64,
                approval_mask: header.approval_mask,
                approval_sigs: RepeatedField::from_iter(
                    header.approval_sigs.iter().map(std::convert::Into::into),
                ),
                total_weight: header.total_weight.to_num(),
                validator_proposal: RepeatedField::from_iter(
                    header.validator_proposal.drain(..).map(std::convert::Into::into),
                ),
                epoch_hash: header.epoch_hash.into(),
                gas_used: header.gas_used,
                gas_limit: header.gas_limit,
                gas_price: SingularPtrField::some(header.gas_price.into()),
                cached_size: Default::default(),
                unknown_fields: Default::default(),
            }),
            signature: header.signature.into(),
            cached_size: Default::default(),
            unknown_fields: Default::default(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Bytes(Vec<u8>);

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct Block {
    pub header: BlockHeader,
    pub chunks: Vec<ShardChunkHeader>,
    pub transactions: Vec<SignedTransaction>,
}

impl Block {
    /// Returns genesis block for given genesis date and state root.
    pub fn genesis(
        state_roots: Vec<MerkleHash>,
        timestamp: DateTime<Utc>,
        num_shards: ShardId,
        initial_gas_limit: GasUsage,
        initial_gas_price: Balance,
    ) -> Self {
        let chunks = Block::genesis_chunks(state_roots, num_shards, initial_gas_limit);
        Block {
            header: BlockHeader::genesis(
                Block::compute_state_root(&chunks),
                timestamp,
                initial_gas_limit,
                initial_gas_price,
            ),
            chunks,
            transactions: vec![],
        }
    }

    pub fn genesis_chunks(
        state_roots: Vec<CryptoHash>,
        num_shards: ShardId,
        initial_gas_limit: GasUsage,
    ) -> Vec<ShardChunkHeader> {
        assert!(state_roots.len() == 1 || state_roots.len() == (num_shards as usize));
        (0..num_shards)
            .map(|i| ShardChunkHeader {
                prev_block_hash: CryptoHash::default(),
                prev_state_root: state_roots[i as usize % state_roots.len()],
                encoded_merkle_root: CryptoHash::default(),
                encoded_length: 0,
                height_created: 0,
                height_included: 0,
                shard_id: i,
                gas_used: 0,
                gas_limit: initial_gas_limit,
                validator_proposal: vec![],
                signature: DEFAULT_SIGNATURE,
            })
            .collect()
    }

    /// Produces new block from header of previous block, current state root and set of transactions.
    pub fn produce(
        prev: &BlockHeader,
        height: BlockIndex,
        chunks: Vec<ShardChunkHeader>,
        epoch_hash: CryptoHash,
        transactions: Vec<SignedTransaction>,
        mut approvals: HashMap<usize, Signature>,
        signer: Arc<dyn EDSigner>,
    ) -> Self {
        // TODO: merkelize transactions.
        let tx_root = CryptoHash::default();
        let (approval_mask, approval_sigs) = if let Some(max_approver) = approvals.keys().max() {
            (
                (0..=*max_approver).map(|i| approvals.contains_key(&i)).collect(),
                (0..=*max_approver).filter_map(|i| approvals.remove(&i)).collect(),
            )
        } else {
            (vec![], vec![])
        };

        // Collect aggregate of validators and gas usage/limits from chunks.
        let mut validator_proposals = vec![];
        let mut gas_used = 0;
        let mut gas_limit = 0;
        for chunk in chunks.iter() {
            if chunk.height_included == height {
                validator_proposals.extend_from_slice(&chunk.validator_proposal);
                gas_used += chunk.gas_used;
                gas_limit += chunk.gas_limit;
            }
        }
        // TODO: fix this incorrect calculation
        gas_limit /= chunks.len() as u64;

        let total_weight = (prev.total_weight.to_num() + (approval_sigs.len() as u64) + 1).into();
        Block {
            header: BlockHeader::new(
                height,
                prev.hash(),
                Block::compute_state_root(&chunks),
                tx_root,
                Utc::now(),
                approval_mask,
                approval_sigs,
                total_weight,
                validator_proposals,
                epoch_hash,
                gas_used,
                gas_limit,
                // TODO: calculate this correctly
                prev.gas_price,
                signer,
            ),
            chunks,
            transactions,
        }
    }

    pub fn compute_state_root(chunks: &Vec<ShardChunkHeader>) -> CryptoHash {
        merklize(&chunks.iter().map(|chunk| chunk.prev_state_root).collect::<Vec<CryptoHash>>()).0
    }

    pub fn hash(&self) -> CryptoHash {
        self.header.hash()
    }
}

impl TryFrom<chain_proto::Block> for Block {
    type Error = Box<dyn std::error::Error>;

    fn try_from(proto: chain_proto::Block) -> Result<Self, Self::Error> {
        let transactions =
            proto.transactions.into_iter().map(TryInto::try_into).collect::<Result<Vec<_>, _>>()?;
        let chunks =
            proto.chunks.into_iter().map(TryInto::try_into).collect::<Result<Vec<_>, _>>()?;
        Ok(Block { header: proto_to_type(proto.header)?, chunks, transactions })
    }
}

impl From<Block> for chain_proto::Block {
    fn from(block: Block) -> Self {
        chain_proto::Block {
            header: SingularPtrField::some(block.header.into()),
            chunks: block.chunks.into_iter().map(std::convert::Into::into).collect(),
            transactions: block.transactions.into_iter().map(std::convert::Into::into).collect(),
            cached_size: Default::default(),
            unknown_fields: Default::default(),
        }
    }
}

/// The weight is defined as the number of unique validators approving this fork.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Eq, Ord, Serialize, Deserialize, Default)]
pub struct Weight {
    num: u64,
}

impl Weight {
    pub fn to_num(&self) -> u64 {
        self.num
    }

    pub fn next(&self, num: u64) -> Self {
        Weight { num: self.num + num + 1 }
    }
}

impl From<u64> for Weight {
    fn from(num: u64) -> Self {
        Weight { num }
    }
}

impl std::fmt::Display for Weight {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.num)
    }
}
