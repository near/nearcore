use std::collections::HashMap;
use std::convert::{TryFrom, TryInto};
use std::fmt;
use std::iter::FromIterator;
use std::sync::Arc;

use chrono::prelude::{DateTime, NaiveDateTime, Utc};
use chrono::serde::ts_nanoseconds;
use protobuf::{Message as ProtoMessage, RepeatedField, SingularPtrField};

use near_primitives::crypto::signature::{verify, PublicKey, Signature, DEFAULT_SIGNATURE};
use near_primitives::crypto::signer::EDSigner;
use near_primitives::hash::{hash, CryptoHash};
use near_primitives::rpc::ABCIQueryResponse;
use near_primitives::transaction::{ReceiptTransaction, SignedTransaction, TransactionResult};
use near_primitives::types::{AccountId, BlockIndex, MerkleHash, ShardId, ValidatorStake};
use near_primitives::utils::proto_to_type;
use near_protos::chain as chain_proto;
use near_store::{StoreUpdate, WrappedTrieChanges};

use crate::error::Error;

/// Number of nano seconds in one second.
const NS_IN_SECOND: u64 = 1_000_000_000;

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct BlockHeader {
    /// Height of this block since the genesis block (height 0).
    pub height: BlockIndex,
    /// Hash of the block previous to this in the chain.
    pub prev_hash: CryptoHash,
    /// Root hash of the state at the previous block.
    pub prev_state_root: MerkleHash,
    /// Root hash of the transactions in the given block.
    pub tx_root: MerkleHash,
    /// Timestamp at which the block was built.
    #[serde(with = "ts_nanoseconds")]
    pub timestamp: DateTime<Utc>,
    /// Approval mask, given current block producers.
    pub approval_mask: Vec<bool>,
    /// Approval signatures.
    pub approval_sigs: Vec<Signature>,
    /// Total weight.
    pub total_weight: Weight,
    /// Validator proposals.
    pub validator_proposal: Vec<ValidatorStake>,

    /// Signature of the block producer.
    pub signature: Signature,

    /// Cached value of hash for this block.
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
            ..Default::default()
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
        );
        let bytes = hb.write_to_bytes().expect("Failed to serialize");
        let hash = hash(&bytes);
        let h = chain_proto::BlockHeader {
            body: SingularPtrField::some(hb),
            signature: signer.sign(hash.as_ref()).into(),
            ..Default::default()
        };
        h.try_into().expect("Failed to parse just created header")
    }

    pub fn genesis(state_root: MerkleHash, timestamp: DateTime<Utc>) -> Self {
        chain_proto::BlockHeader {
            body: SingularPtrField::some(Self::header_body(
                0,
                CryptoHash::default(),
                state_root,
                MerkleHash::default(),
                timestamp,
                vec![],
                vec![],
                0.into(),
                vec![],
            )),
            signature: DEFAULT_SIGNATURE.into(),
            ..Default::default()
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
                ..Default::default()
            }),
            signature: header.signature.into(),
            ..Default::default()
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Bytes(Vec<u8>);

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct Block {
    pub header: BlockHeader,
    pub transactions: Vec<SignedTransaction>,
}

impl Block {
    /// Returns genesis block for given genesis date and state root.
    pub fn genesis(state_root: MerkleHash, timestamp: DateTime<Utc>) -> Self {
        Block { header: BlockHeader::genesis(state_root, timestamp), transactions: vec![] }
    }

    /// Produces new block from header of previous block, current state root and set of transactions.
    pub fn produce(
        prev: &BlockHeader,
        height: BlockIndex,
        state_root: MerkleHash,
        transactions: Vec<SignedTransaction>,
        mut approvals: HashMap<usize, Signature>,
        validator_proposal: Vec<ValidatorStake>,
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
        let total_weight = (prev.total_weight.to_num() + (approval_sigs.len() as u64) + 1).into();
        Block {
            header: BlockHeader::new(
                height,
                prev.hash(),
                state_root,
                tx_root,
                Utc::now(),
                approval_mask,
                approval_sigs,
                total_weight,
                validator_proposal,
                signer,
            ),
            transactions,
        }
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
        Ok(Block { header: proto_to_type(proto.header)?, transactions })
    }
}

impl From<Block> for chain_proto::Block {
    fn from(block: Block) -> Self {
        chain_proto::Block {
            header: SingularPtrField::some(block.header.into()),
            transactions: block.transactions.into_iter().map(std::convert::Into::into).collect(),
            ..Default::default()
        }
    }
}

#[derive(Eq, PartialEq, Debug)]
pub enum BlockStatus {
    /// Block is the "next" block, updating the chain head.
    Next,
    /// Block does not update the chain head and is a fork.
    Fork,
    /// Block updates the chain head via a (potentially disruptive) "reorg".
    /// Previous block was not our previous chain head.
    Reorg,
}

/// Options for block origin.
#[derive(Eq, PartialEq)]
pub enum Provenance {
    /// No provenance.
    NONE,
    /// Adds block while in syncing mode.
    SYNC,
    /// Block we produced ourselves.
    PRODUCED,
}

/// Information about valid transaction that was processed by chain + runtime.
pub struct ValidTransaction {
    pub transaction: SignedTransaction,
}

/// Map of shard to list of receipts to send to it.
pub type ReceiptResult = HashMap<ShardId, Vec<ReceiptTransaction>>;

/// Bridge between the chain and the runtime.
/// Main function is to update state given transactions.
/// Additionally handles validators and block weight computation.
pub trait RuntimeAdapter: Send + Sync {
    /// Initialize state to genesis state and returns StoreUpdate and state root.
    /// StoreUpdate can be discarded if the chain past the genesis.
    fn genesis_state(&self, shard_id: ShardId) -> (StoreUpdate, MerkleHash);

    /// Verify block producer validity and return weight of given block for fork choice rule.
    fn compute_block_weight(
        &self,
        prev_header: &BlockHeader,
        header: &BlockHeader,
    ) -> Result<Weight, Error>;

    /// Epoch block proposers with number of seats they have for given shard.
    /// Returns error if height is outside of known boundaries.
    fn get_epoch_block_proposers(
        &self,
        height: BlockIndex,
    ) -> Result<Vec<(AccountId, u64)>, Box<dyn std::error::Error>>;

    /// Block proposer for given height for the main block. Return error if outside of known boundaries.
    fn get_block_proposer(
        &self,
        height: BlockIndex,
    ) -> Result<AccountId, Box<dyn std::error::Error>>;

    /// Chunk proposer for given height for given shard. Return error if outside of known boundaries.
    fn get_chunk_proposer(
        &self,
        shard_id: ShardId,
        height: BlockIndex,
    ) -> Result<AccountId, Box<dyn std::error::Error>>;

    /// Check validator's signature.
    fn check_validator_signature(&self, account_id: &AccountId, signature: &Signature) -> bool;

    /// Get current number of shards.
    fn num_shards(&self) -> ShardId;

    /// Account Id to Shard Id mapping, given current number of shards.
    fn account_id_to_shard_id(&self, account_id: &AccountId) -> ShardId;

    /// Validate transaction and return transaction information relevant to ordering it in the mempool.
    fn validate_tx(
        &self,
        shard_id: ShardId,
        state_root: MerkleHash,
        transaction: SignedTransaction,
    ) -> Result<ValidTransaction, String>;

    /// Apply transactions to given state root and return store update and new state root.
    /// Also returns transaction result for each transaction and new receipts.
    fn apply_transactions(
        &self,
        shard_id: ShardId,
        merkle_hash: &MerkleHash,
        block_index: BlockIndex,
        prev_block_hash: &CryptoHash,
        receipts: &Vec<Vec<ReceiptTransaction>>,
        transactions: &Vec<SignedTransaction>,
    ) -> Result<
        (WrappedTrieChanges, MerkleHash, Vec<TransactionResult>, ReceiptResult),
        Box<dyn std::error::Error>,
    >;

    /// Query runtime with given `path` and `data`.
    fn query(
        &self,
        state_root: MerkleHash,
        height: BlockIndex,
        path: &str,
        data: &[u8],
    ) -> Result<ABCIQueryResponse, Box<dyn std::error::Error>>;
}

/// The weight is defined as the number of unique validators approving this fork.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Eq, Ord, Serialize, Deserialize)]
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

impl fmt::Display for Weight {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.num)
    }
}

/// The tip of a fork. A handle to the fork ancestry from its leaf in the
/// blockchain tree. References the max height and the latest and previous
/// blocks for convenience and the total weight.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Tip {
    /// Height of the tip (max height of the fork)
    pub height: u64,
    /// Last block pushed to the fork
    pub last_block_hash: CryptoHash,
    /// Previous block
    pub prev_block_hash: CryptoHash,
    /// Total weight on that fork
    pub total_weight: Weight,
}

impl Tip {
    /// Creates a new tip based on provided header.
    pub fn from_header(header: &BlockHeader) -> Tip {
        Tip {
            height: header.height,
            last_block_hash: header.hash(),
            prev_block_hash: header.prev_hash,
            total_weight: header.total_weight,
        }
    }
}

/// Block approval by other block producers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockApproval {
    pub hash: CryptoHash,
    pub signature: Signature,
    pub target: AccountId,
}

impl BlockApproval {
    pub fn new(hash: CryptoHash, signer: &dyn EDSigner, target: AccountId) -> Self {
        let signature = signer.sign(hash.as_ref());
        BlockApproval { hash, signature, target }
    }
}

#[cfg(test)]
mod tests {
    use near_primitives::crypto::signer::InMemorySigner;

    use super::*;

    #[test]
    fn test_block_produce() {
        let genesis = Block::genesis(MerkleHash::default(), Utc::now());
        let signer = Arc::new(InMemorySigner::from_seed("other", "other"));
        let b1 = Block::produce(
            &genesis.header,
            1,
            MerkleHash::default(),
            vec![],
            HashMap::default(),
            vec![],
            signer.clone(),
        );
        assert!(signer.verify(b1.hash().as_ref(), &b1.header.signature));
        assert_eq!(b1.header.total_weight.to_num(), 1);
        let other_signer = Arc::new(InMemorySigner::from_seed("other2", "other2"));
        let approvals: HashMap<usize, Signature> =
            vec![(1, other_signer.sign(b1.hash().as_ref()))].into_iter().collect();
        let b2 = Block::produce(
            &b1.header,
            2,
            MerkleHash::default(),
            vec![],
            approvals,
            vec![],
            signer.clone(),
        );
        assert!(signer.verify(b2.hash().as_ref(), &b2.header.signature));
        assert_eq!(b2.header.total_weight.to_num(), 3);
    }
}
