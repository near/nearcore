use std::collections::HashMap;
use std::sync::Arc;

use chrono::prelude::{DateTime, Utc};

use borsh::{BorshDeserialize, BorshSerialize, Serializable};

use crate::crypto::signature::{verify, PublicKey, Signature, DEFAULT_SIGNATURE};
use crate::crypto::signer::EDSigner;
use crate::hash::{hash, CryptoHash};
use crate::transaction::SignedTransaction;
use crate::types::{BlockIndex, MerkleHash, ValidatorStake};
use crate::utils::{from_timestamp, to_timestamp};

#[derive(BorshSerialize, BorshDeserialize, Debug, Clone, Eq, PartialEq)]
pub struct BlockHeaderInner {
    /// Height of this block since the genesis block (height 0).
    pub height: BlockIndex,
    /// Epoch start hash of the previous epoch.
    /// Used for retrieving validator information
    pub epoch_hash: CryptoHash,
    /// Hash of the block previous to this in the chain.
    pub prev_hash: CryptoHash,
    /// Root hash of the state at the previous block.
    pub prev_state_root: MerkleHash,
    /// Root hash of the transactions in the given block.
    pub tx_root: MerkleHash,
    /// Timestamp at which the block was built.
    pub timestamp: u64,
    /// Approval mask, given current block producers.
    pub approval_mask: Vec<bool>,
    /// Approval signatures.
    pub approval_sigs: Vec<Signature>,
    /// Total weight.
    pub total_weight: Weight,
    /// Validator proposals.
    pub validator_proposals: Vec<ValidatorStake>,
}

impl BlockHeaderInner {
    pub fn new(
        height: BlockIndex,
        epoch_hash: CryptoHash,
        prev_hash: CryptoHash,
        prev_state_root: MerkleHash,
        tx_root: MerkleHash,
        time: DateTime<Utc>,
        approval_mask: Vec<bool>,
        approval_sigs: Vec<Signature>,
        total_weight: Weight,
        validator_proposals: Vec<ValidatorStake>,
    ) -> Self {
        BlockHeaderInner {
            height,
            epoch_hash,
            prev_hash,
            prev_state_root,
            tx_root,
            timestamp: to_timestamp(time),
            approval_mask,
            approval_sigs,
            total_weight,
            validator_proposals,
        }
    }
}

#[derive(BorshSerialize, BorshDeserialize, Debug, Clone, Eq, PartialEq)]
#[borsh_init(init)]
pub struct BlockHeader {
    /// Inner part of the block header that gets hashed.
    pub inner: BlockHeaderInner,

    /// Signature of the block producer.
    pub signature: Signature,

    /// Cached value of hash for this block.
    #[borsh_skip]
    pub hash: CryptoHash,
}

impl BlockHeader {
    pub fn init(&mut self) {
        self.hash = hash(&self.inner.try_to_vec().expect("Failed to serialize"));
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
        signer: Arc<dyn EDSigner>,
    ) -> Self {
        let inner = BlockHeaderInner::new(
            height,
            epoch_hash,
            prev_hash,
            prev_state_root,
            tx_root,
            timestamp,
            approval_mask,
            approval_sigs,
            total_weight,
            validator_proposal,
        );
        let hash = hash(&inner.try_to_vec().expect("Failed to serialize"));
        Self { inner, signature: signer.sign(hash.as_ref()), hash }
    }

    pub fn genesis(state_root: MerkleHash, timestamp: DateTime<Utc>) -> Self {
        let inner = BlockHeaderInner::new(
            0,
            CryptoHash::default(),
            CryptoHash::default(),
            state_root,
            MerkleHash::default(),
            timestamp,
            vec![],
            vec![],
            0.into(),
            vec![],
        );
        let hash = hash(&inner.try_to_vec().expect("Failed to serialize"));
        Self { inner, signature: DEFAULT_SIGNATURE, hash }
    }

    pub fn hash(&self) -> CryptoHash {
        self.hash
    }

    /// Verifies that given public key produced the block.
    pub fn verify_block_producer(&self, public_key: &PublicKey) -> bool {
        verify(self.hash.as_ref(), &self.signature, public_key)
    }

    pub fn timestamp(&self) -> DateTime<Utc> {
        from_timestamp(self.inner.timestamp)
    }
}

#[derive(BorshSerialize, BorshDeserialize, Debug, Clone, Eq, PartialEq)]
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
        epoch_hash: CryptoHash,
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
        let total_weight =
            (prev.inner.total_weight.to_num() + (approval_sigs.len() as u64) + 1).into();
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
                epoch_hash,
                signer,
            ),
            transactions,
        }
    }

    pub fn hash(&self) -> CryptoHash {
        self.header.hash()
    }

    // for tests
    pub fn empty(prev: &BlockHeader, signer: Arc<dyn EDSigner>) -> Self {
        Block::produce(
            prev,
            prev.inner.height + 1,
            prev.inner.prev_state_root,
            prev.inner.epoch_hash,
            vec![],
            HashMap::default(),
            vec![],
            signer,
        )
    }
}

/// The weight is defined as the number of unique validators approving this fork.
#[derive(
    BorshSerialize, BorshDeserialize, Debug, Clone, Copy, PartialEq, PartialOrd, Eq, Ord, Default,
)]
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
