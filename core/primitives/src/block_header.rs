use borsh::{BorshDeserialize, BorshSerialize};
use chrono::{DateTime, Utc};
use serde::Serialize;

use near_crypto::{KeyType, PublicKey, Signature};

use crate::challenge::ChallengesResult;
use crate::hash::{hash, CryptoHash};
use crate::merkle::combine_hash;
use crate::types::{AccountId, Balance, BlockHeight, EpochId, MerkleHash, ValidatorStake};
use crate::utils::{from_timestamp, to_timestamp};
use crate::validator_signer::ValidatorSigner;

#[derive(BorshSerialize, BorshDeserialize, Serialize, Debug, Clone, Eq, PartialEq)]
pub struct BlockHeaderInnerLite {
    /// Height of this block since the genesis block (height 0).
    pub height: BlockHeight,
    /// Epoch start hash of this block's epoch.
    /// Used for retrieving validator information
    pub epoch_id: EpochId,
    pub next_epoch_id: EpochId,
    /// Root hash of the state at the previous block.
    pub prev_state_root: MerkleHash,
    /// Root of the outcomes of transactions and receipts.
    pub outcome_root: MerkleHash,
    /// Timestamp at which the block was built (number of non-leap-nanoseconds since January 1, 1970 0:00:00 UTC).
    pub timestamp: u64,
    /// Hash of the next epoch block producers set
    pub next_bp_hash: CryptoHash,
    /// Merkle root of block hashes up to the current block.
    pub block_merkle_root: CryptoHash,
}

#[derive(BorshSerialize, BorshDeserialize, Serialize, Debug, Clone, Eq, PartialEq)]
pub struct BlockHeaderInnerRest {
    /// Root hash of the chunk receipts in the given block.
    pub chunk_receipts_root: MerkleHash,
    /// Root hash of the chunk headers in the given block.
    pub chunk_headers_root: MerkleHash,
    /// Root hash of the chunk transactions in the given block.
    pub chunk_tx_root: MerkleHash,
    /// Number of chunks included into the block.
    pub chunks_included: u64,
    /// Root hash of the challenges in the given block.
    pub challenges_root: MerkleHash,
    /// The output of the randomness beacon
    pub random_value: CryptoHash,
    /// Validator proposals.
    pub validator_proposals: Vec<ValidatorStake>,
    /// Mask for new chunks included in the block
    pub chunk_mask: Vec<bool>,
    /// Gas price. Same for all chunks
    pub gas_price: Balance,
    /// Total supply of tokens in the system
    pub total_supply: Balance,
    /// List of challenges result from previous block.
    pub challenges_result: ChallengesResult,

    /// Last block that has full BFT finality
    pub last_final_block: CryptoHash,
    /// Last block that has doomslug finality
    pub last_ds_final_block: CryptoHash,

    /// All the approvals included in this block
    pub approvals: Vec<Option<Signature>>,
}

impl BlockHeaderInnerLite {
    pub fn new(
        height: BlockHeight,
        epoch_id: EpochId,
        next_epoch_id: EpochId,
        prev_state_root: MerkleHash,
        outcome_root: MerkleHash,
        timestamp: u64,
        next_bp_hash: CryptoHash,
        block_merkle_root: CryptoHash,
    ) -> Self {
        Self {
            height,
            epoch_id,
            next_epoch_id,
            prev_state_root,
            outcome_root,
            timestamp,
            next_bp_hash,
            block_merkle_root,
        }
    }

    pub fn hash(&self) -> CryptoHash {
        hash(&self.try_to_vec().expect("Failed to serialize"))
    }
}

impl BlockHeaderInnerRest {
    pub fn new(
        chunk_receipts_root: MerkleHash,
        chunk_headers_root: MerkleHash,
        chunk_tx_root: MerkleHash,
        chunks_included: u64,
        challenges_root: MerkleHash,
        random_value: CryptoHash,
        validator_proposals: Vec<ValidatorStake>,
        chunk_mask: Vec<bool>,
        gas_price: Balance,
        total_supply: Balance,
        challenges_result: ChallengesResult,
        last_final_block: CryptoHash,
        last_ds_final_block: CryptoHash,
        approvals: Vec<Option<Signature>>,
    ) -> Self {
        Self {
            chunk_receipts_root,
            chunk_headers_root,
            chunk_tx_root,
            chunks_included,
            challenges_root,
            random_value,
            validator_proposals,
            chunk_mask,
            gas_price,
            total_supply,
            challenges_result,
            last_final_block,
            last_ds_final_block,
            approvals,
        }
    }

    pub fn hash(&self) -> CryptoHash {
        hash(&self.try_to_vec().expect("Failed to serialize"))
    }
}

/// The part of the block approval that is different for endorsements and skips
#[derive(BorshSerialize, BorshDeserialize, Serialize, Debug, Clone, PartialEq, Eq, Hash)]
pub enum ApprovalInner {
    Endorsement(CryptoHash),
    Skip(BlockHeight),
}

/// Block approval by other block producers with a signature
#[derive(BorshSerialize, BorshDeserialize, Serialize, Debug, Clone, PartialEq, Eq)]
pub struct Approval {
    pub inner: ApprovalInner,
    pub target_height: BlockHeight,
    pub signature: Signature,
    pub account_id: AccountId,
}

/// Block approval by other block producers.
#[derive(BorshSerialize, BorshDeserialize, Serialize, Debug, Clone, PartialEq, Eq)]
pub struct ApprovalMessage {
    pub approval: Approval,
    pub target: AccountId,
}

impl ApprovalInner {
    pub fn new(
        parent_hash: &CryptoHash,
        parent_height: BlockHeight,
        target_height: BlockHeight,
    ) -> Self {
        if target_height == parent_height + 1 {
            ApprovalInner::Endorsement(parent_hash.clone())
        } else {
            ApprovalInner::Skip(parent_height)
        }
    }
}

impl Approval {
    pub fn new(
        parent_hash: CryptoHash,
        parent_height: BlockHeight,
        target_height: BlockHeight,
        signer: &dyn ValidatorSigner,
    ) -> Self {
        let inner = ApprovalInner::new(&parent_hash, parent_height, target_height);
        let signature = signer.sign_approval(&inner, target_height);
        Approval { inner, target_height, signature, account_id: signer.validator_id().clone() }
    }

    pub fn get_data_for_sig(inner: &ApprovalInner, target_height: BlockHeight) -> Vec<u8> {
        [inner.try_to_vec().unwrap().as_ref(), target_height.to_be_bytes().as_ref()].concat()
    }
}

impl ApprovalMessage {
    pub fn new(approval: Approval, target: AccountId) -> Self {
        ApprovalMessage { approval, target }
    }
}

#[derive(BorshSerialize, BorshDeserialize, Serialize, Debug, Clone, Eq, PartialEq)]
#[borsh_init(init)]
pub struct BlockHeader {
    pub prev_hash: CryptoHash,

    /// Inner part of the block header that gets hashed, split into two parts, one that is sent
    ///    to light clients, and the rest
    pub inner_lite: BlockHeaderInnerLite,
    pub inner_rest: BlockHeaderInnerRest,

    /// Signature of the block producer.
    pub signature: Signature,

    /// Cached value of hash for this block.
    #[borsh_skip]
    pub hash: CryptoHash,
}

impl BlockHeader {
    pub fn compute_inner_hash(
        inner_lite: &BlockHeaderInnerLite,
        inner_rest: &BlockHeaderInnerRest,
    ) -> CryptoHash {
        let hash_lite = inner_lite.hash();
        let hash_rest = inner_rest.hash();
        combine_hash(hash_lite, hash_rest)
    }

    pub fn compute_hash(
        prev_hash: CryptoHash,
        inner_lite: &BlockHeaderInnerLite,
        inner_rest: &BlockHeaderInnerRest,
    ) -> CryptoHash {
        let hash_inner = BlockHeader::compute_inner_hash(inner_lite, inner_rest);

        return combine_hash(hash_inner, prev_hash);
    }

    pub fn init(&mut self) {
        self.hash = BlockHeader::compute_hash(self.prev_hash, &self.inner_lite, &self.inner_rest);
    }

    pub fn new(
        height: BlockHeight,
        prev_hash: CryptoHash,
        prev_state_root: MerkleHash,
        chunk_receipts_root: MerkleHash,
        chunk_headers_root: MerkleHash,
        chunk_tx_root: MerkleHash,
        outcome_root: MerkleHash,
        timestamp: u64,
        chunks_included: u64,
        challenges_root: MerkleHash,
        random_value: CryptoHash,
        validator_proposals: Vec<ValidatorStake>,
        chunk_mask: Vec<bool>,
        epoch_id: EpochId,
        next_epoch_id: EpochId,
        gas_price: Balance,
        total_supply: Balance,
        challenges_result: ChallengesResult,
        signer: &dyn ValidatorSigner,
        last_final_block: CryptoHash,
        last_ds_final_block: CryptoHash,
        approvals: Vec<Option<Signature>>,
        next_bp_hash: CryptoHash,
        block_merkle_root: CryptoHash,
    ) -> Self {
        let inner_lite = BlockHeaderInnerLite::new(
            height,
            epoch_id,
            next_epoch_id,
            prev_state_root,
            outcome_root,
            timestamp,
            next_bp_hash,
            block_merkle_root,
        );
        let inner_rest = BlockHeaderInnerRest::new(
            chunk_receipts_root,
            chunk_headers_root,
            chunk_tx_root,
            chunks_included,
            challenges_root,
            random_value,
            validator_proposals,
            chunk_mask,
            gas_price,
            total_supply,
            challenges_result,
            last_final_block,
            last_ds_final_block,
            approvals,
        );
        let (hash, signature) = signer.sign_block_header_parts(prev_hash, &inner_lite, &inner_rest);
        Self { prev_hash, inner_lite, inner_rest, signature, hash }
    }

    pub fn genesis(
        height: BlockHeight,
        state_root: MerkleHash,
        chunk_receipts_root: MerkleHash,
        chunk_headers_root: MerkleHash,
        chunk_tx_root: MerkleHash,
        chunks_included: u64,
        challenges_root: MerkleHash,
        timestamp: DateTime<Utc>,
        initial_gas_price: Balance,
        initial_total_supply: Balance,
        next_bp_hash: CryptoHash,
    ) -> Self {
        let inner_lite = BlockHeaderInnerLite::new(
            height,
            EpochId::default(),
            EpochId::default(),
            state_root,
            CryptoHash::default(),
            to_timestamp(timestamp),
            next_bp_hash,
            CryptoHash::default(),
        );
        let inner_rest = BlockHeaderInnerRest::new(
            chunk_receipts_root,
            chunk_headers_root,
            chunk_tx_root,
            chunks_included,
            challenges_root,
            CryptoHash::default(),
            vec![],
            vec![],
            initial_gas_price,
            initial_total_supply,
            vec![],
            CryptoHash::default(),
            CryptoHash::default(),
            vec![],
        );
        let hash = BlockHeader::compute_hash(CryptoHash::default(), &inner_lite, &inner_rest);
        Self {
            prev_hash: CryptoHash::default(),
            inner_lite,
            inner_rest,
            signature: Signature::empty(KeyType::ED25519),
            hash,
        }
    }

    pub fn hash(&self) -> CryptoHash {
        self.hash
    }

    /// Verifies that given public key produced the block.
    pub fn verify_block_producer(&self, public_key: &PublicKey) -> bool {
        self.signature.verify(self.hash.as_ref(), public_key)
    }

    pub fn timestamp(&self) -> DateTime<Utc> {
        from_timestamp(self.inner_lite.timestamp)
    }

    pub fn num_approvals(&self) -> u64 {
        self.inner_rest.approvals.iter().filter(|x| x.is_some()).count() as u64
    }
}
