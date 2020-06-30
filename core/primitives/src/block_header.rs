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
use crate::version::{ProtocolVersion, PROTOCOL_VERSION};

#[derive(BorshSerialize, BorshDeserialize, Serialize, Debug, Clone, Eq, PartialEq)]
pub struct BlockHeaderInnerLite {
    /// Height of this block.
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

    /// Latest protocol version that this block producer has.
    pub latest_protocol_version: ProtocolVersion,
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
        [inner.try_to_vec().unwrap().as_ref(), target_height.to_le_bytes().as_ref()].concat()
    }
}

impl ApprovalMessage {
    pub fn new(approval: Approval, target: AccountId) -> Self {
        ApprovalMessage { approval, target }
    }
}

#[derive(BorshSerialize, BorshDeserialize, Serialize, Debug, Clone, Eq, PartialEq)]
#[borsh_init(init)]
pub struct BlockHeaderV1 {
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

impl BlockHeaderV1 {
    pub fn init(&mut self) {
        self.hash = BlockHeader::compute_hash(
            self.prev_hash,
            &self.inner_lite.try_to_vec().expect("Failed to serialize"),
            &self.inner_rest.try_to_vec().expect("Failed to serialize"),
        );
    }
}

/// Versioned BlockHeader data structure.
/// For each next version, document what are the changes between versions.
#[derive(BorshSerialize, BorshDeserialize, Serialize, Debug, Clone, Eq, PartialEq)]
pub enum BlockHeader {
    BlockHeaderV1(Box<BlockHeaderV1>),
}

impl BlockHeader {
    pub fn compute_inner_hash(inner_lite: &[u8], inner_rest: &[u8]) -> CryptoHash {
        let hash_lite = hash(inner_lite);
        let hash_rest = hash(inner_rest);
        combine_hash(hash_lite, hash_rest)
    }

    pub fn compute_hash(prev_hash: CryptoHash, inner_lite: &[u8], inner_rest: &[u8]) -> CryptoHash {
        let hash_inner = BlockHeader::compute_inner_hash(inner_lite, inner_rest);

        return combine_hash(hash_inner, prev_hash);
    }

    pub fn new(
        _protocol_version: ProtocolVersion,
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
        let inner_lite = BlockHeaderInnerLite {
            height,
            epoch_id,
            next_epoch_id,
            prev_state_root,
            outcome_root,
            timestamp,
            next_bp_hash,
            block_merkle_root,
        };
        let inner_rest = BlockHeaderInnerRest {
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
            latest_protocol_version: PROTOCOL_VERSION,
        };
        let (hash, signature) = signer.sign_block_header_parts(
            prev_hash,
            &inner_lite.try_to_vec().expect("Failed to serialize"),
            &inner_rest.try_to_vec().expect("Failed to serialize"),
        );
        Self::BlockHeaderV1(Box::new(BlockHeaderV1 {
            prev_hash,
            inner_lite,
            inner_rest,
            signature,
            hash,
        }))
    }

    pub fn genesis(
        genesis_protocol_version: ProtocolVersion,
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
        let inner_lite = BlockHeaderInnerLite {
            height,
            epoch_id: EpochId::default(),
            next_epoch_id: EpochId::default(),
            prev_state_root: state_root,
            outcome_root: CryptoHash::default(),
            timestamp: to_timestamp(timestamp),
            next_bp_hash,
            block_merkle_root: CryptoHash::default(),
        };
        let inner_rest = BlockHeaderInnerRest {
            chunk_receipts_root,
            chunk_headers_root,
            chunk_tx_root,
            chunks_included,
            challenges_root,
            random_value: CryptoHash::default(),
            validator_proposals: vec![],
            chunk_mask: vec![],
            gas_price: initial_gas_price,
            total_supply: initial_total_supply,
            challenges_result: vec![],
            last_final_block: CryptoHash::default(),
            last_ds_final_block: CryptoHash::default(),
            approvals: vec![],
            latest_protocol_version: genesis_protocol_version,
        };
        let hash = BlockHeader::compute_hash(
            CryptoHash::default(),
            &inner_lite.try_to_vec().expect("Failed to serialize"),
            &inner_rest.try_to_vec().expect("Failed to serialize"),
        );
        // Genesis always has v1 of BlockHeader.
        Self::BlockHeaderV1(Box::new(BlockHeaderV1 {
            prev_hash: CryptoHash::default(),
            inner_lite,
            inner_rest,
            signature: Signature::empty(KeyType::ED25519),
            hash,
        }))
    }

    pub fn hash(&self) -> &CryptoHash {
        match self {
            BlockHeader::BlockHeaderV1(header) => &header.hash,
        }
    }

    pub fn prev_hash(&self) -> &CryptoHash {
        match self {
            BlockHeader::BlockHeaderV1(header) => &header.prev_hash,
        }
    }

    pub fn signature(&self) -> &Signature {
        match self {
            BlockHeader::BlockHeaderV1(header) => &header.signature,
        }
    }

    pub fn height(&self) -> BlockHeight {
        match self {
            BlockHeader::BlockHeaderV1(header) => header.inner_lite.height,
        }
    }

    pub fn epoch_id(&self) -> &EpochId {
        match self {
            BlockHeader::BlockHeaderV1(header) => &header.inner_lite.epoch_id,
        }
    }

    pub fn next_epoch_id(&self) -> &EpochId {
        match self {
            BlockHeader::BlockHeaderV1(header) => &header.inner_lite.next_epoch_id,
        }
    }

    pub fn prev_state_root(&self) -> &MerkleHash {
        match self {
            BlockHeader::BlockHeaderV1(header) => &header.inner_lite.prev_state_root,
        }
    }

    pub fn chunk_receipts_root(&self) -> &MerkleHash {
        match self {
            BlockHeader::BlockHeaderV1(header) => &header.inner_rest.chunk_receipts_root,
        }
    }

    pub fn chunk_headers_root(&self) -> &MerkleHash {
        match self {
            BlockHeader::BlockHeaderV1(header) => &header.inner_rest.chunk_headers_root,
        }
    }

    pub fn chunk_tx_root(&self) -> &MerkleHash {
        match self {
            BlockHeader::BlockHeaderV1(header) => &header.inner_rest.chunk_tx_root,
        }
    }

    pub fn chunks_included(&self) -> u64 {
        match self {
            BlockHeader::BlockHeaderV1(header) => header.inner_rest.chunks_included,
        }
    }

    pub fn challenges_root(&self) -> &MerkleHash {
        match self {
            BlockHeader::BlockHeaderV1(header) => &header.inner_rest.challenges_root,
        }
    }

    pub fn outcome_root(&self) -> &MerkleHash {
        match self {
            BlockHeader::BlockHeaderV1(header) => &header.inner_lite.outcome_root,
        }
    }

    pub fn raw_timestamp(&self) -> u64 {
        match self {
            BlockHeader::BlockHeaderV1(header) => header.inner_lite.timestamp,
        }
    }

    pub fn validator_proposals(&self) -> &[ValidatorStake] {
        match self {
            BlockHeader::BlockHeaderV1(header) => &header.inner_rest.validator_proposals,
        }
    }

    pub fn chunk_mask(&self) -> &[bool] {
        match self {
            BlockHeader::BlockHeaderV1(header) => &header.inner_rest.chunk_mask,
        }
    }

    pub fn gas_price(&self) -> Balance {
        match self {
            BlockHeader::BlockHeaderV1(header) => header.inner_rest.gas_price,
        }
    }

    pub fn total_supply(&self) -> Balance {
        match self {
            BlockHeader::BlockHeaderV1(header) => header.inner_rest.total_supply,
        }
    }

    pub fn random_value(&self) -> &CryptoHash {
        match self {
            BlockHeader::BlockHeaderV1(header) => &header.inner_rest.random_value,
        }
    }

    pub fn last_final_block(&self) -> &CryptoHash {
        match self {
            BlockHeader::BlockHeaderV1(header) => &header.inner_rest.last_final_block,
        }
    }

    pub fn last_ds_final_block(&self) -> &CryptoHash {
        match self {
            BlockHeader::BlockHeaderV1(header) => &header.inner_rest.last_ds_final_block,
        }
    }

    pub fn challenges_result(&self) -> &ChallengesResult {
        match self {
            BlockHeader::BlockHeaderV1(header) => &header.inner_rest.challenges_result,
        }
    }

    pub fn next_bp_hash(&self) -> &CryptoHash {
        match self {
            BlockHeader::BlockHeaderV1(header) => &header.inner_lite.next_bp_hash,
        }
    }

    pub fn block_merkle_root(&self) -> &CryptoHash {
        match self {
            BlockHeader::BlockHeaderV1(header) => &header.inner_lite.block_merkle_root,
        }
    }

    pub fn approvals(&self) -> &[Option<Signature>] {
        match self {
            BlockHeader::BlockHeaderV1(header) => &header.inner_rest.approvals,
        }
    }

    /// Verifies that given public key produced the block.
    pub fn verify_block_producer(&self, public_key: &PublicKey) -> bool {
        self.signature().verify(self.hash().as_ref(), public_key)
    }

    pub fn timestamp(&self) -> DateTime<Utc> {
        from_timestamp(self.raw_timestamp())
    }

    pub fn num_approvals(&self) -> u64 {
        self.approvals().iter().filter(|x| x.is_some()).count() as u64
    }

    pub fn latest_protocol_version(&self) -> u32 {
        match self {
            BlockHeader::BlockHeaderV1(header) => header.inner_rest.latest_protocol_version,
        }
    }

    pub fn inner_lite_bytes(&self) -> Vec<u8> {
        match self {
            BlockHeader::BlockHeaderV1(header) => {
                header.inner_lite.try_to_vec().expect("Failed to serialize")
            }
        }
    }

    pub fn inner_rest_bytes(&self) -> Vec<u8> {
        match self {
            BlockHeader::BlockHeaderV1(header) => {
                header.inner_rest.try_to_vec().expect("Failed to serialize")
            }
        }
    }
}
