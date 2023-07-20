use crate::challenge::ChallengesResult;
use crate::hash::{hash, CryptoHash};
use crate::merkle::combine_hash;
use crate::network::PeerId;
use crate::types::validator_stake::{ValidatorStake, ValidatorStakeIter, ValidatorStakeV1};
use crate::types::{AccountId, Balance, BlockHeight, EpochId, MerkleHash, NumBlocks};
use crate::utils::{from_timestamp, to_timestamp};
use crate::validator_signer::ValidatorSigner;
use crate::version::{get_protocol_version, ProtocolVersion, PROTOCOL_VERSION};
use borsh::{BorshDeserialize, BorshSerialize};
use chrono::{DateTime, Utc};
use near_crypto::{KeyType, PublicKey, Signature};
use std::sync::Arc;

#[derive(BorshSerialize, BorshDeserialize, serde::Serialize, Debug, Clone, Eq, PartialEq)]
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

#[derive(BorshSerialize, BorshDeserialize, serde::Serialize, Debug, Clone, Eq, PartialEq)]
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
    pub validator_proposals: Vec<ValidatorStakeV1>,
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

/// Remove `chunks_included` from V1
#[derive(BorshSerialize, BorshDeserialize, serde::Serialize, Debug, Clone, Eq, PartialEq)]
pub struct BlockHeaderInnerRestV2 {
    /// Root hash of the chunk receipts in the given block.
    pub chunk_receipts_root: MerkleHash,
    /// Root hash of the chunk headers in the given block.
    pub chunk_headers_root: MerkleHash,
    /// Root hash of the chunk transactions in the given block.
    pub chunk_tx_root: MerkleHash,
    /// Root hash of the challenges in the given block.
    pub challenges_root: MerkleHash,
    /// The output of the randomness beacon
    pub random_value: CryptoHash,
    /// Validator proposals.
    pub validator_proposals: Vec<ValidatorStakeV1>,
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

/// Add `prev_height`
/// Add `block_ordinal`
/// Add `epoch_sync_data_hash`
/// Use new `ValidatorStake` struct
#[derive(BorshSerialize, BorshDeserialize, serde::Serialize, Debug, Clone, Eq, PartialEq)]
pub struct BlockHeaderInnerRestV3 {
    /// Root hash of the chunk receipts in the given block.
    pub chunk_receipts_root: MerkleHash,
    /// Root hash of the chunk headers in the given block.
    pub chunk_headers_root: MerkleHash,
    /// Root hash of the chunk transactions in the given block.
    pub chunk_tx_root: MerkleHash,
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

    /// The ordinal of the Block on the Canonical Chain
    pub block_ordinal: NumBlocks,

    pub prev_height: BlockHeight,

    pub epoch_sync_data_hash: Option<CryptoHash>,

    /// All the approvals included in this block
    pub approvals: Vec<Option<Signature>>,

    /// Latest protocol version that this block producer has.
    pub latest_protocol_version: ProtocolVersion,
}

/// Add `block_body_hash`
#[derive(BorshSerialize, BorshDeserialize, serde::Serialize, Debug, Clone, Eq, PartialEq)]
pub struct BlockHeaderInnerRestV4 {
    /// Hash of block body
    pub block_body_hash: CryptoHash,
    /// Root hash of the chunk receipts in the given block.
    pub chunk_receipts_root: MerkleHash,
    /// Root hash of the chunk headers in the given block.
    pub chunk_headers_root: MerkleHash,
    /// Root hash of the chunk transactions in the given block.
    pub chunk_tx_root: MerkleHash,
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

    /// The ordinal of the Block on the Canonical Chain
    pub block_ordinal: NumBlocks,

    pub prev_height: BlockHeight,

    pub epoch_sync_data_hash: Option<CryptoHash>,

    /// All the approvals included in this block
    pub approvals: Vec<Option<Signature>>,

    /// Latest protocol version that this block producer has.
    pub latest_protocol_version: ProtocolVersion,
}

/// The part of the block approval that is different for endorsements and skips
#[derive(BorshSerialize, BorshDeserialize, serde::Serialize, Debug, Clone, PartialEq, Eq, Hash)]
pub enum ApprovalInner {
    Endorsement(CryptoHash),
    Skip(BlockHeight),
}

/// Block approval by other block producers with a signature
#[derive(BorshSerialize, BorshDeserialize, serde::Serialize, Debug, Clone, PartialEq, Eq)]
pub struct Approval {
    pub inner: ApprovalInner,
    pub target_height: BlockHeight,
    pub signature: Signature,
    pub account_id: AccountId,
}

/// The type of approvals. It is either approval from self or from a peer
#[derive(PartialEq, Eq, Debug)]
pub enum ApprovalType {
    SelfApproval,
    PeerApproval(PeerId),
}

/// Block approval by other block producers.
#[derive(BorshSerialize, BorshDeserialize, serde::Serialize, Debug, Clone, PartialEq, Eq)]
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
            ApprovalInner::Endorsement(*parent_hash)
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

#[derive(BorshSerialize, BorshDeserialize, serde::Serialize, Debug, Clone, Eq, PartialEq)]
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

/// V1 -> V2: Remove `chunks_included` from `inner_reset`
#[derive(BorshSerialize, BorshDeserialize, serde::Serialize, Debug, Clone, Eq, PartialEq)]
#[borsh_init(init)]
pub struct BlockHeaderV2 {
    pub prev_hash: CryptoHash,

    /// Inner part of the block header that gets hashed, split into two parts, one that is sent
    ///    to light clients, and the rest
    pub inner_lite: BlockHeaderInnerLite,
    pub inner_rest: BlockHeaderInnerRestV2,

    /// Signature of the block producer.
    pub signature: Signature,

    /// Cached value of hash for this block.
    #[borsh_skip]
    pub hash: CryptoHash,
}

/// V2 -> V3: Add `prev_height` to `inner_rest` and use new `ValidatorStake`
// Add `block_ordinal` to `inner_rest`
#[derive(BorshSerialize, BorshDeserialize, serde::Serialize, Debug, Clone, Eq, PartialEq)]
#[borsh_init(init)]
pub struct BlockHeaderV3 {
    pub prev_hash: CryptoHash,

    /// Inner part of the block header that gets hashed, split into two parts, one that is sent
    ///    to light clients, and the rest
    pub inner_lite: BlockHeaderInnerLite,
    pub inner_rest: BlockHeaderInnerRestV3,

    /// Signature of the block producer.
    pub signature: Signature,

    /// Cached value of hash for this block.
    #[borsh_skip]
    pub hash: CryptoHash,
}

/// V3 -> V4: Add hash of block body to inner_rest
#[derive(BorshSerialize, BorshDeserialize, serde::Serialize, Debug, Clone, Eq, PartialEq)]
#[borsh_init(init)]
pub struct BlockHeaderV4 {
    pub prev_hash: CryptoHash,

    /// Inner part of the block header that gets hashed, split into two parts, one that is sent
    ///    to light clients, and the rest
    pub inner_lite: BlockHeaderInnerLite,
    pub inner_rest: BlockHeaderInnerRestV4,

    /// Signature of the block producer.
    pub signature: Signature,

    /// Cached value of hash for this block.
    #[borsh_skip]
    pub hash: CryptoHash,
}

impl BlockHeaderV2 {
    pub fn init(&mut self) {
        self.hash = BlockHeader::compute_hash(
            self.prev_hash,
            &self.inner_lite.try_to_vec().expect("Failed to serialize"),
            &self.inner_rest.try_to_vec().expect("Failed to serialize"),
        );
    }
}

impl BlockHeaderV3 {
    pub fn init(&mut self) {
        self.hash = BlockHeader::compute_hash(
            self.prev_hash,
            &self.inner_lite.try_to_vec().expect("Failed to serialize"),
            &self.inner_rest.try_to_vec().expect("Failed to serialize"),
        );
    }
}

impl BlockHeaderV4 {
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
#[derive(BorshSerialize, BorshDeserialize, serde::Serialize, Debug, Clone, Eq, PartialEq)]
pub enum BlockHeader {
    BlockHeaderV1(Arc<BlockHeaderV1>),
    BlockHeaderV2(Arc<BlockHeaderV2>),
    BlockHeaderV3(Arc<BlockHeaderV3>),
    BlockHeaderV4(Arc<BlockHeaderV4>),
}

impl BlockHeader {
    pub fn compute_inner_hash(inner_lite: &[u8], inner_rest: &[u8]) -> CryptoHash {
        let hash_lite = hash(inner_lite);
        let hash_rest = hash(inner_rest);
        combine_hash(&hash_lite, &hash_rest)
    }

    pub fn compute_hash(prev_hash: CryptoHash, inner_lite: &[u8], inner_rest: &[u8]) -> CryptoHash {
        let hash_inner = BlockHeader::compute_inner_hash(inner_lite, inner_rest);

        combine_hash(&hash_inner, &prev_hash)
    }

    pub fn new(
        this_epoch_protocol_version: ProtocolVersion,
        next_epoch_protocol_version: ProtocolVersion,
        height: BlockHeight,
        prev_hash: CryptoHash,
        #[cfg(feature = "protocol_feature_block_header_v4")] block_body_hash: CryptoHash,
        prev_state_root: MerkleHash,
        chunk_receipts_root: MerkleHash,
        chunk_headers_root: MerkleHash,
        chunk_tx_root: MerkleHash,
        outcome_root: MerkleHash,
        timestamp: u64,
        challenges_root: MerkleHash,
        random_value: CryptoHash,
        validator_proposals: Vec<ValidatorStake>,
        chunk_mask: Vec<bool>,
        block_ordinal: NumBlocks,
        epoch_id: EpochId,
        next_epoch_id: EpochId,
        gas_price: Balance,
        total_supply: Balance,
        challenges_result: ChallengesResult,
        signer: &dyn ValidatorSigner,
        last_final_block: CryptoHash,
        last_ds_final_block: CryptoHash,
        epoch_sync_data_hash: Option<CryptoHash>,
        approvals: Vec<Option<Signature>>,
        next_bp_hash: CryptoHash,
        block_merkle_root: CryptoHash,
        prev_height: BlockHeight,
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
        // This function still preserves code for old block header versions. These code are no longer
        // used in production, but we still have features tests in the code that uses them.
        // So we still keep the old code here.
        let last_header_v2_version =
            crate::version::ProtocolFeature::BlockHeaderV3.protocol_version() - 1;
        // Previously we passed next_epoch_protocol_version here, which is incorrect, but we need
        // to preserve this for archival nodes
        if next_epoch_protocol_version <= 29 {
            let chunks_included = chunk_mask.iter().map(|val| *val as u64).sum::<u64>();
            let inner_rest = BlockHeaderInnerRest {
                chunk_receipts_root,
                chunk_headers_root,
                chunk_tx_root,
                chunks_included,
                challenges_root,
                random_value,
                validator_proposals: validator_proposals.into_iter().map(|v| v.into_v1()).collect(),
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
            Self::BlockHeaderV1(Arc::new(BlockHeaderV1 {
                prev_hash,
                inner_lite,
                inner_rest,
                signature,
                hash,
            }))
        } else if this_epoch_protocol_version <= last_header_v2_version {
            let inner_rest = BlockHeaderInnerRestV2 {
                chunk_receipts_root,
                chunk_headers_root,
                chunk_tx_root,
                challenges_root,
                random_value,
                validator_proposals: validator_proposals.into_iter().map(|v| v.into_v1()).collect(),
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
            Self::BlockHeaderV2(Arc::new(BlockHeaderV2 {
                prev_hash,
                inner_lite,
                inner_rest,
                signature,
                hash,
            }))
        } else {
            #[cfg(feature = "protocol_feature_block_header_v4")]
            if crate::checked_feature!(
                "protocol_feature_block_header_v4",
                BlockHeaderV4,
                this_epoch_protocol_version
            ) {
                let inner_rest = BlockHeaderInnerRestV4 {
                    block_body_hash,
                    chunk_receipts_root,
                    chunk_headers_root,
                    chunk_tx_root,
                    challenges_root,
                    random_value,
                    validator_proposals,
                    chunk_mask,
                    gas_price,
                    block_ordinal,
                    total_supply,
                    challenges_result,
                    last_final_block,
                    last_ds_final_block,
                    prev_height,
                    epoch_sync_data_hash,
                    approvals,
                    latest_protocol_version: get_protocol_version(next_epoch_protocol_version),
                };
                let (hash, signature) = signer.sign_block_header_parts(
                    prev_hash,
                    &inner_lite.try_to_vec().expect("Failed to serialize"),
                    &inner_rest.try_to_vec().expect("Failed to serialize"),
                );
                return Self::BlockHeaderV4(Arc::new(BlockHeaderV4 {
                    prev_hash,
                    inner_lite,
                    inner_rest,
                    signature,
                    hash,
                }));
            }
            let inner_rest = BlockHeaderInnerRestV3 {
                chunk_receipts_root,
                chunk_headers_root,
                chunk_tx_root,
                challenges_root,
                random_value,
                validator_proposals,
                chunk_mask,
                gas_price,
                block_ordinal,
                total_supply,
                challenges_result,
                last_final_block,
                last_ds_final_block,
                prev_height,
                epoch_sync_data_hash,
                approvals,
                latest_protocol_version: get_protocol_version(next_epoch_protocol_version),
            };
            let (hash, signature) = signer.sign_block_header_parts(
                prev_hash,
                &inner_lite.try_to_vec().expect("Failed to serialize"),
                &inner_rest.try_to_vec().expect("Failed to serialize"),
            );
            Self::BlockHeaderV3(Arc::new(BlockHeaderV3 {
                prev_hash,
                inner_lite,
                inner_rest,
                signature,
                hash,
            }))
        }
    }

    pub fn genesis(
        genesis_protocol_version: ProtocolVersion,
        height: BlockHeight,
        state_root: MerkleHash,
        #[cfg(feature = "protocol_feature_block_header_v4")] block_body_hash: CryptoHash,
        chunk_receipts_root: MerkleHash,
        chunk_headers_root: MerkleHash,
        chunk_tx_root: MerkleHash,
        num_shards: u64,
        challenges_root: MerkleHash,
        timestamp: DateTime<Utc>,
        initial_gas_price: Balance,
        initial_total_supply: Balance,
        next_bp_hash: CryptoHash,
    ) -> Self {
        let chunks_included = if height == 0 { num_shards } else { 0 };
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
        let last_header_v2_version =
            crate::version::ProtocolFeature::BlockHeaderV3.protocol_version() - 1;
        if genesis_protocol_version <= 29 {
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
            Self::BlockHeaderV1(Arc::new(BlockHeaderV1 {
                prev_hash: CryptoHash::default(),
                inner_lite,
                inner_rest,
                signature: Signature::empty(KeyType::ED25519),
                hash,
            }))
        } else if genesis_protocol_version <= last_header_v2_version {
            let inner_rest = BlockHeaderInnerRestV2 {
                chunk_receipts_root,
                chunk_headers_root,
                chunk_tx_root,
                challenges_root,
                random_value: CryptoHash::default(),
                validator_proposals: vec![],
                chunk_mask: vec![true; chunks_included as usize],
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
            Self::BlockHeaderV2(Arc::new(BlockHeaderV2 {
                prev_hash: CryptoHash::default(),
                inner_lite,
                inner_rest,
                signature: Signature::empty(KeyType::ED25519),
                hash,
            }))
        } else {
            #[cfg(feature = "protocol_feature_block_header_v4")]
            if crate::checked_feature!(
                "protocol_feature_block_header_v4",
                BlockHeaderV4,
                genesis_protocol_version
            ) {
                let inner_rest = BlockHeaderInnerRestV4 {
                    chunk_receipts_root,
                    chunk_headers_root,
                    chunk_tx_root,
                    challenges_root,
                    block_body_hash,
                    random_value: CryptoHash::default(),
                    validator_proposals: vec![],
                    chunk_mask: vec![true; chunks_included as usize],
                    block_ordinal: 1, // It is guaranteed that Chain has the only Block which is Genesis
                    gas_price: initial_gas_price,
                    total_supply: initial_total_supply,
                    challenges_result: vec![],
                    last_final_block: CryptoHash::default(),
                    last_ds_final_block: CryptoHash::default(),
                    prev_height: 0,
                    epoch_sync_data_hash: None, // Epoch Sync cannot be executed up to Genesis
                    approvals: vec![],
                    latest_protocol_version: genesis_protocol_version,
                };
                let hash = BlockHeader::compute_hash(
                    CryptoHash::default(),
                    &inner_lite.try_to_vec().expect("Failed to serialize"),
                    &inner_rest.try_to_vec().expect("Failed to serialize"),
                );
                return Self::BlockHeaderV4(Arc::new(BlockHeaderV4 {
                    prev_hash: CryptoHash::default(),
                    inner_lite,
                    inner_rest,
                    signature: Signature::empty(KeyType::ED25519),
                    hash,
                }));
            }
            let inner_rest = BlockHeaderInnerRestV3 {
                chunk_receipts_root,
                chunk_headers_root,
                chunk_tx_root,
                challenges_root,
                random_value: CryptoHash::default(),
                validator_proposals: vec![],
                chunk_mask: vec![true; chunks_included as usize],
                block_ordinal: 1, // It is guaranteed that Chain has the only Block which is Genesis
                gas_price: initial_gas_price,
                total_supply: initial_total_supply,
                challenges_result: vec![],
                last_final_block: CryptoHash::default(),
                last_ds_final_block: CryptoHash::default(),
                prev_height: 0,
                epoch_sync_data_hash: None, // Epoch Sync cannot be executed up to Genesis
                approvals: vec![],
                latest_protocol_version: genesis_protocol_version,
            };
            let hash = BlockHeader::compute_hash(
                CryptoHash::default(),
                &inner_lite.try_to_vec().expect("Failed to serialize"),
                &inner_rest.try_to_vec().expect("Failed to serialize"),
            );
            Self::BlockHeaderV3(Arc::new(BlockHeaderV3 {
                prev_hash: CryptoHash::default(),
                inner_lite,
                inner_rest,
                signature: Signature::empty(KeyType::ED25519),
                hash,
            }))
        }
    }

    #[inline]
    pub fn hash(&self) -> &CryptoHash {
        match self {
            BlockHeader::BlockHeaderV1(header) => &header.hash,
            BlockHeader::BlockHeaderV2(header) => &header.hash,
            BlockHeader::BlockHeaderV3(header) => &header.hash,
            BlockHeader::BlockHeaderV4(header) => &header.hash,
        }
    }

    #[inline]
    pub fn prev_hash(&self) -> &CryptoHash {
        match self {
            BlockHeader::BlockHeaderV1(header) => &header.prev_hash,
            BlockHeader::BlockHeaderV2(header) => &header.prev_hash,
            BlockHeader::BlockHeaderV3(header) => &header.prev_hash,
            BlockHeader::BlockHeaderV4(header) => &header.prev_hash,
        }
    }

    #[inline]
    pub fn signature(&self) -> &Signature {
        match self {
            BlockHeader::BlockHeaderV1(header) => &header.signature,
            BlockHeader::BlockHeaderV2(header) => &header.signature,
            BlockHeader::BlockHeaderV3(header) => &header.signature,
            BlockHeader::BlockHeaderV4(header) => &header.signature,
        }
    }

    #[inline]
    pub fn height(&self) -> BlockHeight {
        match self {
            BlockHeader::BlockHeaderV1(header) => header.inner_lite.height,
            BlockHeader::BlockHeaderV2(header) => header.inner_lite.height,
            BlockHeader::BlockHeaderV3(header) => header.inner_lite.height,
            BlockHeader::BlockHeaderV4(header) => header.inner_lite.height,
        }
    }

    #[inline]
    pub fn prev_height(&self) -> Option<BlockHeight> {
        match self {
            BlockHeader::BlockHeaderV1(_) => None,
            BlockHeader::BlockHeaderV2(_) => None,
            BlockHeader::BlockHeaderV3(header) => Some(header.inner_rest.prev_height),
            BlockHeader::BlockHeaderV4(header) => Some(header.inner_rest.prev_height),
        }
    }

    #[inline]
    pub fn epoch_id(&self) -> &EpochId {
        match self {
            BlockHeader::BlockHeaderV1(header) => &header.inner_lite.epoch_id,
            BlockHeader::BlockHeaderV2(header) => &header.inner_lite.epoch_id,
            BlockHeader::BlockHeaderV3(header) => &header.inner_lite.epoch_id,
            BlockHeader::BlockHeaderV4(header) => &header.inner_lite.epoch_id,
        }
    }

    #[inline]
    pub fn next_epoch_id(&self) -> &EpochId {
        match self {
            BlockHeader::BlockHeaderV1(header) => &header.inner_lite.next_epoch_id,
            BlockHeader::BlockHeaderV2(header) => &header.inner_lite.next_epoch_id,
            BlockHeader::BlockHeaderV3(header) => &header.inner_lite.next_epoch_id,
            BlockHeader::BlockHeaderV4(header) => &header.inner_lite.next_epoch_id,
        }
    }

    #[inline]
    pub fn prev_state_root(&self) -> &MerkleHash {
        match self {
            BlockHeader::BlockHeaderV1(header) => &header.inner_lite.prev_state_root,
            BlockHeader::BlockHeaderV2(header) => &header.inner_lite.prev_state_root,
            BlockHeader::BlockHeaderV3(header) => &header.inner_lite.prev_state_root,
            BlockHeader::BlockHeaderV4(header) => &header.inner_lite.prev_state_root,
        }
    }

    #[inline]
    pub fn chunk_receipts_root(&self) -> &MerkleHash {
        match self {
            BlockHeader::BlockHeaderV1(header) => &header.inner_rest.chunk_receipts_root,
            BlockHeader::BlockHeaderV2(header) => &header.inner_rest.chunk_receipts_root,
            BlockHeader::BlockHeaderV3(header) => &header.inner_rest.chunk_receipts_root,
            BlockHeader::BlockHeaderV4(header) => &header.inner_rest.chunk_receipts_root,
        }
    }

    #[inline]
    pub fn chunk_headers_root(&self) -> &MerkleHash {
        match self {
            BlockHeader::BlockHeaderV1(header) => &header.inner_rest.chunk_headers_root,
            BlockHeader::BlockHeaderV2(header) => &header.inner_rest.chunk_headers_root,
            BlockHeader::BlockHeaderV3(header) => &header.inner_rest.chunk_headers_root,
            BlockHeader::BlockHeaderV4(header) => &header.inner_rest.chunk_headers_root,
        }
    }

    #[inline]
    pub fn chunk_tx_root(&self) -> &MerkleHash {
        match self {
            BlockHeader::BlockHeaderV1(header) => &header.inner_rest.chunk_tx_root,
            BlockHeader::BlockHeaderV2(header) => &header.inner_rest.chunk_tx_root,
            BlockHeader::BlockHeaderV3(header) => &header.inner_rest.chunk_tx_root,
            BlockHeader::BlockHeaderV4(header) => &header.inner_rest.chunk_tx_root,
        }
    }

    pub fn chunks_included(&self) -> u64 {
        match self {
            BlockHeader::BlockHeaderV1(header) => header.inner_rest.chunks_included,
            BlockHeader::BlockHeaderV2(header) => {
                header.inner_rest.chunk_mask.iter().map(|&x| u64::from(x)).sum::<u64>()
            }
            BlockHeader::BlockHeaderV3(header) => {
                header.inner_rest.chunk_mask.iter().map(|&x| u64::from(x)).sum::<u64>()
            }
            BlockHeader::BlockHeaderV4(header) => {
                header.inner_rest.chunk_mask.iter().map(|&x| u64::from(x)).sum::<u64>()
            }
        }
    }

    #[inline]
    pub fn challenges_root(&self) -> &MerkleHash {
        match self {
            BlockHeader::BlockHeaderV1(header) => &header.inner_rest.challenges_root,
            BlockHeader::BlockHeaderV2(header) => &header.inner_rest.challenges_root,
            BlockHeader::BlockHeaderV3(header) => &header.inner_rest.challenges_root,
            BlockHeader::BlockHeaderV4(header) => &header.inner_rest.challenges_root,
        }
    }

    #[inline]
    pub fn outcome_root(&self) -> &MerkleHash {
        match self {
            BlockHeader::BlockHeaderV1(header) => &header.inner_lite.outcome_root,
            BlockHeader::BlockHeaderV2(header) => &header.inner_lite.outcome_root,
            BlockHeader::BlockHeaderV3(header) => &header.inner_lite.outcome_root,
            BlockHeader::BlockHeaderV4(header) => &header.inner_lite.outcome_root,
        }
    }

    #[inline]
    pub fn block_body_hash(&self) -> Option<CryptoHash> {
        match self {
            BlockHeader::BlockHeaderV1(_) => None,
            BlockHeader::BlockHeaderV2(_) => None,
            BlockHeader::BlockHeaderV3(_) => None,
            BlockHeader::BlockHeaderV4(header) => Some(header.inner_rest.block_body_hash),
        }
    }

    #[inline]
    pub fn raw_timestamp(&self) -> u64 {
        match self {
            BlockHeader::BlockHeaderV1(header) => header.inner_lite.timestamp,
            BlockHeader::BlockHeaderV2(header) => header.inner_lite.timestamp,
            BlockHeader::BlockHeaderV3(header) => header.inner_lite.timestamp,
            BlockHeader::BlockHeaderV4(header) => header.inner_lite.timestamp,
        }
    }

    #[inline]
    pub fn validator_proposals(&self) -> ValidatorStakeIter {
        match self {
            BlockHeader::BlockHeaderV1(header) => {
                ValidatorStakeIter::v1(&header.inner_rest.validator_proposals)
            }
            BlockHeader::BlockHeaderV2(header) => {
                ValidatorStakeIter::v1(&header.inner_rest.validator_proposals)
            }
            BlockHeader::BlockHeaderV3(header) => {
                ValidatorStakeIter::new(&header.inner_rest.validator_proposals)
            }
            BlockHeader::BlockHeaderV4(header) => {
                ValidatorStakeIter::new(&header.inner_rest.validator_proposals)
            }
        }
    }

    #[inline]
    pub fn chunk_mask(&self) -> &[bool] {
        match self {
            BlockHeader::BlockHeaderV1(header) => &header.inner_rest.chunk_mask,
            BlockHeader::BlockHeaderV2(header) => &header.inner_rest.chunk_mask,
            BlockHeader::BlockHeaderV3(header) => &header.inner_rest.chunk_mask,
            BlockHeader::BlockHeaderV4(header) => &header.inner_rest.chunk_mask,
        }
    }

    #[inline]
    pub fn block_ordinal(&self) -> NumBlocks {
        match self {
            BlockHeader::BlockHeaderV1(_) => 0, // not applicable
            BlockHeader::BlockHeaderV2(_) => 0, // not applicable
            BlockHeader::BlockHeaderV3(header) => header.inner_rest.block_ordinal,
            BlockHeader::BlockHeaderV4(header) => header.inner_rest.block_ordinal,
        }
    }

    #[inline]
    pub fn gas_price(&self) -> Balance {
        match self {
            BlockHeader::BlockHeaderV1(header) => header.inner_rest.gas_price,
            BlockHeader::BlockHeaderV2(header) => header.inner_rest.gas_price,
            BlockHeader::BlockHeaderV3(header) => header.inner_rest.gas_price,
            BlockHeader::BlockHeaderV4(header) => header.inner_rest.gas_price,
        }
    }

    #[inline]
    pub fn total_supply(&self) -> Balance {
        match self {
            BlockHeader::BlockHeaderV1(header) => header.inner_rest.total_supply,
            BlockHeader::BlockHeaderV2(header) => header.inner_rest.total_supply,
            BlockHeader::BlockHeaderV3(header) => header.inner_rest.total_supply,
            BlockHeader::BlockHeaderV4(header) => header.inner_rest.total_supply,
        }
    }

    #[inline]
    pub fn random_value(&self) -> &CryptoHash {
        match self {
            BlockHeader::BlockHeaderV1(header) => &header.inner_rest.random_value,
            BlockHeader::BlockHeaderV2(header) => &header.inner_rest.random_value,
            BlockHeader::BlockHeaderV3(header) => &header.inner_rest.random_value,
            BlockHeader::BlockHeaderV4(header) => &header.inner_rest.random_value,
        }
    }

    #[inline]
    pub fn last_final_block(&self) -> &CryptoHash {
        match self {
            BlockHeader::BlockHeaderV1(header) => &header.inner_rest.last_final_block,
            BlockHeader::BlockHeaderV2(header) => &header.inner_rest.last_final_block,
            BlockHeader::BlockHeaderV3(header) => &header.inner_rest.last_final_block,
            BlockHeader::BlockHeaderV4(header) => &header.inner_rest.last_final_block,
        }
    }

    #[inline]
    pub fn last_ds_final_block(&self) -> &CryptoHash {
        match self {
            BlockHeader::BlockHeaderV1(header) => &header.inner_rest.last_ds_final_block,
            BlockHeader::BlockHeaderV2(header) => &header.inner_rest.last_ds_final_block,
            BlockHeader::BlockHeaderV3(header) => &header.inner_rest.last_ds_final_block,
            BlockHeader::BlockHeaderV4(header) => &header.inner_rest.last_ds_final_block,
        }
    }

    #[inline]
    pub fn challenges_result(&self) -> &ChallengesResult {
        match self {
            BlockHeader::BlockHeaderV1(header) => &header.inner_rest.challenges_result,
            BlockHeader::BlockHeaderV2(header) => &header.inner_rest.challenges_result,
            BlockHeader::BlockHeaderV3(header) => &header.inner_rest.challenges_result,
            BlockHeader::BlockHeaderV4(header) => &header.inner_rest.challenges_result,
        }
    }

    #[inline]
    pub fn next_bp_hash(&self) -> &CryptoHash {
        match self {
            BlockHeader::BlockHeaderV1(header) => &header.inner_lite.next_bp_hash,
            BlockHeader::BlockHeaderV2(header) => &header.inner_lite.next_bp_hash,
            BlockHeader::BlockHeaderV3(header) => &header.inner_lite.next_bp_hash,
            BlockHeader::BlockHeaderV4(header) => &header.inner_lite.next_bp_hash,
        }
    }

    #[inline]
    pub fn block_merkle_root(&self) -> &CryptoHash {
        match self {
            BlockHeader::BlockHeaderV1(header) => &header.inner_lite.block_merkle_root,
            BlockHeader::BlockHeaderV2(header) => &header.inner_lite.block_merkle_root,
            BlockHeader::BlockHeaderV3(header) => &header.inner_lite.block_merkle_root,
            BlockHeader::BlockHeaderV4(header) => &header.inner_lite.block_merkle_root,
        }
    }

    #[inline]
    pub fn epoch_sync_data_hash(&self) -> Option<CryptoHash> {
        match self {
            BlockHeader::BlockHeaderV1(_) => None,
            BlockHeader::BlockHeaderV2(_) => None,
            BlockHeader::BlockHeaderV3(header) => header.inner_rest.epoch_sync_data_hash,
            BlockHeader::BlockHeaderV4(header) => header.inner_rest.epoch_sync_data_hash,
        }
    }

    #[inline]
    pub fn approvals(&self) -> &[Option<Signature>] {
        match self {
            BlockHeader::BlockHeaderV1(header) => &header.inner_rest.approvals,
            BlockHeader::BlockHeaderV2(header) => &header.inner_rest.approvals,
            BlockHeader::BlockHeaderV3(header) => &header.inner_rest.approvals,
            BlockHeader::BlockHeaderV4(header) => &header.inner_rest.approvals,
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

    pub fn verify_chunks_included(&self) -> bool {
        match self {
            BlockHeader::BlockHeaderV1(header) => {
                header.inner_rest.chunk_mask.iter().map(|&x| u64::from(x)).sum::<u64>()
                    == header.inner_rest.chunks_included
            }
            BlockHeader::BlockHeaderV2(_header) => true,
            BlockHeader::BlockHeaderV3(_header) => true,
            BlockHeader::BlockHeaderV4(_header) => true,
        }
    }

    #[inline]
    pub fn latest_protocol_version(&self) -> u32 {
        match self {
            BlockHeader::BlockHeaderV1(header) => header.inner_rest.latest_protocol_version,
            BlockHeader::BlockHeaderV2(header) => header.inner_rest.latest_protocol_version,
            BlockHeader::BlockHeaderV3(header) => header.inner_rest.latest_protocol_version,
            BlockHeader::BlockHeaderV4(header) => header.inner_rest.latest_protocol_version,
        }
    }

    pub fn inner_lite_bytes(&self) -> Vec<u8> {
        match self {
            BlockHeader::BlockHeaderV1(header) => {
                header.inner_lite.try_to_vec().expect("Failed to serialize")
            }
            BlockHeader::BlockHeaderV2(header) => {
                header.inner_lite.try_to_vec().expect("Failed to serialize")
            }
            BlockHeader::BlockHeaderV3(header) => {
                header.inner_lite.try_to_vec().expect("Failed to serialize")
            }
            BlockHeader::BlockHeaderV4(header) => {
                header.inner_lite.try_to_vec().expect("Failed to serialize")
            }
        }
    }

    pub fn inner_rest_bytes(&self) -> Vec<u8> {
        match self {
            BlockHeader::BlockHeaderV1(header) => {
                header.inner_rest.try_to_vec().expect("Failed to serialize")
            }
            BlockHeader::BlockHeaderV2(header) => {
                header.inner_rest.try_to_vec().expect("Failed to serialize")
            }
            BlockHeader::BlockHeaderV3(header) => {
                header.inner_rest.try_to_vec().expect("Failed to serialize")
            }
            BlockHeader::BlockHeaderV4(header) => {
                header.inner_rest.try_to_vec().expect("Failed to serialize")
            }
        }
    }
}
