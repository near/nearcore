use crate::block_header::BlockHeader;
use crate::challenge::SlashedValidator;
use crate::types::validator_stake::{ValidatorStake, ValidatorStakeIter};
use crate::types::{AccountId, EpochId, ValidatorStakeV1};
use borsh::{BorshDeserialize, BorshSerialize};
use near_primitives_core::hash::CryptoHash;
use near_primitives_core::types::{Balance, BlockHeight, ProtocolVersion};
use near_schema_checker_lib::ProtocolSchema;
use std::collections::HashMap;

/// Information per each block.
#[derive(
    BorshSerialize, BorshDeserialize, Eq, PartialEq, Clone, Debug, serde::Serialize, ProtocolSchema,
)]
pub enum BlockInfo {
    V1(BlockInfoV1),
    V2(BlockInfoV2),
}

impl Default for BlockInfo {
    fn default() -> Self {
        Self::V2(BlockInfoV2::default())
    }
}

impl BlockInfo {
    pub fn new(
        hash: CryptoHash,
        height: BlockHeight,
        last_finalized_height: BlockHeight,
        last_final_block_hash: CryptoHash,
        prev_hash: CryptoHash,
        proposals: Vec<ValidatorStake>,
        validator_mask: Vec<bool>,
        slashed: Vec<SlashedValidator>,
        total_supply: Balance,
        latest_protocol_version: ProtocolVersion,
        timestamp_nanosec: u64,
    ) -> Self {
        Self::V2(BlockInfoV2 {
            hash,
            height,
            last_finalized_height,
            last_final_block_hash,
            prev_hash,
            proposals,
            chunk_mask: validator_mask,
            latest_protocol_version,
            slashed: slashed
                .into_iter()
                .map(|s| {
                    let slash_state =
                        if s.is_double_sign { SlashState::DoubleSign } else { SlashState::Other };
                    (s.account_id, slash_state)
                })
                .collect(),
            total_supply,
            epoch_first_block: Default::default(),
            epoch_id: Default::default(),
            timestamp_nanosec,
        })
    }

    pub fn from_header(header: &BlockHeader, last_finalized_height: BlockHeight) -> Self {
        // Check that genesis block doesn't have any proposals.
        let prev_validator_proposals: Vec<_> = header.prev_validator_proposals().collect();
        assert!(header.height() > 0 || prev_validator_proposals.is_empty());
        BlockInfo::new(
            *header.hash(),
            header.height(),
            last_finalized_height,
            *header.last_final_block(),
            *header.prev_hash(),
            prev_validator_proposals,
            header.chunk_mask().to_vec(),
            vec![],
            header.total_supply(),
            header.latest_protocol_version(),
            header.raw_timestamp(),
        )
    }

    #[inline]
    pub fn proposals_iter(&self) -> ValidatorStakeIter {
        match self {
            Self::V1(v1) => ValidatorStakeIter::v1(&v1.proposals),
            Self::V2(v2) => ValidatorStakeIter::new(&v2.proposals),
        }
    }

    #[inline]
    pub fn hash(&self) -> &CryptoHash {
        match self {
            Self::V1(v1) => &v1.hash,
            Self::V2(v2) => &v2.hash,
        }
    }

    #[inline]
    pub fn height(&self) -> BlockHeight {
        match self {
            Self::V1(v1) => v1.height,
            Self::V2(v2) => v2.height,
        }
    }

    #[inline]
    pub fn last_finalized_height(&self) -> BlockHeight {
        match self {
            Self::V1(v1) => v1.last_finalized_height,
            Self::V2(v2) => v2.last_finalized_height,
        }
    }

    #[inline]
    pub fn last_final_block_hash(&self) -> &CryptoHash {
        match self {
            Self::V1(v1) => &v1.last_final_block_hash,
            Self::V2(v2) => &v2.last_final_block_hash,
        }
    }

    #[inline]
    pub fn prev_hash(&self) -> &CryptoHash {
        match self {
            Self::V1(v1) => &v1.prev_hash,
            Self::V2(v2) => &v2.prev_hash,
        }
    }

    #[inline]
    pub fn is_genesis(&self) -> bool {
        self.prev_hash() == &CryptoHash::default()
    }

    #[inline]
    pub fn epoch_first_block(&self) -> &CryptoHash {
        match self {
            Self::V1(v1) => &v1.epoch_first_block,
            Self::V2(v2) => &v2.epoch_first_block,
        }
    }

    #[inline]
    pub fn epoch_first_block_mut(&mut self) -> &mut CryptoHash {
        match self {
            Self::V1(v1) => &mut v1.epoch_first_block,
            Self::V2(v2) => &mut v2.epoch_first_block,
        }
    }

    #[inline]
    pub fn epoch_id(&self) -> &EpochId {
        match self {
            Self::V1(v1) => &v1.epoch_id,
            Self::V2(v2) => &v2.epoch_id,
        }
    }

    #[inline]
    pub fn epoch_id_mut(&mut self) -> &mut EpochId {
        match self {
            Self::V1(v1) => &mut v1.epoch_id,
            Self::V2(v2) => &mut v2.epoch_id,
        }
    }

    #[inline]
    pub fn chunk_mask(&self) -> &[bool] {
        match self {
            Self::V1(v1) => &v1.chunk_mask,
            Self::V2(v2) => &v2.chunk_mask,
        }
    }

    #[inline]
    pub fn latest_protocol_version(&self) -> &ProtocolVersion {
        match self {
            Self::V1(v1) => &v1.latest_protocol_version,
            Self::V2(v2) => &v2.latest_protocol_version,
        }
    }

    #[inline]
    pub fn slashed(&self) -> &HashMap<AccountId, SlashState> {
        match self {
            Self::V1(v1) => &v1.slashed,
            Self::V2(v2) => &v2.slashed,
        }
    }

    #[inline]
    pub fn slashed_mut(&mut self) -> &mut HashMap<AccountId, SlashState> {
        match self {
            Self::V1(v1) => &mut v1.slashed,
            Self::V2(v2) => &mut v2.slashed,
        }
    }

    #[inline]
    pub fn total_supply(&self) -> &Balance {
        match self {
            Self::V1(v1) => &v1.total_supply,
            Self::V2(v2) => &v2.total_supply,
        }
    }

    #[inline]
    pub fn timestamp_nanosec(&self) -> &u64 {
        match self {
            Self::V1(v1) => &v1.timestamp_nanosec,
            Self::V2(v2) => &v2.timestamp_nanosec,
        }
    }
}

// V1 -> V2: Use versioned ValidatorStake structure in proposals
#[derive(
    Default,
    BorshSerialize,
    BorshDeserialize,
    Eq,
    PartialEq,
    Clone,
    Debug,
    serde::Serialize,
    ProtocolSchema,
)]
pub struct BlockInfoV2 {
    pub hash: CryptoHash,
    pub height: BlockHeight,
    pub last_finalized_height: BlockHeight,
    pub last_final_block_hash: CryptoHash,
    pub prev_hash: CryptoHash,
    pub epoch_first_block: CryptoHash,
    pub epoch_id: EpochId,
    pub proposals: Vec<ValidatorStake>,
    pub chunk_mask: Vec<bool>,
    /// Latest protocol version this validator observes.
    pub latest_protocol_version: ProtocolVersion,
    /// Validators slashed since the start of epoch or in previous epoch.
    pub slashed: HashMap<AccountId, SlashState>,
    /// Total supply at this block.
    pub total_supply: Balance,
    pub timestamp_nanosec: u64,
}

/// Information per each block.
#[derive(
    Default,
    BorshSerialize,
    BorshDeserialize,
    Eq,
    PartialEq,
    Clone,
    Debug,
    serde::Serialize,
    ProtocolSchema,
)]
pub struct BlockInfoV1 {
    pub hash: CryptoHash,
    pub height: BlockHeight,
    pub last_finalized_height: BlockHeight,
    pub last_final_block_hash: CryptoHash,
    pub prev_hash: CryptoHash,
    pub epoch_first_block: CryptoHash,
    pub epoch_id: EpochId,
    pub proposals: Vec<ValidatorStakeV1>,
    pub chunk_mask: Vec<bool>,
    /// Latest protocol version this validator observes.
    pub latest_protocol_version: ProtocolVersion,
    /// Validators slashed since the start of epoch or in previous epoch.
    pub slashed: HashMap<AccountId, SlashState>,
    /// Total supply at this block.
    pub total_supply: Balance,
    pub timestamp_nanosec: u64,
}

impl BlockInfoV1 {
    pub fn new(
        hash: CryptoHash,
        height: BlockHeight,
        last_finalized_height: BlockHeight,
        last_final_block_hash: CryptoHash,
        prev_hash: CryptoHash,
        proposals: Vec<ValidatorStakeV1>,
        validator_mask: Vec<bool>,
        slashed: Vec<SlashedValidator>,
        total_supply: Balance,
        latest_protocol_version: ProtocolVersion,
        timestamp_nanosec: u64,
    ) -> Self {
        Self {
            hash,
            height,
            last_finalized_height,
            last_final_block_hash,
            prev_hash,
            proposals,
            chunk_mask: validator_mask,
            latest_protocol_version,
            slashed: slashed
                .into_iter()
                .map(|s| {
                    let slash_state =
                        if s.is_double_sign { SlashState::DoubleSign } else { SlashState::Other };
                    (s.account_id, slash_state)
                })
                .collect(),
            total_supply,
            epoch_first_block: Default::default(),
            epoch_id: Default::default(),
            timestamp_nanosec,
        }
    }
}

/// State that a slashed validator can be in.
#[derive(
    BorshSerialize, BorshDeserialize, serde::Serialize, Debug, Clone, PartialEq, Eq, ProtocolSchema,
)]
pub enum SlashState {
    /// Double Sign, will be partially slashed.
    DoubleSign,
    /// Malicious behavior but is already slashed (tokens taken away from account).
    AlreadySlashed,
    /// All other cases (tokens should be entirely slashed),
    Other,
}
