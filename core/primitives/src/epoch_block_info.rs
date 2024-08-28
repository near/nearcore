use crate::block_header::BlockHeader;
use crate::challenge::SlashedValidator;
use crate::stateless_validation::chunk_endorsements_bitmap::ChunkEndorsementsBitmap;
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
    V3(BlockInfoV3),
}

impl Default for BlockInfo {
    fn default() -> Self {
        Self::V3(BlockInfoV3::default())
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
        chunk_endorsements: Option<ChunkEndorsementsBitmap>,
    ) -> Self {
        if let Some(chunk_endorsements) = chunk_endorsements {
            Self::V3(BlockInfoV3 {
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
                        let slash_state = if s.is_double_sign {
                            SlashState::DoubleSign
                        } else {
                            SlashState::Other
                        };
                        (s.account_id, slash_state)
                    })
                    .collect(),
                total_supply,
                epoch_first_block: Default::default(),
                epoch_id: Default::default(),
                timestamp_nanosec,
                chunk_endorsements,
            })
        } else {
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
                        let slash_state = if s.is_double_sign {
                            SlashState::DoubleSign
                        } else {
                            SlashState::Other
                        };
                        (s.account_id, slash_state)
                    })
                    .collect(),
                total_supply,
                epoch_first_block: Default::default(),
                epoch_id: Default::default(),
                timestamp_nanosec,
            })
        }
    }

    pub fn from_header(header: &BlockHeader, last_finalized_height: BlockHeight) -> Self {
        BlockInfo::new(
            *header.hash(),
            header.height(),
            last_finalized_height,
            *header.last_final_block(),
            *header.prev_hash(),
            header.prev_validator_proposals().collect(),
            header.chunk_mask().to_vec(),
            vec![],
            header.total_supply(),
            header.latest_protocol_version(),
            header.raw_timestamp(),
            header.chunk_endorsements().cloned(),
        )
    }

    // TODO(#11900): Remove this and use `from_header` only, when endorsements bitmap is added to the BlockHeader.
    pub fn from_header_and_endorsements(
        header: &BlockHeader,
        last_finalized_height: BlockHeight,
        chunk_endorsements: Option<ChunkEndorsementsBitmap>,
    ) -> Self {
        BlockInfo::new(
            *header.hash(),
            header.height(),
            last_finalized_height,
            *header.last_final_block(),
            *header.prev_hash(),
            header.prev_validator_proposals().collect(),
            header.chunk_mask().to_vec(),
            vec![],
            header.total_supply(),
            header.latest_protocol_version(),
            header.raw_timestamp(),
            header.chunk_endorsements().cloned().or(chunk_endorsements),
        )
    }

    #[inline]
    pub fn proposals_iter(&self) -> ValidatorStakeIter {
        match self {
            Self::V1(info) => ValidatorStakeIter::v1(&info.proposals),
            Self::V2(info) => ValidatorStakeIter::new(&info.proposals),
            Self::V3(info) => ValidatorStakeIter::new(&info.proposals),
        }
    }

    #[inline]
    pub fn hash(&self) -> &CryptoHash {
        match self {
            Self::V1(info) => &info.hash,
            Self::V2(info) => &info.hash,
            Self::V3(info) => &info.hash,
        }
    }

    #[inline]
    pub fn height(&self) -> BlockHeight {
        match self {
            Self::V1(info) => info.height,
            Self::V2(info) => info.height,
            Self::V3(info) => info.height,
        }
    }

    #[inline]
    pub fn last_finalized_height(&self) -> BlockHeight {
        match self {
            Self::V1(info) => info.last_finalized_height,
            Self::V2(info) => info.last_finalized_height,
            Self::V3(info) => info.last_finalized_height,
        }
    }

    #[inline]
    pub fn last_final_block_hash(&self) -> &CryptoHash {
        match self {
            Self::V1(info) => &info.last_final_block_hash,
            Self::V2(info) => &info.last_final_block_hash,
            Self::V3(info) => &info.last_final_block_hash,
        }
    }

    #[inline]
    pub fn prev_hash(&self) -> &CryptoHash {
        match self {
            Self::V1(info) => &info.prev_hash,
            Self::V2(info) => &info.prev_hash,
            Self::V3(info) => &info.prev_hash,
        }
    }

    #[inline]
    pub fn is_genesis(&self) -> bool {
        self.prev_hash() == &CryptoHash::default()
    }

    #[inline]
    pub fn epoch_first_block(&self) -> &CryptoHash {
        match self {
            Self::V1(info) => &info.epoch_first_block,
            Self::V2(info) => &info.epoch_first_block,
            Self::V3(info) => &info.epoch_first_block,
        }
    }

    #[inline]
    pub fn epoch_first_block_mut(&mut self) -> &mut CryptoHash {
        match self {
            Self::V1(info) => &mut info.epoch_first_block,
            Self::V2(info) => &mut info.epoch_first_block,
            Self::V3(info) => &mut info.epoch_first_block,
        }
    }

    #[inline]
    pub fn epoch_id(&self) -> &EpochId {
        match self {
            Self::V1(info) => &info.epoch_id,
            Self::V2(info) => &info.epoch_id,
            Self::V3(info) => &info.epoch_id,
        }
    }

    #[inline]
    pub fn epoch_id_mut(&mut self) -> &mut EpochId {
        match self {
            Self::V1(info) => &mut info.epoch_id,
            Self::V2(info) => &mut info.epoch_id,
            Self::V3(info) => &mut info.epoch_id,
        }
    }

    #[inline]
    pub fn chunk_mask(&self) -> &[bool] {
        match self {
            Self::V1(info) => &info.chunk_mask,
            Self::V2(info) => &info.chunk_mask,
            Self::V3(info) => &info.chunk_mask,
        }
    }

    #[inline]
    pub fn latest_protocol_version(&self) -> &ProtocolVersion {
        match self {
            Self::V1(info) => &info.latest_protocol_version,
            Self::V2(info) => &info.latest_protocol_version,
            Self::V3(info) => &info.latest_protocol_version,
        }
    }

    #[inline]
    pub fn slashed(&self) -> &HashMap<AccountId, SlashState> {
        match self {
            Self::V1(info) => &info.slashed,
            Self::V2(info) => &info.slashed,
            Self::V3(info) => &info.slashed,
        }
    }

    #[inline]
    pub fn slashed_mut(&mut self) -> &mut HashMap<AccountId, SlashState> {
        match self {
            Self::V1(info) => &mut info.slashed,
            Self::V2(info) => &mut info.slashed,
            Self::V3(info) => &mut info.slashed,
        }
    }

    #[inline]
    pub fn total_supply(&self) -> &Balance {
        match self {
            Self::V1(info) => &info.total_supply,
            Self::V2(info) => &info.total_supply,
            Self::V3(info) => &info.total_supply,
        }
    }

    #[inline]
    pub fn timestamp_nanosec(&self) -> &u64 {
        match self {
            Self::V1(info) => &info.timestamp_nanosec,
            Self::V2(info) => &info.timestamp_nanosec,
            Self::V3(info) => &info.timestamp_nanosec,
        }
    }

    #[inline]
    pub fn chunk_endorsements(&self) -> Option<&ChunkEndorsementsBitmap> {
        match self {
            Self::V1(_) => None,
            Self::V2(_) => None,
            Self::V3(info) => Some(&info.chunk_endorsements),
        }
    }
}

// V2 -> V3: Add chunk_endorsements bitmap
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
pub struct BlockInfoV3 {
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
    pub chunk_endorsements: ChunkEndorsementsBitmap,
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
