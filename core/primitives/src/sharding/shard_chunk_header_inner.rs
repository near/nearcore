use crate::bandwidth_scheduler::BandwidthRequests;
use crate::congestion_info::CongestionInfo;
use crate::types::StateRoot;
use crate::types::validator_stake::{ValidatorStake, ValidatorStakeIter, ValidatorStakeV1};
use borsh::{BorshDeserialize, BorshSerialize};
use near_primitives_core::hash::CryptoHash;
use near_primitives_core::types::{Balance, BlockHeight, Gas, ShardId};
use near_schema_checker_lib::ProtocolSchema;

// When removing CryptoHash fields from new versions to be safe we return defaults instead of
// panicking.
const DEFAULT_CRYPTO_HASH: &CryptoHash = &CryptoHash::new();

#[derive(BorshSerialize, BorshDeserialize, Clone, PartialEq, Eq, Debug, ProtocolSchema)]
#[borsh(use_discriminant = true)]
#[repr(u8)]
pub enum ShardChunkHeaderInner {
    V1(ShardChunkHeaderInnerV1) = 0,
    V2(ShardChunkHeaderInnerV2) = 1,
    V3(ShardChunkHeaderInnerV3) = 2,
    V4(ShardChunkHeaderInnerV4) = 3,
    V5(ShardChunkHeaderInnerV5SpiceTxOnly) = 4,
}

impl ShardChunkHeaderInner {
    #[inline]
    pub fn prev_state_root(&self) -> &StateRoot {
        match self {
            Self::V1(inner) => &inner.prev_state_root,
            Self::V2(inner) => &inner.prev_state_root,
            Self::V3(inner) => &inner.prev_state_root,
            Self::V4(inner) => &inner.prev_state_root,
            Self::V5(_) => {
                debug_assert!(false, "Transaction only header doesn't include prev_state_root");
                DEFAULT_CRYPTO_HASH
            }
        }
    }

    #[inline]
    pub fn prev_block_hash(&self) -> &CryptoHash {
        match self {
            Self::V1(inner) => &inner.prev_block_hash,
            Self::V2(inner) => &inner.prev_block_hash,
            Self::V3(inner) => &inner.prev_block_hash,
            Self::V4(inner) => &inner.prev_block_hash,
            Self::V5(inner) => &inner.prev_block_hash,
        }
    }

    #[inline]
    pub fn gas_limit(&self) -> Gas {
        match self {
            Self::V1(inner) => inner.gas_limit,
            Self::V2(inner) => inner.gas_limit,
            Self::V3(inner) => inner.gas_limit,
            Self::V4(inner) => inner.gas_limit,
            Self::V5(_) => {
                // TODO(spice): debug_assert this is unreachable after verifying that nothing depend on this
                // anymore.
                0
            }
        }
    }

    #[inline]
    pub fn prev_gas_used(&self) -> Gas {
        match self {
            Self::V1(inner) => inner.prev_gas_used,
            Self::V2(inner) => inner.prev_gas_used,
            Self::V3(inner) => inner.prev_gas_used,
            Self::V4(inner) => inner.prev_gas_used,
            Self::V5(_) => {
                // TODO(spice): debug_assert this is unreachable after verifying that nothing depend on this
                // anymore.
                0
            }
        }
    }

    #[inline]
    pub fn prev_validator_proposals(&self) -> ValidatorStakeIter {
        match self {
            Self::V1(inner) => ValidatorStakeIter::v1(&inner.prev_validator_proposals),
            Self::V2(inner) => ValidatorStakeIter::new(&inner.prev_validator_proposals),
            Self::V3(inner) => ValidatorStakeIter::new(&inner.prev_validator_proposals),
            Self::V4(inner) => ValidatorStakeIter::new(&inner.prev_validator_proposals),
            Self::V5(_) => {
                // TODO(spice): debug_assert this is unreachable after verifying that nothing depend on this
                // anymore.
                ValidatorStakeIter::empty()
            }
        }
    }

    #[inline]
    pub fn height_created(&self) -> BlockHeight {
        match self {
            Self::V1(inner) => inner.height_created,
            Self::V2(inner) => inner.height_created,
            Self::V3(inner) => inner.height_created,
            Self::V4(inner) => inner.height_created,
            Self::V5(inner) => inner.height_created,
        }
    }

    #[inline]
    pub fn shard_id(&self) -> ShardId {
        match self {
            Self::V1(inner) => inner.shard_id,
            Self::V2(inner) => inner.shard_id,
            Self::V3(inner) => inner.shard_id,
            Self::V4(inner) => inner.shard_id,
            Self::V5(inner) => inner.shard_id,
        }
    }

    #[inline]
    pub fn prev_outcome_root(&self) -> &CryptoHash {
        match self {
            Self::V1(inner) => &inner.prev_outcome_root,
            Self::V2(inner) => &inner.prev_outcome_root,
            Self::V3(inner) => &inner.prev_outcome_root,
            Self::V4(inner) => &inner.prev_outcome_root,
            Self::V5(_) => {
                // TODO(spice): add debug_assert after making sure nothing depends on
                // prev_outcome_root.
                DEFAULT_CRYPTO_HASH
            }
        }
    }

    #[inline]
    pub fn encoded_merkle_root(&self) -> &CryptoHash {
        match self {
            Self::V1(inner) => &inner.encoded_merkle_root,
            Self::V2(inner) => &inner.encoded_merkle_root,
            Self::V3(inner) => &inner.encoded_merkle_root,
            Self::V4(inner) => &inner.encoded_merkle_root,
            Self::V5(inner) => &inner.encoded_merkle_root,
        }
    }

    #[inline]
    pub fn encoded_length(&self) -> u64 {
        match self {
            Self::V1(inner) => inner.encoded_length,
            Self::V2(inner) => inner.encoded_length,
            Self::V3(inner) => inner.encoded_length,
            Self::V4(inner) => inner.encoded_length,
            Self::V5(inner) => inner.encoded_length,
        }
    }

    #[inline]
    pub fn prev_balance_burnt(&self) -> Balance {
        match self {
            Self::V1(inner) => inner.prev_balance_burnt,
            Self::V2(inner) => inner.prev_balance_burnt,
            Self::V3(inner) => inner.prev_balance_burnt,
            Self::V4(inner) => inner.prev_balance_burnt,
            Self::V5(_) => {
                // TODO(spice): debug_assert this is unreachable after verifying that nothing depend on this
                // anymore.
                0
            }
        }
    }

    #[inline]
    pub fn prev_outgoing_receipts_root(&self) -> &CryptoHash {
        match self {
            Self::V1(inner) => &inner.prev_outgoing_receipts_root,
            Self::V2(inner) => &inner.prev_outgoing_receipts_root,
            Self::V3(inner) => &inner.prev_outgoing_receipts_root,
            Self::V4(inner) => &inner.prev_outgoing_receipts_root,
            // TODO(spice): debug_assert as unreachable. See comment on the field for more details.
            Self::V5(inner) => &inner.prev_outgoing_receipts_root,
        }
    }

    #[inline]
    pub fn tx_root(&self) -> &CryptoHash {
        match self {
            Self::V1(inner) => &inner.tx_root,
            Self::V2(inner) => &inner.tx_root,
            Self::V3(inner) => &inner.tx_root,
            Self::V4(inner) => &inner.tx_root,
            Self::V5(inner) => &inner.tx_root,
        }
    }

    #[inline]
    pub fn congestion_info(&self) -> CongestionInfo {
        match self {
            Self::V1(_) | Self::V2(_) => {
                debug_assert!(false, "Calling congestion_info on V1 or V2 header version");
                Default::default()
            }
            Self::V3(v3) => v3.congestion_info,
            Self::V4(v4) => v4.congestion_info,
            // TODO(spice): debug_assert this is unreachable after verifying that nothing depend on this
            // anymore.
            Self::V5(_) => CongestionInfo::default(),
        }
    }

    #[inline]
    pub fn bandwidth_requests(&self) -> Option<&BandwidthRequests> {
        match self {
            Self::V1(_) | Self::V2(_) | Self::V3(_) => None,
            Self::V4(inner) => Some(&inner.bandwidth_requests),
            // TODO(spice): debug_assert this is unreachable after verifying that nothing depend on this
            // anymore.
            Self::V5(_) => None,
        }
    }

    /// Used for error messages, use `match` for other code.
    #[inline]
    pub(crate) fn version_number(&self) -> u64 {
        match self {
            Self::V1(_) => 1,
            Self::V2(_) => 2,
            Self::V3(_) => 3,
            Self::V4(_) => 4,
            Self::V5(_) => 5,
        }
    }
}

#[derive(BorshSerialize, BorshDeserialize, Clone, PartialEq, Eq, Debug, ProtocolSchema)]
pub struct ShardChunkHeaderInnerV1 {
    /// Previous block hash.
    pub prev_block_hash: CryptoHash,
    pub prev_state_root: StateRoot,
    /// Root of the outcomes from execution transactions and results of the previous chunk.
    pub prev_outcome_root: CryptoHash,
    pub encoded_merkle_root: CryptoHash,
    pub encoded_length: u64,
    pub height_created: BlockHeight,
    /// Shard index.
    pub shard_id: ShardId,
    /// Gas used in the previous chunk.
    pub prev_gas_used: Gas,
    /// Gas limit voted by validators.
    pub gas_limit: Gas,
    /// Total balance burnt in the previous chunk.
    pub prev_balance_burnt: Balance,
    /// Previous chunk's outgoing receipts merkle root.
    pub prev_outgoing_receipts_root: CryptoHash,
    /// Tx merkle root.
    pub tx_root: CryptoHash,
    /// Validator proposals from the previous chunk.
    pub prev_validator_proposals: Vec<ValidatorStakeV1>,
}

// V1 -> V2: Use versioned ValidatorStake structure in proposals
#[derive(BorshSerialize, BorshDeserialize, Clone, PartialEq, Eq, Debug, ProtocolSchema)]
pub struct ShardChunkHeaderInnerV2 {
    /// Previous block hash.
    pub prev_block_hash: CryptoHash,
    pub prev_state_root: StateRoot,
    /// Root of the outcomes from execution transactions and results of the previous chunk.
    pub prev_outcome_root: CryptoHash,
    pub encoded_merkle_root: CryptoHash,
    pub encoded_length: u64,
    pub height_created: BlockHeight,
    /// Shard index.
    pub shard_id: ShardId,
    /// Gas used in the previous chunk.
    pub prev_gas_used: Gas,
    /// Gas limit voted by validators.
    pub gas_limit: Gas,
    /// Total balance burnt in the previous chunk.
    pub prev_balance_burnt: Balance,
    /// Previous chunk's outgoing receipts merkle root.
    pub prev_outgoing_receipts_root: CryptoHash,
    /// Tx merkle root.
    pub tx_root: CryptoHash,
    /// Validator proposals from the previous chunk.
    pub prev_validator_proposals: Vec<ValidatorStake>,
}

// V2 -> V3: Add congestion info.
#[derive(BorshSerialize, BorshDeserialize, Clone, PartialEq, Eq, Debug, ProtocolSchema)]
pub struct ShardChunkHeaderInnerV3 {
    /// Previous block hash.
    pub prev_block_hash: CryptoHash,
    pub prev_state_root: StateRoot,
    /// Root of the outcomes from execution transactions and results of the previous chunk.
    pub prev_outcome_root: CryptoHash,
    pub encoded_merkle_root: CryptoHash,
    pub encoded_length: u64,
    pub height_created: BlockHeight,
    /// Shard index.
    pub shard_id: ShardId,
    /// Gas used in the previous chunk.
    pub prev_gas_used: Gas,
    /// Gas limit voted by validators.
    pub gas_limit: Gas,
    /// Total balance burnt in the previous chunk.
    pub prev_balance_burnt: Balance,
    /// Previous chunk's outgoing receipts merkle root.
    pub prev_outgoing_receipts_root: CryptoHash,
    /// Tx merkle root.
    pub tx_root: CryptoHash,
    /// Validator proposals from the previous chunk.
    pub prev_validator_proposals: Vec<ValidatorStake>,
    /// Congestion info about this shard after the previous chunk was applied.
    pub congestion_info: CongestionInfo,
}

// V3 -> V4: Add bandwidth requests.
#[derive(BorshSerialize, BorshDeserialize, Clone, PartialEq, Eq, Debug, ProtocolSchema)]
pub struct ShardChunkHeaderInnerV4 {
    /// Previous block hash.
    pub prev_block_hash: CryptoHash,
    pub prev_state_root: StateRoot,
    /// Root of the outcomes from execution transactions and results of the previous chunk.
    pub prev_outcome_root: CryptoHash,
    pub encoded_merkle_root: CryptoHash,
    pub encoded_length: u64,
    pub height_created: BlockHeight,
    /// Shard index.
    pub shard_id: ShardId,
    /// Gas used in the previous chunk.
    pub prev_gas_used: Gas,
    /// Gas limit voted by validators.
    pub gas_limit: Gas,
    /// Total balance burnt in the previous chunk.
    pub prev_balance_burnt: Balance,
    /// Previous chunk's outgoing receipts merkle root.
    pub prev_outgoing_receipts_root: CryptoHash,
    /// Tx merkle root.
    pub tx_root: CryptoHash,
    /// Validator proposals from the previous chunk.
    pub prev_validator_proposals: Vec<ValidatorStake>,
    /// Congestion info about this shard after the previous chunk was applied.
    pub congestion_info: CongestionInfo,
    /// Requests for bandwidth to send receipts to other shards.
    pub bandwidth_requests: BandwidthRequests,
}

// V4 -> V5: a version for spice of a chunk header including only transactions (no previous
// execution results).
#[derive(BorshSerialize, BorshDeserialize, Clone, PartialEq, Eq, Debug, ProtocolSchema)]
pub struct ShardChunkHeaderInnerV5SpiceTxOnly {
    /// Previous block hash.
    pub prev_block_hash: CryptoHash,
    pub encoded_merkle_root: CryptoHash,
    pub encoded_length: u64,
    pub height_created: BlockHeight,
    /// Shard index.
    pub shard_id: ShardId,
    // TODO(spice): remove prev_outgoing_receipts_root. We have it for now
    // so that some of the existing validations pass. List of outgoing receipts is always empty,
    // but it wouldn't mean that prev_outgoing_receipts_root is CryptoHash::default() since it's
    // computed as root of merkle tree of those empty lists from all shards.
    /// Previous chunk's outgoing receipts merkle root.
    pub prev_outgoing_receipts_root: CryptoHash,
    /// Tx merkle root.
    pub tx_root: CryptoHash,
}
