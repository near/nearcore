//! The stored form of a light client block.
//!
//! `DBCol::EpochLightClientBlocks` is written once per epoch, never rewritten and
//! never garbage collected, so its rows outlive every binary that wrote them. The
//! types here own that layout so that `LightClientBlockView`, which is an RPC
//! response shape, is free to change without rewriting stored data.
//!
//! Variants are frozen. A change to the view adds a variant here instead of
//! editing an existing one.

use borsh::{BorshDeserialize, BorshSerialize};
use near_crypto::Signature;
use near_primitives::hash::CryptoHash;
use near_primitives::types::BlockHeight;
use near_primitives::views::validator_stake_view::ValidatorStakeView;
use near_primitives::views::{BlockHeaderInnerLiteView, LightClientBlockView};
use near_schema_checker_lib::ProtocolSchema;

#[derive(BorshSerialize, BorshDeserialize, Debug, Clone, PartialEq, Eq, ProtocolSchema)]
#[borsh(use_discriminant = true)]
#[repr(u8)]
pub enum StoredLightClientBlock {
    V1(StoredLightClientBlockV1) = 0,
}

#[derive(BorshSerialize, BorshDeserialize, Debug, Clone, PartialEq, Eq, ProtocolSchema)]
pub struct StoredLightClientBlockV1 {
    pub prev_block_hash: CryptoHash,
    pub next_block_inner_hash: CryptoHash,
    pub inner_lite: StoredBlockHeaderInnerLiteV1,
    pub inner_rest_hash: CryptoHash,
    pub next_bps: Option<Vec<ValidatorStakeView>>,
    pub approvals_after_next: Vec<Option<Box<Signature>>>,
}

#[derive(BorshSerialize, BorshDeserialize, Debug, Clone, PartialEq, Eq, ProtocolSchema)]
pub struct StoredBlockHeaderInnerLiteV1 {
    pub height: BlockHeight,
    pub epoch_id: CryptoHash,
    pub next_epoch_id: CryptoHash,
    pub prev_state_root: CryptoHash,
    pub outcome_root: CryptoHash,
    pub timestamp: u64,
    pub timestamp_nanosec: u64,
    pub next_bp_hash: CryptoHash,
    pub block_merkle_root: CryptoHash,
    /// `None` for a pre-spice block, whose header commits no execution results.
    pub chunk_execution_root: Option<CryptoHash>,
}

impl From<LightClientBlockView> for StoredLightClientBlock {
    fn from(view: LightClientBlockView) -> Self {
        Self::V1(StoredLightClientBlockV1 {
            prev_block_hash: view.prev_block_hash,
            next_block_inner_hash: view.next_block_inner_hash,
            inner_lite: StoredBlockHeaderInnerLiteV1 {
                height: view.inner_lite.height,
                epoch_id: view.inner_lite.epoch_id,
                next_epoch_id: view.inner_lite.next_epoch_id,
                prev_state_root: view.inner_lite.prev_state_root,
                outcome_root: view.inner_lite.outcome_root,
                timestamp: view.inner_lite.timestamp,
                timestamp_nanosec: view.inner_lite.timestamp_nanosec,
                next_bp_hash: view.inner_lite.next_bp_hash,
                block_merkle_root: view.inner_lite.block_merkle_root,
                chunk_execution_root: view.inner_lite.chunk_execution_root,
            },
            inner_rest_hash: view.inner_rest_hash,
            next_bps: view.next_bps,
            approvals_after_next: view.approvals_after_next,
        })
    }
}

impl From<StoredLightClientBlock> for LightClientBlockView {
    fn from(stored: StoredLightClientBlock) -> Self {
        let StoredLightClientBlock::V1(stored) = stored;
        LightClientBlockView {
            prev_block_hash: stored.prev_block_hash,
            next_block_inner_hash: stored.next_block_inner_hash,
            inner_lite: BlockHeaderInnerLiteView {
                height: stored.inner_lite.height,
                epoch_id: stored.inner_lite.epoch_id,
                next_epoch_id: stored.inner_lite.next_epoch_id,
                prev_state_root: stored.inner_lite.prev_state_root,
                outcome_root: stored.inner_lite.outcome_root,
                timestamp: stored.inner_lite.timestamp,
                timestamp_nanosec: stored.inner_lite.timestamp_nanosec,
                next_bp_hash: stored.inner_lite.next_bp_hash,
                block_merkle_root: stored.inner_lite.block_merkle_root,
                chunk_execution_root: stored.inner_lite.chunk_execution_root,
            },
            inner_rest_hash: stored.inner_rest_hash,
            next_bps: stored.next_bps,
            approvals_after_next: stored.approvals_after_next,
        }
    }
}
