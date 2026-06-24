use crate::ChainStoreAccess;
use near_chain_primitives::Error;
use near_epoch_manager::EpochManagerAdapter;
use near_primitives::block::BlockHeader;
use near_primitives::hash::{CryptoHash, hash};
use near_primitives::merkle::MerklePath;
use near_primitives::types::EpochId;
use near_primitives::views::validator_stake_view::ValidatorStakeView;
use near_primitives::views::{
    BlockHeaderInnerLiteView, LightClientBlockLiteView, LightClientBlockView,
};

/// Spice: the two-hop certified-execution proof for a block `H`. `H`'s leaf proves into the
/// batch root of its certifying block `C` (carried in `certifying_block_header_lite`), and
/// `C` proves into the trusted head's `block_merkle_root` via `block_proof`.
pub struct SpiceCertifiedBatchProof {
    pub block_header_lite: LightClientBlockLiteView,
    pub batch_proof: MerklePath,
    pub certifying_block_header_lite: LightClientBlockLiteView,
    pub block_proof: MerklePath,
}

pub fn get_epoch_block_producers_view(
    epoch_id: &EpochId,
    epoch_manager: &dyn EpochManagerAdapter,
) -> Result<Vec<ValidatorStakeView>, Error> {
    Ok(epoch_manager
        .get_epoch_block_producers_ordered(epoch_id)?
        .iter()
        .map(|x| x.clone().into())
        .collect::<Vec<_>>())
}

/// Light-client lite view of a certified spice block, with the certified roots in the
/// classic `prev_state_root` / `outcome_root` slots. Its `hash()` is the certified leaf.
pub fn reconstruct_certified_lite_view(
    block_header: &BlockHeader,
    certified_state_root: CryptoHash,
    certified_outcome_root: CryptoHash,
) -> LightClientBlockLiteView {
    let inner_lite = BlockHeaderInnerLiteView {
        height: block_header.height(),
        epoch_id: block_header.epoch_id().0,
        next_epoch_id: block_header.next_epoch_id().0,
        prev_state_root: certified_state_root,
        outcome_root: certified_outcome_root,
        timestamp: block_header.raw_timestamp(),
        timestamp_nanosec: block_header.raw_timestamp(),
        next_bp_hash: *block_header.next_bp_hash(),
        block_merkle_root: *block_header.block_merkle_root(),
        certified_block_merkle_root: block_header.certified_block_merkle_root().copied(),
        last_certified_block: block_header.last_certified_block().copied(),
    };
    LightClientBlockLiteView {
        prev_block_hash: *block_header.prev_hash(),
        inner_rest_hash: hash(&block_header.inner_rest_bytes()),
        inner_lite,
    }
}

/// Creates the `LightClientBlock` from the information in the chain store for a given block.
///
/// # Arguments
///  * `block_header` - block header of the block for which the `LightClientBlock` is computed
///  * `chain_store`
///  * `block_producers` - the ordered list of block producers in the epoch of the block that
///                   corresponds to `block_header`
///  * `next_block_producers` - the ordered list of block producers in the next epoch, if the light
///                   client should contain them (otherwise `None`)
///
/// # Requirements:
///    `get_next_final_block_hash` in the chain is set for the block that `block_header` corresponds
///                   to and for the next block, and the three blocks must have sequential heights.
pub fn create_light_client_block_view(
    block_header: &BlockHeader,
    chain_store: &dyn ChainStoreAccess,
    next_block_producers: Option<Vec<ValidatorStakeView>>,
) -> Result<LightClientBlockView, Error> {
    let inner_lite_view = BlockHeaderInnerLiteView {
        height: block_header.height(),
        epoch_id: block_header.epoch_id().0,
        next_epoch_id: block_header.next_epoch_id().0,
        prev_state_root: *block_header.prev_state_root(),
        outcome_root: *block_header.outcome_root(),
        timestamp: block_header.raw_timestamp(),
        timestamp_nanosec: block_header.raw_timestamp(),
        next_bp_hash: *block_header.next_bp_hash(),
        block_merkle_root: *block_header.block_merkle_root(),
        certified_block_merkle_root: block_header.certified_block_merkle_root().copied(),
        last_certified_block: block_header.last_certified_block().copied(),
    };
    let inner_rest_hash = hash(&block_header.inner_rest_bytes());

    let next_block_hash = chain_store.get_next_block_hash(block_header.hash())?;
    let next_block_header = chain_store.get_block_header(&next_block_hash)?;
    let next_block_inner_hash = BlockHeader::compute_inner_hash(
        &next_block_header.inner_lite_bytes(),
        &next_block_header.inner_rest_bytes(),
    );

    let after_next_block_hash = chain_store.get_next_block_hash(&next_block_hash)?;
    let after_next_block_header = chain_store.get_block_header(&after_next_block_hash)?;
    let approvals_after_next = after_next_block_header.approvals().to_vec();

    Ok(LightClientBlockView {
        prev_block_hash: *block_header.prev_hash(),
        next_block_inner_hash,
        inner_lite: inner_lite_view,
        inner_rest_hash,
        next_bps: next_block_producers,
        approvals_after_next,
    })
}
