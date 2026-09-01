use near_chain_primitives::Error;
use near_epoch_manager::EpochManagerAdapter;
use near_primitives::block::{Block, Tip};
use near_primitives::block_header::BlockHeader;
use near_primitives::hash::CryptoHash;
use near_primitives::version::ProtocolFeature;
use near_store::StoreUpdate;
use near_store::adapter::StoreUpdateAdapter;

/// Whether `block_hash` is a spice activation parent: a last block of the last
/// pre-spice epoch, so every child of it is a first spice block.
///
/// Epoch-manager-backed on purpose: the next epoch's protocol version is fixed by the
/// time `block_hash` exists, so the answer is stable regardless of the caller's head.
pub fn is_spice_activation_parent(
    epoch_manager: &dyn EpochManagerAdapter,
    block_hash: &CryptoHash,
) -> Result<bool, Error> {
    if !epoch_manager.is_next_block_epoch_start(block_hash)? {
        return Ok(false);
    }
    let epoch_id = epoch_manager.get_epoch_id(block_hash)?;
    let epoch_protocol_version = epoch_manager.get_epoch_protocol_version(&epoch_id)?;
    if ProtocolFeature::Spice.enabled(epoch_protocol_version) {
        return Ok(false);
    }
    let next_epoch_protocol_version = epoch_manager.get_next_epoch_protocol_version(block_hash)?;
    Ok(ProtocolFeature::Spice.enabled(next_epoch_protocol_version))
}

/// Seeds the spice execution heads when `block` is a first spice block, i.e. when
/// `parent_header` is still pre-spice; a no-op otherwise.
pub fn seed_execution_heads_at_activation(
    store_update: &mut StoreUpdate,
    block: &Block,
    parent_header: &BlockHeader,
) -> Result<(), Error> {
    if parent_header.is_spice() {
        return Ok(());
    }
    let mut adapter = store_update.chain_store_update();
    adapter.set_spice_execution_head(&Tip::from_header(parent_header))?;
    adapter.update_spice_final_execution_head(block)?;
    Ok(())
}
