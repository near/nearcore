use crate::types::EpochManagerAdapter;
use near_primitives::hash::CryptoHash;
use near_primitives::version::ProtocolFeature;

pub fn is_spice_enabled(
    epoch_manager: &dyn EpochManagerAdapter,
    prev_block_hash: &CryptoHash,
) -> Result<bool, near_chain_primitives::Error> {
    let epoch_id = epoch_manager.get_epoch_id_from_prev_block(prev_block_hash)?;
    let protocol_version = epoch_manager.get_epoch_protocol_version(&epoch_id)?;
    Ok(ProtocolFeature::Spice.enabled(protocol_version))
}
