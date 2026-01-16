use std::mem::size_of;

use near_crypto::PublicKey;
use near_parameters::RuntimeFeesConfig;
use near_primitives::account::AccessKey;
use near_primitives::gas::Gas;
use near_primitives::types::{Compute, Nonce, NonceIndex, StorageUsage};

use crate::access_key_storage_usage;

pub(crate) fn gas_key_storage_cost(
    fee_config: &RuntimeFeesConfig,
    public_key: &PublicKey,
    access_key: &AccessKey,
    num_nonces: NonceIndex,
) -> StorageUsage {
    let storage_config = &fee_config.storage_usage_config;
    let per_nonce_value_size = borsh::object_length(&(0 as Nonce)).unwrap() as u64;
    let per_nonce_key_size = borsh::object_length(public_key).unwrap() as u64 +  // Public key is part of the key
        size_of::<NonceIndex>() as u64; // Nonce index is part of the key        

    num_nonces as u64
        * (per_nonce_key_size + per_nonce_value_size + storage_config.num_extra_bytes_record)
        + access_key_storage_usage(fee_config, public_key, access_key)
}

/// Returns the compute cost for deleting a single gas key nonce.
pub(crate) fn gas_key_nonce_delete_compute_cost() -> Compute {
    // TODO(gas-keys): properly handle GasKey fees
    Gas::ZERO.as_gas()
}
