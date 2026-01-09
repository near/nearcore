use near_crypto::PublicKey;
use near_parameters::RuntimeFeesConfig;
use near_primitives::account::{AccessKey, Account};
use near_primitives::action::{AddKeyAction, DeleteGasKeyAction};
use near_primitives::errors::ActionErrorKind;
use near_primitives::types::{AccountId, Balance, Nonce, NonceIndex, StorageUsage};
use near_store::{
    StorageError, TrieUpdate, get_access_key, remove_access_key, remove_gas_key_nonce,
    set_access_key, set_gas_key_nonce,
};

use crate::{ActionResult, ApplyState, access_key_storage_usage, initial_nonce_value};

pub(crate) fn action_add_gas_key(
    apply_state: &ApplyState,
    state_update: &mut TrieUpdate,
    account: &mut Account,
    result: &mut ActionResult,
    account_id: &AccountId,
    add_key: &AddKeyAction,
) -> Result<(), StorageError> {
    if get_access_key(state_update, account_id, &add_key.public_key)?.is_some() {
        result.result = Err(ActionErrorKind::AddKeyAlreadyExists {
            account_id: account_id.to_owned(),
            public_key: add_key.public_key.clone().into(),
        }
        .into());
        return Ok(());
    }
    let Some(gas_key_info) = add_key.access_key.gas_key_info() else {
        // Adding a non-gas key via AddGasKey action is not allowed.
        // This is checked in verify, but we double check here to be safe.
        result.result = Err(ActionErrorKind::KeyPermissionInvalid {
            permission: add_key.access_key.permission.clone().into(),
        }
        .into());
        return Ok(());
    };
    // Check that the gas key is not being added with a non-zero balance.
    // This is checked in verify, but we double check here to be safe.
    if gas_key_info.balance != Balance::ZERO {
        result.result = Err(ActionErrorKind::KeyPermissionInvalid {
            permission: add_key.access_key.permission.clone().into(),
        }
        .into());
        return Ok(());
    }

    let mut access_key = add_key.access_key.clone();
    access_key.nonce = 0; // Nonce stored on access key is not used for gas keys should always be zero.
    set_access_key(state_update, account_id.clone(), add_key.public_key.clone(), &access_key);

    let num_nonces = add_key.access_key.gas_key_info().expect("verified in verifier").num_nonces;
    let nonce = initial_nonce_value(apply_state.block_height);
    for i in 0..num_nonces {
        set_gas_key_nonce(state_update, account_id.clone(), add_key.public_key.clone(), i, nonce);
    }
    account.set_storage_usage(
        account
            .storage_usage()
            .checked_add(gas_key_storage_cost(
                &apply_state.config.fees,
                &add_key.public_key,
                &add_key.access_key,
                num_nonces,
            ))
            .ok_or_else(|| {
                StorageError::StorageInconsistentState(format!(
                    "Storage usage integer overflow for account {}",
                    account_id
                ))
            })?,
    );
    Ok(())
}

pub(crate) fn action_delete_gas_key(
    fee_config: &RuntimeFeesConfig,
    state_update: &mut TrieUpdate,
    account: &mut Account,
    result: &mut ActionResult,
    account_id: &AccountId,
    delete_gas_key: &DeleteGasKeyAction,
) -> Result<(), StorageError> {
    let access_key = get_access_key(state_update, account_id, &delete_gas_key.public_key)?;
    if let Some(access_key) = access_key {
        // TODO(gas-keys): Fail if there is a mismatch in num_nonces between stored gas key and action.
        if let Some(gas_key_info) = access_key.gas_key_info() {
            // TODO(gas-keys): Add check for too high balance (for user convenience), as balance will be burned.
            remove_access_key(state_update, account_id.clone(), delete_gas_key.public_key.clone());
            for i in 0..gas_key_info.num_nonces {
                remove_gas_key_nonce(
                    state_update,
                    account_id.clone(),
                    delete_gas_key.public_key.clone(),
                    i,
                );
            }
            account.set_storage_usage(account.storage_usage().saturating_sub(
                gas_key_storage_cost(
                    fee_config,
                    &delete_gas_key.public_key,
                    &access_key,
                    gas_key_info.num_nonces,
                ),
            ));
        } else {
            result.result = Err(ActionErrorKind::DeleteKeyDoesNotExist {
                public_key: delete_gas_key.public_key.clone().into(),
                account_id: account_id.clone(),
            }
            .into());
        }
    } else {
        result.result = Err(ActionErrorKind::DeleteKeyDoesNotExist {
            public_key: delete_gas_key.public_key.clone().into(),
            account_id: account_id.clone(),
        }
        .into());
    }
    Ok(())
}

fn gas_key_storage_cost(
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
