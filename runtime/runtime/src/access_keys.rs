use crate::config::{safe_add_compute, storage_removes_compute};
use crate::{ActionResult, ApplyState};
use near_crypto::PublicKey;
use near_parameters::{RuntimeConfig, RuntimeFeesConfig};
use near_primitives::account::{AccessKey, Account, GasKeyInfo};
use near_primitives::action::{TransferToGasKeyAction, WithdrawFromGasKeyAction};
use near_primitives::errors::{ActionErrorKind, IntegerOverflowError, RuntimeError};
use near_primitives::transaction::{AddKeyAction, DeleteKeyAction};
use near_primitives::trie_key::gas_key_nonce_key_len;
use near_primitives::types::{AccountId, BlockHeight, Nonce, NonceIndex, StorageUsage};
use near_store::{
    StorageError, TrieUpdate, get_access_key, remove_access_key, remove_gas_key_nonce,
    set_access_key, set_gas_key_nonce,
};
use std::mem::size_of;

fn access_key_storage_usage(
    fee_config: &RuntimeFeesConfig,
    public_key: &PublicKey,
    access_key: &AccessKey,
) -> StorageUsage {
    let storage_usage_config = &fee_config.storage_usage_config;
    // Use the on-trie identifier length, not the borsh-serialized pubkey
    // length: ML-DSA-65 access keys live in the trie as a SHA3-256 hash
    // (33 bytes incl. type tag), not as a 1953-byte full pubkey.
    public_key.trie_id_len() as u64
        + borsh::object_length(access_key).unwrap() as u64
        + storage_usage_config.num_extra_bytes_record
}

fn gas_key_storage_cost(
    fee_config: &RuntimeFeesConfig,
    public_key: &PublicKey,
    access_key: &AccessKey,
    num_nonces: NonceIndex,
) -> StorageUsage {
    let storage_config = &fee_config.storage_usage_config;
    let per_nonce_value_size = borsh::object_length(&(0 as Nonce)).unwrap() as u64;
    let per_nonce_key_size = public_key.trie_id_len() as u64 + size_of::<NonceIndex>() as u64;

    num_nonces as u64
        * (per_nonce_key_size + per_nonce_value_size + storage_config.num_extra_bytes_record)
        + access_key_storage_usage(fee_config, public_key, access_key)
}

pub(crate) fn initial_nonce_value(block_height: BlockHeight) -> Nonce {
    // Set default nonce for newly created access key to avoid transaction hash collision.
    // See <https://github.com/near/nearcore/issues/3779>.
    (block_height - 1) * near_primitives::account::AccessKey::ACCESS_KEY_NONCE_RANGE_MULTIPLIER
}

pub(crate) fn action_delete_key(
    config: &RuntimeConfig,
    state_update: &mut TrieUpdate,
    account: &mut Account,
    result: &mut ActionResult,
    account_id: &AccountId,
    delete_key: &DeleteKeyAction,
) -> Result<(), RuntimeError> {
    let access_key = get_access_key(state_update, account_id, &delete_key.public_key)?;
    if let Some(access_key) = access_key {
        if let Some(gas_key_info) = access_key.gas_key_info() {
            delete_gas_key(
                config,
                state_update,
                account,
                result,
                account_id,
                &delete_key.public_key,
                &access_key,
                gas_key_info,
            )?;
        } else {
            delete_regular_key(
                &config.fees,
                state_update,
                account,
                account_id,
                &delete_key.public_key,
                &access_key,
            );
        }
    } else {
        result.result = Err(ActionErrorKind::DeleteKeyDoesNotExist {
            public_key: delete_key.public_key.clone().into(),
            account_id: account_id.clone(),
        }
        .into());
    }
    Ok(())
}

fn delete_gas_key(
    config: &RuntimeConfig,
    state_update: &mut TrieUpdate,
    account: &mut Account,
    result: &mut ActionResult,
    account_id: &AccountId,
    public_key: &PublicKey,
    access_key: &AccessKey,
    gas_key_info: &GasKeyInfo,
) -> Result<(), RuntimeError> {
    if gas_key_info.balance > GasKeyInfo::MAX_BALANCE_TO_BURN {
        result.result = Err(ActionErrorKind::GasKeyBalanceTooHigh {
            account_id: account_id.clone(),
            public_key: Some(Box::new(public_key.clone())),
            balance: gas_key_info.balance,
        }
        .into());
        return Ok(());
    }
    result.tokens_burnt =
        result.tokens_burnt.checked_add(gas_key_info.balance).ok_or(IntegerOverflowError)?;
    let num_nonces = gas_key_info.num_nonces as usize;
    for i in 0..gas_key_info.num_nonces {
        remove_gas_key_nonce(state_update, account_id.clone(), public_key.clone(), i);
    }
    let nonce_key_len = gas_key_nonce_key_len(account_id, &public_key.into());
    let nonce_remove_compute = storage_removes_compute(
        &config.wasm_config.ext_costs,
        num_nonces,
        nonce_key_len * num_nonces,
        AccessKey::NONCE_VALUE_LEN * num_nonces,
    );
    result.compute_usage = safe_add_compute(result.compute_usage, nonce_remove_compute)?;
    remove_access_key(state_update, account_id.clone(), public_key.clone());
    account.set_storage_usage(account.storage_usage().saturating_sub(gas_key_storage_cost(
        &config.fees,
        public_key,
        access_key,
        gas_key_info.num_nonces,
    )));
    Ok(())
}

fn delete_regular_key(
    fee_config: &RuntimeFeesConfig,
    state_update: &mut TrieUpdate,
    account: &mut Account,
    account_id: &AccountId,
    public_key: &PublicKey,
    access_key: &AccessKey,
) {
    let storage_usage = access_key_storage_usage(fee_config, public_key, access_key);
    remove_access_key(state_update, account_id.clone(), public_key.clone());
    account.set_storage_usage(account.storage_usage().saturating_sub(storage_usage));
}

pub(crate) fn action_add_key(
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

    let fee_config = &apply_state.config.fees;

    if let Some(gas_key_info) = add_key.access_key.gas_key_info() {
        add_gas_key(
            fee_config,
            state_update,
            account,
            account_id,
            &add_key.public_key,
            &add_key.access_key,
            gas_key_info,
            apply_state.block_height,
        )?;
    } else {
        add_regular_key(
            fee_config,
            state_update,
            account,
            account_id,
            &add_key.public_key,
            &add_key.access_key,
            apply_state.block_height,
        )?;
    }

    Ok(())
}

fn add_gas_key(
    fee_config: &RuntimeFeesConfig,
    state_update: &mut TrieUpdate,
    account: &mut Account,
    account_id: &AccountId,
    public_key: &PublicKey,
    access_key: &AccessKey,
    gas_key_info: &GasKeyInfo,
    block_height: BlockHeight,
) -> Result<(), StorageError> {
    // For gas keys, nonce stored on access key is not used and should always be zero
    let mut access_key = access_key.clone();
    access_key.nonce = 0;
    set_access_key(state_update, account_id.clone(), public_key.clone(), &access_key);

    // Set up nonces for gas key
    let num_nonces = gas_key_info.num_nonces;
    let nonce = initial_nonce_value(block_height);
    for i in 0..num_nonces {
        set_gas_key_nonce(state_update, account_id.clone(), public_key.clone(), i, nonce);
    }

    account.set_storage_usage(
        account
            .storage_usage()
            .checked_add(gas_key_storage_cost(fee_config, public_key, &access_key, num_nonces))
            .ok_or_else(|| {
                StorageError::StorageInconsistentState(format!(
                    "Storage usage integer overflow for account {}",
                    account_id
                ))
            })?,
    );
    Ok(())
}

fn add_regular_key(
    fee_config: &RuntimeFeesConfig,
    state_update: &mut TrieUpdate,
    account: &mut Account,
    account_id: &AccountId,
    public_key: &PublicKey,
    access_key: &AccessKey,
    block_height: BlockHeight,
) -> Result<(), StorageError> {
    let mut access_key = access_key.clone();
    access_key.nonce = initial_nonce_value(block_height);
    set_access_key(state_update, account_id.clone(), public_key.clone(), &access_key);

    account.set_storage_usage(
        account
            .storage_usage()
            .checked_add(access_key_storage_usage(fee_config, public_key, &access_key))
            .ok_or_else(|| {
                StorageError::StorageInconsistentState(format!(
                    "Storage usage integer overflow for account {}",
                    account_id
                ))
            })?,
    );
    Ok(())
}

pub(crate) fn action_transfer_to_gas_key(
    state_update: &mut TrieUpdate,
    result: &mut ActionResult,
    account_id: &AccountId,
    action: &TransferToGasKeyAction,
) -> Result<(), RuntimeError> {
    let Some(mut access_key) = get_access_key(state_update, account_id, &action.public_key)? else {
        result.result = Err(ActionErrorKind::GasKeyDoesNotExist {
            account_id: account_id.clone(),
            public_key: Box::new(action.public_key.clone()),
        }
        .into());
        return Ok(());
    };
    let Some(gas_key_info) = access_key.gas_key_info_mut() else {
        // Key exists but is not a gas key
        result.result = Err(ActionErrorKind::GasKeyDoesNotExist {
            account_id: account_id.clone(),
            public_key: Box::new(action.public_key.clone()),
        }
        .into());
        return Ok(());
    };

    gas_key_info.balance = gas_key_info.balance.checked_add(action.deposit).ok_or_else(|| {
        RuntimeError::StorageError(StorageError::StorageInconsistentState(
            "gas key balance integer overflow".to_string(),
        ))
    })?;
    set_access_key(state_update, account_id.clone(), action.public_key.clone(), &access_key);
    Ok(())
}

pub(crate) fn action_withdraw_from_gas_key(
    state_update: &mut TrieUpdate,
    account: &mut Account,
    result: &mut ActionResult,
    account_id: &AccountId,
    action: &WithdrawFromGasKeyAction,
) -> Result<(), RuntimeError> {
    let Some(mut access_key) = get_access_key(state_update, account_id, &action.public_key)? else {
        result.result = Err(ActionErrorKind::GasKeyDoesNotExist {
            account_id: account_id.clone(),
            public_key: Box::new(action.public_key.clone()),
        }
        .into());
        return Ok(());
    };
    let Some(gas_key_info) = access_key.gas_key_info_mut() else {
        // Key exists but is not a gas key
        result.result = Err(ActionErrorKind::GasKeyDoesNotExist {
            account_id: account_id.clone(),
            public_key: Box::new(action.public_key.clone()),
        }
        .into());
        return Ok(());
    };

    let Some(updated_balance) = gas_key_info.balance.checked_sub(action.amount) else {
        result.result = Err(ActionErrorKind::InsufficientGasKeyBalance {
            account_id: account_id.clone(),
            public_key: Box::new(action.public_key.clone()),
            balance: gas_key_info.balance,
            required: action.amount,
        }
        .into());
        return Ok(());
    };
    gas_key_info.balance = updated_balance;
    set_access_key(state_update, account_id.clone(), action.public_key.clone(), &access_key);

    let new_account_balance = account.amount().checked_add(action.amount).ok_or_else(|| {
        RuntimeError::StorageError(StorageError::StorageInconsistentState(
            "Account balance integer overflow".to_string(),
        ))
    })?;
    account.set_amount(new_account_balance);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ActionResult;
    use crate::ApplyState;
    use crate::actions_test_utils::{setup_account, setup_account_with_tries, test_delete_account};
    use crate::config::storage_removes_compute;
    use crate::state_viewer::TrieViewer;
    use crate::state_viewer::errors::ViewAccessKeyError;
    use near_crypto::{InMemorySigner, KeyType, PublicKeyHandle};
    use near_parameters::{RuntimeConfig, RuntimeConfigStore};
    use near_primitives::account::{
        AccessKey, AccessKeyPermission, Account, AccountContract, GasKeyInfo,
    };
    use near_primitives::apply::ApplyChunkReason;
    use near_primitives::bandwidth_scheduler::BlockBandwidthRequests;
    use near_primitives::congestion_info::BlockCongestionInfo;
    use near_primitives::errors::ActionErrorKind;
    use near_primitives::hash::CryptoHash;
    use near_primitives::transaction::{AddKeyAction, DeleteKeyAction};
    use near_primitives::trie_key::trie_key_parsers;
    use near_primitives::types::{
        AccountId, Balance, BlockHeight, EpochId, NonceIndex, StateChangeCause,
    };
    use near_primitives::version::PROTOCOL_VERSION;
    use near_store::{
        ShardTries, ShardUId, Trie, TrieUpdate, get_access_key, get_account, get_gas_key_nonce,
        set_access_key, set_account,
    };
    use std::collections::HashSet;
    use std::num::NonZeroU32;
    use std::sync::Arc;

    const TEST_NUM_NONCES: NonceIndex = 2;
    const TEST_GAS_KEY_BLOCK_HEIGHT: BlockHeight = 10;

    fn expected_nonce_remove_compute(
        account_id: &AccountId,
        public_key: &PublicKey,
        num_nonces: usize,
    ) -> u64 {
        let config = RuntimeConfig::test();
        let nonce_key_len = gas_key_nonce_key_len(account_id, &public_key.into());
        storage_removes_compute(
            &config.wasm_config.ext_costs,
            num_nonces,
            nonce_key_len * num_nonces,
            AccessKey::NONCE_VALUE_LEN * num_nonces,
        )
    }

    fn create_apply_state(block_height: BlockHeight) -> ApplyState {
        ApplyState {
            apply_reason: ApplyChunkReason::UpdateTrackedShard,
            block_height,
            prev_block_hash: CryptoHash::default(),
            shard_id: ShardUId::single_shard().shard_id(),
            epoch_id: EpochId::default(),
            epoch_height: 3,
            gas_price: Balance::from_yoctonear(2),
            block_timestamp: 1,
            gas_limit: None,
            random_seed: CryptoHash::default(),
            current_protocol_version: 1,
            config: Arc::new(RuntimeConfig::test()),
            next_wasm_config: None,
            cache: None,
            is_new_chunk: false,
            save_receipt_to_tx: false,
            congestion_info: BlockCongestionInfo::default(),
            bandwidth_requests: BlockBandwidthRequests::empty(),
            trie_access_tracker_state: Default::default(),
            on_post_state_ready: None,
        }
    }

    fn test_account_keys() -> (AccountId, PublicKey, AccessKey) {
        let account_id: AccountId = "alice.near".parse().unwrap();
        let public_key: PublicKey =
            "ed25519:32LnPNBZQJ3uhY8yV6JqnNxtRW8E27Ps9YD1XeUNuA1m".parse().unwrap();
        let access_key = AccessKey { nonce: 0, permission: AccessKeyPermission::FullAccess };
        (account_id, public_key, access_key)
    }

    fn add_gas_key_to_account(
        state_update: &mut TrieUpdate,
        account: &mut Account,
        account_id: &AccountId,
        public_key: &PublicKey,
    ) -> AccessKey {
        let mut result = ActionResult::default();
        let apply_state = create_apply_state(TEST_GAS_KEY_BLOCK_HEIGHT);
        let action = AddKeyAction {
            public_key: public_key.clone(),
            access_key: AccessKey::gas_key_full_access(TEST_NUM_NONCES),
        };
        action_add_key(&apply_state, state_update, account, &mut result, account_id, &action)
            .unwrap();
        assert!(result.result.is_ok(), "result error: {:?}", result.result);

        get_access_key(state_update, account_id, public_key)
            .expect("could not find gas key")
            .unwrap()
    }

    #[test]
    fn test_add_gas_key() {
        let (account_id, public_key, access_key) = test_account_keys();
        let mut state_update = setup_account(&account_id, &public_key, &access_key);
        let mut account =
            get_account(&state_update, &account_id).expect("failed to get account").unwrap();
        let storage_before = account.storage_usage();

        let gas_key_public_key =
            InMemorySigner::from_seed(account_id.clone(), KeyType::ED25519, "gas_key").public_key();
        let gas_key = add_gas_key_to_account(
            &mut state_update,
            &mut account,
            &account_id,
            &gas_key_public_key,
        );

        let AccessKeyPermission::GasKeyFullAccess(gas_key_info) = &gas_key.permission else {
            unreachable!();
        };
        assert_eq!(gas_key_info.num_nonces, TEST_NUM_NONCES);
        assert_eq!(gas_key_info.balance, Balance::ZERO);
        assert!(account.storage_usage() > storage_before);
        assert_eq!(
            account.storage_usage(),
            storage_before
                + gas_key_storage_cost(
                    &RuntimeFeesConfig::test(),
                    &public_key,
                    &gas_key,
                    gas_key_info.num_nonces
                )
        );

        // Check gas key nonces were initialized
        let expected_nonce = (TEST_GAS_KEY_BLOCK_HEIGHT - 1)
            * near_primitives::account::AccessKey::ACCESS_KEY_NONCE_RANGE_MULTIPLIER;
        for i in 0..gas_key_info.num_nonces {
            let gas_key_nonce =
                get_gas_key_nonce(&state_update, &account_id, &gas_key_public_key, i)
                    .expect("failed to get gas key nonce")
                    .expect("gas key nonce not found");
            assert_eq!(gas_key_nonce, expected_nonce);
        }
    }

    #[test]
    fn test_cannot_add_duplicate_gas_key() {
        let (account_id, public_key, access_key) = test_account_keys();
        let mut state_update = setup_account(&account_id, &public_key, &access_key);
        let mut account =
            get_account(&state_update, &account_id).expect("failed to get account").unwrap();
        let gas_key_public_key =
            InMemorySigner::from_seed(account_id.clone(), KeyType::ED25519, "gas_key").public_key();
        add_gas_key_to_account(&mut state_update, &mut account, &account_id, &gas_key_public_key);

        let mut result = ActionResult::default();
        let action = AddKeyAction {
            public_key: gas_key_public_key.clone(),
            access_key: AccessKey::gas_key_full_access(TEST_NUM_NONCES),
        };
        action_add_key(
            &create_apply_state(TEST_GAS_KEY_BLOCK_HEIGHT),
            &mut state_update,
            &mut account,
            &mut result,
            &account_id,
            &action,
        )
        .unwrap();
        assert_eq!(
            result.result,
            Err(ActionErrorKind::AddKeyAlreadyExists {
                account_id: account_id.clone(),
                public_key: gas_key_public_key.into(),
            }
            .into())
        );
    }

    #[test]
    fn test_cannot_add_gas_key_with_same_public_key_as_access_key() {
        let (account_id, public_key, access_key) = test_account_keys();
        let mut state_update = setup_account(&account_id, &public_key, &access_key);
        let mut account =
            get_account(&state_update, &account_id).expect("failed to get account").unwrap();
        let mut result = ActionResult::default();
        let action = AddKeyAction {
            public_key: public_key.clone(),
            access_key: AccessKey::gas_key_full_access(TEST_NUM_NONCES),
        };
        action_add_key(
            &create_apply_state(TEST_GAS_KEY_BLOCK_HEIGHT),
            &mut state_update,
            &mut account,
            &mut result,
            &account_id,
            &action,
        )
        .unwrap();
        assert_eq!(
            result.result,
            Err(ActionErrorKind::AddKeyAlreadyExists {
                account_id: account_id.clone(),
                public_key: public_key.into(),
            }
            .into())
        );
    }

    #[test]
    fn test_delete_gas_key() {
        let (account_id, public_key, access_key) = test_account_keys();
        let mut state_update = setup_account(&account_id, &public_key, &access_key);
        let mut account =
            get_account(&state_update, &account_id).expect("failed to get account").unwrap();
        let storage_before = account.storage_usage();

        let gas_key_public_key =
            InMemorySigner::from_seed(account_id.clone(), KeyType::ED25519, "gas_key").public_key();
        let gas_key = add_gas_key_to_account(
            &mut state_update,
            &mut account,
            &account_id,
            &gas_key_public_key,
        );

        let mut result = ActionResult::default();
        let action = DeleteKeyAction { public_key: gas_key_public_key.clone() };
        action_delete_key(
            &RuntimeConfig::test(),
            &mut state_update,
            &mut account,
            &mut result,
            &account_id,
            &action,
        )
        .unwrap();
        assert!(result.result.is_ok(), "result error: {:?}", result.result);

        let stored_gas_key = get_access_key(&state_update, &account_id, &gas_key_public_key)
            .expect("failed to get gas key");
        assert!(stored_gas_key.is_none());
        assert_eq!(account.storage_usage(), storage_before);

        let AccessKeyPermission::GasKeyFullAccess(gas_key_info) = &gas_key.permission else {
            unreachable!();
        };

        // Check gas key nonces were deleted
        for i in 0..gas_key_info.num_nonces {
            let gas_key_nonce =
                get_gas_key_nonce(&state_update, &account_id, &gas_key_public_key, i)
                    .expect("failed to get gas key nonce");
            assert!(gas_key_nonce.is_none());
        }

        let expected_compute = expected_nonce_remove_compute(
            &account_id,
            &gas_key_public_key,
            TEST_NUM_NONCES as usize,
        );
        assert_eq!(result.compute_usage, expected_compute);
    }

    #[test]
    fn test_delete_gas_key_burns_balance() {
        let (account_id, public_key, access_key) = test_account_keys();
        let mut state_update = setup_account(&account_id, &public_key, &access_key);
        let mut account =
            get_account(&state_update, &account_id).expect("failed to get account").unwrap();

        let gas_key_public_key =
            InMemorySigner::from_seed(account_id.clone(), KeyType::ED25519, "gas_key").public_key();
        add_gas_key_to_account(&mut state_update, &mut account, &account_id, &gas_key_public_key);

        // Fund the gas key
        let deposit_amount = Balance::from_yoctonear(1_000_000);
        transfer_to_gas_key(&mut state_update, &account_id, &gas_key_public_key, deposit_amount);

        // Delete the gas key
        let mut result = ActionResult::default();
        let action = DeleteKeyAction { public_key: gas_key_public_key.clone() };
        action_delete_key(
            &RuntimeConfig::test(),
            &mut state_update,
            &mut account,
            &mut result,
            &account_id,
            &action,
        )
        .unwrap();
        assert!(result.result.is_ok());

        // Verify the balance was burned
        assert_eq!(result.tokens_burnt, deposit_amount);
        let expected_compute = expected_nonce_remove_compute(
            &account_id,
            &gas_key_public_key,
            TEST_NUM_NONCES as usize,
        );
        assert_eq!(result.compute_usage, expected_compute);
    }

    #[test]
    fn test_delete_nonexistent_gas_key() {
        let (account_id, public_key, access_key) = test_account_keys();
        let mut state_update = setup_account(&account_id, &public_key, &access_key);
        let mut account =
            get_account(&state_update, &account_id).expect("failed to get account").unwrap();

        // Try to delete a key that doesn't exist
        let nonexistent_public_key =
            InMemorySigner::from_seed(account_id.clone(), KeyType::ED25519, "nonexistent")
                .public_key();
        let mut result = ActionResult::default();
        let action = DeleteKeyAction { public_key: nonexistent_public_key.clone() };
        action_delete_key(
            &RuntimeConfig::test(),
            &mut state_update,
            &mut account,
            &mut result,
            &account_id,
            &action,
        )
        .unwrap();
        assert_eq!(
            result.result,
            Err(ActionErrorKind::DeleteKeyDoesNotExist {
                account_id: account_id.clone(),
                public_key: nonexistent_public_key.into(),
            }
            .into())
        );
    }

    #[test]
    fn test_delete_account_removes_gas_keys() {
        let (account_id, public_key, access_key) = test_account_keys();
        let public_keys: Vec<PublicKey> = (0..3)
            .map(|i| PublicKey::from_seed(KeyType::ED25519, &format!("gas_key_{i}")))
            .collect();
        let mut state_update = setup_account(&account_id, &public_key, &access_key);
        let mut account = get_account(&state_update, &account_id).unwrap().unwrap();
        for public_key in &public_keys {
            add_gas_key_to_account(&mut state_update, &mut account, &account_id, public_key);
        }
        state_update.commit(StateChangeCause::InitialState);

        let action_result = test_delete_account(
            &account_id,
            AccountContract::from_local_code_hash(CryptoHash::default()),
            100,
            PROTOCOL_VERSION,
            &mut state_update,
        );
        assert!(action_result.result.is_ok());
        state_update.commit(StateChangeCause::InitialState);

        let expected_compute: u64 = public_keys
            .iter()
            .map(|pk| expected_nonce_remove_compute(&account_id, pk, TEST_NUM_NONCES as usize))
            .sum();
        assert_eq!(action_result.compute_usage, expected_compute);

        let lock = state_update.trie().lock_for_iter();
        let trie_key_count = state_update
            .locked_iter(&trie_key_parsers::get_raw_prefix_for_access_keys(&account_id), &lock)
            .expect("could not get trie iterator")
            .count();
        assert_eq!(trie_key_count, 0);
    }

    #[test]
    fn test_delete_account_burns_gas_key_balances() {
        let (account_id, public_key, access_key) = test_account_keys();
        let public_keys: Vec<PublicKey> = (0..3)
            .map(|i| PublicKey::from_seed(KeyType::ED25519, &format!("gas_key_{i}")))
            .collect();
        let mut state_update = setup_account(&account_id, &public_key, &access_key);
        let mut account = get_account(&state_update, &account_id).unwrap().unwrap();
        for public_key in &public_keys {
            add_gas_key_to_account(&mut state_update, &mut account, &account_id, public_key);
        }

        // Fund each gas key with different amounts
        let deposit_amounts = [
            Balance::from_yoctonear(100_000),
            Balance::from_yoctonear(200_000),
            Balance::from_yoctonear(300_000),
        ];
        for (public_key, amount) in public_keys.iter().zip(deposit_amounts.iter()) {
            transfer_to_gas_key(&mut state_update, &account_id, public_key, *amount);
        }
        state_update.commit(StateChangeCause::InitialState);

        let action_result = test_delete_account(
            &account_id,
            AccountContract::from_local_code_hash(CryptoHash::default()),
            100,
            PROTOCOL_VERSION,
            &mut state_update,
        );
        assert!(action_result.result.is_ok());

        // Verify total burned balance equals sum of all gas key balances
        let expected_burnt =
            deposit_amounts.iter().fold(Balance::ZERO, |acc, x| acc.checked_add(*x).unwrap());
        assert_eq!(action_result.tokens_burnt, expected_burnt);
        let expected_compute: u64 = public_keys
            .iter()
            .map(|pk| expected_nonce_remove_compute(&account_id, pk, TEST_NUM_NONCES as usize))
            .sum();
        assert_eq!(action_result.compute_usage, expected_compute);
    }

    #[test]
    fn test_view_gas_key_nonces() {
        let (account_id, public_key, access_key) = test_account_keys();
        let mut state_update = setup_account(&account_id, &public_key, &access_key);
        let mut account = get_account(&state_update, &account_id).unwrap().unwrap();

        let gas_key_public_key =
            InMemorySigner::from_seed(account_id.clone(), KeyType::ED25519, "gas_key").public_key();
        add_gas_key_to_account(&mut state_update, &mut account, &account_id, &gas_key_public_key);

        let viewer = TrieViewer::default();
        let view_gas_key = viewer
            .view_access_key(&state_update, &account_id, &gas_key_public_key)
            .expect("expected to find gas key");
        let AccessKeyPermission::GasKeyFullAccess(gas_key_info) = &view_gas_key.permission else {
            unreachable!();
        };
        let expected = GasKeyInfo { num_nonces: TEST_NUM_NONCES, balance: Balance::ZERO };
        assert_eq!(gas_key_info, &expected);

        let expected_nonce = initial_nonce_value(TEST_GAS_KEY_BLOCK_HEIGHT);
        let view_nonces = viewer
            .view_gas_key_nonces(&state_update, &account_id, &gas_key_public_key)
            .expect("expected to find gas key nonces");
        let expected_nonces = vec![expected_nonce; TEST_NUM_NONCES as usize];
        assert_eq!(view_nonces, expected_nonces);
    }

    /// Gas keys store their nonces as extra rows under the `ACCESS_KEY` column
    /// (the access-key key plus a `NonceIndex` suffix). Those rows must be
    /// filtered out, so each gas key appears in the listing exactly once.
    #[test]
    fn test_view_access_keys_returns_gas_keys() {
        let (account_id, public_key, access_key) = test_account_keys();
        let (tries, mut state_update) =
            setup_account_with_tries(&account_id, &public_key, &access_key);
        let mut account = get_account(&state_update, &account_id).unwrap().unwrap();

        let gas_key_public_key1 =
            InMemorySigner::from_seed(account_id.clone(), KeyType::ED25519, "gas_key1")
                .public_key();
        add_gas_key_to_account(&mut state_update, &mut account, &account_id, &gas_key_public_key1);
        let gas_key_public_key2 =
            InMemorySigner::from_seed(account_id.clone(), KeyType::ED25519, "gas_key2")
                .public_key();
        add_gas_key_to_account(&mut state_update, &mut account, &account_id, &gas_key_public_key2);

        let trie = commit_to_view_trie(&tries, state_update);
        let (access_keys, last_key) = TrieViewer::default()
            .view_access_keys(&trie, &account_id, None, None)
            .expect("expected to find access keys");
        assert_eq!(last_key, None, "unpaginated listing should not return a cursor");

        let expected: HashSet<PublicKeyHandle> =
            [public_key, gas_key_public_key1, gas_key_public_key2]
                .into_iter()
                .map(PublicKeyHandle::from)
                .collect();
        // Length check before de-duplicating: each gas key also has
        // `TEST_NUM_NONCES` nonce rows, so a broken nonce filter would return its
        // handle more than once — which the set comparison alone would hide.
        assert_eq!(access_keys.len(), expected.len(), "gas-key nonce rows were not filtered out");
        let returned: HashSet<PublicKeyHandle> =
            access_keys.into_iter().map(|(pk, _)| pk).collect();
        assert_eq!(returned, expected);
    }

    /// A listing must be bounded to the requested account and never leak another
    /// account's keys. The second account's id has the first's id as a byte
    /// prefix.
    #[test]
    fn test_view_access_keys_excludes_other_accounts() {
        let account_a: AccountId = "alice.near".parse().unwrap();
        let account_b: AccountId = "alice.nearby".parse().unwrap();

        let setup_pk = PublicKey::from_seed(KeyType::ED25519, "a_setup");
        let (tries, mut state_update) =
            setup_account_with_tries(&account_a, &setup_pk, &AccessKey::full_access());

        let mut expected: HashSet<PublicKeyHandle> = HashSet::new();
        expected.insert(PublicKeyHandle::from(setup_pk));
        expected.extend(seed_extra_keys(&mut state_update, &account_a, 3));

        // A second account sharing a leading substring, with its own key.
        let account_b_obj =
            Account::new(Balance::from_yoctonear(100), Balance::ZERO, AccountContract::None, 100);
        set_account(&mut state_update, account_b.clone(), &account_b_obj);
        let b_only = PublicKey::from_seed(KeyType::ED25519, "b_only_key");
        set_access_key(&mut state_update, account_b, b_only.clone(), &AccessKey::full_access());

        let trie = commit_to_view_trie(&tries, state_update);
        let (access_keys, last_key) = TrieViewer::default()
            .view_access_keys(&trie, &account_a, None, None)
            .expect("listing account_a should succeed");
        assert_eq!(last_key, None);
        let returned: HashSet<PublicKeyHandle> =
            access_keys.into_iter().map(|(pk, _)| pk).collect();
        assert!(
            !returned.contains(&PublicKeyHandle::from(b_only)),
            "another account's key leaked into the listing"
        );
        assert_eq!(returned, expected, "listing must return exactly account_a's keys");
    }

    fn viewer_with_limit(limit: u32) -> TrieViewer {
        TrieViewer::new(RuntimeConfigStore::new(None), None, limit, None)
    }

    /// Commits `state_update`'s overlay writes to its backing store and returns a
    /// read-only view `Trie` at the resulting state root, mirroring the
    /// production view path (which iterates a committed trie with no overlay).
    fn commit_to_view_trie(tries: &ShardTries, mut state_update: TrieUpdate) -> Trie {
        state_update.commit(StateChangeCause::InitialState);
        let trie_changes = state_update.finalize().unwrap().trie_changes;
        let mut store_update = tries.store_update();
        let root = tries.apply_all(&trie_changes, ShardUId::single_shard(), &mut store_update);
        store_update.commit();
        tries.get_view_trie_for_shard(ShardUId::single_shard(), root)
    }

    /// Seeds `count` extra plain full-access keys on top of the account's setup
    /// key, returning their handles.
    fn seed_extra_keys(
        state_update: &mut TrieUpdate,
        account_id: &AccountId,
        count: usize,
    ) -> Vec<PublicKeyHandle> {
        let access_key = AccessKey::full_access();
        (0..count)
            .map(|i| {
                let pk = PublicKey::from_seed(KeyType::ED25519, &format!("extra_key_{i}"));
                set_access_key(state_update, account_id.clone(), pk.clone(), &access_key);
                PublicKeyHandle::from(pk)
            })
            .collect()
    }

    #[test]
    fn test_view_access_keys_unpaginated_over_limit_errors() {
        let (account_id, public_key, access_key) = test_account_keys();
        let (tries, mut state_update) =
            setup_account_with_tries(&account_id, &public_key, &access_key);
        // Setup provides 1 key; add 6 more -> 7 total, over the limit of 5.
        seed_extra_keys(&mut state_update, &account_id, 6);

        let trie = commit_to_view_trie(&tries, state_update);
        let err = viewer_with_limit(5)
            .view_access_keys(&trie, &account_id, None, None)
            .expect_err("unpaginated listing over the limit should error");
        assert!(
            matches!(err, ViewAccessKeyError::TooManyAccessKeys { limit: 5, .. }),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn test_view_access_keys_unpaginated_at_limit_ok() {
        let (account_id, public_key, access_key) = test_account_keys();
        let (tries, mut state_update) =
            setup_account_with_tries(&account_id, &public_key, &access_key);
        // Setup provides 1 key; add 4 more -> exactly 5, at the limit.
        seed_extra_keys(&mut state_update, &account_id, 4);

        let trie = commit_to_view_trie(&tries, state_update);
        let (keys, last_key) = viewer_with_limit(5)
            .view_access_keys(&trie, &account_id, None, None)
            .expect("exactly-at-limit unpaginated listing should succeed");
        assert_eq!(keys.len(), 5);
        assert_eq!(last_key, None, "a complete listing should not return a cursor");
    }

    #[test]
    fn test_view_access_keys_paginated_request_over_limit_clamps() {
        let (account_id, public_key, access_key) = test_account_keys();
        let (tries, mut state_update) =
            setup_account_with_tries(&account_id, &public_key, &access_key);
        seed_extra_keys(&mut state_update, &account_id, 6);

        let trie = commit_to_view_trie(&tries, state_update);
        let (keys, last_key) = viewer_with_limit(5)
            .view_access_keys(&trie, &account_id, None, NonZeroU32::new(6))
            .expect("a page larger than the max should be clamped, not rejected");
        assert_eq!(keys.len(), 5, "page should be clamped to the configured max");
        assert!(last_key.is_some(), "a truncated listing should return a resume cursor");
    }

    /// Walks the whole list page by page against a configured limit: the first
    /// page passes an explicit `limit`; resumed pages default their page size to
    /// the config max. Checks full coverage with no duplicates or omissions, and
    /// that gas-key-nonce rows are filtered across page boundaries.
    #[test]
    fn test_view_access_keys_paginated_walk() {
        let (account_id, public_key, access_key) = test_account_keys();
        let (tries, mut state_update) =
            setup_account_with_tries(&account_id, &public_key, &access_key);
        let mut account = get_account(&state_update, &account_id).unwrap().unwrap();

        let mut expected: HashSet<PublicKeyHandle> = HashSet::new();
        expected.insert(PublicKeyHandle::from(public_key));
        expected.extend(seed_extra_keys(&mut state_update, &account_id, 20));
        for i in 0..3 {
            let pk = PublicKey::from_seed(KeyType::ED25519, &format!("gas_key_{i}"));
            add_gas_key_to_account(&mut state_update, &mut account, &account_id, &pk);
            expected.insert(PublicKeyHandle::from(pk));
        }

        let trie = commit_to_view_trie(&tries, state_update);
        let viewer = viewer_with_limit(5);
        let mut collected: HashSet<PublicKeyHandle> = HashSet::new();
        // First page uses an explicit limit; resumes default to the config max.
        let (mut page, mut cursor) = viewer
            .view_access_keys(&trie, &account_id, None, NonZeroU32::new(5))
            .expect("first page should succeed");
        loop {
            assert!(page.len() <= 5, "no page may exceed the config limit");
            for (pk, _) in page {
                assert!(collected.insert(pk), "a key was returned on more than one page");
            }
            let Some(after) = cursor else { break };
            (page, cursor) = viewer
                .view_access_keys(&trie, &account_id, Some(&after), None)
                .expect("resume should succeed");
        }
        assert_eq!(collected, expected, "paged walk did not cover the full key set exactly");
    }

    fn transfer_to_gas_key(
        state_update: &mut TrieUpdate,
        account_id: &AccountId,
        public_key: &PublicKey,
        amount: Balance,
    ) {
        let mut result = ActionResult::default();
        let action = TransferToGasKeyAction { public_key: public_key.clone(), deposit: amount };
        action_transfer_to_gas_key(state_update, &mut result, account_id, &action).unwrap();
        assert!(result.result.is_ok());
    }

    #[test]
    fn test_transfer_to_gas_key_success() {
        let (account_id, public_key, access_key) = test_account_keys();
        let mut state_update = setup_account(&account_id, &public_key, &access_key);
        let mut account = get_account(&state_update, &account_id).unwrap().unwrap();

        let gas_key_public_key =
            InMemorySigner::from_seed(account_id.clone(), KeyType::ED25519, "gas_key").public_key();
        add_gas_key_to_account(&mut state_update, &mut account, &account_id, &gas_key_public_key);

        let deposit_amount = Balance::from_yoctonear(1_000_000);
        transfer_to_gas_key(&mut state_update, &account_id, &gas_key_public_key, deposit_amount);

        let gas_key =
            get_access_key(&state_update, &account_id, &gas_key_public_key).unwrap().unwrap();
        let gas_key_info = gas_key.gas_key_info().unwrap();
        assert_eq!(gas_key_info.balance, deposit_amount);

        // Transfer more and verify accumulation
        transfer_to_gas_key(&mut state_update, &account_id, &gas_key_public_key, deposit_amount);
        let gas_key =
            get_access_key(&state_update, &account_id, &gas_key_public_key).unwrap().unwrap();
        let gas_key_info = gas_key.gas_key_info().unwrap();
        assert_eq!(gas_key_info.balance, Balance::from_yoctonear(2_000_000));
    }

    #[test]
    fn test_transfer_to_gas_key_nonexistent_key() {
        let (account_id, public_key, access_key) = test_account_keys();
        let mut state_update = setup_account(&account_id, &public_key, &access_key);

        let nonexistent_key =
            InMemorySigner::from_seed(account_id.clone(), KeyType::ED25519, "nonexistent")
                .public_key();
        let mut result = ActionResult::default();
        let action = TransferToGasKeyAction {
            public_key: nonexistent_key.clone(),
            deposit: Balance::from_yoctonear(1_000_000),
        };
        action_transfer_to_gas_key(&mut state_update, &mut result, &account_id, &action).unwrap();

        assert_eq!(
            result.result,
            Err(ActionErrorKind::GasKeyDoesNotExist {
                account_id: account_id.clone(),
                public_key: Box::new(nonexistent_key),
            }
            .into())
        );
    }

    #[test]
    fn test_transfer_to_gas_key_not_gas_key() {
        let (account_id, public_key, access_key) = test_account_keys();
        let mut state_update = setup_account(&account_id, &public_key, &access_key);

        // Try to transfer to a regular access key (not a gas key)
        let mut result = ActionResult::default();
        let action = TransferToGasKeyAction {
            public_key: public_key.clone(),
            deposit: Balance::from_yoctonear(1_000_000),
        };
        action_transfer_to_gas_key(&mut state_update, &mut result, &account_id, &action).unwrap();

        assert_eq!(
            result.result,
            Err(ActionErrorKind::GasKeyDoesNotExist {
                account_id: account_id.clone(),
                public_key: Box::new(public_key),
            }
            .into())
        );
    }

    #[test]
    fn test_withdraw_from_gas_key_success() {
        let (account_id, public_key, access_key) = test_account_keys();
        let mut state_update = setup_account(&account_id, &public_key, &access_key);
        let mut account = get_account(&state_update, &account_id).unwrap().unwrap();
        let initial_account_balance = account.amount();

        let gas_key_public_key =
            InMemorySigner::from_seed(account_id.clone(), KeyType::ED25519, "gas_key").public_key();
        add_gas_key_to_account(&mut state_update, &mut account, &account_id, &gas_key_public_key);

        // Fund the gas key
        let deposit_amount = Balance::from_yoctonear(1_000_000);
        transfer_to_gas_key(&mut state_update, &account_id, &gas_key_public_key, deposit_amount);

        // Withdraw some amount
        let withdraw_amount = Balance::from_yoctonear(400_000);
        let mut result = ActionResult::default();
        let action = WithdrawFromGasKeyAction {
            public_key: gas_key_public_key.clone(),
            amount: withdraw_amount,
        };
        action_withdraw_from_gas_key(
            &mut state_update,
            &mut account,
            &mut result,
            &account_id,
            &action,
        )
        .unwrap();
        assert!(result.result.is_ok());

        // Verify gas key balance decreased
        let gas_key =
            get_access_key(&state_update, &account_id, &gas_key_public_key).unwrap().unwrap();
        let gas_key_info = gas_key.gas_key_info().unwrap();
        assert_eq!(gas_key_info.balance, deposit_amount.checked_sub(withdraw_amount).unwrap());

        // Verify account balance increased
        assert_eq!(account.amount(), initial_account_balance.checked_add(withdraw_amount).unwrap());
    }

    #[test]
    fn test_withdraw_from_gas_key_insufficient_balance() {
        let (account_id, public_key, access_key) = test_account_keys();
        let mut state_update = setup_account(&account_id, &public_key, &access_key);
        let mut account = get_account(&state_update, &account_id).unwrap().unwrap();

        let gas_key_public_key =
            InMemorySigner::from_seed(account_id.clone(), KeyType::ED25519, "gas_key").public_key();
        add_gas_key_to_account(&mut state_update, &mut account, &account_id, &gas_key_public_key);

        // Fund with small amount
        let deposit_amount = Balance::from_yoctonear(100);
        transfer_to_gas_key(&mut state_update, &account_id, &gas_key_public_key, deposit_amount);

        // Try to withdraw more than available
        let withdraw_amount = Balance::from_yoctonear(1_000);
        let mut result = ActionResult::default();
        let action = WithdrawFromGasKeyAction {
            public_key: gas_key_public_key.clone(),
            amount: withdraw_amount,
        };
        action_withdraw_from_gas_key(
            &mut state_update,
            &mut account,
            &mut result,
            &account_id,
            &action,
        )
        .unwrap();

        assert_eq!(
            result.result,
            Err(ActionErrorKind::InsufficientGasKeyBalance {
                account_id: account_id.clone(),
                public_key: Box::new(gas_key_public_key),
                balance: deposit_amount,
                required: withdraw_amount,
            }
            .into())
        );
    }

    #[test]
    fn test_withdraw_from_gas_key_nonexistent_key() {
        let (account_id, public_key, access_key) = test_account_keys();
        let mut state_update = setup_account(&account_id, &public_key, &access_key);
        let mut account = get_account(&state_update, &account_id).unwrap().unwrap();

        let nonexistent_key =
            InMemorySigner::from_seed(account_id.clone(), KeyType::ED25519, "nonexistent")
                .public_key();
        let mut result = ActionResult::default();
        let action = WithdrawFromGasKeyAction {
            public_key: nonexistent_key.clone(),
            amount: Balance::from_yoctonear(1_000),
        };
        action_withdraw_from_gas_key(
            &mut state_update,
            &mut account,
            &mut result,
            &account_id,
            &action,
        )
        .unwrap();

        assert_eq!(
            result.result,
            Err(ActionErrorKind::GasKeyDoesNotExist {
                account_id: account_id.clone(),
                public_key: Box::new(nonexistent_key),
            }
            .into())
        );
    }

    #[test]
    fn test_withdraw_from_gas_key_not_gas_key() {
        let (account_id, public_key, access_key) = test_account_keys();
        let mut state_update = setup_account(&account_id, &public_key, &access_key);
        let mut account = get_account(&state_update, &account_id).unwrap().unwrap();

        // Try to withdraw from a regular access key
        let mut result = ActionResult::default();
        let action = WithdrawFromGasKeyAction {
            public_key: public_key.clone(),
            amount: Balance::from_yoctonear(1_000),
        };
        action_withdraw_from_gas_key(
            &mut state_update,
            &mut account,
            &mut result,
            &account_id,
            &action,
        )
        .unwrap();

        assert_eq!(
            result.result,
            Err(ActionErrorKind::GasKeyDoesNotExist {
                account_id: account_id.clone(),
                public_key: Box::new(public_key),
            }
            .into())
        );
    }

    #[test]
    fn test_delete_gas_key_balance_too_high() {
        let (account_id, public_key, access_key) = test_account_keys();
        let mut state_update = setup_account(&account_id, &public_key, &access_key);
        let mut account = get_account(&state_update, &account_id).unwrap().unwrap();

        let gas_key_public_key =
            InMemorySigner::from_seed(account_id.clone(), KeyType::ED25519, "gas_key").public_key();
        add_gas_key_to_account(&mut state_update, &mut account, &account_id, &gas_key_public_key);

        let deposit_amount = Balance::from_near(1).checked_add(Balance::from_yoctonear(1)).unwrap();
        transfer_to_gas_key(&mut state_update, &account_id, &gas_key_public_key, deposit_amount);

        let mut result = ActionResult::default();
        let action = DeleteKeyAction { public_key: gas_key_public_key.clone() };
        action_delete_key(
            &RuntimeConfig::test(),
            &mut state_update,
            &mut account,
            &mut result,
            &account_id,
            &action,
        )
        .unwrap();
        assert_eq!(
            result.result,
            Err(ActionErrorKind::GasKeyBalanceTooHigh {
                account_id: account_id.clone(),
                public_key: Some(Box::new(gas_key_public_key.clone())),
                balance: deposit_amount,
            }
            .into())
        );
        assert_eq!(result.tokens_burnt, Balance::ZERO);

        // Key should still exist
        let stored_key = get_access_key(&state_update, &account_id, &gas_key_public_key).unwrap();
        assert!(stored_key.is_some());
    }

    #[test]
    fn test_delete_gas_key_balance_at_threshold() {
        let (account_id, public_key, access_key) = test_account_keys();
        let mut state_update = setup_account(&account_id, &public_key, &access_key);
        let mut account = get_account(&state_update, &account_id).unwrap().unwrap();

        let gas_key_public_key =
            InMemorySigner::from_seed(account_id.clone(), KeyType::ED25519, "gas_key").public_key();
        add_gas_key_to_account(&mut state_update, &mut account, &account_id, &gas_key_public_key);

        let deposit_amount = Balance::from_near(1);
        transfer_to_gas_key(&mut state_update, &account_id, &gas_key_public_key, deposit_amount);

        let mut result = ActionResult::default();
        let action = DeleteKeyAction { public_key: gas_key_public_key.clone() };
        action_delete_key(
            &RuntimeConfig::test(),
            &mut state_update,
            &mut account,
            &mut result,
            &account_id,
            &action,
        )
        .unwrap();
        assert!(result.result.is_ok());
        assert_eq!(result.tokens_burnt, deposit_amount);

        // Key should be deleted
        let stored_key = get_access_key(&state_update, &account_id, &gas_key_public_key).unwrap();
        assert!(stored_key.is_none());
    }

    #[test]
    fn test_delete_account_gas_key_balance_too_high() {
        let (account_id, public_key, access_key) = test_account_keys();
        let public_keys: Vec<PublicKey> = (0..3)
            .map(|i| PublicKey::from_seed(KeyType::ED25519, &format!("gas_key_{i}")))
            .collect();
        let mut state_update = setup_account(&account_id, &public_key, &access_key);
        let mut account = get_account(&state_update, &account_id).unwrap().unwrap();
        for public_key in &public_keys {
            add_gas_key_to_account(&mut state_update, &mut account, &account_id, public_key);
        }

        // Fund gas keys so total exceeds 1 NEAR
        let deposit_amounts = [
            Balance::from_millinear(400),
            Balance::from_millinear(400),
            Balance::from_millinear(201),
        ];
        for (pk, amount) in public_keys.iter().zip(deposit_amounts.iter()) {
            transfer_to_gas_key(&mut state_update, &account_id, pk, *amount);
        }
        state_update.commit(StateChangeCause::InitialState);

        let action_result = test_delete_account(
            &account_id,
            AccountContract::from_local_code_hash(CryptoHash::default()),
            100,
            PROTOCOL_VERSION,
            &mut state_update,
        );
        let expected_total =
            deposit_amounts.iter().fold(Balance::ZERO, |acc, x| acc.checked_add(*x).unwrap());
        assert_eq!(
            action_result.result,
            Err(ActionErrorKind::GasKeyBalanceTooHigh {
                account_id: account_id.clone(),
                public_key: None,
                balance: expected_total,
            }
            .into())
        );
        assert_eq!(action_result.tokens_burnt, Balance::ZERO);
    }

    #[test]
    fn test_delete_account_gas_key_balance_at_threshold() {
        let (account_id, public_key, access_key) = test_account_keys();
        let public_keys: Vec<PublicKey> = (0..3)
            .map(|i| PublicKey::from_seed(KeyType::ED25519, &format!("gas_key_{i}")))
            .collect();
        let mut state_update = setup_account(&account_id, &public_key, &access_key);
        let mut account = get_account(&state_update, &account_id).unwrap().unwrap();
        for public_key in &public_keys {
            add_gas_key_to_account(&mut state_update, &mut account, &account_id, public_key);
        }

        // Fund gas keys so total is exactly 1 NEAR
        let deposit_amounts = [
            Balance::from_millinear(400),
            Balance::from_millinear(400),
            Balance::from_millinear(200),
        ];
        for (pk, amount) in public_keys.iter().zip(deposit_amounts.iter()) {
            transfer_to_gas_key(&mut state_update, &account_id, pk, *amount);
        }
        state_update.commit(StateChangeCause::InitialState);

        let action_result = test_delete_account(
            &account_id,
            AccountContract::from_local_code_hash(CryptoHash::default()),
            100,
            PROTOCOL_VERSION,
            &mut state_update,
        );
        assert!(action_result.result.is_ok());
        let expected_burnt =
            deposit_amounts.iter().fold(Balance::ZERO, |acc, x| acc.checked_add(*x).unwrap());
        assert_eq!(action_result.tokens_burnt, expected_burnt);
    }

    /// `access_key_storage_usage` uses `public_key.trie_id_len()`, which for
    /// ML-DSA-65 is the SHA3-256 hash form (1 tag + 32 bytes = 33 bytes),
    /// not the 1953-byte borsh-encoded full pubkey. The 32-byte digest is the
    /// same size as an ed25519 key, so the storage usage of an ML-DSA-65
    /// access key matches an ed25519 one exactly - rather than the
    /// ~1920-byte-larger figure a raw pubkey would produce.
    #[test]
    fn test_ml_dsa_65_access_key_storage_scales() {
        let config = RuntimeConfig::test();
        let access_key = AccessKey::full_access();

        let ed_pk: PublicKey =
            near_crypto::SecretKey::from_seed(near_crypto::KeyType::ED25519, "alice-ed")
                .public_key();
        let pq_pk: PublicKey =
            near_crypto::SecretKey::from_seed(near_crypto::KeyType::MLDSA65, "alice-pq")
                .public_key();

        let ed_usage = access_key_storage_usage(&config.fees, &ed_pk, &access_key);
        let pq_usage = access_key_storage_usage(&config.fees, &pq_pk, &access_key);

        // The trie-id encoded lengths are identical:
        // ML-DSA-65: [tag=3] + 32-byte SHA3-256 hash = 33 bytes.
        // ED25519:   [tag=0] + 32-byte raw pubkey    = 33 bytes.
        // So storage usage matches exactly. Stored raw, the ML-DSA-65 pubkey
        // would be 1953 bytes and the usage would be ~1920 bytes larger.
        assert_eq!(
            pq_usage, ed_usage,
            "ML-DSA-65 storage usage ({pq_usage}) must match ed25519 ({ed_usage}); \
             both are 33-byte trie ids"
        );
    }

    /// Pre-hash sanity: the borsh-serialized full pubkey for ML-DSA-65 is
    /// 1953 bytes, but we explicitly do NOT use that length for storage -
    /// `trie_id_len()` is what counts.
    #[test]
    fn test_ml_dsa_65_trie_id_len_is_hash_size() {
        let pq_pk: PublicKey =
            near_crypto::SecretKey::from_seed(near_crypto::KeyType::MLDSA65, "trie-id-len")
                .public_key();
        assert_eq!(pq_pk.trie_id_len(), 1 + 32);
        assert_eq!(pq_pk.len(), 1 + 1952); // borsh form still reports full
    }
}
