use std::mem::size_of;

use near_crypto::PublicKey;
use near_parameters::RuntimeFeesConfig;
use near_primitives::account::{AccessKey, Account, GasKeyInfo};
use near_primitives::action::{TransferToGasKeyAction, WithdrawFromGasKeyAction};
use near_primitives::errors::{ActionErrorKind, RuntimeError};
use near_primitives::transaction::{AddKeyAction, DeleteKeyAction};
use near_primitives::types::{AccountId, BlockHeight, Compute, Nonce, NonceIndex, StorageUsage};

use crate::config::safe_add_compute;
use crate::{ActionResult, ApplyState};
use near_store::{
    StorageError, TrieUpdate, get_access_key, remove_access_key, remove_gas_key_nonce,
    set_access_key, set_gas_key_nonce,
};

fn access_key_storage_usage(
    fee_config: &RuntimeFeesConfig,
    public_key: &PublicKey,
    access_key: &AccessKey,
) -> StorageUsage {
    let storage_usage_config = &fee_config.storage_usage_config;
    borsh::object_length(public_key).unwrap() as u64
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
    let per_nonce_key_size = borsh::object_length(public_key).unwrap() as u64 +  // Public key is part of the key
        size_of::<NonceIndex>() as u64; // Nonce index is part of the key        

    num_nonces as u64
        * (per_nonce_key_size + per_nonce_value_size + storage_config.num_extra_bytes_record)
        + access_key_storage_usage(fee_config, public_key, access_key)
}

/// Returns the compute cost for deleting a single gas key nonce.
fn gas_key_nonce_delete_compute_cost() -> Compute {
    // TODO(gas-keys): properly handle GasKey fees
    near_primitives::gas::Gas::ZERO.as_gas()
}

pub(crate) fn initial_nonce_value(block_height: BlockHeight) -> Nonce {
    // Set default nonce for newly created access key to avoid transaction hash collision.
    // See <https://github.com/near/nearcore/issues/3779>.
    (block_height - 1) * near_primitives::account::AccessKey::ACCESS_KEY_NONCE_RANGE_MULTIPLIER
}

pub(crate) fn action_delete_key(
    fee_config: &RuntimeFeesConfig,
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
                fee_config,
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
                fee_config,
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
    fee_config: &RuntimeFeesConfig,
    state_update: &mut TrieUpdate,
    account: &mut Account,
    result: &mut ActionResult,
    account_id: &AccountId,
    public_key: &PublicKey,
    access_key: &AccessKey,
    gas_key_info: &GasKeyInfo,
) -> Result<(), RuntimeError> {
    for i in 0..gas_key_info.num_nonces {
        remove_gas_key_nonce(state_update, account_id.clone(), public_key.clone(), i);
    }
    let nonce_delete_compute_cost =
        gas_key_nonce_delete_compute_cost() * gas_key_info.num_nonces as u64;
    result.compute_usage = safe_add_compute(result.compute_usage, nonce_delete_compute_cost)?;
    remove_access_key(state_update, account_id.clone(), public_key.clone());
    account.set_storage_usage(account.storage_usage().saturating_sub(gas_key_storage_cost(
        fee_config,
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
    let mut access_key = match get_access_key(state_update, account_id, &action.public_key)? {
        Some(key) => key,
        None => {
            result.result = Err(ActionErrorKind::GasKeyDoesNotExist {
                account_id: account_id.clone(),
                public_key: Box::new(action.public_key.clone()),
            }
            .into());
            return Ok(());
        }
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
    let mut access_key = match get_access_key(state_update, account_id, &action.public_key)? {
        Some(key) => key,
        None => {
            result.result = Err(ActionErrorKind::GasKeyDoesNotExist {
                account_id: account_id.clone(),
                public_key: Box::new(action.public_key.clone()),
            }
            .into());
            return Ok(());
        }
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

    if gas_key_info.balance < action.amount {
        result.result = Err(ActionErrorKind::InsufficientGasKeyBalance {
            account_id: account_id.clone(),
            public_key: Box::new(action.public_key.clone()),
            balance: gas_key_info.balance,
            required: action.amount,
        }
        .into());
        return Ok(());
    }
    gas_key_info.balance = gas_key_info.balance.checked_sub(action.amount).unwrap();
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
    use std::collections::HashSet;
    use std::sync::Arc;

    use crate::ActionResult;
    use crate::ApplyState;
    use crate::actions_test_utils::{setup_account, test_delete_large_account};
    use crate::state_viewer::TrieViewer;

    use super::*;
    use near_crypto::{InMemorySigner, KeyType, SecretKey};
    use near_parameters::RuntimeConfig;
    use near_primitives::account::{AccessKey, AccessKeyPermission, Account, GasKeyInfo};
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
    use near_store::{ShardUId, TrieUpdate, get_access_key, get_account, get_gas_key_nonce};

    const TEST_NUM_NONCES: NonceIndex = 2;
    const TEST_GAS_KEY_BLOCK_HEIGHT: BlockHeight = 10;

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
            cache: None,
            is_new_chunk: false,
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
            &RuntimeFeesConfig::test(),
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
            &RuntimeFeesConfig::test(),
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
        // TODO(gas-keys): This test will change when account deletion fails if gas keys exist.
        let (account_id, public_key, access_key) = test_account_keys();
        let public_keys: Vec<PublicKey> =
            (0..3).map(|_| SecretKey::from_random(KeyType::ED25519).public_key()).collect();
        let mut state_update = setup_account(&account_id, &public_key, &access_key);
        let mut account = get_account(&state_update, &account_id).unwrap().unwrap();
        for public_key in &public_keys {
            add_gas_key_to_account(&mut state_update, &mut account, &account_id, public_key);
        }
        state_update.commit(StateChangeCause::InitialState);

        let action_result =
            test_delete_large_account(&account_id, &CryptoHash::default(), 100, &mut state_update);
        assert!(action_result.result.is_ok());
        state_update.commit(StateChangeCause::InitialState);

        let lock = state_update.trie().lock_for_iter();
        let trie_key_count = state_update
            .locked_iter(&trie_key_parsers::get_raw_prefix_for_access_keys(&account_id), &lock)
            .expect("could not get trie iterator")
            .count();
        assert_eq!(trie_key_count, 0);
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

    #[test]
    fn test_view_access_keys_returns_gas_keys() {
        let (account_id, public_key, access_key) = test_account_keys();
        let mut state_update = setup_account(&account_id, &public_key, &access_key);
        let mut account = get_account(&state_update, &account_id).unwrap().unwrap();

        let gas_key_public_key1 =
            InMemorySigner::from_seed(account_id.clone(), KeyType::ED25519, "gas_key1")
                .public_key();
        add_gas_key_to_account(&mut state_update, &mut account, &account_id, &gas_key_public_key1);
        let gas_key_public_key2 =
            InMemorySigner::from_seed(account_id.clone(), KeyType::ED25519, "gas_key2")
                .public_key();
        add_gas_key_to_account(&mut state_update, &mut account, &account_id, &gas_key_public_key2);

        let viewer = TrieViewer::default();
        let access_keys = viewer
            .view_access_keys(&state_update, &account_id)
            .expect("expected to find access keys");
        let public_keys = access_keys.into_iter().map(|(pk, _)| pk).collect::<HashSet<PublicKey>>();
        let expected_public_keys = vec![public_key, gas_key_public_key1, gas_key_public_key2]
            .into_iter()
            .collect::<HashSet<PublicKey>>();
        assert_eq!(public_keys, expected_public_keys);
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
}
