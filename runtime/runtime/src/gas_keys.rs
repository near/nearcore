use near_crypto::PublicKey;
use near_parameters::RuntimeFeesConfig;
use near_primitives::account::{Account, GasKey};
use near_primitives::action::{AddGasKeyAction, DeleteGasKeyAction, TransferToGasKeyAction};
use near_primitives::errors::ActionErrorKind;
use near_primitives::types::{AccountId, Balance, Nonce, NonceIndex, StorageUsage};
use near_store::{
    StorageError, TrieUpdate, get_gas_key, remove_gas_key, remove_gas_key_nonce, set_gas_key,
    set_gas_key_nonce,
};

use crate::{ActionResult, ApplyState};

pub(crate) fn action_transfer_to_gas_key(
    state_update: &mut TrieUpdate,
    account_id: &AccountId,
    action: &TransferToGasKeyAction,
    result: &mut ActionResult,
) -> Result<(), StorageError> {
    if let Some(mut gas_key) = get_gas_key(state_update, account_id, &action.public_key)? {
        gas_key.balance = gas_key.balance.checked_add(action.deposit).ok_or_else(|| {
            StorageError::StorageInconsistentState("Gas key balance integer overflow".to_string())
        })?;
        set_gas_key(state_update, account_id.clone(), action.public_key.clone(), &gas_key);
    } else {
        result.result = Err(ActionErrorKind::GasKeyDoesNotExist {
            account_id: account_id.clone(),
            public_key: action.public_key.clone().into(),
        }
        .into());
    }
    Ok(())
}

pub(crate) fn action_add_gas_key(
    apply_state: &ApplyState,
    state_update: &mut TrieUpdate,
    account: &mut Account,
    result: &mut ActionResult,
    account_id: &AccountId,
    add_gas_key: &AddGasKeyAction,
) -> Result<(), StorageError> {
    if get_gas_key(state_update, account_id, &add_gas_key.public_key)?.is_some() {
        result.result = Err(ActionErrorKind::GasKeyAlreadyExists {
            account_id: account_id.to_owned(),
            public_key: add_gas_key.public_key.clone().into(),
        }
        .into());
        return Ok(());
    }
    let gas_key = GasKey {
        num_nonces: add_gas_key.num_nonces,
        permission: add_gas_key.permission.clone(),
        balance: Balance::ZERO,
    };
    set_gas_key(state_update, account_id.clone(), add_gas_key.public_key.clone(), &gas_key);

    for i in 0..gas_key.num_nonces {
        let nonce = (apply_state.block_height - 1)
            * near_primitives::account::AccessKey::ACCESS_KEY_NONCE_RANGE_MULTIPLIER;
        set_gas_key_nonce(
            state_update,
            account_id.clone(),
            add_gas_key.public_key.clone(),
            i,
            nonce,
        );
    }
    account.set_storage_usage(
        account
            .storage_usage()
            .checked_add(gas_key_storage_cost(
                &apply_state.config.fees,
                &add_gas_key.public_key,
                &gas_key,
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
    let gas_key = get_gas_key(state_update, account_id, &delete_gas_key.public_key)?;
    if let Some(gas_key) = gas_key {
        // TODO(gas-key): Add check for too high balance (for user convenience), as balance will be burned.
        remove_gas_key(state_update, account_id.clone(), delete_gas_key.public_key.clone());
        for i in 0..gas_key.num_nonces {
            remove_gas_key_nonce(
                state_update,
                account_id.clone(),
                delete_gas_key.public_key.clone(),
                i,
            );
        }
        account.set_storage_usage(account.storage_usage().saturating_sub(gas_key_storage_cost(
            fee_config,
            &delete_gas_key.public_key,
            &gas_key,
        )));
    } else {
        result.result = Err(ActionErrorKind::GasKeyDoesNotExist {
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
    gas_key: &GasKey,
) -> StorageUsage {
    let storage_config = &fee_config.storage_usage_config;
    let nonce_storage_usage = gas_key.num_nonces as u64
        * (borsh::object_length(&(0 as NonceIndex)).unwrap() as u64 + // NonceIndex is part of the key
            borsh::object_length(&(0 as Nonce)).unwrap() as u64 + // Value of nonce
            storage_config.num_extra_bytes_record);

    borsh::object_length(public_key).unwrap() as u64
        + borsh::object_length(gas_key).unwrap() as u64
        + storage_config.num_extra_bytes_record
        + nonce_storage_usage
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::actions_test_utils::{setup_account, test_delete_large_account};

    use super::*;
    use near_crypto::{KeyType, SecretKey};
    use near_parameters::RuntimeConfig;
    use near_primitives::account::{AccessKey, AccessKeyPermission};
    use near_primitives::apply::ApplyChunkReason;
    use near_primitives::bandwidth_scheduler::BlockBandwidthRequests;
    use near_primitives::congestion_info::BlockCongestionInfo;
    use near_primitives::hash::CryptoHash;
    use near_primitives::trie_key::trie_key_parsers;
    use near_primitives::types::{BlockHeight, EpochId, NonceIndex, StateChangeCause};
    use near_store::{ShardUId, TrieUpdate, get_account, get_gas_key_nonce};

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
    ) -> GasKey {
        let mut result = ActionResult::default();
        let apply_state = create_apply_state(TEST_GAS_KEY_BLOCK_HEIGHT);
        let action = AddGasKeyAction {
            public_key: public_key.clone(),
            num_nonces: TEST_NUM_NONCES,
            permission: AccessKeyPermission::FullAccess,
        };
        action_add_gas_key(&apply_state, state_update, account, &mut result, &account_id, &action)
            .expect("Expect ok");
        assert!(result.result.is_ok(), "Result error: {:?}", result.result);

        get_gas_key(state_update, &account_id, &public_key)
            .expect("could not find gas key")
            .unwrap()
    }

    #[test]
    fn test_add_gas_key() {
        let (account_id, public_key, access_key) = test_account_keys();
        let mut state_update = setup_account(&account_id, &public_key, &access_key);
        let mut account =
            get_account(&state_update, &account_id).expect("Failed to get account").unwrap();
        let storage_before = account.storage_usage();

        let gas_key =
            add_gas_key_to_account(&mut state_update, &mut account, &account_id, &public_key);

        assert_eq!(gas_key.num_nonces, TEST_NUM_NONCES);
        assert_eq!(gas_key.balance, Balance::ZERO);
        assert_eq!(gas_key.permission, AccessKeyPermission::FullAccess);
        assert!(account.storage_usage() > storage_before);
        assert_eq!(
            account.storage_usage(),
            storage_before
                + gas_key_storage_cost(&RuntimeFeesConfig::test(), &public_key, &gas_key)
        );

        // Check gas key nonces were initialized
        let expected_nonce = (TEST_GAS_KEY_BLOCK_HEIGHT - 1)
            * near_primitives::account::AccessKey::ACCESS_KEY_NONCE_RANGE_MULTIPLIER;
        for i in 0..gas_key.num_nonces {
            let gas_key_nonce = get_gas_key_nonce(&state_update, &account_id, &public_key, i)
                .expect("Failed to get gas key nonce")
                .expect("Gas key nonce not found");
            assert_eq!(gas_key_nonce, expected_nonce);
        }
    }

    #[test]
    fn test_add_duplicate_gas_key() {
        let (account_id, public_key, access_key) = test_account_keys();
        let mut state_update = setup_account(&account_id, &public_key, &access_key);
        let mut account =
            get_account(&state_update, &account_id).expect("Failed to get account").unwrap();
        add_gas_key_to_account(&mut state_update, &mut account, &account_id, &public_key);

        let mut result = ActionResult::default();
        let action = AddGasKeyAction {
            public_key: public_key.clone(),
            num_nonces: TEST_NUM_NONCES,
            permission: AccessKeyPermission::FullAccess,
        };
        action_add_gas_key(
            &create_apply_state(TEST_GAS_KEY_BLOCK_HEIGHT),
            &mut state_update,
            &mut account,
            &mut result,
            &account_id,
            &action,
        )
        .expect("Expect ok");
        assert_eq!(
            result.result,
            Err(ActionErrorKind::GasKeyAlreadyExists {
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
            get_account(&state_update, &account_id).expect("Failed to get account").unwrap();
        let storage_before = account.storage_usage();
        let gas_key =
            add_gas_key_to_account(&mut state_update, &mut account, &account_id, &public_key);

        let mut result = ActionResult::default();
        let action = DeleteGasKeyAction { public_key: public_key.clone() };
        action_delete_gas_key(
            &RuntimeFeesConfig::test(),
            &mut state_update,
            &mut account,
            &mut result,
            &account_id,
            &action,
        )
        .expect("Expect ok");
        assert!(result.result.is_ok(), "Result error: {:?}", result.result);

        let stored_gas_key =
            get_gas_key(&state_update, &account_id, &public_key).expect("Failed to get gas key");
        assert!(stored_gas_key.is_none());
        assert_eq!(account.storage_usage(), storage_before);

        // Check gas key nonces were deleted
        for i in 0..gas_key.num_nonces {
            let gas_key_nonce = get_gas_key_nonce(&state_update, &account_id, &public_key, i)
                .expect("Failed to get gas key nonce");
            assert!(gas_key_nonce.is_none());
        }
    }

    #[test]
    fn test_delete_nonexistent_gas_key() {
        let (account_id, public_key, access_key) = test_account_keys();
        let mut state_update = setup_account(&account_id, &public_key, &access_key);
        let mut account =
            get_account(&state_update, &account_id).expect("Failed to get account").unwrap();

        let mut result = ActionResult::default();
        let action = DeleteGasKeyAction { public_key: public_key.clone() };
        action_delete_gas_key(
            &RuntimeFeesConfig::test(),
            &mut state_update,
            &mut account,
            &mut result,
            &account_id,
            &action,
        )
        .expect("Expect ok");
        assert_eq!(
            result.result,
            Err(ActionErrorKind::GasKeyDoesNotExist {
                account_id: account_id.clone(),
                public_key: public_key.into(),
            }
            .into())
        );
    }

    #[test]
    fn test_transfer_to_gas_key() {
        let (account_id, public_key, access_key) = test_account_keys();
        let mut state_update = setup_account(&account_id, &public_key, &access_key);
        let mut account =
            get_account(&state_update, &account_id).expect("Failed to get account").unwrap();
        add_gas_key_to_account(&mut state_update, &mut account, &account_id, &public_key);

        let action = TransferToGasKeyAction {
            public_key: public_key.clone(),
            deposit: Balance::from_near(1),
        };
        let mut result = ActionResult::default();
        action_transfer_to_gas_key(&mut state_update, &account_id, &action, &mut result)
            .expect("Expect ok");
        assert!(result.result.is_ok(), "Result error: {:?}", result.result);

        let stored_gas_key = get_gas_key(&state_update, &account_id, &public_key)
            .expect("Failed to get gas key")
            .expect("Gas key not found");
        assert_eq!(stored_gas_key.balance, action.deposit);
    }

    #[test]
    fn test_transfer_to_nonexistent_gas_key() {
        let (account_id, public_key, access_key) = test_account_keys();
        let mut state_update = setup_account(&account_id, &public_key, &access_key);

        let mut result = ActionResult::default();
        let deposit = Balance::from_near(1);
        let action = TransferToGasKeyAction { public_key: public_key.clone(), deposit };
        action_transfer_to_gas_key(&mut state_update, &account_id, &action, &mut result)
            .expect("Expect ok");
        assert_eq!(
            result.result,
            Err(ActionErrorKind::GasKeyDoesNotExist {
                account_id: account_id.clone(),
                public_key: public_key.into(),
            }
            .into())
        );
    }

    #[test]
    fn test_delete_account_removes_gas_keys() {
        let account_id: AccountId = "alice".parse().unwrap();
        let public_keys: Vec<PublicKey> =
            (0..3).map(|_| SecretKey::from_random(KeyType::ED25519).public_key()).collect();
        let mut state_update = setup_account(
            &account_id,
            &public_keys[0],
            &AccessKey { nonce: 0, permission: AccessKeyPermission::FullAccess },
        );
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
        let gas_key_count = state_update
            .locked_iter(&trie_key_parsers::get_raw_prefix_for_gas_keys(&account_id), &lock)
            .expect("could not get trie iterator")
            .count();
        assert_eq!(gas_key_count, 0);
    }
}
