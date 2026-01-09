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

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;

    use crate::actions_test_utils::{setup_account, test_delete_large_account};
    use crate::state_viewer::TrieViewer;

    use super::*;
    use near_crypto::{InMemorySigner, KeyType, SecretKey};
    use near_parameters::RuntimeConfig;
    use near_primitives::account::{AccessKey, AccessKeyPermission, GasKeyInfo};
    use near_primitives::apply::ApplyChunkReason;
    use near_primitives::bandwidth_scheduler::BlockBandwidthRequests;
    use near_primitives::congestion_info::BlockCongestionInfo;
    use near_primitives::hash::CryptoHash;
    use near_primitives::trie_key::trie_key_parsers;
    use near_primitives::types::{Balance, BlockHeight, EpochId, NonceIndex, StateChangeCause};
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
    ) -> AccessKey {
        let mut result = ActionResult::default();
        let apply_state = create_apply_state(TEST_GAS_KEY_BLOCK_HEIGHT);
        let action = AddKeyAction {
            public_key: public_key.clone(),
            access_key: AccessKey::gas_key_full_access(TEST_NUM_NONCES),
        };
        action_add_gas_key(&apply_state, state_update, account, &mut result, &account_id, &action)
            .expect("Expect ok");
        assert!(result.result.is_ok(), "Result error: {:?}", result.result);

        get_access_key(state_update, &account_id, &public_key)
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

        let gas_key_public_key =
            InMemorySigner::from_seed(account_id.clone(), KeyType::ED25519, "gas_key").public_key();
        let gas_key = add_gas_key_to_account(
            &mut state_update,
            &mut account,
            &account_id,
            &gas_key_public_key,
        );

        let AccessKeyPermission::GasKeyFullAccess(gas_key_info) = &gas_key.permission else {
            panic!("expected GasKeyFullAccess permission");
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
                    .expect("Failed to get gas key nonce")
                    .expect("Gas key nonce not found");
            assert_eq!(gas_key_nonce, expected_nonce);
        }
    }

    #[test]
    fn test_cannot_add_duplicate_gas_key() {
        let (account_id, public_key, access_key) = test_account_keys();
        let mut state_update = setup_account(&account_id, &public_key, &access_key);
        let mut account =
            get_account(&state_update, &account_id).expect("Failed to get account").unwrap();
        let gas_key_public_key =
            InMemorySigner::from_seed(account_id.clone(), KeyType::ED25519, "gas_key").public_key();
        add_gas_key_to_account(&mut state_update, &mut account, &account_id, &gas_key_public_key);

        let mut result = ActionResult::default();
        let action = AddKeyAction {
            public_key: gas_key_public_key.clone(),
            access_key: AccessKey::gas_key_full_access(TEST_NUM_NONCES),
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
            get_account(&state_update, &account_id).expect("Failed to get account").unwrap();
        let mut result = ActionResult::default();
        let action = AddKeyAction {
            public_key: public_key.clone(),
            access_key: AccessKey::gas_key_full_access(TEST_NUM_NONCES),
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
            Err(ActionErrorKind::AddKeyAlreadyExists {
                account_id: account_id.clone(),
                public_key: public_key.into(),
            }
            .into())
        );
        // TODO(gas-keys): Add a test that adding an access key with same public key as existing gas key fails.
    }

    #[test]
    fn test_delete_gas_key() {
        let (account_id, public_key, access_key) = test_account_keys();
        let mut state_update = setup_account(&account_id, &public_key, &access_key);
        let mut account =
            get_account(&state_update, &account_id).expect("Failed to get account").unwrap();
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
        let action = DeleteGasKeyAction {
            public_key: gas_key_public_key.clone(),
            num_nonces: TEST_NUM_NONCES,
        };
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

        let stored_gas_key = get_access_key(&state_update, &account_id, &gas_key_public_key)
            .expect("Failed to get gas key");
        assert!(stored_gas_key.is_none());
        assert_eq!(account.storage_usage(), storage_before);

        let AccessKeyPermission::GasKeyFullAccess(gas_key_info) = &gas_key.permission else {
            panic!("expected GasKeyFullAccess permission");
        };

        // Check gas key nonces were deleted
        for i in 0..gas_key_info.num_nonces {
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
        let action =
            DeleteGasKeyAction { public_key: public_key.clone(), num_nonces: TEST_NUM_NONCES };
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
            Err(ActionErrorKind::DeleteKeyDoesNotExist {
                account_id: account_id.clone(),
                public_key: public_key.into(),
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
            panic!("expected GasKeyFullAccess permission");
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
}
