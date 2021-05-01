use log::debug;
use near_crypto::{KeyType, PublicKey};
use near_primitives::{
    account::{AccessKey, Account},
    borsh::BorshDeserialize,
    contract::ContractCode,
    hash::CryptoHash,
    receipt::ActionReceipt,
    runtime::{apply_state::ApplyState, config::RuntimeConfig},
    serialize::to_base64,
    transaction::FunctionCallAction,
    trie_key::trie_key_parsers,
    types::{AccountId, EpochInfoProvider},
    views::{StateItem, ViewApplyState, ViewStateResult},
};
use near_runtime_utils::is_valid_account_id;
use near_store::{get_access_key, get_account, get_code, TrieUpdate};
use near_vm_logic::ReturnData;
use std::{str, sync::Arc, time::Instant};

use crate::{actions::execute_function_call, ext::RuntimeExt};

pub mod errors;

pub struct TrieViewer {
    /// Upper bound of the byte size of contract state that is still viewable. None is no limit
    state_size_limit: Option<u64>,
}

impl TrieViewer {
    pub fn new_with_state_size_limit(state_size_limit: Option<u64>) -> Self {
        Self { state_size_limit }
    }

    pub fn view_account(
        &self,
        state_update: &TrieUpdate,
        account_id: &AccountId,
    ) -> Result<Account, errors::ViewAccountError> {
        if !is_valid_account_id(account_id) {
            return Err(errors::ViewAccountError::InvalidAccountId {
                requested_account_id: account_id.clone(),
            });
        }

        get_account(state_update, &account_id)?.ok_or_else(|| {
            errors::ViewAccountError::AccountDoesNotExist {
                requested_account_id: account_id.clone(),
            }
        })
    }

    pub fn view_contract_code(
        &self,
        state_update: &TrieUpdate,
        account_id: &AccountId,
    ) -> Result<ContractCode, errors::ViewContractCodeError> {
        let account = self.view_account(state_update, account_id)?;
        get_code(state_update, account_id, Some(account.code_hash()))?.ok_or_else(|| {
            errors::ViewContractCodeError::NoContractCode {
                contract_account_id: account_id.clone(),
            }
        })
    }

    pub fn view_access_key(
        &self,
        state_update: &TrieUpdate,
        account_id: &AccountId,
        public_key: &PublicKey,
    ) -> Result<AccessKey, errors::ViewAccessKeyError> {
        if !is_valid_account_id(account_id) {
            return Err(errors::ViewAccessKeyError::InvalidAccountId {
                requested_account_id: account_id.clone(),
            });
        }

        get_access_key(state_update, account_id, public_key)?.ok_or_else(|| {
            errors::ViewAccessKeyError::AccessKeyDoesNotExist { public_key: public_key.clone() }
        })
    }

    pub fn view_access_keys(
        &self,
        state_update: &TrieUpdate,
        account_id: &AccountId,
    ) -> Result<Vec<(PublicKey, AccessKey)>, errors::ViewAccessKeyError> {
        if !is_valid_account_id(account_id) {
            return Err(errors::ViewAccessKeyError::InvalidAccountId {
                requested_account_id: account_id.clone(),
            });
        }

        let prefix = trie_key_parsers::get_raw_prefix_for_access_keys(account_id);
        let raw_prefix: &[u8] = prefix.as_ref();
        let access_keys =
            state_update
                .iter(&prefix)?
                .map(|key| {
                    let key = key?;
                    let public_key = &key[raw_prefix.len()..];
                    let access_key = near_store::get_access_key_raw(&state_update, &key)?
                        .ok_or_else(|| errors::ViewAccessKeyError::InternalError {
                            error_message: "Unexpected missing key from iterator".to_string(),
                        })?;
                    PublicKey::try_from_slice(public_key)
                        .map_err(|_| errors::ViewAccessKeyError::InternalError {
                            error_message: format!(
                                "Unexpected invalid public key {:?} received from store",
                                public_key
                            ),
                        })
                        .map(|key| (key, access_key))
                })
                .collect::<Result<Vec<_>, errors::ViewAccessKeyError>>();
        access_keys
    }

    pub fn view_state(
        &self,
        state_update: &TrieUpdate,
        account_id: &AccountId,
        prefix: &[u8],
    ) -> Result<ViewStateResult, errors::ViewStateError> {
        if !is_valid_account_id(account_id) {
            return Err(errors::ViewStateError::InvalidAccountId {
                requested_account_id: account_id.clone(),
            });
        }
        match get_account(state_update, account_id)? {
            Some(account) => {
                let code_len = get_code(state_update, account_id, Some(account.code_hash()))?
                    .map(|c| c.code.len() as u64)
                    .unwrap_or_default();
                if let Some(limit) = self.state_size_limit {
                    if account.storage_usage().saturating_sub(code_len) > limit {
                        return Err(errors::ViewStateError::AccountStateTooLarge {
                            requested_account_id: account_id.clone(),
                        });
                    }
                }
            }
            None => {
                return Err(errors::ViewStateError::AccountDoesNotExist {
                    requested_account_id: account_id.clone(),
                })
            }
        };

        let mut values = vec![];
        let query = trie_key_parsers::get_raw_prefix_for_contract_data(account_id, prefix);
        let acc_sep_len = query.len() - prefix.len();
        let mut iter = state_update.trie.iter(&state_update.get_root())?;
        iter.seek(&query)?;
        for item in iter {
            let (key, value) = item?;
            if !key.starts_with(&query.as_ref()) {
                break;
            }
            values.push(StateItem {
                key: to_base64(&key[acc_sep_len..]),
                value: to_base64(&value),
                proof: vec![],
            });
        }
        // TODO(2076): Add proofs for the storage items.
        Ok(ViewStateResult { values, proof: vec![] })
    }

    pub fn call_function(
        &self,
        mut state_update: TrieUpdate,
        view_state: ViewApplyState,
        contract_id: &AccountId,
        method_name: &str,
        args: &[u8],
        logs: &mut Vec<String>,
        epoch_info_provider: &dyn EpochInfoProvider,
    ) -> Result<Vec<u8>, errors::CallFunctionError> {
        let now = Instant::now();
        if !is_valid_account_id(contract_id) {
            return Err(errors::CallFunctionError::InvalidAccountId {
                requested_account_id: contract_id.clone(),
            });
        }
        let root = state_update.get_root();
        let mut account = get_account(&state_update, contract_id)?.ok_or_else(|| {
            errors::CallFunctionError::AccountDoesNotExist {
                requested_account_id: contract_id.clone(),
            }
        })?;
        // TODO(#1015): Add ability to pass public key and originator_id
        let originator_id = contract_id;
        let public_key = PublicKey::empty(KeyType::ED25519);
        let empty_hash = CryptoHash::default();
        let mut runtime_ext = RuntimeExt::new(
            &mut state_update,
            contract_id,
            originator_id,
            &public_key,
            0,
            &empty_hash,
            &view_state.epoch_id,
            &view_state.prev_block_hash,
            &view_state.block_hash,
            epoch_info_provider,
            view_state.current_protocol_version,
        );
        let config = Arc::new(RuntimeConfig::default());
        let apply_state = ApplyState {
            block_index: view_state.block_height,
            // Used for legacy reasons
            prev_block_hash: view_state.prev_block_hash,
            block_hash: view_state.block_hash,
            epoch_id: view_state.epoch_id.clone(),
            epoch_height: view_state.epoch_height,
            gas_price: 0,
            block_timestamp: view_state.block_timestamp,
            gas_limit: None,
            random_seed: root,
            current_protocol_version: view_state.current_protocol_version,
            config: config.clone(),
            cache: view_state.cache,
            is_new_chunk: false,
            #[cfg(feature = "protocol_feature_evm")]
            evm_chain_id: view_state.evm_chain_id,
            profile: Default::default(),
        };
        let action_receipt = ActionReceipt {
            signer_id: originator_id.clone(),
            signer_public_key: public_key.clone(),
            gas_price: 0,
            output_data_receivers: vec![],
            input_data_ids: vec![],
            actions: vec![],
        };
        let function_call = FunctionCallAction {
            method_name: method_name.to_string(),
            args: args.to_vec(),
            gas: config.wasm_config.limit_config.max_gas_burnt_view,
            deposit: 0,
        };
        let (outcome, err) = execute_function_call(
            &apply_state,
            &mut runtime_ext,
            &mut account,
            &originator_id,
            &action_receipt,
            &[],
            &function_call,
            &empty_hash,
            &config,
            true,
            true,
        );
        let elapsed = now.elapsed();
        let time_ms =
            (elapsed.as_secs() as f64 / 1_000.0) + f64::from(elapsed.subsec_nanos()) / 1_000_000.0;
        let time_str = format!("{:.*}ms", 2, time_ms);

        if let Some(err) = err {
            if let Some(outcome) = outcome {
                logs.extend(outcome.logs);
            }
            let message = format!("wasm execution failed with error: {:?}", err);
            debug!(target: "runtime", "(exec time {}) {}", time_str, message);
            Err(errors::CallFunctionError::VMError { error_message: message })
        } else {
            let outcome = outcome.unwrap();
            debug!(target: "runtime", "(exec time {}) result of execution: {:?}", time_str, outcome);
            logs.extend(outcome.logs);
            let result = match outcome.return_data {
                ReturnData::Value(buf) => buf,
                ReturnData::ReceiptIndex(_) | ReturnData::None => vec![],
            };
            Ok(result)
        }
    }
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "protocol_feature_evm")]
    use near_chain_configs::TESTNET_EVM_CHAIN_ID;
    use near_primitives::{
        test_utils::MockEpochInfoProvider,
        trie_key::TrieKey,
        types::{EpochId, StateChangeCause},
        version::PROTOCOL_VERSION,
    };
    use testlib::runtime_utils::{
        alice_account, encode_int, get_runtime_and_trie, get_test_trie_viewer,
    };

    use super::*;
    use near_store::set_account;

    #[test]
    fn test_view_call() {
        let (viewer, root) = get_test_trie_viewer();

        let mut logs = vec![];
        let view_state = ViewApplyState {
            block_height: 1,
            prev_block_hash: CryptoHash::default(),
            block_hash: CryptoHash::default(),
            epoch_id: EpochId::default(),
            epoch_height: 0,
            block_timestamp: 1,
            current_protocol_version: PROTOCOL_VERSION,
            cache: None,
            #[cfg(feature = "protocol_feature_evm")]
            evm_chain_id: TESTNET_EVM_CHAIN_ID,
        };
        let result = viewer.call_function(
            root,
            view_state,
            &AccountId::from("test.contract"),
            "run_test",
            &[],
            &mut logs,
            &MockEpochInfoProvider::default(),
        );

        assert_eq!(result.unwrap(), encode_int(10));
    }

    #[test]
    fn test_view_call_bad_contract_id() {
        let (viewer, root) = get_test_trie_viewer();

        let mut logs = vec![];
        let view_state = ViewApplyState {
            block_height: 1,
            prev_block_hash: CryptoHash::default(),
            block_hash: CryptoHash::default(),
            epoch_id: EpochId::default(),
            epoch_height: 0,
            block_timestamp: 1,
            current_protocol_version: PROTOCOL_VERSION,
            cache: None,
            #[cfg(feature = "protocol_feature_evm")]
            evm_chain_id: TESTNET_EVM_CHAIN_ID,
        };
        let result = viewer.call_function(
            root,
            view_state,
            &"bad!contract".to_string(),
            "run_test",
            &[],
            &mut logs,
            &MockEpochInfoProvider::default(),
        );

        let err = result.unwrap_err();
        assert!(
            err.to_string().contains(r#"Account ID "bad!contract" is invalid"#),
            "Got different error that doesn't match: {}",
            err
        );
    }

    #[test]
    fn test_view_call_try_changing_storage() {
        let (viewer, root) = get_test_trie_viewer();

        let mut logs = vec![];
        let view_state = ViewApplyState {
            block_height: 1,
            prev_block_hash: CryptoHash::default(),
            block_hash: CryptoHash::default(),
            epoch_id: EpochId::default(),
            epoch_height: 0,
            block_timestamp: 1,
            current_protocol_version: PROTOCOL_VERSION,
            cache: None,
            #[cfg(feature = "protocol_feature_evm")]
            evm_chain_id: 0x99,
        };
        let result = viewer.call_function(
            root,
            view_state,
            &AccountId::from("test.contract"),
            "run_test_with_storage_change",
            &[],
            &mut logs,
            &MockEpochInfoProvider::default(),
        );
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains(r#"ProhibitedInView { method_name: "storage_write" }"#),
            "Got different error that doesn't match: {}",
            err
        );
    }

    #[test]
    fn test_view_call_with_args() {
        let (viewer, root) = get_test_trie_viewer();
        let args: Vec<_> = [1u64, 2u64].iter().flat_map(|x| (*x).to_le_bytes().to_vec()).collect();
        let mut logs = vec![];
        let view_state = ViewApplyState {
            block_height: 1,
            prev_block_hash: CryptoHash::default(),
            block_hash: CryptoHash::default(),
            epoch_id: EpochId::default(),
            epoch_height: 0,
            block_timestamp: 1,
            current_protocol_version: PROTOCOL_VERSION,
            cache: None,
            #[cfg(feature = "protocol_feature_evm")]
            evm_chain_id: 0x99,
        };
        let view_call_result = viewer.call_function(
            root,
            view_state,
            &AccountId::from("test.contract"),
            "sum_with_input",
            &args,
            &mut logs,
            &MockEpochInfoProvider::default(),
        );
        assert_eq!(view_call_result.unwrap(), 3u64.to_le_bytes().to_vec());
    }

    #[test]
    fn test_view_state() {
        let (_, tries, root) = get_runtime_and_trie();
        let mut state_update = tries.new_trie_update(0, root);
        state_update.set(
            TrieKey::ContractData { account_id: alice_account(), key: b"test123".to_vec() },
            b"123".to_vec(),
        );
        state_update.set(
            TrieKey::ContractData { account_id: alice_account(), key: b"test321".to_vec() },
            b"321".to_vec(),
        );
        state_update.set(
            TrieKey::ContractData { account_id: "alina".to_string(), key: b"qqq".to_vec() },
            b"321".to_vec(),
        );
        state_update.set(
            TrieKey::ContractData { account_id: "alex".to_string(), key: b"qqq".to_vec() },
            b"321".to_vec(),
        );
        state_update.commit(StateChangeCause::InitialState);
        let trie_changes = state_update.finalize().unwrap().0;
        let (db_changes, new_root) = tries.apply_all(&trie_changes, 0).unwrap();
        db_changes.commit().unwrap();

        let state_update = tries.new_trie_update(0, new_root);
        let trie_viewer = TrieViewer::new_with_state_size_limit(None);
        let result = trie_viewer.view_state(&state_update, &alice_account(), b"").unwrap();
        assert_eq!(result.proof, Vec::<String>::new());
        assert_eq!(
            result.values,
            [
                StateItem {
                    key: "dGVzdDEyMw==".to_string(),
                    value: "MTIz".to_string(),
                    proof: vec![]
                },
                StateItem {
                    key: "dGVzdDMyMQ==".to_string(),
                    value: "MzIx".to_string(),
                    proof: vec![]
                }
            ]
        );
        let result = trie_viewer.view_state(&state_update, &alice_account(), b"xyz").unwrap();
        assert_eq!(result.values, []);
        let result = trie_viewer.view_state(&state_update, &alice_account(), b"test123").unwrap();
        assert_eq!(
            result.values,
            [StateItem {
                key: "dGVzdDEyMw==".to_string(),
                value: "MTIz".to_string(),
                proof: vec![]
            }]
        );
    }

    #[test]
    fn test_view_state_too_large() {
        let (_, tries, root) = get_runtime_and_trie();
        let mut state_update = tries.new_trie_update(0, root);
        set_account(
            &mut state_update,
            alice_account(),
            &Account::new(0, 0, CryptoHash::default(), 50_001),
        );
        let trie_viewer = TrieViewer::new_with_state_size_limit(Some(50_000));
        let result = trie_viewer.view_state(&state_update, &alice_account(), b"");
        assert!(matches!(result, Err(errors::ViewStateError::AccountStateTooLarge { .. })));
    }

    #[test]
    fn test_view_state_with_large_contract() {
        let (_, tries, root) = get_runtime_and_trie();
        let mut state_update = tries.new_trie_update(0, root);
        set_account(
            &mut state_update,
            alice_account(),
            &Account::new(0, 0, CryptoHash::default(), 50_001),
        );
        state_update.set(
            TrieKey::ContractCode { account_id: alice_account() },
            [0; Account::MAX_ACCOUNT_DELETION_STORAGE_USAGE as usize].to_vec(),
        );
        let trie_viewer = TrieViewer::new_with_state_size_limit(Some(50_000));
        let result = trie_viewer.view_state(&state_update, &alice_account(), b"");
        assert!(result.is_ok());
    }

    #[test]
    fn test_log_when_panic() {
        let (viewer, root) = get_test_trie_viewer();
        let view_state = ViewApplyState {
            block_height: 1,
            prev_block_hash: CryptoHash::default(),
            block_hash: CryptoHash::default(),
            epoch_id: EpochId::default(),
            epoch_height: 0,
            block_timestamp: 1,
            current_protocol_version: PROTOCOL_VERSION,
            cache: None,
            #[cfg(feature = "protocol_feature_evm")]
            evm_chain_id: 0x99,
        };
        let mut logs = vec![];
        viewer
            .call_function(
                root,
                view_state,
                &AccountId::from("test.contract"),
                "panic_after_logging",
                &[],
                &mut logs,
                &MockEpochInfoProvider::default(),
            )
            .unwrap_err();

        assert_eq!(logs, vec!["hello".to_string()]);
    }
}
