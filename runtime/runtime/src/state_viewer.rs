use std::collections::HashMap;
use std::str;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use borsh::BorshSerialize;

use near_crypto::{KeyType, PublicKey};
use near_primitives::account::{AccessKey, Account};
use near_primitives::hash::CryptoHash;
use near_primitives::types::AccountId;
use near_primitives::utils::{is_valid_account_id, prefix_for_data};
use near_primitives::views::ViewStateResult;
use near_store::{get_access_key, get_account, TrieUpdate};
use near_vm_logic::{Config, ReturnData, VMContext};

use crate::actions::get_code_with_cache;
use crate::ethereum::EthashProvider;
use crate::ext::RuntimeExt;

#[allow(dead_code)]
pub struct TrieViewer {
    ethash_provider: Arc<Mutex<EthashProvider>>,
}

impl TrieViewer {
    pub fn new(ethash_provider: Arc<Mutex<EthashProvider>>) -> Self {
        Self { ethash_provider }
    }

    pub fn view_account(
        &self,
        state_update: &TrieUpdate,
        account_id: &AccountId,
    ) -> Result<Account, Box<dyn std::error::Error>> {
        if !is_valid_account_id(account_id) {
            return Err(format!("Account ID '{}' is not valid", account_id).into());
        }

        get_account(state_update, &account_id)?
            .ok_or_else(|| format!("account {} does not exist while viewing", account_id).into())
    }

    pub fn view_access_key(
        &self,
        state_update: &TrieUpdate,
        account_id: &AccountId,
        public_key: &PublicKey,
    ) -> Result<Option<AccessKey>, Box<dyn std::error::Error>> {
        if !is_valid_account_id(account_id) {
            return Err(format!("Account ID '{}' is not valid", account_id).into());
        }

        get_access_key(state_update, account_id, public_key).map_err(|e| Box::new(e).into())
    }

    pub fn view_state(
        &self,
        state_update: &TrieUpdate,
        account_id: &AccountId,
        prefix: &[u8],
    ) -> Result<ViewStateResult, Box<dyn std::error::Error>> {
        if !is_valid_account_id(account_id) {
            return Err(format!("Account ID '{}' is not valid", account_id).into());
        }
        let mut values = HashMap::default();
        let mut query = prefix_for_data(account_id);
        let acc_sep_len = query.len();
        query.extend_from_slice(prefix);
        state_update.for_keys_with_prefix(&query, |key| {
            // TODO error
            if let Ok(Some(value)) = state_update.get(key) {
                values.insert(key[acc_sep_len..].to_vec(), value.to_vec());
            }
        });
        Ok(ViewStateResult { values })
    }

    pub fn call_function(
        &self,
        mut state_update: TrieUpdate,
        block_index: u64,
        block_timestamp: u64,
        contract_id: &AccountId,
        method_name: &str,
        args: &[u8],
        logs: &mut Vec<String>,
    ) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let now = Instant::now();
        if !is_valid_account_id(contract_id) {
            return Err(format!("Contract ID {:?} is not valid", contract_id).into());
        }
        let root = state_update.get_root();
        let account = get_account(&state_update, contract_id)?
            .ok_or_else(|| format!("Account {:?} doesn't exists", contract_id))?;
        let code = get_code_with_cache(&state_update, contract_id, &account)?.ok_or_else(|| {
            format!("cannot find contract code for account {}", contract_id.clone())
        })?;
        // TODO(#1015): Add ability to pass public key and originator_id
        let originator_id = contract_id;
        let public_key = PublicKey::empty(KeyType::ED25519);
        let (outcome, err) = {
            let empty_hash = CryptoHash::default();
            let mut runtime_ext = RuntimeExt::new(
                &mut state_update,
                contract_id,
                originator_id,
                &public_key,
                0,
                &empty_hash,
            );

            let context = VMContext {
                current_account_id: contract_id.clone(),
                signer_account_id: originator_id.clone(),
                signer_account_pk: public_key.try_to_vec().expect("Failed to serialize"),
                predecessor_account_id: originator_id.clone(),
                input: args.to_owned(),
                block_index,
                block_timestamp,
                account_balance: account.amount,
                storage_usage: account.storage_usage,
                attached_deposit: 0,
                prepaid_gas: 0,
                random_seed: root.as_ref().into(),
                free_of_charge: true,
                output_data_receivers: vec![],
            };

            near_vm_runner::run(
                code.hash.as_ref().to_vec(),
                &code.code,
                method_name.as_bytes(),
                &mut runtime_ext,
                context,
                &Config::default(),
                &[],
            )
        };
        let elapsed = now.elapsed();
        let time_ms =
            (elapsed.as_secs() as f64 / 1_000.0) + f64::from(elapsed.subsec_nanos()) / 1_000_000.0;
        let time_str = format!("{:.*}ms", 2, time_ms);

        if let Some(err) = err {
            let message = format!("wasm execution failed with error: {:?}", err);
            debug!(target: "runtime", "(exec time {}) {}", time_str, message);
            Err(message.into())
        } else {
            let outcome = outcome.unwrap();
            debug!(target: "runtime", "(exec time {}) result of execution: {:#?}", time_str, outcome);
            logs.extend(outcome.logs);
            let trie_update = state_update.finalize()?;
            if trie_update.new_root != root {
                return Err("function call for viewing tried to change storage".into());
            }
            let mut result = vec![];
            if let ReturnData::Value(buf) = &outcome.return_data {
                result = buf.clone();
            }
            Ok(result)
        }
    }
}

#[cfg(test)]
mod tests {
    use kvdb::DBValue;
    use tempdir::TempDir;

    use near_primitives::utils::key_for_data;
    use testlib::runtime_utils::{
        alice_account, encode_int, get_runtime_and_trie, get_test_trie_viewer,
    };

    use super::*;

    #[test]
    fn test_view_call() {
        let (viewer, root) = get_test_trie_viewer();

        let mut logs = vec![];
        let result = viewer.call_function(root, 1, 1, &alice_account(), "run_test", &[], &mut logs);

        assert_eq!(result.unwrap(), encode_int(10));
    }

    #[test]
    fn test_view_call_bad_contract_id() {
        let (viewer, root) = get_test_trie_viewer();

        let mut logs = vec![];
        let result =
            viewer.call_function(root, 1, 1, &"bad!contract".to_string(), "run_test", &[], &mut logs);

        assert!(result.is_err());
    }

    #[test]
    fn test_view_call_try_changing_storage() {
        let (viewer, root) = get_test_trie_viewer();

        let mut logs = vec![];
        let result = viewer.call_function(
            root,
            1,
            1,
            &alice_account(),
            "run_test_with_storage_change",
            &[],
            &mut logs,
        );
        // run_test tries to change storage, so it should fail
        assert!(result.is_err());
    }

    #[test]
    fn test_view_call_with_args() {
        let (viewer, root) = get_test_trie_viewer();
        let args: Vec<_> = [1u64, 2u64].iter().flat_map(|x| (*x).to_le_bytes().to_vec()).collect();
        let mut logs = vec![];
        let view_call_result =
            viewer.call_function(root, 1, 1, &alice_account(), "sum_with_input", &args, &mut logs);
        assert_eq!(view_call_result.unwrap(), 3u64.to_le_bytes().to_vec());
    }

    #[test]
    fn test_view_state() {
        let (_, trie, root) = get_runtime_and_trie();
        let mut state_update = TrieUpdate::new(trie.clone(), root);
        state_update.set(key_for_data(&alice_account(), b"test123"), DBValue::from_slice(b"123"));
        let (db_changes, new_root) = state_update.finalize().unwrap().into(trie.clone()).unwrap();
        db_changes.commit().unwrap();

        let state_update = TrieUpdate::new(trie, new_root);
        let ethash_provider =
            EthashProvider::new(TempDir::new("runtime_user_test_ethash").unwrap().path());
        let trie_viewer = TrieViewer::new(Arc::new(Mutex::new(ethash_provider)));
        let result = trie_viewer.view_state(&state_update, &alice_account(), b"").unwrap();
        assert_eq!(
            result.values,
            [(b"test123".to_vec(), b"123".to_vec())].iter().cloned().collect()
        );
        let result = trie_viewer.view_state(&state_update, &alice_account(), b"test321").unwrap();
        assert_eq!(result.values, [].iter().cloned().collect());
        let result = trie_viewer.view_state(&state_update, &alice_account(), b"test123").unwrap();
        assert_eq!(
            result.values,
            [(b"test123".to_vec(), b"123".to_vec())].iter().cloned().collect()
        )
    }
}
