use std::collections::HashMap;
use std::str;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use near_primitives::account::{AccessKey, Account};
use near_primitives::crypto::signature::PublicKey;
use near_primitives::hash::CryptoHash;
use near_primitives::rpc::{AccountViewCallResult, ViewStateResult};
use near_primitives::types::AccountId;
use near_primitives::utils::{is_valid_account_id, key_for_access_key, key_for_account};
use near_store::{get, TrieUpdate};
use wasm::executor;
use wasm::types::{ReturnData, RuntimeContext};

use crate::ethereum::EthashProvider;
use crate::Runtime;

use super::ext::ACCOUNT_DATA_SEPARATOR;
use super::RuntimeExt;

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
    ) -> Result<AccountViewCallResult, Box<dyn std::error::Error>> {
        if !is_valid_account_id(account_id) {
            return Err(format!("Account ID '{}' is not valid", account_id).into());
        }

        match get::<Account>(state_update, &key_for_account(account_id)) {
            Some(account) => Ok(AccountViewCallResult {
                account_id: account_id.clone(),
                nonce: account.nonce,
                amount: account.amount,
                stake: account.staked,
                public_keys: account.public_keys,
                code_hash: account.code_hash,
            }),
            _ => Err(format!("account {} does not exist while viewing", account_id).into()),
        }
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

        Ok(get(state_update, &key_for_access_key(account_id, public_key)))
    }

    pub fn get_public_keys_for_account(
        &self,
        state_update: &TrieUpdate,
        account_id: &AccountId,
    ) -> Result<Vec<PublicKey>, Box<dyn std::error::Error>> {
        self.view_account(state_update, account_id).map(|account| account.public_keys)
    }

    pub fn view_state(
        &self,
        state_update: &TrieUpdate,
        account_id: &AccountId,
    ) -> Result<ViewStateResult, Box<dyn std::error::Error>> {
        if !is_valid_account_id(account_id) {
            return Err(format!("Account ID '{}' is not valid", account_id).into());
        }
        let mut values = HashMap::default();
        let mut prefix = key_for_account(account_id);
        prefix.extend_from_slice(ACCOUNT_DATA_SEPARATOR);
        state_update.for_keys_with_prefix(&prefix, |key| {
            if let Some(value) = state_update.get(key) {
                values.insert(key[prefix.len()..].to_vec(), value.to_vec());
            }
        });
        Ok(ViewStateResult { values })
    }

    pub fn call_function(
        &self,
        mut state_update: TrieUpdate,
        block_index: u64,
        contract_id: &AccountId,
        method_name: &str,
        args: &[u8],
        logs: &mut Vec<String>,
    ) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let now = Instant::now();
        if !is_valid_account_id(contract_id) {
            return Err(format!("Contract ID '{}' is not valid", contract_id).into());
        }
        let root = state_update.get_root();
        let code = Runtime::get_code(&state_update, contract_id)?;
        // TODO(#1015): Add ability to pass public key and originator_id
        let originator_id = contract_id;
        let public_key = PublicKey::empty();
        let wasm_res = match get::<Account>(&state_update, &key_for_account(contract_id)) {
            Some(account) => {
                let empty_hash = CryptoHash::default();
                let mut runtime_ext = RuntimeExt::new(
                    &mut state_update,
                    contract_id,
                    originator_id,
                    &empty_hash,
                    self.ethash_provider.clone(),
                    originator_id,
                    &public_key,
                );
                executor::execute(
                    &code,
                    method_name.as_bytes(),
                    &args.to_owned(),
                    &[],
                    &mut runtime_ext,
                    &wasm::types::Config::default(),
                    &RuntimeContext::new(
                        account.amount,
                        0,
                        originator_id,
                        contract_id,
                        0,
                        block_index,
                        root.as_ref().into(),
                        true,
                        originator_id,
                        &public_key,
                    ),
                )
            }
            None => return Err(format!("contract {} does not exist", contract_id).into()),
        };
        let elapsed = now.elapsed();
        let time_ms =
            (elapsed.as_secs() as f64 / 1_000.0) + f64::from(elapsed.subsec_nanos()) / 1_000_000.0;
        let time_str = format!("{:.*}ms", 2, time_ms);
        match wasm_res {
            Ok(res) => {
                debug!(target: "runtime", "(exec time {}) result of execution: {:#?}", time_str, res);
                logs.extend(res.logs);
                match res.return_data {
                    Ok(return_data) => {
                        let trie_update = state_update.finalize()?;
                        if trie_update.new_root != root {
                            return Err("function call for viewing tried to change storage".into());
                        }
                        let mut result = vec![];
                        if let ReturnData::Value(buf) = return_data {
                            result.extend(&buf);
                        }
                        Ok(result)
                    }
                    Err(e) => {
                        let message =
                            format!("wasm view call execution failed with error: {:?}", e);
                        debug!(target: "runtime", "{}", message);
                        Err(message.into())
                    }
                }
            }
            Err(e) => {
                let message = format!("wasm execution failed with error: {:?}", e);
                debug!(target: "runtime", "(exec time {}) {}", time_str, message);
                Err(message.into())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use kvdb::DBValue;
    use tempdir::TempDir;

    use near_primitives::types::AccountId;
    use testlib::runtime_utils::{
        alice_account_id, encode_int, get_runtime_and_trie, get_test_trie_viewer,
    };

    use super::*;

    #[test]
    fn test_view_call() {
        let (viewer, root) = get_test_trie_viewer();

        let mut logs = vec![];
        let result =
            viewer.call_function(root, 1, &alice_account_id(), "run_test", &vec![], &mut logs);

        assert_eq!(result.unwrap(), encode_int(10));
    }

    #[test]
    fn test_view_call_bad_contract_id() {
        let (viewer, root) = get_test_trie_viewer();

        let mut logs = vec![];
        let result = viewer.call_function(
            root,
            1,
            &"bad!contract".to_string(),
            "run_test",
            &vec![],
            &mut logs,
        );

        assert!(result.is_err());
    }

    #[test]
    fn test_view_call_try_changing_storage() {
        let (viewer, root) = get_test_trie_viewer();

        let mut logs = vec![];
        let result = viewer.call_function(
            root,
            1,
            &alice_account_id(),
            "run_test_with_storage_change",
            &vec![],
            &mut logs,
        );
        // run_test tries to change storage, so it should fail
        assert!(result.is_err());
    }

    #[test]
    fn test_view_call_with_args() {
        let (viewer, root) = get_test_trie_viewer();
        let args = (1..3).into_iter().flat_map(|x| encode_int(x).to_vec()).collect::<Vec<_>>();
        let mut logs = vec![];
        let view_call_result =
            viewer.call_function(root, 1, &alice_account_id(), "sum_with_input", &args, &mut logs);
        assert_eq!(view_call_result.unwrap(), encode_int(3).to_vec());
    }

    fn account_suffix(account_id: &AccountId, suffix: &[u8]) -> Vec<u8> {
        let mut bytes = key_for_account(account_id);
        bytes.append(&mut ACCOUNT_DATA_SEPARATOR.to_vec());
        bytes.append(&mut suffix.clone().to_vec());
        bytes
    }

    #[test]
    fn test_view_state() {
        let (_, trie, root) = get_runtime_and_trie();
        let mut state_update = TrieUpdate::new(trie.clone(), root);
        state_update
            .set(account_suffix(&alice_account_id(), b"test123"), DBValue::from_slice(b"123"));
        let (db_changes, new_root) = state_update.finalize().unwrap().into(trie.clone()).unwrap();
        db_changes.commit().unwrap();

        let state_update = TrieUpdate::new(trie, new_root);
        let ethash_provider =
            EthashProvider::new(TempDir::new("runtime_user_test_ethash").unwrap().path());
        let trie_viewer = TrieViewer::new(Arc::new(Mutex::new(ethash_provider)));
        let result = trie_viewer.view_state(&state_update, &alice_account_id()).unwrap();
        assert_eq!(
            result.values,
            [(b"test123".to_vec(), b"123".to_vec())].iter().cloned().collect()
        );
    }
}
