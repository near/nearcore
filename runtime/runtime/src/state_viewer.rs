use std::collections::HashMap;
use std::str;
use std::time::Instant;

use primitives::account::{AccessKey, Account};
use primitives::crypto::signature::PublicKey;
use primitives::hash::{bs58_format, CryptoHash};
use primitives::types::{AccountId, AccountingInfo, Balance, Nonce};
use primitives::utils::{is_valid_account_id, key_for_access_key, key_for_account, key_for_code};
use storage::{get, TrieUpdate};
use wasm::executor;
use wasm::types::{ContractCode, ReturnData, RuntimeContext};

use super::ext::ACCOUNT_DATA_SEPARATOR;
use super::RuntimeExt;

#[derive(Serialize, Deserialize)]
pub struct ViewStateResult {
    pub values: HashMap<Vec<u8>, Vec<u8>>,
}

pub struct TrieViewer {}

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
pub struct AccountViewCallResult {
    pub account_id: AccountId,
    pub nonce: Nonce,
    pub amount: Balance,
    pub stake: u64,
    pub public_keys: Vec<PublicKey>,
    #[serde(with = "bs58_format")]
    pub code_hash: CryptoHash,
}

impl TrieViewer {
    pub fn view_account(
        &self,
        state_update: &TrieUpdate,
        account_id: &AccountId,
    ) -> Result<AccountViewCallResult, String> {
        if !is_valid_account_id(account_id) {
            return Err(format!("Account ID '{}' is not valid", account_id));
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
            _ => Err(format!("account {} does not exist while viewing", account_id)),
        }
    }

    pub fn view_access_key(
        &self,
        state_update: &TrieUpdate,
        account_id: &AccountId,
        public_key: &PublicKey,
    ) -> Result<Option<AccessKey>, String> {
        if !is_valid_account_id(account_id) {
            return Err(format!("Account ID '{}' is not valid", account_id));
        }

        Ok(get(state_update, &key_for_access_key(account_id, public_key)))
    }

    pub fn get_public_keys_for_account(
        &self,
        state_update: &TrieUpdate,
        account_id: &AccountId,
    ) -> Result<Vec<PublicKey>, String> {
        self.view_account(state_update, account_id).map(|account| account.public_keys)
    }

    pub fn view_state(
        &self,
        state_update: &TrieUpdate,
        account_id: &AccountId,
    ) -> Result<ViewStateResult, String> {
        if !is_valid_account_id(account_id) {
            return Err(format!("Account ID '{}' is not valid", account_id));
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
    ) -> Result<Vec<u8>, String> {
        let now = Instant::now();
        if !is_valid_account_id(contract_id) {
            return Err(format!("Contract ID '{}' is not valid", contract_id));
        }
        let root = state_update.get_root();
        let code: ContractCode =
            get(&state_update, &key_for_code(contract_id)).ok_or_else(|| {
                format!("account {} does not have contract code", contract_id.clone())
            })?;
        let wasm_res = match get::<Account>(&state_update, &key_for_account(contract_id)) {
            Some(account) => {
                let empty_hash = CryptoHash::default();
                let mut runtime_ext = RuntimeExt::new(
                    &mut state_update,
                    contract_id,
                    &AccountingInfo { originator: contract_id.clone(), contract_id: None },
                    &empty_hash,
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
                        contract_id,
                        contract_id,
                        0,
                        block_index,
                        root.as_ref().into(),
                    ),
                )
            }
            None => return Err(format!("contract {} does not exist", contract_id)),
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
                        let (root_after, _) = state_update.finalize();
                        if root_after != root {
                            return Err(
                                "function call for viewing tried to change storage".to_string()
                            );
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
                        Err(message)
                    }
                }
            }
            Err(e) => {
                let message = format!("wasm execution failed with error: {:?}", e);
                debug!(target: "runtime", "(exec time {}) {}", time_str, message);
                Err(message)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use kvdb::DBValue;
    use primitives::types::AccountId;

    use testlib::runtime_utils::{
        alice_account, encode_int, get_runtime_and_trie, get_test_trie_viewer,
    };

    #[test]
    fn test_view_call() {
        let (viewer, root) = get_test_trie_viewer();

        let mut logs = vec![];
        let result =
            viewer.call_function(root, 1, &alice_account(), "run_test", &vec![], &mut logs);

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
            &alice_account(),
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
            viewer.call_function(root, 1, &alice_account(), "sum_with_input", &args, &mut logs);
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
            .set(&account_suffix(&alice_account(), b"test123"), &DBValue::from_slice(b"123"));
        let (new_root, db_changes) = state_update.finalize();
        trie.apply_changes(db_changes).unwrap();

        let state_update = TrieUpdate::new(trie, new_root);
        let trie_viewer = TrieViewer {};
        let result = trie_viewer.view_state(&state_update, &alice_account()).unwrap();
        assert_eq!(
            result.values,
            [(b"test123".to_vec(), b"123".to_vec())].iter().cloned().collect()
        );
    }
}
