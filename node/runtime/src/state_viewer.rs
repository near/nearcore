use std::collections::HashMap;
use std::str;
use std::time::Instant;

use primitives::hash::CryptoHash;
use primitives::utils::is_valid_account_id;
use primitives::types::{AccountId, Balance, AccountingInfo};
use storage::{TrieUpdate};
use wasm::executor;
use wasm::types::{ReturnData, RuntimeContext};

use super::{
    Account, account_id_to_bytes, get, RuntimeExt, COL_ACCOUNT, COL_CODE,
};
use primitives::signature::PublicKey;

#[derive(Serialize, Deserialize)]
pub struct ViewStateResult {
    pub values: HashMap<Vec<u8>, Vec<u8>>
}

pub struct TrieViewer {}

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
pub struct AccountViewCallResult {
    pub account: AccountId,
    pub nonce: u64,
    pub amount: Balance,
    pub stake: u64,
    pub code_hash: CryptoHash,
}

impl TrieViewer {

    pub fn view_account(
        &self,
        state_update: &mut TrieUpdate,
        account_id: &AccountId,
    ) -> Result<AccountViewCallResult, String> {
        if !is_valid_account_id(account_id) {
            return Err(format!("Account ID '{}' is not valid", account_id));
        }

        match get::<Account>(state_update, &account_id_to_bytes(COL_ACCOUNT, account_id)) {
            Some(account) => {
                Ok(AccountViewCallResult {
                    account: account_id.clone(),
                    nonce: account.nonce,
                    amount: account.amount,
                    stake: account.staked,
                    code_hash: account.code_hash
                })
            },
            _ => Err(format!("account {} does not exist while viewing", account_id)),
        }
    }

    pub fn get_public_keys_for_account(
        &self,
        state_update: &mut TrieUpdate,
        account_id: &AccountId,
    ) -> Result<Vec<PublicKey>, String> {
        if !is_valid_account_id(account_id) {
            return Err(format!("Account ID '{}' is not valid", account_id));
        }
        match get::<Account>(state_update, &account_id_to_bytes(COL_ACCOUNT, account_id)) {
            Some(account) => Ok(account.public_keys),
            _ => Err(format!("account {} does not exist while viewing", account_id)),
        }
    }

    pub fn view_state(
        &self,
        state_update: &TrieUpdate,
        account_id: &AccountId
    ) -> Result<ViewStateResult, String> {
        if !is_valid_account_id(account_id) {
            return Err(format!("Account ID '{}' is not valid", account_id));
        }
        let mut values = HashMap::default();
        let mut prefix = account_id_to_bytes(COL_ACCOUNT, account_id);
        prefix.append(&mut b",".to_vec());
        state_update.for_keys_with_prefix(&prefix, |key| {
            if let Some(value) = state_update.get(key) {
                values.insert(key.to_vec(), value.to_vec());
            }
        });
        Ok(ViewStateResult {
            values
        })
    }

    pub fn call_function(
        &self,
        mut state_update: TrieUpdate,
        block_index: u64,
        contract_id: &AccountId,
        method_name: &str,
        args: &[u8],
    ) -> Result<Vec<u8>, String> {
        let now = Instant::now();
        if !is_valid_account_id(contract_id) {
            return Err(format!("Contract ID '{}' is not valid", contract_id));
        }
        let root = state_update.get_root();
        let code: Vec<u8> = get(&mut state_update, &account_id_to_bytes(COL_CODE, contract_id))
            .ok_or_else(|| format!("account {} does not have contract code", contract_id.clone()))?;
        let wasm_res = match get::<Account>(&mut state_update, &account_id_to_bytes(COL_ACCOUNT, contract_id)) {
            Some(account) => {
                let empty_hash = CryptoHash::default();
                let mut runtime_ext = RuntimeExt::new(
                    &mut state_update,
                    contract_id,
                    &AccountingInfo {
                        originator: contract_id.clone(),
                        contract_id: None,
                    },
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
            None => return Err(format!("contract {} does not exist", contract_id))
        };
        let elapsed = now.elapsed();
        let time_ms = (elapsed.as_secs() as f64 / 1_000.0) + f64::from(elapsed.subsec_nanos()) / 1_000_000.0;
        let time_str = format!("{:.*}ms", 2, time_ms);
        match wasm_res {
            Ok(res) => {
                debug!(target: "runtime", "(exec time {}) result of execution: {:#?}", time_str, res);
                match res.return_data {
                    Ok(return_data) => {
                        let (root_after, _) = state_update.finalize();
                        if root_after != root {
                            return Err("function call for viewing tried to change storage".to_string());
                        }
                        let mut result = vec![];
                        if let ReturnData::Value(buf) = return_data {
                            result.extend(&buf);
                        }
                        Ok(result)
                    }
                    Err(e) => {
                        let message = format!("wasm view call execution failed with error: {:?}", e);
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
    use primitives::types::AccountId;
    use std::collections::HashMap;
    use crate::test_utils::*;

    fn alice_account() -> AccountId {
        "alice.near".to_string()
    }

    #[test]
    fn test_view_call() {
        let (viewer, root) = get_test_trie_viewer();

        let result = viewer.call_function(
            root, 1,
            &alice_account(),
            "run_test",
            &vec![]
        );

        assert_eq!(result.unwrap(), encode_int(10));
    }

    #[test]
    fn test_view_call_bad_contract_id() {
        let (viewer, root) = get_test_trie_viewer();

        let result = viewer.call_function(
            root, 1,
            &"bad!contract".to_string(),
            "run_test",
            &vec![]
        );

        assert!(result.is_err());
    }

    #[test]
    fn test_view_call_try_changing_storage() {
        let (viewer, root) = get_test_trie_viewer();

        let result = viewer.call_function(
            root, 1,
            &alice_account(),
            "run_test_with_storage_change",
            &vec![]
        );
        // run_test tries to change storage, so it should fail
        assert!(result.is_err());
    }

    #[test]
    fn test_view_call_with_args() {
        let (viewer, root) = get_test_trie_viewer();
        let args = (1..3).into_iter()
            .flat_map(|x| encode_int(x).to_vec())
            .collect::<Vec<_>>();
        let view_call_result = viewer.call_function(
            root, 1,
            &alice_account(),
            "sum_with_input",
            &args,
        );
        assert_eq!(view_call_result.unwrap(), encode_int(3).to_vec());
    }

    #[test]
    fn test_view_state() {
        let (viewer, state_update) = get_test_trie_viewer();
        let result = viewer.view_state(&state_update, &alice_account()).unwrap();
        assert_eq!(result.values, HashMap::default());
        // TODO: make this test actually do stuff.
    }
}
