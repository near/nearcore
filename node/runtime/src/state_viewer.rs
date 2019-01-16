use std::collections::HashMap;
use std::sync::Arc;
use std::str;

use primitives::hash::CryptoHash;
use primitives::types::{AccountId, Balance, MerkleHash, AccountingInfo};
use shard::ShardBlockChain;
use storage::{StateDb, StateDbUpdate};
use wasm::executor;
use wasm::types::{ReturnData, RuntimeContext};

use super::{
    Account, account_id_to_bytes, get, RuntimeExt, COL_ACCOUNT, COL_CODE,
};

#[derive(Serialize, Deserialize)]
pub struct ViewStateResult {
    pub values: HashMap<Vec<u8>, Vec<u8>>
}

pub struct StateDbViewer {
    shard_chain: Arc<ShardBlockChain>,
    state_db: Arc<StateDb>,
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
pub struct AccountViewCallResult {
    pub account: AccountId,
    pub nonce: u64,
    pub amount: Balance,
    pub stake: u64,
    pub code_hash: CryptoHash,
}

impl StateDbViewer {
    pub fn new(shard_chain: Arc<ShardBlockChain>, state_db: Arc<StateDb>) -> Self {
        StateDbViewer {
            shard_chain,
            state_db,
        }
    }

    pub fn get_root(&self) -> MerkleHash {
        self.shard_chain.chain.best_block().body.header.merkle_root_state
    }

    pub fn view_account_at(
        &self,
        account_id: &AccountId,
        root: MerkleHash,
    ) -> Result<AccountViewCallResult, String> {
        let mut state_update = StateDbUpdate::new(self.state_db.clone(), root);

        match get::<Account>(&mut state_update, &account_id_to_bytes(COL_ACCOUNT, account_id)) {
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

    pub fn view_account(
        &self,
        account_id: &AccountId,
    ) -> Result<AccountViewCallResult, String> {
        let root = self.get_root();
        self.view_account_at(account_id, root)
    }

    pub fn view_state(&self, account_id: &AccountId) -> ViewStateResult {
        let root = self.get_root();
        let mut values = HashMap::default();
        let state_update = StateDbUpdate::new(self.state_db.clone(), root);
        let mut prefix = account_id_to_bytes(COL_ACCOUNT, account_id);
        prefix.append(&mut b",".to_vec());
        state_update.for_keys_with_prefix(&prefix, |key| {
            if let Some(value) = state_update.get(key) {
                values.insert(key.to_vec(), value.to_vec());
            }
        });
        ViewStateResult {
            values
        }
    }

    pub fn call_function_at(
        &self,
        originator_id: &AccountId,
        contract_id: &AccountId,
        method_name: &str,
        args: &[u8],
        root: MerkleHash,
    ) -> Result<Vec<u8>, String> {
        let mut state_update = StateDbUpdate::new(self.state_db.clone(), root);
        let code: Vec<u8> = get(&mut state_update, &account_id_to_bytes(COL_CODE, contract_id))
            .ok_or_else(|| format!("account {} does not have contract code", contract_id.clone()))?;
        let wasm_res = match get::<Account>(&mut state_update, &account_id_to_bytes(COL_ACCOUNT, contract_id)) {
            Some(account) => {
                let mut runtime_ext = RuntimeExt::new(
                    &mut state_update,
                    contract_id,
                    &AccountingInfo {
                        originator: originator_id.clone(),
                        contract_id: Some(contract_id.clone()),
                    },
                    &[],
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
                        self.shard_chain.chain.best_index(),
                        root.into(),
                    ),
                )
            }
            None => return Err(format!("contract {} does not exist", contract_id))
        };
        match wasm_res {
            Ok(res) => {
                debug!(target: "runtime", "result of execution: {:?}", res);
                match res.return_data {
                    Ok(return_data) => {
                        let (_, root_after) = state_update.finalize();
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
                debug!(target: "runtime", "{}", message);
                Err(message)
            }
        }
    }

    pub fn call_function(
        &self,
        originator_id: &AccountId,
        contract_id: &AccountId,
        method_name: &str,
        args: &[u8],
    ) -> Result<Vec<u8>, String> {
        let root = self.get_root();
        self.call_function_at(
            originator_id,
            contract_id,
            method_name,
            args,
            root,
        )
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
        let viewer = get_test_state_db_viewer();

        let result = viewer.call_function(
            &alice_account(),
            &alice_account(),
            "run_test",
            &vec![]
        );

        assert_eq!(result.unwrap(), encode_int(10));
    }

    #[test]
    fn test_view_call_try_changing_storage() {
        let viewer = get_test_state_db_viewer();

        let result = viewer.call_function(
            &alice_account(),
            &alice_account(),
            "run_test_with_storage_change",
            &vec![]
        );
        // run_test tries to change storage, so it should fail
        assert!(result.is_err());
    }

    #[test]
    fn test_view_call_with_args() {
        let viewer = get_test_state_db_viewer();
        let args = (1..3).into_iter()
            .flat_map(|x| encode_int(x).to_vec())
            .collect::<Vec<_>>();
        let view_call_result = viewer.call_function(
            &alice_account(),
            &alice_account(),
            "sum_with_input",
            &args,
        );
        assert_eq!(view_call_result.unwrap(), encode_int(3).to_vec());
    }

    #[test]
    fn test_view_state() {
        let viewer = get_test_state_db_viewer();
        let result = viewer.view_state(&alice_account());
        assert_eq!(result.values, HashMap::default());
    }
}
