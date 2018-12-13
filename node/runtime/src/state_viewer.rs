use std::collections::HashMap;
use std::sync::Arc;
use std::str;

use primitives::types::{AccountId, MerkleHash, ViewCall, ViewCallResult};
use primitives::utils::concat;
use shard::ShardBlockChain;
use storage::{StateDb, StateDbUpdate};
use wasm::executor;
use wasm::types::{ReturnData, RuntimeContext};

use super::{Account, account_id_to_bytes, get, RUNTIME_DATA, RuntimeData, RuntimeExt};

#[derive(Serialize, Deserialize)]
pub struct ViewStateResult {
    pub values: HashMap<String, String>
}

pub struct StateDbViewer {
    shard_chain: Arc<ShardBlockChain>,
    state_db: Arc<StateDb>,
}

impl StateDbViewer {
    pub fn new(shard_chain: Arc<ShardBlockChain>, state_db: Arc<StateDb>) -> Self {
        StateDbViewer {
            shard_chain,
            state_db,
        }
    }

    pub fn get_root(&self) -> MerkleHash {
        self.shard_chain.best_block().body.header.merkle_root_state
    }

    pub fn view(&self, view_call: &ViewCall) -> ViewCallResult {
        let root = self.get_root();
        self.view_at(view_call, root)
    }

    pub fn view_state(&self, account: &AccountId) -> ViewStateResult {
        let root = self.get_root();
        // let mut state_update = StateDbUpdate::new(self.state_db.clone(), root);
        let mut values = HashMap::default();
        for (key, value) in self.state_db.iter_prefix(account.as_ref()) {
            values.insert(String::from_utf8(key.to_vec()).expect("Failed to convert"),
                          String::from_utf8(value.to_vec()).expect("Failed to convert"));
        }
        // state_update.
        ViewStateResult {
            values
        }
    }

    pub fn view_at(&self, view_call: &ViewCall, root: MerkleHash) -> ViewCallResult {
        let mut state_update = StateDbUpdate::new(self.state_db.clone(), root);
        let runtime_data: RuntimeData = get(&mut state_update, RUNTIME_DATA).expect("Runtime data is missing");
        // TODO(#172): Distinguish sender and receiver accounts.
        match get::<Account>(&mut state_update, &account_id_to_bytes(view_call.account)) {
            Some(account) => {
                let mut result = vec![];
                if !view_call.method_name.is_empty() {
                    let mut runtime_ext = RuntimeExt::new(&mut state_update, view_call.account, vec![]);
                    let wasm_res = executor::execute(
                        &account.code,
                        view_call.method_name.as_bytes(),
                        &concat(view_call.args.clone()),
                        &[],
                        &mut runtime_ext,
                        &wasm::types::Config::default(),
                        &RuntimeContext::new(
                            account.amount,
                            0,
                            view_call.account,
                            view_call.account,
                            0,
                        ),
                    );
                    match wasm_res {
                        Ok(res) => {
                            debug!(target: "runtime", "result of execution: {:?}", res);
                            // TODO: Handle other ExecutionOutcome results
                            if let ReturnData::Value(buf) = res.return_data {
                                result.extend(&buf);
                            }
                        }
                        Err(e) => {
                            debug!(target: "runtime", "wasm execution failed with error: {:?}", e);
                        }
                    }
                }
                ViewCallResult {
                    account: view_call.account,
                    amount: account.amount,
                    stake: runtime_data.at_stake(view_call.account),
                    nonce: account.nonce,
                    result,
                }
            }
            None => {
                ViewCallResult { 
                    account: view_call.account,
                    amount: 0,
                    stake: 0,
                    nonce: 0,
                    result: vec![]
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use primitives::hash::hash;
    use test_utils::{encode_int, get_test_state_db_viewer};

    use super::*;

    #[test]
    fn test_view_call() {
        let viewer = get_test_state_db_viewer();
        let view_call = ViewCall::func_call(hash(b"alice"), "run_test".into(), vec![]);
        let view_call_result = viewer.view(&view_call);
        assert_eq!(view_call_result.result, encode_int(20).to_vec());
    }

    #[test]
    fn test_view_call_with_args() {
        let viewer = get_test_state_db_viewer();
        let args = (1..3).into_iter().map(|x| encode_int(x).to_vec()).collect();
        let view_call = ViewCall::func_call(hash(b"alice"), "sum_with_input".into(), args);
        let view_call_result = viewer.view(&view_call);
        assert_eq!(view_call_result.result, encode_int(3).to_vec());
    }

    #[test]
    fn test_view_state() {
        let viewer = get_test_state_db_viewer();
        let result = viewer.view_state(&hash(b"alice"));
        assert_eq!(result.values, HashMap::default());
    }
}