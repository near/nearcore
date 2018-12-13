use std::sync::Arc;

use primitives::types::{
    AccountViewCall, AccountViewCallResult, FunctionCallViewCall,
    FunctionCallViewCallResult, MerkleHash,
};

use shard::ShardBlockChain;
use storage::{StateDb, StateDbUpdate};
use wasm::executor;
use wasm::types::{RuntimeContext, ReturnData};

use super::{
    Account, account_id_to_bytes, get, RUNTIME_DATA, RuntimeData, RuntimeExt,
};

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

    pub fn view_account_at(
        &self,
        call: &AccountViewCall,
        root: MerkleHash,
    ) -> Result<AccountViewCallResult, &str> {
        let mut state_update = StateDbUpdate::new(self.state_db.clone(), root);
        let runtime_data: RuntimeData = get(&mut state_update, RUNTIME_DATA)
            .expect("Runtime data is missing");
        match get::<Account>(&mut state_update, &account_id_to_bytes(call.account)) {
            Some(account) => {
                Ok(AccountViewCallResult {
                    account: call.account,
                    nonce: account.nonce,
                    amount: account.amount,
                    stake: runtime_data.get_stake_for_account(call.account),
                })
            },
            _ => Err("account does not exist"),
        }
    }

    pub fn view_account(
        &self,
        call: &AccountViewCall,
    ) -> Result<AccountViewCallResult, &str> {
        let root = self.get_root();
        self.view_account_at(call, root)
    }

    pub fn call_function_at(
        &self,
        call: &FunctionCallViewCall,
        root: MerkleHash,
    ) -> Result<FunctionCallViewCallResult, &str> {
        let mut state_update = StateDbUpdate::new(self.state_db.clone(), root);
        match get::<Account>(&mut state_update, &account_id_to_bytes(call.contract)) {
            Some(account) => {
                let mut result = vec![];
                if !call.method_name.is_empty() {
                    let mut runtime_ext = RuntimeExt::new(
                        &mut state_update,
                        call.contract,
                        vec![],
                    );
                    let wasm_res = executor::execute(
                        &account.code,
                        call.method_name.as_bytes(),
                        &call.args.clone(),
                        &[],
                        &mut runtime_ext,
                        &wasm::types::Config::default(),
                        &RuntimeContext::new(
                            account.amount,
                            0,
                            call.originator,
                            call.contract,
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
                Ok(FunctionCallViewCallResult {
                    contract: call.contract,
                    amount: account.amount,
                    nonce: account.nonce,
                    result,
                })
            }
            None => Err("contract does not exist")
        }
    }

    pub fn call_function(
        &self,
        call: &FunctionCallViewCall,
    ) -> Result<FunctionCallViewCallResult, &str> {
        let root = self.get_root();
        self.call_function_at(call, root)
    }
}

#[cfg(test)]
mod tests {
    use primitives::hash::hash;
    use test_utils::*;
    use super::*;

    #[test]
    fn test_view_call() {
        let viewer = get_test_state_db_viewer();
        let view_call = FunctionCallViewCall {
            originator: hash(b"alice"),
            contract: hash(b"alice"),
            method_name: "run_test".into(),
            args: vec![],
        };

        let view_call_result = viewer.call_function(&view_call);
        assert_eq!(view_call_result.unwrap().result, encode_int(20).to_vec());
    }

    #[test]
    fn test_view_call_with_args() {
        let viewer = get_test_state_db_viewer();
        let args = (1..3).into_iter().flat_map(|x| encode_int(x).to_vec()).collect();
        let view_call = FunctionCallViewCall {
            originator: hash(b"alice"),
            contract: hash(b"alice"),
            method_name: "sum_with_input".into(),
            args,
        };
        let view_call_result = viewer.call_function(&view_call);
        assert_eq!(view_call_result.unwrap().result, encode_int(3).to_vec());
    }
}
