use std::str;
use std::time::Instant;

use log::debug;

use near_crypto::{KeyType, PublicKey};
use near_primitives::account::{AccessKey, Account};
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::ActionReceipt;
use near_primitives::serialize::to_base64;
use near_primitives::transaction::FunctionCallAction;
use near_primitives::trie_key::trie_key_parsers;
use near_primitives::types::EpochHeight;
use near_primitives::types::{AccountId, BlockHeight, EpochId, EpochInfoProvider};
use near_primitives::version::ProtocolVersion;
use near_primitives::views::{StateItem, ViewStateResult};
use near_runtime_configs::RuntimeConfig;
use near_runtime_utils::is_valid_account_id;
use near_store::{get_access_key, get_account, TrieUpdate};
use near_vm_logic::ReturnData;

use crate::actions::execute_function_call;
use crate::ext::RuntimeExt;
use crate::ApplyState;
use std::sync::Arc;

pub struct TrieViewer {}

impl TrieViewer {
    pub fn new() -> Self {
        Self {}
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
    ) -> Result<AccessKey, Box<dyn std::error::Error>> {
        if !is_valid_account_id(account_id) {
            return Err(format!("Account ID '{}' is not valid", account_id).into());
        }

        get_access_key(state_update, account_id, public_key)?
            .ok_or_else(|| format!("access key {} does not exist while viewing", public_key).into())
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
        block_height: BlockHeight,
        block_timestamp: u64,
        last_block_hash: &CryptoHash,
        epoch_height: EpochHeight,
        epoch_id: &EpochId,
        contract_id: &AccountId,
        method_name: &str,
        args: &[u8],
        logs: &mut Vec<String>,
        epoch_info_provider: &dyn EpochInfoProvider,
        current_protocol_version: ProtocolVersion,
        evm_chain_id: u128,
    ) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let now = Instant::now();
        if !is_valid_account_id(contract_id) {
            return Err(format!("Contract ID {:?} is not valid", contract_id).into());
        }
        let root = state_update.get_root();
        let mut account = get_account(&state_update, contract_id)?
            .ok_or_else(|| format!("Account {:?} doesn't exist", contract_id))?;
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
            epoch_id,
            last_block_hash,
            epoch_info_provider,
            current_protocol_version,
        );
        let config = Arc::new(RuntimeConfig::default());
        let apply_state = ApplyState {
            block_index: block_height,
            last_block_hash: *last_block_hash,
            epoch_id: epoch_id.clone(),
            epoch_height,
            gas_price: 0,
            block_timestamp,
            gas_limit: None,
            random_seed: root,
            current_protocol_version,
            config: config.clone(),
            cache: None,
            evm_chain_id,
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
            Err(message.into())
        } else {
            let outcome = outcome.unwrap();
            debug!(target: "runtime", "(exec time {}) result of execution: {:#?}", time_str, outcome);
            logs.extend(outcome.logs);
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
    use near_chain_configs::TEST_EVM_CHAIN_ID;
    use near_primitives::test_utils::MockEpochInfoProvider;
    use near_primitives::trie_key::TrieKey;
    use near_primitives::types::StateChangeCause;
    use near_primitives::version::PROTOCOL_VERSION;
    use near_primitives::views::StateItem;
    use testlib::runtime_utils::{
        alice_account, encode_int, get_runtime_and_trie, get_test_trie_viewer,
    };

    use super::*;

    #[test]
    fn test_view_call() {
        let (viewer, root) = get_test_trie_viewer();

        let mut logs = vec![];
        let result = viewer.call_function(
            root,
            1,
            1,
            &CryptoHash::default(),
            0,
            &EpochId::default(),
            &AccountId::from("test.contract"),
            "run_test",
            &[],
            &mut logs,
            &MockEpochInfoProvider::default(),
            PROTOCOL_VERSION,
            TEST_EVM_CHAIN_ID,
        );

        assert_eq!(result.unwrap(), encode_int(10));
    }

    #[test]
    fn test_view_call_bad_contract_id() {
        let (viewer, root) = get_test_trie_viewer();

        let mut logs = vec![];
        let result = viewer.call_function(
            root,
            1,
            1,
            &CryptoHash::default(),
            0,
            &EpochId::default(),
            &"bad!contract".to_string(),
            "run_test",
            &[],
            &mut logs,
            &MockEpochInfoProvider::default(),
            PROTOCOL_VERSION,
            TEST_EVM_CHAIN_ID,
        );

        let err = result.unwrap_err();
        assert!(
            err.to_string().contains(r#"Contract ID "bad!contract" is not valid"#),
            format!("Got different error that doesn't match: {}", err)
        );
    }

    #[test]
    fn test_view_call_try_changing_storage() {
        let (viewer, root) = get_test_trie_viewer();

        let mut logs = vec![];
        let result = viewer.call_function(
            root,
            1,
            1,
            &CryptoHash::default(),
            0,
            &EpochId::default(),
            &AccountId::from("test.contract"),
            "run_test_with_storage_change",
            &[],
            &mut logs,
            &MockEpochInfoProvider::default(),
            PROTOCOL_VERSION,
            0x99,
        );
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains(r#"ProhibitedInView { method_name: "storage_write" }"#),
            format!("Got different error that doesn't match: {}", err)
        );
    }

    #[test]
    fn test_view_call_with_args() {
        let (viewer, root) = get_test_trie_viewer();
        let args: Vec<_> = [1u64, 2u64].iter().flat_map(|x| (*x).to_le_bytes().to_vec()).collect();
        let mut logs = vec![];
        let view_call_result = viewer.call_function(
            root,
            1,
            1,
            &CryptoHash::default(),
            0,
            &EpochId::default(),
            &AccountId::from("test.contract"),
            "sum_with_input",
            &args,
            &mut logs,
            &MockEpochInfoProvider::default(),
            PROTOCOL_VERSION,
            0x99,
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
        let trie_viewer = TrieViewer::new();
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
    fn test_log_when_panic() {
        let (viewer, root) = get_test_trie_viewer();

        let mut logs = vec![];
        viewer
            .call_function(
                root,
                1,
                1,
                &CryptoHash::default(),
                0,
                &EpochId::default(),
                &AccountId::from("test.contract"),
                "panic_after_logging",
                &[],
                &mut logs,
                &MockEpochInfoProvider::default(),
                PROTOCOL_VERSION,
                0x99,
            )
            .unwrap_err();

        assert_eq!(logs, vec!["hello".to_string()]);
    }
}
