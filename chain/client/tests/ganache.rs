use std::path::Path;

use std::sync::Arc;

use near_chain::{ChainGenesis, ChainStoreAccess, Provenance, RuntimeAdapter};
use near_chain_configs::Genesis;

use near_client::test_utils::TestEnv;

use near_crypto::{InMemorySigner, KeyType};

#[cfg(feature = "metric_recorder")]
use near_network::recorder::MetricRecorder;

#[cfg(not(feature = "protocol_feature_block_header_v3"))]
#[cfg(feature = "protocol_feature_block_header_v3")]
use near_primitives::sharding::{ShardChunkHeaderInner, ShardChunkHeaderV3};

use near_primitives::serialize::{from_base64, to_base64};
use near_primitives::state_record::StateRecord;

use near_primitives::transaction::{
    Action, DeployContractAction, FunctionCallAction, SignedTransaction,
};

use near_primitives::types::AccountId;

use near_primitives::views::{QueryRequest, QueryResponseKind};

use near_store::test_utils::create_test_store;
use neard::config::GenesisExt;

#[cfg(feature = "ganache")]
#[test]
fn test_patch_state() {
    let epoch_length = 5;
    let mut genesis = Genesis::test(vec!["test0", "test1"], 1);
    genesis.config.epoch_length = epoch_length;
    let mut env = TestEnv::new_with_runtime(
        ChainGenesis::test(),
        1,
        1,
        vec![Arc::new(neard::NightshadeRuntime::new(
            Path::new("."),
            create_test_store(),
            &genesis,
            vec![],
            vec![],
            None,
        )) as Arc<dyn RuntimeAdapter>],
    );
    let genesis_block = env.clients[0].chain.get_block_by_height(0).unwrap().clone();
    let _genesis_height = genesis_block.header().height();

    let signer = InMemorySigner::from_seed("test0", KeyType::ED25519, "test0");
    let tx = SignedTransaction::from_actions(
        1,
        "test0".to_string(),
        "test0".to_string(),
        &signer,
        vec![Action::DeployContract(DeployContractAction {
            code: near_test_contracts::rs_contract().to_vec(),
        })],
        *genesis_block.hash(),
    );
    env.clients[0].process_tx(tx, false, false);
    let mut last_block = genesis_block;
    for i in 1..3 {
        last_block = env.clients[0].produce_block(i).unwrap().unwrap();
        env.process_block(0, last_block.clone(), Provenance::PRODUCED);
    }
    let query_state = |chain: &mut near_chain::Chain,
                       runtime_adapter: Arc<dyn RuntimeAdapter>,
                       account_id: AccountId| {
        let final_head = chain.store().final_head().unwrap();
        let last_final_block = chain.get_block(&final_head.last_block_hash).unwrap().clone();
        let response = runtime_adapter
            .query(
                0,
                &last_final_block.chunks()[0].prev_state_root(),
                last_final_block.header().height(),
                last_final_block.header().raw_timestamp(),
                &final_head.prev_block_hash,
                last_final_block.hash(),
                last_final_block.header().epoch_id(),
                &QueryRequest::ViewState { account_id, prefix: vec![].into() },
            )
            .unwrap();
        match response.kind {
            QueryResponseKind::ViewState(view_state_result) => view_state_result.values,
            // QueryResponseKind::ViewCode(code) => code.code,
            _ => panic!("Wrong return value"),
        }
    };

    let function_call_tx = SignedTransaction::from_actions(
        2,
        "test0".to_string(),
        "test0".to_string(),
        &signer,
        vec![Action::FunctionCall(FunctionCallAction {
            method_name: "write_random_value".to_string(),
            args: vec![],
            gas: 100000000000000,
            deposit: 0,
        })],
        *last_block.hash(),
    );
    env.clients[0].process_tx(function_call_tx, false, false);
    for i in 3..9 {
        last_block = env.clients[0].produce_block(i).unwrap().unwrap();
        env.process_block(0, last_block.clone(), Provenance::PRODUCED);
    }

    let runtime_adapter = env.clients[0].runtime_adapter.clone();
    let state =
        query_state(&mut env.clients[0].chain, runtime_adapter.clone(), "test0".to_string());

    env.clients[0].chain.patch_state(vec![StateRecord::Data {
        account_id: "test0".to_string(),
        data_key: from_base64(&state[0].key).unwrap(),
        value: b"world".to_vec(),
    }]);

    for i in 9..20 {
        last_block = env.clients[0].produce_block(i).unwrap().unwrap();
        env.process_block(0, last_block.clone(), Provenance::PRODUCED);
    }

    let runtime_adapter = env.clients[0].runtime_adapter.clone();
    let state2 =
        query_state(&mut env.clients[0].chain, runtime_adapter.clone(), "test0".to_string());
    assert_eq!(state2.len(), 1);
    assert_eq!(state2[0].value, to_base64(b"world"));
}
