#[cfg(test)]
#[cfg(feature = "sandbox")]
mod tests {
    use std::path::Path;

    use std::sync::Arc;

    use near_chain::{ChainGenesis, ChainStoreAccess, Provenance, RuntimeAdapter};
    use near_chain_configs::Genesis;

    use near_client::test_utils::TestEnv;

    use near_crypto::{InMemorySigner, KeyType};

    #[cfg(not(feature = "protocol_feature_block_header_v3"))]
    #[cfg(feature = "protocol_feature_block_header_v3")]
    use near_primitives::sharding::{ShardChunkHeaderInner, ShardChunkHeaderV3};

    use near_primitives::serialize::{from_base64, to_base64};
    use near_primitives::state_record::StateRecord;

    use near_primitives::transaction::{
        Action, DeployContractAction, FunctionCallAction, SignedTransaction,
    };

    use near_primitives::types::{AccountId, BlockHeight, Nonce};

    use near_primitives::views::{QueryRequest, QueryResponseKind, StateItem};

    use near_store::test_utils::create_test_store;
    use neard::config::GenesisExt;

    fn test_setup() -> (TestEnv, InMemorySigner, Arc<dyn RuntimeAdapter>) {
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
        let signer = InMemorySigner::from_seed("test0", KeyType::ED25519, "test0");
        let runtime_adapter = env.clients[0].runtime_adapter.clone();
        send_tx(
            &mut env,
            1,
            "test0".to_string(),
            "test0".to_string(),
            &signer,
            vec![Action::DeployContract(DeployContractAction {
                code: near_test_contracts::rs_contract().to_vec(),
            })],
        );
        do_blocks(&mut env, 1, 3);

        send_tx(
            &mut env,
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
        );
        do_blocks(&mut env, 3, 9);
        (env, signer, runtime_adapter)
    }

    fn do_blocks(env: &mut TestEnv, start: BlockHeight, end: BlockHeight) {
        for i in start..end {
            let last_block = env.clients[0].produce_block(i).unwrap().unwrap();
            env.process_block(0, last_block.clone(), Provenance::PRODUCED);
        }
    }

    fn send_tx(
        env: &mut TestEnv,
        nonce: Nonce,
        signer_id: AccountId,
        receiver_id: AccountId,
        signer: &InMemorySigner,
        actions: Vec<Action>,
    ) {
        let hash = env.clients[0].chain.head().unwrap().last_block_hash;
        let tx =
            SignedTransaction::from_actions(nonce, signer_id, receiver_id, signer, actions, hash);
        env.clients[0].process_tx(tx, false, false);
    }

    fn query_state(
        chain: &mut near_chain::Chain,
        runtime_adapter: Arc<dyn RuntimeAdapter>,
        account_id: AccountId,
    ) -> Vec<StateItem> {
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
            _ => panic!("Wrong return value"),
        }
    }

    #[test]
    fn test_patch_state() {
        let (mut env, _signer, runtime_adapter) = test_setup();

        let state =
            query_state(&mut env.clients[0].chain, runtime_adapter.clone(), "test0".to_string());
        env.clients[0].chain.patch_state(vec![StateRecord::Data {
            account_id: "test0".to_string(),
            data_key: from_base64(&state[0].key).unwrap(),
            value: b"world".to_vec(),
        }]);

        do_blocks(&mut env, 9, 20);
        let state2 =
            query_state(&mut env.clients[0].chain, runtime_adapter.clone(), "test0".to_string());
        assert_eq!(state2.len(), 1);
        assert_eq!(state2[0].value, to_base64(b"world"));
    }
}
