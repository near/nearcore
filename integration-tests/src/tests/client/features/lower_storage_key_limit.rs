use assert_matches::assert_matches;
use near_chain::{ChainGenesis, Provenance, RuntimeWithEpochManagerAdapter};
use near_chain_configs::Genesis;
use near_client::test_utils::TestEnv;
use near_crypto::{InMemorySigner, KeyType, Signer};
use near_epoch_manager::shard_tracker::TrackedConfig;
use near_o11y::testonly::init_test_logger;
use near_primitives::errors::TxExecutionError;
use near_primitives::hash::CryptoHash;
use near_primitives::runtime::config_store::RuntimeConfigStore;
use near_primitives::transaction::{Action, FunctionCallAction, Transaction};
use near_primitives::types::BlockHeight;
use near_primitives::views::FinalExecutionStatus;
use near_store::test_utils::create_test_store;
use nearcore::config::GenesisExt;
use std::path::Path;
use std::sync::Arc;

use crate::tests::client::process_blocks::{
    deploy_test_contract_with_protocol_version, produce_blocks_from_height_with_protocol_version,
};

/// Check correctness of the protocol upgrade and ability to write 2 KB keys.
#[test]
fn protocol_upgrade() {
    init_test_logger();

    let old_protocol_version =
        near_primitives::version::ProtocolFeature::LowerStorageKeyLimit.protocol_version() - 1;
    let new_protocol_version = old_protocol_version + 1;
    let new_storage_key_limit = 2usize.pow(11); // 2 KB
    let args: Vec<u8> = vec![1u8; new_storage_key_limit + 1]
        .into_iter()
        .chain(near_primitives::test_utils::encode(&[10u64]).into_iter())
        .collect();
    let epoch_length: BlockHeight = 5;

    // The immediate protocol upgrade needs to be set for this test to pass in
    // the release branch where the protocol upgrade date is set.
    std::env::set_var("NEAR_TESTS_IMMEDIATE_PROTOCOL_UPGRADE", "1");

    // Prepare TestEnv with a contract at the old protocol version.
    let mut env = {
        let mut genesis =
            Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
        genesis.config.epoch_length = epoch_length;
        genesis.config.protocol_version = old_protocol_version;
        let chain_genesis = ChainGenesis::new(&genesis);
        let runtimes: Vec<Arc<dyn RuntimeWithEpochManagerAdapter>> =
            vec![nearcore::NightshadeRuntime::test_with_runtime_config_store(
                Path::new("."),
                create_test_store(),
                &genesis,
                TrackedConfig::AllShards,
                RuntimeConfigStore::new(None),
            ) as Arc<dyn RuntimeWithEpochManagerAdapter>];
        let mut env = TestEnv::builder(chain_genesis).runtime_adapters(runtimes).build();

        deploy_test_contract_with_protocol_version(
            &mut env,
            "test0".parse().unwrap(),
            near_test_contracts::base_rs_contract(),
            epoch_length,
            1,
            old_protocol_version,
        );
        env
    };

    let signer = InMemorySigner::from_seed("test0".parse().unwrap(), KeyType::ED25519, "test0");
    let tx = Transaction {
        signer_id: "test0".parse().unwrap(),
        receiver_id: "test0".parse().unwrap(),
        public_key: signer.public_key(),
        actions: vec![Action::FunctionCall(FunctionCallAction {
            method_name: "write_key_value".to_string(),
            args,
            gas: 10u64.pow(14),
            deposit: 0,
        })],

        nonce: 0,
        block_hash: CryptoHash::default(),
    };

    // Run transaction writing storage key exceeding the limit. Check that execution succeeds.
    {
        let tip = env.clients[0].chain.head().unwrap();
        let signed_tx =
            Transaction { nonce: tip.height + 1, block_hash: tip.last_block_hash, ..tx.clone() }
                .sign(&signer);
        let tx_hash = signed_tx.get_hash();
        env.clients[0].process_tx(signed_tx, false, false);
        produce_blocks_from_height_with_protocol_version(
            &mut env,
            epoch_length,
            tip.height + 1,
            old_protocol_version,
        );
        let final_result = env.clients[0].chain.get_final_transaction_result(&tx_hash).unwrap();
        assert_matches!(final_result.status, FinalExecutionStatus::SuccessValue(_));
    }

    env.upgrade_protocol(new_protocol_version);

    // Re-run the transaction, check that execution fails.
    {
        let tip = env.clients[0].chain.head().unwrap();
        let signed_tx =
            Transaction { nonce: tip.height + 1, block_hash: tip.last_block_hash, ..tx }
                .sign(&signer);
        let tx_hash = signed_tx.get_hash();
        env.clients[0].process_tx(signed_tx, false, false);
        for i in 0..epoch_length {
            let block = env.clients[0].produce_block(tip.height + i + 1).unwrap().unwrap();
            env.process_block(0, block.clone(), Provenance::PRODUCED);
        }
        let final_result = env.clients[0].chain.get_final_transaction_result(&tx_hash).unwrap();
        assert_matches!(
            final_result.status,
            FinalExecutionStatus::Failure(TxExecutionError::ActionError(_))
        );
    }

    // Run transaction where storage key exactly fits the new limit, check that execution succeeds.
    {
        let args: Vec<u8> = vec![1u8; new_storage_key_limit]
            .into_iter()
            .chain(near_primitives::test_utils::encode(&[20u64]).into_iter())
            .collect();
        let tx = Transaction {
            signer_id: "test0".parse().unwrap(),
            receiver_id: "test0".parse().unwrap(),
            public_key: signer.public_key(),
            actions: vec![Action::FunctionCall(FunctionCallAction {
                method_name: "write_key_value".to_string(),
                args,
                gas: 10u64.pow(14),
                deposit: 0,
            })],

            nonce: 0,
            block_hash: CryptoHash::default(),
        };
        let tip = env.clients[0].chain.head().unwrap();
        let signed_tx =
            Transaction { nonce: tip.height + 1, block_hash: tip.last_block_hash, ..tx }
                .sign(&signer);
        let tx_hash = signed_tx.get_hash();
        env.clients[0].process_tx(signed_tx, false, false);
        for i in 0..epoch_length {
            let block = env.clients[0].produce_block(tip.height + i + 1).unwrap().unwrap();
            env.process_block(0, block.clone(), Provenance::PRODUCED);
        }
        let final_result = env.clients[0].chain.get_final_transaction_result(&tx_hash).unwrap();
        assert_matches!(final_result.status, FinalExecutionStatus::SuccessValue(_));
    }
}
