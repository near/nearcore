#![cfg_attr(not(feature = "nightly"), allow(unused_imports))]

use crate::tests::client::process_blocks::deploy_test_contract;
use near_chain_configs::Genesis;
use near_client::test_utils::TestEnv;
use near_client::ProcessTxResponse;
use near_crypto::{InMemorySigner, KeyType, Signer};
use near_parameters::RuntimeConfigStore;
use near_primitives::hash::CryptoHash;
use near_primitives::transaction::{Action, FunctionCallAction, Transaction, TransactionV0};
use nearcore::test_utils::TestEnvNightshadeSetupExt;

#[cfg_attr(all(target_arch = "aarch64", target_vendor = "apple"), ignore)]
#[test]
fn test_nearvm_upgrade() {
    let mut capture = near_o11y::testonly::TracingCapture::enable();

    let old_protocol_version =
        near_primitives::version::ProtocolFeature::NearVmRuntime.protocol_version() - 1;

    // Prepare TestEnv with a contract at the old protocol version.
    let mut env = {
        let epoch_length = 5;
        let mut genesis =
            Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
        genesis.config.epoch_length = epoch_length;
        genesis.config.protocol_version = old_protocol_version;
        let mut env = TestEnv::builder(&genesis.config)
            .nightshade_runtimes_with_runtime_config_store(
                &genesis,
                vec![RuntimeConfigStore::new(None)],
            )
            .build();

        deploy_test_contract(
            &mut env,
            "test0".parse().unwrap(),
            near_test_contracts::backwards_compatible_rs_contract(),
            epoch_length,
            1,
        );
        env
    };

    let signer: Signer =
        InMemorySigner::from_seed("test0".parse().unwrap(), KeyType::ED25519, "test0").into();
    let tx = TransactionV0 {
        signer_id: "test0".parse().unwrap(),
        receiver_id: "test0".parse().unwrap(),
        public_key: signer.public_key(),
        actions: vec![Action::FunctionCall(Box::new(FunctionCallAction {
            method_name: "log_something".to_string(),
            args: Vec::new(),
            gas: 100_000_000_000_000,
            deposit: 0,
        }))],

        nonce: 0,
        block_hash: CryptoHash::default(),
    };

    // Run the transaction & collect the logs.
    let logs_at_old_version = {
        let tip = env.clients[0].chain.head().unwrap();
        let signed_transaction = Transaction::V0(TransactionV0 {
            nonce: 10,
            block_hash: tip.last_block_hash,
            ..tx.clone()
        })
        .sign(&signer);
        assert_eq!(
            env.clients[0].process_tx(signed_transaction, false, false),
            ProcessTxResponse::ValidTx
        );
        for i in 0..3 {
            env.produce_block(0, tip.height + i + 1);
        }
        capture.drain()
    };

    env.upgrade_protocol_to_latest_version();

    // Re-run the transaction.
    let logs_at_new_version = {
        let tip = env.clients[0].chain.head().unwrap();
        let signed_transaction =
            Transaction::V0(TransactionV0 { nonce: 11, block_hash: tip.last_block_hash, ..tx })
                .sign(&signer);
        assert_eq!(
            env.clients[0].process_tx(signed_transaction, false, false),
            ProcessTxResponse::ValidTx
        );
        for i in 0..3 {
            env.produce_block(0, tip.height + i + 1);
        }
        capture.drain()
    };

    assert!(
        logs_at_old_version.iter().any(|l| l.contains(&"Wasmer2VM::run_method")),
        "{:#?}",
        logs_at_old_version
    );
    assert!(
        logs_at_new_version.iter().any(|l| l.contains(&"NearVM::run_method")),
        "{:#?}",
        logs_at_new_version
    );
}
