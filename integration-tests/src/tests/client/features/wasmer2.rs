use near_chain::ChainGenesis;
use near_chain_configs::Genesis;
use near_client::test_utils::TestEnv;
use near_crypto::{InMemorySigner, KeyType, Signer};
use near_primitives::hash::CryptoHash;
use near_primitives::transaction::{Action, FunctionCallAction, Transaction};
use nearcore::config::GenesisExt;

use crate::tests::client::process_blocks::{create_nightshade_runtimes, deploy_test_contract};

#[test]
fn test_wasmer2_upgrade() {
    let mut capture = near_o11y::TracingCapture::enable();

    let old_protocol_version =
        near_primitives::version::ProtocolFeature::Wasmer2.protocol_version() - 1;
    let new_protocol_version = old_protocol_version + 1;

    // Prepare TestEnv with a contract at the old protocol version.
    let mut env = {
        let epoch_length = 5;
        let mut genesis =
            Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
        genesis.config.epoch_length = epoch_length;
        genesis.config.protocol_version = old_protocol_version;
        let chain_genesis = ChainGenesis::new(&genesis);
        let mut env = TestEnv::builder(chain_genesis)
            .runtime_adapters(create_nightshade_runtimes(&genesis, 1))
            .build();

        deploy_test_contract(
            &mut env,
            "test0".parse().unwrap(),
            near_test_contracts::rs_contract(),
            epoch_length,
            1,
        );
        env
    };

    let signer = InMemorySigner::from_seed("test0".parse().unwrap(), KeyType::ED25519, "test0");
    let tx = Transaction {
        signer_id: "test0".parse().unwrap(),
        receiver_id: "test0".parse().unwrap(),
        public_key: signer.public_key(),
        actions: vec![Action::FunctionCall(FunctionCallAction {
            method_name: "log_something".to_string(),
            args: Vec::new(),
            gas: 100_000_000_000_000,
            deposit: 0,
        })],

        nonce: 0,
        block_hash: CryptoHash::default(),
    };

    // Run the transaction & collect the logs.
    let logs_at_old_version = {
        let tip = env.clients[0].chain.head().unwrap();
        let signed_transaction =
            Transaction { nonce: 10, block_hash: tip.last_block_hash, ..tx.clone() }.sign(&signer);
        env.clients[0].process_tx(signed_transaction, false, false);
        for i in 0..3 {
            env.produce_block(0, tip.height + i + 1);
        }
        capture.drain()
    };

    env.upgrade_protocol(new_protocol_version);

    // Re-run the transaction.
    let logs_at_new_version = {
        let tip = env.clients[0].chain.head().unwrap();
        let signed_transaction =
            Transaction { nonce: 11, block_hash: tip.last_block_hash, ..tx }.sign(&signer);
        env.clients[0].process_tx(signed_transaction, false, false);
        for i in 0..3 {
            env.produce_block(0, tip.height + i + 1);
        }
        capture.drain()
    };

    assert!(logs_at_old_version.iter().any(|l| l.contains(&"vm_kind=Wasmer0")));
    assert!(logs_at_new_version.iter().any(|l| l.contains(&"vm_kind=Wasmer2")));
}
