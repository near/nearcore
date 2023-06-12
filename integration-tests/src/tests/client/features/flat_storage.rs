use crate::tests::client::process_blocks::deploy_test_contract_with_protocol_version;
use crate::tests::client::utils::TestEnvNightshadeSetupExt;
use near_chain::ChainGenesis;
use near_chain_configs::Genesis;
use near_client::test_utils::TestEnv;
use near_client::ProcessTxResponse;
use near_crypto::{InMemorySigner, KeyType, Signer};
use near_primitives::test_utils::encode;
use near_primitives::transaction::{Action, ExecutionMetadata, FunctionCallAction, Transaction};
use near_primitives::version::ProtocolFeature;
use near_primitives_core::config::ExtCosts;
use near_primitives_core::hash::CryptoHash;
use near_primitives_core::types::Gas;
use nearcore::config::GenesisExt;

/// Check that after flat storage upgrade:
/// - value read from contract is the same;
/// - touching trie node cost for read decreases to zero.
#[test]
fn test_flat_storage_upgrade() {
    // The immediate protocol upgrade needs to be set for this test to pass in
    // the release branch where the protocol upgrade date is set.
    std::env::set_var("NEAR_TESTS_IMMEDIATE_PROTOCOL_UPGRADE", "1");

    let mut genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
    let epoch_length = 12;
    let new_protocol_version = ProtocolFeature::FlatStorageReads.protocol_version();
    let old_protocol_version = new_protocol_version - 1;
    genesis.config.epoch_length = epoch_length;
    genesis.config.protocol_version = old_protocol_version;
    let chain_genesis = ChainGenesis::new(&genesis);
    let mut env = TestEnv::builder(chain_genesis)
        .real_epoch_managers(&genesis.config)
        .nightshade_runtimes(&genesis)
        .build();

    // We assume that it is enough to process 4 blocks to get a single txn included and processed.
    // At the same time, once we process `>= 2 * epoch_length` blocks, protocol can get
    // auto-upgraded to latest version. We use this value to process 3 transactions for older
    // protocol version. So we choose this value to be `epoch_length / 3` and we process only
    // `epoch_length` blocks in total.
    // TODO (#8703): resolve this properly
    let blocks_to_process_txn = epoch_length / 3;

    // Deploy contract to state.
    deploy_test_contract_with_protocol_version(
        &mut env,
        "test0".parse().unwrap(),
        near_test_contracts::backwards_compatible_rs_contract(),
        blocks_to_process_txn,
        1,
        old_protocol_version,
    );

    let signer = InMemorySigner::from_seed("test0".parse().unwrap(), KeyType::ED25519, "test0");
    let gas = 20_000_000_000_000;
    let tx = Transaction {
        signer_id: "test0".parse().unwrap(),
        receiver_id: "test0".parse().unwrap(),
        public_key: signer.public_key(),
        actions: vec![],
        nonce: 0,
        block_hash: CryptoHash::default(),
    };

    // Write key-value pair to state.
    {
        let write_value_action = vec![Action::FunctionCall(FunctionCallAction {
            args: encode(&[1u64, 10u64]),
            method_name: "write_key_value".to_string(),
            gas,
            deposit: 0,
        })];
        let tip = env.clients[0].chain.head().unwrap();
        let signed_transaction = Transaction {
            nonce: 10,
            block_hash: tip.last_block_hash,
            actions: write_value_action,
            ..tx.clone()
        }
        .sign(&signer);
        let tx_hash = signed_transaction.get_hash();
        assert_eq!(
            env.clients[0].process_tx(signed_transaction, false, false),
            ProcessTxResponse::ValidTx
        );
        for i in 0..blocks_to_process_txn {
            env.produce_block(0, tip.height + i + 1);
        }

        env.clients[0].chain.get_final_transaction_result(&tx_hash).unwrap().assert_success();
    }

    let touching_trie_node_costs: Vec<_> = (0..2)
        .map(|i| {
            let read_value_action = vec![Action::FunctionCall(FunctionCallAction {
                args: encode(&[1u64]),
                method_name: "read_value".to_string(),
                gas,
                deposit: 0,
            })];
            let tip = env.clients[0].chain.head().unwrap();
            let signed_transaction = Transaction {
                nonce: 20 + i,
                block_hash: tip.last_block_hash,
                actions: read_value_action,
                ..tx.clone()
            }
            .sign(&signer);
            let tx_hash = signed_transaction.get_hash();
            assert_eq!(
                env.clients[0].process_tx(signed_transaction, false, false),
                ProcessTxResponse::ValidTx
            );
            for i in 0..blocks_to_process_txn {
                env.produce_block(0, tip.height + i + 1);
            }
            if i == 0 {
                env.upgrade_protocol(new_protocol_version);
            }

            let final_transaction_result =
                env.clients[0].chain.get_final_transaction_result(&tx_hash).unwrap();
            final_transaction_result.assert_success();
            let receipt_id = final_transaction_result.receipts_outcome[0].id;
            let metadata = env.clients[0]
                .chain
                .get_execution_outcome(&receipt_id)
                .unwrap()
                .outcome_with_id
                .outcome
                .metadata;
            if let ExecutionMetadata::V3(profile_data) = metadata {
                profile_data.get_ext_cost(ExtCosts::touching_trie_node)
            } else {
                panic!("Too old version of metadata: {metadata:?}");
            }
        })
        .collect();

    // Guaranteed touching trie node cost in all protocol versions until
    // `ProtocolFeature::FlatStorageReads`, included.
    let touching_trie_node_base_cost: Gas = 16_101_955_926;

    // For the first read, cost should be 3 TTNs because trie path is:
    // (Branch) -> (Extension) -> (Leaf) -> (Value)
    // but due to a bug in storage_read we don't charge for Value.
    assert_eq!(touching_trie_node_costs[0], touching_trie_node_base_cost * 3);

    // For the second read, we don't go to Flat storage and don't charge TTN.
    assert_eq!(touching_trie_node_costs[1], 0);
}
