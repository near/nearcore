use crate::env::nightshade_setup::TestEnvNightshadeSetupExt;
use crate::env::test_env::TestEnv;
use near_chain::{Block, Provenance};
use near_chain_configs::Genesis;
use near_client::ProcessTxResponse;
use near_crypto::InMemorySigner;
use near_o11y::testonly::init_test_logger;
use near_primitives::action::{Action, DeployContractAction, FunctionCallAction};
use near_primitives::hash::CryptoHash;
use near_primitives::test_utils::create_test_signer;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{AccountId, BlockHeight};
use near_primitives::version::PROTOCOL_VERSION;
use near_vm_runner::logic::ProtocolVersion;
use node_runtime::config::Rational32;

pub fn set_block_protocol_version(
    block: &mut Block,
    block_producer: AccountId,
    protocol_version: ProtocolVersion,
) {
    let validator_signer = create_test_signer(block_producer.as_str());

    block.mut_header().set_latest_protocol_version(protocol_version);
    block.mut_header().resign(&validator_signer);
}

/// Produce `blocks_number` block in the given environment, starting from the given height.
/// Returns the first unoccupied height in the chain after this operation.
pub fn produce_blocks_from_height_with_protocol_version(
    env: &mut TestEnv,
    blocks_number: u64,
    height: BlockHeight,
    protocol_version: ProtocolVersion,
) -> BlockHeight {
    let next_height = height + blocks_number;
    for i in height..next_height {
        let mut block = env.clients[0].produce_block(i).unwrap().unwrap();
        set_block_protocol_version(&mut block, env.get_client_id(0), protocol_version);
        env.process_block(0, block.clone(), Provenance::PRODUCED);
        for j in 1..env.clients.len() {
            env.process_block(j, block.clone(), Provenance::NONE);
        }
    }
    next_height
}

pub fn produce_blocks_from_height(
    env: &mut TestEnv,
    blocks_number: u64,
    height: BlockHeight,
) -> BlockHeight {
    produce_blocks_from_height_with_protocol_version(env, blocks_number, height, PROTOCOL_VERSION)
}

pub fn create_account(
    env: &mut TestEnv,
    old_account_id: AccountId,
    new_account_id: AccountId,
    epoch_length: u64,
    height: BlockHeight,
    protocol_version: ProtocolVersion,
) -> CryptoHash {
    let block = env.clients[0].chain.get_block_by_height(height - 1).unwrap();
    let signer = InMemorySigner::test_signer(&old_account_id);

    let tx = SignedTransaction::create_account(
        height,
        old_account_id,
        new_account_id,
        10u128.pow(24),
        signer.public_key(),
        &signer,
        *block.hash(),
    );
    let tx_hash = tx.get_hash();
    assert_eq!(env.rpc_handlers[0].process_tx(tx, false, false), ProcessTxResponse::ValidTx);
    produce_blocks_from_height_with_protocol_version(env, epoch_length, height, protocol_version);
    tx_hash
}

pub fn deploy_test_contract_with_protocol_version(
    env: &mut TestEnv,
    account_id: AccountId,
    wasm_code: &[u8],
    epoch_length: u64,
    height: BlockHeight,
    protocol_version: ProtocolVersion,
) -> BlockHeight {
    let block = env.clients[0].chain.get_block_by_height(height - 1).unwrap();
    let signer = InMemorySigner::test_signer(&account_id);

    let tx = SignedTransaction::from_actions(
        height,
        account_id.clone(),
        account_id,
        &signer,
        vec![Action::DeployContract(DeployContractAction { code: wasm_code.to_vec() })],
        *block.hash(),
        0,
    );
    assert_eq!(env.rpc_handlers[0].process_tx(tx, false, false), ProcessTxResponse::ValidTx);
    produce_blocks_from_height_with_protocol_version(env, epoch_length, height, protocol_version)
}

pub fn deploy_test_contract(
    env: &mut TestEnv,
    account_id: AccountId,
    wasm_code: &[u8],
    epoch_length: u64,
    height: BlockHeight,
) -> BlockHeight {
    deploy_test_contract_with_protocol_version(
        env,
        account_id,
        wasm_code,
        epoch_length,
        height,
        PROTOCOL_VERSION,
    )
}

/// Create environment and set of transactions which cause congestion on the chain.
pub fn prepare_env_with_congestion(
    protocol_version: ProtocolVersion,
    gas_price_adjustment_rate: Option<Rational32>,
    number_of_transactions: u64,
) -> (TestEnv, Vec<CryptoHash>) {
    init_test_logger();
    let epoch_length = 100;
    let mut genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
    genesis.config.protocol_version = protocol_version;
    genesis.config.epoch_length = epoch_length;
    genesis.config.gas_limit = 10_000_000_000_000;
    if let Some(gas_price_adjustment_rate) = gas_price_adjustment_rate {
        genesis.config.gas_price_adjustment_rate = gas_price_adjustment_rate;
    }
    let mut env = TestEnv::builder(&genesis.config).nightshade_runtimes(&genesis).build();
    let genesis_block = env.clients[0].chain.get_block_by_height(0).unwrap();
    let signer = InMemorySigner::test_signer(&"test0".parse().unwrap());

    // Deploy contract to test0.
    let tx = SignedTransaction::from_actions(
        1,
        "test0".parse().unwrap(),
        "test0".parse().unwrap(),
        &signer,
        vec![Action::DeployContract(DeployContractAction {
            code: near_test_contracts::backwards_compatible_rs_contract().to_vec(),
        })],
        *genesis_block.hash(),
        0,
    );
    assert_eq!(env.rpc_handlers[0].process_tx(tx, false, false), ProcessTxResponse::ValidTx);
    for i in 1..3 {
        env.produce_block(0, i);
    }

    // Create function call transactions that generate promises.
    let gas_1 = 9_000_000_000_000;
    let gas_2 = gas_1 / 3;
    let mut tx_hashes = vec![];

    for i in 0..number_of_transactions {
        let data = serde_json::json!([
            {"create": {
            "account_id": "test0",
            "method_name": "call_promise",
            "arguments": [],
            "amount": "0",
            "gas": gas_2,
            }, "id": 0 }
        ]);

        let signed_transaction = SignedTransaction::from_actions(
            i + 10,
            "test0".parse().unwrap(),
            "test0".parse().unwrap(),
            &signer,
            vec![Action::FunctionCall(Box::new(FunctionCallAction {
                method_name: "call_promise".to_string(),
                args: serde_json::to_vec(&data).unwrap(),
                gas: gas_1,
                deposit: 0,
            }))],
            *genesis_block.hash(),
            0,
        );
        tx_hashes.push(signed_transaction.get_hash());
        assert_eq!(
            env.rpc_handlers[0].process_tx(signed_transaction, false, false),
            ProcessTxResponse::ValidTx
        );
    }

    (env, tx_hashes)
}
