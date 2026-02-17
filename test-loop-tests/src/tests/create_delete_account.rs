use assert_matches::assert_matches;
use itertools::Itertools;
use near_async::futures::{DelayedActionRunner, DelayedActionRunnerExt};
use near_async::time::Duration;
use near_chain_configs::test_genesis::{TestEpochConfigBuilder, ValidatorsSpec};
use near_client::QueryError;
use near_client::client_actor::ClientActor;
use near_o11y::testonly::init_test_logger;
use near_primitives::test_utils::create_user_test_signer;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{AccountId, Balance, Gas};
use near_primitives::views::FinalExecutionStatus;

use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::utils::account::{
    create_account_ids, create_validators_spec, validators_spec_clients_with_rpc,
};
use crate::utils::transactions::{
    call_contract, check_txs, do_create_account, do_delete_account, do_deploy_contract,
    get_next_nonce,
};

/// Write block height to contract storage.
fn do_call_contract(env: &mut TestLoopEnv, rpc_id: &AccountId, contract_id: &AccountId) {
    tracing::info!(target: "test", "calling contract");
    let nonce = get_next_nonce(&env.test_loop.data, &env.node_datas, contract_id);
    let tx = call_contract(
        &mut env.test_loop,
        &env.node_datas,
        rpc_id,
        contract_id,
        contract_id,
        "write_block_height".to_string(),
        vec![],
        nonce,
    );
    env.test_loop.run_for(Duration::seconds(5));
    check_txs(&env.test_loop.data, &env.node_datas, rpc_id, &[tx]);
}

/// Tracks latest block heights and checks that all chunks are produced.
fn check_chunks(
    actor: &ClientActor,
    runner: &mut dyn DelayedActionRunner<ClientActor>,
    latest_block_height: std::cell::Cell<u64>,
) {
    let client = &actor.client;
    let tip = client.chain.head().unwrap().height;
    if tip > latest_block_height.get() {
        latest_block_height.set(tip);
        let block = client.chain.get_block_by_height(tip).unwrap();
        let num_shards = block.header().chunk_mask().len();
        println!("Chain tip: {} Chunks: {:?}", tip, block.header().chunk_mask());
        assert_eq!(block.header().chunk_mask(), vec![true; num_shards]);
    }

    runner.run_later("check_chunks", Duration::milliseconds(500), move |this, runner| {
        check_chunks(this, runner, latest_block_height);
    });
}

/// Tests account existence flow, from creation to deletion.
#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_create_delete_account() {
    init_test_logger();
    let builder = TestLoopBuilder::new();

    let epoch_length = 10;
    let accounts =
        (0..5).map(|i| format!("account{}", i).parse().unwrap()).collect::<Vec<AccountId>>();
    let clients = accounts.clone();

    // Split the clients into producers, validators, and rpc nodes.
    let tmp = accounts.iter().map(|t| t.as_str()).collect_vec();
    let (producers, tmp) = tmp.split_at(2);
    let (validators, tmp) = tmp.split_at(2);
    let (rpcs, tmp) = tmp.split_at(1);
    let rpc_id = rpcs[0].parse().unwrap();
    assert!(tmp.is_empty());

    // Build test environment.
    let genesis = TestLoopBuilder::new_genesis_builder()
        .epoch_length(epoch_length)
        .validators_spec(ValidatorsSpec::desired_roles(&producers, &validators))
        .add_user_accounts_simple(&accounts, Balance::from_near(1_000_000))
        .build();
    let epoch_config_store = TestEpochConfigBuilder::build_store_from_genesis(&genesis);
    let mut env = builder
        .genesis(genesis)
        .epoch_config_store(epoch_config_store)
        .clients(clients)
        .build()
        .warmup();

    // Launch a task to check that all chunks are produced.
    // Needed to make sure that chunks are valid. Currently, if chunk
    // validation fails, it only prevents production of new chunks, but the
    // last chunk containing problematic tx or receipt is still executed on the
    // rpc nodes, and result will be considered final based on block
    // endorsements.
    let mut client_sender = env.node_datas[0].client_sender.clone();
    client_sender.run_later("check_chunks", Duration::seconds(0), move |actor, runner| {
        check_chunks(actor, runner, std::cell::Cell::new(0));
    });

    let new_account: AccountId = format!("alice.{}", accounts[0]).parse().unwrap();
    let contract_code = near_test_contracts::rs_contract().to_vec();

    // Create account.
    do_create_account(&mut env, &rpc_id, &accounts[0], &new_account, Balance::from_near(100));
    // Deploy contract.
    do_deploy_contract(&mut env, &rpc_id, &new_account, contract_code);
    // Write a key-value pair to the contract storage.
    do_call_contract(&mut env, &rpc_id, &new_account);
    // Delete account. Should remove everything - account, contract code and
    // storage.
    do_delete_account(&mut env, &rpc_id, &new_account, &accounts[1]);

    env.test_loop.run_for(Duration::seconds(20));
    env.shutdown_and_drain_remaining_events(Duration::seconds(20));
}

/// Tests that a receipt containing a single DeleteAccount action is processed
/// as an instant receipt (in the same block as the parent receipt that created it).
///
/// The test deploys the rs_contract to a user account, then calls `call_promise`
/// to create a batch promise on itself with a single `action_delete_account`.
/// It verifies that the delete account receipt was processed in the same block
/// as the parent `call_promise` receipt.
#[test]
fn test_instant_delete_account() {
    init_test_logger();

    let user_accounts = create_account_ids(["account0", "account1"]);
    let initial_balance = Balance::from_near(1_000_000);
    let validators_spec = create_validators_spec(2, 0);
    let clients = validators_spec_clients_with_rpc(&validators_spec);
    let genesis = TestLoopBuilder::new_genesis_builder()
        .validators_spec(validators_spec)
        .add_user_accounts_simple(&user_accounts, initial_balance)
        .build();
    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store_from_genesis()
        .clients(clients)
        .build()
        .warmup();

    let [contract_account, beneficiary] = &user_accounts;
    let contract_signer = create_user_test_signer(contract_account);

    // Deploy rs_contract.
    let nonce = 1;
    let block_hash = env.rpc_node().head().last_block_hash;
    let tx = SignedTransaction::deploy_contract(
        nonce,
        contract_account,
        near_test_contracts::rs_contract().to_vec(),
        &contract_signer,
        block_hash,
    );
    env.rpc_runner().run_tx(tx, Duration::seconds(5));

    // Call `call_promise` on the contract to create a batch promise on itself
    // with a single DeleteAccount action. The contract deletes its own account.
    // This produces a child receipt with only DeleteAccount, which should be instant.
    let nonce = 2;
    let block_hash = env.rpc_node().head().last_block_hash;
    let call_promise_args = serde_json::json!([
        {
            "batch_create": { "account_id": contract_account.as_str() },
            "id": 0
        },
        {
            "action_delete_account": {
                "promise_index": 0,
                "beneficiary_id": beneficiary.as_str()
            },
            "id": 0,
            "return": true
        }
    ]);
    let tx = SignedTransaction::call(
        nonce,
        contract_account.clone(),
        contract_account.clone(),
        &contract_signer,
        Balance::ZERO,
        "call_promise".to_string(),
        serde_json::to_vec(&call_promise_args).unwrap(),
        Gas::from_teragas(300),
        block_hash,
    );
    let outcome = env.rpc_runner().execute_tx(tx, Duration::seconds(10)).unwrap();
    assert!(
        matches!(outcome.status, FinalExecutionStatus::SuccessValue(_)),
        "transaction failed: {:?}",
        outcome.status
    );

    let call_promise_outcome = &outcome.receipts_outcome[0];
    let delete_outcome = &outcome.receipts_outcome[1];
    assert_eq!(
        call_promise_outcome.outcome.receipt_ids,
        vec![delete_outcome.id],
        "call_promise should produce exactly the DeleteAccount receipt"
    );

    // The key assertion: the DeleteAccount receipt was processed in the same
    // block as the parent call_promise receipt, proving it was an instant receipt.
    assert_eq!(
        call_promise_outcome.block_hash, delete_outcome.block_hash,
        "DeleteAccount receipt should be processed in the same block as the parent (instant receipt)"
    );

    // Verify the account no longer exists.
    assert_matches!(
        env.rpc_node().view_account_query(contract_account),
        Err(QueryError::UnknownAccount { .. })
    );

    env.shutdown_and_drain_remaining_events(Duration::seconds(20));
}
