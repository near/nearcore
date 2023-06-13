//! Tests to verify the compute cost limit for storage operations are increased
//! for protocol version 61. See `core/primitives/res/runtime_configs/61.yaml`
//! for the exact changes.
//!
//! We test `storage_write` and `storage_remove` because they can easily be
//! tested through existing methods `insert_strings` and `delete_strings` in
//! `near_test_contracts::rs_contract()`. This doesn't cover all the changed
//! costs individually but if it works for two parameters we can be reasonably
//! confident that it works for the others as well.
//! We also test unaffected cases to make sure compute costs only affect
//! parameters they should.

use near_chain::ChainGenesis;
use near_chain_configs::Genesis;
use near_client::test_utils::TestEnv;
use near_client::ProcessTxResponse;
use near_crypto::{InMemorySigner, KeyType};
use near_primitives::config::ActionCosts;
use near_primitives::runtime::config_store::RuntimeConfigStore;
use near_primitives::sharding::ShardChunk;
use near_primitives::transaction::{
    Action, DeployContractAction, FunctionCallAction, SignedTransaction,
};
use near_primitives::types::AccountId;
use near_primitives::version::ProtocolFeature;
use nearcore::config::GenesisExt;
use node_runtime::config::RuntimeConfig;

use crate::tests::client::utils::TestEnvNightshadeSetupExt;

/// Tracked in https://github.com/near/nearcore/issues/8938
const INCREASED_STORAGE_COSTS_PROTOCOL_VERSION: u32 = 61;

/// Test that `storage_write` compute limit is respected in new version.
#[test]
fn test_storage_write() {
    //  `insert_strings(from: u64, to: u64)` makes (`to` - `from`) `storage_write` calls.
    let method_name = "insert_strings".to_owned();
    let num_writes = 100u64;
    let method_args: Vec<u8> =
        0u64.to_le_bytes().into_iter().chain(num_writes.to_le_bytes()).collect();
    let num_transactions = 200;
    let uses_storage = true;
    let fails = false;
    let gas_divider = 1;
    assert_compute_limit_reached(
        method_name,
        method_args,
        num_transactions,
        uses_storage,
        gas_divider,
        fails,
    );
}

/// Test that `storage_remove` compute limit is respected in new version.
#[test]
fn test_storage_remove() {
    //  `delete_strings(from: u64, to: u64)` makes (`to` - `from`) `storage_remove` calls.
    let method_name = "delete_strings".to_owned();
    let num_deletes = 1000u64;
    let method_args: Vec<u8> =
        0u64.to_le_bytes().into_iter().chain(num_deletes.to_le_bytes()).collect();
    let num_transactions = 10;
    let uses_storage = true;
    let fails = false;
    let gas_divider = 10;
    assert_compute_limit_reached(
        method_name,
        method_args,
        num_transactions,
        uses_storage,
        gas_divider,
        fails,
    );
}

/// Test that `storage_write` compute limit is respected in new version,
/// specifically when running out of gas.
#[test]
fn test_storage_write_gas_exceeded() {
    //  `insert_strings(from: u64, to: u64)` makes (`to` - `from`) `storage_write` calls.
    let method_name = "insert_strings".to_owned();
    // 10000 writes should be too much and result in gas exceeded
    let num_writes = 10000u64;
    let method_args: Vec<u8> =
        0u64.to_le_bytes().into_iter().chain(num_writes.to_le_bytes()).collect();
    let num_transactions = 10;
    let uses_storage = true;
    let fails = true;
    let gas_divider = 1;
    assert_compute_limit_reached(
        method_name,
        method_args,
        num_transactions,
        uses_storage,
        gas_divider,
        fails,
    );
}

/// Check receipts that don't touch storage are unaffected by the new compute costs.
#[test]
fn test_non_storage() {
    // `sum_n(u64)` just does some WASM computation.
    // It should not be affected by compute costs, as it doesn't access storage.
    let method_name = "sum_n".to_owned();
    // note: If we make `method_args` only 1M, then the gas cost increase due to
    // finite-wasm change (also included in version 61) is large enough that we
    // see the number of receipts fitting in a chunk going from 64 to 61 even
    // without the compute costs. 10.03M is the sweet spot where the granularity is
    // course enough that the finite-wasm change doesn't interfere. We can
    // process exactly 8 receipts per chunk before and after finite-wasm.
    // And if compute costs would additionally affect this, the test would fail
    // because it is very close at the border between 7 or 8 receipts fitting
    // (7 receipts with finite wasm require 999 Tgas, 8 receipts require 1142 Tgas).
    let method_args: Vec<u8> = 10_030_000u64.to_le_bytes().to_vec();
    let num_transactions = 2;
    let uses_storage = false;
    let fails: bool = false;
    let gas_divider = 10;
    assert_compute_limit_reached(
        method_name,
        method_args,
        num_transactions,
        uses_storage,
        gas_divider,
        fails,
    );
}

/// Test the case where a function call fails and the limit is unaffected by compute costs.
#[test]
fn test_non_storage_gas_exceeded() {
    // `loop_forever()` loops until either gas is exhausted.
    // It should not be affected by compute costs, as it doesn't access storage.
    let method_name = "loop_forever".to_owned();
    let method_args: Vec<u8> = vec![];
    let num_transactions = 2;
    let uses_storage = false;
    let gas_divider = 10;
    let fails = true;
    assert_compute_limit_reached(
        method_name,
        method_args,
        num_transactions,
        uses_storage,
        gas_divider,
        fails,
    );
}

/// Checks that a specific function call reaches the expected limits.
///
/// (This is a helper function called by all tests above)
///
/// The function call is on the test contract
/// (`near_test_contracts::rs_contract()`). The gas limit is tested at version
/// 60 (before compute costs) and the limit is checked again with version 61
/// (with compute costs). The boolean `uses_storage` defines
/// whether we expect the second limit to be more restrictive or if they should
/// both fill a chunk equally.
///
/// Regarding `num_transactions`, this is how many function calls are queued up,
/// it should not be more than can be converted in a single chunk but more than can
/// be executed in a single chunk. Otherwise, the test doesn't work to check the
/// limits and consequently some assertions will fail.
fn assert_compute_limit_reached(
    method_name: String,
    method_args: Vec<u8>,
    num_transactions: u64,
    uses_storage: bool,
    gas_divider: u64,
    should_fail: bool,
) {
    // The immediate protocol upgrade needs to be set for this test to pass in
    // the release branch where the protocol upgrade date is set.
    std::env::set_var("NEAR_TESTS_IMMEDIATE_PROTOCOL_UPGRADE", "1");
    near_o11y::testonly::init_test_logger();

    let new_protocol_version = INCREASED_STORAGE_COSTS_PROTOCOL_VERSION;
    assert!(
        new_protocol_version >= ProtocolFeature::ComputeCosts.protocol_version(),
        "relies on compute costs feature"
    );
    let old_protocol_version = new_protocol_version - 1;

    // Prepare TestEnv with a contract at the old protocol version.
    let epoch_length = 100;
    let contract_account: AccountId = "test0".parse().unwrap();
    let user_account: AccountId = "test1".parse().unwrap();
    let runtime_config_store = RuntimeConfigStore::new(None);
    let old_config = runtime_config_store.get_config(old_protocol_version).clone();
    let new_config = runtime_config_store.get_config(new_protocol_version).clone();
    let mut env = {
        let mut genesis = Genesis::test(vec![contract_account.clone(), user_account.clone()], 1);
        genesis.config.epoch_length = epoch_length;
        genesis.config.protocol_version = old_protocol_version;
        genesis.config.gas_limit = genesis.config.gas_limit / gas_divider;
        let chain_genesis = ChainGenesis::new(&genesis);
        TestEnv::builder(chain_genesis)
            .real_epoch_managers(&genesis.config)
            .nightshade_runtimes_with_runtime_config_store(&genesis, vec![runtime_config_store])
            .build()
    };

    // setup: deploy the contract
    {
        // This contract has a bunch of methods to invoke storage operations.
        let code = near_test_contracts::backwards_compatible_rs_contract().to_vec();
        let actions = vec![Action::DeployContract(DeployContractAction { code })];

        let signer = InMemorySigner::from_seed(
            contract_account.clone(),
            KeyType::ED25519,
            &contract_account,
        );
        let tx = env.tx_from_actions(actions, &signer, signer.account_id.clone());
        env.execute_tx(tx).unwrap().assert_success();
    }

    // Fetching the correct nonce from `env` is a bit fiddly, we would have to
    // query the access key of the user. It's easier to keep a shared counter
    // that starts at 1 and increases monotonically.
    let mut nonce = 1;

    let old_chunk = produce_saturated_chunk(
        &mut env,
        &user_account,
        &contract_account,
        method_name.clone(),
        method_args.clone(),
        num_transactions,
        should_fail,
        old_config.as_ref(),
        &mut nonce,
    );
    let chunk_header = old_chunk.cloned_header();
    let gas_burnt = chunk_header.gas_used();
    let gas_limit: u64 = chunk_header.gas_limit();
    assert!(
        gas_burnt >= gas_limit,
        "should saturate gas limit, only burnt {gas_burnt} when limit was {gas_limit}"
    );

    env.upgrade_protocol(new_protocol_version);

    let new_chunk = produce_saturated_chunk(
        &mut env,
        &user_account,
        &contract_account,
        method_name,
        method_args,
        num_transactions,
        should_fail,
        new_config.as_ref(),
        &mut nonce,
    );

    let old_receipts_num = old_chunk.receipts().len();
    let new_receipts_num = new_chunk.receipts().len();
    if uses_storage {
        assert!(
            new_receipts_num < old_receipts_num,
            "should reach compute limit before gas limit (receipts before: {} receipts now: {})",
            old_receipts_num,
            new_receipts_num,
        );
    } else {
        assert_eq!(old_receipts_num, new_receipts_num, "compute costs should not affect this test");
    }
}

/// Saturate a chunk with function call receipts and returns that chunk.
///
/// (This is a helper function called twice by the helper function above, once
/// before and once after the upgrade.)
///
/// This function creates many function call receipts with the given signer,
/// receiver, method name, and method argument. Then it submits it to a client
/// and produces a few blocks. Then it returns the first chunk that executes
/// these receipts. This chunk should be saturated with receipts, so either the
/// gas limit or the compute limit were reached.
/// (depending on protocol version)
fn produce_saturated_chunk(
    env: &mut TestEnv,
    user_account: &AccountId,
    contract_account: &AccountId,
    method_name: String,
    args: Vec<u8>,
    num_transactions: u64,
    should_fail: bool,
    config: &RuntimeConfig,
    nonce: &mut u64,
) -> std::sync::Arc<ShardChunk> {
    let msg_len = (method_name.len() + args.len()) as u64; // needed for gas computation later
    let gas = 300_000_000_000_000;
    let actions =
        vec![Action::FunctionCall(FunctionCallAction { method_name, args, gas, deposit: 0 })];
    let signer = InMemorySigner::from_seed(user_account.clone(), KeyType::ED25519, user_account);

    let tip = env.clients[0].chain.head().unwrap();
    let mut tx_factory = || {
        let tx = SignedTransaction::from_actions(
            *nonce,
            signer.account_id.clone(),
            contract_account.clone(),
            &signer,
            actions.clone(),
            tip.last_block_hash,
        );
        *nonce += 1;
        tx
    };

    // IMPORTANT: Run one warm-up round to set up the trie shape. Without this,
    // the gas costs are all over the place, which usually results in the first
    // chunk being much cheaper than everything that follows. Which makes it
    // look like compute costs work even if they don't!
    let result = env.execute_tx(tx_factory()).unwrap();
    if !should_fail {
        result.assert_success();
    }

    let tip = env.clients[0].chain.head().unwrap();
    let mut tx_ids = vec![];
    for _ in 0..num_transactions {
        let tx = tx_factory();
        tx_ids.push(tx.get_hash());

        // add tx to the mempool but don't execute it yet
        assert_eq!(env.clients[0].process_tx(tx, false, false), ProcessTxResponse::ValidTx);
    }

    // process the queued transactions
    env.produce_block(0, tip.height + 1); // this produces an empty chunk
    env.produce_block(0, tip.height + 2); // transactions are included in the chunk
    env.produce_block(0, tip.height + 3); // receipts are included in the chunk
    env.produce_block(0, tip.height + 4); // receipts are executed, one refund receipt generated for each receipt executed

    // chunk with transactions accepted but not yet executed
    {
        let chunk = chunk_info(env, tip.height + 2);
        assert_eq!(0, chunk.receipts().len(), "First chunk should only include transactions");
        assert_eq!(
            num_transactions as usize,
            chunk.transactions().len(),
            "All created transactions should be accepted in one chunk"
        );
    }
    // chunk where transactions are converted to receipts
    {
        let chunk = chunk_info(env, tip.height + 3);
        assert_eq!(
            num_transactions as usize,
            chunk.receipts().len(),
            "Second chunk should include all receipts"
        );
        assert_eq!(0, chunk.transactions().len(), "Second chunk shouldn't have new transactions");

        // Note: Receipts are included in chunk, but executed are
        // transactions from previous chunk, so the gas here is for
        // transactions -> receipt conversion.
        let gas_burnt = chunk.cloned_header().gas_used();
        let want_gas_per_tx = config.fees.fee(ActionCosts::new_action_receipt).send_not_sir
            + config.fees.fee(ActionCosts::function_call_base).send_not_sir
            + config.fees.fee(ActionCosts::function_call_byte).send_not_sir * msg_len;
        assert_eq!(
            gas_burnt,
            want_gas_per_tx * num_transactions,
            "Didn't burn the expected amount of gas to convert all transactions to receipts"
        );
    }
    // chunk receipts start to execute, we are counting the refund receipts generated
    let saturated_chunk = {
        let chunk: std::sync::Arc<ShardChunk> = chunk_info(env, tip.height + 4);
        assert_ne!(
            num_transactions as usize,
            chunk.receipts().len(),
            "Not all receipts should fit in a single chunk"
        );
        assert_ne!(0, chunk.receipts().len(), "No receipts executed");
        chunk
    };

    // execute more blocks to get all pending receipts flushed out
    for i in 5..30 {
        env.produce_block(0, tip.height + i); // receipts are executed
    }
    // check all transactions are successfully executed (unless the test
    // explicitly wants failing receipts)
    if !should_fail {
        for id in tx_ids {
            env.clients[0].chain.get_final_transaction_result(&id).unwrap().assert_success();
        }
    }

    saturated_chunk
}

/// fetch chunk for shard 0 and specified block height
fn chunk_info(
    env: &mut TestEnv,
    height: u64,
) -> std::sync::Arc<near_primitives::sharding::ShardChunk> {
    let block = &env.clients[0].chain.get_block_by_height(height).unwrap();
    let chunks = &block.chunks();
    assert_eq!(chunks.len(), 1, "test assumes single shard");
    let chunk_header = chunks.get(0).unwrap().clone();
    env.clients[0].chain.get_chunk(&chunk_header.chunk_hash()).unwrap()
}
