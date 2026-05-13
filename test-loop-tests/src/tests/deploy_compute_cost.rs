use crate::setup::builder::TestLoopBuilder;
use crate::utils::account::create_account_id;
use near_async::time::Duration;
use near_o11y::testonly::init_test_logger;
use near_parameters::{RuntimeConfig, RuntimeConfigStore};
use near_primitives::transaction::ExecutionStatus;
use near_primitives::types::Balance;
use near_primitives::version::PROTOCOL_VERSION;
use std::collections::HashSet;
use std::sync::Arc;

/// Verifies base compute costs for deploy contract actions work.
///
/// Base compute per deploy: 20 Tgas => at most 50 per chunk
#[test]
fn test_deploy_compute_cost_limit() {
    let code = near_test_contracts::trivial_contract().to_vec();
    let max_per_chunk = 50;

    check_deploy_compute_cost_limits_chunk_capacity(code, max_per_chunk)
}

/// Verifies per-byte compute costs for deploy contract actions work.
///
/// Per-byte compute: 250Mgas per byte => at most 4MB per chunk
#[test]
fn test_deploy_compute_cost_per_byte_limit() {
    // 1MB contract
    let code = near_test_contracts::sized_contract(1_000_000).to_vec();
    let max_per_chunk = 4;

    check_deploy_compute_cost_limits_chunk_capacity(code, max_per_chunk)
}

fn check_deploy_compute_cost_limits_chunk_capacity(code: Vec<u8>, max_per_chunk: usize) {
    init_test_logger();

    let num_txs = max_per_chunk + 1;
    // Use many accounts to get around SPICE's limit to one deployment per
    // account per chunk.
    let users: Vec<_> = (0..num_txs).map(|i| create_account_id(&format!("user{i}"))).collect();
    let mut config = RuntimeConfigStore::new(None);

    let arc_cfg: &mut Arc<RuntimeConfig> = config.get_config_mut(PROTOCOL_VERSION);
    let cfg: &mut RuntimeConfig = Arc::make_mut(arc_cfg);
    cfg.witness_config.combined_transactions_size_limit = usize::MAX;

    let mut env = TestLoopBuilder::new()
        .add_user_accounts(&users, Balance::from_near(100))
        .runtime_config_store(config)
        .enable_rpc()
        .build();

    let txs: Vec<_> =
        users.iter().map(|user| env.rpc_node().tx_deploy_contract(user, code.clone())).collect();

    for tx in &txs {
        env.rpc_node().submit_tx(tx.clone());
    }

    // Wait until one chunk contains all submitted transactions.
    let tx_hashes: HashSet<_> = txs.iter().map(|tx| tx.hash()).collect();
    env.rpc_runner().run_until(
        |node| {
            let head_block = node.head_block();
            let chunk = node.block_chunks(&head_block).pop().unwrap();
            let chunk_hashes: HashSet<_> =
                chunk.to_transactions().iter().map(|tx| tx.hash()).collect();
            chunk_hashes == tx_hashes
        },
        Duration::seconds(10),
    );
    let block_with_txs = env.rpc_node().head_block();

    env.rpc_runner().run_until_block_executed(block_with_txs.header(), Duration::seconds(5));

    // Collect the deploy receipt IDs produced by each transaction.
    let rpc_node = env.rpc_node();
    let chain = &rpc_node.client().chain;
    let receipt_ids: Vec<_> = txs
        .iter()
        .map(|tx| {
            match chain.get_execution_outcome(tx.hash()).unwrap().outcome_with_id.outcome.status {
                ExecutionStatus::SuccessReceiptId(id) => id,
                status => panic!("unexpected tx outcome status: {status:?}"),
            }
        })
        .collect();

    // max_per_chunk is an overestimation; partition into executed vs delayed.
    let (executed, delayed): (Vec<_>, Vec<_>) =
        receipt_ids.iter().copied().partition(|id| chain.get_execution_outcome(id).is_ok());

    assert!(
        executed.len() >= max_per_chunk / 2,
        "too few receipts executed in the chunk (got {})",
        executed.len()
    );
    assert!(!delayed.is_empty(), "no receipts were delayed — compute limit was not enforced");

    // Ensure all delayed receipts execute eventually.
    for receipt_id in delayed {
        env.rpc_runner().run_until_outcome_available(receipt_id, Duration::seconds(10));
    }
}
