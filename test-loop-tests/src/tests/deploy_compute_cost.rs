use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::utils::account::create_account_id;
use crate::utils::node::TestLoopNode;
use near_async::time::Duration;
use near_client_primitives::types::QueryError;
use near_o11y::testonly::init_test_logger;
use near_parameters::{RuntimeConfig, RuntimeConfigStore};
use near_primitives::action::GlobalContractDeployMode;
use near_primitives::gas::Gas;
use near_primitives::hash::{CryptoHash, hash};
use near_primitives::receipt::ReceiptEnum;
use near_primitives::transaction::ExecutionStatus;
use near_primitives::types::{Balance, ShardId};
use near_primitives::version::PROTOCOL_VERSION;
use near_primitives::views::{QueryRequest, QueryResponseKind};
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

const GLOBAL_CONTRACT_SIZE: usize = 1000;

/// Two global-contract deploys must execute in different chunks: each
/// `GlobalContractDistribution` receipt charges
/// `deploy_global_contract_execution_base + per_byte * code_len` of compute,
/// and we pin the chunk's compute budget (chunk `gas_limit`, which today doubles
/// as `compute_limit`) to exactly that value. Each receipt then saturates the
/// chunk on its own, deferring the next one to the delayed-receipts queue.
///
/// The two contracts have slightly different sizes (and thus different code
/// hashes / identifiers) so both distribution receipts do full work; with a
/// shared identifier the second deploy would bump the on-chain nonce before
/// the first distribution receipt is applied, making it stale and short-circuit
/// to zero compute. We also use two distinct signer accounts because SPICE's
/// pending-tx queue enforces deploy exclusivity per signer (NEP-611), which
/// would otherwise serialize the two deploys into separate source chunks.
#[test]
fn test_deploy_global_contract_compute_cost_splits_chunks() {
    init_test_logger();

    let runtime_config_store = RuntimeConfigStore::new(None);
    let fees = &runtime_config_store.get_config(PROTOCOL_VERSION).fees;
    let compute_per_receipt = fees.deploy_global_contract_execution_base
        + (GLOBAL_CONTRACT_SIZE as u64) * fees.deploy_global_contract_execution_per_byte;
    let gas_limit = Gas::from_gas(compute_per_receipt);

    let user1 = create_account_id("user1");
    let user2 = create_account_id("user2");
    let mut env = TestLoopBuilder::new()
        .add_user_accounts([&user1, &user2], Balance::from_near(100_000))
        .gas_limit(gas_limit)
        .build();

    let code1 = near_test_contracts::sized_contract(GLOBAL_CONTRACT_SIZE);
    let code2 = near_test_contracts::sized_contract(GLOBAL_CONTRACT_SIZE + 1);
    let code_hash_1 = hash(&code1);
    let code_hash_2 = hash(&code2);

    let tx1 = env.validator().tx_deploy_global_contract(
        &user1,
        code1,
        GlobalContractDeployMode::CodeHash,
    );
    let tx2 = env.validator().tx_deploy_global_contract(
        &user2,
        code2,
        GlobalContractDeployMode::CodeHash,
    );

    env.validator().submit_tx(tx1);
    env.validator().submit_tx(tx2);

    // Wait for the chunk that emits both `GlobalContractDistribution` receipts
    // as outgoing — i.e. the chunk where both deploys' action receipts have
    // just been applied.
    env.validator_runner()
        .run_until(|node| count_outgoing_distribution_receipts(node) == 2, Duration::seconds(20));

    // Run one more block: the two distribution receipts arrive together, but
    // the chunk's compute budget only fits one — one is applied, the other is
    // deferred to the delayed-receipts queue. With two distinct signers, the
    // intra-chunk ordering of receipts is not nonce-deterministic, so we only
    // assert that exactly one of the two contracts has become available.
    env.validator_runner().run_for_number_of_blocks(1);
    let block_processing_first = env.validator().head_block();
    env.validator_runner()
        .run_until_block_executed(block_processing_first.header(), Duration::seconds(10));
    let available_1 = is_global_contract_available(&env, code_hash_1);
    let available_2 = is_global_contract_available(&env, code_hash_2);
    assert!(
        available_1 ^ available_2,
        "exactly one contract should be available after one chunk processes a distribution receipt (1={available_1}, 2={available_2})",
    );

    // Run one more block: the deferred distribution receipt is popped from the
    // queue and applied; the other contract becomes available too.
    env.validator_runner().run_for_number_of_blocks(1);
    let block_processing_second = env.validator().head_block();
    env.validator_runner()
        .run_until_block_executed(block_processing_second.header(), Duration::seconds(10));
    assert!(
        is_global_contract_available(&env, code_hash_1),
        "first contract should be available after the deferred receipt is processed",
    );
    assert!(
        is_global_contract_available(&env, code_hash_2),
        "second contract should be available after the deferred receipt is processed",
    );
}

fn count_outgoing_distribution_receipts(node: &TestLoopNode) -> usize {
    let head = node.head();
    let receipts = node
        .client()
        .chain
        .get_outgoing_receipts_for_shard(head.last_block_hash, ShardId::new(0), head.height)
        .unwrap_or_default();
    receipts
        .iter()
        .filter(|r| matches!(r.receipt(), ReceiptEnum::GlobalContractDistribution(_)))
        .count()
}

fn is_global_contract_available(env: &TestLoopEnv, code_hash: CryptoHash) -> bool {
    match env.validator().runtime_query(QueryRequest::ViewGlobalContractCode { code_hash }) {
        Ok(response) => {
            matches!(response.kind, QueryResponseKind::ViewCode(_))
        }
        Err(QueryError::NoGlobalContractCode { .. }) => false,
        Err(e) => panic!("unexpected query error for code hash {code_hash}: {e:?}"),
    }
}
