//! Phase 1 tests for the pending-compile-queue feature.
//!
//! These tests rely only on the eviction and admission paths landed in
//! Phase 1; producer-side advancement signaling and `compiled_indices`
//! injection arrive in later phases. Every admitted entry sits in the
//! queue until TTL eviction.

use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::utils::sharding::get_memtrie_for_shard;
use near_async::time::Duration;
use near_chain::ChainStoreAccess;
use near_epoch_manager::shard_assignment::account_id_to_shard_id;
use near_o11y::testonly::init_test_logger;
use near_primitives::action::{Action, DeployContractAction};
use near_primitives::errors::{ActionError, ActionErrorKind, TxExecutionError};
use near_primitives::receipt::PendingCompileQueueEntry;
use near_primitives::shard_layout::ShardUId;
use near_primitives::transaction::{ExecutionStatus, SignedTransaction};
use near_primitives::types::{AccountId, Balance, BlockHeight};
use near_primitives::version::PROTOCOL_VERSION;
use near_primitives_core::version::ProtocolFeature;
use near_store::trie::receipts_column_helper::read_pending_compile_queue;
use near_test_contracts::wat_contract;

const ADMISSION_CAP: usize = 20;
const TTL_BLOCKS: u64 = 5;

/// Read every entry currently in the pending-compile queue on the shard
/// owning `account` at the env's RPC node head. Phase 0's
/// `read_pending_compile_queue` already skips holes, so the returned
/// vector reflects the live entries.
fn read_queue(env: &TestLoopEnv, account: &AccountId) -> Vec<(u64, PendingCompileQueueEntry)> {
    let node = env.rpc_node();
    let client = node.client();
    let tip = client.chain.head().unwrap();
    let shard_layout = client.epoch_manager.get_shard_layout(&tip.epoch_id).unwrap();
    let shard_id =
        account_id_to_shard_id(client.epoch_manager.as_ref(), account, &tip.epoch_id).unwrap();
    let shard_uid = ShardUId::from_shard_id_and_layout(shard_id, &shard_layout);
    let trie = get_memtrie_for_shard(client, &shard_uid, &tip.last_block_hash);
    read_pending_compile_queue(&trie).unwrap()
}

/// Length of the delayed-receipt queue on the shard owning `account` at
/// the env's RPC node head, derived from `CongestionInfo`.
fn delayed_receipts_gas(env: &TestLoopEnv, account: &AccountId) -> u64 {
    let node = env.rpc_node();
    let client = node.client();
    let tip = client.chain.head().unwrap();
    let shard_layout = client.epoch_manager.get_shard_layout(&tip.epoch_id).unwrap();
    let shard_id =
        account_id_to_shard_id(client.epoch_manager.as_ref(), account, &tip.epoch_id).unwrap();
    let shard_uid = ShardUId::from_shard_id_and_layout(shard_id, &shard_layout);
    client
        .chain
        .chain_store()
        .get_chunk_extra(&tip.last_block_hash, &shard_uid)
        .unwrap()
        .congestion_info()
        .delayed_receipts_gas() as u64
}

/// Build a small, valid wasm contract whose bytecode is unique per `tag`.
/// Used to ensure each deploy in the cap/spillover/steady-state tests has
/// a distinct `code_hash`.
fn unique_contract(tag: u32) -> Vec<u8> {
    wat_contract(&format!(
        r#"(module (memory 1) (func (export "main")) (data (i32.const 0) "tag-{tag}"))"#,
    ))
}

fn deploy_tx(env: &TestLoopEnv, account: &AccountId, code: Vec<u8>) -> SignedTransaction {
    env.rpc_node().tx_from_actions(
        account,
        account,
        vec![Action::DeployContract(DeployContractAction { code })],
    )
}

fn run_until_height(env: &mut TestLoopEnv, target: BlockHeight) {
    env.rpc_runner().run_until_head_height(target);
}

#[test]
fn test_pending_compile_queue_ttl_expiry() {
    init_test_logger();
    let user_account: AccountId = "user".parse().unwrap();
    let mut env = TestLoopBuilder::new()
        .add_user_account(&user_account, Balance::from_near(100))
        .enable_rpc()
        .build();
    assert!(
        ProtocolFeature::CompileQueueDeferral.enabled(PROTOCOL_VERSION),
        "test requires a protocol version where CompileQueueDeferral is active",
    );

    let tx = env.rpc_node().tx_deploy_test_contract(&user_account);
    let tx_hash = tx.get_hash();
    env.rpc_node().submit_tx(tx);
    env.rpc_runner().run_until_outcome_available(tx_hash, Duration::seconds(5));

    let queue_after_admission = read_queue(&env, &user_account);
    assert_eq!(
        queue_after_admission.len(),
        1,
        "queue should hold exactly one entry after admission",
    );
    let (_, entry) = &queue_after_admission[0];
    assert_eq!(
        entry.code_hashes.len(),
        1,
        "entry should track exactly one code hash for a single-deploy receipt",
    );

    let outcome_before_eviction = env.rpc_node().execution_outcome(tx_hash);
    let receipt_id = match outcome_before_eviction.outcome.status {
        ExecutionStatus::SuccessReceiptId(receipt_id) => receipt_id,
        ref status => panic!("unexpected tx outcome status: {status:?}"),
    };

    // At pushed_at + TTL the entry is still permitted to advance.
    run_until_height(&mut env, entry.pushed_at_height + TTL_BLOCKS);
    assert_eq!(
        read_queue(&env, &user_account).len(),
        1,
        "entry should remain queued through pushed_at + TTL",
    );

    // pushed_at + TTL + 1 evicts.
    run_until_height(&mut env, entry.pushed_at_height + TTL_BLOCKS + 1);
    env.rpc_runner().run_until_outcome_available(receipt_id, Duration::seconds(2));

    assert!(read_queue(&env, &user_account).is_empty(), "queue should be empty after eviction",);
    let outcome_after_eviction = env.rpc_node().execution_outcome(receipt_id);
    assert!(
        matches!(
            outcome_after_eviction.outcome.status,
            ExecutionStatus::Failure(TxExecutionError::ActionError(ActionError {
                kind: ActionErrorKind::CompileQueueExpired,
                ..
            }))
        ),
        "evicted receipt outcome should be CompileQueueExpired, got: {:?}",
        outcome_after_eviction.outcome.status,
    );
}

#[test]
fn test_pending_compile_queue_admission_cap_spillover() {
    init_test_logger();
    let user_account: AccountId = "user".parse().unwrap();
    let total_deploys = 30usize;
    let mut env = TestLoopBuilder::new()
        .add_user_account(&user_account, Balance::from_near(1000))
        .enable_rpc()
        .build();
    assert!(ProtocolFeature::CompileQueueDeferral.enabled(PROTOCOL_VERSION));

    let initial_height = env.rpc_node().head().height;
    let txs: Vec<SignedTransaction> = (0..total_deploys)
        .map(|i| deploy_tx(&env, &user_account, unique_contract(i as u32 + 1)))
        .collect();
    for tx in &txs {
        env.rpc_node().submit_tx(tx.clone());
    }
    env.rpc_runner().run_until_head_height(initial_height + 3);

    let queue = read_queue(&env, &user_account);
    assert_eq!(
        queue.len(),
        ADMISSION_CAP,
        "first chunk should admit exactly the per-chunk cap; got {}",
        queue.len(),
    );
    assert!(
        delayed_receipts_gas(&env, &user_account) > 0,
        "the spilled-over deploys must be sitting in the delayed-receipt queue",
    );
}

#[test]
fn test_pending_compile_queue_steady_state_bound() {
    init_test_logger();
    let user_account: AccountId = "user".parse().unwrap();
    let mut env = TestLoopBuilder::new()
        .add_user_account(&user_account, Balance::from_near(10_000))
        .enable_rpc()
        .build();
    assert!(ProtocolFeature::CompileQueueDeferral.enabled(PROTOCOL_VERSION));

    let mut tag: u32 = 1;
    let deploys_per_block = ADMISSION_CAP + 5;
    let admission_blocks = 12u64;
    let initial_height = env.rpc_node().head().height;
    let mut max_observed: usize = 0;

    for block_offset in 0..admission_blocks {
        let target = initial_height + block_offset + 1;
        for _ in 0..deploys_per_block {
            let tx = deploy_tx(&env, &user_account, unique_contract(tag));
            tag += 1;
            env.rpc_node().submit_tx(tx);
        }
        env.rpc_runner().run_until_head_height(target);
        let len = read_queue(&env, &user_account).len();
        max_observed = max_observed.max(len);
    }

    let upper_bound = (TTL_BLOCKS as usize + 1) * ADMISSION_CAP;
    assert!(
        max_observed <= upper_bound,
        "queue exceeded the (TTL+1) * admission_cap bound: observed {} > {}",
        max_observed,
        upper_bound,
    );
}

#[test]
fn test_pending_compile_queue_pre_feature_regression() {
    init_test_logger();
    let post_feature = ProtocolFeature::CompileQueueDeferral.protocol_version();
    let pre_feature = post_feature - 1;
    assert!(
        !ProtocolFeature::CompileQueueDeferral.enabled(pre_feature),
        "pre_feature must be a protocol version where CompileQueueDeferral is inactive",
    );

    let user_account: AccountId = "user".parse().unwrap();
    let mut env = TestLoopBuilder::new()
        .protocol_version(pre_feature)
        .add_user_account(&user_account, Balance::from_near(100))
        .enable_rpc()
        .build();

    let tx = deploy_tx(&env, &user_account, unique_contract(7));
    let tx_hash = tx.get_hash();
    env.rpc_runner().run_tx(tx, Duration::seconds(5));

    let queue = read_queue(&env, &user_account);
    assert!(queue.is_empty(), "pre-feature deploy must not enter the pending-compile queue",);

    let outcome = env.rpc_node().execution_outcome(tx_hash);
    let receipt_id = match outcome.outcome.status {
        ExecutionStatus::SuccessReceiptId(receipt_id) => receipt_id,
        ref status => panic!("unexpected tx outcome status: {status:?}"),
    };
    let receipt_outcome = env.rpc_node().execution_outcome(receipt_id);
    assert!(
        matches!(
            receipt_outcome.outcome.status,
            ExecutionStatus::SuccessValue(_) | ExecutionStatus::SuccessReceiptId(_)
        ),
        "pre-feature deploy receipt should execute inline successfully, got: {:?}",
        receipt_outcome.outcome.status,
    );
}
