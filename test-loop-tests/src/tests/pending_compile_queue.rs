//! Tests for the pending-compile-queue feature (Phases 1 and 2).
//!
//! Phase 1 covered admission, eviction, spillover, and the steady-state
//! bound. Phase 2 added the apply-loop advancement step driven by
//! `compiled_indices` in the chunk header, plus a `test_features` knob
//! `CompiledIndicesOverride` on the chunk producer that lets tests inject
//! the indices that would otherwise come from natural producer signaling
//! (Phase 3).

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

/// Set the `compiled_indices` override on the chunk producer at node 0
/// (the validator-producer in single-shard test setups).
#[cfg(feature = "test_features")]
fn set_compiled_indices_override(
    env: &mut TestLoopEnv,
    override_value: near_client::CompiledIndicesOverride,
) {
    env.validator_runner().send_adversarial_message(
        near_client::NetworkAdversarialMessage::AdvSetCompiledIndicesOverride(override_value),
    );
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

// --- Phase 2 tests: apply-loop advancement via the `compiled_indices`
// injection knob. Each test enables a `Force(...)` override on the
// chunk producer for one chunk, then resets to `Off` so subsequent
// chunks don't re-attempt the (now-popped or invalid) advancement.

#[cfg(feature = "test_features")]
#[test]
fn test_pending_compile_queue_advancement_happy_path() {
    use near_client::CompiledIndicesOverride;
    use near_primitives::hash::CryptoHash;
    use near_primitives::types::Gas;

    init_test_logger();
    let user: AccountId = "user".parse().unwrap();
    let mut env = TestLoopBuilder::new()
        .add_user_account(&user, Balance::from_near(100))
        .enable_rpc()
        .build();
    assert!(ProtocolFeature::CompileQueueDeferral.enabled(PROTOCOL_VERSION));

    // Use unique_contract — exports `main` so we can call it post-advance.
    let code = unique_contract(99);
    let code_hash = CryptoHash::hash_bytes(&code);
    let tx = env.rpc_node().tx_deploy_contract(&user, code);
    let tx_hash = tx.get_hash();
    env.rpc_node().submit_tx(tx);
    env.rpc_runner().run_until_outcome_available(tx_hash, Duration::seconds(5));

    let queue = read_queue(&env, &user);
    assert_eq!(queue.len(), 1, "deploy must be admitted to the queue");
    let (index, _entry) = queue[0].clone();

    // Pre-advancement: account must NOT yet hold the new code.
    let account_before = env.rpc_node().view_account_query(&user).unwrap();
    assert_ne!(
        account_before.code_hash, code_hash,
        "account must not show the deployed code before advancement",
    );

    let receipt_id = match env.rpc_node().execution_outcome(tx_hash).outcome.status {
        ExecutionStatus::SuccessReceiptId(id) => id,
        ref status => panic!("unexpected tx outcome status: {status:?}"),
    };

    // While the deploy still sits in the queue, a follow-up FunctionCall
    // on the same account fails because the account has no contract yet.
    // The design doc accepts this race: per-account ordering of follow-up
    // calls is deferred to a future PoC.
    let pre_call_tx =
        env.rpc_node().tx_call(&user, &user, "main", vec![], Balance::ZERO, Gas::from_teragas(30));
    let pre_call_tx_hash = pre_call_tx.get_hash();
    env.rpc_node().submit_tx(pre_call_tx);
    env.rpc_runner().run_until_outcome_available(pre_call_tx_hash, Duration::seconds(5));
    let pre_call_receipt_id =
        match env.rpc_node().execution_outcome(pre_call_tx_hash).outcome.status {
            ExecutionStatus::SuccessReceiptId(id) => id,
            ref status => panic!("unexpected pre-advance call tx outcome status: {status:?}"),
        };
    env.rpc_runner().run_until_outcome_available(pre_call_receipt_id, Duration::seconds(5));
    let pre_call_outcome = env.rpc_node().execution_outcome(pre_call_receipt_id);
    assert!(
        matches!(pre_call_outcome.outcome.status, ExecutionStatus::Failure(_)),
        "function call before advancement must fail (deploy still queued, no contract on account), got: {:?}",
        pre_call_outcome.outcome.status,
    );
    assert_eq!(
        read_queue(&env, &user).len(),
        1,
        "deploy entry must still be queued while pre-advance call was failing",
    );

    set_compiled_indices_override(&mut env, CompiledIndicesOverride::Force(vec![index]));
    let target = env.rpc_node().head().height + 1;
    run_until_height(&mut env, target);
    set_compiled_indices_override(&mut env, CompiledIndicesOverride::Off);
    let target = env.rpc_node().head().height + 1;
    run_until_height(&mut env, target);

    assert!(read_queue(&env, &user).is_empty(), "advancement should empty the queue");

    env.rpc_runner().run_until_outcome_available(receipt_id, Duration::seconds(2));
    let receipt_outcome = env.rpc_node().execution_outcome(receipt_id);
    assert!(
        matches!(
            receipt_outcome.outcome.status,
            ExecutionStatus::SuccessValue(_) | ExecutionStatus::SuccessReceiptId(_)
        ),
        "advanced deploy receipt should succeed, got: {:?}",
        receipt_outcome.outcome.status,
    );

    // Post-advancement: account state shows the deployed code.
    let account_after = env.rpc_node().view_account_query(&user).unwrap();
    assert_eq!(
        account_after.code_hash, code_hash,
        "account code_hash must match the deployed contract after advancement",
    );

    // Follow-up FunctionCall on the deployed contract must succeed.
    let call_tx =
        env.rpc_node().tx_call(&user, &user, "main", vec![], Balance::ZERO, Gas::from_teragas(30));
    let call_tx_hash = call_tx.get_hash();
    env.rpc_node().submit_tx(call_tx);
    env.rpc_runner().run_until_outcome_available(call_tx_hash, Duration::seconds(5));
    let call_receipt_id = match env.rpc_node().execution_outcome(call_tx_hash).outcome.status {
        ExecutionStatus::SuccessReceiptId(id) => id,
        ref status => panic!("unexpected function-call tx outcome status: {status:?}"),
    };
    env.rpc_runner().run_until_outcome_available(call_receipt_id, Duration::seconds(5));
    let call_receipt_outcome = env.rpc_node().execution_outcome(call_receipt_id);
    assert!(
        matches!(
            call_receipt_outcome.outcome.status,
            ExecutionStatus::SuccessValue(_) | ExecutionStatus::SuccessReceiptId(_)
        ),
        "function call on deployed contract must succeed, got: {:?}",
        call_receipt_outcome.outcome.status,
    );
}

#[cfg(feature = "test_features")]
#[test]
fn test_pending_compile_queue_multi_deploy_atomicity() {
    use near_client::CompiledIndicesOverride;

    init_test_logger();
    let user: AccountId = "user".parse().unwrap();
    let mut env = TestLoopBuilder::new()
        .add_user_account(&user, Balance::from_near(100))
        .enable_rpc()
        .build();
    assert!(ProtocolFeature::CompileQueueDeferral.enabled(PROTOCOL_VERSION));

    let code_a = unique_contract(101);
    let code_b = unique_contract(102);
    let hash_a = near_primitives::hash::CryptoHash::hash_bytes(&code_a);
    let hash_b = near_primitives::hash::CryptoHash::hash_bytes(&code_b);

    let tx = env.rpc_node().tx_from_actions(
        &user,
        &user,
        vec![
            Action::DeployContract(DeployContractAction { code: code_a }),
            Action::DeployContract(DeployContractAction { code: code_b.clone() }),
        ],
    );
    let tx_hash = tx.get_hash();
    env.rpc_node().submit_tx(tx);
    env.rpc_runner().run_until_outcome_available(tx_hash, Duration::seconds(5));

    let queue = read_queue(&env, &user);
    assert_eq!(queue.len(), 1, "multi-deploy receipt must occupy a single queue entry");
    let (index, entry) = queue[0].clone();
    assert_eq!(entry.code_hashes.len(), 2);
    assert!(entry.code_hashes.contains(&hash_a));
    assert!(entry.code_hashes.contains(&hash_b));

    let receipt_id = match env.rpc_node().execution_outcome(tx_hash).outcome.status {
        ExecutionStatus::SuccessReceiptId(id) => id,
        ref status => panic!("unexpected tx outcome status: {status:?}"),
    };

    set_compiled_indices_override(&mut env, CompiledIndicesOverride::Force(vec![index]));
    let target = env.rpc_node().head().height + 1;
    run_until_height(&mut env, target);
    set_compiled_indices_override(&mut env, CompiledIndicesOverride::Off);
    let target = env.rpc_node().head().height + 1;
    run_until_height(&mut env, target);

    assert!(read_queue(&env, &user).is_empty(), "advancement should empty the queue");
    env.rpc_runner().run_until_outcome_available(receipt_id, Duration::seconds(2));
    let receipt_outcome = env.rpc_node().execution_outcome(receipt_id);
    assert!(
        matches!(
            receipt_outcome.outcome.status,
            ExecutionStatus::SuccessValue(_) | ExecutionStatus::SuccessReceiptId(_)
        ),
        "multi-deploy receipt should succeed atomically, got: {:?}",
        receipt_outcome.outcome.status,
    );
}

#[cfg(feature = "test_features")]
#[test]
fn test_pending_compile_queue_out_of_order_advancement() {
    use near_client::CompiledIndicesOverride;

    init_test_logger();
    let user: AccountId = "user".parse().unwrap();
    let mut env = TestLoopBuilder::new()
        .add_user_account(&user, Balance::from_near(100))
        .enable_rpc()
        .build();
    assert!(ProtocolFeature::CompileQueueDeferral.enabled(PROTOCOL_VERSION));

    let tx_a = deploy_tx(&env, &user, unique_contract(201));
    let tx_b = deploy_tx(&env, &user, unique_contract(202));
    let tx_a_hash = tx_a.get_hash();
    let tx_b_hash = tx_b.get_hash();
    env.rpc_node().submit_tx(tx_a);
    env.rpc_node().submit_tx(tx_b);
    env.rpc_runner().run_until_outcome_available(tx_a_hash, Duration::seconds(5));
    env.rpc_runner().run_until_outcome_available(tx_b_hash, Duration::seconds(5));

    let queue = read_queue(&env, &user);
    assert_eq!(queue.len(), 2, "two deploys must be admitted");
    let (index_a, _) = queue[0].clone();
    let (index_b, _) = queue[1].clone();
    assert!(index_a < index_b, "FIFO admission");

    set_compiled_indices_override(&mut env, CompiledIndicesOverride::Force(vec![index_b]));
    let target = env.rpc_node().head().height + 1;
    run_until_height(&mut env, target);
    set_compiled_indices_override(&mut env, CompiledIndicesOverride::Off);
    let target = env.rpc_node().head().height + 1;
    run_until_height(&mut env, target);

    let queue = read_queue(&env, &user);
    assert_eq!(queue.len(), 1, "only B should have advanced");
    assert_eq!(queue[0].0, index_a, "A is still queued");

    set_compiled_indices_override(&mut env, CompiledIndicesOverride::Force(vec![index_a]));
    let target = env.rpc_node().head().height + 1;
    run_until_height(&mut env, target);
    set_compiled_indices_override(&mut env, CompiledIndicesOverride::Off);
    let target = env.rpc_node().head().height + 1;
    run_until_height(&mut env, target);

    assert!(read_queue(&env, &user).is_empty(), "queue empty after advancing A");
}

/// Drive a single test where the chunk producer signals an invalid
/// `compiled_indices` and verify that the queue entry it tried to remove
/// is still in the queue afterward. Once the producer commits to the
/// invalid chunk the chain may stall, so we use `run_for` (which won't
/// panic on a stuck chain) and then assert on queue state.
#[cfg(feature = "test_features")]
fn invalid_compiled_indices_variant(force_indices: Vec<u64>) {
    use near_client::CompiledIndicesOverride;

    init_test_logger();
    let user: AccountId = "user".parse().unwrap();
    let mut env = TestLoopBuilder::new()
        .add_user_account(&user, Balance::from_near(100))
        .enable_rpc()
        .build();
    assert!(ProtocolFeature::CompileQueueDeferral.enabled(PROTOCOL_VERSION));

    let tx = env.rpc_node().tx_deploy_test_contract(&user);
    let tx_hash = tx.get_hash();
    env.rpc_node().submit_tx(tx);
    env.rpc_runner().run_until_outcome_available(tx_hash, Duration::seconds(5));

    let queue_before = read_queue(&env, &user);
    assert_eq!(queue_before.len(), 1, "deploy must be admitted to the queue");

    set_compiled_indices_override(&mut env, CompiledIndicesOverride::Force(force_indices));
    env.test_loop.run_for(Duration::seconds(2));

    let queue_after = read_queue(&env, &user);
    assert_eq!(
        queue_after.len(),
        1,
        "queue entry must persist when chunk apply rejects bogus compiled_indices",
    );
    assert_eq!(queue_after[0].0, queue_before[0].0);

    // Restore Off so the test loop's drop check (no pending events) is happy
    // and any final shutdown bookkeeping works.
    set_compiled_indices_override(&mut env, CompiledIndicesOverride::Off);
    env.test_loop.run_for(Duration::seconds(1));
}

/// Cross-shard admission: a deploy receipt that arrives via the
/// incoming-receipts path on its destination shard must enter that
/// shard's queue, not the source shard's. We don't drive advancement
/// here because the single-validator setup produces both shards' chunks
/// from one `compiled_indices` override, which would force-advance an
/// invalid index on the wrong shard. Advancement on the destination
/// shard is exercised separately by `..._advancement_happy_path`.
#[test]
fn test_pending_compile_queue_cross_shard_admission() {
    use near_primitives::shard_layout::ShardLayout;
    init_test_logger();
    // Layout splits at "near": accounts before "near" go to shard 0,
    // accounts at/after "near" go to shard 1. "alice" -> shard 0,
    // "zorro" -> shard 1.
    let layout = ShardLayout::multi_shard_custom(vec!["near".parse().unwrap()], 1);
    let sender: AccountId = "alice".parse().unwrap();
    let receiver: AccountId = "zorro".parse().unwrap();
    assert_ne!(
        layout.account_id_to_shard_id(&sender),
        layout.account_id_to_shard_id(&receiver),
        "test layout must place sender and receiver on different shards",
    );

    let mut env = TestLoopBuilder::new()
        .shard_layout(layout)
        .add_user_account(&sender, Balance::from_near(100))
        .add_user_account(&receiver, Balance::from_near(100))
        .enable_rpc()
        .build();
    assert!(ProtocolFeature::CompileQueueDeferral.enabled(PROTOCOL_VERSION));

    // Tx from `sender` targets `receiver` and carries a DeployContract
    // action. The action receipt is generated on `sender`'s shard and
    // forwarded to `receiver`'s shard, where admission should fire.
    let code = unique_contract(301);
    let tx = env.rpc_node().tx_from_actions(
        &sender,
        &receiver,
        vec![Action::DeployContract(DeployContractAction { code })],
    );
    let tx_hash = tx.get_hash();
    env.rpc_node().submit_tx(tx);
    env.rpc_runner().run_until_outcome_available(tx_hash, Duration::seconds(5));
    // One more block so the cross-shard receipt has time to land on the
    // destination shard's incoming-receipts queue.
    let target = env.rpc_node().head().height + 2;
    run_until_height(&mut env, target);

    let sender_queue = read_queue(&env, &sender);
    let receiver_queue = read_queue(&env, &receiver);
    assert!(sender_queue.is_empty(), "sender shard must NOT queue the deploy receipt");
    assert_eq!(
        receiver_queue.len(),
        1,
        "destination shard must queue the deploy receipt admitted via incoming-receipts path",
    );
}

#[cfg(feature = "test_features")]
#[test]
fn test_pending_compile_queue_invalid_compiled_indices_out_of_range() {
    invalid_compiled_indices_variant(vec![1_000_000]);
}

#[cfg(feature = "test_features")]
#[test]
fn test_pending_compile_queue_invalid_compiled_indices_duplicate() {
    invalid_compiled_indices_variant(vec![0, 0]);
}
