use itertools::Itertools;
use near_async::test_loop::data::TestLoopData;
use near_async::time::Duration;
use near_chain::Error;
use near_chain::types::Tip;
use near_client::metrics::{
    PREPARE_TRANSACTIONS_JOB_ERROR_TOTAL, PREPARE_TRANSACTIONS_JOB_RESULT_NOT_FOUND_TOTAL,
    PREPARE_TRANSACTIONS_JOB_RESULT_USED_TOTAL, PREPARE_TRANSACTIONS_JOB_STARTED_TOTAL,
};
use near_o11y::metrics::IntCounterVec;
use near_primitives::block::ChunkType;
use std::collections::HashSet;
use std::sync::{Arc, LazyLock};

use near_async::messaging::CanSend;
use near_async::test_loop::TestLoopV2;
use near_chain_configs::test_genesis::ValidatorsSpec;
use near_client::ProcessTxRequest;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::test_utils::create_user_test_signer;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{AccountId, Balance, BlockHeight, BlockHeightDelta};

use crate::setup::builder::TestLoopBuilder;
use crate::setup::drop_condition::DropCondition;
use crate::setup::env::TestLoopEnv;
use crate::setup::state::NodeExecutionData;

/// N block/chunk producer nodes. 1 shard.
fn setup(num_nodes: usize, epoch_length: BlockHeightDelta) -> TestLoopEnv {
    let accounts =
        (0..num_nodes).map(|i| format!("account{i}").parse().unwrap()).collect::<Vec<AccountId>>();
    let account_strs = accounts.iter().map(|acc| acc.as_str()).collect_vec();
    let shard_layout = ShardLayout::single_shard();
    let validators_spec = ValidatorsSpec::desired_roles(&account_strs, &[]);
    let genesis = TestLoopBuilder::new_genesis_builder()
        .epoch_length(epoch_length)
        .shard_layout(shard_layout)
        .validators_spec(validators_spec)
        .add_user_accounts_simple(&accounts, Balance::from_near(10_000))
        .genesis_height(10000)
        .transaction_validity_period(1000)
        .build();
    TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store_from_genesis()
        .clients(accounts)
        .build()
        .warmup()
}

/// Very long epochs, no epoch switches in the test.
const LONG_EPOCH_LENGTH: BlockHeightDelta = 100_000;

/// Submit a few transactions which send 1 mNEAR from account0 to account1
fn submit_transactions(
    node_datas: &[NodeExecutionData],
    base_block_hash: CryptoHash,
    nonce_counter: &mut u64,
) {
    let account0: AccountId = "account0".parse().unwrap();
    let account0_signer = &create_user_test_signer(&account0).into();
    let account1: AccountId = "account1".parse().unwrap();

    for _ in 0..4 {
        let tx = SignedTransaction::send_money(
            *nonce_counter,
            account0.clone(),
            account1.clone(),
            &account0_signer,
            Balance::from_millinear(1),
            base_block_hash,
        );
        *nonce_counter += 1;

        node_datas[0].rpc_handler_sender.send(ProcessTxRequest {
            transaction: tx,
            is_forwarded: false,
            check_only: false,
        });
    }
}

fn get_tip(test_loop_data: &TestLoopData, node_datas: &[NodeExecutionData]) -> Arc<Tip> {
    test_loop_data.get(&node_datas[0].client_sender.actor_handle()).client.chain.head().unwrap()
}

fn run_until_next_height(test_loop: &mut TestLoopV2, node_datas: &[NodeExecutionData]) {
    let initial_height = get_tip(&test_loop.data, node_datas).height;
    test_loop.run_until(
        |tl_data| {
            let cur_height = get_tip(tl_data, node_datas).height;
            cur_height > initial_height
        },
        Duration::seconds(30),
    );
}

#[derive(Default)]
struct ChainTxs {
    txs: Vec<SignedTransaction>,
    num_blocks_with_txs: usize,
    num_missing_chunks: usize,
    num_missing_blocks: u64,
}

fn collect_chain_txs(
    start_height: BlockHeight,
    test_loop: &TestLoopV2,
    node_datas: &[NodeExecutionData],
) -> ChainTxs {
    let mut result: ChainTxs = ChainTxs::default();

    let chain = &test_loop.data.get(&node_datas[0].client_sender.actor_handle()).client.chain;
    let last_height = chain.head().unwrap().height;

    for height in start_height..=last_height {
        let block = match chain.get_block_by_height(height) {
            Ok(block) => block,
            Err(Error::DBNotFoundErr(_)) => {
                result.num_missing_blocks += 1;
                continue;
            }
            Err(e) => panic!("Error reading block: {}", e),
        };

        let mut contains_txs = false;
        for chunk in block.chunks().iter() {
            match chunk {
                ChunkType::Old(_) => {
                    result.num_missing_chunks += 1;
                }
                ChunkType::New(chunk_header) => {
                    let chunk = chain.get_chunk(chunk_header.chunk_hash()).unwrap();
                    for tx in chunk.into_transactions() {
                        result.txs.push(tx);
                        contains_txs = true;
                    }
                }
            }
        }
        if contains_txs {
            result.num_blocks_with_txs += 1;
        }
    }

    result
}

fn assert_no_duplicates(chain_txs: &ChainTxs) {
    let mut observed_txs = HashSet::new();

    for tx in &chain_txs.txs {
        if observed_txs.contains(&tx.get_hash()) {
            panic!("Transaction {:?} occurs twice!", tx);
        }
        observed_txs.insert(tx.hash());
    }
}

fn assert_all_included(chain_txs: &ChainTxs, nonce_counter: u64) {
    let mut observed_nonces = chain_txs.txs.iter().map(|tx| tx.transaction.nonce()).collect_vec();
    observed_nonces.sort();
    for i in 1..nonce_counter {
        if observed_nonces[(i - 1) as usize] != i {
            panic!("Transaction with nonce {} is missing! (all nonces: {:?})", i, observed_nonces);
        }
    }
}

struct MetricTracker {
    initial_value: u64,
    metric: &'static LazyLock<IntCounterVec>,
}

impl MetricTracker {
    pub fn new(metric: &'static LazyLock<IntCounterVec>) -> Self {
        Self { initial_value: Self::raw_metric_val(metric), metric }
    }

    pub fn get(&self) -> u64 {
        let cur_val = Self::raw_metric_val(self.metric);
        cur_val - self.initial_value
    }

    fn raw_metric_val(metric: &'static LazyLock<IntCounterVec>) -> u64 {
        let shard_str = ShardLayout::single_shard().shard_ids().next().unwrap().to_string();
        metric.get_metric_with_label_values(&[&shard_str]).unwrap().get()
    }
}

struct MetricTrackers {
    pub job_started_total: MetricTracker,
    pub job_result_used_total: MetricTracker,
    pub job_result_not_found_total: MetricTracker,
    pub job_error_total: MetricTracker,
}

impl MetricTrackers {
    pub fn new() -> Self {
        MetricTrackers {
            job_started_total: MetricTracker::new(&PREPARE_TRANSACTIONS_JOB_STARTED_TOTAL),
            job_result_used_total: MetricTracker::new(&PREPARE_TRANSACTIONS_JOB_RESULT_USED_TOTAL),
            job_result_not_found_total: MetricTracker::new(
                &PREPARE_TRANSACTIONS_JOB_RESULT_NOT_FOUND_TOTAL,
            ),
            job_error_total: MetricTracker::new(&PREPARE_TRANSACTIONS_JOB_ERROR_TOTAL),
        }
    }
}

/// Test that early transaction preparation works as expected on the happy path
#[test]
fn test_early_prepare_tx_basic() {
    let TestLoopEnv { mut test_loop, node_datas, shared_state } = setup(2, LONG_EPOCH_LENGTH);
    let start_height = get_tip(&test_loop.data, &node_datas).height;
    let metrics = MetricTrackers::new();

    // Run the chain for a few heights, submit some transactions at each height
    let mut nonce_counter = 1;
    for _ in 0..5 {
        let block_hash = get_tip(&test_loop.data, &node_datas).last_block_hash;
        submit_transactions(&node_datas, block_hash, &mut nonce_counter);
        run_until_next_height(&mut test_loop, &node_datas);
    }

    // Run for a few more heights to make sure that all transactions were picked up
    for _ in 0..5 {
        run_until_next_height(&mut test_loop, &node_datas);
    }

    // Collect transactions included in chunks
    let chain_txs = collect_chain_txs(start_height, &test_loop, &node_datas);

    // Make sure that all transactions were included, none were rejected/lost.
    assert_all_included(&chain_txs, nonce_counter);

    // Make sure that no transactions were included twice
    assert_no_duplicates(&chain_txs);

    // There were at least 3 blocks with some transactions inside
    assert!(chain_txs.num_blocks_with_txs > 3);

    // No missing chunks and blocks
    assert_eq!(chain_txs.num_missing_chunks, 0);
    assert_eq!(chain_txs.num_missing_blocks, 0);

    // Metrics have the right values - all jobs were successfully started and used
    assert_eq!(metrics.job_started_total.get(), 10);
    assert_eq!(metrics.job_result_used_total.get(), 10);
    assert_eq!(metrics.job_result_not_found_total.get(), 0);
    assert_eq!(metrics.job_error_total.get(), 0);

    TestLoopEnv { test_loop, node_datas, shared_state }
        .shutdown_and_drain_remaining_events(Duration::seconds(20));
}

/// Test that early transaction preparation works as expected when there is a missing chunk
#[test]
fn test_early_prepare_tx_missing_chunk() {
    let env = setup(2, LONG_EPOCH_LENGTH);
    let start_height = get_tip(&env.test_loop.data, &env.node_datas).height;

    // Drop a chunk 3 heights after the start height
    let start_height_after_epoch = start_height - 10000; // genesis height from setup()
    let shard_id = ShardLayout::single_shard().shard_ids().next().unwrap();
    let mut drop_map = vec![true; 1000];
    drop_map[start_height_after_epoch as usize + 3] = false;
    let env = env.drop(DropCondition::ChunksProducedByHeight(
        std::iter::once((shard_id, drop_map)).collect(),
    ));

    let TestLoopEnv { mut test_loop, node_datas, shared_state } = env;
    let metrics = MetricTrackers::new();

    // Run the chain for a few heights, submit some transactions at each height
    let mut nonce_counter = 1;
    for _ in 0..5 {
        let block_hash = get_tip(&test_loop.data, &node_datas).last_block_hash;
        submit_transactions(&node_datas, block_hash, &mut nonce_counter);
        run_until_next_height(&mut test_loop, &node_datas);
    }

    // Make sure that there was a missing chunk during those 5 blocks
    assert_eq!(collect_chain_txs(start_height, &test_loop, &node_datas).num_missing_chunks, 1);

    // Run for a few more heights to make sure that all transactions were picked up
    for _ in 0..5 {
        run_until_next_height(&mut test_loop, &node_datas);
    }

    // Collect transactions included in chunks
    let chain_txs = collect_chain_txs(start_height, &test_loop, &node_datas);

    // Make sure that all transactions were included, none were rejected/lost.
    assert_all_included(&chain_txs, nonce_counter);

    // Make sure that no transactions were included twice
    assert_no_duplicates(&chain_txs);

    // There were at least 3 blocks with some transactions inside
    assert!(chain_txs.num_blocks_with_txs > 3);

    // No missing chunks and blocks
    assert_eq!(chain_txs.num_missing_chunks, 1);
    assert_eq!(chain_txs.num_missing_blocks, 0);

    // Metrics have the right values
    assert_eq!(metrics.job_started_total.get(), 10);

    // One job was not used because of the missing chunk
    assert_eq!(metrics.job_result_used_total.get(), 9);
    assert_eq!(metrics.job_result_not_found_total.get(), 1);
    assert_eq!(metrics.job_error_total.get(), 0);

    TestLoopEnv { test_loop, node_datas, shared_state }
        .shutdown_and_drain_remaining_events(Duration::seconds(20));
}

/// Test that early transaction preparation works as expected when there is a missing block
#[test]
fn test_early_prepare_tx_missing_block() {
    // `DropCondition::BlocksByHeight` doesn't work well with 2 nodes, the chain gets stuck. With 4 nodes it works fine.
    let env = setup(4, LONG_EPOCH_LENGTH);
    let start_height = get_tip(&env.test_loop.data, &env.node_datas).height;

    // Drop a block 3 heights after the start height
    let env = env.drop(DropCondition::BlocksByHeight(std::iter::once(start_height + 3).collect()));

    let TestLoopEnv { mut test_loop, node_datas, shared_state } = env;
    let metrics = MetricTrackers::new();

    // Run the chain for a few heights, submit some transactions at each height
    let mut nonce_counter = 1;
    for _ in 0..5 {
        let block_hash = get_tip(&test_loop.data, &node_datas).last_block_hash;
        submit_transactions(&node_datas, block_hash, &mut nonce_counter);
        run_until_next_height(&mut test_loop, &node_datas);
    }

    // Make sure that there was a missing block during those 5 blocks
    assert_eq!(collect_chain_txs(start_height, &test_loop, &node_datas).num_missing_blocks, 1);

    // Run for a few more heights to make sure that all transactions were picked up
    for _ in 0..5 {
        run_until_next_height(&mut test_loop, &node_datas);
    }

    // Collect transactions included in chunks
    let chain_txs = collect_chain_txs(start_height, &test_loop, &node_datas);

    // Make sure that all transactions were included, none were rejected/lost.
    assert_all_included(&chain_txs, nonce_counter);

    // Make sure that no transactions were included twice
    assert_no_duplicates(&chain_txs);

    // There were at least 3 blocks with some transactions inside
    assert!(chain_txs.num_blocks_with_txs > 3);

    // No missing chunks and blocks
    assert_eq!(chain_txs.num_missing_chunks, 0);
    assert_eq!(chain_txs.num_missing_blocks, 1);

    // Metrics have the right values
    assert_eq!(metrics.job_started_total.get(), 11);

    // One job was not used because of the missing block
    assert_eq!(metrics.job_result_used_total.get(), 10);
    assert_eq!(metrics.job_result_not_found_total.get(), 0);
    assert_eq!(metrics.job_error_total.get(), 0);

    TestLoopEnv { test_loop, node_datas, shared_state }
        .shutdown_and_drain_remaining_events(Duration::seconds(20));
}

/// Test that early transaction preparation works as expected when there is an epoch switch
#[test]
fn test_early_prepare_tx_epoch_switch() {
    let short_epoch_length = 5;
    let TestLoopEnv { mut test_loop, node_datas, shared_state } = setup(2, short_epoch_length);
    let start_tip = get_tip(&test_loop.data, &node_datas);
    let start_height = start_tip.height;
    let start_epoch = start_tip.epoch_id;
    let metrics = MetricTrackers::new();

    // Run the chain for a few heights, submit some transactions at each height
    let mut nonce_counter = 1;
    for _ in 0..8 {
        let block_hash = get_tip(&test_loop.data, &node_datas).last_block_hash;
        submit_transactions(&node_datas, block_hash, &mut nonce_counter);
        run_until_next_height(&mut test_loop, &node_datas);
    }

    // Make sure that the epoch switch happened while submitting transactions
    let epoch_id = get_tip(&test_loop.data, &node_datas).epoch_id;
    assert_ne!(start_epoch, epoch_id);

    // Run for a few more heights to make sure that all transactions were picked up
    for _ in 0..2 {
        run_until_next_height(&mut test_loop, &node_datas);
    }

    // Collect transactions included in chunks
    let chain_txs = collect_chain_txs(start_height, &test_loop, &node_datas);

    // Make sure that all transactions were included, none were rejected/lost.
    assert_all_included(&chain_txs, nonce_counter);

    // Make sure that no transactions were included twice
    assert_no_duplicates(&chain_txs);

    // There were at least 3 blocks with some transactions inside
    assert!(chain_txs.num_blocks_with_txs > 3);

    // No missing chunks and blocks
    assert_eq!(chain_txs.num_missing_chunks, 0);
    assert_eq!(chain_txs.num_missing_blocks, 0);

    // Metrics have the right values
    // 2 jobs were not used because of the epoch switch
    assert_eq!(metrics.job_started_total.get(), 10);
    assert_eq!(metrics.job_result_used_total.get(), 8);
    assert_eq!(metrics.job_result_not_found_total.get(), 2);
    assert_eq!(metrics.job_error_total.get(), 0);

    TestLoopEnv { test_loop, node_datas, shared_state }
        .shutdown_and_drain_remaining_events(Duration::seconds(20));
}
