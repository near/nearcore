//! Tests regarding gas limit adjustments at runtime as described in:
//!
//! * #11863
//! * https://near.zulipchat.com/#narrow/stream/295302-general/topic/Adjusting.20gas.20limit.20at.20runtime
//!
//! Note that this functionality is **NOT** to be used in production. So far it is intended for test
//! and benchmark environments only. To enable this feature, put `Some(GasLimitAdjustmentConfig)
//! into your `RuntimeConfig`.

use std::cmp::Ordering;

use itertools::Itertools;
use near_async::time::Duration;
use near_chain_configs::test_genesis::TestGenesisBuilder;
use near_client::Client;
use near_o11y::testonly::init_test_logger;
use near_primitives::hash::CryptoHash;
use near_primitives::types::AccountId;

use crate::test_loop::builder::TestLoopBuilder;
use crate::test_loop::env::TestLoopEnv;
use crate::test_loop::utils::transactions::{execute_money_transfers, get_shared_block_hash};
use crate::test_loop::utils::ONE_NEAR;

const NUM_CLIENTS: usize = 4;

/// Without `GasLimitAdjustmentConfig`, the gas limit is expected to remain constant.
#[test]
fn test_by_default_gas_limit_is_constant() {
    init_test_logger();
    let builder = TestLoopBuilder::new();

    let initial_balance = 10000 * ONE_NEAR;
    let accounts =
        (0..100).map(|i| format!("account{}", i).parse().unwrap()).collect::<Vec<AccountId>>();
    let clients = accounts.iter().take(NUM_CLIENTS).cloned().collect_vec();

    // Since we are **not** patching in a `RuntimeConfig` with `Some(GasLimitAdjustmentConfig)`, the
    // adjustment is disabled by default.
    let mut genesis_builder = TestGenesisBuilder::new();
    genesis_builder
        .genesis_time_from_clock(&builder.clock())
        .protocol_version_latest()
        .genesis_height(10000)
        .gas_prices_free()
        .gas_limit_one_petagas()
        .shard_layout_simple_v1(&["account3", "account5", "account7"])
        .transaction_validity_period(1000)
        .epoch_length(10)
        .validators_desired_roles(&clients.iter().map(|t| t.as_str()).collect_vec(), &[])
        .shuffle_shard_assignment_for_chunk_producers(true);
    for account in &accounts {
        genesis_builder.add_user_account_simple(account.clone(), initial_balance);
    }
    let genesis = genesis_builder.build();

    let initial_gas_limit = genesis.config.gas_limit;

    let TestLoopEnv { mut test_loop, datas: node_datas, tempdir } =
        builder.genesis(genesis).clients(clients).build();

    // Generate some load.
    execute_money_transfers(&mut test_loop, &node_datas, &accounts);

    // Make sure the chain progresses for several epochs.
    let client_handle = node_datas[0].client_sender.actor_handle();
    test_loop.run_until(
        |test_loop_data| {
            test_loop_data.get(&client_handle).client.chain.head().unwrap().height > 10050
        },
        Duration::seconds(10),
    );

    let block_hash = get_shared_block_hash(&node_datas, &test_loop);
    let clients = node_datas
        .iter()
        .map(|data| &test_loop.data.get(&data.client_sender.actor_handle()).client)
        .collect_vec();

    // Assert that the gas limit behaves as expected after running for a while under some load.
    // Without specifying a `GasLimitAdjustmentConfig` in `RuntimeConfig`, the gas limit must
    // remain constant.
    for client in clients.iter() {
        assert_gas_limit(client, &block_hash, initial_gas_limit, Ordering::Equal);
    }

    // Give the test a chance to finish off remaining events in the event loop, which can
    // be important for properly shutting down the nodes.
    TestLoopEnv { test_loop, datas: node_datas, tempdir }
        .shutdown_and_drain_remaining_events(Duration::seconds(20));
}

/// Assert's that the client's gas limit for all chunks of a particular block compare as expected
/// to `reference_gas_limit`.
fn assert_gas_limit(
    client: &Client,
    block_hash: &CryptoHash,
    reference_gas_limit: u64,
    cmp: Ordering,
) {
    let block = client.chain.get_block(block_hash).expect("block should exist");
    for chunk_header in block.chunks().iter() {
        let actual_gas_limit = chunk_header.gas_limit();
        match cmp {
            Ordering::Less => assert!(actual_gas_limit < reference_gas_limit),
            Ordering::Equal => assert_eq!(actual_gas_limit, reference_gas_limit),
            Ordering::Greater => assert!(actual_gas_limit > reference_gas_limit),
        }
    }
}
