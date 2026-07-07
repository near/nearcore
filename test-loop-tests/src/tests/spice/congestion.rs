//! Spice-native test: congestion control and bandwidth scheduling under lagging
//! execution (the defining property of spice). Drives a multi-shard workload with
//! an artificial endorsement-propagation delay so execution provably trails
//! consensus, and asserts backpressure accrues and the bandwidth scheduler runs in
//! that regime. Congestion/bandwidth come from the previous block's executed
//! `ChunkExtra` (see `build_spice_apply_chunk_block_context`).

use crate::setup::builder::TestLoopBuilder;
use near_async::time::Duration;
use near_chain::ChainStoreAccess;
use near_o11y::testonly::init_test_logger;
use near_primitives::bandwidth_scheduler::BandwidthRequests;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::{ShardLayout, ShardUId};
use near_primitives::types::{AccountId, Balance, Gas};
use near_primitives::views::FinalExecutionStatus;

const NUM_ACCOUNTS: usize = 60;

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn slow_test_spice_congestion_and_bandwidth_with_execution_lag() {
    init_test_logger();

    let epoch_length = 50;
    // Delay endorsement propagation so that execution provably trails consensus.
    let execution_delay = 3;

    // 4 shards; the contract (and many senders) live on shard 0.
    let boundary_accounts =
        ["account3", "account5", "account7"].iter().map(|a| a.parse().unwrap()).collect();
    let shard_layout = ShardLayout::multi_shard_custom(boundary_accounts, 1);

    let accounts: Vec<AccountId> =
        (0..NUM_ACCOUNTS).map(|i| format!("account{i}").parse().unwrap()).collect();
    let contract_id = accounts[0].clone();

    // Two producers (which also endorse); no shard-assignment shuffle, which is
    // not yet supported by spice execution certification across epoch boundaries.
    let mut env = TestLoopBuilder::new()
        .epoch_length(epoch_length)
        .shard_layout(shard_layout.clone())
        .validators(2, 0)
        .add_user_accounts(&accounts, Balance::from_near(1_000_000))
        .track_all_shards()
        .delay_warmup()
        .build();
    env.delay_endorsements_propagation(execution_delay);
    let mut env = env.warmup();

    // Deploy the contract on the congested shard.
    let deploy_tx = env.validator().tx_deploy_test_contract(&contract_id);
    env.validator_runner().run_tx(deploy_tx, Duration::seconds(20));

    // Fire a burst of gas-burning calls from every account to the single
    // contract, congesting its shard and generating cross-shard receipts.
    let burn_gas = Gas::from_teragas(250);
    let args = burn_gas.as_gas().to_le_bytes().to_vec();
    let tx_hashes: Vec<CryptoHash> = {
        let node = env.validator();
        accounts
            .iter()
            .map(|sender_id| {
                let tx = node.tx_call(
                    sender_id,
                    &contract_id,
                    "burn_gas_raw",
                    args.clone(),
                    Balance::ZERO,
                    Gas::from_teragas(300),
                );
                node.submit_tx(tx)
            })
            .collect()
    };

    let contract_shard_uid =
        ShardUId::new(shard_layout.version(), shard_layout.account_id_to_shard_id(&contract_id));

    // Drive until every tx is final, recording from the executed ChunkExtras that
    // execution lags consensus, the congested shard accrues backpressure, and the
    // bandwidth scheduler runs. This workload only burns gas, so outgoing buffers
    // stay below `base_bandwidth` and the scheduler emits no non-empty bandwidth
    // *requests* — that is covered by
    // `ultra_slow_test_bandwidth_scheduler_three_shards_random_receipts`.
    let mut max_lag: u64 = 0;
    let mut max_delayed_gas: u128 = 0;
    let mut saw_bandwidth_scheduler_output = false;
    env.validator_runner().run_until(
        |node| {
            let head = node.head();
            let exec = node.last_executed();
            max_lag = max_lag.max(head.height.saturating_sub(exec.height));

            let exec_hash = exec.last_block_hash;
            let chain_store = node.client().chain.chain_store();
            if let Ok(chunk_extra) = chain_store.get_chunk_extra(&exec_hash, &contract_shard_uid) {
                max_delayed_gas =
                    max_delayed_gas.max(chunk_extra.congestion_info().delayed_receipts_gas());
            }
            for shard_id in shard_layout.shard_ids() {
                let shard_uid = ShardUId::new(shard_layout.version(), shard_id);
                if let Ok(chunk_extra) = chain_store.get_chunk_extra(&exec_hash, &shard_uid) {
                    if let Some(BandwidthRequests::V1(_)) = chunk_extra.bandwidth_requests() {
                        saw_bandwidth_scheduler_output = true;
                    }
                }
            }

            // Stop once all txs are final.
            tx_hashes.iter().all(|tx| {
                matches!(
                    node.client().chain.get_partial_transaction_result(tx).map(|o| o.status),
                    Ok(FinalExecutionStatus::SuccessValue(_))
                )
            })
        },
        Duration::seconds(120),
    );

    assert!(max_lag >= 2, "expected execution to lag block production, max_lag={max_lag}");
    assert!(
        max_delayed_gas > 0,
        "expected congestion backpressure to build on the contract shard under execution lag"
    );
    assert!(
        saw_bandwidth_scheduler_output,
        "expected the bandwidth scheduler to run and record requests in execution results under lag"
    );
}
