use crate::setup::builder::TestLoopBuilder;
use crate::utils::account::create_account_id;
use near_async::time::Duration;
use near_o11y::testonly::init_test_logger;
use near_primitives::types::Balance;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

/// Demonstrates that slow contract compilation causes stale chunks on the
/// affected shard. Simulates a 3-second compilation delay using the test-loop
/// artificial delay mechanism.
#[cfg_attr(not(feature = "test_features"), ignore)]
#[test]
fn test_slow_compilation_causes_stale_chunks() {
    init_test_logger();

    let user = create_account_id("user");

    let num_block_and_chunk_producers = 2;
    let num_chunk_validators_only = 2;

    // Flag to enable slow compilation only after baseline is established.
    let slow_mode = Arc::new(AtomicBool::new(false));
    let slow_mode_clone = slow_mode.clone();

    let mut env = TestLoopBuilder::new()
        .validators(num_block_and_chunk_producers, num_chunk_validators_only)
        .num_shards(2)
        .enable_rpc()
        .add_user_account(&user, Balance::from_near(100))
        .async_computation_delay(move |name| {
            if slow_mode_clone.load(Ordering::Relaxed) {
                match name {
                    "apply_chunks" | "stateless_validation" | "precompile_deployed_contracts" => {
                        Duration::seconds(3)
                    }
                    _ => Duration::milliseconds(80),
                }
            } else {
                Duration::milliseconds(80)
            }
        })
        .build();

    // Run a baseline of healthy blocks.
    env.rpc_runner().run_for_number_of_blocks(5);
    let baseline_height = env.rpc_node().head().height;

    // Enable slow compilation and deploy a contract.
    slow_mode.store(true, Ordering::Relaxed);
    let deploy_tx = env.rpc_node().tx_deploy_test_contract(&user);
    env.rpc_node().client().process_tx(deploy_tx, false, false);

    // Run for enough virtual time to cover the deploy + several blocks of recovery.
    // With 3s delay per apply_chunks, we need ~30s for 10 blocks.
    env.rpc_runner().run_for_number_of_blocks(15);
    let end_height = env.rpc_node().head().height;

    // Inspect blocks for stale chunks.
    let mut stale_chunks_found = false;
    for height in baseline_height..=end_height {
        let block_hash =
            match env.rpc_node().client().chain.chain_store().get_block_hash_by_height(height) {
                Ok(hash) => hash,
                Err(_) => continue,
            };
        let block = env.rpc_node().block(block_hash);
        for chunk_header in block.chunks().iter_raw() {
            let height_included = chunk_header.height_included();
            if height_included < height {
                let lag = height - height_included;
                tracing::info!(
                    height,
                    shard_id = %chunk_header.shard_id(),
                    height_included,
                    lag,
                    "stale chunk"
                );
                stale_chunks_found = true;
            }
        }
    }

    assert!(stale_chunks_found, "expected at least one stale chunk due to slow compilation delay");
}
