use crate::setup::builder::TestLoopBuilder;
use crate::utils::account::create_account_id;
use near_async::time::Duration;
use near_o11y::testonly::init_test_logger;
use near_primitives::types::Balance;
use std::sync::Arc;
use std::sync::atomic::{AtomicI32, Ordering};

/// Demonstrates that slow contract compilation causes missing chunks on the
/// affected shard.
///
/// Uses atomic counters to simulate a one-shot 3-second compilation delay:
/// when the signal is set, each node's apply_chunks and stateless_validation
/// get one slow invocation (simulating a cache-miss compile), then subsequent
/// invocations return to normal (simulating a cache hit).
#[cfg_attr(not(feature = "test_features"), ignore)]
#[test]
fn test_slow_compilation_causes_stale_chunks() {
    init_test_logger();

    let user = create_account_id("user");

    let num_block_and_chunk_producers = 2;
    let num_chunk_validators_only = 2;
    let total_nodes = num_block_and_chunk_producers + num_chunk_validators_only + 1; // +1 for rpc

    // Counters for one-shot delays. When > 0, the next invocation gets a 3s
    // delay and decrements the counter. When 0, normal 80ms delay.
    // Start at 0 (no delay). We'll set them right before deploying.
    let slow_apply = Arc::new(AtomicI32::new(0));
    let slow_validation = Arc::new(AtomicI32::new(0));
    let slow_precompile = Arc::new(AtomicI32::new(0));

    let slow_apply_clone = slow_apply.clone();
    let slow_validation_clone = slow_validation.clone();
    let slow_precompile_clone = slow_precompile.clone();

    let mut env = TestLoopBuilder::new()
        .validators(num_block_and_chunk_producers, num_chunk_validators_only)
        .enable_rpc()
        .add_user_account(&user, Balance::from_near(100))
        .async_computation_delay(move |name| {
            let counter = match name {
                "apply_chunks" => &slow_apply_clone,
                "stateless_validation" => &slow_validation_clone,
                "precompile_deployed_contracts" => &slow_precompile_clone,
                _ => return Duration::milliseconds(80),
            };
            // Try to decrement. If we were > 0, this invocation gets the big delay.
            let prev = counter.fetch_sub(1, Ordering::Relaxed);
            if prev > 0 {
                Duration::seconds(3)
            } else {
                // We went below 0, restore to 0 and return normal delay.
                counter.fetch_max(0, Ordering::Relaxed);
                Duration::milliseconds(80)
            }
        })
        .build();

    // Run a baseline of healthy blocks.
    env.rpc_runner().run_for_number_of_blocks(5);
    let baseline_height = env.rpc_node().head().height;
    tracing::warn!(baseline_height, "baseline established");

    // Arm the one-shot delays: each node gets one slow apply_chunks,
    // one slow stateless_validation, and one slow precompile.
    slow_apply.store(total_nodes as i32, Ordering::Relaxed);
    slow_validation.store(total_nodes as i32, Ordering::Relaxed);
    slow_precompile.store(total_nodes as i32, Ordering::Relaxed);

    // Deploy a contract — this will trigger compilation on all nodes.
    let deploy_tx = env.rpc_node().tx_deploy_test_contract(&user);
    env.rpc_runner().run_tx(deploy_tx, Duration::seconds(30));

    // Run for more blocks to see recovery.
    env.rpc_runner().run_for_number_of_blocks(5);
    let end_height = env.rpc_node().head().height;

    // Inspect blocks for missing chunks.
    let mut missing_count = 0;
    tracing::warn!("block-by-block chunk inclusion:");
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
                tracing::warn!(
                    height,
                    shard_id = %chunk_header.shard_id(),
                    height_included,
                    lag,
                    "missing chunk"
                );
                missing_count += 1;
            }
        }
    }

    tracing::warn!(missing_count, "total missing chunks found");
    assert!(missing_count > 0, "expected at least one missing chunk due to slow compilation delay");
}
