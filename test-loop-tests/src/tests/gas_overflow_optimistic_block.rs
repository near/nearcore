//! Regression test for gas overflow handling in the optimistic-block path.
//!
//! Header gas fields are summed across all shards there, before the per-shard
//! cross-check against `prev_chunk_extra` applies. The sum is therefore fallible:
//! on overflow, early transaction preparation is skipped rather than panicking.

#![cfg(feature = "test_features")]

use crate::setup::builder::TestLoopBuilder;
use near_async::time::Duration;
use near_client::NetworkAdversarialMessage;
use near_o11y::testonly::init_test_logger;
use near_primitives::types::BlockHeight;
use std::mem;
use std::panic::{AssertUnwindSafe, catch_unwind};

/// Heights the honest validators must still gain after the forgery starts.
const REQUIRED_PROGRESS: BlockHeight = 5;

/// One chunk producer per shard, so each validator applies only its own shard, as on
/// mainnet. The forged chunks are never endorsed, so honest validators should keep going.
#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_forged_max_gas_chunk_header_does_not_abort_honest_validators() {
    init_test_logger();

    let num_shards = 4;
    let mut env = TestLoopBuilder::new()
        .num_shards(num_shards)
        .chunk_producer_per_shard()
        .epoch_length(100)
        .build();

    // Assert the untracked-shard precondition instead of assuming it.
    let attacker_idx = num_shards - 1;
    let attacker_shards = env.node(attacker_idx).tracked_shards();
    assert!(!attacker_shards.is_empty(), "attacker must produce chunks for some shard");
    let victim_indices: Vec<usize> = (0..num_shards).filter(|idx| *idx != attacker_idx).collect();
    for &victim_idx in &victim_indices {
        let victim_shards = env.node(victim_idx).tracked_shards();
        assert!(!victim_shards.is_empty(), "victim {victim_idx} must apply some shard");
        assert!(
            !victim_shards.iter().any(|shard| attacker_shards.contains(shard)),
            "victim {victim_idx} tracks {victim_shards:?}, \
             which overlaps the attacker's {attacker_shards:?}"
        );
    }

    let start_height = env.node(0).head().height;
    env.node_runner(attacker_idx)
        .send_adversarial_message(NetworkAdversarialMessage::AdvProduceMaxGasChunkHeader(true));

    // Not `#[should_panic]`: `TestLoopEnv::drop` re-enters the loop, so an escaping panic
    // fires again mid-unwind and aborts the whole test binary.
    let result = catch_unwind(AssertUnwindSafe(|| {
        env.test_loop.run_for(Duration::seconds(10));
    }));

    if let Err(payload) = result {
        let message = payload
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| payload.downcast_ref::<&str>().copied())
            .unwrap_or("<non-string panic payload>")
            .to_string();
        mem::forget(env); // dropping it would re-enter the loop, see above
        panic!("a forged Gas::MAX chunk header took down an honest validator: {message}");
    }

    for &victim_idx in &victim_indices {
        let height = env.node(victim_idx).head().height;
        assert!(
            height >= start_height + REQUIRED_PROGRESS,
            "victim {victim_idx} stalled at {height}, started at {start_height}"
        );
    }
}
