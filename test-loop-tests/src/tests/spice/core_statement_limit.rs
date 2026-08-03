use crate::setup::builder::TestLoopBuilder;
use crate::utils::account::create_validators_spec;
use near_async::time::Duration;
use near_chain::spice::core::{MAX_REFERENCED_CHUNKS_PER_BLOCK, get_last_certified_block_header};
use near_o11y::testonly::init_test_logger;
use near_primitives::types::BlockHeight;
use std::collections::HashSet;

/// Holds every endorsement back until more chunks are awaiting certification than a single block may
/// reference, then releases them all at once. The producer then holds endorsements for more chunks
/// than it can put in one block, so the limit binds for several blocks in a row, and the chain has
/// to keep certifying until it has caught up.
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn slow_test_spice_core_statement_limit_binds_and_chain_catches_up() {
    init_test_logger();

    let num_shards = 8;
    let validators_spec = create_validators_spec(2, 0);
    let epoch_length = 200;

    let mut env = TestLoopBuilder::new()
        .epoch_length(epoch_length)
        .num_shards(num_shards)
        .validators_spec(validators_spec)
        // Stretch the block interval: the endorsements released by the flood are processed by the
        // core writer at a bounded rate, and the producer can only reference chunks it has already
        // learned about by the time it builds the next block.
        .config_modifier(|config, _| {
            let block_prod_time = Duration::milliseconds(4000);
            config.min_block_production_delay.update(block_prod_time);
            config.max_block_production_delay.update(3 * block_prod_time);
            config.max_block_wait_delay.update(3 * block_prod_time);
        })
        .delay_warmup()
        .build();
    env.delay_endorsements_propagation(100_000);
    let mut env = env.warmup();

    let target_backlog = MAX_REFERENCED_CHUNKS_PER_BLOCK * 2;
    env.node_runner(0).run_until(
        |node| {
            let head = node.head();
            node.client()
                .chain
                .spice_core_reader
                .get_uncertified_chunks(&head.last_block_hash)
                .is_ok_and(|chunks| chunks.len() >= target_backlog)
        },
        Duration::seconds(600),
    );

    let stalled_head = env.node(0).head();
    let backlog = env
        .node(0)
        .client()
        .chain
        .spice_core_reader
        .get_uncertified_chunks(&stalled_head.last_block_hash)
        .unwrap()
        .len();
    assert!(
        backlog > MAX_REFERENCED_CHUNKS_PER_BLOCK,
        "expected a backlog past the limit, got {backlog}"
    );

    env.delay_endorsements_propagation(0);
    env.node_runner(0).run_until(
        |node| {
            let head = node.head();
            let last_certified = get_last_certified_block_header(
                &node.client().chain.chain_store,
                &head.last_block_hash,
            )
            .expect("last certified block should be queryable");
            last_certified.height() >= stalled_head.height
        },
        Duration::seconds(120),
    );

    let head = env.node(0).head();
    let mut max_referenced = 0;
    let mut block_hash = head.last_block_hash;
    loop {
        let block = env
            .node(0)
            .client()
            .chain
            .get_block(&block_hash)
            .expect("every block from the stall onwards should still be in store");
        let referenced: HashSet<_> =
            block.spice_core_statements().iter().map(|statement| statement.chunk_id()).collect();
        max_referenced = max_referenced.max(referenced.len());
        if block.header().height() <= stalled_head.height {
            break;
        }
        block_hash = *block.header().prev_hash();
    }
    assert_eq!(
        max_referenced, MAX_REFERENCED_CHUNKS_PER_BLOCK,
        "no block referenced the full budget, so the limit never bound"
    );

    // And the chain kept producing blocks throughout, rather than wedging on the backlog.
    let final_height: BlockHeight = env.node(0).head().height;
    assert!(
        final_height > stalled_head.height,
        "chain stopped producing blocks: {final_height} <= {}",
        stalled_head.height
    );
}
