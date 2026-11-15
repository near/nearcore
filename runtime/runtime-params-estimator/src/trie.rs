use crate::estimator_context::{BlockLatency, EstimatorContext, Testbed};
use crate::gas_cost::{GasCost, NonNegativeTolerance};
use crate::utils::{
    aggregate_per_block_measurements, extra_refund_block_latency, overhead_per_measured_block,
};
use near_parameters::ExtCosts;
use near_primitives::trie_key::TrieKey;
use std::ops::Bound;

pub(crate) fn write_node(
    ctx: &mut EstimatorContext,
    warmup_iters: usize,
    measured_iters: usize,
    final_key_len: usize,
) -> GasCost {
    let block_latency = extra_refund_block_latency();
    let overhead = overhead_per_measured_block(ctx, block_latency);
    let mut testbed = ctx.testbed();
    let tb = testbed.transaction_builder();
    // Prepare a long chain in the trie
    let signer = tb.random_account();
    let key = "j".repeat(final_key_len);
    let mut setup_block = Vec::new();
    for key_len in 0..final_key_len {
        let key = &key.as_bytes()[..key_len];
        let value = b"0";
        setup_block.push(tb.account_insert_key(signer.clone(), key, value));
    }
    let mut blocks = Vec::with_capacity(1 + 2 * warmup_iters + 2 * measured_iters);
    blocks.push(setup_block);
    blocks.extend(
        [b"1", b"2", b"3"]
            .iter()
            .cycle()
            .map(|value| vec![tb.account_insert_key(signer.clone(), &key.as_bytes()[0..1], *value)])
            .take(measured_iters + warmup_iters),
    );
    blocks.extend(
        [b"1", b"2", b"3"]
            .iter()
            .cycle()
            .map(|value| vec![tb.account_insert_key(signer.clone(), key.as_bytes(), *value)])
            .take(measured_iters + warmup_iters),
    );
    let results = &testbed.measure_blocks(blocks, BlockLatency::Uniform(block_latency))[1..];
    let (short_key_results, long_key_results) = results.split_at(measured_iters + warmup_iters);
    let (cost_short_key, ext_cost_short_key) = aggregate_per_block_measurements(
        1,
        short_key_results[warmup_iters..].to_vec(),
        Some(overhead.clone()),
    );
    let (cost_long_key, ext_cost_long_key) = aggregate_per_block_measurements(
        1,
        long_key_results[warmup_iters..].to_vec(),
        Some(overhead),
    );
    let nodes_touched_delta = ext_cost_long_key[&ExtCosts::touching_trie_node]
        - ext_cost_short_key[&ExtCosts::touching_trie_node];
    // The exact number of touched nodes is an implementation that we don't want
    // to test here but it should be close to 2*final_key_len
    assert!(nodes_touched_delta as usize <= 2 * final_key_len + 10);
    assert!(nodes_touched_delta as usize >= 2 * final_key_len - 10);
    let cost_delta =
        cost_long_key.saturating_sub(&cost_short_key, &NonNegativeTolerance::PER_MILLE);
    let cost = cost_delta / nodes_touched_delta;
    cost
}

/// Estimate the cost of reading a trie node by iterating over the trie until
/// the `max_read_bytes` limit is reached.
pub(crate) fn read_trie_node_estimate_by_iteration(
    testbed: &mut Testbed,
    max_read_bytes: usize,
) -> GasCost {
    let start_account =
        TrieKey::Account { account_id: testbed.transaction_builder().random_account() }.to_vec();
    // Use recorder to get number of trie nodes accessed and the total approximate bytes read.
    let trie = testbed.trie().recording_reads_new_recorder();
    if !trie.has_memtries() {
        eprintln!(
            "Cannot estimate trie node read cost, no memtries available. \
             Run with --memtries to enable memtries."
        );
        return GasCost::zero();
    }

    let start = GasCost::measure(testbed.config.metric);
    {
        let locked_trie = trie.lock_for_iter();
        let mut iter = locked_trie.iter().expect("Failed to get iterator");
        iter.seek(Bound::Included(&start_account)).expect("Failed to seek");
        iter.take_while(|_| trie.recorded_storage_size() < max_read_bytes).for_each(|_| {});
    }

    let accessed_nodes = trie.recorded_storage().expect("recorded storage missing").nodes.len();
    let cost = start.elapsed();

    // Round to nanos, using precise rational math with different denominators
    // may overflow. Expected to take several hundreds to few thousands of
    // nanos.
    (cost / accessed_nodes as u64).round_nanos()
}
