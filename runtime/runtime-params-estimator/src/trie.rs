use crate::estimator_context::{EstimatorContext, Testbed};
use crate::gas_cost::{GasCost, NonNegativeTolerance};
use crate::utils::{aggregate_per_block_measurements, overhead_per_measured_block, percentiles};
use near_primitives::hash::hash;
use near_primitives::types::TrieCacheMode;
use near_store::{TrieCachingStorage, TrieStorage};
use near_vm_runner::logic::ExtCosts;
use std::sync::atomic::{AtomicUsize, Ordering};

static SINK: AtomicUsize = AtomicUsize::new(0);

pub(crate) fn write_node(
    ctx: &mut EstimatorContext,
    warmup_iters: usize,
    measured_iters: usize,
    final_key_len: usize,
) -> GasCost {
    let block_latency = 0;
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
    let results = &testbed.measure_blocks(blocks, block_latency)[1..];
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
    // The exact number of touched nodes is a implementation that we don't want
    // to test here but it should be close to 2*final_key_len
    assert!(nodes_touched_delta as usize <= 2 * final_key_len + 10);
    assert!(nodes_touched_delta as usize >= 2 * final_key_len - 10);
    let cost_delta =
        cost_long_key.saturating_sub(&cost_short_key, &NonNegativeTolerance::PER_MILLE);
    let cost = cost_delta / nodes_touched_delta;
    cost
}

pub(crate) fn read_node_from_chunk_cache(testbed: &mut Testbed) -> GasCost {
    let debug = testbed.config.debug;
    let iters = 200;
    let percentiles_of_interest = &[0.5, 0.9, 0.99, 0.999];

    // Worst-case
    // - L3 CPU cache is filled with dummy data before measuring
    let spoil_l3 = true;
    // - Completely cold cache
    let warmups = 0;
    // - Single node read, no amortization possible
    let num_values = 1;
    // - Data is spread in main memory
    let data_spread = 7;

    let mut estimation = |debug_name: &'static str,
                          iters: usize,
                          num_values: usize,
                          num_warmup_values: usize,
                          data_spread_factor: usize,
                          spoil_l3: bool| {
        let results = read_node_from_chunk_cache_ext(
            testbed,
            iters,
            num_values,
            num_warmup_values,
            data_spread_factor,
            spoil_l3,
        );
        let p_results = percentiles(results, percentiles_of_interest).collect::<Vec<_>>();
        if debug {
            eprint!("{:<32}", debug_name);
            for cost in p_results.iter() {
                eprint!("{:>8} ", cost.to_gas() / 1_000_000);
            }
            eprintln!();
        }
        p_results
    };

    // Print header of debug table
    if debug {
        eprintln!(
            "{:<32}{:>8.3} {:>8.3} {:>8.3} {:>8.3}",
            "",
            percentiles_of_interest[0],
            percentiles_of_interest[1],
            percentiles_of_interest[2],
            percentiles_of_interest[3]
        );
    }

    // For the base case, worst-case assumption is slightly relaxed. The base at
    // the 90th percentile case is used as final estimation.
    let base_case = {
        // Reading a different node before the measurement loads data structure
        // into cache. It would be difficult for an attacker to avoid this
        // consistently, so the base case assumes this is in cache.
        let warmups = 1;
        // Some amortization should also be allowed, or how would an attacker
        // actually abuse undercharged costs?
        let num_values = 16;

        let mut p_results =
            estimation("Base Case", iters, num_values, warmups, data_spread, spoil_l3);
        // Take the 90th percentile measured.
        p_results.swap_remove(1)
    };

    // If debug output is enable, run the same estimation using different
    // assumptions and print a table of results.
    if debug {
        // Base case with better data locality.
        {
            let warmups = 1;
            let num_values = 16;
            let data_spread = 1;
            estimation("Base Case w locality", iters, num_values, warmups, data_spread, spoil_l3);
        }
        // Worst-case: All parameters as explained above.
        {
            estimation("Worst Case", iters, num_values, warmups, data_spread, spoil_l3);
        }
        // Worst-case, but one warm up value to load data structures and code
        // into cache.
        {
            let warmups = 1;
            estimation("Warmed-up", iters, num_values, warmups, data_spread, spoil_l3);
        }
        // Worst-case, but amortized costs over several values, allowing
        // hardware level optimizations to kick in.
        {
            let num_values = 128;
            let iters = 30; // For estimation speed only, should not affect results
            estimation("Amortized", iters, num_values, warmups, data_spread, spoil_l3);
        }
        // Almost-best-case, only the L3 is still overwritten between
        // iterations.
        {
            let warmups = 1;
            let num_values = 128;
            let iters = 30; // For estimation speed only, should not affect results
            let data_spread = 1;
            estimation("Best Case(from memory)", iters, num_values, warmups, data_spread, spoil_l3);
        }
        // Best-case: Nothing attempted to worsen memory latency, just iterate
        // over measurements and allow the hardware to do all optimizations it
        // can.
        {
            let spoil_l3 = false;
            let warmups = 1;
            let num_values = 128;
            let iters = 30; // For estimation speed only, should not affect results
            let data_spread_factor = 1;
            estimation("Best Case", iters, num_values, warmups, data_spread_factor, spoil_l3);
        }
    }

    base_case
}

fn read_node_from_chunk_cache_ext(
    testbed: &mut Testbed,
    iters: usize,
    // How many values are read after each other. The higher the number, the
    // larger the amortization effect is.
    num_values: usize,
    // Values to read before measurement, to warm up CPU caches with data
    // structures used in looking up trie nodes
    num_warmup_values: usize,
    // Spread factor to reduce data locality
    data_spread_factor: usize,
    // Before measuring, completely overwrite L3 CPU cache content
    spoil_l3: bool,
) -> Vec<GasCost> {
    // Trie nodes are largest when they hold part of a storage key (Extension or
    // Leaf) and keys can be up to 2kiB. Therefore, this is about the maximum
    // size possible.
    let value_len: usize = 2048;

    // Prepare a data buffer that we can read again later to overwrite CPU caches
    let assumed_max_l3_size = 128 * 1024 * 1024;
    let dummy_data = crate::utils::random_vec(assumed_max_l3_size);

    (0..iters)
        .map(|i| {
            // Setup:
            // Insert a number of worst-case trie nodes *somehow*. It really
            // doesn't matter how we do it. But it matters that they are
            // extension nodes with the desired key length.
            // The easiest way to insert such a node is by encoding it manually
            // and inserting it as a value. This means it's not even part of the
            // actual trie, but it looks like a trie node and can be accessed by
            // hash. Thus, it is sufficient for estimating the cost to read end
            // decode such a worst-case node.
            let tb = testbed.transaction_builder();
            let signer = tb.random_account();
            let values_inserted = num_values * data_spread_factor;
            let values: Vec<_> = (0..values_inserted)
                .map(|_| {
                    let extention_key = crate::utils::random_vec(value_len);
                    near_store::estimator::encode_extension_node(extention_key)
                })
                .collect();
            let mut setup_block = Vec::new();
            for (j, value) in values.iter().cloned().enumerate() {
                let key = j.to_le_bytes().to_vec();
                setup_block.push(tb.account_insert_key(signer.clone(), &key, &value));
            }
            testbed.process_block(setup_block, 0);

            // Collect keys of the inserted nodes and select a subset for testing.
            let all_value_hashes: Vec<_> = values.iter().map(|value| hash(value)).collect();
            let measured_value_hashes: Vec<_> =
                all_value_hashes.iter().step_by(data_spread_factor).cloned().collect();
            let unmeasured_value_hashes = &all_value_hashes[0..num_warmup_values];
            assert_eq!(measured_value_hashes.len(), num_values);

            // Create a new cache and load nodes into it as preparation.
            let caching_storage = testbed.trie_caching_storage();
            caching_storage.set_mode(TrieCacheMode::CachingChunk);
            let _dummy_sum = read_raw_nodes_from_storage(&caching_storage, &all_value_hashes);

            // Remove trie nodes from CPU caches by filling the caches with useless data.
            // (To measure latency from main memory, not CPU caches)
            if spoil_l3 {
                let dummy_count = dummy_data.iter().filter(|n| **n == i as u8).count();
                SINK.fetch_add(dummy_count, Ordering::SeqCst);
            }

            // Read some nodes from the cache, to warm up caches again. (We only
            // want the trie node to come from main memory, the data structures
            // around that are expected to always be in cache)
            let dummy_sum = read_raw_nodes_from_storage(&caching_storage, unmeasured_value_hashes);
            SINK.fetch_add(dummy_sum, Ordering::SeqCst);

            let start = GasCost::measure(testbed.config.metric);
            let dummy_sum = read_raw_nodes_from_storage(&caching_storage, &measured_value_hashes);
            let cost = start.elapsed();
            SINK.fetch_add(dummy_sum, Ordering::SeqCst);

            cost / num_values as u64
        })
        .collect()
}

/// Read trie nodes directly from a `TrieCachingStorage`, without the runtime.
/// Keys are hashes of the nodes.
/// The return value is just a value to ensure nothing gets optimized out by the
/// compiler.
fn read_raw_nodes_from_storage(
    caching_storage: &TrieCachingStorage,
    keys: &[near_primitives::hash::CryptoHash],
) -> usize {
    keys.iter()
        .map(|key| {
            let bytes = caching_storage.retrieve_raw_bytes(key).unwrap();
            near_store::estimator::decode_extension_node(&bytes).len()
        })
        .sum()
}
