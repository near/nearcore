use near_primitives::hash::hash;
use std::collections::HashMap;
use std::iter;

use near_primitives::types::TrieCacheMode;
use near_store::{RawTrieNode, RawTrieNodeWithSize, TrieStorage};
use near_vm_logic::ExtCosts;

use crate::estimator_context::Testbed;
use crate::gas_cost::{GasCost, NonNegativeTolerance};
use crate::utils::aggregate_per_block_measurements;

pub(crate) fn write_node(
    testbed: &mut Testbed,
    warmup_iters: usize,
    measured_iters: usize,
) -> GasCost {
    let tb = testbed.transaction_builder();
    // Number of bytes in the final key. Will create 2x that many nodes.
    // Picked somewhat arbitrarily, balancing estimation time vs accuracy.
    let final_key_len = 1000;
    // Prepare a long chain in the trie
    let signer = tb.random_account();
    let key = std::iter::repeat('j').take(final_key_len).collect::<String>();
    let mut setup_block = Vec::new();
    for key_len in 0..final_key_len {
        let key = &key.as_str()[..key_len];
        let value = "0";
        setup_block.push(tb.account_insert_key(signer.clone(), key, value));
    }
    let mut blocks = Vec::with_capacity(1 + 2 * warmup_iters + 2 * measured_iters);
    blocks.push(setup_block);
    blocks.extend(
        ["1", "2", "3"]
            .iter()
            .cycle()
            .map(|value| vec![tb.account_insert_key(signer.clone(), &key.as_str()[0..1], value)])
            .take(measured_iters + warmup_iters),
    );
    blocks.extend(
        ["1", "2", "3"]
            .iter()
            .cycle()
            .map(|value| vec![tb.account_insert_key(signer.clone(), &key, value)])
            .take(measured_iters + warmup_iters),
    );
    let results = &testbed.measure_blocks(blocks, 0)[1..];
    let (short_key_results, long_key_results) = results.split_at(measured_iters + warmup_iters);
    let (cost_short_key, ext_cost_short_key) = aggregate_per_block_measurements(
        testbed.config,
        1,
        short_key_results[warmup_iters..].to_vec(),
    );
    let (cost_long_key, ext_cost_long_key) = aggregate_per_block_measurements(
        testbed.config,
        1,
        long_key_results[warmup_iters..].to_vec(),
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

pub(crate) fn read_node_from_db(
    testbed: &mut Testbed,
    warmup_iters: usize,
    measured_iters: usize,
) -> GasCost {
    let tb = testbed.transaction_builder();
    // Number of bytes in the final key. Will create 2x that many nodes.
    // Picked somewhat arbitrarily, balancing estimation time vs accuracy.
    let final_key_len = 1000;
    // Prepare a long chain in the trie
    let signer = tb.random_account();
    let key = "j".repeat(final_key_len);
    let mut setup_block = Vec::new();
    for key_len in 0..final_key_len {
        let key = &key.as_str()[..key_len];
        let value = "0";
        setup_block.push(tb.account_insert_key(signer.clone(), key, value));
    }
    let mut blocks = Vec::with_capacity(1 + 2 * warmup_iters + 2 * measured_iters);
    blocks.push(setup_block);
    blocks.extend(
        iter::repeat_with(|| vec![tb.account_has_key(signer.clone(), &key.as_str()[0..1])])
            .take(measured_iters + warmup_iters),
    );
    blocks.extend(
        iter::repeat_with(|| vec![tb.account_has_key(signer.clone(), &key)])
            .take(measured_iters + warmup_iters),
    );
    let results = &testbed.measure_blocks(blocks, 0)[1..];
    let (short_key_results, long_key_results) = results.split_at(measured_iters + warmup_iters);
    let (cost_short_key, ext_cost_short_key) = aggregate_per_block_measurements(
        testbed.config,
        1,
        short_key_results[warmup_iters..].to_vec(),
    );
    let (cost_long_key, ext_cost_long_key) = aggregate_per_block_measurements(
        testbed.config,
        1,
        long_key_results[warmup_iters..].to_vec(),
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

pub(crate) fn read_node_from_chunk_cache(
    testbed: &mut Testbed,
    warmup_iters: usize,
    measured_iters: usize,
    num_values: usize,
) -> GasCost {
    let iters = warmup_iters + measured_iters;
    let results = (0..iters)
        .map(|_| {
            let tb = testbed.transaction_builder();

            let value_len: usize = 2000;
            let signer = tb.random_account();
            let values: Vec<_> = (0..num_values)
                .map(|_| {
                    let v = tb.random_vec(value_len);
                    let h = hash(&v);
                    let node = RawTrieNode::Extension(v, h);
                    let node_with_size = RawTrieNodeWithSize { node, memory_usage: 1 };
                    node_with_size.encode().unwrap()
                })
                .collect();
            let mut setup_block = Vec::new();
            let mut blocks = vec![];
            for (i, value) in values.iter().cloned().enumerate() {
                let key = vec![(i / 256) as u8, (i % 256) as u8];
                setup_block.push(tb.account_insert_key_bytes(signer.clone(), key, value));
            }
            blocks.push(setup_block.clone());

            let value_hashes: Vec<_> = values.iter().map(|value| hash(value)).collect();
            testbed.measure_blocks(blocks, 0);

            let caching_storage = testbed.trie_caching_storage();
            caching_storage.set_mode(TrieCacheMode::CachingChunk);

            let results: Vec<_> = (0..2)
                .map(|_| {
                    let start = GasCost::measure(testbed.config.metric);
                    let _sum: usize = value_hashes
                        .iter()
                        .enumerate()
                        .map(|(_i, key)| {
                            let bytes = caching_storage.retrieve_raw_bytes(key).unwrap();
                            let node = RawTrieNodeWithSize::decode(&bytes).unwrap();
                            match node.node {
                                RawTrieNode::Extension(v, _) => v.len(),
                                _ => {
                                    unreachable!();
                                }
                            }
                        })
                        .sum();
                    // assert_eq!(sum, num_values * value_len);
                    (start.elapsed(), HashMap::new())
                })
                .collect();

            results.iter().for_each(|(cost, _)| {
                eprintln!("cost = {:?}", cost);
            });

            results[results.len() - 1].clone()
        })
        .collect::<Vec<_>>();
    let (cost, _) = aggregate_per_block_measurements(
        &testbed.config,
        num_values,
        results[1 + warmup_iters..].to_vec(),
    );
    cost
}
