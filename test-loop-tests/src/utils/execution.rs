use crate::setup::env::TestLoopEnv;
use near_chain::ChainStoreAccess as _;
use near_chain::near_chain_primitives::ApplyChunksMode;
use near_primitives::types::BlockHeight;

/// How far a node's execution head may fall behind its head at the end of a test.
pub(crate) const MAX_EXECUTION_LAG: BlockHeight = 3;

/// Asserts that on every live node the execution head is within `MAX_EXECUTION_LAG` blocks of the
/// head, that every node applied the shards it caught up, and that what it applied was certified.
pub(crate) fn assert_execution_kept_up(env: &TestLoopEnv) {
    for (idx, node_data) in env.node_datas.iter().enumerate() {
        if env.test_loop.is_denylisted(&node_data.identifier) {
            continue;
        }
        let node = env.node(idx);
        let head = node.head().height;
        let executed = node.last_executed().height;
        assert!(
            executed + MAX_EXECUTION_LAG >= head,
            "execution of {} fell behind the head: head {head}, executed {executed}",
            node_data.account_id
        );
    }
    assert_caught_up_shards_applied(env);
    assert_execution_matches_certification(env);
}

/// Asserts that every live node applied each shard it caught up for an epoch in every canonical
/// block of that epoch after the epoch's first block, where spice syncs the shard, for the epochs
/// its execution has moved past and garbage collection has not reached yet, and that there was at
/// least one such shard to check.
fn assert_caught_up_shards_applied(env: &TestLoopEnv) {
    if !cfg!(feature = "protocol_feature_spice") {
        return;
    }
    let mut num_checked = 0;
    for (idx, node_data) in env.node_datas.iter().enumerate() {
        if env.test_loop.is_denylisted(&node_data.identifier) {
            continue;
        }
        let node = env.node(idx);
        let client = node.client();
        let chain_store = client.chain.chain_store();
        let executed = node.last_executed();
        for height in chain_store.tail() + 1..=executed.height {
            let Ok(block_hash) = chain_store.get_block_hash_by_height(height) else {
                continue;
            };
            let header = chain_store.get_block_header(&block_hash).unwrap();
            // The epoch data of a parent at the garbage collection tail can be gone already.
            let Ok(is_epoch_start) =
                client.epoch_manager.is_next_block_epoch_start(header.prev_hash())
            else {
                continue;
            };
            if is_epoch_start || header.epoch_id() == &executed.epoch_id {
                continue;
            }
            let shard_layout = client.epoch_manager.get_shard_layout(header.epoch_id()).unwrap();
            for shard_uid in shard_layout.shard_uids() {
                if !client.shard_tracker.should_apply_chunk(
                    ApplyChunksMode::OnlyCatchupShards,
                    header.prev_hash(),
                    shard_uid.shard_id(),
                ) {
                    continue;
                }
                num_checked += 1;
                assert!(
                    chain_store.get_chunk_extra(&block_hash, &shard_uid).is_ok(),
                    "{} did not apply caught-up shard {} at height {height}",
                    node_data.account_id,
                    shard_uid.shard_id()
                );
            }
        }
    }
    assert!(num_checked > 0, "no node caught up a shard in an epoch its execution moved past");
}

/// Asserts that every chunk a live node applied, up to its last certified block, has the result
/// certified for it. A node that endorses nothing, like an observer, is checked nowhere else.
fn assert_execution_matches_certification(env: &TestLoopEnv) {
    if !cfg!(feature = "protocol_feature_spice") {
        return;
    }
    let mut num_checked = 0;
    for (idx, node_data) in env.node_datas.iter().enumerate() {
        if env.test_loop.is_denylisted(&node_data.identifier) {
            continue;
        }
        let node = env.node(idx);
        let client = node.client();
        let chain_store = client.chain.chain_store();
        for height in chain_store.tail() + 1..=node.last_certified_block_header().height() {
            let Ok(block_hash) = chain_store.get_block_hash_by_height(height) else {
                continue;
            };
            let header = chain_store.get_block_header(&block_hash).unwrap();
            let certified =
                client.chain.spice_core_reader.get_execution_results_by_shard_id(&header).unwrap();
            let shard_layout = client.epoch_manager.get_shard_layout(header.epoch_id()).unwrap();
            for shard_uid in shard_layout.shard_uids() {
                let Ok(chunk_extra) = chain_store.get_chunk_extra(&block_hash, &shard_uid) else {
                    continue;
                };
                let Some(result) = certified.get(&shard_uid.shard_id()) else {
                    continue;
                };
                num_checked += 1;
                assert_eq!(
                    chunk_extra.as_ref(),
                    &result.chunk_extra,
                    "{} applied shard {} at height {height} differently than certified",
                    node_data.account_id,
                    shard_uid.shard_id()
                );
            }
        }
    }
    assert!(num_checked > 0, "no node applied a certified chunk");
}
