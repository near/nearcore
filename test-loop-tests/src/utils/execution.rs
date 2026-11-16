use crate::setup::env::TestLoopEnv;
use near_chain::ChainStoreAccess as _;
use near_chain::near_chain_primitives::ApplyChunksMode;
use near_primitives::types::BlockHeight;
use near_primitives::version::{PROTOCOL_VERSION, ProtocolFeature};

/// How far a node's execution head may fall behind its head at the end of a test.
pub(crate) const MAX_EXECUTION_LAG: BlockHeight = 3;

/// Asserts that on every live node the execution head is within `MAX_EXECUTION_LAG` blocks of the
/// head, and that what it applied was certified.
pub(crate) fn assert_execution_kept_up(env: &TestLoopEnv) {
    for node in env.live_nodes() {
        let head = node.head().height;
        let executed = node.last_executed().height;
        assert!(
            executed + MAX_EXECUTION_LAG >= head,
            "execution of {} fell behind the head: head {head}, executed {executed}",
            node.node_data.account_id
        );
    }
    assert_execution_matches_certification(env);
}

/// Asserts that every live node applied each shard it caught up for an epoch in every canonical
/// block of that epoch after the epoch's first block, where spice syncs the shard, for the epochs
/// its execution has moved past and whose first block has not yet been garbage collected, and
/// that there was at least one such shard to check.
pub(crate) fn assert_caught_up_shards_applied(env: &TestLoopEnv) {
    if !ProtocolFeature::Spice.enabled(PROTOCOL_VERSION) {
        return;
    }
    let mut num_checked = 0;
    for node in env.live_nodes() {
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
            let Ok(block_info) = client.epoch_manager.get_block_info(&block_hash) else {
                continue;
            };
            let epoch_first_block = block_info.epoch_first_block();
            let shard_layout = client.epoch_manager.get_shard_layout(header.epoch_id()).unwrap();
            for shard_uid in shard_layout.shard_uids() {
                if !client.shard_tracker.should_apply_chunk(
                    ApplyChunksMode::OnlyCatchupShards,
                    header.prev_hash(),
                    shard_uid.shard_id(),
                ) {
                    continue;
                }
                // A node that state synced into a later epoch also holds a few blocks of this one,
                // downloaded for the sync, which it never applied. Only a shard caught up at this
                // epoch's first block, where state sync writes its chunk extra, has to be applied
                // in the rest of the epoch.
                if chain_store.get_chunk_extra(epoch_first_block, &shard_uid).is_err() {
                    continue;
                }
                num_checked += 1;
                assert!(
                    chain_store.get_chunk_extra(&block_hash, &shard_uid).is_ok(),
                    "{} did not apply caught-up shard {} at height {height}",
                    node.node_data.account_id,
                    shard_uid.shard_id()
                );
            }
        }
    }
    assert!(num_checked > 0, "no node caught up a shard in an epoch its execution moved past");
}

/// Asserts that every chunk a live node applied, up to its last certified block, has the result
/// certified for it, and that every live node that tracks a shard and is done syncing applied one.
fn assert_execution_matches_certification(env: &TestLoopEnv) {
    if !ProtocolFeature::Spice.enabled(PROTOCOL_VERSION) {
        return;
    }
    for node in env.live_nodes() {
        let client = node.client();
        let chain_store = client.chain.chain_store();
        let mut applied_certified_chunk = false;
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
                applied_certified_chunk = true;
                assert_eq!(
                    chunk_extra.as_ref(),
                    &result.chunk_extra,
                    "{} applied shard {} at height {height} differently than certified",
                    node.node_data.account_id,
                    shard_uid.shard_id()
                );
            }
        }

        let tracks_a_shard = !client
            .shard_tracker
            .tracked_shard_uids_this_or_next_epoch(&node.head().prev_block_hash)
            .unwrap()
            .is_empty();
        let synced = !client.sync_handler.sync_status.is_syncing();

        if tracks_a_shard && synced {
            assert!(
                applied_certified_chunk,
                "{} tracks a shard but applied no certified chunk",
                node.node_data.account_id
            );
        }
    }
}
