/// ChunkProducers resolution works across a resharding boundary: every shard in the new
/// epoch's layout resolves correctly when looked up via the old epoch's last block.
#[cfg(feature = "nightly")]
mod tests {
    use crate::setup::builder::TestLoopBuilder;
    use crate::utils::account::{create_validators_spec, validators_spec_clients};
    use crate::utils::setups::derive_new_epoch_config_from_boundary;
    use near_async::time::Duration;
    use near_chain_configs::test_genesis::TestEpochConfigBuilder;
    use near_o11y::testonly::init_test_logger;
    use near_primitives::epoch_manager::EpochConfigStore;
    use near_primitives::shard_layout::ShardLayout;
    use near_primitives::stateless_validation::ChunkProductionKey;
    use near_primitives::types::AccountId;
    use near_primitives::version::PROTOCOL_VERSION;
    use std::collections::BTreeMap;
    use std::sync::Arc;

    #[test]
    fn test_chunk_producers_resharding_boundary() {
        init_test_logger();

        // One protocol version behind so the upgrade triggers resharding.
        let version = 3;
        let base_shard_layout = ShardLayout::multi_shard(3, version);
        let epoch_length = 5;
        let validators_spec = create_validators_spec(1, 0);
        let clients = validators_spec_clients(&validators_spec);
        let genesis = TestLoopBuilder::new_genesis_builder()
            .protocol_version(PROTOCOL_VERSION - 1)
            .validators_spec(validators_spec)
            .shard_layout(base_shard_layout.clone())
            .epoch_length(epoch_length)
            .build();

        let base_epoch_config = TestEpochConfigBuilder::from_genesis(&genesis).build();
        let (new_epoch_config, new_shard_layout) = {
            let boundary_account: AccountId = "boundary".parse().unwrap();
            derive_new_epoch_config_from_boundary(&base_epoch_config, &boundary_account)
        };

        let epoch_configs = vec![
            (genesis.config.protocol_version, Arc::new(base_epoch_config)),
            (genesis.config.protocol_version + 1, Arc::new(new_epoch_config)),
        ];
        let epoch_config_store = EpochConfigStore::test(BTreeMap::from_iter(epoch_configs));

        let mut env = TestLoopBuilder::new()
            .genesis(genesis)
            .clients(clients)
            .epoch_config_store(epoch_config_store)
            .build();

        let epoch_manager = env.validator().client().epoch_manager.clone();

        env.validator_runner().run_until(
            |node| {
                let epoch_id = node.head().epoch_id;
                epoch_manager.get_shard_layout(&epoch_id).unwrap() == new_shard_layout
            },
            Duration::seconds((3 * epoch_length) as i64),
        );

        // Walk back to the last block of the old epoch.
        let head_block = env.validator().head_block();
        let mut block = head_block;
        loop {
            let prev_hash = *block.header().prev_hash();
            let prev_block = env.validator().block(prev_hash);
            let prev_epoch_id = epoch_manager.get_epoch_id(prev_block.hash()).unwrap();
            let prev_shard_layout = epoch_manager.get_shard_layout(&prev_epoch_id).unwrap();
            if prev_shard_layout == base_shard_layout {
                // prev_block is the last block of the old epoch.
                block = prev_block;
                break;
            }
            block = prev_block;
        }

        let boundary_block_hash = block.hash();
        let boundary_epoch_id = epoch_manager.get_epoch_id(boundary_block_hash).unwrap();
        let boundary_shard_layout = epoch_manager.get_shard_layout(&boundary_epoch_id).unwrap();

        assert_eq!(boundary_shard_layout, base_shard_layout);
        assert!(
            epoch_manager.is_next_block_epoch_start(boundary_block_hash).unwrap(),
            "boundary block should be the last block of the old epoch"
        );

        assert!(
            new_shard_layout.shard_ids().count() > base_shard_layout.shard_ids().count(),
            "new layout should have more shards than old layout"
        );

        // Every shard in the new layout resolves via the old epoch's last block hash.
        let next_epoch_id =
            epoch_manager.get_epoch_id_from_prev_block(boundary_block_hash).unwrap();
        let next_height = block.header().height() + 1;

        for shard_id in new_shard_layout.shard_ids() {
            let db_result = epoch_manager
                .get_chunk_producer_info_from_prev_block(boundary_block_hash, shard_id);
            assert!(
                db_result.is_ok(),
                "get_chunk_producer_info_from_prev_block failed for shard_id={} at boundary block: {:?}",
                shard_id,
                db_result.err()
            );

            // Cross-check resolution against the canonical CPK computation.
            let cpk_result = epoch_manager
                .get_chunk_producer_info(&ChunkProductionKey {
                    epoch_id: next_epoch_id,
                    height_created: next_height,
                    shard_id,
                })
                .unwrap();

            assert_eq!(
                db_result.unwrap().account_id(),
                cpk_result.account_id(),
                "DB and CPK chunk producer mismatch for shard_id={} at boundary",
                shard_id,
            );
        }
    }
}

/// `DBCol::ChunkProducers` rows are copied to the cold store by the prefix-scan special
/// case, for both a mid-epoch anchor (own-epoch layout == child-epoch layout) and a
/// reshard-boundary anchor (rows keyed by the NEXT epoch's layout). The boundary anchor is
/// the case the generic own-epoch `combine_keys` copy would miss; the special case handles
/// both. The mid-epoch anchor guards that non-boundary anchors are copied too.
#[cfg(feature = "nightly")]
mod cold_storage_tests {
    use crate::setup::builder::{ArchivalKind, TestLoopBuilder};
    use crate::utils::setups::derive_new_epoch_config_from_boundary;
    use near_async::messaging::Handler;
    use near_async::time::Duration;
    use near_chain_configs::test_genesis::TestEpochConfigBuilder;
    use near_client::GetSplitStorageInfo;
    use near_o11y::testonly::init_test_logger;
    use near_primitives::epoch_manager::EpochConfigStore;
    use near_primitives::shard_layout::ShardLayout;
    use near_primitives::types::AccountId;
    use near_primitives::types::validator_stake::ValidatorStake;
    use near_primitives::utils::get_block_shard_id;
    use near_primitives::version::{PROTOCOL_VERSION, ProtocolFeature};
    use near_store::DBCol;
    use std::collections::BTreeMap;
    use std::sync::Arc;

    const EPOCH_LENGTH: u64 = 6;
    const GC_NUM_EPOCHS_TO_KEEP: u64 = 3;

    #[test]
    // TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
    #[cfg_attr(feature = "protocol_feature_spice", ignore)]
    fn test_chunk_producers_copied_to_cold_store() {
        init_test_logger();

        // The boundary anchor lives in the pre-reshard (PROTOCOL_VERSION - 1) epoch, so its
        // ChunkProducers rows are only written if EarlyKickout is enabled there. Assert the
        // dependency up front so a version regression fails fast with a clear reason rather
        // than surfacing later as a missing-row panic.
        assert!(
            ProtocolFeature::EarlyKickout.enabled(PROTOCOL_VERSION - 1),
            "test requires EarlyKickout enabled at PROTOCOL_VERSION - 1 (the pre-reshard epoch)"
        );

        // Shard layout that reshards (grows) at the protocol upgrade boundary.
        let version = 3;
        let base_shard_layout = ShardLayout::multi_shard(3, version);
        let boundary_account: AccountId = "boundary".parse().unwrap();

        let base_epoch_config = TestEpochConfigBuilder::new()
            .shard_layout(base_shard_layout.clone())
            .epoch_length(EPOCH_LENGTH)
            .build();
        let (new_epoch_config, new_shard_layout) =
            derive_new_epoch_config_from_boundary(&base_epoch_config, &boundary_account);
        assert!(
            new_shard_layout.shard_ids().count() > base_shard_layout.shard_ids().count(),
            "reshard should increase the shard count"
        );
        let epoch_config_store = EpochConfigStore::test(BTreeMap::from_iter([
            (PROTOCOL_VERSION - 1, Arc::new(base_epoch_config)),
            (PROTOCOL_VERSION, Arc::new(new_epoch_config)),
        ]));

        let mut env = TestLoopBuilder::new()
            .shard_layout(base_shard_layout.clone())
            .protocol_version(PROTOCOL_VERSION - 1)
            .epoch_length(EPOCH_LENGTH)
            .enable_archival_node(ArchivalKind::Cold)
            .epoch_config_store(epoch_config_store)
            .gc_num_epochs_to_keep(GC_NUM_EPOCHS_TO_KEEP)
            .build();

        // Run until resharding completes, then a few blocks further so the mid-epoch anchor
        // (boundary + 2) has been produced.
        env.archival_runner().run_until(
            |node| {
                let epoch_id = node.head().epoch_id;
                node.client().epoch_manager.get_shard_layout(&epoch_id).unwrap() == new_shard_layout
            },
            Duration::seconds((4 * EPOCH_LENGTH) as i64),
        );
        let reshard_done_height = env.archival_node().head().height;
        env.archival_runner().run_until_head_height(reshard_done_height + 4);

        // Capture the anchors + their hot ChunkProducers rows now, before hot GC removes them:
        // the rows are GC'd together with the anchor's block data, so read them before the GC
        // tail reaches the anchor.
        let (mid_height, expected) = {
            let node = env.archival_node();
            let client = node.client();
            let chain = &client.chain;
            let epoch_manager = client.epoch_manager.clone();
            let hot_store = node.store();

            // Boundary anchor B: last block of the pre-reshard epoch. Its ChunkProducers rows
            // are keyed by the NEXT (post-reshard) epoch's shard_ids.
            let mut block_hash = client.chain.head().unwrap().last_block_hash;
            let boundary_block_hash = loop {
                let header = chain.get_block_header(&block_hash).unwrap();
                let shard_layout = epoch_manager.get_shard_layout(header.epoch_id()).unwrap();
                if shard_layout == base_shard_layout {
                    break *header.hash();
                }
                block_hash = *header.prev_hash();
            };
            assert!(
                epoch_manager.is_next_block_epoch_start(&boundary_block_hash).unwrap(),
                "boundary block should be the last block of the pre-reshard epoch"
            );
            let boundary_height = chain.get_block_header(&boundary_block_hash).unwrap().height();

            // Mid-epoch anchor A: a block well inside the post-reshard epoch, so its own-epoch
            // layout equals its child-epoch layout (no reshard between anchor and its chunks).
            let mid_height = boundary_height + 2;
            let mid_block_hash = chain.get_block_hash_by_height(mid_height).unwrap();
            assert!(
                !epoch_manager.is_next_block_epoch_start(&mid_block_hash).unwrap(),
                "mid-epoch anchor must not be an epoch's last block"
            );
            let mid_epoch_id = epoch_manager.get_epoch_id(&mid_block_hash).unwrap();
            assert_eq!(
                epoch_manager.get_shard_layout(&mid_epoch_id).unwrap(),
                new_shard_layout,
                "mid-epoch anchor should be in the post-reshard layout"
            );

            // Both anchors resolve to rows keyed by the post-reshard layout's shard_ids.
            let mut expected: Vec<(Vec<u8>, ValidatorStake)> = Vec::new();
            for anchor_hash in [boundary_block_hash, mid_block_hash] {
                for shard_id in new_shard_layout.shard_ids() {
                    let key = get_block_shard_id(&anchor_hash, shard_id);
                    let hot: Option<ValidatorStake> =
                        hot_store.get_ser(DBCol::ChunkProducers, &key);
                    // Loud failure if EarlyKickout is not enabled (no rows would be written),
                    // rather than a vacuously passing cold check.
                    let hot = hot.unwrap_or_else(|| {
                        panic!(
                            "hot ChunkProducers row missing for anchor {anchor_hash} shard {shard_id}; \
                             is EarlyKickout enabled for this protocol version?"
                        )
                    });
                    expected.push((key, hot));
                }
            }
            (mid_height, expected)
        };

        // Advance well past the anchors so the cold copy loop has processed them.
        let target_height = reshard_done_height + EPOCH_LENGTH * (GC_NUM_EPOCHS_TO_KEEP + 2);
        env.archival_runner().run_until_head_height(target_height);

        // Cold head must have advanced past the anchors, else the cold check is vacuous.
        let cold_head_height = {
            let info =
                env.archival_node_mut().view_client_actor().handle(GetSplitStorageInfo {}).unwrap();
            info.cold_head_height.expect("cold head should be set")
        };
        assert!(
            cold_head_height >= mid_height,
            "cold head ({cold_head_height}) should have passed the anchors (up to height {mid_height})"
        );

        // Cold DB handle (same pattern as `resharding_cold_storage.rs`).
        let archival_idx = env.archival_data_idx();
        let cold_store_sender = env.node_datas[archival_idx].cold_store_sender.as_ref().unwrap();
        let cold_store_actor = env.test_loop.data.get(&cold_store_sender.actor_handle());
        let cold_db = cold_store_actor.get_cold_db();
        let cold_store = cold_db.as_store();

        for (key, hot) in expected {
            let cold: Option<ValidatorStake> = cold_store.get_ser(DBCol::ChunkProducers, &key);
            assert_eq!(
                cold.as_ref(),
                Some(&hot),
                "cold ChunkProducers row must match hot for key {key:?}"
            );
        }
    }
}

/// `DBCol::ChunkProducers` rows are hot-garbage-collected with the anchor block's data: a
/// below-tail anchor's rows disappear while an in-window anchor's rows survive and still
/// resolve. A wrongly-evicted in-window row would surface as `ChunkProducerNotInDB` and stall
/// the chain, so reaching the target height at all is the liveness check.
#[cfg(feature = "nightly")]
mod hot_gc_tests {
    use crate::setup::builder::TestLoopBuilder;
    use crate::utils::setups::derive_new_epoch_config_from_boundary;
    use near_async::time::Duration;
    use near_chain_configs::test_genesis::TestEpochConfigBuilder;
    use near_o11y::testonly::init_test_logger;
    use near_primitives::epoch_manager::EpochConfigStore;
    use near_primitives::shard_layout::ShardLayout;
    use near_primitives::types::validator_stake::ValidatorStake;
    use near_primitives::types::{AccountId, ShardId};
    use near_primitives::utils::get_block_shard_id;
    use near_primitives::version::{PROTOCOL_VERSION, ProtocolFeature};
    use near_store::DBCol;
    use std::collections::BTreeMap;
    use std::sync::Arc;

    const EPOCH_LENGTH: u64 = 5;
    const GC_NUM_EPOCHS_TO_KEEP: u64 = 3;

    #[test]
    // TODO(spice-test): assess relevance for spice.
    #[cfg_attr(feature = "protocol_feature_spice", ignore)]
    fn test_chunk_producers_garbage_collected() {
        init_test_logger();

        // Rows are only written when EarlyKickout is enabled; fail loud otherwise.
        assert!(
            ProtocolFeature::EarlyKickout.enabled(PROTOCOL_VERSION),
            "test requires EarlyKickout enabled at PROTOCOL_VERSION"
        );

        let shard_layout = ShardLayout::single_shard();
        let shard_id = ShardId::new(0);
        let mut env = TestLoopBuilder::new()
            .shard_layout(shard_layout)
            .epoch_length(EPOCH_LENGTH)
            .gc_num_epochs_to_keep(GC_NUM_EPOCHS_TO_KEEP)
            .build();

        // Record a low-height anchor's key while its block header still exists.
        env.validator_runner().run_until_head_height(2 * EPOCH_LENGTH);
        let old_anchor_height = EPOCH_LENGTH - 1;
        let old_key = {
            let node = env.validator();
            let old_anchor_hash =
                node.client().chain.get_block_hash_by_height(old_anchor_height).unwrap();
            let key = get_block_shard_id(&old_anchor_hash, shard_id);
            let row: Option<ValidatorStake> = node.store().get_ser(DBCol::ChunkProducers, &key);
            assert!(row.is_some(), "row should exist before GC for the old anchor");
            key
        };

        // Advance until the GC tail passes the old anchor. Reaching this height proves no
        // in-window row was wrongly evicted (an eviction would stall via ChunkProducerNotInDB).
        let target_height = (GC_NUM_EPOCHS_TO_KEEP + 4) * EPOCH_LENGTH;
        env.validator_runner().run_until_head_height(target_height);

        let node = env.validator();
        let chain = &node.client().chain;
        let epoch_manager = node.client().epoch_manager.clone();

        let old_row: Option<ValidatorStake> = node.store().get_ser(DBCol::ChunkProducers, &old_key);
        assert!(old_row.is_none(), "below-tail anchor's row must be garbage collected");

        // A chunk built on `child` anchors on its grandparent `anchor` (height anchor+1's
        // grandparent), so the retained anchor's row must still resolve.
        let head_height = node.head().height;
        let anchor_height = head_height - 3;
        let anchor_hash = chain.get_block_hash_by_height(anchor_height).unwrap();
        let child_hash = chain.get_block_hash_by_height(anchor_height + 1).unwrap();
        let in_window: Option<ValidatorStake> = node
            .store()
            .get_ser(DBCol::ChunkProducers, &get_block_shard_id(&anchor_hash, shard_id));
        assert!(in_window.is_some(), "in-window anchor's row must be retained");
        assert!(
            epoch_manager.get_chunk_producer_info_from_prev_block(&child_hash, shard_id).is_ok(),
            "retained in-window anchor must still resolve"
        );
    }

    /// Hot GC deletes a reshard-boundary anchor's rows, which are keyed by the NEXT
    /// (post-reshard) epoch's shard_ids. This is the case a naive own-epoch-layout delete would
    /// leak: clear_block_data iterates get_shard_uids_to_gc (this-epoch ∪ next-epoch), so every
    /// next-layout row for the boundary anchor is dropped once it falls below the GC tail.
    #[test]
    // TODO(spice-test): assess relevance for spice.
    #[cfg_attr(feature = "protocol_feature_spice", ignore)]
    fn test_chunk_producers_garbage_collected_at_reshard_boundary() {
        init_test_logger();

        // The boundary anchor lives in the pre-reshard (PROTOCOL_VERSION - 1) epoch, so its rows
        // are only written if EarlyKickout is enabled there.
        assert!(
            ProtocolFeature::EarlyKickout.enabled(PROTOCOL_VERSION - 1),
            "test requires EarlyKickout enabled at PROTOCOL_VERSION - 1 (the pre-reshard epoch)"
        );

        // Shard layout that grows at the protocol upgrade boundary.
        let version = 3;
        let base_shard_layout = ShardLayout::multi_shard(3, version);
        let boundary_account: AccountId = "boundary".parse().unwrap();
        let base_epoch_config = TestEpochConfigBuilder::new()
            .shard_layout(base_shard_layout.clone())
            .epoch_length(EPOCH_LENGTH)
            .build();
        let (new_epoch_config, new_shard_layout) =
            derive_new_epoch_config_from_boundary(&base_epoch_config, &boundary_account);
        assert!(
            new_shard_layout.shard_ids().count() > base_shard_layout.shard_ids().count(),
            "reshard should increase the shard count"
        );
        let epoch_config_store = EpochConfigStore::test(BTreeMap::from_iter([
            (PROTOCOL_VERSION - 1, Arc::new(base_epoch_config)),
            (PROTOCOL_VERSION, Arc::new(new_epoch_config)),
        ]));

        let mut env = TestLoopBuilder::new()
            .shard_layout(base_shard_layout.clone())
            .protocol_version(PROTOCOL_VERSION - 1)
            .epoch_length(EPOCH_LENGTH)
            .gc_num_epochs_to_keep(GC_NUM_EPOCHS_TO_KEEP)
            .epoch_config_store(epoch_config_store)
            .build();

        // Run until resharding completes.
        env.validator_runner().run_until(
            |node| {
                let epoch_id = node.head().epoch_id;
                node.client().epoch_manager.get_shard_layout(&epoch_id).unwrap() == new_shard_layout
            },
            Duration::seconds((4 * EPOCH_LENGTH) as i64),
        );
        let reshard_done_height = env.validator().head().height;
        env.validator_runner().run_until_head_height(reshard_done_height + 4);

        // Capture the boundary anchor + its next-layout row keys BEFORE GC removes the block.
        let (boundary_hash, boundary_keys) = {
            let node = env.validator();
            let client = node.client();
            let chain = &client.chain;
            let epoch_manager = client.epoch_manager.clone();
            let store = node.store();

            // Boundary anchor: last block of the pre-reshard epoch.
            let mut block_hash = chain.head().unwrap().last_block_hash;
            let boundary_hash = loop {
                let header = chain.get_block_header(&block_hash).unwrap();
                let shard_layout = epoch_manager.get_shard_layout(header.epoch_id()).unwrap();
                if shard_layout == base_shard_layout {
                    break *header.hash();
                }
                block_hash = *header.prev_hash();
            };
            assert!(
                epoch_manager.is_next_block_epoch_start(&boundary_hash).unwrap(),
                "boundary block should be the last block of the pre-reshard epoch"
            );

            // Rows keyed by the NEXT (post-reshard) layout's shard_ids.
            let mut keys = Vec::new();
            for shard_id in new_shard_layout.shard_ids() {
                let key = get_block_shard_id(&boundary_hash, shard_id);
                let row: Option<ValidatorStake> = store.get_ser(DBCol::ChunkProducers, &key);
                assert!(
                    row.is_some(),
                    "boundary next-layout row must exist before GC, shard {shard_id}"
                );
                keys.push(key);
            }
            (boundary_hash, keys)
        };

        // Advance well past the boundary so the GC tail deletes its block data.
        let target_height = reshard_done_height + EPOCH_LENGTH * (GC_NUM_EPOCHS_TO_KEEP + 2);
        env.validator_runner().run_until_head_height(target_height);

        let node = env.validator();
        // Sanity: the boundary block itself is GC'd, so the row absence below is a real delete
        // (not a never-existed vacuous pass).
        assert!(
            node.client().chain.get_block(&boundary_hash).is_err(),
            "boundary block should be garbage collected by now"
        );
        for key in &boundary_keys {
            let row: Option<ValidatorStake> = node.store().get_ser(DBCol::ChunkProducers, key);
            assert!(row.is_none(), "boundary next-layout row must be garbage collected: {key:?}");
        }
    }
}
