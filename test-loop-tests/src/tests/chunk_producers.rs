/// Tests for the ChunkProducers DB column across resharding boundaries.
///
/// Verifies that `get_chunk_producer_info_db(prev_block_hash, shard_id)` returns
/// the correct chunk producer even when the shard layout changes between epochs
/// (i.e. a shard that didn't exist in epoch E1 can be looked up using the hash
/// of the last block in E1, because the write side saves using the next epoch's
/// shard layout at the boundary).
///
/// Scope note (early kickout, PR6): with no chunk misses the blacklist is empty, so
/// the producer written/read here is the default sampling result. The full behavioral
/// "node stops producing -> slots reassign mid-epoch" e2e (a multi-validator chain that
/// actually drops chunks) is intentionally deferred to PR7. This module is NOT a
/// post-epoch-sync production proof either: seeding boundary `ChunkProducers` after epoch
/// sync is owned by PR4b.
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
