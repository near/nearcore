use crate::setup::builder::TestLoopBuilder;
use crate::utils::account::{create_validators_spec, validators_spec_clients};
use near_async::test_loop::data::TestLoopData;
use near_async::time::Duration;
use near_chain_configs::test_genesis::TestEpochConfigBuilder;
use near_o11y::testonly::init_test_logger;
use near_primitives::epoch_manager::EpochConfigStore;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::upgrade_schedule::ProtocolUpgradeVotingSchedule;
use near_primitives::version::ProtocolFeature;
use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_early_kickout_version_upgrade() {
    init_test_logger();

    let old_protocol = ProtocolFeature::EarlyKickout.protocol_version() - 1;
    let new_protocol = ProtocolFeature::EarlyKickout.protocol_version();
    let epoch_length = 5;

    // Several block+chunk producers over several shards, so chunks are gossiped between nodes
    // and each producer runs arrival-time verification on the *other* shards' chunk headers.
    let validators_spec = create_validators_spec(3, 0);
    let clients = validators_spec_clients(&validators_spec);
    let shard_layout = ShardLayout::multi_shard(3, 0);

    let genesis = TestLoopBuilder::new_genesis_builder()
        .protocol_version(old_protocol)
        .validators_spec(validators_spec)
        .shard_layout(shard_layout)
        .epoch_length(epoch_length)
        .build();
    let epoch_config = TestEpochConfigBuilder::from_genesis(&genesis).build();
    let epoch_config_store = EpochConfigStore::test(BTreeMap::from([
        (old_protocol, Arc::new(epoch_config.clone())),
        (new_protocol, Arc::new(epoch_config)),
    ]));

    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .clients(clients)
        .epoch_config_store(epoch_config_store)
        .protocol_upgrade_schedule(ProtocolUpgradeVotingSchedule::new_immediate(new_protocol))
        .build();

    let start_height = env.node(0).head().height;
    let client_handle = env.node_datas[0].client_sender.actor_handle();
    let old_epochs = RefCell::new(BTreeSet::new());
    let new_epochs = RefCell::new(BTreeSet::new());
    let last_height = Cell::new(0);
    let success_condition = |data: &mut TestLoopData| -> bool {
        let client = &data.get(&client_handle).client;
        let tip = client.chain.head().unwrap();
        let height = client.chain.get_block_header(&tip.last_block_hash).unwrap().height();
        if height != last_height.get() {
            if last_height.get() != 0 {
                assert_eq!(last_height.get() + 1, height, "block skipped at height {height}");
            }
            last_height.set(height);
        }
        let protocol_version =
            client.epoch_manager.get_epoch_info(&tip.epoch_id).unwrap().protocol_version();
        if protocol_version < new_protocol {
            old_epochs.borrow_mut().insert(tip.epoch_id);
        } else {
            new_epochs.borrow_mut().insert(tip.epoch_id);
        }
        // Stop once we have seen the legacy regime and settled into the new one.
        !old_epochs.borrow().is_empty() && new_epochs.borrow().len() >= 2
    };
    env.test_loop.run_until(success_condition, Duration::seconds((12 * epoch_length) as i64));

    // Walk every steady-state block and check that no valid chunk was dropped across the
    // EarlyKickout boundary. The anchor rides the `PartialEncodedChunkV3` wire message, not the
    // chunk header, so the header version does not change across the boundary and there is
    // nothing header-side to assert here; the observable signal of a verification gap is a
    // missing chunk (a valid V3 chunk dropped at arrival on the non-producing nodes).
    let client = &env.test_loop.data.get(&client_handle).client;
    let head_height = client.chain.head().unwrap().height;
    let mut saw_old = false;
    let mut saw_new = false;
    for height in (start_height + 1)..=head_height {
        let Ok(block) = client.chain.get_block_by_height(height) else {
            continue;
        };
        let protocol_version = client
            .epoch_manager
            .get_epoch_info(block.header().epoch_id())
            .unwrap()
            .protocol_version();

        // A verification gap would drop valid chunks at arrival, surfacing as a false in the mask.
        assert!(
            block.header().chunk_mask().iter().all(|&present| present),
            "missing chunk at height {height} (protocol {protocol_version}) — \
             a verification gap would drop valid chunks"
        );

        if protocol_version >= new_protocol {
            saw_new = true;
        } else {
            saw_old = true;
        }
    }
    assert!(saw_old, "expected to observe pre-EarlyKickout blocks");
    assert!(saw_new, "expected to observe post-EarlyKickout blocks");
}
