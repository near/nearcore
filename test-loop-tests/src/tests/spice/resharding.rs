use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::utils::account::{create_validators_spec, validators_spec_clients};
use crate::utils::setups::derive_new_epoch_config_from_boundary;
use itertools::Itertools as _;
use near_async::messaging::CanSend as _;
use near_async::time::Duration;
use near_chain::spice::core::get_last_certified_block_header;
use near_chain_configs::test_genesis::TestEpochConfigBuilder;
use near_client::spice::data_distributor_actor::MalformedDataRequest;
use near_network::recv_permit::RecvMessagePermit;
use near_network::spice::data_distribution::{SpiceDataRequest, SpiceDataRequestMessage};
use near_o11y::testonly::init_test_logger;
use near_primitives::epoch_manager::EpochConfigStore;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::spice::partial_data::SpiceDataIdentifier;
use near_primitives::types::{AccountId, ShardId};
use near_primitives::version::PROTOCOL_VERSION;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

/// Regression test: verifies that block processing works across a resharding
/// boundary in SPICE mode. A bug in get_last_certified_execution_results_for_next_block
/// used block_header.epoch_id() instead of last_certified_block_header.epoch_id()
/// to compute num_shards. With resharding and certification lag, this caused a
/// panic when the shard count changed between the certified block's epoch and
/// the current block's epoch.
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_spice_certified_results_across_resharding() {
    init_test_logger();

    let version = 3;
    let base_shard_layout = ShardLayout::multi_shard(1, version);
    let epoch_length = 5;
    let validators_spec = create_validators_spec(1, 0);
    let clients = validators_spec_clients(&validators_spec);

    let genesis = TestLoopBuilder::new_genesis_builder()
        .protocol_version(PROTOCOL_VERSION - 1)
        .validators_spec(validators_spec)
        .shard_layout(base_shard_layout)
        .epoch_length(epoch_length)
        .build();

    let base_epoch_config = TestEpochConfigBuilder::from_genesis(&genesis).build();
    let boundary_account: AccountId = "boundary".parse().unwrap();
    let (new_epoch_config, new_shard_layout) =
        derive_new_epoch_config_from_boundary(&base_epoch_config, &boundary_account);

    let epoch_configs = vec![
        (genesis.config.protocol_version, Arc::new(base_epoch_config)),
        (genesis.config.protocol_version + 1, Arc::new(new_epoch_config)),
    ];
    let epoch_config_store = EpochConfigStore::test(BTreeMap::from_iter(epoch_configs));

    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .clients(clients)
        .epoch_config_store(epoch_config_store)
        .delay_warmup()
        .build();

    let execution_delay = 2;
    env.delay_endorsements_propagation(execution_delay);
    env = env.warmup();

    let epoch_manager = env.validator().client().epoch_manager.clone();

    env.validator_runner().run_until(
        |node| {
            let epoch_id = node.head().epoch_id;
            epoch_manager.get_shard_layout(&epoch_id).unwrap() == new_shard_layout
        },
        Duration::seconds((3 * epoch_length) as i64),
    );
    let new_epoch_start = env.validator().head().height;

    // Run a few more blocks in the resharded epoch to exercise the code path
    // where last_certified_block is in the old epoch but current block is in the new epoch.
    env.validator_runner().run_for_number_of_blocks(5);

    // Assert that the first block of the resharded epoch has its last certified
    // block in the previous epoch with a different number of shards.
    let node = env.validator();
    let chain_store = &node.client().chain.chain_store;
    let header = chain_store.get_block_header_by_height(new_epoch_start).unwrap();
    let last_certified = get_last_certified_block_header(chain_store, header.hash()).unwrap();
    let certified_shard_layout = epoch_manager.get_shard_layout(last_certified.epoch_id()).unwrap();
    assert_ne!(
        epoch_manager.get_shard_layout(header.epoch_id()).unwrap(),
        certified_shard_layout,
        "expected the first block of the resharded epoch to have its last certified block in the previous epoch with different shard count"
    );
}

/// A receipt proof from the last block before a resharding names its destination in that
/// block's shard layout: its producer serves a request naming the split shard and refuses one
/// naming a shard it splits into.
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_spice_receipt_proof_request_names_destination_in_the_source_blocks_layout() {
    init_test_logger();

    let version = 3;
    let base_shard_layout = ShardLayout::multi_shard(2, version);
    let epoch_length = 10;
    let validators_spec = create_validators_spec(4, 0);
    let clients = validators_spec_clients(&validators_spec);

    let genesis = TestLoopBuilder::new_genesis_builder()
        .protocol_version(PROTOCOL_VERSION - 1)
        .validators_spec(validators_spec)
        .shard_layout(base_shard_layout.clone())
        .epoch_length(epoch_length)
        .build();

    let base_epoch_config = TestEpochConfigBuilder::from_genesis(&genesis).build();
    let boundary_account: AccountId = "boundary".parse().unwrap();
    let (new_epoch_config, new_shard_layout) =
        derive_new_epoch_config_from_boundary(&base_epoch_config, &boundary_account);
    let epoch_configs = vec![
        (genesis.config.protocol_version, Arc::new(base_epoch_config)),
        (genesis.config.protocol_version + 1, Arc::new(new_epoch_config)),
    ];
    let epoch_config_store = EpochConfigStore::test(BTreeMap::from_iter(epoch_configs));

    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .clients(clients)
        .epoch_config_store(epoch_config_store)
        .track_all_shards()
        .build();

    let epoch_manager = env.validator().client().epoch_manager.clone();
    env.validator_runner().run_until(
        |node| epoch_manager.get_shard_layout(&node.head().epoch_id).unwrap() == new_shard_layout,
        Duration::seconds((3 * epoch_length) as i64),
    );

    let node = env.validator();
    let first_resharded = node.head().last_block_hash;
    let last_before = *node.client().chain.get_block_header(&first_resharded).unwrap().prev_hash();
    let source_epoch_id = *node.client().chain.get_block_header(&last_before).unwrap().epoch_id();
    let split_shard = base_shard_layout
        .shard_ids()
        .find(|shard_id| !new_shard_layout.shard_ids().contains(shard_id))
        .unwrap();
    let child_shard = new_shard_layout.get_children_shards_ids(split_shard).unwrap()[0];
    let from_shard =
        base_shard_layout.shard_ids().find(|shard_id| *shard_id != split_shard).unwrap();
    let producer = epoch_manager
        .get_epoch_chunk_producers_for_shard(&source_epoch_id, from_shard)
        .unwrap()
        .swap_remove(0);
    let distributor = env
        .node_datas
        .iter()
        .find(|data| data.account_id == producer)
        .unwrap()
        .spice_data_distributor_sender
        .clone();
    let request = |to_shard: ShardId| SpiceDataRequestMessage {
        request: SpiceDataRequest::new(
            BTreeMap::from([(
                SpiceDataIdentifier::ReceiptProof {
                    block_hash: last_before,
                    from_shard_id: from_shard,
                    to_shard_id: to_shard,
                },
                BTreeSet::from([0]),
            )]),
            "requester.near".parse().unwrap(),
        ),
        recv_permit: RecvMessagePermit::none(),
    };
    let refused = |env: &TestLoopEnv| {
        env.test_loop
            .data
            .get(&distributor.actor_handle())
            .malformed_data_request_count(MalformedDataRequest::UnknownShard)
    };
    let refused_before = refused(&env);

    distributor.send(request(split_shard));
    env.test_loop.run_for(Duration::seconds(1));
    assert_eq!(refused(&env), refused_before, "the split shard was refused");

    distributor.send(request(child_shard));
    env.test_loop.run_for(Duration::seconds(1));
    assert_eq!(refused(&env), refused_before + 1, "the child shard was served");
}
