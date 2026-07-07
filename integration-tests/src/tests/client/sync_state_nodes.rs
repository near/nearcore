use itertools::Itertools;
use near_async::ActorSystem;
use near_async::messaging::CanSendAsync;
use near_chain_configs::{Genesis, TrackedShardsConfig};
use near_client::GetBlock;
use near_client_primitives::types::GetValidatorInfo;
use near_network::client::{StatePartOrHeader, StateRequestHeader, StateRequestPart};
use near_network::tcp;
use near_network::test_utils::{convert_boot_nodes, wait_or_timeout};
use near_o11y::testonly::init_test_logger;
use near_primitives::types::{BlockId, BlockReference, EpochId, EpochReference};
use near_store::db::RocksDB;
use nearcore::{load_test_config, start_with_config};
use std::ops::ControlFlow;
use std::sync::Arc;

#[tokio::test]
// Tests StateRequestHeader and StateRequestPart.
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
async fn slow_test_state_sync_headers() {
    init_test_logger();

    let _dir1 =
        Arc::new(tempfile::Builder::new().prefix("test_state_sync_headers").tempdir().unwrap());
    let dir1 = _dir1.clone();
    let actor_system = ActorSystem::new();
    let mut genesis = Genesis::test(vec!["test1".parse().unwrap()], 1);
    // Increase epoch_length if the test is flaky.
    genesis.config.epoch_length = 100;
    genesis.config.transaction_validity_period = 200;

    let mut near1 =
        load_test_config("test1", tcp::ListenerAddr::reserve_for_test(), genesis.clone());
    near1.client_config.min_num_peers = 0;
    near1.client_config.tracked_shards_config = TrackedShardsConfig::AllShards;
    near1.config.store.enable_state_snapshot();

    let nearcore::NearNode {
        view_client: view_client1,
        state_request_client: state_request_client1,
        ..
    } = start_with_config(dir1.path(), near1, actor_system.clone())
        .await
        .expect("start_with_config");

    // First we need to find sync_hash. That is done in 3 steps:
    // 1. Get the latest block
    // 2. Query validators for the epoch_id of that block.
    // 3. Get a block at 'epoch_start_height' that is found in the response of the validators method.
    //
    // Second, we request state sync header.
    // Third, we request state sync part with part_id = 0.
    wait_or_timeout(1000, 110000, || async {
        let epoch_id = match view_client1.send_async(GetBlock::latest()).await {
            Ok(Ok(b)) => Some(b.header.epoch_id),
            _ => None,
        };
        // async is hard, will use this construct to reduce nested code.
        let epoch_id = match epoch_id {
            Some(x) => x,
            None => return ControlFlow::Continue(()),
        };
        tracing::info!(?epoch_id, "got epoch_id");

        let epoch_start_height = match view_client1
            .send_async(GetValidatorInfo {
                epoch_reference: EpochReference::EpochId(EpochId(epoch_id)),
            })
            .await
        {
            Ok(Ok(v)) => Some(v.epoch_start_height),
            _ => None,
        };
        let epoch_start_height = match epoch_start_height {
            Some(x) => x,
            None => return ControlFlow::Continue(()),
        };
        tracing::info!(epoch_start_height, "got epoch_start_height");

        // here since there's only one block/chunk producer, we assume that no blocks will be missing chunks.
        let sync_height = epoch_start_height + 3;

        let block_id = BlockReference::BlockId(BlockId::Height(sync_height));
        let block_view = view_client1.send_async(GetBlock(block_id)).await;
        let Ok(Ok(block_view)) = block_view else {
            return ControlFlow::Continue(());
        };
        let sync_hash = block_view.header.hash;
        let shard_ids = block_view.chunks.iter().map(|c| c.shard_id).collect_vec();
        tracing::info!(?sync_hash, ?shard_ids, "got sync_hash");

        for shard_id in shard_ids {
            // Make StateRequestHeader and expect that the response contains a header.
            let state_response_info = match state_request_client1
                .send_async(StateRequestHeader { shard_id, sync_hash })
                .await
            {
                Ok(Some(StatePartOrHeader(state_response_info))) => Some(state_response_info),
                _ => None,
            };
            let state_response_info = match state_response_info {
                Some(x) => x,
                None => return ControlFlow::Continue(()),
            };
            let state_response = state_response_info.take_state_response();
            assert!(state_response.clone().take_part().is_none());
            let header = state_response.take_header();
            if header.is_some() {
                tracing::info!(?sync_hash, %shard_id, "got header");
            } else {
                tracing::info!(?sync_hash, %shard_id, "got no header");
                return ControlFlow::Continue(());
            }

            // Make StateRequestPart and expect that the response contains a part and part_id = 0 and the node has all parts cached.
            let state_response_info = match state_request_client1
                .send_async(StateRequestPart { shard_id, sync_hash, part_id: 0 })
                .await
            {
                Ok(Some(StatePartOrHeader(state_response_info))) => Some(state_response_info),
                _ => None,
            };
            let state_response_info = match state_response_info {
                Some(x) => x,
                None => return ControlFlow::Continue(()),
            };
            let state_response = state_response_info.take_state_response();
            assert!(state_response.clone().take_header().is_none());
            let part = state_response.take_part();
            if let Some((part_id, _part)) = part {
                if part_id != 0 {
                    tracing::info!(?sync_hash, %shard_id, part_id, "got wrong part");
                    return ControlFlow::Continue(());
                }
                tracing::info!(?sync_hash, %shard_id, part_id, "got part");
            } else {
                tracing::info!(?sync_hash, %shard_id, "got no part");
                return ControlFlow::Continue(());
            }
        }
        return ControlFlow::Break(());
    })
    .await
    .unwrap();
    drop(_dir1);
    actor_system.stop();
    RocksDB::block_until_all_instances_are_dropped();
}

#[tokio::test]
// Tests StateRequestHeader and StateRequestPart.
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
async fn slow_test_state_sync_headers_no_tracked_shards() {
    // Huh. The compiler complains about type system cycle if this async move is stripped.
    Box::pin(async move {
        init_test_logger();

        let _dir1 = Arc::new(
            tempfile::Builder::new()
                .prefix("test_state_sync_headers_no_tracked_shards_1")
                .tempdir()
                .unwrap(),
        );
        let dir1 = _dir1.clone();
        let _dir2 = Arc::new(
            tempfile::Builder::new()
                .prefix("test_state_sync_headers_no_tracked_shards_2")
                .tempdir()
                .unwrap(),
        );
        let dir2 = _dir2.clone();
        let actor_system = ActorSystem::new();
        let mut genesis = Genesis::test(vec!["test1".parse().unwrap()], 1);
        // Increase epoch_length if the test is flaky.
        let epoch_length = 100;
        genesis.config.epoch_length = epoch_length;
        genesis.config.transaction_validity_period = epoch_length * 2;

        let port1 = tcp::ListenerAddr::reserve_for_test();
        let mut near1 = load_test_config("test1", port1, genesis.clone());
        near1.client_config.min_num_peers = 0;
        // TODO(cloud_archival): Since stateless validation, validators do not need to track all shards.
        // That should likely be changed to `TrackedShardsConfig::NoShards`.
        near1.client_config.tracked_shards_config = TrackedShardsConfig::AllShards; // Track all shards, it is a validator.
        near1.config.store.disable_state_snapshot();

        start_with_config(dir1.path(), near1, actor_system.clone())
            .await
            .expect("start_with_config");

        let mut near2 =
            load_test_config("test2", tcp::ListenerAddr::reserve_for_test(), genesis.clone());
        near2.network_config.peer_store.boot_nodes = convert_boot_nodes(vec![("test1", *port1)]);
        near2.client_config.min_num_peers = 0;
        near2.client_config.tracked_shards_config = TrackedShardsConfig::NoShards;
        near2.config.store.enable_state_snapshot();

        let nearcore::NearNode {
            view_client: view_client2,
            state_request_client: state_request_client2,
            ..
        } = start_with_config(dir2.path(), near2, actor_system.clone())
            .await
            .expect("start_with_config");

        // First we need to find sync_hash. That is done in 3 steps:
        // 1. Get the latest block
        // 2. Query validators for the epoch_id of that block.
        // 3. Get a block at 'epoch_start_height' that is found in the response of the validators method.
        //
        // Second, we request state sync header.
        // Third, we request state sync part with part_id = 0.
        wait_or_timeout(1000, 110000, async || {
            let epoch_id = match view_client2.send_async(GetBlock::latest()).await {
                Ok(Ok(b)) => Some(b.header.epoch_id),
                _ => None,
            };
            // async is hard, will use this construct to reduce nested code.
            let epoch_id = match epoch_id {
                Some(x) => x,
                None => return ControlFlow::Continue(()),
            };
            tracing::info!(?epoch_id, "got epoch_id");

            let epoch_start_height = match view_client2
                .send_async(GetValidatorInfo {
                    epoch_reference: EpochReference::EpochId(EpochId(epoch_id)),
                })
                .await
            {
                Ok(Ok(v)) => Some(v.epoch_start_height),
                _ => None,
            };
            let epoch_start_height = match epoch_start_height {
                Some(x) => x,
                None => return ControlFlow::Continue(()),
            };
            tracing::info!(epoch_start_height, "got epoch_start_height");
            if epoch_start_height < 2 * epoch_length {
                return ControlFlow::Continue(());
            }

            // here since there's only one block/chunk producer, we assume that no blocks will be missing chunks.
            let sync_height = epoch_start_height + 3;

            let block_id = BlockReference::BlockId(BlockId::Height(sync_height));
            let block_view = view_client2.send_async(GetBlock(block_id)).await;
            let Ok(Ok(block_view)) = block_view else {
                return ControlFlow::Continue(());
            };
            let sync_hash = block_view.header.hash;
            let shard_ids = block_view.chunks.iter().map(|c| c.shard_id).collect_vec();
            tracing::info!(?sync_hash, ?shard_ids, "got sync_hash");

            for shard_id in shard_ids {
                // Make StateRequestHeader and expect that the response contains a header.
                let state_response_info = match state_request_client2
                    .send_async(StateRequestHeader { shard_id, sync_hash })
                    .await
                {
                    Ok(Some(StatePartOrHeader(state_response_info))) => Some(state_response_info),
                    _ => None,
                };
                let state_response_info = match state_response_info {
                    Some(x) => x,
                    None => return ControlFlow::Continue(()),
                };
                tracing::info!(?state_response_info, "got header state response");
                let state_response = state_response_info.take_state_response();
                assert!(state_response.clone().take_header().is_none());
                assert!(state_response.take_part().is_none());

                // Make StateRequestPart and expect that the response contains a part and part_id = 0 and the node has all parts cached.
                let state_response_info = match state_request_client2
                    .send_async(StateRequestPart { shard_id, sync_hash, part_id: 0 })
                    .await
                {
                    Ok(Some(StatePartOrHeader(state_response_info))) => Some(state_response_info),
                    _ => None,
                };
                let state_response_info = match state_response_info {
                    Some(x) => x,
                    None => return ControlFlow::Continue(()),
                };
                tracing::info!(?state_response_info, "got state part response");
                let state_response = state_response_info.take_state_response();
                assert!(state_response.clone().take_header().is_none());
                assert!(state_response.take_part().is_none());
            }
            return ControlFlow::Break(());
        })
        .await
        .unwrap();
        drop(_dir1);
        drop(_dir2);
        actor_system.stop();
        RocksDB::block_until_all_instances_are_dropped();
    })
    .await;
}
