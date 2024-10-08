//! Provides functions for setting up a mock network from configs and home dirs.

use crate::{MockNetworkConfig, MockPeer};
use anyhow::Context;
use near_chain::types::RuntimeAdapter;
use near_chain::ChainStoreUpdate;
use near_chain::{Chain, ChainGenesis, ChainStore, ChainStoreAccess, DoomslugThresholdMode};
use near_crypto::{KeyType, SecretKey};
use near_epoch_manager::shard_tracker::{ShardTracker, TrackedConfig};
use near_epoch_manager::{EpochManager, EpochManagerAdapter, EpochManagerHandle};
use near_jsonrpc_client::JsonRpcClient;
use near_network::tcp;
use near_network::types::PeerInfo;
use near_primitives::network::PeerId;
use near_primitives::state_part::PartId;
use near_primitives::state_sync::get_num_state_parts;
use near_primitives::types::{BlockHeight, NumShards, ShardId};
use near_store::test_utils::create_test_store;
use near_time::Clock;
use nearcore::{NearConfig, NightshadeRuntime, NightshadeRuntimeExt};
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use std::cmp::min;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

fn setup_runtime(
    home_dir: &Path,
    config: &NearConfig,
    in_memory_storage: bool,
) -> (Arc<EpochManagerHandle>, ShardTracker, Arc<NightshadeRuntime>) {
    let store = if in_memory_storage {
        create_test_store()
    } else {
        near_store::NodeStorage::opener(home_dir, config.config.archive, &config.config.store, None)
            .open()
            .unwrap()
            .get_hot_store()
    };
    let epoch_manager = EpochManager::new_arc_handle(store.clone(), &config.genesis.config);
    let shard_tracker =
        ShardTracker::new(TrackedConfig::from_config(&config.client_config), epoch_manager.clone());
    let runtime = NightshadeRuntime::from_config(home_dir, store, config, epoch_manager.clone())
        .expect("could not create transaction runtime");
    (epoch_manager, shard_tracker, runtime)
}

fn setup_mock_peer(
    chain: Chain,
    config: &mut NearConfig,
    network_start_height: Option<BlockHeight>,
    network_config: MockNetworkConfig,
    target_height: BlockHeight,
    num_shards: ShardId,
    mock_listen_addr: tcp::ListenerAddr,
) -> tokio::task::JoinHandle<anyhow::Result<()>> {
    let network_start_height = match network_start_height {
        None => target_height,
        Some(0) => chain.genesis_block().header().height(),
        Some(it) => it,
    };
    let secret_key = SecretKey::from_random(KeyType::ED25519);
    config
        .network_config
        .peer_store
        .boot_nodes
        .push(PeerInfo::new(PeerId::new(secret_key.public_key()), *mock_listen_addr));
    let chain_id = config.genesis.config.chain_id.clone();
    let block_production_delay = config.client_config.min_block_production_delay;
    let archival = config.client_config.archive;
    actix::spawn(async move {
        let mock = MockPeer::new(
            chain,
            secret_key,
            mock_listen_addr,
            chain_id,
            archival,
            block_production_delay.unsigned_abs(),
            num_shards,
            network_start_height,
            network_config,
        )
        .await?;
        mock.run(target_height).await
    })
}

pub struct MockNode {
    // target height actually available to sync to in the chain history database
    pub target_height: BlockHeight,
    pub mock_peer: tokio::task::JoinHandle<anyhow::Result<()>>,
    // client that allows making RPC requests to the node under test
    pub rpc_client: JsonRpcClient,
}

/// Setup up a mock node, including setting up
/// a MockPeerManagerActor and a ClientActor and a ViewClientActor
/// `client_home_dir`: home dir for the new client
/// `network_home_dir`: home dir that contains the pre-generated chain history, will be used
///                     to construct `MockPeerManagerActor`
/// `config`: config for the new client
/// `network_delay`: delay for getting response from the simulated network
/// `client_start_height`: start height for client
/// `network_start_height`: height at which the simulated network starts producing blocks
/// `target_height`: height that the simulated peers will produce blocks until. If None, will
///                  use the height from the chain head in storage
/// `in_memory_storage`: if true, make client use in memory storage instead of rocksdb
///
/// Returns a struct representing the node under test
pub fn setup_mock_node(
    client_home_dir: &Path,
    network_home_dir: &Path,
    mut config: NearConfig,
    network_config: &MockNetworkConfig,
    client_start_height: BlockHeight,
    network_start_height: Option<BlockHeight>,
    target_height: Option<BlockHeight>,
    in_memory_storage: bool,
    mock_listen_addr: tcp::ListenerAddr,
) -> MockNode {
    let parent_span = tracing::debug_span!(target: "mock_node", "setup_mock_node").entered();
    let (mock_network_epoch_manager, mock_network_shard_tracker, mock_network_runtime) =
        setup_runtime(network_home_dir, &config, false);
    tracing::info!(target: "mock_node", ?network_home_dir, "Setup network runtime");

    let chain_genesis = ChainGenesis::new(&config.genesis.config);

    // set up client dir to be ready to process blocks from client_start_height
    if client_start_height > 0 {
        tracing::info!(target: "mock_node", "Preparing client data dir to be able to start at the specified start height {}", client_start_height);
        let (client_epoch_manager, _, client_runtime) =
            setup_runtime(client_home_dir, &config, in_memory_storage);
        tracing::info!(target: "mock_node", ?client_home_dir, "Setup client runtime");
        let mut chain_store = ChainStore::new(
            client_runtime.store().clone(),
            config.genesis.config.genesis_height,
            config.client_config.save_trie_changes,
        );
        let mut network_chain_store = ChainStore::new(
            mock_network_runtime.store().clone(),
            config.genesis.config.genesis_height,
            config.client_config.save_trie_changes,
        );

        let network_tail_height = network_chain_store.tail().unwrap();
        let network_head_height = network_chain_store.head().unwrap().height;
        tracing::info!(target: "mock_node", network_tail_height, network_head_height, "network data chain");
        assert!(
            client_start_height <= network_head_height
                && client_start_height >= network_tail_height,
            "client start height {} is not within the network chain range [{}, {}]",
            client_start_height,
            network_tail_height,
            network_head_height
        );
        let hash = network_chain_store.get_block_hash_by_height(client_start_height).unwrap();
        tracing::info!(target: "mock_node", "Checking whether the given start height is the last block of an epoch.");
        if !mock_network_epoch_manager.is_next_block_epoch_start(&hash).unwrap() {
            let epoch_start_height =
                mock_network_epoch_manager.get_epoch_start_height(&hash).unwrap();
            panic!(
                "start height must be the last block of an epoch, try using {} instead.",
                epoch_start_height - 1
            );
        }

        // copy chain info
        let chain_store_update = ChainStoreUpdate::copy_chain_state_as_of_block(
            &mut chain_store,
            &hash,
            mock_network_epoch_manager.as_ref(),
            &mut network_chain_store,
        )
        .unwrap();
        chain_store_update.commit().unwrap();
        tracing::info!(target: "mock_node", "Done preparing chain state");

        client_epoch_manager
            .write()
            .copy_epoch_info_as_of_block(&hash, &mock_network_epoch_manager.read())
            .unwrap();
        tracing::info!(target: "mock_node", "Done preparing epoch info");

        // copy state for all shards
        let block = network_chain_store.get_block(&hash).unwrap();
        let prev_hash = *block.header().prev_hash();
        for (shard_id, chunk_header) in block.chunks().iter().enumerate() {
            let shard_id = shard_id as u64;
            let state_root = chunk_header.prev_state_root();
            let state_root_node =
                mock_network_runtime.get_state_root_node(shard_id, &hash, &state_root).unwrap();
            let num_parts = get_num_state_parts(state_root_node.memory_usage);
            let finished_parts_count = Arc::new(AtomicUsize::new(0));
            tracing::info!(target: "mock_node", ?shard_id, ?state_root, num_parts, "Preparing state for a shard");

            (0..num_parts)
                .into_par_iter()
                .try_for_each(|part_id| -> anyhow::Result<()> {
                    let _span = tracing::debug_span!(
                        target: "mock_node",
                        parent: &parent_span,
                        "obtain_and_apply_state_part",
                        part_id,
                        shard_id)
                    .entered();

                    let state_part = mock_network_runtime
                        .obtain_state_part(
                            shard_id,
                            &prev_hash,
                            &state_root,
                            PartId::new(part_id, num_parts),
                        )
                        .with_context(|| {
                            format!("Obtaining state part {} in shard {}", part_id, shard_id)
                        })?;
                    client_runtime
                        .apply_state_part(
                            shard_id,
                            &state_root,
                            PartId::new(part_id, num_parts),
                            &state_part,
                            &mock_network_epoch_manager.get_epoch_id_from_prev_block(&hash)?,
                        )
                        .with_context(|| {
                            format!("Applying state part {} in shard {}", part_id, shard_id)
                        })?;
                    finished_parts_count.fetch_add(1, Ordering::SeqCst);
                    tracing::info!(
                        target: "mock_node",
                        "Done {}/{} parts for shard {}",
                        finished_parts_count.load(Ordering::SeqCst),
                        num_parts,
                        shard_id,
                    );
                    Ok(())
                })
                .unwrap();
        }
    }

    let chain = Chain::new_for_view_client(
        Clock::real(),
        mock_network_epoch_manager.clone(),
        mock_network_shard_tracker,
        mock_network_runtime,
        &chain_genesis,
        DoomslugThresholdMode::NoApprovals,
        config.client_config.save_trie_changes,
    )
    .unwrap();
    let head = chain.head().unwrap();
    let target_height = min(target_height.unwrap_or(head.height), head.height);
    let num_shards =
        mock_network_epoch_manager.shard_ids(&head.epoch_id).unwrap().len() as NumShards;

    config.network_config.peer_store.boot_nodes.clear();
    let mock_peer = setup_mock_peer(
        chain,
        &mut config,
        network_start_height,
        network_config.clone(),
        target_height,
        num_shards,
        mock_listen_addr,
    );

    let rpc_client = near_jsonrpc_client::new_client(&format!(
        "http://{}",
        &config.rpc_config.as_ref().expect("the JSON RPC config must be set").addr
    ));
    let _node = nearcore::start_with_config(client_home_dir, config).unwrap();

    MockNode { target_height, mock_peer, rpc_client }
}
