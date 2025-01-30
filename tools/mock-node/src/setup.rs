//! Provides functions for setting up a mock network from configs and home dirs.

use crate::{MockNetworkConfig, MockPeer};
use anyhow::Context;
use near_chain::{Chain, ChainGenesis, DoomslugThresholdMode};
use near_chain_configs::GenesisValidationMode;
use near_epoch_manager::shard_tracker::{ShardTracker, TrackedConfig};
use near_epoch_manager::{EpochManager, EpochManagerAdapter};
use near_network::tcp;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::types::BlockHeight;
use near_time::Clock;
use nearcore::{NearConfig, NightshadeRuntime, NightshadeRuntimeExt};
use std::cmp::min;
use std::path::Path;

pub(crate) fn setup_mock_peer(
    chain: Chain,
    config: NearConfig,
    network_start_height: Option<BlockHeight>,
    network_config: MockNetworkConfig,
    target_height: BlockHeight,
    shard_layout: ShardLayout,
) -> tokio::task::JoinHandle<anyhow::Result<()>> {
    let network_start_height = match network_start_height {
        None => target_height,
        Some(0) => chain.genesis_block().header().height(),
        Some(it) => it,
    };
    let secret_key = config.network_config.node_key;
    let chain_id = config.genesis.config.chain_id;
    let block_production_delay = config.client_config.min_block_production_delay;
    let archival = config.client_config.archive;
    let listen_addr = match config.network_config.node_addr {
        Some(a) => a,
        None => tcp::ListenerAddr::new("127.0.0.1".parse().unwrap()),
    };
    let mock_peer = actix::spawn(async move {
        let mock = MockPeer::new(
            chain,
            secret_key,
            listen_addr,
            chain_id,
            archival,
            block_production_delay.unsigned_abs(),
            shard_layout,
            network_start_height,
            network_config,
        )
        .await?;
        mock.run(target_height).await
    });
    mock_peer
}

/// Set up a mock node, which will read blocks and chunks from the DB at
/// `home_dir`, and provide them to any node that connects. If `network_start_height` is
/// Some(), it will first send a block at that height, so that the connecting node sees
/// that its head is at that height, and it will then send higher heights periodically.
/// If target_height is Some(), it will not send any blocks or chunks of higher height.
pub fn setup_mock_node(
    home_dir: &Path,
    network_config: MockNetworkConfig,
    network_start_height: Option<BlockHeight>,
    target_height: Option<BlockHeight>,
) -> anyhow::Result<tokio::task::JoinHandle<anyhow::Result<()>>> {
    let near_config = nearcore::config::load_config(home_dir, GenesisValidationMode::Full)
        .context("Error loading config")?;

    let store = near_store::NodeStorage::opener(
        home_dir,
        &near_config.config.store,
        near_config.config.archival_config(),
    )
    .open()
    .context("failed opening storage")?
    .get_hot_store();
    let epoch_manager =
        EpochManager::new_arc_handle(store.clone(), &near_config.genesis.config, Some(home_dir));
    let shard_tracker = ShardTracker::new(
        TrackedConfig::from_config(&near_config.client_config),
        epoch_manager.clone(),
    );
    let runtime =
        NightshadeRuntime::from_config(home_dir, store, &near_config, epoch_manager.clone())
            .context("could not create transaction runtime")?;

    let chain_genesis = ChainGenesis::new(&near_config.genesis.config);
    let chain = Chain::new_for_view_client(
        Clock::real(),
        epoch_manager.clone(),
        shard_tracker,
        runtime,
        &chain_genesis,
        DoomslugThresholdMode::NoApprovals,
        near_config.client_config.save_trie_changes,
    )
    .context("failed creating Chain")?;

    let head = chain.head().context("failed getting chain head")?;
    let epoch_id = head.epoch_id;
    let shard_layout = epoch_manager.get_shard_layout(&epoch_id).unwrap();

    let target_height = min(target_height.unwrap_or(head.height), head.height);

    Ok(setup_mock_peer(
        chain,
        near_config,
        network_start_height,
        network_config,
        target_height,
        shard_layout,
    ))
}
