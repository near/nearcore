//! Provides functions for setting up a mock network from configs and home dirs.

use crate::{MockNetworkConfig, MockNode};
use anyhow::Context;
use near_chain::{Chain, ChainGenesis, DoomslugThresholdMode};
use near_chain_configs::GenesisValidationMode;
use near_epoch_manager::shard_tracker::ShardTracker;
use near_epoch_manager::{EpochManager, EpochManagerAdapter};
use near_network::tcp;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::types::BlockHeight;
use near_primitives::version::ProtocolVersion;
use near_store::adapter::StoreAdapter;
use near_store::adapter::chain_store::ChainStoreAdapter;

use near_time::Clock;

use nearcore::{NearConfig, NightshadeRuntime, NightshadeRuntimeExt};

use std::cmp::min;
use std::path::Path;
use std::sync::Arc;

/// Starts a mock server listening on the addr specified in `config`
/// The `archival` field does not refer to whether the database is archival
/// (which is still controlled by `config`), but tells whether the mock server should
/// advertise itself as archival in handshakes with peers. We might want to use the mock
/// server with clients with `state_sync_enabled=false` in the config and with a head
/// more than one epoch behind ours. In that case, if the client believes we're not archival,
/// it wont send us any chunk part requests for old chunks.
pub(crate) fn setup_mock_peer(
    chain: Chain,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    config: NearConfig,
    network_start_height: Option<BlockHeight>,
    network_config: MockNetworkConfig,
    target_height: BlockHeight,
    shard_layout: ShardLayout,
    handshake_protocol_version: Option<ProtocolVersion>,
    archival: bool,
) -> tokio::task::JoinHandle<anyhow::Result<()>> {
    let network_start_height = match network_start_height {
        None => target_height,
        Some(0) => chain.genesis_block().header().height(),
        Some(it) => it,
    };
    let secret_key = config.network_config.node_key;
    let chain_id = config.genesis.config.chain_id;
    let block_production_delay = config.client_config.min_block_production_delay;
    let listen_addr = match config.network_config.node_addr {
        Some(a) => a,
        None => tcp::ListenerAddr::new("127.0.0.1".parse().unwrap()),
    };
    let mock_peer = tokio::spawn(async move {
        let mock = MockNode::new(
            ChainStoreAdapter::new(chain.chain_store().store()),
            epoch_manager,
            *chain.genesis().hash(),
            secret_key,
            listen_addr,
            chain_id,
            archival,
            block_production_delay.unsigned_abs(),
            shard_layout,
            network_start_height,
            network_config,
            handshake_protocol_version,
        )?;
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
    handshake_protocol_version: Option<ProtocolVersion>,
    archival: bool,
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
        near_config.client_config.tracked_shards_config.clone(),
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
        epoch_manager,
        near_config,
        network_start_height,
        network_config,
        target_height,
        shard_layout,
        handshake_protocol_version,
        archival,
    ))
}
