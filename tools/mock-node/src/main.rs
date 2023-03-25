//! A binary that starts a mock testing environment for ClientActor. It
//! simulates the entire network by substituting PeerManagerActor with a mock
//! network, responding to the client's network requests by reading from a
//! pre-generated chain history in storage.

use actix::System;
use anyhow::Context;
use mock_node::setup::{setup_mock_node, MockNode};
use mock_node::MockNetworkConfig;
use near_actix_test_utils::run_actix;
use near_chain_configs::GenesisValidationMode;
use near_crypto::{InMemorySigner, KeyType};
use near_jsonrpc_client::JsonRpcClient;
use near_network::tcp;
use near_o11y::testonly::init_integration_logger;
use near_primitives::types::BlockHeight;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

/// Program to start a mock node, which runs a regular client in a mock network environment.
/// The mock network simulates the entire network by replaying a pre-generated chain history
/// from storage and responds to the client's network requests.
///
/// There are two ways to replay the stored history:
///   * catchup: client is behind the network and applies the blocks as fast as possible
///   * normal block production: client accept "new" blocks as they are produced
///     (in reality, blocks are just fetched from the pre-generated store).
///
/// This is controlled by two flags:
///   * `--client-height` specifies the height the client starts at. Defaults to 0.
///   * `--network-height` specifies the hight the rest of the (simulated)
///     network starts at. Defaults to the latest recorded height.
///
/// As a shortcut, `--start-height` sets both.
///
///
/// Examples
///
/// ```console
/// # Pure catchup from genesis height to the end of the recorded history.
/// $ mock-node ~/.near/localnet/node0
///
/// # Pure block production starting from block height 61.
/// $ mock-node ~/.near/localnet/node0 --start-height 61
///
/// # Mixed: client starts at genesis and tries to catch up with the network, which starts at height 20.
/// $ mock-node ~/.near/localnet/node0 --network-height 20
/// ```
#[derive(clap::Parser)]
struct Cli {
    /// Existing home dir for the pre-generated chain history. For example, you can use
    /// the home dir of a near node.
    chain_history_home_dir: String,
    /// Home dir for the new client that will be started. If not specified, the binary will
    /// generate a temporary directory
    client_home_dir: Option<PathBuf>,
    /// Simulated network delay (in ms)
    #[clap(short = 'd', long)]
    network_delay: Option<u64>,
    /// If specified, the binary will set up client home dir before starting the
    /// client node so head of the client chain will be the specified height
    /// when the client starts. The given height must be the last block in an
    /// epoch.
    #[clap(long, default_value = "0")]
    client_height: BlockHeight,
    /// The height at which the mock network starts. The client would have to
    /// catch up to this height before participating in new block production.
    ///
    /// Defaults to the largest height in history.
    #[clap(long)]
    network_height: Option<BlockHeight>,
    /// Shortcut to set both `--client-height` and `--network-height`.
    #[clap(long, conflicts_with_all(&["client-height", "network-height"]))]
    start_height: Option<BlockHeight>,
    /// Target height that the client should sync to before stopping. If not specified,
    /// use the height of the last block in chain history
    #[clap(long)]
    target_height: Option<BlockHeight>,
    /// If true, use in memory storage instead of rocksdb for the client
    #[clap(short = 'i', long)]
    in_memory_storage: bool,
    /// port the mock node should listen on
    #[clap(long)]
    mock_port: Option<u16>,
}

async fn target_height_reached(client: &JsonRpcClient, target_height: BlockHeight) -> bool {
    let t = Instant::now();
    let status = client.status().await;
    let latency = t.elapsed();
    if latency > Duration::from_millis(100) {
        tracing::warn!(
            target: "mock_node", latency = %format_args!("{latency:0.2?}"),
            "client is unresponsive, took too long to handle status request"
        );
    }
    match status {
        Ok(status) => status.sync_info.latest_block_height >= target_height,
        Err(_) => false,
    }
}

fn main() -> anyhow::Result<()> {
    init_integration_logger();
    let args: Cli = clap::Parser::parse();
    let home_dir = Path::new(&args.chain_history_home_dir);
    let mut near_config = nearcore::config::load_config(home_dir, GenesisValidationMode::Full)
        .context("Error loading config")?;
    near_config.validator_signer = None;
    near_config.client_config.min_num_peers = 1;
    let signer = InMemorySigner::from_random("mock_node".parse().unwrap(), KeyType::ED25519);
    near_config.network_config.node_key = signer.secret_key;
    near_config.client_config.tracked_shards =
        (0..near_config.genesis.config.shard_layout.num_shards()).collect();
    if near_config.rpc_config.is_none() {
        near_config.rpc_config = Some(near_jsonrpc::RpcConfig::default());
    }
    let tempdir;
    let client_home_dir = match &args.client_home_dir {
        Some(it) => it.as_path(),
        None => {
            tempdir = tempfile::Builder::new().prefix("mock_node").tempdir().unwrap();
            tempdir.path()
        }
    };

    let mock_config_path = home_dir.join("mock.json");
    let mut network_config = if mock_config_path.exists() {
        MockNetworkConfig::from_file(&mock_config_path).with_context(|| {
            format!("Error loading mock config from {}", mock_config_path.display())
        })?
    } else {
        MockNetworkConfig::default()
    };
    if let Some(delay) = args.network_delay {
        network_config.response_delay = Duration::from_millis(delay);
    }

    let client_height = args.start_height.unwrap_or(args.client_height);
    let network_height = args.start_height.or(args.network_height);
    let addr = tcp::ListenerAddr::new(SocketAddr::new(
        "127.0.0.1".parse().unwrap(),
        args.mock_port.unwrap_or(24566),
    ));

    run_actix(async move {
        let MockNode { target_height, mut mock_peer, rpc_client } = setup_mock_node(
            Path::new(&client_home_dir),
            home_dir,
            near_config,
            &network_config,
            client_height,
            network_height,
            args.target_height,
            args.in_memory_storage,
            addr,
        );

        // TODO: would be nice to be able to somehow quit right after the target block
        // is applied rather than polling like this
        let mut interval = tokio::time::interval(Duration::from_millis(100));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        let start = Instant::now();
        // Let's set the timeout to 5 seconds per block - just in case we test on very full blocks.
        let timeout = target_height * 5;
        let timeout = u32::try_from(timeout).unwrap_or(u32::MAX) * Duration::from_secs(1);

        loop {
            if start.elapsed() > timeout {
                tracing::error!(
                    "node still hasn't made it to #{} after {:?}",
                    target_height,
                    timeout
                );
                mock_peer.abort();
                break;
            }
            tokio::select! {
                _ = interval.tick() => {
                    if target_height_reached(&rpc_client, target_height).await {
                        tracing::info!("node reached target height");
                        mock_peer.abort();
                        break;
                    }
                }
                result = &mut mock_peer => {
                    match result {
                        Ok(Ok(_)) => tracing::info!("mock peer exited"),
                        Ok(Err(e)) => tracing::error!("mock peer exited with error: {:?}", e),
                        Err(e) => tracing::error!("failed running mock peer task: {:?}", e),
                    };
                    break;
                }
            }
        }

        System::current().stop();
    });
    Ok(())
}
