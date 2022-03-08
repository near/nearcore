extern crate integration_tests;

use actix::{Actor, System};
use clap::Clap;
use futures::{future, FutureExt};
use std::path::Path;

use integration_tests::mock_network::setup::{setup_mock_network, SyncMode};
use integration_tests::mock_network::GetChainTargetBlockHeight;
use near_actix_test_utils::run_actix;
use near_chain_configs::GenesisValidationMode;
use near_client::GetBlock;
use near_crypto::{InMemorySigner, KeyType};
use near_logger_utils::init_integration_logger;
use near_network::test_utils::WaitOrTimeoutActor;
use near_primitives::types::BlockHeight;
use std::time::Duration;

/// Program to start a testing environment for one client and a mock network environment
/// The mock network simulates the entire network by reading a pre-generated chain history
/// on storage and responds to the client's network requests.
/// The binary runs in two modes, Sync and NoSync, determined by flag `sync`.
///
/// In Sync mode, the client and mock network start from different height and client is trying
/// to catch up. The client starts from genesis height, and the mock network starts from
/// target height (see args.target_height) and no new blocks are produced.
///
/// In NoSync mode, the client and mock network start at the same height and new blocks will
/// be produced. They both start from genesis height and the mock network will produce
/// blocks until target height.
#[derive(Clap)]
struct Cli {
    /// Existing home dir for the pre-generated chain history. For example, you can use
    /// the home dir of a near node.
    chain_history_home_dir: String,
    /// Home dir for the new client that will be started. If not specified, the binary will
    /// generate a temporary directory
    client_home_dir: Option<String>,
    /// Simulated network delay (in ms)
    #[clap(short = 'd', long, default_value = "100")]
    network_delay: u64,
    /// Sync mode of the binary
    #[clap(short = 'S', long)]
    sync: bool,
    /// If specified, the binary will set up client home dir before starting the client node
    /// so head of the client chain will be the specified height when the client starts
    #[clap(short = 's', long)]
    client_start_height: Option<BlockHeight>,
    /// Target height that the client should sync to before stopping. If not specified,
    /// use the height of the last block in chain history
    #[clap(short = 'h', long)]
    target_height: Option<BlockHeight>,
}

fn main() {
    init_integration_logger();
    let args = Cli::parse();
    let home_dir = Path::new(&args.chain_history_home_dir);
    let mut near_config = nearcore::config::load_config(home_dir, GenesisValidationMode::Full);
    near_config.validator_signer = None;
    near_config.client_config.min_num_peers = 1;
    let signer =
        InMemorySigner::from_random("mock_network_node".parse().unwrap(), KeyType::ED25519);
    near_config.network_config.public_key = signer.public_key;
    near_config.network_config.secret_key = signer.secret_key;
    near_config.client_config.tracked_shards =
        (0..near_config.genesis.config.shard_layout.num_shards()).collect();

    let tempdir = tempfile::Builder::new().prefix("mock_network_node").tempdir().unwrap();
    let client_home_dir =
        args.client_home_dir.unwrap_or(String::from(tempdir.path().to_str().unwrap()));
    let sync_mode = if args.sync { SyncMode::Sync } else { SyncMode::NoSync };
    let network_delay = Duration::from_millis(args.network_delay);
    run_actix(async move {
        let (mock_network, _client, view_client) = setup_mock_network(
            Path::new(&client_home_dir),
            home_dir,
            &near_config,
            sync_mode,
            network_delay,
            args.client_start_height,
            args.target_height,
        );
        let target_height =
            async { mock_network.send(GetChainTargetBlockHeight).await }.await.unwrap();
        // wait until the client reach target_height
        WaitOrTimeoutActor::new(
            Box::new(move |_ctx| {
                actix::spawn(view_client.send(GetBlock::latest()).then(move |res| {
                    if let Ok(Ok(block)) = res {
                        if block.header.height >= target_height {
                            System::current().stop()
                        }
                    }
                    future::ready(())
                }));
            }),
            100,
            target_height * 1000,
        )
        .start();
    })
}
