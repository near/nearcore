extern crate integration_tests;

use actix::{Actor, System};
use clap::Clap;
use futures::{future, FutureExt};
use std::path::Path;

use integration_tests::mock_network::setup::{setup_mock_network, SyncMode};
use integration_tests::mock_network::GetChainHistoryFinalBlockHeight;
use near_actix_test_utils::run_actix;
use near_chain_configs::GenesisValidationMode;
use near_client::GetBlock;
use near_crypto::{InMemorySigner, KeyType};
use near_logger_utils::init_integration_logger;
use near_network::test_utils::WaitOrTimeoutActor;

/// Program to start a testing environment for one client and a mock network environment
/// The mock network simulates the entire network by reading a pre-generated chain history
/// on storage and responds to the client's network requests.
#[derive(Clap)]
struct Cli {
    /// Existing home dir for the pre-generated chain history. For example, you can use
    /// the home dir of a near node.
    chain_history_home_dir: String,
    /// Home dir for the new client that will be started. If not specified, the binary will
    /// generate a temporary directory
    client_home_dir: Option<String>,
    /// If true, the mock network simulates the client in syncing mode, otherwise,
    /// the client and its simulated peers will all start from the genesis block
    #[clap(short, long)]
    sync: bool,
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

    let tempdir = tempfile::Builder::new().prefix("mock_network_node").tempdir().unwrap();
    let client_home_dir =
        args.client_home_dir.unwrap_or(String::from(tempdir.path().to_str().unwrap()));
    let sync_mode = if args.sync { SyncMode::Sync } else { SyncMode::NoSync };
    run_actix(async move {
        let (mock_network, _client, view_client) =
            setup_mock_network(Path::new(&client_home_dir), home_dir, &near_config, sync_mode);
        let chain_height =
            async { mock_network.send(GetChainHistoryFinalBlockHeight).await }.await.unwrap();
        WaitOrTimeoutActor::new(
            Box::new(move |_ctx| {
                actix::spawn(view_client.send(GetBlock::latest()).then(move |res| {
                    if let Ok(Ok(block)) = res {
                        if block.header.height >= chain_height {
                            System::current().stop()
                        }
                    }
                    future::ready(())
                }));
            }),
            100,
            60000,
        )
        .start();
    })
}
