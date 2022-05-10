use actix::{Actor, System};
use clap::Parser;
use futures::{future, FutureExt};
use std::path::Path;

use mock_node::setup::{setup_mock_node, MockNetworkMode};
use mock_node::GetChainTargetBlockHeight;
use near_actix_test_utils::run_actix;
use near_chain_configs::GenesisValidationMode;
use near_client::GetBlock;
use near_crypto::{InMemorySigner, KeyType};
use near_logger_utils::init_integration_logger;
use near_network::test_utils::WaitOrTimeoutActor;
use near_primitives::types::BlockHeight;
use std::time::Duration;

/// Program to start a mock node, which runs a regular client in a mock network environment.
/// The mock network simulates the entire network by reading a pre-generated chain history
/// on storage and responds to the client's network requests.
/// The binary runs in two modes, NoNewBlocks and ProduceNewBlocks, determined by flag `mode`.
///
/// In NoNewBlocks mode, the mock network will not simulate block production.
/// The simulated peers will start from the target height and no new blocks are produced.
///
/// In ProduceNewBlocks mode, new blocks will be produced, i.e., the client will receive new
/// blocks from the mock network. User can provide a number in this mode, which will be the starting
/// height the new produced blocks. If no number is provided, the mock network will start at
/// the same height as the client, which is specified by client_start_height.
///
/// Example commands:
/// start_mock_node ~/.near/localnet/node0 -h 100 --mode no_new_blocks
///
/// Client starts at genesis height and mock network starts at height 100. No new blocks will be produced.
/// The simulated peers stay at height 100.
///
/// start_mock_node ~/.near/localnet/node0 -s 61 -h 100 --mode produce_new_blocks
///
/// Both client and mock network starts at height 61, mock network will produce new blocks until height 100
///  
/// start_mock_node ~/.near/localnet/node0 -h 100 --mode "produce_new_blocks(20)"
///
/// Client starts at genesis height and mock network starts at heigh 20,
/// mock network will produce new blocks until height 100
#[derive(Parser)]
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
    /// Mode of the mock network, choices: [no_new_blocks, produce_new_blocks, produce_new_blocks(u64)]
    #[clap(short = 'M', long)]
    mode: MockNetworkMode,
    /// If specified, the binary will set up client home dir before starting the client node
    /// so head of the client chain will be the specified height when the client starts.
    /// The given height must be the last block in an epoch.
    #[clap(short = 's', long)]
    client_start_height: Option<BlockHeight>,
    /// Target height that the client should sync to before stopping. If not specified,
    /// use the height of the last block in chain history
    #[clap(short = 'h', long)]
    target_height: Option<BlockHeight>,
    /// If true, use in memory storage instead of rocksdb for the client
    #[clap(short = 'i', long)]
    in_memory_storage: bool,
}

fn main() {
    init_integration_logger();
    let args = Cli::parse();
    let home_dir = Path::new(&args.chain_history_home_dir);
    let mut near_config = nearcore::config::load_config(home_dir, GenesisValidationMode::Full)
        .unwrap_or_else(|e| panic!("Error loading config: {:#}", e));
    near_config.validator_signer = None;
    near_config.client_config.min_num_peers = 1;
    let signer = InMemorySigner::from_random("mock_node".parse().unwrap(), KeyType::ED25519);
    near_config.network_config.public_key = signer.public_key;
    near_config.network_config.secret_key = signer.secret_key;
    near_config.client_config.tracked_shards =
        (0..near_config.genesis.config.shard_layout.num_shards()).collect();

    let tempdir = tempfile::Builder::new().prefix("mock_node").tempdir().unwrap();
    let client_home_dir =
        args.client_home_dir.unwrap_or(String::from(tempdir.path().to_str().unwrap()));
    let network_delay = Duration::from_millis(args.network_delay);
    run_actix(async move {
        let (mock_network, _client, view_client) = setup_mock_node(
            Path::new(&client_home_dir),
            home_dir,
            near_config,
            args.mode,
            network_delay,
            args.client_start_height,
            args.target_height,
            args.in_memory_storage,
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
            // Let's set the timeout to 5 seconds per block - just in case we test on very full blocks.
            target_height * 5000,
        )
        .start();
    })
}
