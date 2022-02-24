extern crate integration_tests;

use actix::{Actor, System};
use clap::Clap;
use futures::{future, FutureExt};
use std::path::Path;

use futures::executor::block_on;
use integration_tests::mock_network::setup::setup_mock_network;
use integration_tests::mock_network::GetChainHistoryFinalBlockHeight;
use near_actix_test_utils::run_actix;
use near_chain_configs::GenesisValidationMode;
use near_client::GetBlock;
use near_crypto::{InMemorySigner, KeyType};
use near_logger_utils::init_integration_logger;
use near_network::test_utils::WaitOrTimeoutActor;

#[derive(Clap)]
struct Cli {
    chain_history_home_dir: String,
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

    let dir1 = tempfile::Builder::new().prefix("mock_network_node").tempdir().unwrap();
    run_actix(async move {
        let (mock_network, _client, view_client) =
            setup_mock_network(dir1.path().clone(), home_dir, &near_config);
        let chain_height =
            async { mock_network.send(GetChainHistoryFinalBlockHeight).await }.await.unwrap();
        println!("chain height {:?}", chain_height);
        WaitOrTimeoutActor::new(
            Box::new(move |_ctx| {
                actix::spawn(view_client.send(GetBlock::latest()).then(|res| {
                    if let Ok(Ok(block)) = res {
                        if block.header.height >= 20 {
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
