#[macro_use]
extern crate criterion;

use actix::System;
use criterion::Criterion;
use mock_node::setup::{setup_mock_node, MockNetworkMode};
use mock_node::GetChainTargetBlockHeight;
use near_actix_test_utils::{block_on_interruptible, setup_actix};
use near_chain_configs::GenesisValidationMode;
use near_client::GetBlock;
use near_crypto::{InMemorySigner, KeyType};
use near_primitives::types::BlockHeight;
use std::path::Path;
use std::time::{Duration, Instant};

fn do_bench(c: &mut Criterion, home: &str, target_height: Option<BlockHeight>) {
    let mut group = c.benchmark_group("mock_node_sync");
    group.sample_size(10);
    group.bench_function("mock_node_sync", |bench| {
        bench.iter_with_setup(|| {
            // TODO: we only have to do this because shutdown is not well handled right now. Ideally we would not have
            // to tear down the whole system, and could just stop the client actor/view client actor each time.
            if let Some(sys) = System::try_current() {
                sys.stop();
                near_store::db::RocksDB::block_until_all_instances_are_dropped();
            }
            setup_actix()
        },
        |sys| {
            let home_dir = Path::new(home);
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
            block_on_interruptible(&sys, async move {
                let (mock_network, _client, view_client) = setup_mock_node(
                    tempdir.path(),
                    home_dir,
                    near_config,
                    MockNetworkMode::NoNewBlocks,
                    Duration::from_millis(100),
                    None,
                    target_height,
                    false,
                );
                let target_height =
                    mock_network.send(GetChainTargetBlockHeight).await.unwrap();

                let started = Instant::now();
                loop {
                    // TODO: make it so that we can just get notified when syncing has finished instead
                    // of asking over and over.
                    if let Ok(Ok(block)) = view_client.send(GetBlock::latest()).await {
                        if block.header.height >= target_height {
                            break;
                        }
                    }
                    if started.elapsed() > Duration::from_millis(target_height * 5000) {
                        tracing::error!("mock_node sync bench timed out with home dir {}, target height {:?}", home, target_height);
                    }
                }
            })
        })
    });
    group.finish();
}

fn sync_full_chunks(c: &mut Criterion) {
    do_bench(c, "./benches/node_full", Some(100))
}

criterion_group!(benches, sync_full_chunks,);
criterion_main!(benches);
