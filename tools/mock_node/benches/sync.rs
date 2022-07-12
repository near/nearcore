#[macro_use]
extern crate criterion;

use actix::System;
use anyhow::anyhow;
use criterion::Criterion;
use flate2::read::GzDecoder;
use mock_node::setup::{setup_mock_node, MockNode};
use mock_node::MockNetworkConfig;
use near_actix_test_utils::{block_on_interruptible, setup_actix};
use near_chain_configs::GenesisValidationMode;
use near_client::GetBlock;
use near_crypto::{InMemorySigner, KeyType};
use near_primitives::types::BlockHeight;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tar::Archive;

// The point of this is to return this struct from the benchmark
// function so that we can run the code in its drop() function after the
// benchmark has been measured. It would be possible to just run all
// that code in the benchmark function, but we don't really want to
// include that in the measurements.
struct Sys {
    sys: Option<actix_rt::SystemRunner>,
    servers: Vec<(&'static str, actix_web::dev::ServerHandle)>,
}

impl Drop for Sys {
    fn drop(&mut self) {
        // TODO: we only have to do this because shutdown is not well handled right now. Ideally we would not have
        // to tear down the whole system, and could just stop the client actor/view client actors each time.
        let system = System::current();
        let sys = self.sys.take().unwrap();

        sys.block_on(async move {
            futures::future::join_all(self.servers.iter().map(|(_name, server)| async move {
                server.stop(true).await;
            }))
            .await;
        });
        system.stop();
        sys.run().unwrap();
        near_store::db::RocksDB::block_until_all_instances_are_dropped();
    }
}

/// "./benches/empty.tar.gz" -> "empty"
fn test_name(home_archive: &str) -> &str {
    Path::new(home_archive.strip_suffix(".tar.gz").unwrap()).file_name().unwrap().to_str().unwrap()
}

/// "./benches/empty.tar.gz" -> "/tmp/near_mock_node_sync_empty"
fn extracted_path(home_archive: &str) -> anyhow::Result<PathBuf> {
    if !home_archive.ends_with(".tar.gz") {
        return Err(anyhow!("{} doesn't end with .tar.gz", home_archive));
    }
    let mut ret = PathBuf::from("/tmp");
    let dir_name = String::from("near_mock_node_sync_") + test_name(home_archive);
    ret.push(dir_name);
    Ok(ret)
}

// Expects home_archive to be a gzipped tar archive and extracts
// it to a /tmp directory if not already extracted.
fn extract_home(home_archive: &str) -> anyhow::Result<PathBuf> {
    let extracted = extracted_path(home_archive)?;
    if extracted.exists() {
        return Ok(extracted);
    }

    let tar_gz = File::open(home_archive)?;
    let tar = GzDecoder::new(tar_gz);
    let mut archive = Archive::new(tar);
    archive.unpack(&extracted)?;
    Ok(extracted)
}

// Sets up a mock node with the extracted contents of `home_archive` serving as the equivalent
// to the `chain_history_home_dir` argument to the `start_mock_node` tool, and measures the time
// taken to sync to target_height.
fn do_bench(c: &mut Criterion, home_archive: &str, target_height: Option<BlockHeight>) {
    let name = String::from("mock_node_sync_") + test_name(home_archive);
    let mut group = c.benchmark_group(name.clone());
    // The default of 100 is way too big for the longer running ones, and 10 is actually the minimum allowed.
    group.sample_size(10);
    group.bench_function(name, |bench| {
        bench.iter_with_setup(|| {
            let home = extract_home(home_archive).unwrap();
            let mut near_config = nearcore::config::load_config(home.as_path(), GenesisValidationMode::Full)
                .unwrap_or_else(|e| panic!("Error loading config: {:#}", e));
            near_config.validator_signer = None;
            near_config.client_config.min_num_peers = 1;
            let signer = InMemorySigner::from_random("mock_node".parse().unwrap(), KeyType::ED25519);
            near_config.network_config.node_key = signer.secret_key;
            near_config.client_config.tracked_shards =
                (0..near_config.genesis.config.shard_layout.num_shards()).collect();
            (setup_actix(), near_config, home)
        },
        |(sys, near_config, home)| {
            let tempdir = tempfile::Builder::new().prefix("mock_node").tempdir().unwrap();
            let network_config = MockNetworkConfig::with_delay(Duration::from_millis(100));
            let servers = block_on_interruptible(&sys, async move {
                let MockNode {view_client, servers, target_height, ..} = setup_mock_node(
                    tempdir.path(),
                    home.as_path(),
                    near_config,
                    &network_config,
                    0,
                    None,
                    target_height,
                    false,
                );

                let started = Instant::now();
                loop {
                    // TODO: make it so that we can just get notified when syncing has finished instead
                    // of asking over and over.
                    if let Ok(Ok(block)) = view_client.send(GetBlock::latest()).await {
                        if block.header.height >= target_height {
                            break;
                        }
                    }
                    if started.elapsed() > Duration::from_secs(target_height * 5) {
                        panic!("mock_node sync bench timed out with home dir {:?}, target height {:?}", &home, target_height);
                    }
                }
                servers
            });
            Sys{sys: Some(sys), servers: servers.unwrap()}
        })
    });
    group.finish();
}

fn sync_empty_chunks(c: &mut Criterion) {
    do_bench(c, "./benches/empty.tar.gz", Some(100))
}

fn sync_full_chunks(c: &mut Criterion) {
    do_bench(c, "./benches/full.tar.gz", Some(100))
}

criterion_group!(benches, sync_empty_chunks, sync_full_chunks);
criterion_main!(benches);
