use near_chain::{ChainGenesis, Provenance};
use near_chain_configs::ExternalStorageLocation::Filesystem;
use near_chain_configs::{DumpConfig, Genesis};
use near_client::sync::state::external_storage_location;
use near_client::test_utils::TestEnv;
use near_epoch_manager::EpochManagerAdapter;
use near_network::test_utils::wait_or_timeout;
use near_o11y::testonly::init_test_logger;
use near_primitives::types::BlockHeight;
use near_store::test_utils::create_test_store;
use near_store::Store;
use nearcore::config::GenesisExt;
use nearcore::state_sync::spawn_state_sync_dump;
use std::ops::ControlFlow;
use std::sync::Arc;
use std::time::Duration;

use super::utils::TestEnvNightshadeSetupExt;

/// Setup environment with one Near client for testing.
fn setup_env(genesis: &Genesis, store: Store) -> (TestEnv, Arc<dyn EpochManagerAdapter>) {
    let chain_genesis = ChainGenesis::new(genesis);
    let env = TestEnv::builder(chain_genesis)
        .stores(vec![store])
        .real_epoch_managers(&genesis.config)
        .nightshade_runtimes(genesis)
        .build();
    let epoch_manager = env.clients[0].epoch_manager.clone();
    (env, epoch_manager)
}

#[test]
/// Produce several blocks, wait for the state dump thread to notice and
/// write files to a temp dir.
fn test_state_dump() {
    init_test_logger();

    let mut genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
    genesis.config.epoch_length = 5;
    let store = create_test_store();
    let (mut env, epoch_manager) = setup_env(&genesis, store);

    let chain_genesis = ChainGenesis::new(&genesis);

    let chain = &env.clients[0].chain;
    let epoch_manager = epoch_manager.clone();
    let shard_tracker = chain.shard_tracker.clone();
    let runtime = chain.runtime_adapter.clone();
    let mut config = env.clients[0].config.clone();
    let root_dir = tempfile::Builder::new().prefix("state_dump").tempdir().unwrap();
    config.state_sync.dump = Some(DumpConfig {
        location: Filesystem { root_dir: root_dir.path().to_path_buf() },
        restart_dump_for_shards: None,
        iteration_delay: Some(Duration::from_millis(250)),
    });

    const MAX_HEIGHT: BlockHeight = 37;

    near_actix_test_utils::run_actix(async move {
        let _state_sync_dump_handle = spawn_state_sync_dump(
            &config,
            chain_genesis,
            epoch_manager.clone(),
            shard_tracker.clone(),
            runtime.clone(),
            Some("test0".parse().unwrap()),
        )
        .unwrap();
        for i in 1..=MAX_HEIGHT {
            let block = env.clients[0].produce_block(i as u64).unwrap().unwrap();
            env.process_block(0, block, Provenance::PRODUCED);
        }
        let head = &env.clients[0].chain.head().unwrap();
        let epoch_id = head.clone().epoch_id;
        let epoch_info = epoch_manager.get_epoch_info(&epoch_id).unwrap();
        let epoch_height = epoch_info.epoch_height();

        wait_or_timeout(100, 10000, || async {
            let mut all_parts_present = true;

            let num_shards = epoch_manager.num_shards(&epoch_id).unwrap();
            assert_ne!(num_shards, 0);

            for shard_id in 0..num_shards {
                let num_parts = 3;
                for part_id in 0..num_parts {
                    let path = root_dir.path().join(external_storage_location(
                        "unittest",
                        &epoch_id,
                        epoch_height,
                        shard_id,
                        part_id,
                        num_parts,
                    ));
                    if std::fs::read(&path).is_err() {
                        println!("Missing {:?}", path);
                        all_parts_present = false;
                    }
                }
            }
            if all_parts_present {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        })
        .await
        .unwrap();
        actix_rt::System::current().stop();
    });
}
