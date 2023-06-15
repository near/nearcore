use near_chain::types::RuntimeAdapter;
use near_chain::{ChainGenesis, Provenance};
use near_chain_configs::ExternalStorageLocation::Filesystem;
use near_chain_configs::{DumpConfig, Genesis};
use near_client::sync::state::external_storage_location;
use near_client::test_utils::TestEnv;
use near_epoch_manager::{EpochManager, EpochManagerAdapter, EpochManagerHandle};
use near_network::test_utils::wait_or_timeout;
use near_o11y::testonly::init_test_logger;
use near_primitives::types::BlockHeight;
use near_store::{NodeStorage, Store};
use nearcore::config::GenesisExt;
use nearcore::state_sync::spawn_state_sync_dump;
use nearcore::NightshadeRuntime;
use std::ops::ControlFlow;
use std::sync::Arc;
use std::time::Duration;

#[test]
/// Produce several blocks, wait for the state dump thread to notice and
/// write files to a temp dir.
fn test_state_dump() {
    init_test_logger();

    let mut genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
    genesis.config.epoch_length = 25;

    near_actix_test_utils::run_actix(async {
        let num_clients = 1;
        let env_objects = (0..num_clients).map(|_|{
            let tmp_dir = tempfile::tempdir().unwrap();
            // Use default StoreConfig rather than NodeStorage::test_opener so we’re using the
            // same configuration as in production.
            let store = NodeStorage::opener(&tmp_dir.path(), false, &Default::default(), None)
                .open()
                .unwrap()
                .get_hot_store();
            let epoch_manager = EpochManager::new_arc_handle(store.clone(), &genesis.config);
            let runtime =
                NightshadeRuntime::test(tmp_dir.path(), store.clone(), &genesis, epoch_manager.clone())
                    as Arc<dyn RuntimeAdapter>;
            (tmp_dir, store, epoch_manager, runtime)
        }).collect::<Vec<(tempfile::TempDir, Store, Arc<EpochManagerHandle>, Arc<dyn RuntimeAdapter>)>>();

        let stores = env_objects.iter().map(|x| x.1.clone()).collect::<Vec<_>>();
        let epoch_managers = env_objects.iter().map(|x| x.2.clone()).collect::<Vec<_>>();
        let runtimes = env_objects.iter().map(|x| x.3.clone()).collect::<Vec<_>>();

        let mut env = TestEnv::builder(ChainGenesis::test())
            .clients_count(env_objects.len())
            .stores(stores.clone())
            .epoch_managers(epoch_managers.clone())
            .runtimes(runtimes.clone())
            .use_state_snapshots()
            .build();

        let chain_genesis = ChainGenesis::new(&genesis);

        let chain = &env.clients[0].chain;
        let shard_tracker = chain.shard_tracker.clone();
        let mut config = env.clients[0].config.clone();
        let root_dir = tempfile::Builder::new().prefix("state_dump").tempdir().unwrap();
        config.state_sync.dump = Some(DumpConfig {
            location: Filesystem { root_dir: root_dir.path().to_path_buf() },
            restart_dump_for_shards: None,
            iteration_delay: Some(Duration::ZERO),
        });

        let _state_sync_dump_handle = spawn_state_sync_dump(
            &config,
            chain_genesis,
            epoch_managers[0].clone(),
            shard_tracker.clone(),
            runtimes[0].clone(),
            Some("test0".parse().unwrap()),
        )
        .unwrap();

        const MAX_HEIGHT: BlockHeight = 37;
        for i in 1..=MAX_HEIGHT {
            let block = env.clients[0].produce_block(i as u64).unwrap().unwrap();
            env.process_block(0, block, Provenance::PRODUCED);
        }
        let head = &env.clients[0].chain.head().unwrap();
        let epoch_id = head.clone().epoch_id;
        let epoch_info = epoch_managers[0].get_epoch_info(&epoch_id).unwrap();
        let epoch_height = epoch_info.epoch_height();

        wait_or_timeout(100, 10000, || async {
            let mut all_parts_present = true;

            let num_shards = epoch_managers[0].num_shards(&epoch_id).unwrap();
            assert_ne!(num_shards, 0);

            for shard_id in 0..num_shards {
                let num_parts = 1;
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
