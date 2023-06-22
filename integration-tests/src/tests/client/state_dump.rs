use assert_matches::assert_matches;
use near_chain::chain::ApplyStatePartsRequest;
use near_chain::types::RuntimeAdapter;
use near_chain::{ChainGenesis, Provenance};
use near_chain::near_chain_primitives::error::QueryError;
use near_chain_configs::ExternalStorageLocation::Filesystem;
use near_chain_configs::{DumpConfig, Genesis};
use near_client::sync::state::external_storage_location;
use near_client::test_utils::TestEnv;
use near_client::ProcessTxResponse;
use near_crypto::{InMemorySigner, KeyType, Signer};
use near_epoch_manager::{EpochManager, EpochManagerAdapter, EpochManagerHandle};
use near_network::test_utils::wait_or_timeout;
use near_o11y::testonly::init_test_logger;
use near_primitives::shard_layout::ShardUId;
use near_primitives::state_part::PartId;
use near_primitives::syncing::get_num_state_parts;
use near_primitives::syncing::StatePartKey;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::BlockHeight;
use near_primitives::views::{QueryRequest, QueryResponseKind};
use near_store::DBCol;
use near_store::{NodeStorage, Store};
use nearcore::config::GenesisExt;
use nearcore::state_sync::spawn_state_sync_dump;
use nearcore::{NightshadeRuntime, NEAR_BASE};
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

#[test]
fn test_data_after_state_sync_non_final() {
    //init_test_logger();
    let mut genesis = Genesis::test(vec!["test0".parse().unwrap()], 1);
    let epoch_length = 5;
    genesis.config.epoch_length = epoch_length;
    let chain_genesis = ChainGenesis::new(&genesis);

    near_actix_test_utils::run_actix(async {
        let num_clients = 2;
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

        let signer = InMemorySigner::from_seed("test0".parse().unwrap(), KeyType::ED25519, "test0");
        let genesis_block = env.clients[0].chain.get_block_by_height(0).unwrap();
        let genesis_hash = *genesis_block.hash();

        let mut blocks = vec![];
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

        for i in 1..=5 {
            let block = env.clients[0].produce_block(i).unwrap().unwrap();
            blocks.push(block.clone());
            env.process_block(0, block.clone(), Provenance::PRODUCED);
            env.process_block(1, block, Provenance::NONE);
        }
        assert!(env.clients[1]
            .chain
            .get_chunk_extra(blocks[4].hash(), &ShardUId::single_shard())
            .is_err());

        let tx = SignedTransaction::create_account(
            1,
            "test0".parse().unwrap(),
            "test_account".parse().unwrap(),
            NEAR_BASE,
            signer.public_key(),
            &signer,
            genesis_hash,
        );
        assert_eq!(env.clients[0].process_tx(tx, false, false), ProcessTxResponse::ValidTx);

        for i in 6..=10 {
            let block = env.clients[0].produce_block(i).unwrap().unwrap();
            blocks.push(block.clone());
            env.process_block(0, block.clone(), Provenance::PRODUCED);
            tracing::error!("process_block:{i}:0");
            if i == 6 {
                env.process_block(1, block, Provenance::NONE);
                tracing::error!("process_block:{i}:1");
            }
        }

        assert!(env.clients[1]
            .chain
            .get_chunk_extra(blocks[9].hash(), &ShardUId::single_shard())
            .is_err());
        // check that the new account exists
        let head = env.clients[0].chain.head().unwrap();
        let head_block = env.clients[0].chain.get_block(&head.last_block_hash).unwrap();
        let response = env.clients[0]
            .runtime_adapter
            .query(
                ShardUId::single_shard(),
                &head_block.chunks()[0].prev_state_root(),
                head.height,
                0,
                &head.prev_block_hash,
                &head.last_block_hash,
                head_block.header().epoch_id(),
                &QueryRequest::ViewAccount { account_id: "test_account".parse().unwrap() },
            )
            .unwrap();
        assert_matches!(response.kind, QueryResponseKind::ViewAccount(_));

        for i in 11..12 {
            let block = env.clients[0].produce_block(i).unwrap().unwrap();
            blocks.push(block.clone());
            env.process_block(0, block.clone(), Provenance::PRODUCED);
            tracing::error!("process_block:{i}:0");
            // env.process_block(1, block, Provenance::NONE);
            // tracing::error!("process_block:{i}:1");
        }

        assert!(env.clients[1]
            .chain
            .get_chunk_extra(blocks[10].hash(), &ShardUId::single_shard())
            .is_err());

        let head = env.clients[0].chain.head().unwrap();
        println!("client[0]'s head height {:?}", head.height);
        let header = env.clients[0].chain.get_block_header(&head.last_block_hash).unwrap();
        let final_block_hash = header.last_final_block();
        let final_block_header = env.clients[0].chain.get_block_header(final_block_hash).unwrap();
        let final_block_height = final_block_header.height();
        println!("client[0]'s final block height {:?}", final_block_height);

        let epoch_id = final_block_header.epoch_id().clone();
        let epoch_info = epoch_managers[0].get_epoch_info(&epoch_id).unwrap();
        let epoch_height = epoch_info.epoch_height();

        let sync_hash = *blocks[5].hash();
        assert_ne!(blocks[5].header().epoch_id(), blocks[4].header().epoch_id());
        assert!(env.clients[0].chain.check_sync_hash_validity(&sync_hash).unwrap());
        let state_sync_header =
            env.clients[0].chain.get_state_response_header(0, sync_hash).unwrap();
        let state_root = state_sync_header.chunk_prev_state_root();
        let state_root_node =
            env.clients[0].runtime_adapter.get_state_root_node(0, &sync_hash, &state_root).unwrap();
        let num_parts = get_num_state_parts(state_root_node.memory_usage);
        
        wait_or_timeout(100, 10000, || async {
            let mut all_parts_present = true;

            let num_shards = epoch_managers[0].num_shards(&epoch_id).unwrap();
            assert_ne!(num_shards, 0);

            for shard_id in 0..num_shards {
                for part_id in 0..num_parts {
                    let path = root_dir.path().join(external_storage_location(
                        &config.chain_id,
                        &epoch_id,
                        epoch_height,
                        shard_id,
                        part_id,
                        num_parts,
                    ));
                    if std::fs::read(&path).is_err() {
                        println!("Missing {:?}", path);
                        all_parts_present = false;
                    } else {
                        println!("Populated {:?}", path);
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

        // Simulate state sync

        let head = env.clients[1].chain.head().unwrap();
        println!("client[1]'s initial head height {:?}", head.height);

        env.clients[1].chain.set_state_header(0, sync_hash, state_sync_header).unwrap();
        let runtime_client_1 = Arc::clone(&env.clients[1].runtime_adapter);
        let runtime_client_0 = Arc::clone(&env.clients[0].runtime_adapter);
        let f = move |msg: ApplyStatePartsRequest| {
            use borsh::BorshSerialize;
            let client_0_store = runtime_client_0.store();

            for part_id in 0..msg.num_parts {
                let key = StatePartKey(msg.sync_hash, msg.shard_id, part_id).try_to_vec().unwrap();
                let part = client_0_store.get(DBCol::StateParts, &key).unwrap().unwrap();

                runtime_client_1
                    .apply_state_part(
                        msg.shard_id,
                        &msg.state_root,
                        PartId::new(part_id, msg.num_parts),
                        &part,
                        &msg.epoch_id,
                    )
                    .unwrap();
            }
        };
        env.clients[1].chain.schedule_apply_state_parts(0, sync_hash, num_parts, &f).unwrap();
        env.clients[1].chain.set_state_finalize(0, sync_hash, Ok(())).unwrap();
        let chunk_extra_after_sync = env.clients[1]
            .chain
            .get_chunk_extra(blocks[4].hash(), &ShardUId::single_shard())
            .unwrap();
        let expected_chunk_extra = env.clients[0]
            .chain
            .get_chunk_extra(blocks[4].hash(), &ShardUId::single_shard())
            .unwrap();
        // The chunk extra of the prev block of sync block should be the same as the node that it is syncing from
        assert_eq!(chunk_extra_after_sync, expected_chunk_extra);

        // check that the new account does not exist
        let head = env.clients[1].chain.head().unwrap();
        println!("client[1]'s head height {:?}", head.height);
        let head_block = env.clients[1].chain.get_block(&head.last_block_hash).unwrap();
        let response = env.clients[1]
            .runtime_adapter
            .query(
                ShardUId::single_shard(),
                &head_block.chunks()[0].prev_state_root(),
                head.height,
                0,
                &head.prev_block_hash,
                &head.last_block_hash,
                head_block.header().epoch_id(),
                &QueryRequest::ViewAccount { account_id: "test_account".parse().unwrap() },
            );
        assert!(response.is_err());
        assert_matches!(response.unwrap_err(), QueryError::UnknownAccount{ .. });

        actix_rt::System::current().stop();
    });
}

#[test]
fn test_data_after_state_sync() {
    //init_test_logger();
    let mut genesis = Genesis::test(vec!["test0".parse().unwrap()], 1);
    let epoch_length = 5;
    genesis.config.epoch_length = epoch_length;
    let chain_genesis = ChainGenesis::new(&genesis);

    near_actix_test_utils::run_actix(async {
        let num_clients = 2;
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

        let signer = InMemorySigner::from_seed("test0".parse().unwrap(), KeyType::ED25519, "test0");
        let genesis_block = env.clients[0].chain.get_block_by_height(0).unwrap();
        let genesis_hash = *genesis_block.hash();

        let mut blocks = vec![];

        for i in 1..=5 {
            let block = env.clients[0].produce_block(i).unwrap().unwrap();
            blocks.push(block.clone());
            env.process_block(0, block.clone(), Provenance::PRODUCED);
            env.process_block(1, block, Provenance::NONE);
        }

        let tx = SignedTransaction::create_account(
            1,
            "test0".parse().unwrap(),
            "test_account".parse().unwrap(),
            NEAR_BASE,
            signer.public_key(),
            &signer,
            genesis_hash,
        );
        assert_eq!(env.clients[0].process_tx(tx, false, false), ProcessTxResponse::ValidTx);

        for i in 6..=10 {
            let block = env.clients[0].produce_block(i).unwrap().unwrap();
            blocks.push(block.clone());
            env.process_block(0, block.clone(), Provenance::PRODUCED);
            tracing::error!("process_block:{i}:0");
            env.process_block(1, block, Provenance::NONE);
            tracing::error!("process_block:{i}:1");
        }
        // check that the new account exists
        let head = env.clients[0].chain.head().unwrap();
        let head_block = env.clients[0].chain.get_block(&head.last_block_hash).unwrap();
        let response = env.clients[0]
            .runtime_adapter
            .query(
                ShardUId::single_shard(),
                &head_block.chunks()[0].prev_state_root(),
                head.height,
                0,
                &head.prev_block_hash,
                &head.last_block_hash,
                head_block.header().epoch_id(),
                &QueryRequest::ViewAccount { account_id: "test_account".parse().unwrap() },
            )
            .unwrap();
        assert_matches!(response.kind, QueryResponseKind::ViewAccount(_));

        for i in 11..=15 {
            let block = env.clients[0].produce_block(i).unwrap().unwrap();
            blocks.push(block.clone());
            env.process_block(0, block.clone(), Provenance::PRODUCED);
            tracing::error!("process_block:{i}:0");
            env.process_block(1, block, Provenance::NONE);
            tracing::error!("process_block:{i}:1");
        }

        let chain = &env.clients[0].chain;
        let shard_tracker = chain.shard_tracker.clone();
        let mut config = env.clients[0].config.clone();
        let root_dir = tempfile::Builder::new().prefix("state_dump").tempdir().unwrap();
        config.state_sync.dump = Some(DumpConfig {
            location: Filesystem { root_dir: root_dir.path().to_path_buf() },
            restart_dump_for_shards: None,
            iteration_delay: Some(Duration::ZERO),
        });

        let head = env.clients[0].chain.head().unwrap();
        println!("client[0]'s head height {:?}", head.height);
        let header = env.clients[0].chain.get_block_header(&head.last_block_hash).unwrap();
        let final_block_hash = header.last_final_block();
        let final_block_header = env.clients[0].chain.get_block_header(final_block_hash).unwrap();
        let final_block_height = final_block_header.height();
        println!("client[0]'s final block height {:?}", final_block_height);

        let epoch_id = final_block_header.epoch_id().clone();
        let epoch_info = epoch_managers[0].get_epoch_info(&epoch_id).unwrap();
        let epoch_height = epoch_info.epoch_height();

        let _state_sync_dump_handle = spawn_state_sync_dump(
            &config,
            chain_genesis,
            epoch_managers[0].clone(),
            shard_tracker.clone(),
            runtimes[0].clone(),
            Some("test0".parse().unwrap()),
        )
        .unwrap();

        let sync_hash = *blocks[10].hash();
        assert_ne!(blocks[9].header().epoch_id(), blocks[10].header().epoch_id());
        assert!(env.clients[0].chain.check_sync_hash_validity(&sync_hash).unwrap());
        let state_sync_header =
            env.clients[0].chain.get_state_response_header(0, sync_hash).unwrap();
        let state_root = state_sync_header.chunk_prev_state_root();
        let state_root_node =
            env.clients[0].runtime_adapter.get_state_root_node(0, &sync_hash, &state_root).unwrap();
        let num_parts = get_num_state_parts(state_root_node.memory_usage);
        
        wait_or_timeout(100, 10000, || async {
            let mut all_parts_present = true;

            let num_shards = epoch_managers[0].num_shards(&epoch_id).unwrap();
            assert_ne!(num_shards, 0);

            for shard_id in 0..num_shards {
                for part_id in 0..num_parts {
                    let path = root_dir.path().join(external_storage_location(
                        &config.chain_id,
                        &epoch_id,
                        epoch_height,
                        shard_id,
                        part_id,
                        num_parts,
                    ));
                    if std::fs::read(&path).is_err() {
                        println!("Missing {:?}", path);
                        all_parts_present = false;
                    } else {
                        println!("Found {:?}", path);
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

        // Simulate state sync
        env.clients[1].chain.set_state_header(0, sync_hash, state_sync_header).unwrap();
        let runtime_client_1 = Arc::clone(&env.clients[1].runtime_adapter);
        let runtime_client_0 = Arc::clone(&env.clients[0].runtime_adapter);
        let f = move |msg: ApplyStatePartsRequest| {
            use borsh::BorshSerialize;
            let client_0_store = runtime_client_0.store();

            for part_id in 0..msg.num_parts {
                let key = StatePartKey(msg.sync_hash, msg.shard_id, part_id).try_to_vec().unwrap();
                let part = client_0_store.get(DBCol::StateParts, &key).unwrap().unwrap();

                runtime_client_1
                    .apply_state_part(
                        msg.shard_id,
                        &msg.state_root,
                        PartId::new(part_id, msg.num_parts),
                        &part,
                        &msg.epoch_id,
                    )
                    .unwrap();
            }
        };
        env.clients[1].chain.schedule_apply_state_parts(0, sync_hash, num_parts, &f).unwrap();
        env.clients[1].chain.set_state_finalize(0, sync_hash, Ok(())).unwrap();
        let chunk_extra_after_sync = env.clients[1]
            .chain
            .get_chunk_extra(blocks[9].hash(), &ShardUId::single_shard())
            .unwrap();
        let expected_chunk_extra = env.clients[0]
            .chain
            .get_chunk_extra(blocks[9].hash(), &ShardUId::single_shard())
            .unwrap();
        // The chunk extra of the prev block of sync block should be the same as the node that it is syncing from
        assert_eq!(chunk_extra_after_sync, expected_chunk_extra);

        // check that the new account exists
        let head = env.clients[1].chain.head().unwrap();
        let head_block = env.clients[1].chain.get_block(&head.last_block_hash).unwrap();
        let response = env.clients[1]
            .runtime_adapter
            .query(
                ShardUId::single_shard(),
                &head_block.chunks()[0].prev_state_root(),
                head.height,
                0,
                &head.prev_block_hash,
                &head.last_block_hash,
                head_block.header().epoch_id(),
                &QueryRequest::ViewAccount { account_id: "test_account".parse().unwrap() },
            )
            .unwrap();
        assert_matches!(response.kind, QueryResponseKind::ViewAccount(_));

        actix_rt::System::current().stop();
    });
}
