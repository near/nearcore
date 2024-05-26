use assert_matches::assert_matches;

use near_async::time::{Clock, Duration};
use near_chain::near_chain_primitives::error::QueryError;
use near_chain::{ChainGenesis, ChainStoreAccess, Provenance};
use near_chain_configs::ExternalStorageLocation::Filesystem;
use near_chain_configs::{DumpConfig, Genesis, NEAR_BASE};
use near_client::sync::external::{external_storage_location, StateFileType};
use near_client::test_utils::TestEnv;
use near_client::ProcessTxResponse;
use near_crypto::{InMemorySigner, KeyType, Signer};
use near_network::test_utils::wait_or_timeout;
use near_o11y::testonly::init_test_logger;
use near_primitives::block::Tip;
use near_primitives::shard_layout::ShardUId;
use near_primitives::state::FlatStateValue;
use near_primitives::state_part::PartId;
use near_primitives::state_sync::StatePartKey;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::BlockHeight;
use near_primitives::views::{QueryRequest, QueryResponseKind};
use near_store::flat::store_helper;
use near_store::DBCol;
use near_store::Store;
use nearcore::state_sync::StateSyncDumper;
use nearcore::test_utils::TestEnvNightshadeSetupExt;
use std::ops::ControlFlow;
use std::sync::Arc;

#[test]
/// Produce several blocks, wait for the state dump thread to notice and
/// write files to a temp dir.
fn test_state_dump() {
    init_test_logger();

    let mut genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
    genesis.config.epoch_length = 25;

    near_actix_test_utils::run_actix(async {
        let mut env = TestEnv::builder(&genesis.config)
            .clients_count(1)
            .use_state_snapshots()
            .real_stores()
            .nightshade_runtimes(&genesis)
            .build();

        let chain = &env.clients[0].chain;
        let epoch_manager = env.clients[0].epoch_manager.clone();
        let runtime = env.clients[0].runtime_adapter.clone();
        let shard_tracker = chain.shard_tracker.clone();
        let mut config = env.clients[0].config.clone();
        let root_dir = tempfile::Builder::new().prefix("state_dump").tempdir().unwrap();
        config.state_sync.dump = Some(DumpConfig {
            location: Filesystem { root_dir: root_dir.path().to_path_buf() },
            restart_dump_for_shards: None,
            iteration_delay: Some(Duration::ZERO),
            credentials_file: None,
        });

        let mut state_sync_dumper = StateSyncDumper {
            clock: Clock::real(),
            client_config: config.clone(),
            chain_genesis: ChainGenesis::new(&genesis.config),
            epoch_manager: epoch_manager.clone(),
            shard_tracker,
            runtime,
            account_id: Some("test0".parse().unwrap()),
            dump_future_runner: StateSyncDumper::arbiter_dump_future_runner(),
            handle: None,
        };
        state_sync_dumper.start().unwrap();

        const MAX_HEIGHT: BlockHeight = 37;
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

            let shard_ids = epoch_manager.shard_ids(&epoch_id).unwrap();
            assert_ne!(shard_ids.len(), 0);

            for shard_id in shard_ids {
                let num_parts = 1;
                for part_id in 0..num_parts {
                    let path = root_dir.path().join(external_storage_location(
                        "unittest",
                        &epoch_id,
                        epoch_height,
                        shard_id,
                        &StateFileType::StatePart { part_id, num_parts },
                    ));
                    if std::fs::read(&path).is_err() {
                        tracing::info!("Missing {:?}", path);
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

/// This function tests that after a node does state sync, it has the data that corresponds to the state of the epoch previous to the dumping node's final block.
/// The way the test works:
/// set up 2 nodes: env.client[0] dumps state parts, env.client[1] state syncs with the dumped state parts.
/// A new account will be created in the epoch at account_creation_at_epoch_height, specifically at 2nd block of the epoch.
/// if is_final_block_in_new_epoch = true, dumping node's final block and head will both be in the next epoch after account creation;
/// otherwise, dumping node's head will be in the next epoch while its final block would still be in the epoch of account creation.
/// The test verifies that if dumping node's final block is in the next epoch after account creation, then the syncing node will have the account information after state sync;
/// otherwise, e.g. the dumping node's final block is in the same epoch as account creation, the syncing node should not have the account info after state sync.
fn run_state_sync_with_dumped_parts(
    is_final_block_in_new_epoch: bool,
    account_creation_at_epoch_height: u64,
    epoch_length: u64,
) {
    init_test_logger();
    if is_final_block_in_new_epoch {
        tracing::info!("Testing for case when both head and final block of the dumping node are in new epoch...");
    } else {
        tracing::info!("Testing for case when head is in new epoch, but final block isn't for the dumping node...");
    }
    near_actix_test_utils::run_actix(async {
        let mut genesis = Genesis::test(vec!["test0".parse().unwrap()], 1);
        genesis.config.epoch_length = epoch_length;
        let num_clients = 2;
        let mut env = TestEnv::builder(&genesis.config)
            .clients_count(num_clients)
            .use_state_snapshots()
            .real_stores()
            .nightshade_runtimes(&genesis)
            .build();

        let signer: Signer = InMemorySigner::from_seed("test0".parse().unwrap(), KeyType::ED25519, "test0").into();
        let genesis_block = env.clients[0].chain.get_block_by_height(0).unwrap();
        let genesis_hash = *genesis_block.hash();

        let mut blocks = vec![];
        let chain = &env.clients[0].chain;
        let epoch_manager = env.clients[0].epoch_manager.clone();
        let runtime = env.clients[0].runtime_adapter.clone();
        let shard_tracker = chain.shard_tracker.clone();
        let mut config = env.clients[0].config.clone();
        let root_dir = tempfile::Builder::new().prefix("state_dump").tempdir().unwrap();
        config.state_sync.dump = Some(DumpConfig {
            location: Filesystem { root_dir: root_dir.path().to_path_buf() },
            restart_dump_for_shards: None,
            iteration_delay: Some(Duration::ZERO),
            credentials_file: None,
        });
        let mut state_sync_dumper = StateSyncDumper {
            clock: Clock::real(),
            client_config: config.clone(),
            chain_genesis: ChainGenesis::new(&genesis.config),
            epoch_manager: epoch_manager.clone(),
            shard_tracker,
            runtime,
            account_id: Some("test0".parse().unwrap()),
            dump_future_runner: StateSyncDumper::arbiter_dump_future_runner(),
            handle: None,
        };
        state_sync_dumper.start().unwrap();

        let account_creation_at_height = (account_creation_at_epoch_height - 1) * epoch_length + 2;

        let dump_node_head_height = if is_final_block_in_new_epoch {
            (1 + account_creation_at_epoch_height) * epoch_length
        } else {
            account_creation_at_epoch_height * epoch_length + 1
        };

        for i in 1..=dump_node_head_height {
            if i == account_creation_at_height {
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
            }
            let block = env.clients[0].produce_block(i).unwrap().unwrap();
            blocks.push(block.clone());
            env.process_block(0, block.clone(), Provenance::PRODUCED);
            env.process_block(1, block.clone(), Provenance::NONE);
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

        let header = env.clients[0].chain.get_block_header(&head.last_block_hash).unwrap();
        let final_block_hash = header.last_final_block();
        let final_block_header = env.clients[0].chain.get_block_header(final_block_hash).unwrap();

        tracing::info!(
            dump_node_head_height,
            final_block_height = final_block_header.height(),
            "Dumping node state"
        );

        // check if final block is in the same epoch as head for dumping node
        if is_final_block_in_new_epoch {
            assert_eq!(header.epoch_id().clone(), final_block_header.epoch_id().clone())
        } else {
            assert_ne!(header.epoch_id().clone(), final_block_header.epoch_id().clone())
        }

        let epoch_id = final_block_header.epoch_id().clone();
        let epoch_info = epoch_manager.get_epoch_info(&epoch_id).unwrap();
        let epoch_height = epoch_info.epoch_height();

        let sync_block_height = (epoch_length * epoch_height + 1) as usize;
        let sync_hash = *blocks[sync_block_height - 1].hash();

        // the block at sync_block_height should be the start of an epoch
        assert_ne!(
            blocks[sync_block_height - 1].header().epoch_id(),
            blocks[sync_block_height - 2].header().epoch_id()
        );
        assert!(env.clients[0].chain.check_sync_hash_validity(&sync_hash).unwrap());
        let state_sync_header =
            env.clients[0].chain.get_state_response_header(0, sync_hash).unwrap();
        let state_root = state_sync_header.chunk_prev_state_root();
        let num_parts = state_sync_header.num_state_parts();

        wait_or_timeout(100, 10000, || async {
            let mut all_parts_present = true;

            let shard_ids = epoch_manager.shard_ids(&epoch_id).unwrap();
            assert_ne!(shard_ids.len(), 0);

            for shard_id in shard_ids {
                for part_id in 0..num_parts {
                    let path = root_dir.path().join(external_storage_location(
                        &config.chain_id,
                        &epoch_id,
                        epoch_height,
                        shard_id,
                        &StateFileType::StatePart { part_id, num_parts },
                    ));
                    if std::fs::read(&path).is_err() {
                        tracing::info!("dumping node: Missing {:?}", path);
                        all_parts_present = false;
                    } else {
                        tracing::info!("dumping node: Populated {:?}", path);
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
        tracing::info!("syncing node: simulating state sync..");
        env.clients[1].chain.set_state_header(0, sync_hash, state_sync_header).unwrap();
        let runtime_client_1 = Arc::clone(&env.clients[1].runtime_adapter);
        let runtime_client_0 = Arc::clone(&env.clients[0].runtime_adapter);
        let client_0_store = runtime_client_0.store();
        let mut store_update = runtime_client_1.store().store_update();
        assert!(runtime_client_1
            .get_flat_storage_manager()
            .remove_flat_storage_for_shard(ShardUId::single_shard(), &mut store_update)
            .unwrap());
        store_update.commit().unwrap();

        for part_id in 0..num_parts {
            let key = borsh::to_vec(&StatePartKey(sync_hash, 0, part_id)).unwrap();
            let part = client_0_store.get(DBCol::StateParts, &key).unwrap().unwrap();

            runtime_client_1
                .apply_state_part(0, &state_root, PartId::new(part_id, num_parts), &part, &epoch_id)
                .unwrap();
        }
        env.clients[1].chain.set_state_finalize(0, sync_hash).unwrap();
        tracing::info!("syncing node: state sync finished.");

        let synced_block = env.clients[1].chain.get_block(&sync_hash).unwrap();
        let synced_block_header = env.clients[1].chain.get_block_header(&sync_hash).unwrap();
        let synced_block_tip = Tip::from_header(&synced_block_header);
        let response = env.clients[1].runtime_adapter.query(
            ShardUId::single_shard(),
            &synced_block.chunks()[0].prev_state_root(),
            synced_block_tip.height,
            0,
            &synced_block_tip.prev_block_hash,
            &synced_block_tip.last_block_hash,
            synced_block_header.epoch_id(),
            &QueryRequest::ViewAccount { account_id: "test_account".parse().unwrap() },
        );

        if is_final_block_in_new_epoch {
            tracing::info!(?response, "New Account should exist");
            assert_matches!(
                response.unwrap().kind,
                QueryResponseKind::ViewAccount(_),
                "the synced node should have information about the created account"
            );

            // Check that inlined flat state values remain inlined.
            {
                let store0 = env.clients[0].chain.chain_store().store();
                let store1 = env.clients[1].chain.chain_store().store();
                let (num_inlined_before, num_ref_before) = count_flat_state_value_kinds(store0);
                let (num_inlined_after, num_ref_after) = count_flat_state_value_kinds(store1);
                // Nothing new created, number of flat state values should be identical.
                assert_eq!(num_inlined_before, num_inlined_after);
                assert_eq!(num_ref_before, num_ref_after);
            }
        } else {
            tracing::info!(?response, "New Account shouldn't exist");
            assert!(response.is_err());
            assert_matches!(
                response.unwrap_err(),
                QueryError::UnknownAccount { .. },
                "the synced node should not have information about the created account"
            );

            // Check that inlined flat state values remain inlined.
            {
                let store0 = env.clients[0].chain.chain_store().store();
                let store1 = env.clients[1].chain.chain_store().store();
                let (num_inlined_before, _num_ref_before) = count_flat_state_value_kinds(store0);
                let (num_inlined_after, _num_ref_after) = count_flat_state_value_kinds(store1);
                // Created a new entry, but inlined values should stay inlinedNothing new created, number of flat state values should be identical.
                assert!(num_inlined_before >= num_inlined_after);
                assert!(num_inlined_after > 0);
            }
        }
        actix_rt::System::current().stop();
    });
}

#[test]
/// This test verifies that after state sync, the syncing node has the data that corresponds to the state of the epoch previous to the dumping node's final block.
/// Specifically, it tests that the above holds true in both conditions:
/// - the dumping node's head is in new epoch but final block is not;
/// - the dumping node's head and final block are in same epoch
fn test_state_sync_w_dumped_parts() {
    init_test_logger();
    let epoch_length = 5;
    // excluding account_creation_at_epoch_height=1 because first epoch's epoch_id not being block hash of its first block cause issues
    for account_creation_at_epoch_height in 2..=4 as u64 {
        tracing::info!("account_creation_at_epoch_height = {}", account_creation_at_epoch_height);
        run_state_sync_with_dumped_parts(false, account_creation_at_epoch_height, epoch_length);
        run_state_sync_with_dumped_parts(true, account_creation_at_epoch_height, epoch_length);
    }
}

fn count_flat_state_value_kinds(store: &Store) -> (u64, u64) {
    let mut num_inlined_values = 0;
    let mut num_ref_values = 0;
    for item in store_helper::iter_flat_state_entries(ShardUId::single_shard(), store, None, None) {
        match item {
            Ok((_, FlatStateValue::Ref(_))) => {
                num_ref_values += 1;
            }
            Ok((_, FlatStateValue::Inlined(_))) => {
                num_inlined_values += 1;
            }
            _ => {}
        }
    }
    (num_inlined_values, num_ref_values)
}
