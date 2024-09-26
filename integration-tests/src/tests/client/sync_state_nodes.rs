use actix::{Actor, System};
use futures::{future, FutureExt};
use near_actix_test_utils::run_actix;
use near_async::messaging::Sender;
use near_async::time::Duration;
use near_chain::chain::ApplyStatePartsRequest;
use near_chain::Provenance;
use near_chain_configs::ExternalStorageLocation::Filesystem;
use near_chain_configs::{DumpConfig, ExternalStorageConfig, Genesis, SyncConfig};
use near_client::test_utils::TestEnv;
use near_client::{GetBlock, ProcessTxResponse};
use near_client_primitives::types::GetValidatorInfo;
use near_crypto::{InMemorySigner, KeyType};
use near_network::client::{StateRequestHeader, StateRequestPart, StateResponse};
use near_network::tcp;
use near_network::test_utils::{convert_boot_nodes, wait_or_timeout, WaitOrTimeoutActor};
use near_o11y::testonly::{init_integration_logger, init_test_logger};
use near_o11y::WithSpanContextExt;
use near_primitives::shard_layout::ShardUId;
use near_primitives::state_part::PartId;
use near_primitives::state_sync::{CachedParts, StatePartKey};
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{BlockId, BlockReference, EpochId, EpochReference};
use near_primitives::utils::MaybeValidated;
use near_primitives_core::types::ShardId;
use near_store::DBCol;
use nearcore::test_utils::TestEnvNightshadeSetupExt;
use nearcore::{load_test_config, start_with_config};
use std::ops::ControlFlow;
use std::sync::{Arc, RwLock};

use crate::tests::test_helpers::heavy_test;

/// One client is in front, another must sync to it using state (fast) sync.
#[test]
#[cfg_attr(not(feature = "expensive_tests"), ignore)]
fn sync_state_nodes() {
    heavy_test(|| {
        init_integration_logger();

        let genesis = Genesis::test(vec!["test1".parse().unwrap()], 1);

        let (port1, port2) =
            (tcp::ListenerAddr::reserve_for_test(), tcp::ListenerAddr::reserve_for_test());
        let mut near1 = load_test_config("test1", port1, genesis.clone());
        near1.network_config.peer_store.boot_nodes = convert_boot_nodes(vec![]);
        near1.client_config.min_num_peers = 0;

        let _dir1 = Arc::new(tempfile::Builder::new().prefix("sync_nodes_1").tempdir().unwrap());
        let dir1 = _dir1.clone();
        let _dir2 = Arc::new(tempfile::Builder::new().prefix("sync_nodes_2").tempdir().unwrap());
        let dir2 = _dir2.clone();

        run_actix(async move {
            let nearcore::NearNode { view_client: view_client1, .. } =
                start_with_config(dir1.path(), near1).expect("start_with_config");

            let view_client2_holder = Arc::new(RwLock::new(None));
            let arbiters_holder = Arc::new(RwLock::new(vec![]));
            let arbiters_holder2 = arbiters_holder;

            WaitOrTimeoutActor::new(
                Box::new(move |_ctx| {
                    if view_client2_holder.read().unwrap().is_none() {
                        let view_client2_holder2 = view_client2_holder.clone();
                        let arbiters_holder2 = arbiters_holder2.clone();
                        let genesis2 = genesis.clone();
                        let dir2 = dir2.clone();

                        let actor = view_client1.send(GetBlock::latest().with_span_context());
                        let actor = actor.then(move |res| {
                            match &res {
                                Ok(Ok(b)) if b.header.height >= 101 => {
                                    let mut view_client2_holder2 =
                                        view_client2_holder2.write().unwrap();
                                    let mut arbiters_holder2 = arbiters_holder2.write().unwrap();

                                    if view_client2_holder2.is_none() {
                                        let mut near2 =
                                            load_test_config("test2", port2, genesis2.clone());
                                        near2.client_config.skip_sync_wait = false;
                                        near2.client_config.min_num_peers = 1;
                                        near2.network_config.peer_store.boot_nodes =
                                            convert_boot_nodes(vec![("test1", *port1)]);

                                        let nearcore::NearNode {
                                            view_client: view_client2,
                                            arbiters,
                                            ..
                                        } = start_with_config(dir2.path(), near2)
                                            .expect("start_with_config");
                                        *view_client2_holder2 = Some(view_client2);
                                        *arbiters_holder2 = arbiters;
                                    }
                                }
                                Ok(Ok(b)) if b.header.height < 101 => {
                                    println!("FIRST STAGE {}", b.header.height)
                                }
                                Err(_) => return future::ready(()),
                                _ => {}
                            };
                            future::ready(())
                        });
                        actix::spawn(actor);
                    }

                    if let Some(view_client2) = &*view_client2_holder.write().unwrap() {
                        let actor = view_client2.send(GetBlock::latest().with_span_context());
                        let actor = actor.then(|res| {
                            match &res {
                                Ok(Ok(b)) if b.header.height >= 101 => System::current().stop(),
                                Ok(Ok(b)) if b.header.height < 101 => {
                                    println!("SECOND STAGE {}", b.header.height)
                                }
                                Err(_) => return future::ready(()),
                                _ => {}
                            };
                            future::ready(())
                        });
                        actix::spawn(actor);
                    } else {
                    }
                }),
                100,
                60000,
            )
            .start();
        });
        drop(_dir1);
        drop(_dir2);
    });
}

/// One client is in front, another must sync to it using state (fast) sync.
#[cfg(feature = "expensive_tests")]
#[test]
#[cfg_attr(not(feature = "expensive_tests"), ignore)]
fn sync_state_nodes_multishard() {
    heavy_test(|| {
        init_integration_logger();

        let mut genesis = Genesis::test_sharded_new_version(
            vec![
                "test1".parse().unwrap(),
                "test2".parse().unwrap(),
                "test3".parse().unwrap(),
                "test4".parse().unwrap(),
            ],
            4,
            vec![2, 2],
        );
        genesis.config.epoch_length = 150; // so that by the time test2 joins it is not kicked out yet

        let _dir1 = Arc::new(tempfile::Builder::new().prefix("sync_nodes_1").tempdir().unwrap());
        let dir1 = _dir1.clone();
        let _dir2 = Arc::new(tempfile::Builder::new().prefix("sync_nodes_2").tempdir().unwrap());
        let dir2 = _dir2.clone();
        let _dir3 = Arc::new(tempfile::Builder::new().prefix("sync_nodes_3").tempdir().unwrap());
        let dir3 = _dir3.clone();
        let _dir4 = Arc::new(tempfile::Builder::new().prefix("sync_nodes_4").tempdir().unwrap());
        let dir4 = _dir4.clone();

        run_actix(async move {
            let (port1, port2, port3, port4) = (
                tcp::ListenerAddr::reserve_for_test(),
                tcp::ListenerAddr::reserve_for_test(),
                tcp::ListenerAddr::reserve_for_test(),
                tcp::ListenerAddr::reserve_for_test(),
            );

            let mut near1 = load_test_config("test1", port1, genesis.clone());
            near1.network_config.peer_store.boot_nodes =
                convert_boot_nodes(vec![("test3", *port3), ("test4", *port4)]);
            near1.client_config.min_num_peers = 2;
            near1.client_config.min_block_production_delay = Duration::milliseconds(200);
            near1.client_config.max_block_production_delay = Duration::milliseconds(400);

            let mut near3 = load_test_config("test3", port3, genesis.clone());
            near3.network_config.peer_store.boot_nodes =
                convert_boot_nodes(vec![("test1", *port1), ("test4", *port4)]);
            near3.client_config.min_num_peers = 2;
            near3.client_config.min_block_production_delay =
                near1.client_config.min_block_production_delay;
            near3.client_config.max_block_production_delay =
                near1.client_config.max_block_production_delay;

            let mut near4 = load_test_config("test4", port4, genesis.clone());
            near4.network_config.peer_store.boot_nodes =
                convert_boot_nodes(vec![("test1", *port1), ("test3", *port3)]);
            near4.client_config.min_num_peers = 2;
            near4.client_config.min_block_production_delay =
                near1.client_config.min_block_production_delay;
            near4.client_config.max_block_production_delay =
                near1.client_config.max_block_production_delay;

            let nearcore::NearNode { view_client: view_client1, .. } =
                start_with_config(dir1.path(), near1).expect("start_with_config");

            start_with_config(dir3.path(), near3).expect("start_with_config");
            start_with_config(dir4.path(), near4).expect("start_with_config");

            let view_client2_holder = Arc::new(RwLock::new(None));
            let arbiter_holder = Arc::new(RwLock::new(vec![]));
            let arbiter_holder2 = arbiter_holder;

            WaitOrTimeoutActor::new(
                Box::new(move |_ctx| {
                    if view_client2_holder.read().unwrap().is_none() {
                        let view_client2_holder2 = view_client2_holder.clone();
                        let arbiter_holder2 = arbiter_holder2.clone();
                        let genesis2 = genesis.clone();
                        let dir2 = dir2.clone();

                        let actor = view_client1.send(GetBlock::latest().with_span_context());
                        let actor = actor.then(move |res| {
                            match &res {
                                Ok(Ok(b)) if b.header.height >= 101 => {
                                    let mut view_client2_holder2 =
                                        view_client2_holder2.write().unwrap();
                                    let mut arbiter_holder2 = arbiter_holder2.write().unwrap();

                                    if view_client2_holder2.is_none() {
                                        let mut near2 = load_test_config("test2", port2, genesis2);
                                        near2.client_config.skip_sync_wait = false;
                                        near2.client_config.min_num_peers = 3;
                                        near2.client_config.min_block_production_delay =
                                            Duration::milliseconds(200);
                                        near2.client_config.max_block_production_delay =
                                            Duration::milliseconds(400);
                                        near2.network_config.peer_store.boot_nodes =
                                            convert_boot_nodes(vec![
                                                ("test1", *port1),
                                                ("test3", *port3),
                                                ("test4", *port4),
                                            ]);

                                        let nearcore::NearNode {
                                            view_client: view_client2,
                                            arbiters,
                                            ..
                                        } = start_with_config(dir2.path(), near2)
                                            .expect("start_with_config");
                                        *view_client2_holder2 = Some(view_client2);
                                        *arbiter_holder2 = arbiters;
                                    }
                                }
                                Ok(Ok(b)) if b.header.height < 101 => {
                                    println!("FIRST STAGE {}", b.header.height)
                                }
                                Err(_) => return future::ready(()),
                                _ => {}
                            };
                            future::ready(())
                        });
                        actix::spawn(actor);
                    }

                    if let Some(view_client2) = &*view_client2_holder.write().unwrap() {
                        let actor = view_client2.send(GetBlock::latest().with_span_context());
                        let actor = actor.then(|res| {
                            match &res {
                                Ok(Ok(b)) if b.header.height >= 101 => System::current().stop(),
                                Ok(Ok(b)) if b.header.height < 101 => {
                                    println!("SECOND STAGE {}", b.header.height)
                                }
                                Ok(Err(e)) => {
                                    println!("SECOND STAGE ERROR1: {:?}", e);
                                    return future::ready(());
                                }
                                Err(e) => {
                                    println!("SECOND STAGE ERROR2: {:?}", e);
                                    return future::ready(());
                                }
                                _ => {
                                    assert!(false);
                                }
                            };
                            future::ready(())
                        });
                        actix::spawn(actor);
                    }
                }),
                100,
                600000,
            )
            .start();
        });
        drop(_dir1);
        drop(_dir2);
        drop(_dir3);
        drop(_dir4);
    });
}

/// Start a validator that validators four shards. Since we only have 3 accounts one shard must have
/// empty state. Start another node that does state sync. Check state sync on empty state works.
#[test]
#[cfg_attr(not(feature = "expensive_tests"), ignore)]
fn sync_empty_state() {
    heavy_test(|| {
        init_integration_logger();

        let mut genesis = Genesis::test_sharded_new_version(
            vec!["test1".parse().unwrap(), "test2".parse().unwrap()],
            1,
            vec![1, 1, 1, 1],
        );
        genesis.config.epoch_length = 20;

        let _dir1 = Arc::new(tempfile::Builder::new().prefix("sync_nodes_1").tempdir().unwrap());
        let dir1 = _dir1.clone();
        let _dir2 = Arc::new(tempfile::Builder::new().prefix("sync_nodes_2").tempdir().unwrap());
        let dir2 = _dir2.clone();

        run_actix(async move {
            let (port1, port2) =
                (tcp::ListenerAddr::reserve_for_test(), tcp::ListenerAddr::reserve_for_test());

            // State sync triggers when header head is two epochs in the future.
            // Produce more blocks to make sure that state sync gets triggered when the second node starts.
            let state_sync_horizon = 10;
            let block_header_fetch_horizon = 1;
            let block_fetch_horizon = 1;

            let mut near1 = load_test_config("test1", port1, genesis.clone());
            near1.client_config.min_num_peers = 0;
            near1.client_config.min_block_production_delay = Duration::milliseconds(200);
            near1.client_config.max_block_production_delay = Duration::milliseconds(400);

            let nearcore::NearNode { view_client: view_client1, .. } =
                start_with_config(dir1.path(), near1).expect("start_with_config");

            let view_client2_holder = Arc::new(RwLock::new(None));
            let arbiters_holder = Arc::new(RwLock::new(vec![]));
            let arbiters_holder2 = arbiters_holder;

            WaitOrTimeoutActor::new(
                Box::new(move |_ctx| {
                    if view_client2_holder.read().unwrap().is_none() {
                        let view_client2_holder2 = view_client2_holder.clone();
                        let arbiters_holder2 = arbiters_holder2.clone();
                        let genesis2 = genesis.clone();
                        let dir2 = dir2.clone();

                        let actor = view_client1.send(GetBlock::latest().with_span_context());
                        let actor = actor.then(move |res| {
                            match &res {
                                Ok(Ok(b)) if b.header.height >= state_sync_horizon + 1 => {
                                    let mut view_client2_holder2 =
                                        view_client2_holder2.write().unwrap();
                                    let mut arbiters_holder2 = arbiters_holder2.write().unwrap();

                                    if view_client2_holder2.is_none() {
                                        let mut near2 = load_test_config("test2", port2, genesis2);
                                        near2.network_config.peer_store.boot_nodes =
                                            convert_boot_nodes(vec![("test1", *port1)]);
                                        near2.client_config.min_num_peers = 1;
                                        near2.client_config.min_block_production_delay =
                                            Duration::milliseconds(200);
                                        near2.client_config.max_block_production_delay =
                                            Duration::milliseconds(400);
                                        near2.client_config.block_header_fetch_horizon =
                                            block_header_fetch_horizon;
                                        near2.client_config.block_fetch_horizon =
                                            block_fetch_horizon;
                                        near2.client_config.tracked_shards = vec![0, 1, 2, 3];

                                        let nearcore::NearNode {
                                            view_client: view_client2,
                                            arbiters,
                                            ..
                                        } = start_with_config(dir2.path(), near2)
                                            .expect("start_with_config");
                                        *view_client2_holder2 = Some(view_client2);
                                        *arbiters_holder2 = arbiters;
                                    }
                                }
                                Ok(Ok(b)) if b.header.height <= state_sync_horizon => {
                                    println!("FIRST STAGE {}", b.header.height)
                                }
                                Err(_) => return future::ready(()),
                                _ => {}
                            };
                            future::ready(())
                        });
                        actix::spawn(actor);
                    }

                    if let Some(view_client2) = &*view_client2_holder.write().unwrap() {
                        let actor = view_client2.send(GetBlock::latest().with_span_context());
                        let actor = actor.then(|res| {
                            match &res {
                                Ok(Ok(b)) if b.header.height >= 40 => System::current().stop(),
                                Ok(Ok(b)) if b.header.height < 40 => {
                                    println!("SECOND STAGE {}", b.header.height)
                                }
                                Ok(Err(e)) => {
                                    println!("SECOND STAGE ERROR1: {:?}", e);
                                    return future::ready(());
                                }
                                Err(e) => {
                                    println!("SECOND STAGE ERROR2: {:?}", e);
                                    return future::ready(());
                                }
                                _ => {
                                    assert!(false);
                                }
                            };
                            future::ready(())
                        });
                        actix::spawn(actor);
                    }
                }),
                100,
                600000,
            )
            .start();
        });
        drop(_dir1);
        drop(_dir2);
    });
}

#[test]
#[cfg_attr(not(feature = "expensive_tests"), ignore)]
// FIXME(#9650): locks should not be held across await points, allowed currently only because the
// lint started triggering during a toolchain bump.
#[allow(clippy::await_holding_lock)]
/// Runs one node for some time, which dumps state to a temp directory.
/// Start the second node which gets state parts from that temp directory.
fn sync_state_dump() {
    heavy_test(|| {
        init_integration_logger();

        let mut genesis = Genesis::test_sharded_new_version(
            vec!["test1".parse().unwrap(), "test2".parse().unwrap()],
            1,
            vec![1],
        );
        // Needs to be long enough to give enough time to the second node to
        // start, sync headers and find a dump of state.
        genesis.config.epoch_length = 30;

        let _dump_dir =
            Arc::new(tempfile::Builder::new().prefix("state_dump_1").tempdir().unwrap());
        let dump_dir = _dump_dir.clone();
        let _dir1 = Arc::new(tempfile::Builder::new().prefix("sync_nodes_1").tempdir().unwrap());
        let dir1 = _dir1.clone();
        let _dir2 = Arc::new(tempfile::Builder::new().prefix("sync_nodes_2").tempdir().unwrap());
        let dir2 = _dir2.clone();

        run_actix(async move {
            let (port1, port2) =
                (tcp::ListenerAddr::reserve_for_test(), tcp::ListenerAddr::reserve_for_test());
            // Produce more blocks to make sure that state sync gets triggered when the second node starts.
            let state_sync_horizon = 50;
            let block_header_fetch_horizon = 1;
            let block_fetch_horizon = 1;

            let mut near1 = load_test_config("test1", port1, genesis.clone());
            near1.client_config.min_num_peers = 0;
            // An epoch passes in about 9 seconds.
            near1.client_config.min_block_production_delay = Duration::milliseconds(300);
            near1.client_config.max_block_production_delay = Duration::milliseconds(600);
            near1.client_config.tracked_shards = vec![0]; // Track all shards.

            near1.client_config.state_sync.dump = Some(DumpConfig {
                location: Filesystem { root_dir: dump_dir.path().to_path_buf() },
                restart_dump_for_shards: None,
                iteration_delay: Some(Duration::milliseconds(500)),
                credentials_file: None,
            });
            near1.config.store.state_snapshot_enabled = true;

            let nearcore::NearNode {
                view_client: view_client1,
                // State sync dumper should be kept in the scope to avoid dropping it, which stops the state dumper loop.
                state_sync_dumper: _dumper,
                ..
            } = start_with_config(dir1.path(), near1).expect("start_with_config");

            let view_client2_holder = Arc::new(RwLock::new(None));
            let arbiters_holder = Arc::new(RwLock::new(vec![]));
            let arbiters_holder2 = arbiters_holder;

            wait_or_timeout(1000, 60000, || async {
                if view_client2_holder.read().unwrap().is_none() {
                    let view_client2_holder2 = view_client2_holder.clone();
                    let arbiters_holder2 = arbiters_holder2.clone();
                    let genesis2 = genesis.clone();

                    match view_client1.send(GetBlock::latest().with_span_context()).await {
                        Ok(Ok(b)) if b.header.height >= genesis.config.epoch_length + 2 => {
                            let mut view_client2_holder2 = view_client2_holder2.write().unwrap();
                            let mut arbiters_holder2 = arbiters_holder2.write().unwrap();

                            if view_client2_holder2.is_none() {
                                let mut near2 = load_test_config("test2", port2, genesis2);
                                near2.network_config.peer_store.boot_nodes =
                                    convert_boot_nodes(vec![("test1", *port1)]);
                                near2.client_config.min_num_peers = 1;
                                near2.client_config.min_block_production_delay =
                                    Duration::milliseconds(300);
                                near2.client_config.max_block_production_delay =
                                    Duration::milliseconds(600);
                                near2.client_config.block_header_fetch_horizon =
                                    block_header_fetch_horizon;
                                near2.client_config.block_fetch_horizon = block_fetch_horizon;
                                near2.client_config.tracked_shards = vec![0]; // Track all shards.
                                near2.client_config.state_sync_enabled = true;
                                near2.client_config.state_sync_timeout = Duration::seconds(2);
                                near2.client_config.state_sync.sync =
                                    SyncConfig::ExternalStorage(ExternalStorageConfig {
                                        location: Filesystem {
                                            root_dir: dump_dir.path().to_path_buf(),
                                        },
                                        num_concurrent_requests: 1,
                                        num_concurrent_requests_during_catchup: 1,
                                        external_storage_fallback_threshold: 0,
                                    });

                                let nearcore::NearNode {
                                    view_client: view_client2, arbiters, ..
                                } = start_with_config(dir2.path(), near2)
                                    .expect("start_with_config");
                                *view_client2_holder2 = Some(view_client2);
                                *arbiters_holder2 = arbiters;
                            }
                        }
                        Ok(Ok(b)) if b.header.height <= state_sync_horizon => {
                            tracing::info!("FIRST STAGE {}", b.header.height);
                        }
                        Err(_) => {}
                        _ => {}
                    };
                    return ControlFlow::Continue(());
                }

                if let Some(view_client2) = &*view_client2_holder.write().unwrap() {
                    match view_client2.send(GetBlock::latest().with_span_context()).await {
                        Ok(Ok(b)) if b.header.height >= 40 => {
                            return ControlFlow::Break(());
                        }
                        Ok(Ok(b)) if b.header.height < 40 => {
                            tracing::info!("SECOND STAGE {}", b.header.height)
                        }
                        Ok(Err(e)) => {
                            tracing::info!("SECOND STAGE ERROR1: {:?}", e);
                        }
                        Err(e) => {
                            tracing::info!("SECOND STAGE ERROR2: {:?}", e);
                        }
                        _ => {
                            assert!(false);
                        }
                    };
                    return ControlFlow::Continue(());
                }

                panic!("Unexpected");
            })
            .await
            .unwrap();
            System::current().stop();
        });
        drop(_dump_dir);
        drop(_dir1);
        drop(_dir2);
    });
}

#[test]
// Test that state sync behaves well when the chunks are absent at the end of the epoch.
fn test_dump_epoch_missing_chunk_in_last_block() {
    heavy_test(|| {
        init_test_logger();
        let epoch_length = 10;

        for num_last_chunks_missing in 0..6 {
            assert!(num_last_chunks_missing < epoch_length);

            tracing::info!(
                target: "test",
                ?num_last_chunks_missing,
                "starting test_dump_epoch_missing_chunk_in_last_block"
            );
            let mut genesis =
                Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
            genesis.config.epoch_length = epoch_length;
            let mut env = TestEnv::builder(&genesis.config)
                .clients_count(2)
                .use_state_snapshots()
                .real_stores()
                .nightshade_runtimes_congestion_control_disabled(&genesis)
                .build();

            let genesis_block = env.clients[0].chain.get_block_by_height(0).unwrap();
            let mut blocks = vec![genesis_block.clone()];
            let signer =
                InMemorySigner::from_seed("test0".parse().unwrap(), KeyType::ED25519, "test0")
                    .into();
            let target_height = epoch_length + 1;
            for i in 1..=target_height {
                tracing::info!(
                    target: "test",
                    height=i,
                    "producing block"
                );

                let block = env.clients[0].produce_block(i).unwrap().unwrap();
                blocks.push(block.clone());
                if (i % epoch_length) != 0
                    && epoch_length - (i % epoch_length) <= num_last_chunks_missing
                {
                    // Don't produce chunks for the last blocks of an epoch.
                    env.clients[0]
                        .process_block_test_no_produce_chunk(
                            MaybeValidated::from(block.clone()),
                            Provenance::PRODUCED,
                        )
                        .unwrap();
                    tracing::info!(
                        "Block {i}: {:?} -- produced no chunk",
                        block.header().epoch_id()
                    );
                } else {
                    env.process_block(0, block.clone(), Provenance::PRODUCED);
                    tracing::info!(
                        "Block {i}: {:?} -- also produced a chunk",
                        block.header().epoch_id()
                    );
                }
                env.process_block(1, block, Provenance::NONE);

                let tx = SignedTransaction::send_money(
                    i + 1,
                    "test0".parse().unwrap(),
                    "test1".parse().unwrap(),
                    &signer,
                    1,
                    *genesis_block.hash(),
                );
                assert_eq!(env.clients[0].process_tx(tx, false, false), ProcessTxResponse::ValidTx);
            }

            // Simulate state sync

            tracing::info!(target: "test", "state sync - get parts");
            // No blocks were skipped, therefore we can compute the block height of the first block of the current epoch.
            let sync_hash_height = ((target_height / epoch_length) * epoch_length + 1) as usize;
            let sync_hash = *blocks[sync_hash_height].hash();
            assert_ne!(
                blocks[sync_hash_height].header().epoch_id(),
                blocks[sync_hash_height - 1].header().epoch_id()
            );

            let state_sync_header =
                env.clients[0].chain.get_state_response_header(0, sync_hash).unwrap();
            let num_parts = state_sync_header.num_state_parts();
            let state_root = state_sync_header.chunk_prev_state_root();
            // Check that state parts can be obtained.
            let state_sync_parts: Vec<_> = (0..num_parts)
                .map(|i| {
                    // This should obviously not fail, aka succeed.
                    env.clients[0].chain.get_state_response_part(0, i, sync_hash).unwrap()
                })
                .collect();

            tracing::info!(target: "test", "state sync - apply parts");
            env.clients[1].chain.reset_data_pre_state_sync(sync_hash).unwrap();
            let epoch_id = blocks.last().unwrap().header().epoch_id();
            for i in 0..num_parts {
                env.clients[1]
                    .runtime_adapter
                    .apply_state_part(
                        0,
                        &state_root,
                        PartId::new(i, num_parts),
                        &state_sync_parts[i as usize],
                        &epoch_id,
                    )
                    .unwrap();
            }

            tracing::info!(target: "test", "state sync - set parts");
            env.clients[1].chain.set_state_header(0, sync_hash, state_sync_header).unwrap();
            for i in 0..num_parts {
                env.clients[1]
                    .chain
                    .set_state_part(
                        0,
                        sync_hash,
                        PartId::new(i, num_parts),
                        &state_sync_parts[i as usize],
                    )
                    .unwrap();
            }
            let rt = Arc::clone(&env.clients[1].runtime_adapter);
            let f = Sender::from_fn(move |msg: ApplyStatePartsRequest| {
                let store = rt.store();

                let shard_id = msg.shard_uid.shard_id as ShardId;
                let mut store_update = store.store_update();
                assert!(rt
                    .get_flat_storage_manager()
                    .remove_flat_storage_for_shard(msg.shard_uid, &mut store_update)
                    .unwrap());
                store_update.commit().unwrap();

                for part_id in 0..msg.num_parts {
                    let key =
                        borsh::to_vec(&StatePartKey(msg.sync_hash, shard_id, part_id)).unwrap();
                    let part = store.get(DBCol::StateParts, &key).unwrap().unwrap();

                    rt.apply_state_part(
                        shard_id,
                        &msg.state_root,
                        PartId::new(part_id, msg.num_parts),
                        &part,
                        &msg.epoch_id,
                    )
                    .unwrap();
                }
            });

            tracing::info!(target: "test", "state sync - schedule");
            env.clients[1].chain.schedule_apply_state_parts(0, sync_hash, num_parts, &f).unwrap();

            tracing::info!(target: "test", "state sync - set state finalize");
            env.clients[1].chain.set_state_finalize(0, sync_hash).unwrap();

            let last_chunk_height = epoch_length - num_last_chunks_missing;
            for height in 1..epoch_length {
                if height < last_chunk_height {
                    assert!(env.clients[1]
                        .chain
                        .get_chunk_extra(blocks[height as usize].hash(), &ShardUId::single_shard())
                        .is_err());
                } else {
                    let chunk_extra = env.clients[1]
                        .chain
                        .get_chunk_extra(blocks[height as usize].hash(), &ShardUId::single_shard())
                        .unwrap();
                    let expected_chunk_extra = env.clients[0]
                        .chain
                        .get_chunk_extra(
                            blocks[last_chunk_height as usize].hash(),
                            &ShardUId::single_shard(),
                        )
                        .unwrap();
                    // The chunk extra of the prev block of sync block should be the same as the node that it is syncing from
                    assert_eq!(chunk_extra, expected_chunk_extra);
                }
            }
        }
    });
}

#[test]
// Tests StateRequestHeader and StateRequestPart.
fn test_state_sync_headers() {
    heavy_test(|| {
        init_test_logger();

        let _dir1 =
            Arc::new(tempfile::Builder::new().prefix("test_state_sync_headers").tempdir().unwrap());
        let dir1 = _dir1.clone();

        run_actix(async {
            let mut genesis = Genesis::test(vec!["test1".parse().unwrap()], 1);
            // Increase epoch_length if the test is flaky.
            genesis.config.epoch_length = 50;

            let mut near1 =
                load_test_config("test1", tcp::ListenerAddr::reserve_for_test(), genesis.clone());
            near1.client_config.min_num_peers = 0;
            near1.client_config.tracked_shards = vec![0]; // Track all shards.
            near1.config.store.state_snapshot_enabled = true;

            let nearcore::NearNode { view_client: view_client1, .. } =
                start_with_config(dir1.path(), near1).expect("start_with_config");

            // First we need to find sync_hash. That is done in 3 steps:
            // 1. Get the latest block
            // 2. Query validators for the epoch_id of that block.
            // 3. Get a block at 'epoch_start_height' that is found in the response of the validators method.
            //
            // Second, we request state sync header.
            // Third, we request state sync part with part_id = 0.
            wait_or_timeout(1000, 110000, || async {
                let epoch_id = match view_client1.send(GetBlock::latest().with_span_context()).await
                {
                    Ok(Ok(b)) => Some(b.header.epoch_id),
                    _ => None,
                };
                // async is hard, will use this construct to reduce nestedness.
                let epoch_id = match epoch_id {
                    Some(x) => x,
                    None => return ControlFlow::Continue(()),
                };
                tracing::info!(?epoch_id, "got epoch_id");

                let epoch_start_height = match view_client1
                    .send(
                        GetValidatorInfo {
                            epoch_reference: EpochReference::EpochId(EpochId(epoch_id)),
                        }
                        .with_span_context(),
                    )
                    .await
                {
                    Ok(Ok(v)) => Some(v.epoch_start_height),
                    _ => None,
                };
                let epoch_start_height = match epoch_start_height {
                    Some(x) => x,
                    None => return ControlFlow::Continue(()),
                };
                tracing::info!(epoch_start_height, "got epoch_start_height");

                let sync_hash_and_num_shards = match view_client1
                    .send(
                        GetBlock(BlockReference::BlockId(BlockId::Height(epoch_start_height)))
                            .with_span_context(),
                    )
                    .await
                {
                    Ok(Ok(b)) => Some((b.header.hash, b.chunks.len())),
                    _ => None,
                };
                let (sync_hash, num_shards) = match sync_hash_and_num_shards {
                    Some(x) => x,
                    None => return ControlFlow::Continue(()),
                };
                tracing::info!(?sync_hash, num_shards, "got sync_hash");

                for shard_id in 0..num_shards {
                    // Make StateRequestHeader and expect that the response contains a header and `can_generate` is true.
                    let state_response_info = match view_client1
                        .send(
                            StateRequestHeader { shard_id: shard_id as ShardId, sync_hash }
                                .with_span_context(),
                        )
                        .await
                    {
                        Ok(Some(StateResponse(state_response_info))) => Some(state_response_info),
                        _ => None,
                    };
                    let state_response_info = match state_response_info {
                        Some(x) => x,
                        None => return ControlFlow::Continue(()),
                    };
                    let state_response = state_response_info.take_state_response();
                    let cached_parts = state_response.cached_parts().clone();
                    let can_generate = state_response.can_generate();
                    assert!(state_response.part().is_none());
                    if let Some(_header) = state_response.take_header() {
                        if !can_generate {
                            tracing::info!(
                                ?sync_hash,
                                shard_id,
                                ?cached_parts,
                                can_generate,
                                "got header but cannot generate"
                            );
                            return ControlFlow::Continue(());
                        }
                        tracing::info!(
                            ?sync_hash,
                            shard_id,
                            ?cached_parts,
                            can_generate,
                            "got header"
                        );
                    } else {
                        tracing::info!(
                            ?sync_hash,
                            shard_id,
                            ?cached_parts,
                            can_generate,
                            "got no header"
                        );
                        return ControlFlow::Continue(());
                    }

                    // Make StateRequestPart and expect that the response contains a part and `can_generate` is true and part_id = 0 and the node has all parts cached.
                    let state_response_info = match view_client1
                        .send(
                            StateRequestPart {
                                shard_id: shard_id as ShardId,
                                sync_hash,
                                part_id: 0,
                            }
                            .with_span_context(),
                        )
                        .await
                    {
                        Ok(Some(StateResponse(state_response_info))) => Some(state_response_info),
                        _ => None,
                    };
                    let state_response_info = match state_response_info {
                        Some(x) => x,
                        None => return ControlFlow::Continue(()),
                    };
                    let state_response = state_response_info.take_state_response();
                    let cached_parts = state_response.cached_parts().clone();
                    let can_generate = state_response.can_generate();
                    let part = state_response.part().clone();
                    assert!(state_response.take_header().is_none());
                    if let Some((part_id, _part)) = part {
                        if !can_generate
                            || cached_parts != Some(CachedParts::AllParts)
                            || part_id != 0
                        {
                            tracing::info!(
                                ?sync_hash,
                                shard_id,
                                ?cached_parts,
                                can_generate,
                                part_id,
                                "got part but shard info is unexpected"
                            );
                            return ControlFlow::Continue(());
                        }
                        tracing::info!(
                            ?sync_hash,
                            shard_id,
                            ?cached_parts,
                            can_generate,
                            part_id,
                            "got part"
                        );
                    } else {
                        tracing::info!(
                            ?sync_hash,
                            shard_id,
                            ?cached_parts,
                            can_generate,
                            "got no part"
                        );
                        return ControlFlow::Continue(());
                    }
                }
                return ControlFlow::Break(());
            })
            .await
            .unwrap();
            System::current().stop();
        });
        drop(_dir1);
    });
}

#[test]
// Tests StateRequestHeader and StateRequestPart.
fn test_state_sync_headers_no_tracked_shards() {
    heavy_test(|| {
        init_test_logger();

        let _dir1 = Arc::new(
            tempfile::Builder::new()
                .prefix("test_state_sync_headers_no_tracked_shards_1")
                .tempdir()
                .unwrap(),
        );
        let dir1 = _dir1.clone();
        let _dir2 = Arc::new(
            tempfile::Builder::new()
                .prefix("test_state_sync_headers_no_tracked_shards_2")
                .tempdir()
                .unwrap(),
        );
        let dir2 = _dir2.clone();
        run_actix(async {
            let mut genesis = Genesis::test(vec!["test1".parse().unwrap()], 1);
            // Increase epoch_length if the test is flaky.
            let epoch_length = 50;
            genesis.config.epoch_length = epoch_length;

            let port1 = tcp::ListenerAddr::reserve_for_test();
            let mut near1 = load_test_config("test1", port1, genesis.clone());
            near1.client_config.min_num_peers = 0;
            near1.client_config.tracked_shards = vec![0]; // Track all shards, it is a validator.
            near1.config.store.state_snapshot_enabled = false;
            near1.config.state_sync_enabled = false;
            near1.client_config.state_sync_enabled = false;

            let _node1 = start_with_config(dir1.path(), near1).expect("start_with_config");

            let mut near2 =
                load_test_config("test2", tcp::ListenerAddr::reserve_for_test(), genesis.clone());
            near2.network_config.peer_store.boot_nodes =
                convert_boot_nodes(vec![("test1", *port1)]);
            near2.client_config.min_num_peers = 0;
            near2.client_config.tracked_shards = vec![]; // Track no shards.
            near2.config.store.state_snapshot_enabled = true;
            near2.config.state_sync_enabled = false;
            near2.client_config.state_sync_enabled = false;

            let nearcore::NearNode { view_client: view_client2, .. } =
                start_with_config(dir2.path(), near2).expect("start_with_config");

            // First we need to find sync_hash. That is done in 3 steps:
            // 1. Get the latest block
            // 2. Query validators for the epoch_id of that block.
            // 3. Get a block at 'epoch_start_height' that is found in the response of the validators method.
            //
            // Second, we request state sync header.
            // Third, we request state sync part with part_id = 0.
            wait_or_timeout(1000, 110000, || async {
                let epoch_id = match view_client2.send(GetBlock::latest().with_span_context()).await
                {
                    Ok(Ok(b)) => Some(b.header.epoch_id),
                    _ => None,
                };
                // async is hard, will use this construct to reduce nestedness.
                let epoch_id = match epoch_id {
                    Some(x) => x,
                    None => return ControlFlow::Continue(()),
                };
                tracing::info!(?epoch_id, "got epoch_id");

                let epoch_start_height = match view_client2
                    .send(
                        GetValidatorInfo {
                            epoch_reference: EpochReference::EpochId(EpochId(epoch_id)),
                        }
                        .with_span_context(),
                    )
                    .await
                {
                    Ok(Ok(v)) => Some(v.epoch_start_height),
                    _ => None,
                };
                let epoch_start_height = match epoch_start_height {
                    Some(x) => x,
                    None => return ControlFlow::Continue(()),
                };
                tracing::info!(epoch_start_height, "got epoch_start_height");
                if epoch_start_height < 2 * epoch_length {
                    return ControlFlow::Continue(());
                }

                let sync_hash_and_num_shards = match view_client2
                    .send(
                        GetBlock(BlockReference::BlockId(BlockId::Height(epoch_start_height)))
                            .with_span_context(),
                    )
                    .await
                {
                    Ok(Ok(b)) => Some((b.header.hash, b.chunks.len())),
                    _ => None,
                };
                let (sync_hash, num_shards) = match sync_hash_and_num_shards {
                    Some(x) => x,
                    None => return ControlFlow::Continue(()),
                };
                tracing::info!(?sync_hash, num_shards, "got sync_hash");

                for shard_id in 0..num_shards {
                    // Make StateRequestHeader and expect that the response contains a header and `can_generate` is true.
                    let state_response_info = match view_client2
                        .send(
                            StateRequestHeader { shard_id: shard_id as ShardId, sync_hash }
                                .with_span_context(),
                        )
                        .await
                    {
                        Ok(Some(StateResponse(state_response_info))) => Some(state_response_info),
                        _ => None,
                    };
                    let state_response_info = match state_response_info {
                        Some(x) => x,
                        None => return ControlFlow::Continue(()),
                    };
                    tracing::info!(?state_response_info, "got header state response");
                    let state_response = state_response_info.take_state_response();
                    assert_eq!(state_response.cached_parts(), &None);
                    assert!(!state_response.can_generate());
                    assert!(state_response.part().is_none());
                    assert_eq!(state_response.take_header(), None);

                    // Make StateRequestPart and expect that the response contains a part and `can_generate` is true and part_id = 0 and the node has all parts cached.
                    let state_response_info = match view_client2
                        .send(
                            StateRequestPart {
                                shard_id: shard_id as ShardId,
                                sync_hash,
                                part_id: 0,
                            }
                            .with_span_context(),
                        )
                        .await
                    {
                        Ok(Some(StateResponse(state_response_info))) => Some(state_response_info),
                        _ => None,
                    };
                    let state_response_info = match state_response_info {
                        Some(x) => x,
                        None => return ControlFlow::Continue(()),
                    };
                    tracing::info!(?state_response_info, "got state part response");
                    let state_response = state_response_info.take_state_response();
                    assert_eq!(state_response.cached_parts(), &None);
                    assert!(!state_response.can_generate());
                    assert!(state_response.part().is_none());
                    assert_eq!(state_response.take_header(), None);
                }
                return ControlFlow::Break(());
            })
            .await
            .unwrap();
            System::current().stop();
        });
        drop(_dir1);
        drop(_dir2);
    });
}
