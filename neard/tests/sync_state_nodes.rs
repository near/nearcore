use std::sync::{Arc, RwLock};
use std::time::Duration;

use actix::{Actor, System};
use futures::{future, FutureExt};

use near_chain_configs::Genesis;
use near_client::GetBlock;
use near_network::test_utils::{convert_boot_nodes, open_port, WaitOrTimeout};
use near_primitives::test_utils::{heavy_test, init_integration_logger};
use neard::{config::GenesisExt, load_test_config, start_with_config};

/// One client is in front, another must sync to it using state (fast) sync.
#[test]
fn sync_state_nodes() {
    heavy_test(|| {
        init_integration_logger();

        let genesis = Arc::new(Genesis::test(vec!["test1"], 1));

        let (port1, port2) = (open_port(), open_port());
        let mut near1 = load_test_config("test1", port1, Arc::clone(&genesis));
        near1.network_config.boot_nodes = convert_boot_nodes(vec![]);
        near1.client_config.min_num_peers = 0;
        let system = System::new("NEAR");

        let dir1 = tempfile::Builder::new().prefix("sync_nodes_1").tempdir().unwrap();
        let (_, view_client1) = start_with_config(dir1.path(), near1);

        let view_client2_holder = Arc::new(RwLock::new(None));

        WaitOrTimeout::new(
            Box::new(move |_ctx| {
                if view_client2_holder.read().unwrap().is_none() {
                    let view_client2_holder2 = view_client2_holder.clone();
                    let genesis2 = Arc::clone(&genesis);

                    actix::spawn(view_client1.send(GetBlock::latest()).then(move |res| {
                        match &res {
                            Ok(Ok(b)) if b.header.height >= 101 => {
                                let mut view_client2_holder2 =
                                    view_client2_holder2.write().unwrap();

                                if view_client2_holder2.is_none() {
                                    let mut near2 =
                                        load_test_config("test2", port2, Arc::clone(&genesis2));
                                    near2.client_config.skip_sync_wait = false;
                                    near2.client_config.min_num_peers = 1;
                                    near2.network_config.boot_nodes =
                                        convert_boot_nodes(vec![("test1", port1)]);

                                    let dir2 = tempfile::Builder::new()
                                        .prefix("sync_nodes_2")
                                        .tempdir()
                                        .unwrap();
                                    let (_, view_client2) = start_with_config(dir2.path(), near2);
                                    *view_client2_holder2 = Some(view_client2);
                                }
                            }
                            Ok(Ok(b)) if b.header.height < 101 => {
                                println!("FIRST STAGE {}", b.header.height)
                            }
                            Err(_) => return future::ready(()),
                            _ => {}
                        };
                        future::ready(())
                    }));
                }

                if let Some(view_client2) = &*view_client2_holder.write().unwrap() {
                    actix::spawn(view_client2.send(GetBlock::latest()).then(|res| {
                        match &res {
                            Ok(Ok(b)) if b.header.height >= 101 => System::current().stop(),
                            Ok(Ok(b)) if b.header.height < 101 => {
                                println!("SECOND STAGE {}", b.header.height)
                            }
                            Err(_) => return future::ready(()),
                            _ => {}
                        };
                        future::ready(())
                    }));
                } else {
                }
            }),
            100,
            60000,
        )
        .start();

        system.run().unwrap();
    });
}

/// One client is in front, another must sync to it using state (fast) sync.
#[test]
fn sync_state_nodes_multishard() {
    if !cfg!(feature = "expensive_tests") {
        return;
    }
    heavy_test(|| {
        init_integration_logger();

        let mut genesis =
            Genesis::test_sharded(vec!["test1", "test2", "test3", "test4"], 4, vec![2, 2]);
        genesis.config.epoch_length = 150; // so that by the time test2 joins it is not kicked out yet
        let genesis = Arc::new(genesis);

        let system = System::new("NEAR");

        let (port1, port2, port3, port4) = (open_port(), open_port(), open_port(), open_port());

        let mut near1 = load_test_config("test1", port1, Arc::clone(&genesis));
        near1.network_config.boot_nodes =
            convert_boot_nodes(vec![("test3", port3), ("test4", port4)]);
        near1.client_config.min_num_peers = 2;
        near1.client_config.min_block_production_delay = Duration::from_millis(200);
        near1.client_config.max_block_production_delay = Duration::from_millis(400);

        let mut near3 = load_test_config("test3", port3, Arc::clone(&genesis));
        near3.network_config.boot_nodes =
            convert_boot_nodes(vec![("test1", port1), ("test4", port4)]);
        near3.client_config.min_num_peers = 2;
        near3.client_config.min_block_production_delay =
            near1.client_config.min_block_production_delay;
        near3.client_config.max_block_production_delay =
            near1.client_config.max_block_production_delay;

        let mut near4 = load_test_config("test4", port4, Arc::clone(&genesis));
        near4.network_config.boot_nodes =
            convert_boot_nodes(vec![("test1", port1), ("test3", port3)]);
        near4.client_config.min_num_peers = 2;
        near4.client_config.min_block_production_delay =
            near1.client_config.min_block_production_delay;
        near4.client_config.max_block_production_delay =
            near1.client_config.max_block_production_delay;

        let dir1 = tempfile::Builder::new().prefix("sync_nodes_1").tempdir().unwrap();
        let (_, view_client1) = start_with_config(dir1.path(), near1);

        let dir3 = tempfile::Builder::new().prefix("sync_nodes_3").tempdir().unwrap();
        let (_, _) = start_with_config(dir3.path(), near3);

        let dir4 = tempfile::Builder::new().prefix("sync_nodes_4").tempdir().unwrap();
        let (_, _) = start_with_config(dir4.path(), near4);

        let view_client2_holder = Arc::new(RwLock::new(None));

        WaitOrTimeout::new(
            Box::new(move |_ctx| {
                if view_client2_holder.read().unwrap().is_none() {
                    let view_client2_holder2 = view_client2_holder.clone();
                    let genesis2 = Arc::clone(&genesis);

                    actix::spawn(view_client1.send(GetBlock::latest()).then(move |res| {
                        match &res {
                            Ok(Ok(b)) if b.header.height >= 101 => {
                                let mut view_client2_holder2 =
                                    view_client2_holder2.write().unwrap();

                                if view_client2_holder2.is_none() {
                                    let mut near2 =
                                        load_test_config("test2", port2, Arc::clone(&genesis2));
                                    near2.client_config.skip_sync_wait = false;
                                    near2.client_config.min_num_peers = 3;
                                    near2.client_config.min_block_production_delay =
                                        Duration::from_millis(200);
                                    near2.client_config.max_block_production_delay =
                                        Duration::from_millis(400);
                                    near2.network_config.boot_nodes = convert_boot_nodes(vec![
                                        ("test1", port1),
                                        ("test3", port3),
                                        ("test4", port4),
                                    ]);

                                    let dir2 = tempfile::Builder::new()
                                        .prefix("sync_nodes_2")
                                        .tempdir()
                                        .unwrap();
                                    let (_, view_client2) = start_with_config(dir2.path(), near2);
                                    *view_client2_holder2 = Some(view_client2);
                                }
                            }
                            Ok(Ok(b)) if b.header.height < 101 => {
                                println!("FIRST STAGE {}", b.header.height)
                            }
                            Err(_) => return future::ready(()),
                            _ => {}
                        };
                        future::ready(())
                    }));
                }

                if let Some(view_client2) = &*view_client2_holder.write().unwrap() {
                    actix::spawn(view_client2.send(GetBlock::latest()).then(|res| {
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
                    }));
                }
            }),
            100,
            600000,
        )
        .start();

        system.run().unwrap();
    });
}

/// Start a validator that validators four shards. Since we only have 3 accounts one shard must have
/// empty state. Start another node that does state sync. Check state sync on empty state works.
#[test]
fn sync_empty_state() {
    heavy_test(|| {
        init_integration_logger();

        let mut genesis = Genesis::test_sharded(vec!["test1", "test2"], 1, vec![1, 1, 1, 1]);
        genesis.config.epoch_length = 20;
        let genesis = Arc::new(genesis);

        let system = System::new("NEAR");

        let (port1, port2) = (open_port(), open_port());
        let state_sync_horizon = 10;
        let block_header_fetch_horizon = 1;
        let block_fetch_horizon = 1;

        let mut near1 = load_test_config("test1", port1, Arc::clone(&genesis));
        near1.client_config.min_num_peers = 0;
        near1.client_config.min_block_production_delay = Duration::from_millis(200);
        near1.client_config.max_block_production_delay = Duration::from_millis(400);

        let dir1 = tempfile::Builder::new().prefix("sync_nodes_1").tempdir().unwrap();
        let (_, view_client1) = start_with_config(dir1.path(), near1);
        let dir2 = Arc::new(tempfile::Builder::new().prefix("sync_nodes_2").tempdir().unwrap());

        let view_client2_holder = Arc::new(RwLock::new(None));

        WaitOrTimeout::new(
            Box::new(move |_ctx| {
                if view_client2_holder.read().unwrap().is_none() {
                    let view_client2_holder2 = view_client2_holder.clone();
                    let genesis2 = Arc::clone(&genesis);
                    let dir2 = dir2.clone();

                    actix::spawn(view_client1.send(GetBlock::latest()).then(move |res| {
                        match &res {
                            Ok(Ok(b)) if b.header.height >= state_sync_horizon + 1 => {
                                let mut view_client2_holder2 =
                                    view_client2_holder2.write().unwrap();

                                if view_client2_holder2.is_none() {
                                    let mut near2 =
                                        load_test_config("test2", port2, Arc::clone(&genesis2));
                                    near2.network_config.boot_nodes =
                                        convert_boot_nodes(vec![("test1", port1)]);
                                    near2.client_config.min_num_peers = 1;
                                    near2.client_config.min_block_production_delay =
                                        Duration::from_millis(200);
                                    near2.client_config.max_block_production_delay =
                                        Duration::from_millis(400);
                                    near2.client_config.state_fetch_horizon = state_sync_horizon;
                                    near2.client_config.block_header_fetch_horizon =
                                        block_header_fetch_horizon;
                                    near2.client_config.block_fetch_horizon = block_fetch_horizon;
                                    near2.client_config.tracked_shards = vec![0, 1, 2, 3];

                                    let (_, view_client2) = start_with_config(dir2.path(), near2);
                                    *view_client2_holder2 = Some(view_client2);
                                }
                            }
                            Ok(Ok(b)) if b.header.height <= state_sync_horizon => {
                                println!("FIRST STAGE {}", b.header.height)
                            }
                            Err(_) => return future::ready(()),
                            _ => {}
                        };
                        future::ready(())
                    }));
                }

                if let Some(view_client2) = &*view_client2_holder.write().unwrap() {
                    actix::spawn(view_client2.send(GetBlock::latest()).then(|res| {
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
                    }));
                }
            }),
            100,
            600000,
        )
        .start();

        system.run().unwrap();
    });
}
