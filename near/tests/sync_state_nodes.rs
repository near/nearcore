use std::sync::{Arc, RwLock};

use actix::{Actor, System};
use futures::future::Future;
use tempdir::TempDir;

use near::{load_test_config, start_with_config, GenesisConfig};
use near_client::GetBlock;
use near_network::test_utils::{convert_boot_nodes, open_port, WaitOrTimeout};
use near_primitives::test_utils::{heavy_test, init_test_logger};

/// One client is in front, another must sync to it using state (fast) sync.
#[test]
fn sync_state_nodes() {
    heavy_test(|| {
        init_test_logger();

        let genesis_config = GenesisConfig::test(vec!["test1"]);

        let (port1, port2) = (open_port(), open_port());
        let mut near1 = load_test_config("test1", port1, &genesis_config);
        near1.network_config.boot_nodes = convert_boot_nodes(vec![]);
        near1.client_config.min_num_peers = 0;
        let system = System::new("NEAR");

        let dir1 = TempDir::new("sync_nodes_1").unwrap();
        let (_, view_client1) = start_with_config(dir1.path(), near1);

        let view_client2_holder = Arc::new(RwLock::new(None));

        WaitOrTimeout::new(
            Box::new(move |_ctx| {
                if view_client2_holder.read().unwrap().is_none() {
                    let view_client2_holder2 = view_client2_holder.clone();
                    let genesis_config2 = genesis_config.clone();

                    actix::spawn(view_client1.send(GetBlock::Best).then(move |res| {
                        match &res {
                            Ok(Ok(b)) if b.header.height >= 101 => {
                                let mut view_client2_holder2 =
                                    view_client2_holder2.write().unwrap();

                                if view_client2_holder2.is_none() {
                                    let mut near2 =
                                        load_test_config("test2", port2, &genesis_config2);
                                    near2.client_config.skip_sync_wait = false;
                                    near2.client_config.min_num_peers = 1;
                                    near2.network_config.boot_nodes =
                                        convert_boot_nodes(vec![("test1", port1)]);

                                    let dir2 = TempDir::new("sync_nodes_2").unwrap();
                                    let (_, view_client2) = start_with_config(dir2.path(), near2);
                                    *view_client2_holder2 = Some(view_client2);
                                }
                            }
                            Ok(Ok(b)) if b.header.height < 101 => {
                                println!("FIRST STAGE {}", b.header.height)
                            }
                            Err(_) => return futures::future::err(()),
                            _ => {}
                        };
                        futures::future::ok(())
                    }));
                }

                if let Some(view_client2) = &*view_client2_holder.write().unwrap() {
                    actix::spawn(view_client2.send(GetBlock::Best).then(|res| {
                        match &res {
                            Ok(Ok(b)) if b.header.height >= 101 => System::current().stop(),
                            Ok(Ok(b)) if b.header.height < 101 => {
                                println!("SECOND STAGE {}", b.header.height)
                            }
                            Err(_) => return futures::future::err(()),
                            _ => {}
                        };
                        futures::future::ok(())
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
