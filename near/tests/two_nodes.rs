use actix::{Actor, System};
use futures::future::Future;
use tempdir::TempDir;

use near::{load_test_config, start_with_config, GenesisConfig};
use near_client::GetBlock;
use near_network::test_utils::{convert_boot_nodes, WaitOrTimeout};
use near_primitives::test_utils::init_test_logger;

/// Runs two nodes that should produce blocks one after another.
#[test]
fn two_nodes() {
    init_test_logger();

    let genesis_config = GenesisConfig::test(vec!["test1", "test2"]);

    let mut near1 = load_test_config("test1", 25123, &genesis_config);
    near1.network_config.boot_nodes = convert_boot_nodes(vec![("test2", 25124)]);
    let mut near2 = load_test_config("test2", 25124, &genesis_config);
    near2.network_config.boot_nodes = convert_boot_nodes(vec![("test1", 25123)]);

    let system = System::new("NEAR");

    let dir1 = TempDir::new("two_nodes_1").unwrap();
    let (_client1, view_client1) = start_with_config(dir1.path(), near1);
    let dir2 = TempDir::new("two_nodes_2").unwrap();
    let _ = start_with_config(dir2.path(), near2);

    WaitOrTimeout::new(
        Box::new(move |_ctx| {
            actix::spawn(view_client1.send(GetBlock::Best).then(|res| {
                match &res {
                    Ok(Ok(b)) if b.header.height > 2 && b.header.total_weight.to_num() > 2 => {
                        System::current().stop()
                    }
                    Err(_) => return futures::future::err(()),
                    _ => {}
                };
                futures::future::ok(())
            }));
        }),
        100,
        60000,
    )
    .start();

    system.run().unwrap();
}
