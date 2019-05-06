use actix::{Actor, System};
use futures::future::Future;

use near::{start_with_config, GenesisConfig, NearConfig};
use near_client::{BlockProducer, GetBlock};
use near_network::test_utils::{convert_boot_nodes, WaitOrTimeout};
use near_primitives::test_utils::init_test_logger;
use tempdir::TempDir;

/// Runs two nodes that should produce blocks one after another.
#[test]
fn two_nodes() {
    init_test_logger();

    let genesis_config = GenesisConfig::test(vec!["test1", "test2"]);

    let mut near1 = NearConfig::new(genesis_config.genesis_time.clone(), "test1", 25123);
    near1.network_config.boot_nodes = convert_boot_nodes(vec![("test2", 25124)]);
    let mut near2 = NearConfig::new(genesis_config.genesis_time.clone(), "test2", 25124);
    near2.network_config.boot_nodes = convert_boot_nodes(vec![("test1", 25123)]);

    let system = System::new("NEAR");

    let dir1 = TempDir::new("two_nodes_1").unwrap();
    let client1 = start_with_config(
        dir1.path(),
        genesis_config.clone(),
        near1,
        Some(BlockProducer::test("test1")),
    );
    let dir2 = TempDir::new("two_nodes_2").unwrap();
    let _client2 =
        start_with_config(dir2.path(), genesis_config, near2, Some(BlockProducer::test("test2")));

    WaitOrTimeout::new(
        Box::new(move |_ctx| {
            actix::spawn(client1.send(GetBlock::Best).then(|res| {
                match &res {
                    Ok(Some(b)) if b.header.height > 2 && b.header.total_weight.to_num() > 2 => {
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
