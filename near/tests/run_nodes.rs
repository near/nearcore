use actix::{Actor, System};
use futures::future::Future;
use tempdir::TempDir;

use near::{load_test_config, start_with_config, GenesisConfig};
use near_client::GetBlock;
use near_network::test_utils::{convert_boot_nodes, open_port, WaitOrTimeout};
use near_primitives::test_utils::{heavy_test, init_test_logger};
use near_primitives::types::BlockIndex;

fn run_nodes(num_nodes: usize, epoch_length: BlockIndex, num_blocks: BlockIndex) {
    init_test_logger();

    let validators = (0..num_nodes).map(|i| format!("test{}", i + 1)).collect::<Vec<_>>();
    let mut genesis_config = GenesisConfig::test(validators.iter().map(|v| v.as_str()).collect());
    genesis_config.epoch_length = epoch_length;

    let mut near_configs = vec![];
    let first_node = open_port();
    for i in 0..num_nodes {
        let mut near_config = load_test_config(
            &validators[i],
            if i == 0 { first_node } else { open_port() },
            &genesis_config,
        );
        near_config.client_config.min_num_peers = num_nodes - 1;
        if i > 0 {
            near_config.network_config.boot_nodes =
                convert_boot_nodes(vec![(&validators[0], first_node)]);
        }
        near_configs.push(near_config);
    }

    let system = System::new("NEAR");

    let mut view_clients = vec![];
    for (i, near_config) in near_configs.drain(..).enumerate() {
        let dir = TempDir::new(&format!("two_nodes_{}", i)).unwrap();
        let (_client, view_client) = start_with_config(dir.path(), near_config);
        view_clients.push(view_client)
    }

    let view_client = view_clients.pop().unwrap();
    WaitOrTimeout::new(
        Box::new(move |_ctx| {
            actix::spawn(view_client.send(GetBlock::Best).then(move |res| {
                match &res {
                    Ok(Ok(b))
                        if b.header.height > num_blocks
                            && b.header.total_weight.to_num() > num_blocks =>
                    {
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

/// Runs two nodes that should produce blocks one after another.
#[test]
fn run_nodes_2() {
    heavy_test(|| {
        run_nodes(2, 10, 30);
    });
}

/// Runs 4 nodes that should produce blocks one after another.
#[test]
fn run_nodes_4() {
    heavy_test(|| {
        run_nodes(4, 8, 32);
    });
}
