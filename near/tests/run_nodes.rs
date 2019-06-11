use actix::{Actor, System};
use futures::future::Future;
use tempdir::TempDir;

use near::{load_test_config, start_with_config, GenesisConfig};
use near_client::GetBlock;
use near_network::test_utils::{convert_boot_nodes, WaitOrTimeout, open_port};
use near_primitives::test_utils::init_test_logger;

fn run_nodes(num_nodes: usize) {
    init_test_logger();

    let validators = (0..num_nodes).map(|i| format!("test{}", i + 1)).collect::<Vec<_>>();
    let genesis_config = GenesisConfig::test(validators.iter().map(|v| v.as_str()).collect());

    let mut near_configs = vec![];
    let first_node = open_port();
    for i in 0..num_nodes {
        let mut near_config = load_test_config(&validators[i], if i == 0 { first_node } else { open_port() }, &genesis_config);
        if i > 0 {
            near_config.network_config.boot_nodes = convert_boot_nodes(vec![(&validators[0], first_node)]);
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
            actix::spawn(view_client.send(GetBlock::Best).then(|res| {
                match &res {
                    Ok(Ok(b)) if b.header.height > 4 && b.header.total_weight.to_num() > 6 => {
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
    run_nodes(2);
}

#[test]
fn run_nodes_4() {
    run_nodes(4);
}
