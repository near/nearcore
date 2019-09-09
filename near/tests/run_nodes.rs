use actix::{Actor, System};
use futures::future::Future;
use tempdir::TempDir;

use near::{load_test_config, start_with_config, GenesisConfig};
use near_client::GetBlock;
use near_network::test_utils::{convert_boot_nodes, open_port, WaitOrTimeout};
use near_primitives::test_utils::{heavy_test, init_integration_logger};
use near_primitives::types::{BlockIndex, ShardId};

fn run_nodes(
    num_shards: usize,
    num_nodes: usize,
    num_validators: usize,
    epoch_length: BlockIndex,
    num_blocks: BlockIndex,
) {
    init_integration_logger();

    let seeds = (0..num_nodes).map(|i| format!("near.{}", i)).collect::<Vec<_>>();
    let mut genesis_config = GenesisConfig::test_sharded(
        seeds.iter().map(|s| s.as_str()).collect(),
        num_validators,
        (0..num_shards).map(|_| num_validators).collect(),
    );
    genesis_config.epoch_length = epoch_length;

    let validators = (0..num_validators).map(|i| format!("near.{}", i)).collect::<Vec<_>>();
    let mut near_configs = vec![];
    let first_node = open_port();
    for i in 0..num_nodes {
        let mut near_config = load_test_config(
            if i < num_validators { &validators[i] } else { "" },
            if i == 0 { first_node } else { open_port() },
            &genesis_config,
        );
        near_config.client_config.min_num_peers = num_nodes - 1;
        if i > 0 {
            near_config.network_config.boot_nodes =
                convert_boot_nodes(vec![("near.0", first_node)]);
        }
        // if non validator, add some shards to track.
        if i >= num_validators {
            let shards_per_node = num_shards / (num_nodes - num_validators);
            let (from, to) = (
                ((i - num_validators) * shards_per_node) as ShardId,
                ((i - num_validators + 1) * shards_per_node) as ShardId,
            );
            near_config.client_config.tracked_shards.extend(&(from..to).collect::<Vec<_>>());
        }
        near_configs.push(near_config);
    }

    let system = System::new("NEAR");

    let dirs = (0..num_nodes)
        .map(|i| {
            TempDir::new(&format!("run_nodes_{}_{}_{}", num_nodes, num_validators, i)).unwrap()
        })
        .collect::<Vec<_>>();
    let mut view_clients = vec![];
    for (i, near_config) in near_configs.into_iter().enumerate() {
        let (_client, view_client) = start_with_config(dirs[i].path(), near_config);
        view_clients.push(view_client)
    }

    let view_client = view_clients.pop().unwrap();
    WaitOrTimeout::new(
        Box::new(move |_ctx| {
            actix::spawn(view_client.send(GetBlock::Best).then(move |res| {
                match &res {
                    Ok(Ok(b))
                        if b.header.height > num_blocks && b.header.total_weight > num_blocks =>
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
fn run_nodes_1_2_2() {
    heavy_test(|| {
        run_nodes(1, 2, 2, 10, 30);
    });
}

/// Runs two nodes, where only one is a validator.
#[test]
fn run_nodes_1_2_1() {
    heavy_test(|| {
        run_nodes(1, 2, 1, 10, 30);
    });
}

/// Runs 4 nodes that should produce blocks one after another.
#[test]
fn run_nodes_1_4_4() {
    heavy_test(|| {
        run_nodes(1, 4, 4, 8, 32);
    });
}

/// Run 4 nodes, 4 shards, 2 validators, other two track 2 shards.
#[test]
fn run_nodes_4_4_2() {
    heavy_test(|| {
        run_nodes(4, 4, 2, 8, 32);
    });
}
