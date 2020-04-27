use actix::{Actor, System};
use futures::{future, FutureExt};

use near_client::GetBlock;
use near_network::test_utils::WaitOrTimeout;
use near_primitives::test_utils::heavy_test;
use near_primitives::types::{BlockHeightDelta, NumSeats, NumShards};
use rand::{thread_rng, Rng};
use testlib::start_nodes;

fn run_nodes(
    num_shards: NumShards,
    num_nodes: NumSeats,
    num_validators: NumSeats,
    epoch_length: BlockHeightDelta,
    num_blocks: BlockHeightDelta,
) {
    let mut rng = thread_rng();
    let genesis_height = rng.gen_range(0, 10000);
    let system = System::new("NEAR");
    let dirs = (0..num_nodes)
        .map(|i| {
            tempfile::Builder::new()
                .prefix(&format!("run_nodes_{}_{}_{}", num_nodes, num_validators, i))
                .tempdir()
                .unwrap()
        })
        .collect::<Vec<_>>();
    let (_, _, clients) =
        start_nodes(num_shards, &dirs, num_validators, 0, epoch_length, genesis_height);
    let view_client = clients[clients.len() - 1].1.clone();
    WaitOrTimeout::new(
        Box::new(move |_ctx| {
            actix::spawn(view_client.send(GetBlock::latest()).then(move |res| {
                match &res {
                    Ok(Ok(b)) if b.header.height > num_blocks => System::current().stop(),
                    Err(_) => return future::ready(()),
                    _ => {}
                };
                future::ready(())
            }));
        }),
        100,
        40000,
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
