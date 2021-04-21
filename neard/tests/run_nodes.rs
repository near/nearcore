use actix::{Actor, System};
use futures::{future, FutureExt};

use near_actix_test_utils::spawn_interruptible;
use near_client::GetBlock;
use near_network::test_utils::WaitOrTimeout;
use near_primitives::types::{BlockHeightDelta, NumSeats, NumShards};
use rand::{thread_rng, Rng};

mod node_cluster;
use node_cluster::NodeCluster;

fn run_heavy_nodes(
    num_shards: NumShards,
    num_nodes: NumSeats,
    num_validators: NumSeats,
    epoch_length: BlockHeightDelta,
    num_blocks: BlockHeightDelta,
) {
    let mut rng = thread_rng();
    let genesis_height = rng.gen_range(0, 10000);

    let cluster = NodeCluster::new(num_nodes as usize, |index| {
        format!("run_nodes_{}_{}_{}", num_nodes, num_validators, index)
    })
    .set_num_shards(num_shards)
    .set_num_validator_seats(num_validators)
    .set_num_lightclients(0)
    .set_epoch_length(epoch_length)
    .set_genesis_height(genesis_height);

    cluster.exec_until_stop(|_, _, clients| async move {
        let view_client = clients.last().unwrap().1.clone();

        WaitOrTimeout::new(
            Box::new(move |_ctx| {
                spawn_interruptible(view_client.send(GetBlock::latest()).then(move |res| {
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
    });

    // See https://github.com/near/nearcore/issues/3925 for why it is here.
    //
    // The TL;DR is that actix doesn't allow to cleanly shut down multi-arbiter
    // actor systems, and that might cause RocksDB destructors to run when the
    // test binary exits, breaking stuff. This sleep here is a best-effort to
    // let those destructors finish. This should make the tests less flaky.
    // Hopefully, we'll be able to fix this properly by replacing actix actor
    // framework with something that handles cancellation gracefully.
    std::thread::sleep(std::time::Duration::from_millis(250));
}

/// Runs two nodes that should produce blocks one after another.
#[test]
fn run_nodes_1_2_2() {
    run_heavy_nodes(1, 2, 2, 10, 30);
}

/// Runs two nodes, where only one is a validator.
#[test]
fn run_nodes_1_2_1() {
    run_heavy_nodes(1, 2, 1, 10, 30);
}

/// Runs 4 nodes that should produce blocks one after another.
#[test]
fn run_nodes_1_4_4() {
    run_heavy_nodes(1, 4, 4, 8, 32);
}

/// Run 4 nodes, 4 shards, 2 validators, other two track 2 shards.
#[test]
fn run_nodes_4_4_2() {
    run_heavy_nodes(4, 4, 2, 8, 32);
}
