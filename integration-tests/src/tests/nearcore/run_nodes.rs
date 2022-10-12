use crate::tests::nearcore::node_cluster::NodeCluster;
use actix::System;
use near_client::GetBlock;
use near_network::test_utils::wait_or_timeout;
use near_o11y::WithSpanContextExt;
use near_primitives::types::{BlockHeightDelta, NumSeats, NumShards};
use rand::{thread_rng, Rng};
use std::ops::ControlFlow;

fn run_heavy_nodes(
    num_shards: NumShards,
    num_nodes: NumSeats,
    num_validators: NumSeats,
    epoch_length: BlockHeightDelta,
    num_blocks: BlockHeightDelta,
) {
    let mut rng = thread_rng();
    let genesis_height = rng.gen_range(0..10000);

    let cluster = NodeCluster::default()
        .set_num_shards(num_shards)
        .set_num_nodes(num_nodes)
        .set_num_validator_seats(num_validators)
        .set_num_lightclients(0)
        .set_epoch_length(epoch_length)
        .set_genesis_height(genesis_height);

    cluster.exec_until_stop(|_, _, clients| async move {
        let view_client = clients.last().unwrap().1.clone();

        wait_or_timeout(100, 40000, || async {
            let res = view_client.send(GetBlock::latest().with_span_context()).await;
            match &res {
                Ok(Ok(b)) if b.header.height > num_blocks => return ControlFlow::Break(()),
                Err(_) => return ControlFlow::Continue(()),
                _ => {}
            };
            ControlFlow::Continue(())
        })
        .await
        .unwrap();
        System::current().stop()
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
#[cfg_attr(not(feature = "expensive_tests"), ignore)]
fn run_nodes_1_2_2() {
    run_heavy_nodes(1, 2, 2, 10, 30);
}

/// Runs two nodes, where only one is a validator.
#[test]
#[cfg_attr(not(feature = "expensive_tests"), ignore)]
fn run_nodes_1_2_1() {
    run_heavy_nodes(1, 2, 1, 10, 30);
}

/// Runs 4 nodes that should produce blocks one after another.
#[test]
#[cfg_attr(not(feature = "expensive_tests"), ignore)]
fn run_nodes_1_4_4() {
    run_heavy_nodes(1, 4, 4, 8, 32);
}

/// Run 4 nodes, 4 shards, 2 validators, other two track 2 shards.
#[test]
#[cfg_attr(not(feature = "expensive_tests"), ignore)]
fn run_nodes_4_4_2() {
    run_heavy_nodes(4, 4, 2, 8, 32);
}
