use crate::tests::nearcore::node_cluster::NodeCluster;
use near_async::messaging::CanSendAsync;
use near_client::GetBlock;
use near_network::test_utils::wait_or_timeout;
use near_primitives::types::{BlockHeightDelta, NumSeats, NumShards};
use rand::{Rng, thread_rng};
use std::ops::ControlFlow;

async fn run_heavy_nodes(
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

    cluster
        .run_and_then_shutdown(move |_, _, clients| async move {
            let view_client = clients.last().unwrap().1.clone();

            wait_or_timeout(100, 40000, || async {
                let res = view_client.send_async(GetBlock::latest()).await;
                match &res {
                    Ok(Ok(b)) if b.header.height > num_blocks => return ControlFlow::Break(()),
                    Err(_) => return ControlFlow::Continue(()),
                    _ => {}
                };
                ControlFlow::Continue(())
            })
            .await
            .unwrap();
            near_async::shutdown_all_actors();
        })
        .await;
}

/// Runs two nodes that should produce blocks one after another.
#[tokio::test]
async fn ultra_slow_test_run_nodes_1_2_2() {
    run_heavy_nodes(1, 2, 2, 10, 30).await;
}

/// Runs two nodes, where only one is a validator.
#[tokio::test]
async fn ultra_slow_test_run_nodes_1_2_1() {
    run_heavy_nodes(1, 2, 1, 10, 30).await;
}

/// Runs 4 nodes that should produce blocks one after another.
#[tokio::test]
async fn slow_test_run_nodes_1_4_4() {
    run_heavy_nodes(1, 4, 4, 8, 32).await;
}

/// Run 4 nodes, 4 shards, 2 validators, other two track 2 shards.
#[tokio::test]
async fn slow_test_run_nodes_4_4_2() {
    run_heavy_nodes(4, 4, 2, 8, 32).await;
}
