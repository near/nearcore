use std::ops::ControlFlow;
use std::sync::{Arc, RwLock};

use actix::System;

use near_client::{GetBlock, GetChunk};
use near_network::test_utils::wait_or_timeout;
use near_o11y::testonly::init_integration_logger;
use near_o11y::WithSpanContextExt;
use near_primitives::hash::CryptoHash;

use crate::tests::nearcore::node_cluster::NodeCluster;

#[test]
#[cfg_attr(not(feature = "expensive_tests"), ignore)]
fn track_shards() {
    init_integration_logger();

    let cluster = NodeCluster::default()
        .set_num_shards(4)
        .set_num_nodes(4)
        .set_num_validator_seats(2)
        .set_num_lightclients(0)
        .set_epoch_length(10)
        .set_genesis_height(0);

    cluster.exec_until_stop(|_, _, clients| async move {
        let view_client = clients[clients.len() - 1].1.clone();
        let last_block_hash: Arc<RwLock<Option<CryptoHash>>> = Arc::new(RwLock::new(None));
        wait_or_timeout(100, 30000, || async {
            let bh = *last_block_hash.read().unwrap();
            if let Some(block_hash) = bh {
                let res =
                    view_client.send(GetChunk::BlockHash(block_hash, 3).with_span_context()).await;
                match &res {
                    Ok(Ok(_)) => {
                        return ControlFlow::Break(());
                    }
                    _ => {
                        return ControlFlow::Continue(());
                    }
                }
            } else {
                let last_block_hash1 = last_block_hash.clone();
                let res = view_client.send(GetBlock::latest().with_span_context()).await;
                match &res {
                    Ok(Ok(b)) if b.header.height > 10 => {
                        *last_block_hash1.write().unwrap() = Some(b.header.hash);
                    }
                    Err(_) => return ControlFlow::Continue(()),
                    _ => {}
                };
                ControlFlow::Continue(())
            }
        })
        .await
        .unwrap();
        System::current().stop()
    });
}
