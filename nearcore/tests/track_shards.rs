use std::sync::{Arc, RwLock};

use actix::{Actor, System};
use futures::{future, FutureExt};

use near_actix_test_utils::spawn_interruptible;
use near_client::{GetBlock, GetChunk};
use near_logger_utils::init_integration_logger;
use near_network::test_utils::WaitOrTimeout;
use near_primitives::hash::CryptoHash;

mod node_cluster;
use node_cluster::NodeCluster;

#[test]
fn track_shards() {
    init_integration_logger();

    let cluster = NodeCluster::new(4, |index| format!("track_shards_{}", index))
        .set_num_shards(4)
        .set_num_validator_seats(2)
        .set_num_lightclients(0)
        .set_epoch_length(10)
        .set_genesis_height(0);

    cluster.exec_until_stop(|_, _, clients| async move {
        let view_client = clients[clients.len() - 1].1.clone();
        let last_block_hash: Arc<RwLock<Option<CryptoHash>>> = Arc::new(RwLock::new(None));
        WaitOrTimeout::new(
            Box::new(move |_ctx| {
                let bh = last_block_hash.read().unwrap().map(|h| h.clone());
                if let Some(block_hash) = bh {
                    spawn_interruptible(view_client.send(GetChunk::BlockHash(block_hash, 3)).then(
                        move |res| {
                            match &res {
                                Ok(Ok(_)) => {
                                    System::current().stop();
                                }
                                _ => return future::ready(()),
                            };
                            future::ready(())
                        },
                    ));
                } else {
                    let last_block_hash1 = last_block_hash.clone();
                    spawn_interruptible(view_client.send(GetBlock::latest()).then(move |res| {
                        match &res {
                            Ok(Ok(b)) if b.header.height > 10 => {
                                *last_block_hash1.write().unwrap() =
                                    Some(b.header.hash.clone().into());
                            }
                            Err(_) => return future::ready(()),
                            _ => {}
                        };
                        future::ready(())
                    }));
                }
            }),
            100,
            30000,
        )
        .start();
    });
}
