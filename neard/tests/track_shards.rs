use std::sync::{Arc, RwLock};

use actix::{Actor, System};
use futures::{future, FutureExt};

use near_client::{GetBlock, GetChunk};
use near_network::test_utils::WaitOrTimeout;
use near_primitives::hash::CryptoHash;
use near_primitives::test_utils::{heavy_test, init_integration_logger};
use testlib::start_nodes;

#[test]
fn track_shards() {
    init_integration_logger();
    heavy_test(|| {
        let system = System::new("NEAR");
        let num_nodes = 4;
        let dirs = (0..num_nodes)
            .map(|i| {
                tempfile::Builder::new().prefix(&format!("track_shards_{}", i)).tempdir().unwrap()
            })
            .collect::<Vec<_>>();
        let (_, _, clients) = start_nodes(4, &dirs, 2, 0, 10, 0);
        let view_client = clients[clients.len() - 1].1.clone();
        let last_block_hash: Arc<RwLock<Option<CryptoHash>>> = Arc::new(RwLock::new(None));
        WaitOrTimeout::new(
            Box::new(move |_ctx| {
                let bh = last_block_hash.read().unwrap().map(|h| h.clone());
                if let Some(block_hash) = bh {
                    actix::spawn(view_client.send(GetChunk::BlockHash(block_hash, 3)).then(
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
                    actix::spawn(view_client.send(GetBlock::latest()).then(move |res| {
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
            20000,
        )
        .start();

        system.run().unwrap();
    });
}
