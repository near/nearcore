use std::sync::{Arc, RwLock};

use actix::{Actor, System};
use futures::future::Future;
use tempdir::TempDir;

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
            .map(|i| TempDir::new(&format!("track_shards_{}", i)).unwrap())
            .collect::<Vec<_>>();
        let clients = start_nodes(4, &dirs, 2, 10);
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
                                _ => return futures::future::err(()),
                            };
                            futures::future::ok(())
                        },
                    ));
                } else {
                    let last_block_hash1 = last_block_hash.clone();
                    actix::spawn(view_client.send(GetBlock::Best).then(move |res| {
                        match &res {
                            Ok(Ok(b)) if b.header.height > 10 => {
                                *last_block_hash1.write().unwrap() =
                                    Some(b.header.hash.clone().into());
                            }
                            Err(_) => return futures::future::err(()),
                            _ => {}
                        };
                        futures::future::ok(())
                    }));
                }
            }),
            100,
            5000,
        )
        .start();

        system.run().unwrap();
    });
}
