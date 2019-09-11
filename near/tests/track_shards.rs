use std::sync::{Arc, RwLock};

use actix::{Actor, Addr, System};
use futures::future::Future;
use tempdir::TempDir;

use near::{load_test_config, start_with_config, GenesisConfig};
use near_client::{ClientActor, GetBlock, GetChunk, ViewClientActor};
use near_network::test_utils::{convert_boot_nodes, open_port, WaitOrTimeout};
use near_primitives::test_utils::{heavy_test, init_integration_logger};
use near_primitives::types::{BlockIndex, ShardId};
use testlib::start_nodes;

#[test]
fn track_shards() {
    let clients = start_nodes(num_shards, num_nodes, num_validators, epoch_length, num_blocks);
    let view_client = clients[clients.len() - 1].1.clone();
    let last_block_hash = Arc::new(RwLock::new(None));
    WaitOrTimeout::new(
        Box::new(move |_ctx| {
            if let Some(block_hash) = last_block_hash.read().unwrap() {
                actix::spwan(view_client.send(GetChunk::BlockHash(block_hash, num_shards - 1)))
                    .then(move |res| match &res {});
            } else {
                actix::spawn(view_client.send(GetBlock::Best).then(move |res| {
                    match &res {
                        Ok(Ok(b)) if b.header.height > 10 => {
                            *last_block_hash.write().unwrap() = Some(b.hash);
                        }
                        Err(_) => return futures::future::err(()),
                        _ => {}
                    };
                    futures::future::ok(())
                }));
            }
        }),
        100,
        60000,
    )
    .start();

    system.run().unwrap();
}
