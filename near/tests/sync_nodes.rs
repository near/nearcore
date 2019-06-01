use std::sync::Arc;

use actix::{Actor, System};
use futures::future::Future;
use tempdir::TempDir;

use near::{load_test_config, start_with_config, GenesisConfig, NightshadeRuntime};
use near_chain::{Block, BlockHeader, Chain};
use near_client::GetBlock;
use near_network::test_utils::{convert_boot_nodes, WaitOrTimeout};
use near_network::{NetworkClientMessages, PeerInfo};
use near_primitives::crypto::signer::InMemorySigner;
use near_primitives::test_utils::init_test_logger;
use near_store::test_utils::create_test_store;

/// Utility to generate genesis header from config for testing purposes.
fn genesis_header(genesis_config: GenesisConfig) -> BlockHeader {
    let dir = TempDir::new("unused").unwrap();
    let store = create_test_store();
    let genesis_time = genesis_config.genesis_time.clone();
    let runtime = Arc::new(NightshadeRuntime::new(dir.path(), store.clone(), genesis_config));
    let chain = Chain::new(store, runtime, genesis_time).unwrap();
    chain.genesis().clone()
}

/// One client is in front, another must sync to it before they can produce blocks.
#[test]
fn sync_nodes() {
    init_test_logger();

    let genesis_config = GenesisConfig::test(vec!["other"]);
    let genesis_header = genesis_header(genesis_config.clone());

    let mut near1 = load_test_config("test1", 25123, &genesis_config);
    near1.network_config.boot_nodes = convert_boot_nodes(vec![("test2", 25124)]);
    let mut near2 = load_test_config("test2", 25124, &genesis_config);
    near2.network_config.boot_nodes = convert_boot_nodes(vec![("test1", 25123)]);

    let system = System::new("NEAR");

    let dir1 = TempDir::new("sync_nodes_1").unwrap();
    let (client1, _) = start_with_config(dir1.path(), near1);

    let mut blocks = vec![];
    let mut prev = &genesis_header;
    let signer = Arc::new(InMemorySigner::from_seed("other", "other"));
    for _ in 0..=10 {
        let block = Block::empty(prev, signer.clone());
        let _ = client1.do_send(NetworkClientMessages::Block(
            block.clone(),
            PeerInfo::random().id,
            true,
        ));
        blocks.push(block);
        prev = &blocks[blocks.len() - 1].header;
    }

    let dir2 = TempDir::new("sync_nodes_2").unwrap();
    let (_, view_client2) = start_with_config(dir2.path(), near2);

    WaitOrTimeout::new(
        Box::new(move |_ctx| {
            actix::spawn(view_client2.send(GetBlock::Best).then(|res| {
                match &res {
                    Ok(Ok(b)) if b.header.height == 11 => System::current().stop(),
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
