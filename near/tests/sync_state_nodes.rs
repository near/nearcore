use std::sync::Arc;

use actix::{Actor, System};
use futures::future::Future;
use tempdir::TempDir;

use near::{load_test_config, start_with_config, GenesisConfig, NightshadeRuntime};
use near_chain::{Block, Chain, ChainGenesis};
use near_client::GetBlock;
use near_network::test_utils::{convert_boot_nodes, open_port, WaitOrTimeout};
use near_network::{NetworkClientMessages, PeerInfo};
use near_primitives::crypto::signer::InMemorySigner;
use near_primitives::test_utils::init_test_logger;
use near_store::test_utils::create_test_store;

/// Utility to generate genesis header from config for testing purposes.
fn genesis(genesis_config: GenesisConfig) -> Block {
    let dir = TempDir::new("unused").unwrap();
    let store = create_test_store();
    let genesis_time = genesis_config.genesis_time.clone();
    let runtime = Arc::new(NightshadeRuntime::new(dir.path(), store.clone(), genesis_config));
    let mut chain =
        Chain::new(store, runtime, ChainGenesis::new(genesis_time, 1_000_000, 100)).unwrap();
    chain.get_block(&chain.genesis().hash()).unwrap().clone()
}

/// One client is in front, another must sync to it using state (fast) sync.
#[test]
fn sync_state_nodes() {
    init_test_logger();

    let genesis_config = GenesisConfig::test(vec!["other"]);
    let genesis = genesis(genesis_config.clone());

    let (port1, port2) = (open_port(), open_port());
    let mut near1 = load_test_config("test1", port1, &genesis_config);
    near1.network_config.boot_nodes = convert_boot_nodes(vec![("test2", port2)]);
    let mut near2 = load_test_config("test2", port2, &genesis_config);
    near2.client_config.skip_sync_wait = false;
    near2.client_config.min_num_peers = 1;
    near2.network_config.boot_nodes = convert_boot_nodes(vec![("test1", port1)]);

    let system = System::new("NEAR");

    let dir1 = TempDir::new("sync_nodes_1").unwrap();
    let (client1, _) = start_with_config(dir1.path(), near1);

    let mut blocks = vec![];
    let mut prev = &genesis;
    let signer = Arc::new(InMemorySigner::from_seed("other", "other"));
    for _ in 0..=100 {
        let block = Block::empty(prev, signer.clone());
        let _ = client1.do_send(NetworkClientMessages::Block(
            block.clone(),
            PeerInfo::random().id,
            true,
        ));
        blocks.push(block);
        prev = &blocks[blocks.len() - 1];
    }

    let dir2 = TempDir::new("sync_nodes_2").unwrap();
    let (_, view_client2) = start_with_config(dir2.path(), near2);

    WaitOrTimeout::new(
        Box::new(move |_ctx| {
            actix::spawn(view_client2.send(GetBlock::Best).then(|res| {
                match &res {
                    Ok(Ok(b)) if b.header.height >= 101 => System::current().stop(),
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
