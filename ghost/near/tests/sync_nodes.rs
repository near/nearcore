use actix::{Actor, System};

use futures::future::Future;
use near::{start_with_config, NearConfig, GenesisConfig, NightshadeRuntime};
use near_client::{GetBlock, BlockProducer};
use near_network::test_utils::{convert_boot_nodes, WaitOrTimeout};
use primitives::test_utils::init_test_logger;
use near_chain::{BlockHeader, Chain, Block};
use std::sync::Arc;
use near_store::test_utils::create_test_store;
use near_network::{NetworkClientMessages, PeerInfo};
use primitives::crypto::signer::InMemorySigner;

/// Utility to generate genesis header from config for testing purposes.
fn genesis_header(genesis_config: GenesisConfig) -> BlockHeader {
    let store = create_test_store();
    let genesis_time = genesis_config.genesis_time.clone();
    let runtime = Arc::new(NightshadeRuntime::new(store.clone(), genesis_config));
    let chain = Chain::new(store, runtime, genesis_time).unwrap();
    chain.genesis().clone()
}

/// One client is in front, another must sync to it before they can produce blocks.
#[test]
fn sync_nodes() {
    init_test_logger();

    let genesis_config = GenesisConfig::test(vec!["test2"]);
    let genesis_header = genesis_header(genesis_config.clone());

    let mut near1 = NearConfig::new(genesis_config.genesis_time.clone(), "test1", 25123);
    near1.network_config.boot_nodes = convert_boot_nodes(vec![("test2", 25124)]);
    let mut near2 = NearConfig::new(genesis_config.genesis_time.clone(), "test2", 25124);
    near2.network_config.boot_nodes = convert_boot_nodes(vec![("test1", 25123)]);

    let system = System::new("NEAR");

    let client1 =
        start_with_config(genesis_config.clone(), near1, Some(BlockProducer::test("test1")));

    let mut blocks = vec![];
    let mut prev = &genesis_header;
    let signer = Arc::new(InMemorySigner::from_seed("test2", "test2"));
    for _ in 0..10 {
        blocks.push(Block::empty(prev, signer.clone()));
        prev = &blocks[blocks.len() - 1].header;
        let _ = client1.do_send(NetworkClientMessages::Block(blocks[blocks.len() - 1].clone(), PeerInfo::random().id, true));
    }

    let _client2 = start_with_config(genesis_config, near2, Some(BlockProducer::test("test2")));

    WaitOrTimeout::new(
        Box::new(move |_ctx| {
            actix::spawn(client1.send(GetBlock::Best).then(|res| {
                match &res {
                    Ok(Some(b)) if b.header.height > 14 => {
                        System::current().stop()
                    }
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