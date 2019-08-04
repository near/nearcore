use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use actix::{Actor, Addr, System};
use futures::future::Future;
use tempdir::TempDir;

use near::{load_test_config, start_with_config, GenesisConfig, NightshadeRuntime};
use near_chain::{Block, Chain, ChainGenesis};
use near_client::{ClientActor, GetBlock};
use near_network::test_utils::{convert_boot_nodes, open_port, WaitOrTimeout};
use near_network::{NetworkClientMessages, PeerInfo};
use near_primitives::crypto::signer::InMemorySigner;
use near_primitives::test_utils::init_test_logger;
use near_store::test_utils::create_test_store;

/// Utility to generate genesis header from config for testing purposes.
fn genesis_block(genesis_config: GenesisConfig) -> Block {
    let dir = TempDir::new("unused").unwrap();
    let store = create_test_store();
    let genesis_time = genesis_config.genesis_time.clone();
    let runtime =
        Arc::new(NightshadeRuntime::new(dir.path(), store.clone(), genesis_config.clone()));
    let mut chain = Chain::new(
        store,
        runtime,
        &ChainGenesis::new(
            genesis_time,
            genesis_config.gas_limit,
            genesis_config.gas_price,
            genesis_config.total_supply,
            genesis_config.max_inflation_rate,
            genesis_config.gas_price_adjustment_rate,
        ),
    )
    .unwrap();
    chain.get_block(&chain.genesis().hash()).unwrap().clone()
}

// This assumes that there is no index skipped. Otherwise epoch hash calculation will be wrong.
fn add_blocks(
    start: &Block,
    genesis_block: &Block,
    client: Addr<ClientActor>,
    num: usize,
    signer: Arc<InMemorySigner>,
) -> Block {
    let mut blocks = vec![];
    let mut prev = start;
    for _ in 0..num {
        let mut block = Block::empty(&prev, signer.clone());
        block.chunks = genesis_block.chunks.clone();
        block.header.prev_state_root = Block::compute_state_root(&block.chunks);
        let _ = client.do_send(NetworkClientMessages::Block(
            block.clone(),
            PeerInfo::random().id,
            false,
        ));
        blocks.push(block);
        prev = &blocks[blocks.len() - 1];
    }
    blocks[blocks.len() - 1].clone()
}

/// One client is in front, another must sync to it before they can produce blocks.
#[test]
fn sync_nodes() {
    init_test_logger();

    let mut genesis_config = GenesisConfig::test(vec!["other"]);
    genesis_config.epoch_length = 5;
    let genesis_block = genesis_block(genesis_config.clone());

    let (port1, port2) = (open_port(), open_port());
    let mut near1 = load_test_config("test1", port1, &genesis_config);
    near1.network_config.boot_nodes = convert_boot_nodes(vec![("test2", port2)]);
    let mut near2 = load_test_config("test2", port2, &genesis_config);
    near2.network_config.boot_nodes = convert_boot_nodes(vec![("test1", port1)]);

    let system = System::new("NEAR");

    let dir1 = TempDir::new("sync_nodes_1").unwrap();
    let (client1, _) = start_with_config(dir1.path(), near1);

    let signer = Arc::new(InMemorySigner::from_seed("other", "other"));
    let _ = add_blocks(&genesis_block, &genesis_block, client1, 11, signer);

    let dir2 = TempDir::new("sync_nodes_2").unwrap();
    let (_, view_client2) = start_with_config(dir2.path(), near2);

    WaitOrTimeout::new(
        Box::new(move |_ctx| {
            actix::spawn(view_client2.send(GetBlock::Best).then(|res| {
                match &res {
                    Ok(Ok(b)) if b.header.height == 11 => System::current().stop(),
                    Ok(Ok(b)) if b.header.height < 11 => println!("DAMN {}", b.header.height),
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

/// Clients connect and then one of them becomes in front. The other one must then sync to it.
#[test]
fn sync_after_sync_nodes() {
    init_test_logger();

    let mut genesis_config = GenesisConfig::test(vec!["other"]);
    genesis_config.epoch_length = 5;
    let genesis_block = genesis_block(genesis_config.clone());

    let (port1, port2) = (open_port(), open_port());
    let mut near1 = load_test_config("test1", port1, &genesis_config);
    near1.network_config.boot_nodes = convert_boot_nodes(vec![("test2", port2)]);
    let mut near2 = load_test_config("test2", port2, &genesis_config);
    near2.network_config.boot_nodes = convert_boot_nodes(vec![("test1", port1)]);

    let system = System::new("NEAR");

    let dir1 = TempDir::new("sync_nodes_1").unwrap();
    let (client1, _) = start_with_config(dir1.path(), near1);

    let dir2 = TempDir::new("sync_nodes_2").unwrap();
    let (_, view_client2) = start_with_config(dir2.path(), near2);

    let signer = Arc::new(InMemorySigner::from_seed("other", "other"));
    let last_block =
        add_blocks(&genesis_block, &genesis_block, client1.clone(), 11, signer.clone());

    let next_step = Arc::new(AtomicBool::new(false));
    WaitOrTimeout::new(
        Box::new(move |_ctx| {
            let last_block1 = last_block.clone();
            let client11 = client1.clone();
            let signer1 = signer.clone();
            let next_step1 = next_step.clone();
            let genesis_block1 = genesis_block.clone();
            actix::spawn(view_client2.send(GetBlock::Best).then(move |res| {
                match &res {
                    Ok(Ok(b)) if b.header.height == 11 => {
                        if !next_step1.load(Ordering::Relaxed) {
                            let _ =
                                add_blocks(&last_block1, &genesis_block1, client11, 11, signer1);
                            next_step1.store(true, Ordering::Relaxed);
                        }
                    }
                    Ok(Ok(b)) if b.header.height > 20 => System::current().stop(),
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
