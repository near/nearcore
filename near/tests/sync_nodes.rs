use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use actix::{Actor, Addr, System};
use futures::future;
use futures::future::Future;
use tempdir::TempDir;

use near::config::TESTING_INIT_STAKE;
use near::{load_test_config, start_with_config, GenesisConfig, NightshadeRuntime};
use near_chain::{Block, Chain, ChainGenesis};
use near_client::{ClientActor, GetBlock};
use near_network::test_utils::{convert_boot_nodes, open_port, WaitOrTimeout};
use near_network::{NetworkClientMessages, PeerInfo};
use near_primitives::crypto::signer::InMemorySigner;
use near_primitives::serialize::BaseEncode;
use near_primitives::test_utils::{init_integration_logger, init_test_logger};
use near_primitives::transaction::{StakeTransaction, TransactionBody};
use near_primitives::types::{BlockIndex, EpochId};
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
    mut blocks: Vec<Block>,
    client: Addr<ClientActor>,
    num: usize,
    epoch_length: BlockIndex,
    signer: Arc<InMemorySigner>,
) -> Vec<Block> {
    let mut prev = &blocks[blocks.len() - 1];
    for _ in 0..num {
        let epoch_id = match prev.header.height + 1 {
            height if height <= epoch_length => EpochId::default(),
            height => {
                EpochId(blocks[(((height - 1) / epoch_length - 1) * epoch_length) as usize].hash())
            }
        };
        let block = Block::produce(
            &prev.header,
            prev.header.height + 1,
            blocks[0].chunks.clone(),
            epoch_id,
            vec![],
            HashMap::default(),
            0,
            0,
            signer.clone(),
        );
        let _ = client.do_send(NetworkClientMessages::Block(
            block.clone(),
            PeerInfo::random().id,
            false,
        ));
        blocks.push(block);
        prev = &blocks[blocks.len() - 1];
    }
    blocks
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
    near1.client_config.min_num_peers = 1;
    let mut near2 = load_test_config("test2", port2, &genesis_config);
    near2.network_config.boot_nodes = convert_boot_nodes(vec![("test1", port1)]);
    near2.client_config.min_num_peers = 1;

    let system = System::new("NEAR");

    let dir1 = TempDir::new("sync_nodes_1").unwrap();
    let (client1, _) = start_with_config(dir1.path(), near1);

    let signer = Arc::new(InMemorySigner::from_seed("other", "other"));
    let _ = add_blocks(vec![genesis_block], client1, 12, genesis_config.epoch_length, signer);

    let dir2 = TempDir::new("sync_nodes_2").unwrap();
    let (_, view_client2) = start_with_config(dir2.path(), near2);

    WaitOrTimeout::new(
        Box::new(move |_ctx| {
            actix::spawn(view_client2.send(GetBlock::Best).then(|res| {
                match &res {
                    Ok(Ok(b)) if b.header.height == 12 => System::current().stop(),
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
    near1.client_config.min_num_peers = 1;
    let mut near2 = load_test_config("test2", port2, &genesis_config);
    near2.network_config.boot_nodes = convert_boot_nodes(vec![("test1", port1)]);
    near2.client_config.min_num_peers = 1;

    let system = System::new("NEAR");

    let dir1 = TempDir::new("sync_nodes_1").unwrap();
    let (client1, _) = start_with_config(dir1.path(), near1);

    let dir2 = TempDir::new("sync_nodes_2").unwrap();
    let (_, view_client2) = start_with_config(dir2.path(), near2);

    let signer = Arc::new(InMemorySigner::from_seed("other", "other"));
    let blocks = add_blocks(
        vec![genesis_block],
        client1.clone(),
        12,
        genesis_config.epoch_length,
        signer.clone(),
    );

    let next_step = Arc::new(AtomicBool::new(false));
    let epoch_length = genesis_config.epoch_length;
    WaitOrTimeout::new(
        Box::new(move |_ctx| {
            let blocks1 = blocks.clone();
            let client11 = client1.clone();
            let signer1 = signer.clone();
            let next_step1 = next_step.clone();
            actix::spawn(view_client2.send(GetBlock::Best).then(move |res| {
                match &res {
                    Ok(Ok(b)) if b.header.height == 12 => {
                        if !next_step1.load(Ordering::Relaxed) {
                            let _ = add_blocks(blocks1, client11, 11, epoch_length, signer1);
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

/// Starts one validation node, it reduces it's stake to 1/2 of the stake.
/// Second node starts after 1s, needs to catchup & state sync and then make sure it's
#[test]
fn sync_state_stake_change() {
    init_integration_logger();

    let mut genesis_config = GenesisConfig::test(vec!["test1"]);
    genesis_config.epoch_length = 5;

    let (port1, port2) = (open_port(), open_port());
    let mut near1 = load_test_config("test1", port1, &genesis_config);
    near1.network_config.boot_nodes = convert_boot_nodes(vec![("test2", port2)]);
    near1.client_config.min_num_peers = 0;
    near1.client_config.min_block_production_delay = Duration::from_millis(200);
    let mut near2 = load_test_config("test2", port2, &genesis_config);
    near2.network_config.boot_nodes = convert_boot_nodes(vec![("test1", port1)]);
    near2.client_config.min_block_production_delay = Duration::from_millis(200);
    near2.client_config.min_num_peers = 1;
    near2.client_config.skip_sync_wait = false;

    let system = System::new("NEAR");

    let unstake_transaction = TransactionBody::Stake(StakeTransaction {
        nonce: 1,
        originator: "test1".to_string(),
        amount: TESTING_INIT_STAKE / 2,
        public_key: near1.block_producer.as_ref().unwrap().signer.public_key().to_base(),
    })
    .sign(&*near1.block_producer.as_ref().unwrap().signer);

    let dir1 = TempDir::new("sync_state_stake_change_1").unwrap();
    let dir2 = TempDir::new("sync_state_stake_change_2").unwrap();
    let (client1, view_client1) = start_with_config(dir1.path(), near1);

    actix::spawn(
        client1
            .send(NetworkClientMessages::Transaction(unstake_transaction))
            .map(|_| ())
            .map_err(|_| ()),
    );

    let started = Arc::new(AtomicBool::new(false));
    let dir2_path = dir2.path().to_path_buf();
    WaitOrTimeout::new(
        Box::new(move |_ctx| {
            let started_copy = started.clone();
            let near2_copy = near2.clone();
            let dir2_path_copy = dir2_path.clone();
            actix::spawn(view_client1.send(GetBlock::Best).then(move |res| {
                let latest_block_height = res.unwrap().unwrap().header.height;
                if !started_copy.load(Ordering::SeqCst) && latest_block_height > 10 {
                    started_copy.store(true, Ordering::SeqCst);
                    let (_, view_client2) = start_with_config(&dir2_path_copy, near2_copy);

                    WaitOrTimeout::new(
                        Box::new(move |_ctx| {
                            actix::spawn(view_client2.send(GetBlock::Best).then(move |res| {
                                if let Ok(block) = res.unwrap() {
                                    if block.header.height > latest_block_height + 1 {
                                        System::current().stop()
                                    }
                                }
                                future::result::<_, ()>(Ok(()))
                            }));
                        }),
                        100,
                        30000,
                    )
                    .start();
                }
                future::result(Ok(()))
            }));
        }),
        100,
        35000,
    )
    .start();

    system.run().unwrap();
}
