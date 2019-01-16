extern crate beacon;
extern crate chain;
extern crate network;
extern crate primitives;
extern crate storage;
extern crate substrate_network_libp2p;

extern crate futures;
extern crate parking_lot;
extern crate tokio;

use beacon::types::{BeaconBlockChain, SignedBeaconBlock};
use futures::sync::mpsc::{channel, Receiver, Sender};
use futures::{future, stream, Future, Sink, Stream};
use network::message::Message;
use network::protocol::{Protocol, ProtocolConfig};
use network::service::{new_network_service, spawn_network_tasks};
use network::test_utils::{
    create_secret, raw_key_to_peer_id_str, test_config, test_config_with_secret,
};
use parking_lot::Mutex;
use primitives::hash::CryptoHash;
use std::sync::Arc;
use std::thread;
use std::time::{self, Duration};
use storage::test_utils::create_memory_db;
use substrate_network_libp2p::NodeIndex;
use tokio::timer::Delay;

fn spawn_simple_block_import_task(
    beacon_chain: Arc<BeaconBlockChain>,
    receiver: Receiver<SignedBeaconBlock>,
) {
    let task = receiver
        .fold(beacon_chain, |chain, block| {
            chain.insert_block(block);
            future::ok(chain)
        })
        .and_then(|_| Ok(()));
    tokio::spawn(task);
}

/// just create one block, add to the chain, and send through
/// the channel
fn spawn_simple_block_prod_task(
    beacon_chain: Arc<BeaconBlockChain>,
    sender: Sender<SignedBeaconBlock>,
) {
    let prev_hash = beacon_chain.best_hash();
    let prev_index = beacon_chain.best_index();
    let new_block =
        SignedBeaconBlock::new(prev_index + 1, prev_hash, vec![], CryptoHash::default());
    beacon_chain.insert_block(new_block.clone());
    tokio::spawn({
        let block_tx = sender.clone();
        block_tx.send(new_block).map(|_| ()).map_err(|_| ())
    });
}

fn get_test_protocol(
    chain: Arc<BeaconBlockChain>,
    block_tx: Sender<SignedBeaconBlock>,
    message_tx: Sender<(NodeIndex, Message)>,
) -> Protocol {
    let (transaction_tx, _) = channel(1024);
    let (gossip_tx, _) = channel(1024);
    Protocol::new(ProtocolConfig::default(), chain, block_tx, transaction_tx, message_tx, gossip_tx)
}

#[test]
fn test_block_catch_up_from_start() {
    use chain::SignedBlock;
    let storage1 = Arc::new(create_memory_db());
    let genesis_block =
        SignedBeaconBlock::new(0, CryptoHash::default(), vec![], CryptoHash::default());
    // chain1
    let beacon_chain1 = Arc::new(BeaconBlockChain::new(genesis_block.clone(), storage1.clone()));
    let mut block1 = SignedBeaconBlock::new(1, genesis_block.hash, vec![], CryptoHash::default());
    let mut block2 = SignedBeaconBlock::new(2, block1.hash, vec![], CryptoHash::default());
    block1.add_signature(primitives::signature::DEFAULT_SIGNATURE);
    block2.add_signature(primitives::signature::DEFAULT_SIGNATURE);
    beacon_chain1.insert_block(block1.clone());
    beacon_chain1.insert_block(block2.clone());
    assert_eq!(beacon_chain1.best_index(), 2);
    let (block_tx1, _) = channel(1024);
    let (_, block_outgoing_rx1) = channel(1024);
    let (message_tx1, message_rx1) = channel(1024);
    let (_, authority_rx1) = channel(1024);
    let protocol1 = get_test_protocol(beacon_chain1.clone(), block_tx1, message_tx1);
    let addr = "/ip4/127.0.0.1/tcp/30000";
    let secret = create_secret();
    let config1 = test_config_with_secret(addr, vec![], secret);
    let network_service1 =
        Arc::new(Mutex::new(new_network_service(&ProtocolConfig::default(), config1)));

    //chain2
    let storage2 = Arc::new(create_memory_db());
    let beacon_chain2 = Arc::new(BeaconBlockChain::new(genesis_block.clone(), storage2.clone()));
    assert_eq!(beacon_chain2.best_index(), 0);
    let (block_tx2, block_rx2) = channel(1024);
    let (_, block_outgoing_rx2) = channel(1024);
    let (message_tx2, message_rx2) = channel(1024);
    let (_, authority_rx2) = channel(1024);
    let protocol2 = get_test_protocol(beacon_chain2.clone(), block_tx2, message_tx2);
    let boot_node = "/ip4/127.0.0.1/tcp/30000/p2p/".to_string() + &raw_key_to_peer_id_str(secret);
    let config2 = test_config("/ip4/127.0.0.1/tcp/30001", vec![boot_node]);
    let network_service2 =
        Arc::new(Mutex::new(new_network_service(&ProtocolConfig::default(), config2)));

    // chain2 should catch up with chain1
    let (_, gossip_rx1) = channel(1024);
    let (_, gossip_rx2) = channel(1024);
    let task = futures::lazy({
        let chain = beacon_chain2.clone();
        move || {
            spawn_network_tasks(
                network_service1,
                protocol1,
                message_rx1,
                block_outgoing_rx1,
                authority_rx1,
                gossip_rx1,
            );
            spawn_network_tasks(
                network_service2,
                protocol2,
                message_rx2,
                block_outgoing_rx2,
                authority_rx2,
                gossip_rx2,
            );
            spawn_simple_block_import_task(chain, block_rx2);
            Ok(())
        }
    });
    let handle = thread::spawn(move || {
        tokio::run(task);
    });
    while beacon_chain2.best_index() < 2 {
        thread::sleep(Duration::from_secs(1));
    }

    // This is hacky
    std::mem::drop(handle);
}

// this test is expensive to run. should be ignored when running
// all tests
#[test]
#[ignore]
fn test_block_catchup_from_network_interruption() {
    let storage1 = Arc::new(create_memory_db());
    let genesis_block =
        SignedBeaconBlock::new(0, CryptoHash::default(), vec![], CryptoHash::default());
    // chain1
    let beacon_chain1 = Arc::new(BeaconBlockChain::new(genesis_block.clone(), storage1.clone()));
    let (block_tx1, _) = channel(1024);
    let (_, block_outgoing_rx1) = channel(1024);
    let (message_tx1, message_rx1) = channel(1024);
    let (_, authority_rx1) = channel(1024);
    let protocol1 = get_test_protocol(beacon_chain1.clone(), block_tx1, message_tx1);
    let addr = "/ip4/127.0.0.1/tcp/30002";
    let secret = create_secret();
    let config1 = test_config_with_secret(addr, vec![], secret);
    let network_service1 =
        Arc::new(Mutex::new(new_network_service(&ProtocolConfig::default(), config1)));

    //chain2
    let storage2 = Arc::new(create_memory_db());
    let beacon_chain2 = Arc::new(BeaconBlockChain::new(genesis_block.clone(), storage2.clone()));
    let (block_tx2, block_rx2) = channel(1024);
    let (_, block_outgoing_rx2) = channel(1024);
    let (message_tx2, message_rx2) = channel(1024);
    let (_, authority_rx2) = channel(1024);
    let protocol2 = get_test_protocol(beacon_chain2.clone(), block_tx2, message_tx2);
    let boot_node = "/ip4/127.0.0.1/tcp/30002/p2p/".to_string() + &raw_key_to_peer_id_str(secret);
    let config2 = test_config("/ip4/127.0.0.1/tcp/30003", vec![boot_node]);
    let network_service2 =
        Arc::new(Mutex::new(new_network_service(&ProtocolConfig::default(), config2)));

    // first both protocol produces some blocks, then protocol2 drops out of the network
    // and then comes back
    let network_service = network_service2.clone();
    let block_task =
        Delay::new(time::Instant::now() + Duration::from_secs(1)).map_err(|_| ()).and_then({
            let chain1 = beacon_chain1.clone();
            let chain2 = beacon_chain2.clone();
            move |_| {
                stream::iter_ok::<_, ()>(1..3)
                    .for_each(move |_| {
                        let prev_hash = chain1.best_hash();
                        let prev_index = chain1.best_index();
                        let new_block = SignedBeaconBlock::new(
                            prev_index + 1,
                            prev_hash,
                            vec![],
                            CryptoHash::default(),
                        );
                        chain1.insert_block(new_block.clone());
                        chain2.insert_block(new_block.clone());
                        Ok(())
                    })
                    .and_then(move |_| {
                        // protocol2 leaves network
                        for i in 0..2 {
                            network_service.lock().drop_node(i);
                        }
                        Delay::new(time::Instant::now() + Duration::from_millis(500))
                            .map_err(|_| ())
                    })
                    .and_then({
                        let chain = beacon_chain1.clone();
                        move |_| {
                            // protocol1 produces a block
                            let hash = chain.best_hash();
                            let index = chain.best_index() + 1;
                            let new_block =
                                SignedBeaconBlock::new(index, hash, vec![], CryptoHash::default());
                            chain.insert_block(new_block);
                            Delay::new(time::Instant::now() + Duration::from_millis(500))
                                .map_err(|_| ())
                        }
                    })
            }
        });

    let (_, gossip_rx1) = channel(1024);
    let (_, gossip_rx2) = channel(1024);
    let task = futures::lazy({
        let chain = beacon_chain2.clone();
        move || {
            spawn_network_tasks(
                network_service1,
                protocol1,
                message_rx1,
                block_outgoing_rx1,
                authority_rx1,
                gossip_rx1,
            );
            spawn_network_tasks(
                network_service2.clone(),
                protocol2,
                message_rx2,
                block_outgoing_rx2,
                authority_rx2,
                gossip_rx2,
            );
            spawn_simple_block_import_task(chain, block_rx2);
            block_task
        }
    })
    .map(|_| ())
    .map_err(|_| ());

    let handle = thread::spawn(move || {
        tokio::run(task);
    });

    while beacon_chain2.best_index() < 3 {
        thread::sleep(Duration::from_secs(1));
    }
    std::mem::drop(handle);
}

#[test]
#[ignore]
// there is an issue with running the tests in this file all together
// we might want to run the tests individually
fn test_block_announce() {
    let storage1 = Arc::new(create_memory_db());
    let genesis_block =
        SignedBeaconBlock::new(0, CryptoHash::default(), vec![], CryptoHash::default());
    // chain1
    let beacon_chain1 = Arc::new(BeaconBlockChain::new(genesis_block.clone(), storage1.clone()));
    let (block_tx1, _) = channel(1024);
    let (block_outgoing_tx1, block_outgoing_rx1) = channel(1024);
    let (message_tx1, message_rx1) = channel(1024);
    let (_, authority_rx1) = channel(1024);
    let protocol1 = get_test_protocol(beacon_chain1.clone(), block_tx1, message_tx1);
    let addr = "/ip4/127.0.0.1/tcp/30004";
    let secret = create_secret();
    let config1 = test_config_with_secret(addr, vec![], secret);
    let network_service1 =
        Arc::new(Mutex::new(new_network_service(&ProtocolConfig::default(), config1)));

    //chain2
    let storage2 = Arc::new(create_memory_db());
    let beacon_chain2 = Arc::new(BeaconBlockChain::new(genesis_block.clone(), storage2.clone()));
    let (block_tx2, block_rx2) = channel(1024);
    let (_, block_outgoing_rx2) = channel(1024);
    let (message_tx2, message_rx2) = channel(1024);
    let (_, authority_rx2) = channel(1024);
    let protocol2 = get_test_protocol(beacon_chain2.clone(), block_tx2, message_tx2);
    let boot_node = "/ip4/127.0.0.1/tcp/30004/p2p/".to_string() + &raw_key_to_peer_id_str(secret);
    let config2 = test_config("/ip4/127.0.0.1/tcp/30005", vec![boot_node]);
    let network_service2 =
        Arc::new(Mutex::new(new_network_service(&ProtocolConfig::default(), config2)));

    let (_, gossip_rx1) = channel(1024);
    let (_, gossip_rx2) = channel(1024);
    let task = futures::lazy({
        let chain1 = beacon_chain1.clone();
        let chain2 = beacon_chain2.clone();
        let service1 = network_service1.clone();
        let service2 = network_service2.clone();
        move || {
            spawn_network_tasks(
                service1.clone(),
                protocol1,
                message_rx1,
                block_outgoing_rx1,
                authority_rx1,
                gossip_rx1,
            );
            spawn_network_tasks(
                service2.clone(),
                protocol2,
                message_rx2,
                block_outgoing_rx2,
                authority_rx2,
                gossip_rx2,
            );
            spawn_simple_block_import_task(chain2, block_rx2);
            thread::sleep(Duration::from_secs(3));
            spawn_simple_block_prod_task(chain1, block_outgoing_tx1);
            Ok(())
        }
    });

    let handle = thread::spawn(move || {
        tokio::run(task);
    });

    while beacon_chain2.best_index() < 1 {
        thread::sleep(Duration::from_secs(1));
    }
    std::mem::drop(handle);
}
