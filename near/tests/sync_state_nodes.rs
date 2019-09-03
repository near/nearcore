use std::sync::Arc;

use actix::{Actor, System};
use futures::future::Future;
use tempdir::TempDir;

use near::{load_test_config, start_with_config, GenesisConfig};
use near_chain::Block;
use near_client::GetBlock;
use near_crypto::{InMemorySigner, KeyType};
use near_network::test_utils::{convert_boot_nodes, open_port, WaitOrTimeout};
use near_network::{NetworkClientMessages, PeerInfo};
use near_primitives::test_utils::init_test_logger;
use testlib::genesis_header;

/// One client is in front, another must sync to it using state (fast) sync.
#[test]
fn sync_state_nodes() {
    init_test_logger();

    let genesis_config = GenesisConfig::test(vec!["other"]);
    let genesis_header = genesis_header(&genesis_config);

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
    let mut prev = &genesis_header;
    let signer = Arc::new(InMemorySigner::from_seed("other", KeyType::ED25519, "other"));
    for _ in 0..=100 {
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
