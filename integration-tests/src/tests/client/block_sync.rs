use std::collections::HashSet;
use std::sync::Arc;

use near_async::messaging::IntoMultiSender;
use near_async::time::Clock;
use near_chain::Provenance;
use near_chain::test_utils::wait_for_all_blocks_in_processing;
use near_chain_configs::Genesis;
use near_client::sync::block::BlockSync;
use near_crypto::{KeyType, PublicKey};
use near_network::test_utils::MockPeerManagerAdapter;
use near_network::types::{
    HighestHeightPeerInfo, NetworkRequests, PeerInfo, PeerManagerMessageRequest,
};
use near_o11y::testonly::TracingCapture;
use near_primitives::hash::CryptoHash;
use near_primitives::network::PeerId;
use near_primitives::utils::MaybeValidated;

use crate::env::test_env::TestEnv;

/// Helper function for block sync tests
fn collect_hashes_from_network_adapter(
    network_adapter: &MockPeerManagerAdapter,
) -> HashSet<CryptoHash> {
    let mut network_request = network_adapter.requests.write();
    network_request
        .drain(..)
        .map(|request| match request {
            PeerManagerMessageRequest::NetworkRequests(NetworkRequests::BlockRequest {
                hash,
                ..
            }) => hash,
            _ => panic!("unexpected network request {:?}", request),
        })
        .collect()
}

fn check_hashes_from_network_adapter(
    network_adapter: &MockPeerManagerAdapter,
    expected_hashes: Vec<CryptoHash>,
) {
    let collected_hashes = collect_hashes_from_network_adapter(network_adapter);
    assert_eq!(collected_hashes, expected_hashes.into_iter().collect::<HashSet<_>>());
}

fn create_highest_height_peer_infos(num_peers: usize) -> Vec<HighestHeightPeerInfo> {
    (0..num_peers)
        .map(|_| HighestHeightPeerInfo {
            peer_info: PeerInfo {
                id: PeerId::new(PublicKey::empty(KeyType::ED25519)),
                addr: None,
                account_id: None,
            },
            genesis_id: Default::default(),
            highest_block_height: 0,
            highest_block_hash: Default::default(),
            tracked_shards: vec![],
            archival: false,
        })
        .collect()
}

fn test_env_with_epoch_length(epoch_length: u64) -> TestEnv {
    let mut genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
    genesis.config.epoch_length = epoch_length;

    TestEnv::builder_from_genesis(&genesis).clients_count(2).build()
}

#[test]
fn test_block_sync() {
    let mut capture = TracingCapture::enable();
    let network_adapter = Arc::new(MockPeerManagerAdapter::default());
    let block_fetch_horizon = 10;
    let max_block_requests = 10;
    let mut block_sync = BlockSync::new(
        Clock::real(),
        network_adapter.as_multi_sender(),
        block_fetch_horizon,
        false,
        true,
    );
    let mut env = test_env_with_epoch_length(100);
    let mut blocks = vec![];
    for i in 1..5 * max_block_requests + 1 {
        let block = env.clients[0].produce_block(i as u64).unwrap().unwrap();
        blocks.push(block.clone());
        env.process_block(0, block, Provenance::PRODUCED);
    }
    let block_headers = blocks.iter().map(|b| b.header().clone()).collect::<Vec<_>>();
    let peer_infos = create_highest_height_peer_infos(2);
    env.clients[1].chain.sync_block_headers(block_headers).unwrap();

    // fetch three blocks at a time
    for i in 0..3 {
        block_sync.block_sync(&env.clients[1].chain, &peer_infos, max_block_requests).unwrap();

        let expected_blocks: Vec<_> =
            blocks[i * max_block_requests..(i + 1) * max_block_requests].to_vec();
        check_hashes_from_network_adapter(
            &network_adapter,
            expected_blocks.iter().map(|b| *b.hash()).collect(),
        );

        for block in expected_blocks {
            env.process_block(1, block, Provenance::NONE);
        }
    }

    // Now test when the node receives the block out of order
    // fetch the next three blocks
    block_sync.block_sync(&env.clients[1].chain, &peer_infos, max_block_requests).unwrap();
    check_hashes_from_network_adapter(
        &network_adapter,
        (3 * max_block_requests..4 * max_block_requests).map(|h| *blocks[h].hash()).collect(),
    );
    // assumes that we only get block[4*max_block_requests-1]
    let _ = env.clients[1].process_block_test(
        MaybeValidated::from(blocks[4 * max_block_requests - 1].clone()),
        Provenance::NONE,
    );

    // the next block sync should not request block[4*max_block_requests-1] again
    block_sync.block_sync(&env.clients[1].chain, &peer_infos, max_block_requests).unwrap();
    check_hashes_from_network_adapter(
        &network_adapter,
        (3 * max_block_requests..4 * max_block_requests - 1).map(|h| *blocks[h].hash()).collect(),
    );

    // Receive all blocks. Should not request more. As an extra
    // complication, pause the processing of one block.
    env.pause_block_processing(&mut capture, blocks[4 * max_block_requests - 1].hash());
    for i in 3 * max_block_requests..5 * max_block_requests {
        let _ = env.clients[1]
            .process_block_test(MaybeValidated::from(blocks[i].clone()), Provenance::NONE);
    }

    block_sync.block_sync(&env.clients[1].chain, &peer_infos, max_block_requests).unwrap();
    let requested_block_hashes = collect_hashes_from_network_adapter(&network_adapter);
    assert!(requested_block_hashes.is_empty(), "{:?}", requested_block_hashes);

    // Now finish paused processing and sanity check that we
    // still are fully synced.
    env.resume_block_processing(blocks[4 * max_block_requests - 1].hash());
    wait_for_all_blocks_in_processing(&mut env.clients[1].chain);
    let requested_block_hashes = collect_hashes_from_network_adapter(&network_adapter);
    assert!(requested_block_hashes.is_empty(), "{:?}", requested_block_hashes);
}

#[test]
fn test_block_sync_archival() {
    let network_adapter = Arc::new(MockPeerManagerAdapter::default());
    let block_fetch_horizon = 10;
    let max_block_requests = 10;
    let mut block_sync = BlockSync::new(
        Clock::real(),
        network_adapter.as_multi_sender(),
        block_fetch_horizon,
        true,
        true,
    );
    let mut env = test_env_with_epoch_length(5);
    let mut blocks = vec![];
    for i in 1..41 {
        let block = env.clients[0].produce_block(i).unwrap().unwrap();
        blocks.push(block.clone());
        env.process_block(0, block, Provenance::PRODUCED);
    }
    let block_headers = blocks.iter().map(|b| b.header().clone()).collect::<Vec<_>>();
    let peer_infos = create_highest_height_peer_infos(2);
    env.clients[1].chain.sync_block_headers(block_headers).unwrap();

    block_sync.block_sync(&env.clients[1].chain, &peer_infos, max_block_requests).unwrap();
    let requested_block_hashes = collect_hashes_from_network_adapter(&network_adapter);
    // We don't have archival peers, and thus cannot request any blocks
    assert_eq!(requested_block_hashes, HashSet::new());

    let mut peer_infos = create_highest_height_peer_infos(2);
    for peer in &mut peer_infos {
        peer.archival = true;
    }

    block_sync.block_sync(&env.clients[1].chain, &peer_infos, max_block_requests).unwrap();
    let requested_block_hashes = collect_hashes_from_network_adapter(&network_adapter);
    assert_eq!(
        requested_block_hashes,
        blocks.iter().take(max_block_requests).map(|b| *b.hash()).collect::<HashSet<_>>()
    );
}
