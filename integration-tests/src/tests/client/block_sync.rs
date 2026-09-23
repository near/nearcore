use crate::env::test_env::TestEnv;
use near_async::messaging::IntoMultiSender;
use near_async::time::{Clock, Duration, FakeClock};
use near_chain::Provenance;
use near_chain_configs::Genesis;
use near_client::sync::block::BlockSync;
use near_client::sync::peers::{PEER_FAILURE_COOLDOWN_SECONDS, PeerAdvertisedHead, PeerSelector};
use near_crypto::{KeyType, PublicKey, SecretKey};
use near_network::test_utils::MockPeerManagerAdapter;
use near_network::types::{NetworkRequests, PeerInfo, PeerManagerMessageRequest};
use near_primitives::hash::CryptoHash;
use near_primitives::network::PeerId;
use near_primitives::utils::MaybeValidated;
use rand::SeedableRng;
use rand::rngs::StdRng;
use std::collections::HashSet;
use std::sync::Arc;

/// Which peer each block was requested from.
fn collect_requests_from_network_adapter(
    network_adapter: &MockPeerManagerAdapter,
) -> Vec<(CryptoHash, PeerId)> {
    let mut network_request = network_adapter.requests.write();
    network_request
        .drain(..)
        .map(|request| match request {
            PeerManagerMessageRequest::NetworkRequests(NetworkRequests::BlockRequest {
                hash,
                peer_id,
            }) => (hash, peer_id),
            _ => panic!("unexpected network request {:?}", request),
        })
        .collect()
}

fn distinct_peer_advertised_heads(num_peers: usize) -> Vec<PeerAdvertisedHead> {
    (0..num_peers)
        .map(|index| PeerAdvertisedHead {
            peer_info: PeerInfo {
                id: PeerId::new(
                    SecretKey::from_seed(KeyType::ED25519, &format!("peer{index}")).public_key(),
                ),
                addr: None,
                account_id: None,
            },
            highest_block_height: 0,
            highest_block_hash: Default::default(),
            archival: false,
        })
        .collect()
}

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

#[cfg(feature = "test_features")]
fn check_hashes_from_network_adapter(
    network_adapter: &MockPeerManagerAdapter,
    expected_hashes: Vec<CryptoHash>,
) {
    let collected_hashes = collect_hashes_from_network_adapter(network_adapter);
    assert_eq!(collected_hashes, expected_hashes.into_iter().collect::<HashSet<_>>());
}

fn peer_selector() -> PeerSelector {
    PeerSelector::new(Duration::seconds(PEER_FAILURE_COOLDOWN_SECONDS), StdRng::seed_from_u64(1))
}

fn create_peer_advertised_heads(num_peers: usize) -> Vec<PeerAdvertisedHead> {
    (0..num_peers)
        .map(|_| PeerAdvertisedHead {
            peer_info: PeerInfo {
                id: PeerId::new(PublicKey::empty(KeyType::ED25519)),
                addr: None,
                account_id: None,
            },
            highest_block_height: 0,
            highest_block_hash: Default::default(),
            archival: false,
        })
        .collect()
}

fn test_env_with_epoch_length(epoch_length: u64) -> TestEnv {
    let mut genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
    genesis.config.epoch_length = epoch_length;
    genesis.config.transaction_validity_period = epoch_length * 2;

    TestEnv::builder_from_genesis(&genesis).clients_count(2).build()
}

#[test]
#[cfg(feature = "test_features")]
fn test_block_sync() {
    use near_chain::test_utils::wait_for_all_blocks_in_processing;

    let network_adapter = Arc::new(MockPeerManagerAdapter::default());
    let max_block_requests = 10;
    let mut block_sync =
        BlockSync::new(Clock::real(), network_adapter.as_multi_sender(), false, max_block_requests);
    let mut env = test_env_with_epoch_length(100);
    let mut blocks = vec![];
    for i in 1..5 * max_block_requests + 1 {
        let block = env.clients[0].produce_block(i as u64).unwrap().unwrap();
        blocks.push(block.clone());
        env.process_block(0, block, Provenance::PRODUCED);
    }
    let block_headers = blocks.iter().map(|b| b.header().clone().into()).collect::<Vec<_>>();
    let peer_infos = create_peer_advertised_heads(2);
    env.clients[1].chain.sync_block_headers(block_headers).unwrap();

    // fetch three blocks at a time
    for i in 0..3 {
        block_sync.block_sync(&env.clients[1].chain, &peer_infos, &mut peer_selector()).unwrap();

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
    block_sync.block_sync(&env.clients[1].chain, &peer_infos, &mut peer_selector()).unwrap();
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
    block_sync.block_sync(&env.clients[1].chain, &peer_infos, &mut peer_selector()).unwrap();
    check_hashes_from_network_adapter(
        &network_adapter,
        (3 * max_block_requests..4 * max_block_requests - 1).map(|h| *blocks[h].hash()).collect(),
    );

    // Receive all blocks. Should not request more. As an extra
    // complication, pause the processing of one block.
    env.clients[1].chain.test_paused_blocks.pause(blocks[4 * max_block_requests - 1].hash());
    for i in 3 * max_block_requests..5 * max_block_requests {
        let _ = env.clients[1]
            .process_block_test(MaybeValidated::from(blocks[i].clone()), Provenance::NONE);
    }

    block_sync.block_sync(&env.clients[1].chain, &peer_infos, &mut peer_selector()).unwrap();
    let requested_block_hashes = collect_hashes_from_network_adapter(&network_adapter);
    assert!(requested_block_hashes.is_empty(), "{:?}", requested_block_hashes);

    // Now finish paused processing and sanity check that we
    // still are fully synced.
    env.clients[1].chain.test_paused_blocks.resume(blocks[4 * max_block_requests - 1].hash());
    wait_for_all_blocks_in_processing(&mut env.clients[1].chain);
    let requested_block_hashes = collect_hashes_from_network_adapter(&network_adapter);
    assert!(requested_block_hashes.is_empty(), "{:?}", requested_block_hashes);
}

#[test]
fn test_block_sync_archival() {
    let network_adapter = Arc::new(MockPeerManagerAdapter::default());
    let max_block_requests = 10;
    let mut block_sync =
        BlockSync::new(Clock::real(), network_adapter.as_multi_sender(), true, max_block_requests);
    let mut env = test_env_with_epoch_length(5);
    let mut blocks = vec![];
    for i in 1..41 {
        let block = env.clients[0].produce_block(i).unwrap().unwrap();
        blocks.push(block.clone());
        env.process_block(0, block, Provenance::PRODUCED);
    }
    let block_headers = blocks.iter().map(|b| b.header().clone().into()).collect::<Vec<_>>();
    let peer_infos = create_peer_advertised_heads(2);
    env.clients[1].chain.sync_block_headers(block_headers).unwrap();

    block_sync.block_sync(&env.clients[1].chain, &peer_infos, &mut peer_selector()).unwrap();
    let requested_block_hashes = collect_hashes_from_network_adapter(&network_adapter);
    // We don't have archival peers, and thus cannot request any blocks
    assert_eq!(requested_block_hashes, HashSet::new());

    let mut peer_infos = create_peer_advertised_heads(2);
    for peer in &mut peer_infos {
        peer.archival = true;
    }

    block_sync.block_sync(&env.clients[1].chain, &peer_infos, &mut peer_selector()).unwrap();
    let requested_block_hashes = collect_hashes_from_network_adapter(&network_adapter);
    assert_eq!(
        requested_block_hashes,
        blocks.iter().take(max_block_requests).map(|b| *b.hash()).collect::<HashSet<_>>()
    );
}

#[test]
fn test_block_sync_demotes_only_the_peer_that_withheld_its_block() {
    let clock = FakeClock::default();
    let network_adapter = Arc::new(MockPeerManagerAdapter::default());
    let max_block_requests = 4;
    let mut block_sync =
        BlockSync::new(clock.clock(), network_adapter.as_multi_sender(), false, max_block_requests);
    let mut env = test_env_with_epoch_length(100);

    let mut blocks = vec![];
    for height in 1..2 * max_block_requests as u64 {
        let block = env.clients[0].produce_block(height).unwrap().unwrap();
        blocks.push(block.clone());
        env.process_block(0, block, Provenance::PRODUCED);
    }
    let block_headers = blocks.iter().map(|b| b.header().clone().into()).collect::<Vec<_>>();
    env.clients[1].chain.sync_block_headers(block_headers).unwrap();

    let peers = distinct_peer_advertised_heads(2);
    let mut selector = peer_selector();
    block_sync.run(&env.clients[1].chain, &peers, &mut selector).unwrap();
    let requests = collect_requests_from_network_adapter(&network_adapter);

    let (withheld_hash, withheld_by) = requests[0].clone();
    assert!(
        requests.iter().any(|(_, peer_id)| peer_id != &withheld_by),
        "requests must reach both peers, or this proves nothing",
    );

    // Deliver every requested block but the first, so the head cannot move while
    // most peers have served us.
    for block in blocks.iter().filter(|block| block.hash() != &withheld_hash) {
        let _ = env.clients[1]
            .process_block_test(MaybeValidated::from(block.clone()), Provenance::NONE);
    }

    clock.advance(Duration::seconds(3));
    let now = clock.now_utc();
    block_sync.run(&env.clients[1].chain, &peers, &mut selector).unwrap();

    assert!(
        selector.failed_recently(&withheld_by, now),
        "the peer whose block never arrived should have failed",
    );
    for (hash, peer_id) in requests.iter().skip(1) {
        if peer_id == &withheld_by {
            continue;
        }
        assert!(
            !selector.failed_recently(peer_id, now),
            "peer {peer_id} delivered block {hash} and should not have failed",
        );
    }
}

#[test]
fn test_block_sync_demotes_the_withholder_after_the_head_advances() {
    let clock = FakeClock::default();
    let network_adapter = Arc::new(MockPeerManagerAdapter::default());
    let max_block_requests = 4;
    let mut block_sync =
        BlockSync::new(clock.clock(), network_adapter.as_multi_sender(), false, max_block_requests);
    let mut env = test_env_with_epoch_length(100);

    let mut blocks = vec![];
    for height in 1..3 * max_block_requests as u64 {
        let block = env.clients[0].produce_block(height).unwrap().unwrap();
        blocks.push(block.clone());
        env.process_block(0, block, Provenance::PRODUCED);
    }
    let block_headers = blocks.iter().map(|b| b.header().clone().into()).collect::<Vec<_>>();
    env.clients[1].chain.sync_block_headers(block_headers).unwrap();

    let peers = distinct_peer_advertised_heads(6);
    let mut selector = peer_selector();
    block_sync.run(&env.clients[1].chain, &peers, &mut selector).unwrap();
    let requests = collect_requests_from_network_adapter(&network_adapter);

    // The peer asked for the last block of the batch, so every block below it can
    // be delivered and the head advances.
    let (withheld_hash, withholder) = requests.last().unwrap().clone();
    for block in blocks.iter().filter(|block| block.hash() != &withheld_hash) {
        let _ = env.clients[1]
            .process_block_test(MaybeValidated::from(block.clone()), Provenance::NONE);
    }
    let head_after_delivery = env.clients[1].chain.head().unwrap().height;
    assert!(head_after_delivery > 0, "the head must advance, or this proves nothing");

    // A run while the head is fresh requests the missing block again. The blame must
    // stay with the peer that has not delivered it.
    clock.advance(Duration::seconds(1));
    block_sync.run(&env.clients[1].chain, &peers, &mut selector).unwrap();
    let second_batch = collect_requests_from_network_adapter(&network_adapter);
    assert!(
        second_batch.iter().any(|(hash, peer_id)| hash == &withheld_hash && peer_id != &withholder),
        "the missing block should be requested again from someone else",
    );

    clock.advance(Duration::seconds(3));
    let now = clock.now_utc();
    block_sync.run(&env.clients[1].chain, &peers, &mut selector).unwrap();

    assert!(
        selector.failed_recently(&withholder, now),
        "the peer asked first for the block that never arrived should have failed",
    );
    for peer in &peers {
        if peer.peer_info.id == withholder {
            continue;
        }
        assert!(
            !selector.failed_recently(&peer.peer_info.id, now),
            "peer {} served what it was asked and should not have failed",
            peer.peer_info.id,
        );
    }
}

#[test]
fn test_block_sync_forgets_requests_for_blocks_off_the_canonical_chain() {
    let clock = FakeClock::default();
    let network_adapter = Arc::new(MockPeerManagerAdapter::default());
    let max_block_requests = 4;
    let mut block_sync =
        BlockSync::new(clock.clock(), network_adapter.as_multi_sender(), false, max_block_requests);
    let mut env = test_env_with_epoch_length(100);

    // Heights 1..6 leave the final head at 4, so a fork is only legal from 4 up.
    let mut fork = vec![];
    let mut prev = *env.clients[0].chain.genesis().hash();
    for height in 1..=6 {
        let block = env.clients[0].produce_block_on(height, prev).unwrap().unwrap();
        prev = *block.hash();
        fork.push(block.clone());
        env.process_block(0, block, Provenance::PRODUCED);
    }
    let headers = fork.iter().map(|block| block.header().clone().into()).collect::<Vec<_>>();
    env.clients[1].chain.sync_block_headers(headers).unwrap();

    // Deliver 1..3, so the walk starts at height 4 where the fork will move.
    for block in fork.iter().take(3) {
        let _ = env.clients[1]
            .process_block_test(MaybeValidated::from(block.clone()), Provenance::NONE);
    }

    let peers = distinct_peer_advertised_heads(6);
    let mut selector = peer_selector();
    block_sync.run(&env.clients[1].chain, &peers, &mut selector).unwrap();
    let requests = collect_requests_from_network_adapter(&network_adapter);
    assert_eq!(requests.len(), 3, "expected heights 4, 5 and 6 to be requested");

    // A longer fork from height 4 takes heights 5 and 6 off the canonical chain.
    let mut prev = *fork[3].hash();
    let mut longer_fork = vec![];
    for height in 7..=16 {
        let block = env.clients[0].produce_block_on(height, prev).unwrap().unwrap();
        prev = *block.hash();
        longer_fork.push(block.clone());
        env.clients[0].process_block_test(MaybeValidated::from(block), Provenance::NONE).unwrap();
    }
    let headers = longer_fork.iter().map(|block| block.header().clone().into()).collect::<Vec<_>>();
    env.clients[1].chain.sync_block_headers(headers).unwrap();
    for height in [5, 6] {
        assert!(
            env.clients[1].chain.get_block_header_by_height(height).is_err(),
            "height {height} should have left the canonical chain, or this proves nothing",
        );
    }

    // Deliver height 4, the one requested block the fork left in place. Every request
    // still outstanding is now for a block we stopped wanting.
    let _ =
        env.clients[1].process_block_test(MaybeValidated::from(fork[3].clone()), Provenance::NONE);

    clock.advance(Duration::seconds(3));
    let now = clock.now_utc();
    block_sync.run(&env.clients[1].chain, &peers, &mut selector).unwrap();

    for (_, peer_id) in &requests {
        assert!(
            !selector.failed_recently(peer_id, now),
            "peer {peer_id} owes us nothing after the fork and should keep its place",
        );
    }
}
