use crate::network_protocol::SnapshotHostInfo;
use crate::network_protocol::SyncSnapshotHosts;
use crate::network_protocol::MAX_SHARDS_PER_SNAPSHOT_HOST_INFO;
use crate::peer;
use crate::peer_manager;
use crate::peer_manager::peer_manager_actor::Event as PME;
use crate::tcp;
use crate::testonly::{make_rng, AsSet as _};
use crate::types::NetworkRequests;
use crate::types::PeerManagerMessageRequest;
use crate::types::PeerMessage;
use crate::{network_protocol::testonly as data, peer::testonly::PeerHandle};
use itertools::Itertools;
use near_async::time;
use near_crypto::SecretKey;
use near_o11y::testonly::init_test_logger;
use near_o11y::WithSpanContextExt;
use near_primitives::hash::CryptoHash;
use near_primitives::network::PeerId;
use near_primitives::types::new_shard_id_tmp;
use near_primitives::types::shard_id_as_u64;
use near_primitives::types::shard_id_max;
use near_primitives::types::EpochHeight;
use near_primitives::types::ShardId;
use peer_manager::testonly::FDS_PER_PEER;
use pretty_assertions::assert_eq;
use rand::seq::IteratorRandom;
use rand::Rng;
use std::collections::HashSet;
use std::sync::Arc;

/// Create an instance of SnapshotHostInfo for testing purposes
fn make_snapshot_host_info(
    peer_id: &PeerId,
    secret_key: &SecretKey,
    rng: &mut impl Rng,
) -> Arc<SnapshotHostInfo> {
    let epoch_height: EpochHeight = rng.gen::<EpochHeight>();
    let max_shard_id = 32;
    let shards_num: usize = rng.gen_range(1..16);
    let shards = (0..max_shard_id).choose_multiple(rng, shards_num);
    let shards = shards.into_iter().sorted().map(new_shard_id_tmp).collect();
    let sync_hash = CryptoHash::hash_borsh(epoch_height);
    Arc::new(SnapshotHostInfo::new(peer_id.clone(), sync_hash, epoch_height, shards, secret_key))
}

/// Used to consume peer events until there's an event of type SyncSnapshotHosts
fn take_sync_snapshot_msg(event: crate::peer::testonly::Event) -> Option<SyncSnapshotHosts> {
    match event {
        peer::testonly::Event::Network(PME::MessageProcessed(
            tcp::Tier::T2,
            PeerMessage::SyncSnapshotHosts(msg),
        )) => Some(msg),
        _ => None,
    }
}

/// Receives events from the given PeerHandle until the desired target_info is found
/// Ignores infos defined in allowed_ignorable_infos. Any other unexpected info will trigger a panic.
async fn wait_for_host_info(
    peer: &mut PeerHandle,
    target_info: &SnapshotHostInfo,
    allowed_ignorable_infos: &[Arc<SnapshotHostInfo>],
) {
    loop {
        let msg = peer.events.recv_until(take_sync_snapshot_msg).await;
        for info in msg.hosts {
            if info.as_ref() == target_info {
                return;
            } else if !allowed_ignorable_infos.contains(&info) {
                panic!("wait_for_host_info: received unexpected SnapshotHostInfo: {:?}", info);
            }
        }
    }
}

/// Test that PeerManager broadcasts SnapshotHostInfo messages to all connected peers
#[tokio::test]
async fn broadcast() {
    init_test_logger();
    let mut rng = make_rng(921853233);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));
    let clock = clock.clock();
    let clock = &clock;

    let pm = peer_manager::testonly::start(
        clock.clone(),
        near_store::db::TestDB::new(),
        chain.make_config(rng),
        chain.clone(),
    )
    .await;

    tracing::info!(target:"test", "Connect a peer, expect initial sync to be empty.");
    let peer1_config = chain.make_config(rng);
    let mut peer1 =
        pm.start_inbound(chain.clone(), peer1_config.clone()).await.handshake(clock).await;
    let empty_sync_msg = peer1.events.recv_until(take_sync_snapshot_msg).await;
    assert_eq!(empty_sync_msg.hosts, vec![]);

    tracing::info!(target:"test", "Connect two more peers.");
    let peer2_config = chain.make_config(rng);
    let mut peer2 =
        pm.start_inbound(chain.clone(), peer2_config.clone()).await.handshake(clock).await;
    let empty_sync_msg = peer2.events.recv_until(take_sync_snapshot_msg).await;
    assert_eq!(empty_sync_msg.hosts, vec![]);

    let mut peer3 =
        pm.start_inbound(chain.clone(), chain.make_config(rng)).await.handshake(clock).await;
    let empty_sync_msg = peer3.events.recv_until(take_sync_snapshot_msg).await;
    assert_eq!(empty_sync_msg.hosts, vec![]);

    tracing::info!(target:"test", "Send a SyncSnapshotHosts message from peer1, make sure that all peers receive it.");

    let info1 = make_snapshot_host_info(&peer1_config.node_id(), &peer1_config.node_key, rng);

    peer1
        .send(PeerMessage::SyncSnapshotHosts(SyncSnapshotHosts { hosts: vec![info1.clone()] }))
        .await;

    let got1 = peer1.events.recv_until(take_sync_snapshot_msg).await;
    assert_eq!(got1.hosts, vec![info1.clone()]);

    let got2 = peer2.events.recv_until(take_sync_snapshot_msg).await;
    assert_eq!(got2.hosts, vec![info1.clone()]);

    let got3 = peer3.events.recv_until(take_sync_snapshot_msg).await;
    assert_eq!(got3.hosts, vec![info1.clone()]);

    tracing::info!(target:"test", "Connect another peer, make sure that it receives the correct information on joining.");
    let mut peer4 =
        pm.start_inbound(chain.clone(), chain.make_config(rng)).await.handshake(clock).await;
    let peer4_sync_msg = peer4.events.recv_until(take_sync_snapshot_msg).await;
    assert_eq!(peer4_sync_msg.hosts, vec![info1.clone()]);

    tracing::info!(target:"test", "Publish another piece of snapshot information, check that it's also broadcasted.");
    let info2 = make_snapshot_host_info(&peer2_config.node_id(), &peer2_config.node_key, rng);

    peer2
        .send(PeerMessage::SyncSnapshotHosts(SyncSnapshotHosts { hosts: vec![info2.clone()] }))
        .await;

    let got1 = peer1.events.recv_until(take_sync_snapshot_msg).await;
    assert_eq!(got1.hosts, vec![info2.clone()]);

    tracing::info!(target:"test", "Connect another peer, check that it receieves all of the published information.");
    let mut peer5 =
        pm.start_inbound(chain.clone(), chain.make_config(rng)).await.handshake(clock).await;
    let peer5_sync_msg = peer5.events.recv_until(take_sync_snapshot_msg).await;
    assert_eq!(peer5_sync_msg.hosts.as_set(), vec![info1, info2].as_set());
}

/// Test that a SyncSnapshotHosts message with an invalid signature isn't broadcast by PeerManager.
#[tokio::test]
async fn invalid_signature_not_broadcast() {
    init_test_logger();
    let mut rng = make_rng(921853233);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));
    let clock = clock.clock();
    let clock = &clock;

    let pm = peer_manager::testonly::start(
        clock.clone(),
        near_store::db::TestDB::new(),
        chain.make_config(rng),
        chain.clone(),
    )
    .await;

    tracing::info!(target:"test", "Connect peers, expect initial sync to be empty.");
    let peer1_config = chain.make_config(rng);
    let mut peer1 =
        pm.start_inbound(chain.clone(), peer1_config.clone()).await.handshake(clock).await;
    let empty_sync_msg = peer1.events.recv_until(take_sync_snapshot_msg).await;
    assert_eq!(empty_sync_msg.hosts, vec![]);

    let peer2_config = chain.make_config(rng);
    let mut peer2 =
        pm.start_inbound(chain.clone(), peer2_config.clone()).await.handshake(clock).await;
    let empty_sync_msg = peer2.events.recv_until(take_sync_snapshot_msg).await;
    assert_eq!(empty_sync_msg.hosts, vec![]);

    let mut peer3 =
        pm.start_inbound(chain.clone(), chain.make_config(rng)).await.handshake(clock).await;
    let empty_sync_msg = peer3.events.recv_until(take_sync_snapshot_msg).await;
    assert_eq!(empty_sync_msg.hosts, vec![]);

    tracing::info!(target:"test", "Send an invalid SyncSnapshotHosts message from peer1. One of the host infos has an invalid signature.");
    let random_secret_key = SecretKey::from_random(near_crypto::KeyType::ED25519);
    let invalid_info = make_snapshot_host_info(&peer1_config.node_id(), &random_secret_key, rng);

    let ok_info_a = make_snapshot_host_info(&peer1_config.node_id(), &peer1_config.node_key, rng);
    let ok_info_b = make_snapshot_host_info(&peer1_config.node_id(), &peer1_config.node_key, rng);

    let invalid_message = PeerMessage::SyncSnapshotHosts(SyncSnapshotHosts {
        hosts: vec![ok_info_a.clone(), invalid_info, ok_info_b.clone()],
    });
    peer1.send(invalid_message).await;

    tracing::info!(target:"test", "Send a vaid message from peer2 (as peer1 got banned), it should reach peer3.");

    let info2 = make_snapshot_host_info(&peer2_config.node_id(), &peer2_config.node_key, rng);

    peer2
        .send(PeerMessage::SyncSnapshotHosts(SyncSnapshotHosts { hosts: vec![info2.clone()] }))
        .await;

    tracing::info!(target:"test", "Make sure that only the valid messages are broadcast.");

    // Wait until peer2 receives info2. Ignore ok_info_a and ok_info_b,
    // as the PeerManager could accept and broadcast them despite the neighbouring invalid_info.
    wait_for_host_info(&mut peer2, &info2, &[ok_info_a, ok_info_b]).await;
}

/// Test that a SnapshotHostInfo message with more shards than allowed isn't broadcast by PeerManager.
#[tokio::test]
async fn too_many_shards_not_broadcast() {
    init_test_logger();
    let mut rng = make_rng(921853233);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));
    let clock = clock.clock();
    let clock = &clock;

    let pm = peer_manager::testonly::start(
        clock.clone(),
        near_store::db::TestDB::new(),
        chain.make_config(rng),
        chain.clone(),
    )
    .await;

    tracing::info!(target:"test", "Connect peers, expect initial sync to be empty.");
    let peer1_config = chain.make_config(rng);
    let mut peer1 =
        pm.start_inbound(chain.clone(), peer1_config.clone()).await.handshake(clock).await;
    let empty_sync_msg = peer1.events.recv_until(take_sync_snapshot_msg).await;
    assert_eq!(empty_sync_msg.hosts, vec![]);

    let peer2_config = chain.make_config(rng);
    let mut peer2 =
        pm.start_inbound(chain.clone(), peer2_config.clone()).await.handshake(clock).await;
    let empty_sync_msg = peer2.events.recv_until(take_sync_snapshot_msg).await;
    assert_eq!(empty_sync_msg.hosts, vec![]);

    let mut peer3 =
        pm.start_inbound(chain.clone(), chain.make_config(rng)).await.handshake(clock).await;
    let empty_sync_msg = peer3.events.recv_until(take_sync_snapshot_msg).await;
    assert_eq!(empty_sync_msg.hosts, vec![]);

    tracing::info!(target:"test", "Send an invalid SyncSnapshotHosts message from peer1. One of the host infos has more shard ids than allowed.");
    let too_many_shards: Vec<ShardId> =
        (0..(MAX_SHARDS_PER_SNAPSHOT_HOST_INFO as u64 + 1)).map(Into::into).collect();
    let invalid_info = Arc::new(SnapshotHostInfo::new(
        peer1_config.node_id(),
        CryptoHash::hash_borsh(rng.gen::<u64>()),
        rng.gen(),
        too_many_shards,
        &peer1_config.node_key,
    ));

    let ok_info_a = make_snapshot_host_info(&peer1_config.node_id(), &peer1_config.node_key, rng);
    let ok_info_b = make_snapshot_host_info(&peer1_config.node_id(), &peer1_config.node_key, rng);

    let invalid_message = PeerMessage::SyncSnapshotHosts(SyncSnapshotHosts {
        hosts: vec![ok_info_a.clone(), invalid_info, ok_info_b.clone()],
    });
    peer1.send(invalid_message).await;

    tracing::info!(target:"test", "Send a vaid message from peer2 (as peer1 got banned), it should reach peer3.");

    let info2 = make_snapshot_host_info(&peer2_config.node_id(), &peer2_config.node_key, rng);

    peer2
        .send(PeerMessage::SyncSnapshotHosts(SyncSnapshotHosts { hosts: vec![info2.clone()] }))
        .await;

    tracing::info!(target:"test", "Make sure that only valid messages are broadcast.");

    // Wait until peer2 receives info2. Ignore ok_info_a and ok_info_b,
    // as the PeerManager could accept and broadcast them despite the neighbouring invalid_info.
    wait_for_host_info(&mut peer2, &info2, &[ok_info_a, ok_info_b]).await;
}

/// Test that SyncSnapshotHosts message is propagated between many PeerManager instances.
/// Four peer managers are connected into a ring:
/// [0] - [1]
///  |     |
/// [2] - [3]
/// And then the managers propagate messages among themeselves.
#[tokio::test]
async fn propagate() {
    init_test_logger();
    let mut rng = make_rng(921853233);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));

    // Adjust the file descriptors limit, so that we can create many connection in the test.
    const MAX_CONNECTIONS: usize = 2;
    let limit = rlimit::Resource::NOFILE.get().unwrap();
    rlimit::Resource::NOFILE
        .set(std::cmp::min(limit.1, (1000 + 4 * FDS_PER_PEER * MAX_CONNECTIONS) as u64), limit.1)
        .unwrap();

    tracing::info!(target:"test", "Create four peer manager instances.");
    let mut pms = vec![];
    for _ in 0..4 {
        pms.push(
            peer_manager::testonly::start(
                clock.clock(),
                near_store::db::TestDB::new(),
                chain.make_config(rng),
                chain.clone(),
            )
            .await,
        );
    }

    tracing::info!(target:"test", "Connect the four peer managers into a ring.");
    // [0] - [1]
    //  |     |
    // [2] - [3]
    pms[0].connect_to(&pms[1].peer_info(), tcp::Tier::T2).await;
    pms[0].connect_to(&pms[2].peer_info(), tcp::Tier::T2).await;
    pms[1].connect_to(&pms[3].peer_info(), tcp::Tier::T2).await;
    pms[2].connect_to(&pms[3].peer_info(), tcp::Tier::T2).await;

    tracing::info!(target:"test", "Send a SnapshotHostInfo message from peer manager #1.");
    let info1 = make_snapshot_host_info(&pms[1].peer_info().id, &pms[1].cfg.node_key, rng);

    let message = PeerManagerMessageRequest::NetworkRequests(NetworkRequests::SnapshotHostInfo {
        sync_hash: info1.sync_hash,
        epoch_height: info1.epoch_height,
        shards: info1.shards.clone(),
    });

    pms[1].actix.addr.send(message.with_span_context()).await.unwrap();

    tracing::info!(target:"test", "Make sure that the message sent from #1 reaches #2 on the other side of the ring.");
    let want: HashSet<Arc<SnapshotHostInfo>> = std::iter::once(info1).collect();
    pms[2].wait_for_snapshot_hosts(&want).await;
}

/// Send a SyncSnapshotHosts message with very large shard ids.
/// Makes sure that PeerManager processes large shard ids without any problems.
#[tokio::test]
async fn large_shard_id_in_cache() {
    init_test_logger();
    let mut rng = make_rng(921853233);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));
    let clock = clock.clock();
    let clock = &clock;

    tracing::info!(target:"test", "Create a peer manager.");
    let pm = peer_manager::testonly::start(
        clock.clone(),
        near_store::db::TestDB::new(),
        chain.make_config(rng),
        chain.clone(),
    )
    .await;

    tracing::info!(target:"test", "Connect a peer");
    let peer1_config = chain.make_config(rng);
    let peer1 = pm.start_inbound(chain.clone(), peer1_config.clone()).await.handshake(clock).await;

    tracing::info!(target:"test", "Send a SnapshotHostInfo message with very large shard ids.");
    let max_shard_id = shard_id_max();
    let max_shard_id_minus_one = shard_id_as_u64(max_shard_id) - 1;
    let max_shard_id_minus_one = new_shard_id_tmp(max_shard_id_minus_one);
    let big_shard_info = Arc::new(SnapshotHostInfo::new(
        peer1_config.node_id(),
        CryptoHash::hash_borsh(1234_u64),
        1234,
        vec![new_shard_id_tmp(0), new_shard_id_tmp(1232232), max_shard_id_minus_one, max_shard_id]
            .into_iter()
            .collect(),
        &peer1_config.node_key,
    ));

    peer1
        .send(PeerMessage::SyncSnapshotHosts(SyncSnapshotHosts {
            hosts: vec![big_shard_info.clone()],
        }))
        .await;

    tracing::info!(target:"test", "Make sure that the message is received and processed without any problems.");
    let want: HashSet<Arc<SnapshotHostInfo>> = std::iter::once(big_shard_info).collect();
    pm.wait_for_snapshot_hosts(&want).await;
}

// When PeerManager receives a request to share SnaphotHostInfo with more than MAX_SHARDS_PER_SNAPSHOT_HOST_INFO
// shards it should truncate the list of shards to prevent being banned for abusive behavior by other peers.
// Truncation is done by choosing a random subset from the original list of shards.
#[tokio::test]
async fn too_many_shards_truncate() {
    init_test_logger();
    let mut rng = make_rng(921853233);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));
    let clock = clock.clock();
    let clock = &clock;

    tracing::info!(target:"test", "Create a single peer manager.");
    let pm = peer_manager::testonly::start(
        clock.clone(),
        near_store::db::TestDB::new(),
        chain.make_config(rng),
        chain.clone(),
    )
    .await;

    tracing::info!(target:"test", "Connect a peer, expect initial sync message to be empty.");
    let mut peer1 =
        pm.start_inbound(chain.clone(), chain.make_config(rng)).await.handshake(clock).await;
    let empty_sync_msg = peer1.events.recv_until(take_sync_snapshot_msg).await;
    assert_eq!(empty_sync_msg.hosts, vec![]);

    tracing::info!(target:"test", "Ask peer manager to send out an invalid SyncSnapshotHosts message. The info has more shard ids than allowed.");
    // Create a list of shards with twice as many shard ids as is allowed
    let too_many_shards: Vec<ShardId> =
        (0..(2 * MAX_SHARDS_PER_SNAPSHOT_HOST_INFO as u64)).map(Into::into).collect();

    let sync_hash = CryptoHash::hash_borsh(rng.gen::<u64>());
    let epoch_height: EpochHeight = rng.gen();

    let message = PeerManagerMessageRequest::NetworkRequests(NetworkRequests::SnapshotHostInfo {
        sync_hash,
        epoch_height,
        shards: too_many_shards.clone(),
    });

    pm.actix.addr.send(message.with_span_context()).await.unwrap();

    tracing::info!(target:"test", "Receive the truncated SnapshotHostInfo message on peer1, make sure that the contents are correct.");
    let msg = peer1.events.recv_until(take_sync_snapshot_msg).await;
    assert_eq!(msg.hosts.len(), 1);
    let info: &SnapshotHostInfo = &msg.hosts[0];
    assert_eq!(info.peer_id, pm.peer_info().id);
    assert_eq!(info.epoch_height, epoch_height);
    assert_eq!(info.sync_hash, sync_hash);

    // The list of shards should contain MAX_SHARDS_PER_SNAPSHOT_HOST_INFO randomly sampled, unique shard ids taken from too_many_shards
    assert_eq!(info.shards.len(), MAX_SHARDS_PER_SNAPSHOT_HOST_INFO);
    for &shard_id in &info.shards {
        // Shard ids are taken from the original vector
        assert!(shard_id_as_u64(shard_id) < 2 * MAX_SHARDS_PER_SNAPSHOT_HOST_INFO as u64);
    }
    // The shard_ids are sorted and unique (no two elements are equal, hence the < condition instead of <=)
    assert!(info.shards.windows(2).all(|twoelems| twoelems[0] < twoelems[1]));
    // The list isn't truncated by choosing the first half of the shards vec, it should be chosen randomly.
    // MAX_SHARDS_PER_SNAPSHOT_HOST_INFO is at least 128, so the chance of this check failing due to randomness is extremely low.
    assert_ne!(&info.shards, &too_many_shards[..MAX_SHARDS_PER_SNAPSHOT_HOST_INFO]);
}
