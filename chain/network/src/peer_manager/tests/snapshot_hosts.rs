use crate::network_protocol::testonly as data;
use crate::network_protocol::SnapshotHostInfo;
use crate::network_protocol::SyncSnapshotHosts;
use crate::peer;
use crate::peer_manager;
use crate::peer_manager::peer_manager_actor::Event as PME;
use crate::tcp;
use crate::testonly::{make_rng, AsSet as _};
use crate::types::NetworkRequests;
use crate::types::PeerManagerMessageRequest;
use crate::types::PeerMessage;
use near_async::time;
use near_crypto::SecretKey;
use near_o11y::testonly::init_test_logger;
use near_o11y::WithSpanContextExt;
use near_primitives::hash::CryptoHash;
use near_primitives::network::PeerId;
use near_primitives::types::EpochHeight;
use near_primitives::types::ShardId;
use pretty_assertions::assert_eq;
use std::sync::Arc;

/// Create an instance of SnapshotHostInfo for testing purposes
fn make_snapshot_host_info(
    peer_id: &PeerId,
    epoch_height: EpochHeight,
    shards: Vec<ShardId>,
    secret_key: &SecretKey,
) -> Arc<SnapshotHostInfo> {
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

/// Used to consume peer manager events until there's an event of type SyncSnapshotHosts
fn take_sync_snapshot_msg_manager(
    event: crate::peer_manager::testonly::Event,
) -> Option<SyncSnapshotHosts> {
    match event {
        peer_manager::testonly::Event::PeerManager(PME::MessageProcessed(
            tcp::Tier::T2,
            PeerMessage::SyncSnapshotHosts(msg),
        )) => Some(msg),
        _ => None,
    }
}

/// Each actix arbiter (in fact, the underlying tokio runtime) creates 4 file descriptors:
/// 1. eventfd2()
/// 2. epoll_create1()
/// 3. fcntl() duplicating one end of some globally shared socketpair()
/// 4. fcntl() duplicating epoll socket created in (2)
/// This gives 5 file descriptors per PeerActor (4 + 1 TCP socket).
const FDS_PER_PEER: usize = 5;

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

    let info1 =
        make_snapshot_host_info(&peer1_config.node_id(), 123, vec![0, 1], &peer1_config.node_key);

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
    let info2 =
        make_snapshot_host_info(&peer2_config.node_id(), 11212, vec![3], &peer2_config.node_key);

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

    tracing::info!(target:"test", "Send an invalid SyncSnapshotHosts message from from peer1. One of the host infos has an invalid signature.");
    let random_secret_key = SecretKey::from_random(near_crypto::KeyType::ED25519);
    let invalid_info =
        make_snapshot_host_info(&peer1_config.node_id(), 1337, vec![10, 11], &random_secret_key);

    let ok_info_a =
        make_snapshot_host_info(&peer1_config.node_id(), 2, vec![10012120], &peer1_config.node_key);
    let ok_info_b =
        make_snapshot_host_info(&peer1_config.node_id(), 222, vec![232], &peer1_config.node_key);

    let invalid_message = PeerMessage::SyncSnapshotHosts(SyncSnapshotHosts {
        hosts: vec![ok_info_a, invalid_info, ok_info_b],
    });
    peer1.send(invalid_message).await;

    tracing::info!(target:"test", "Send a vaid message from peer2 (as peer1 got banned), it should reach peer3.");

    let info2 =
        make_snapshot_host_info(&peer2_config.node_id(), 2434, vec![0, 1], &peer2_config.node_key);

    peer2
        .send(PeerMessage::SyncSnapshotHosts(SyncSnapshotHosts { hosts: vec![info2.clone()] }))
        .await;

    tracing::info!(target:"test", "Make sure that only the valid message is broadcast.");

    let msg = peer2.events.recv_until(take_sync_snapshot_msg).await;
    assert_eq!(msg.hosts, vec![info2]);
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

    tracing::info!(target:"test", "Receive the inital empty snapshot info messages.");
    // Each peer_manager connects to two others and receives two empty sync_snapshot messages
    for _ in 0..2 {
        for pm in &mut pms {
            let empty_sync_msg = pm.events.recv_until(take_sync_snapshot_msg_manager).await;
            assert_eq!(empty_sync_msg.hosts, vec![]);
        }
    }

    tracing::info!(target:"test", "Send a SnapshotHostInfo message from peer manager #1.");
    let info1 =
        make_snapshot_host_info(&pms[1].peer_info().id, 123, vec![2, 3], &pms[1].cfg.node_key);

    let message = PeerManagerMessageRequest::NetworkRequests(NetworkRequests::SnapshotHostInfo {
        sync_hash: info1.sync_hash,
        epoch_height: info1.epoch_height,
        shards: info1.shards.clone(),
    });

    pms[1].actix.addr.send(message.with_span_context()).await.unwrap();

    tracing::info!(target:"test", "Make sure that the message sent from #1 reaches #2 on the other side of the ring.");
    let received_message = pms[2].events.recv_until(take_sync_snapshot_msg_manager).await;
    assert_eq!(received_message.hosts, vec![info1]);
}
