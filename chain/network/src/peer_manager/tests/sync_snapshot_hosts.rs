use crate::network_protocol::testonly as data;
use crate::network_protocol::SnapshotHostInfo;
use crate::network_protocol::SyncSnapshotHosts;
use crate::peer;
use crate::peer_manager;
use crate::peer_manager::peer_manager_actor::Event as PME;
use crate::tcp;
use crate::testonly::{make_rng, AsSet as _};
use crate::types::PeerMessage;
use near_async::time;
use near_crypto::SecretKey;
use near_o11y::testonly::init_test_logger;
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
