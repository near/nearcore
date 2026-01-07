use crate::network_protocol::{
    MAX_SHARDS_PER_SNAPSHOT_HOST_INFO, SnapshotHostInfoVerificationError, testonly as data,
};
use crate::snapshot_hosts::{Config, SnapshotHostInfoError, SnapshotHostsCache, priority_score};
use crate::testonly::assert_is_superset;
use crate::testonly::{AsSet as _, make_rng};
use crate::types::SnapshotHostInfo;
use itertools::Itertools;
use near_crypto::SecretKey;
use near_o11y::testonly::init_test_logger;
use near_primitives::hash::CryptoHash;
use near_primitives::network::PeerId;
use near_primitives::types::EpochHeight;
use near_primitives::types::ShardId;
use std::collections::HashSet;
use std::sync::Arc;

fn make_snapshot_host_info(
    peer_id: &PeerId,
    epoch_height: EpochHeight,
    shards: Vec<ShardId>,
    secret_key: &SecretKey,
) -> SnapshotHostInfo {
    let sync_hash = CryptoHash::hash_borsh(epoch_height);
    SnapshotHostInfo::new(peer_id.clone(), sync_hash, epoch_height, shards, secret_key)
}

fn unwrap<'a, T: std::hash::Hash + std::cmp::Eq, E: std::fmt::Debug>(
    v: &'a (T, Option<E>),
) -> &'a T {
    if let Some(err) = &v.1 {
        panic!("unexpected error: {err:?}");
    }
    &v.0
}

#[tokio::test]
async fn happy_path() {
    init_test_logger();
    let mut rng = make_rng(2947294234);
    let rng = &mut rng;

    let key0 = data::make_secret_key(rng);
    let key1 = data::make_secret_key(rng);
    let key2 = data::make_secret_key(rng);

    let peer0 = PeerId::new(key0.public_key());
    let peer1 = PeerId::new(key1.public_key());
    let peer2 = PeerId::new(key2.public_key());

    let config = Config { snapshot_hosts_cache_size: 100, part_selection_cache_batch_size: 1 };
    let cache = SnapshotHostsCache::new(config);
    assert_eq!(cache.get_hosts().len(), 0); // initially empty

    let sid_vec = |v: &[u64]| v.iter().cloned().map(Into::into).collect_vec();

    // initial insert
    let info0 = Arc::new(make_snapshot_host_info(&peer0, 123, sid_vec(&[0, 1, 2, 3]), &key0));
    let info1 = Arc::new(make_snapshot_host_info(&peer1, 123, sid_vec(&[2]), &key1));
    let res = cache.insert(vec![info0.clone(), info1.clone()]).await;
    assert_eq!([&info0, &info1].as_set(), unwrap(&res).as_set());
    assert_eq!([&info0, &info1].as_set(), cache.get_hosts().iter().collect::<HashSet<_>>());

    // second insert with various types of updates
    let info0new = Arc::new(make_snapshot_host_info(&peer0, 124, sid_vec(&[1, 3]), &key0));
    let info1old = Arc::new(make_snapshot_host_info(&peer1, 122, sid_vec(&[0, 1, 2, 3]), &key1));
    let info2 = Arc::new(make_snapshot_host_info(&peer2, 123, sid_vec(&[2]), &key2));
    let res = cache.insert(vec![info0new.clone(), info1old.clone(), info2.clone()]).await;
    assert_eq!([&info0new, &info2].as_set(), unwrap(&res).as_set());
    assert_eq!(
        [&info0new, &info1, &info2].as_set(),
        cache.get_hosts().iter().collect::<HashSet<_>>()
    );
}

#[tokio::test]
async fn invalid_signature() {
    init_test_logger();
    let mut rng = make_rng(2947294234);
    let rng = &mut rng;

    let key0 = data::make_secret_key(rng);
    let key1 = data::make_secret_key(rng);

    let peer0 = PeerId::new(key0.public_key());
    let peer1 = PeerId::new(key1.public_key());

    let config = Config { snapshot_hosts_cache_size: 100, part_selection_cache_batch_size: 1 };
    let cache = SnapshotHostsCache::new(config);

    let sid_vec = |v: &[u64]| v.iter().cloned().map(Into::into).collect_vec();

    let shards = sid_vec(&[0, 1, 2, 3]);
    let info0_invalid_sig = Arc::new(make_snapshot_host_info(&peer0, 1, shards.clone(), &key1));
    let info1 = Arc::new(make_snapshot_host_info(&peer1, 1, shards, &key1));
    let res = cache.insert(vec![info0_invalid_sig.clone(), info1.clone()]).await;
    // invalid signature => InvalidSignature
    assert_eq!(
        Some(SnapshotHostInfoError::VerificationError(
            SnapshotHostInfoVerificationError::InvalidSignature
        )),
        res.1
    );
    // Partial update is allowed in case an error is encountered.
    // The valid info1 may or may not be processed before the invalid info0 is detected
    // due to parallelization, so we check for superset rather than strict equality.
    assert_is_superset(&[&info1].as_set(), &res.0.as_set());
    // Partial update should match the state.
    assert_eq!(res.0.as_set(), cache.get_hosts().iter().collect::<HashSet<_>>());
}

#[tokio::test]
async fn too_many_shards() {
    init_test_logger();
    let mut rng = make_rng(2947294234);
    let rng = &mut rng;

    let key0 = data::make_secret_key(rng);
    let key1 = data::make_secret_key(rng);

    let peer0 = PeerId::new(key0.public_key());
    let peer1 = PeerId::new(key1.public_key());

    let config = Config { snapshot_hosts_cache_size: 100, part_selection_cache_batch_size: 1 };
    let cache = SnapshotHostsCache::new(config);

    let sid_vec = |v: &[u64]| v.iter().cloned().map(Into::into).collect_vec();

    // info0 is valid
    let info0 = Arc::new(make_snapshot_host_info(&peer0, 1, sid_vec(&[0, 1, 2, 3]), &key0));

    // info1 is invalid - it has more shard ids than MAX_SHARDS_PER_SNAPSHOT_HOST_INFO
    let too_many_shards: Vec<ShardId> =
        (0..(MAX_SHARDS_PER_SNAPSHOT_HOST_INFO as u64 + 1)).into_iter().map(Into::into).collect();
    let info1 = Arc::new(make_snapshot_host_info(&peer1, 1, too_many_shards, &key1));

    // info1.verify() should fail
    let expected_error =
        SnapshotHostInfoVerificationError::TooManyShards(MAX_SHARDS_PER_SNAPSHOT_HOST_INFO + 1);
    assert_eq!(info1.verify(), Err(expected_error.clone()));

    // Inserting should return the expected error (TooManyShards)
    let res = cache.insert(vec![info0.clone(), info1.clone()]).await;
    assert_eq!(Some(SnapshotHostInfoError::VerificationError(expected_error)), res.1);
    // Partial update is allowed in case an error is encountered.
    // The valid info0 may or may not be processed before the invalid info1 is detected
    // due to parallelization, so we check for superset rather than strict equality.
    assert_is_superset(&[&info0].as_set(), &res.0.as_set());
    // Partial update should match the state.
    assert_eq!(res.0.as_set(), cache.get_hosts().iter().collect::<HashSet<_>>());
}

#[tokio::test]
async fn duplicate_peer_id() {
    init_test_logger();
    let mut rng = make_rng(2947294234);
    let rng = &mut rng;

    let key0 = data::make_secret_key(rng);
    let peer0 = PeerId::new(key0.public_key());

    let config = Config { snapshot_hosts_cache_size: 100, part_selection_cache_batch_size: 1 };
    let cache = SnapshotHostsCache::new(config);

    let sid_vec = |v: &[u64]| v.iter().cloned().map(Into::into).collect_vec();

    let info00 = Arc::new(make_snapshot_host_info(&peer0, 1, sid_vec(&[0, 1, 2, 3]), &key0));
    let info01 = Arc::new(make_snapshot_host_info(&peer0, 2, sid_vec(&[0, 3]), &key0));
    let res = cache.insert(vec![info00.clone(), info01.clone()]).await;
    // duplicate peer ids => DuplicatePeerId
    assert_eq!(Some(SnapshotHostInfoError::DuplicatePeerId), res.1);
    // this type of malicious behavior is detected before verification even begins;
    // no partial data is stored
    assert_eq!(0, cache.get_hosts().len());
}

#[tokio::test]
async fn test_lru_eviction() {
    init_test_logger();
    let mut rng = make_rng(2947294234);
    let rng = &mut rng;

    let key0 = data::make_secret_key(rng);
    let key1 = data::make_secret_key(rng);
    let key2 = data::make_secret_key(rng);

    let peer0 = PeerId::new(key0.public_key());
    let peer1 = PeerId::new(key1.public_key());
    let peer2 = PeerId::new(key2.public_key());

    let config = Config { snapshot_hosts_cache_size: 2, part_selection_cache_batch_size: 1 };
    let cache = SnapshotHostsCache::new(config);

    let sid_vec = |v: &[u64]| v.iter().cloned().map(Into::into).collect_vec();

    // initial inserts to capacity
    let info0 = Arc::new(make_snapshot_host_info(&peer0, 123, sid_vec(&[0, 1, 2, 3]), &key0));
    let res = cache.insert(vec![info0.clone()]).await;
    assert_eq!([&info0].as_set(), unwrap(&res).as_set());
    assert_eq!([&info0].as_set(), cache.get_hosts().iter().collect::<HashSet<_>>());

    let info1 = Arc::new(make_snapshot_host_info(&peer1, 123, sid_vec(&[2]), &key1));
    let res = cache.insert(vec![info1.clone()]).await;
    assert_eq!([&info1].as_set(), unwrap(&res).as_set());
    assert_eq!([&info0, &info1].as_set(), cache.get_hosts().iter().collect::<HashSet<_>>());

    // insert past capacity
    let info2 = Arc::new(make_snapshot_host_info(&peer2, 123, sid_vec(&[1, 3]), &key2));
    let res = cache.insert(vec![info2.clone()]).await;
    // check that the new data is accepted
    assert_eq!([&info2].as_set(), unwrap(&res).as_set());
    // check that the oldest data was evicted
    assert_eq!([&info1, &info2].as_set(), cache.get_hosts().iter().collect::<HashSet<_>>());
}

// In each test, we will have a list of these, where they will indicate the function we
// should call on the SnapshotHostsCache, and in the case of CallSelect, also the return value
// we should expect to get, in this case expressed as the rank of the returned PeerId by priority score
#[derive(Debug)]
enum SelectPeerAction {
    // CallSelect(epoch_height, wanted)
    CallSelect(EpochHeight, Option<usize>),
    /// CallSetEpoch(epoch_height) Sets the current epoch height for the cache.
    CallSetEpoch(EpochHeight),
    /// InsertHosts(hosts, epoch_height, expected_insert_count) Inserts the given hosts into the cache for the given epoch height.
    InsertHosts(&'static [usize], EpochHeight, usize),
    /// PartReceived Marks the part as received for the given shard and part id.
    PartReceived,
    /// CheckNumberOfHosts(expected_number_of_hosts) Checks that the number of hosts in the cache is equal to the expected number of hosts.
    CheckNumberOfHosts(usize),
}

struct SelectPeerTest {
    name: &'static str,
    num_peers: usize,
    part_selection_cache_batch_size: u32,
    actions: &'static [SelectPeerAction],
}

const FIRST_EPOCH_HEIGHT: EpochHeight = 123;

static SELECT_PEER_CASES: &[SelectPeerTest] = &[
    SelectPeerTest {
        name: "test_select_peer_2_peers",
        num_peers: 2,
        part_selection_cache_batch_size: 1,
        actions: &[
            SelectPeerAction::CallSelect(FIRST_EPOCH_HEIGHT, None),
            SelectPeerAction::InsertHosts(&[0, 1], FIRST_EPOCH_HEIGHT, 2),
            SelectPeerAction::CheckNumberOfHosts(2),
            SelectPeerAction::CallSetEpoch(FIRST_EPOCH_HEIGHT),
            SelectPeerAction::CallSelect(FIRST_EPOCH_HEIGHT, Some(0)),
            SelectPeerAction::CallSelect(FIRST_EPOCH_HEIGHT, Some(1)),
            SelectPeerAction::CallSelect(FIRST_EPOCH_HEIGHT, Some(0)),
        ],
    },
    SelectPeerTest {
        name: "test_select_peer_3_peers",
        num_peers: 3,
        part_selection_cache_batch_size: 1,
        actions: &[
            SelectPeerAction::CallSelect(FIRST_EPOCH_HEIGHT, None),
            SelectPeerAction::CallSelect(FIRST_EPOCH_HEIGHT, None),
            SelectPeerAction::InsertHosts(&[1, 2], FIRST_EPOCH_HEIGHT, 2),
            SelectPeerAction::CheckNumberOfHosts(2),
            SelectPeerAction::CallSetEpoch(FIRST_EPOCH_HEIGHT),
            SelectPeerAction::CallSelect(FIRST_EPOCH_HEIGHT, Some(1)),
            SelectPeerAction::CallSelect(FIRST_EPOCH_HEIGHT, Some(2)),
            SelectPeerAction::CallSelect(FIRST_EPOCH_HEIGHT, Some(1)),
            SelectPeerAction::InsertHosts(&[0], FIRST_EPOCH_HEIGHT, 1),
            SelectPeerAction::CheckNumberOfHosts(3),
            SelectPeerAction::CallSelect(FIRST_EPOCH_HEIGHT, Some(0)),
            // Now 0 and 2 have been returned once and 1 has been returned twice.
            // We should go back to 0 and 2 once each, when each peer will have been returned twice.
            // Then after that it's just a normal looking round robin
            SelectPeerAction::CallSelect(FIRST_EPOCH_HEIGHT, Some(0)),
            SelectPeerAction::CallSelect(FIRST_EPOCH_HEIGHT, Some(2)),
            SelectPeerAction::CallSelect(FIRST_EPOCH_HEIGHT, Some(0)),
            SelectPeerAction::CallSelect(FIRST_EPOCH_HEIGHT, Some(1)),
            SelectPeerAction::CallSelect(FIRST_EPOCH_HEIGHT, Some(2)),
        ],
    },
    SelectPeerTest {
        name: "test_select_peer_2_part_received",
        num_peers: 2,
        part_selection_cache_batch_size: 1,
        actions: &[
            SelectPeerAction::CallSelect(FIRST_EPOCH_HEIGHT, None),
            SelectPeerAction::InsertHosts(&[0], FIRST_EPOCH_HEIGHT, 1),
            SelectPeerAction::CallSetEpoch(FIRST_EPOCH_HEIGHT),
            SelectPeerAction::CallSelect(FIRST_EPOCH_HEIGHT, Some(0)),
            SelectPeerAction::CallSelect(FIRST_EPOCH_HEIGHT, Some(0)),
            SelectPeerAction::InsertHosts(&[1], FIRST_EPOCH_HEIGHT, 1),
            SelectPeerAction::CallSelect(FIRST_EPOCH_HEIGHT, Some(1)),
            SelectPeerAction::CallSelect(FIRST_EPOCH_HEIGHT, Some(1)),
            // calling part_received() should result in correct/acceptable subsequent values
            // returned by select_host(), but where we forget everything about previous calls
            SelectPeerAction::PartReceived,
            SelectPeerAction::CallSelect(FIRST_EPOCH_HEIGHT, Some(0)),
            SelectPeerAction::CallSelect(FIRST_EPOCH_HEIGHT, Some(1)),
            SelectPeerAction::CallSelect(FIRST_EPOCH_HEIGHT, Some(0)),
            SelectPeerAction::CallSelect(FIRST_EPOCH_HEIGHT, Some(1)),
        ],
    },
    SelectPeerTest {
        name: "test_select_peer_5_peers",
        num_peers: 5,
        part_selection_cache_batch_size: 2,
        actions: &[
            SelectPeerAction::CallSelect(FIRST_EPOCH_HEIGHT, None),
            SelectPeerAction::InsertHosts(&[2, 3], FIRST_EPOCH_HEIGHT, 2),
            SelectPeerAction::CallSetEpoch(FIRST_EPOCH_HEIGHT),
            SelectPeerAction::CallSelect(FIRST_EPOCH_HEIGHT, Some(2)),
            SelectPeerAction::CallSelect(FIRST_EPOCH_HEIGHT, Some(3)),
            SelectPeerAction::InsertHosts(&[0, 1, 4], FIRST_EPOCH_HEIGHT, 3),
            SelectPeerAction::CheckNumberOfHosts(5),
            SelectPeerAction::CallSelect(FIRST_EPOCH_HEIGHT, Some(0)),
            SelectPeerAction::CallSelect(FIRST_EPOCH_HEIGHT, Some(1)),
            SelectPeerAction::CallSelect(FIRST_EPOCH_HEIGHT, Some(4)),
            SelectPeerAction::CallSelect(FIRST_EPOCH_HEIGHT, Some(0)),
        ],
    },
    SelectPeerTest {
        name: "test_select_peer_5_peers_insert_older",
        num_peers: 5,
        part_selection_cache_batch_size: 2,
        actions: &[
            SelectPeerAction::CallSelect(FIRST_EPOCH_HEIGHT, None),
            SelectPeerAction::InsertHosts(&[2, 3], FIRST_EPOCH_HEIGHT, 2),
            SelectPeerAction::CallSetEpoch(FIRST_EPOCH_HEIGHT),
            SelectPeerAction::CallSelect(FIRST_EPOCH_HEIGHT, Some(2)),
            SelectPeerAction::CallSelect(FIRST_EPOCH_HEIGHT, Some(3)),
            SelectPeerAction::InsertHosts(&[3, 1, 4], FIRST_EPOCH_HEIGHT, 2),
            SelectPeerAction::CheckNumberOfHosts(4),
            SelectPeerAction::CallSelect(FIRST_EPOCH_HEIGHT, Some(1)),
            SelectPeerAction::CallSelect(FIRST_EPOCH_HEIGHT, Some(4)),
            SelectPeerAction::CallSelect(FIRST_EPOCH_HEIGHT, Some(1)),
        ],
    },
    SelectPeerTest {
        name: "test_select_insert_new_epoch_hosts",
        num_peers: 5,
        part_selection_cache_batch_size: 2,
        actions: &[
            SelectPeerAction::CallSelect(FIRST_EPOCH_HEIGHT, None),
            SelectPeerAction::InsertHosts(&[2, 3], FIRST_EPOCH_HEIGHT, 2),
            SelectPeerAction::CallSetEpoch(FIRST_EPOCH_HEIGHT),
            SelectPeerAction::CallSelect(FIRST_EPOCH_HEIGHT, Some(2)),
            SelectPeerAction::CallSelect(FIRST_EPOCH_HEIGHT, Some(3)),
            SelectPeerAction::InsertHosts(&[1, 4], FIRST_EPOCH_HEIGHT + 2, 2),
            SelectPeerAction::CheckNumberOfHosts(4),
            SelectPeerAction::CallSelect(FIRST_EPOCH_HEIGHT, Some(2)),
            SelectPeerAction::CallSelect(FIRST_EPOCH_HEIGHT, Some(3)),
            SelectPeerAction::CallSetEpoch(FIRST_EPOCH_HEIGHT + 2),
            SelectPeerAction::CheckNumberOfHosts(2),
            SelectPeerAction::CallSelect(FIRST_EPOCH_HEIGHT + 2, Some(1)),
            SelectPeerAction::CallSelect(FIRST_EPOCH_HEIGHT + 2, Some(4)),
            SelectPeerAction::CallSelect(FIRST_EPOCH_HEIGHT + 2, Some(1)),
        ],
    },
    SelectPeerTest {
        name: "test_select_insert_old_epoch_hosts",
        num_peers: 5,
        part_selection_cache_batch_size: 2,
        actions: &[
            SelectPeerAction::CallSelect(FIRST_EPOCH_HEIGHT, None),
            SelectPeerAction::InsertHosts(&[2, 3], FIRST_EPOCH_HEIGHT, 2),
            SelectPeerAction::CheckNumberOfHosts(2),
            SelectPeerAction::CallSetEpoch(FIRST_EPOCH_HEIGHT),
            SelectPeerAction::CallSelect(FIRST_EPOCH_HEIGHT, Some(2)),
            SelectPeerAction::CallSelect(FIRST_EPOCH_HEIGHT, Some(3)),
            SelectPeerAction::InsertHosts(&[3, 1, 4], FIRST_EPOCH_HEIGHT - 2, 0),
            SelectPeerAction::CheckNumberOfHosts(2),
            SelectPeerAction::CallSelect(FIRST_EPOCH_HEIGHT, Some(2)),
            SelectPeerAction::CallSelect(FIRST_EPOCH_HEIGHT, Some(3)),
            SelectPeerAction::CallSelect(FIRST_EPOCH_HEIGHT, Some(2)),
        ],
    },
    SelectPeerTest {
        name: "test_number_of_hosts",
        num_peers: 7,
        part_selection_cache_batch_size: 2,
        actions: &[
            SelectPeerAction::InsertHosts(&[0, 1], FIRST_EPOCH_HEIGHT - 1, 2),
            SelectPeerAction::InsertHosts(&[2, 3], FIRST_EPOCH_HEIGHT, 2),
            SelectPeerAction::InsertHosts(&[4, 5], FIRST_EPOCH_HEIGHT + 1, 2),
            SelectPeerAction::InsertHosts(&[5, 6], FIRST_EPOCH_HEIGHT + 2, 2),
            SelectPeerAction::CheckNumberOfHosts(7),
            SelectPeerAction::CallSetEpoch(FIRST_EPOCH_HEIGHT),
            SelectPeerAction::CheckNumberOfHosts(7),
            SelectPeerAction::CallSetEpoch(FIRST_EPOCH_HEIGHT + 1),
            SelectPeerAction::CheckNumberOfHosts(5),
            SelectPeerAction::CallSetEpoch(FIRST_EPOCH_HEIGHT + 2),
            SelectPeerAction::CheckNumberOfHosts(3),
            SelectPeerAction::CallSetEpoch(FIRST_EPOCH_HEIGHT + 3),
            SelectPeerAction::CheckNumberOfHosts(2),
        ],
    },
];

async fn run_select_peer_test(
    test_name: &'static str,
    actions: &[SelectPeerAction],
    keys: &[SecretKey],
    part_id: u64,
    part_selection_cache_batch_size: u32,
) {
    let config =
        Config { snapshot_hosts_cache_size: keys.len() as u32, part_selection_cache_batch_size };
    let cache = SnapshotHostsCache::new_with_epoch_retention_window(config, 1);

    tracing::debug!(test_name, "start");

    for action in actions {
        tracing::debug!(test_name, ?action, "run");
        match action {
            SelectPeerAction::InsertHosts(hosts, epoch_height, expected_insert_count) => {
                let new_hosts = make_snapshot_hosts_info(hosts, keys, *epoch_height);
                let requested_insert_count = new_hosts.len();
                let (res, err) = cache.insert(new_hosts).await;
                assert_eq!(
                    res.len(),
                    *expected_insert_count,
                    "{test_name} insert should return the expected number of hosts"
                );
                assert!(requested_insert_count == *expected_insert_count || err.is_none());
            }
            SelectPeerAction::CallSetEpoch(epoch_height) => {
                cache.set_current_epoch_height(*epoch_height);
            }
            SelectPeerAction::CallSelect(epoch_height, wanted) => {
                let sync_hash = CryptoHash::hash_borsh(epoch_height);
                let peer = cache.select_host_for_part(&sync_hash, ShardId::new(0), part_id);
                let wanted = match wanted {
                    Some(idx) => Some(PeerId::new(keys[*idx].public_key())),
                    None => None,
                };
                assert!(peer == wanted, "{test_name} got: {:?} want: {:?}", &peer, &wanted);
            }
            SelectPeerAction::PartReceived => {
                let shard_id = ShardId::new(0);
                assert!(cache.has_selector(shard_id, part_id));
                cache.part_received(shard_id, part_id);
                assert!(!cache.has_selector(shard_id, part_id));
            }
            SelectPeerAction::CheckNumberOfHosts(expected_number_of_hosts) => {
                assert_eq!(cache.get_hosts().len(), *expected_number_of_hosts);
            }
        }
    }
}

fn make_snapshot_hosts_info(
    hosts: &[usize],
    keys: &[SecretKey],
    epoch_height: EpochHeight,
) -> Vec<Arc<SnapshotHostInfo>> {
    let mut peers = Vec::with_capacity(keys.len());
    let sid_vec = |v: &[u64]| v.iter().cloned().map(Into::into).collect_vec();
    for host in hosts {
        let key = &keys[*host];
        let peer_id = PeerId::new(key.public_key());
        let sync_hash = CryptoHash::hash_borsh(epoch_height);
        let info = Arc::new(SnapshotHostInfo::new(
            peer_id,
            sync_hash,
            epoch_height,
            sid_vec(&[0, 1, 2, 3]),
            &key,
        ));
        peers.push(info);
    }

    peers
}

#[tokio::test]
async fn test_select_peer() {
    init_test_logger();
    let mut rng = make_rng(2947294234);
    let part_id = 0;
    let num_peers = SELECT_PEER_CASES.iter().map(|t| t.num_peers).max().unwrap();

    let mut keys = Vec::with_capacity(num_peers);

    for _ in 0..num_peers {
        let key = data::make_secret_key(&mut rng);
        let peer_id = PeerId::new(key.public_key());
        let score = priority_score(&peer_id, ShardId::new(0), part_id);
        keys.push((key.clone(), score));
    }
    // cspell:ignore lkey lscore rkey rscore
    keys.sort_by(|(_lkey, lscore), (_rkey, rscore)| lscore.partial_cmp(rscore).unwrap().reverse());
    let keys = keys.into_iter().map(|(key, _score)| key).collect::<Vec<_>>();

    for t in SELECT_PEER_CASES {
        run_select_peer_test(
            t.name,
            &t.actions,
            &keys[..t.num_peers],
            part_id,
            t.part_selection_cache_batch_size,
        )
        .await;
    }
}
