use crate::network_protocol::testonly as data;
use crate::snapshot_hosts::{Config, SnapshotHostInfoError, SnapshotHostsCache};
use crate::testonly::assert_is_superset;
use crate::testonly::{make_rng, AsSet as _};
use crate::types::SnapshotHostInfo;
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

    let config = Config { snapshot_hosts_cache_size: 100 };
    let cache = SnapshotHostsCache::new(config);
    assert_eq!(cache.get_hosts().len(), 0); // initially empty

    // initial insert
    let info0 = Arc::new(make_snapshot_host_info(&peer0, 123, vec![0, 1, 2, 3], &key0));
    let info1 = Arc::new(make_snapshot_host_info(&peer1, 123, vec![2], &key1));
    let res = cache.insert(vec![info0.clone(), info1.clone()]).await;
    assert_eq!([&info0, &info1].as_set(), unwrap(&res).as_set());
    assert_eq!([&info0, &info1].as_set(), cache.get_hosts().iter().collect::<HashSet<_>>());

    // second insert with various types of updates
    let info0new = Arc::new(make_snapshot_host_info(&peer0, 124, vec![1, 3], &key0));
    let info1old = Arc::new(make_snapshot_host_info(&peer1, 122, vec![0, 1, 2, 3], &key1));
    let info2 = Arc::new(make_snapshot_host_info(&peer2, 123, vec![2], &key2));
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

    let config = Config { snapshot_hosts_cache_size: 100 };
    let cache = SnapshotHostsCache::new(config);

    let info0_invalid_sig = Arc::new(make_snapshot_host_info(&peer0, 1, vec![0, 1, 2, 3], &key1));
    let info1 = Arc::new(make_snapshot_host_info(&peer1, 1, vec![0, 1, 2, 3], &key1));
    let res = cache.insert(vec![info0_invalid_sig.clone(), info1.clone()]).await;
    // invalid signature => InvalidSignature
    assert_eq!(Some(SnapshotHostInfoError::InvalidSignature), res.1);
    // Partial update is allowed in case an error is encountered.
    // The valid info1 may or may not be processed before the invalid info0 is detected
    // due to parallelization, so we check for superset rather than strict equality.
    assert_is_superset(&[&info1].as_set(), &res.0.as_set());
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

    let config = Config { snapshot_hosts_cache_size: 100 };
    let cache = SnapshotHostsCache::new(config);

    let info00 = Arc::new(make_snapshot_host_info(&peer0, 1, vec![0, 1, 2, 3], &key0));
    let info01 = Arc::new(make_snapshot_host_info(&peer0, 2, vec![0, 3], &key0));
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

    let config = Config { snapshot_hosts_cache_size: 2 };
    let cache = SnapshotHostsCache::new(config);

    // initial inserts to capacity
    let info0 = Arc::new(make_snapshot_host_info(&peer0, 123, vec![0, 1, 2, 3], &key0));
    let res = cache.insert(vec![info0.clone()]).await;
    assert_eq!([&info0].as_set(), unwrap(&res).as_set());
    assert_eq!([&info0].as_set(), cache.get_hosts().iter().collect::<HashSet<_>>());

    let info1 = Arc::new(make_snapshot_host_info(&peer1, 123, vec![2], &key1));
    let res = cache.insert(vec![info1.clone()]).await;
    assert_eq!([&info1].as_set(), unwrap(&res).as_set());
    assert_eq!([&info0, &info1].as_set(), cache.get_hosts().iter().collect::<HashSet<_>>());

    // insert past capacity
    let info2 = Arc::new(make_snapshot_host_info(&peer2, 123, vec![1, 3], &key2));
    let res = cache.insert(vec![info2.clone()]).await;
    // check that the new data is accepted
    assert_eq!([&info2].as_set(), unwrap(&res).as_set());
    // check that the oldest data was evicted
    assert_eq!([&info1, &info2].as_set(), cache.get_hosts().iter().collect::<HashSet<_>>());
}
