use crate::accounts_data::*;
use crate::network_protocol::testonly as data;
use crate::network_protocol::SignedAccountData;
use crate::testonly::{assert_is_superset, make_rng, AsSet as _, Rng};
use near_async::time;
use near_o11y::testonly::init_test_logger;
use near_primitives::validator_signer::InMemoryValidatorSigner;
use pretty_assertions::assert_eq;
use std::collections::HashSet;
use std::sync::Arc;

fn make_account_data(
    rng: &mut Rng,
    clock: &time::Clock,
    version: u64,
    signer: &InMemoryValidatorSigner,
) -> SignedAccountData {
    let peer_id = data::make_peer_id(rng);
    data::make_account_data(rng, version, clock.now_utc(), signer.public_key(), peer_id)
        .sign(signer)
        .unwrap()
}

fn unwrap<'a, T: std::hash::Hash + std::cmp::Eq, E: std::fmt::Debug>(
    v: &'a (T, Option<E>),
) -> &'a T {
    if let Some(err) = &v.1 {
        panic!("unexpected error: {err:?}");
    }
    &v.0
}

fn make_signers(rng: &mut Rng, n: usize) -> Vec<InMemoryValidatorSigner> {
    (0..n).map(|_| data::make_validator_signer(rng)).collect()
}

#[tokio::test]
async fn happy_path() {
    init_test_logger();
    let mut rng = make_rng(2947294234);
    let rng = &mut rng;
    let clock = time::FakeClock::default();

    let signers: Vec<_> = make_signers(rng, 7);
    let e0 = Arc::new(data::make_account_keys(&signers[0..5]));
    let e1 = Arc::new(data::make_account_keys(&signers[2..7]));

    let cache = Arc::new(AccountDataCache::new());
    assert_eq!(cache.load().data.values().count(), 0); // initially empty
    assert!(cache.set_keys(e0.clone()));
    assert_eq!(cache.load().data.values().count(), 0); // empty after initial set_keys.

    // initial insert
    let a0 = Arc::new(make_account_data(rng, &clock.clock(), 1, &signers[0]));
    let a1 = Arc::new(make_account_data(rng, &clock.clock(), 1, &signers[1]));
    let res = cache.clone().insert(&clock.clock(), vec![a0.clone(), a1.clone()]).await;
    assert_eq!([&a0, &a1].as_set(), unwrap(&res).as_set());
    assert_eq!([&a0, &a1].as_set(), cache.load().data.values().collect::<HashSet<_>>());

    // entries of various types
    let a0new = Arc::new(make_account_data(rng, &clock.clock(), 2, &signers[0]));
    let a1old = Arc::new(make_account_data(rng, &clock.clock(), 0, &signers[1]));
    let a2 = Arc::new(make_account_data(rng, &clock.clock(), 1, &signers[2]));
    let a5 = Arc::new(make_account_data(rng, &clock.clock(), 1, &signers[5]));
    let res = cache
        .clone()
        .insert(
            &clock.clock(),
            vec![
                a2.clone(),    // initial value => insert
                a0new.clone(), // with newer timestamp => insert,
                a1old.clone(), // with older timestamp => filter out,
                a5.clone(),    // not in e0 => filter out.
            ],
        )
        .await;
    assert_eq!([&a2, &a0new].as_set(), unwrap(&res).as_set());
    assert_eq!([&a0new, &a1, &a2].as_set(), cache.load().data.values().collect::<HashSet<_>>());

    // try setting the same key set again, should be a noop.
    assert!(!cache.set_keys(e0));
    assert_eq!([&a0new, &a1, &a2].as_set(), cache.load().data.values().collect::<HashSet<_>>());

    // set_keys again. Data for accounts which are not in the new set should be dropped.
    assert!(cache.set_keys(e1));
    assert_eq!([&a2].as_set(), cache.load().data.values().collect::<HashSet<_>>());
    // insert some entries again.
    let res = cache
        .clone()
        .insert(
            &clock.clock(),
            vec![
                a0.clone(), // a0 is not in e1 => filter out
                a5.clone(), // a5 is in e1 => insert,
            ],
        )
        .await;
    assert_eq!([&a5].as_set(), unwrap(&res).as_set());
    assert_eq!([&a2, &a5].as_set(), cache.load().data.values().collect::<HashSet<_>>());
}

#[tokio::test]
async fn data_too_large() {
    init_test_logger();
    let mut rng = make_rng(2947294234);
    let rng = &mut rng;
    let clock = time::FakeClock::default();

    let signers = make_signers(rng, 3);
    let e = Arc::new(data::make_account_keys(&signers));

    let cache = Arc::new(AccountDataCache::new());
    cache.set_keys(e);
    let a0 = Arc::new(make_account_data(rng, &clock.clock(), 1, &signers[0]));
    let a1 = Arc::new(make_account_data(rng, &clock.clock(), 1, &signers[1]));
    let mut a2_too_large: SignedAccountData =
        make_account_data(rng, &clock.clock(), 1, &signers[2]);
    *a2_too_large.payload_mut() =
        (0..crate::network_protocol::MAX_ACCOUNT_DATA_SIZE_BYTES + 1).map(|_| 17).collect();
    let a2_too_large = Arc::new(a2_too_large);

    // too large payload => DataTooLarge
    let res = cache
        .clone()
        .insert(
            &clock.clock(),
            vec![
                a0.clone(),
                a1.clone(),
                a2_too_large.clone(), // invalid entry => DataTooLarge
            ],
        )
        .await;
    assert_eq!(Some(AccountDataError::DataTooLarge), res.1);
    // Partial update is allowed, in case an error is encountered.
    assert_is_superset(&[&a0, &a1].as_set(), &res.0.as_set());
    // Partial update should match the state.
    assert_eq!(res.0.as_set(), cache.load().data.values().collect::<HashSet<_>>());
}

#[tokio::test]
async fn invalid_signature() {
    init_test_logger();
    let mut rng = make_rng(2947294234);
    let rng = &mut rng;
    let clock = time::FakeClock::default();

    let signers = make_signers(rng, 3);
    let e = Arc::new(data::make_account_keys(&signers));

    let cache = Arc::new(AccountDataCache::new());
    cache.set_keys(e);
    let a0 = Arc::new(make_account_data(rng, &clock.clock(), 1, &signers[0]));
    let mut a1 = make_account_data(rng, &clock.clock(), 1, &signers[1]);
    let mut a2_invalid_sig = make_account_data(rng, &clock.clock(), 1, &signers[2]);
    *a2_invalid_sig.signature_mut() = a1.signature_mut().clone();
    let a1 = Arc::new(a1);
    let a2_invalid_sig = Arc::new(a2_invalid_sig);

    // invalid signature => InvalidSignature
    let res = cache
        .clone()
        .insert(
            &clock.clock(),
            vec![
                a0.clone(),
                a1.clone(),
                a2_invalid_sig.clone(), // invalid entry => DataTooLarge
            ],
        )
        .await;
    assert_eq!(Some(AccountDataError::InvalidSignature), res.1);
    // Partial update is allowed, in case an error is encountered.
    assert_is_superset(&[&a0, &a1].as_set(), &res.0.as_set());
    // Partial update should match the state.
    assert_eq!(res.0.as_set(), cache.load().data.values().collect::<HashSet<_>>());
}

#[tokio::test]
async fn single_account_multiple_data() {
    init_test_logger();
    let mut rng = make_rng(2947294234);
    let rng = &mut rng;
    let clock = time::FakeClock::default();

    let signers = make_signers(rng, 3);
    let e = Arc::new(data::make_account_keys(&signers));

    let cache = Arc::new(AccountDataCache::new());
    cache.set_keys(e);
    let a0 = Arc::new(make_account_data(rng, &clock.clock(), 1, &signers[0]));
    let a1 = Arc::new(make_account_data(rng, &clock.clock(), 1, &signers[1]));
    let a2old = Arc::new(make_account_data(rng, &clock.clock(), 1, &signers[2]));
    let a2new = Arc::new(make_account_data(rng, &clock.clock(), 2, &signers[2]));

    // 2 entries for the same (epoch_id,account_id) => SingleAccountMultipleData
    let res = cache
        .clone()
        .insert(&clock.clock(), vec![a0.clone(), a1.clone(), a2old.clone(), a2new.clone()])
        .await;
    assert_eq!(Some(AccountDataError::SingleAccountMultipleData), res.1);
    // Partial update is allowed, in case an error is encountered.
    assert_is_superset(&[&a0, &a1, &a2old, &a2new].as_set(), &res.0.as_set());
    // Partial update should match the state, this also verifies that only 1 of the competing
    // entries has been applied.
    assert_eq!(res.0.as_set(), cache.load().data.values().collect::<HashSet<_>>());
}

/// Test checking that cache immediately overrides any inserted AccountData for local.signer
/// with local.data.
#[tokio::test]
async fn set_local() {
    init_test_logger();
    let mut rng = make_rng(2947294234);
    let rng = &mut rng;
    let clock = time::FakeClock::default();

    let signers: Vec<_> = make_signers(rng, 3);
    let e0 = Arc::new(data::make_account_keys(&signers[0..2]));
    let e1 = Arc::new(data::make_account_keys(&signers[1..3]));

    let cache = Arc::new(AccountDataCache::new());
    assert!(cache.set_keys(e0.clone()));

    // Set local while local.signer is in cache.keys.
    // A new AccountData should be signed.
    let local = LocalAccountData {
        signer: Arc::new(signers[0].clone()),
        data: Arc::new(make_account_data(rng, &clock.clock(), 1, &signers[0]).data.clone()),
    };
    let got = cache.set_local(&clock.clock(), local.clone()).unwrap();
    assert_eq!(local.data.as_ref(), &got.data);
    assert_eq!(local.signer.public_key(), got.account_key);
    clock.advance(time::Duration::hours(1));

    // Insert new version while local data is set and local.signer is in cache.keys.
    // AccountDataCache should immediately emit AccountData for local.signer which overrides
    // the new version received.
    let a0 = Arc::new(make_account_data(rng, &clock.clock(), 7, &signers[0]));
    // Regular entry for a signer in cache.keys. The new version should be accepted.
    let a1 = Arc::new(make_account_data(rng, &clock.clock(), 10, &signers[1]));
    // Regular entry for a signer outside of cache.keys. The new version should be ignored.
    let a2 = Arc::new(make_account_data(rng, &clock.clock(), 8, &signers[2]));

    let res = cache.clone().insert(&clock.clock(), vec![a0.clone(), a1.clone(), a2.clone()]).await;
    assert_eq!(res.0.as_set(), cache.load().data.values().collect::<HashSet<_>>());
    let res: HashMap<_, _> = unwrap(&res).iter().map(|a| (a.account_key.clone(), a)).collect();
    let got = res.get(&signers[0].public_key()).unwrap();
    assert_eq!(local.data.as_ref(), &got.data);
    assert!(a0.version < got.version);
    assert_eq!(a1.as_ref(), res.get(&signers[1].public_key()).unwrap().as_ref());
    assert_eq!(None, res.get(&signers[2].public_key()));

    // Insert new version while local data is set but local.signer is not in cache.keys.
    // local data should be just ignored.
    clock.advance(time::Duration::hours(1));
    assert!(cache.set_keys(e1.clone()));
    let a0 = Arc::new(make_account_data(rng, &clock.clock(), got.version + 1, &signers[0]));
    // Regular entries for a signers in cache.keys. The new version should be accepted.
    let a1 = Arc::new(make_account_data(rng, &clock.clock(), a1.version + 1, &signers[1]));
    let a2 = Arc::new(make_account_data(rng, &clock.clock(), a2.version + 1, &signers[2]));

    let res = cache.clone().insert(&clock.clock(), vec![a0.clone(), a1.clone(), a2.clone()]).await;
    assert_eq!(res.0.as_set(), cache.load().data.values().collect::<HashSet<_>>());
    assert_eq!([&a1, &a2].as_set(), unwrap(&res).as_set());

    // Update local data to a signer in cache.keys.
    let local = LocalAccountData {
        signer: Arc::new(signers[2].clone()),
        data: Arc::new(make_account_data(rng, &clock.clock(), 1, &signers[2]).data.clone()),
    };
    let got = cache.set_local(&clock.clock(), local.clone()).unwrap();
    assert_eq!(local.data.as_ref(), &got.data);
    assert_eq!(local.signer.public_key(), got.account_key);
    assert_eq!([&a1, &got].as_set(), cache.load().data.values().collect::<HashSet<_>>());

    // Update local data to a signer outside of cache.keys.
    let local = LocalAccountData {
        signer: Arc::new(signers[0].clone()),
        data: Arc::new(make_account_data(rng, &clock.clock(), 1, &signers[0]).data.clone()),
    };
    assert_eq!(None, cache.set_local(&clock.clock(), local));
    assert_eq!([&a1, &got].as_set(), cache.load().data.values().collect::<HashSet<_>>());
}
