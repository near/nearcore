use crate::accounts_data::*;
use crate::network_protocol::testonly as data;
use crate::network_protocol::SignedAccountData;
use crate::testonly::{assert_is_superset, make_rng, AsSet as _, Rng};
use crate::time;
use near_primitives::validator_signer::InMemoryValidatorSigner;
use pretty_assertions::assert_eq;
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
    let mut rng = make_rng(2947294234);
    let rng = &mut rng;
    let clock = time::FakeClock::default();

    let signers: Vec<_> = make_signers(rng, 7);
    let e0 = Arc::new(data::make_account_keys(&signers[0..5]));
    let e1 = Arc::new(data::make_account_keys(&signers[2..7]));

    let cache = Arc::new(Cache::new());
    assert_eq!(cache.load().data.values().count(), 0); // initially empty
    assert!(cache.set_keys(e0.clone()));
    assert_eq!(cache.load().data.values().count(), 0); // empty after initial set_keys.

    // initial insert
    let a0 = Arc::new(make_account_data(rng, &clock.clock(), 1, &signers[0]));
    let a1 = Arc::new(make_account_data(rng, &clock.clock(), 1, &signers[1]));
    let res = cache.clone().insert(vec![a0.clone(), a1.clone()]).await;
    assert_eq!([&a0, &a1].as_set(), unwrap(&res).as_set());
    assert_eq!([&a0, &a1].as_set(), cache.load().data.values().collect());

    // entries of various types
    let a0new = Arc::new(make_account_data(rng, &clock.clock(), 2, &signers[0]));
    let a1old = Arc::new(make_account_data(rng, &clock.clock(), 0, &signers[1]));
    let a2 = Arc::new(make_account_data(rng, &clock.clock(), 1, &signers[2]));
    let a5 = Arc::new(make_account_data(rng, &clock.clock(), 1, &signers[5]));
    let res = cache
        .clone()
        .insert(vec![
            a2.clone(),    // initial value => insert
            a0new.clone(), // with newer timestamp => insert,
            a1old.clone(), // with older timestamp => filter out,
            a5.clone(),    // not in e0 => filter out.
        ])
        .await;
    assert_eq!([&a2, &a0new].as_set(), unwrap(&res).as_set());
    assert_eq!([&a0new, &a1, &a2].as_set(), cache.load().data.values().collect());

    // try setting the same key set again, should be a noop.
    assert!(!cache.set_keys(e0));
    assert_eq!([&a0new, &a1, &a2].as_set(), cache.load().data.values().collect());

    // set_keys again. Data for accounts which are not in the new set should be dropped.
    assert!(cache.set_keys(e1));
    assert_eq!([&a2].as_set(), cache.load().data.values().collect());
    // insert some entries again.
    let res = cache
        .clone()
        .insert(vec![
            a0.clone(), // a0 is not in e1 => filter out
            a5.clone(), // a5 is in e1 => insert,
        ])
        .await;
    assert_eq!([&a5].as_set(), unwrap(&res).as_set());
    assert_eq!([&a2, &a5].as_set(), cache.load().data.values().collect());
}

#[tokio::test]
async fn data_too_large() {
    let mut rng = make_rng(2947294234);
    let rng = &mut rng;
    let clock = time::FakeClock::default();

    let signers = make_signers(rng, 3);
    let e = Arc::new(data::make_account_keys(&signers));

    let cache = Arc::new(Cache::new());
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
        .insert(vec![
            a0.clone(),
            a1.clone(),
            a2_too_large.clone(), // invalid entry => DataTooLarge
        ])
        .await;
    assert_eq!(Some(Error::DataTooLarge), res.1);
    // Partial update is allowed, in case an error is encountered.
    assert_is_superset(&[&a0, &a1].as_set(), &res.0.as_set());
    // Partial update should match the state.
    assert_eq!(res.0.as_set(), cache.load().data.values().collect());
}

#[tokio::test]
async fn invalid_signature() {
    let mut rng = make_rng(2947294234);
    let rng = &mut rng;
    let clock = time::FakeClock::default();

    let signers = make_signers(rng, 3);
    let e = Arc::new(data::make_account_keys(&signers));

    let cache = Arc::new(Cache::new());
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
        .insert(vec![
            a0.clone(),
            a1.clone(),
            a2_invalid_sig.clone(), // invalid entry => DataTooLarge
        ])
        .await;
    assert_eq!(Some(Error::InvalidSignature), res.1);
    // Partial update is allowed, in case an error is encountered.
    assert_is_superset(&[&a0, &a1].as_set(), &res.0.as_set());
    // Partial update should match the state.
    assert_eq!(res.0.as_set(), cache.load().data.values().collect());
}

#[tokio::test]
async fn single_account_multiple_data() {
    let mut rng = make_rng(2947294234);
    let rng = &mut rng;
    let clock = time::FakeClock::default();

    let signers = make_signers(rng, 3);
    let e = Arc::new(data::make_account_keys(&signers));

    let cache = Arc::new(Cache::new());
    cache.set_keys(e);
    let a0 = Arc::new(make_account_data(rng, &clock.clock(), 1, &signers[0]));
    let a1 = Arc::new(make_account_data(rng, &clock.clock(), 1, &signers[1]));
    let a2old = Arc::new(make_account_data(rng, &clock.clock(), 1, &signers[2]));
    let a2new = Arc::new(make_account_data(rng, &clock.clock(), 2, &signers[2]));

    // 2 entries for the same (epoch_id,account_id) => SingleAccountMultipleData
    let res =
        cache.clone().insert(vec![a0.clone(), a1.clone(), a2old.clone(), a2new.clone()]).await;
    assert_eq!(Some(Error::SingleAccountMultipleData), res.1);
    // Partial update is allowed, in case an error is encountered.
    assert_is_superset(&[&a0, &a1, &a2old, &a2new].as_set(), &res.0.as_set());
    // Partial update should match the state, this also verifies that only 1 of the competing
    // entries has been applied.
    assert_eq!(res.0.as_set(), cache.load().data.values().collect());
}
