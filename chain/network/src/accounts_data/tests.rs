use crate::accounts_data::*;
use crate::network_protocol::testonly as data;
use crate::network_protocol::{AccountData, SignedAccountData};
use crate::testonly::{make_rng, AsSet as _, Rng};
use near_crypto::{InMemorySigner, SecretKey};
use near_network_primitives::time;
use near_network_primitives::types::NetworkEpochInfo;
use near_primitives::types::{AccountId, EpochId};
use pretty_assertions::assert_eq;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Default)]
struct Signers {
    keys: HashMap<(EpochId, AccountId), SecretKey>,
}

impl Signers {
    fn key(&mut self, rng: &mut Rng, epoch_id: &EpochId, account_id: &AccountId) -> &SecretKey {
        self.keys
            .entry((epoch_id.clone(), account_id.clone()))
            .or_insert_with(|| data::make_secret_key(rng))
    }

    fn make_epoch(&mut self, rng: &mut Rng, account_ids: &[&AccountId]) -> NetworkEpochInfo {
        let epoch_id = data::make_epoch_id(rng);
        NetworkEpochInfo {
            id: epoch_id.clone(),
            priority_accounts: account_ids
                .iter()
                .map(|aid| ((*aid).clone(), self.key(rng, &epoch_id, &aid).public_key()))
                .collect(),
        }
    }

    fn make_account_data(
        &mut self,
        rng: &mut Rng,
        epoch_id: &EpochId,
        account_id: &AccountId,
        timestamp: time::Utc,
    ) -> SignedAccountData {
        let signer = InMemorySigner::from_secret_key(
            account_id.clone(),
            self.key(rng, epoch_id, account_id).clone(),
        );
        AccountData {
            peers: (0..3)
                .map(|_| {
                    let ip = data::make_ipv6(rng);
                    data::make_peer_addr(rng, ip)
                })
                .collect(),
            account_id: account_id.clone(),
            epoch_id: epoch_id.clone(),
            timestamp,
        }
        .sign(&signer)
        .unwrap()
    }
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
    let mut rng = make_rng(2947294234);
    let rng = &mut rng;
    let clock = time::FakeClock::default();
    let now = clock.now_utc();

    let accounts: Vec<_> = (0..4).map(|_| data::make_account_id(rng)).collect();
    let mut signers = Signers::default();
    let e0 = signers.make_epoch(rng, &[&accounts[0], &accounts[1], &accounts[2]]);
    let e1 = signers.make_epoch(rng, &[&accounts[0], &accounts[3]]);
    let e2 = signers.make_epoch(rng, &[&accounts[1], &accounts[2]]);

    let cache = Arc::new(Cache::new());
    assert_eq!(cache.dump(), vec![]); // initially empty
    cache.set_epochs(vec![&e0, &e1]);
    assert_eq!(cache.dump(), vec![]); // empty after initial set_epoch.

    // initial insert
    let e0a0 = signers.make_account_data(rng, &e0.id, &accounts[0], now);
    let e0a1 = signers.make_account_data(rng, &e0.id, &accounts[1], now);
    let res = cache.clone().insert(vec![e0a0.clone(), e0a1.clone()]).await;
    assert_eq!([&e0a0, &e0a1].as_set(), unwrap(&res).as_set());
    assert_eq!([&e0a0, &e0a1].as_set(), cache.dump().as_set());

    // entries of various types
    let e1a0 = signers.make_account_data(rng, &e1.id, &accounts[0], now);
    let e0a0new =
        signers.make_account_data(rng, &e0.id, &accounts[0], now + time::Duration::seconds(1));
    let e0a1old =
        signers.make_account_data(rng, &e0.id, &accounts[1], now - time::Duration::seconds(1));
    let e2a1 = signers.make_account_data(rng, &e2.id, &accounts[1], now);
    let res = cache
        .clone()
        .insert(vec![
            e1a0.clone(),    // initial value => insert
            e0a0new.clone(), // with newer timestamp => insert,
            e0a1old.clone(), // with older timestamp => filter out,
            e2a1.clone(),    // from inactive epoch => filter out,
        ])
        .await;
    assert_eq!([&e1a0, &e0a0new].as_set(), unwrap(&res).as_set());
    assert_eq!([&e0a0new, &e0a1, &e1a0].as_set(), cache.dump().as_set());

    // set_epoch again. Entries from inactive epochs should be dropped.
    cache.set_epochs(vec![&e0, &e2]);
    assert_eq!([&e0a0new, &e0a1].as_set(), cache.dump().as_set());
    // insert some entries again.
    let e0a2 = signers.make_account_data(rng, &e0.id, &accounts[2], now);
    let e1a3 = signers.make_account_data(rng, &e1.id, &accounts[3], now);
    let res = cache
        .clone()
        .insert(vec![
            e0a2.clone(), // e0 is still active => insert,
            e1a3.clone(), // e1 is not active any more => filter out,
            e2a1.clone(), // e2 has become active => insert,
        ])
        .await;
    assert_eq!([&e0a2, &e2a1].as_set(), unwrap(&res).as_set());
    assert_eq!([&e0a0new, &e0a1, &e0a2, &e2a1].as_set(), cache.dump().as_set());
}

#[tokio::test]
async fn data_too_large() {
    let mut rng = make_rng(2947294234);
    let rng = &mut rng;
    let clock = time::FakeClock::default();
    let now = clock.now_utc();

    let accounts: Vec<_> = (0..3).map(|_| data::make_account_id(rng)).collect();
    let mut signers = Signers::default();
    let e = signers.make_epoch(rng, &[&accounts[0], &accounts[1], &accounts[2]]);

    let cache = Arc::new(Cache::new());
    cache.set_epochs(vec![&e]);
    let a0 = signers.make_account_data(rng, &e.id, &accounts[0], now);
    let a1 = signers.make_account_data(rng, &e.id, &accounts[1], now);
    let mut a2_too_large = signers.make_account_data(rng, &e.id, &accounts[2], now);
    *a2_too_large.payload_mut() =
        (0..crate::network_protocol::MAX_ACCOUNT_DATA_SIZE_BYTES + 1).map(|_| 17).collect();

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
    assert!([&a0, &a1].as_set().is_superset(&res.0.as_set()));
    // Partial update should match the state.
    assert_eq!(res.0.as_set(), cache.dump().as_set());
}

#[tokio::test]
async fn invalid_account() {
    let mut rng = make_rng(2947294234);
    let rng = &mut rng;
    let clock = time::FakeClock::default();
    let now = clock.now_utc();

    let accounts: Vec<_> = (0..2).map(|_| data::make_account_id(rng)).collect();
    let mut signers = Signers::default();
    let e1 = signers.make_epoch(rng, &[&accounts[0]]);
    let e2 = signers.make_epoch(rng, &[&accounts[1]]);

    let cache = Arc::new(Cache::new());
    cache.set_epochs(vec![&e1, &e2]);
    let a0 = signers.make_account_data(rng, &e1.id, &accounts[0], now);
    // Account 1 is active in epoch e2, not e1. It should cause InvalidAccount error.
    let a1 = signers.make_account_data(rng, &e1.id, &accounts[1], now);

    let res = cache.clone().insert(vec![a0.clone(), a1.clone()]).await;
    assert_eq!(Some(Error::InvalidAccount), res.1);
    // Partial update is allowed, in case an error is encountered.
    assert!([&a0].as_set().is_superset(&res.0.as_set()));
    // Partial update should match the state.
    assert_eq!(res.0.as_set(), cache.dump().as_set());
}

#[tokio::test]
async fn invalid_signature() {
    let mut rng = make_rng(2947294234);
    let rng = &mut rng;
    let clock = time::FakeClock::default();
    let now = clock.now_utc();

    let accounts: Vec<_> = (0..3).map(|_| data::make_account_id(rng)).collect();
    let mut signers = Signers::default();
    let e = signers.make_epoch(rng, &[&accounts[0], &accounts[1], &accounts[2]]);

    let cache = Arc::new(Cache::new());
    cache.set_epochs(vec![&e]);
    let a0 = signers.make_account_data(rng, &e.id, &accounts[0], now);
    let mut a1 = signers.make_account_data(rng, &e.id, &accounts[1], now);
    let mut a2_invalid_sig = signers.make_account_data(rng, &e.id, &accounts[2], now);
    *a2_invalid_sig.signature_mut() = a1.signature_mut().clone();

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
    assert!([&a0, &a1].as_set().is_superset(&res.0.as_set()));
    // Partial update should match the state.
    assert_eq!(res.0.as_set(), cache.dump().as_set());
}

#[tokio::test]
async fn single_account_multiple_data() {
    let mut rng = make_rng(2947294234);
    let rng = &mut rng;
    let clock = time::FakeClock::default();
    let now = clock.now_utc();

    let accounts: Vec<_> = (0..3).map(|_| data::make_account_id(rng)).collect();
    let mut signers = Signers::default();
    let e = signers.make_epoch(rng, &[&accounts[0], &accounts[1], &accounts[2]]);

    let cache = Arc::new(Cache::new());
    cache.set_epochs(vec![&e]);
    let a0 = signers.make_account_data(rng, &e.id, &accounts[0], now);
    let a1 = signers.make_account_data(rng, &e.id, &accounts[1], now);
    let a2old = signers.make_account_data(rng, &e.id, &accounts[2], now);
    let a2new =
        signers.make_account_data(rng, &e.id, &accounts[2], now + time::Duration::seconds(1));

    // 2 entries for the same (epoch_id,account_id) => SingleAccountMultipleData
    let res =
        cache.clone().insert(vec![a0.clone(), a1.clone(), a2old.clone(), a2new.clone()]).await;
    assert_eq!(Some(Error::SingleAccountMultipleData), res.1);
    // Partial update is allowed, in case an error is encountered.
    assert!([&a0, &a1, &a2old, &a2new].as_set().is_superset(&res.0.as_set()));
    // Partial update should match the state, this also verifies that only 1 of the competing
    // entries has been applied.
    assert_eq!(res.0.as_set(), cache.dump().as_set());
}
