use crate::announce_accounts::*;
use crate::test_utils::{random_epoch_id, random_peer_id};
use near_crypto::Signature;
use near_primitives::network::AnnounceAccount;

#[test]
fn announcement_same_epoch() {
    let store = crate::store::Store::from(near_store::db::TestDB::new());

    let peer_id0 = random_peer_id();
    let peer_id1 = random_peer_id();
    let epoch_id0 = random_epoch_id();

    let announcements_cache = AnnounceAccountCache::new(store);

    let announce0 = AnnounceAccount {
        account_id: "near0".parse().unwrap(),
        peer_id: peer_id0.clone(),
        epoch_id: epoch_id0.clone(),
        signature: Signature::default(),
    };

    // Same as announce1 but with different peer id
    let announce1 = AnnounceAccount {
        account_id: "near0".parse().unwrap(),
        peer_id: peer_id1,
        epoch_id: epoch_id0,
        signature: Signature::default(),
    };

    // Adding multiple announcements for the same account_id and epoch_id.
    // The first one should win.
    assert_eq!(
        announcements_cache.add_accounts(vec![announce0.clone(), announce1.clone()]),
        vec![announce0.clone()]
    );
    assert_eq!(announcements_cache.get_announcements(), vec![announce0.clone()]);
    assert_eq!(announcements_cache.get_account_owner(&announce0.account_id).unwrap(), peer_id0);

    // Adding a conflicting announcement later. Should be a noop.
    assert_eq!(announcements_cache.add_accounts(vec![announce1]), vec![]);
    assert_eq!(announcements_cache.get_announcements(), vec![announce0.clone()]);
    assert_eq!(announcements_cache.get_account_owner(&announce0.account_id).unwrap(), peer_id0);
}

#[test]
fn dont_load_on_build() {
    let store = crate::store::Store::from(near_store::db::TestDB::new());

    let peer_id0 = random_peer_id();
    let peer_id1 = random_peer_id();
    let epoch_id0 = random_epoch_id();
    let epoch_id1 = random_epoch_id();

    let announcements_cache = AnnounceAccountCache::new(store.clone());

    let announce0 = AnnounceAccount {
        account_id: "near0".parse().unwrap(),
        peer_id: peer_id0,
        epoch_id: epoch_id0,
        signature: Signature::default(),
    };

    // Same as announce1 but with different peer id
    let announce1 = AnnounceAccount {
        account_id: "near1".parse().unwrap(),
        peer_id: peer_id1,
        epoch_id: epoch_id1,
        signature: Signature::default(),
    };

    announcements_cache.add_accounts(vec![announce0.clone()]);
    announcements_cache.add_accounts(vec![announce1.clone()]);
    let accounts: Vec<AnnounceAccount> = announcements_cache.get_announcements();
    assert!(vec![announce0, announce1].iter().all(|announce| { accounts.contains(&announce) }));
    assert_eq!(accounts.len(), 2);

    let announcements_cache1 = AnnounceAccountCache::new(store);
    assert_eq!(announcements_cache1.get_announcements().len(), 0);
}

#[test]
fn load_from_disk() {
    let store = crate::store::Store::from(near_store::db::TestDB::new());

    let peer_id0 = random_peer_id();
    let epoch_id0 = random_epoch_id();

    let announcements_cache = AnnounceAccountCache::new(store.clone());
    let announcements_cache1 = AnnounceAccountCache::new(store);

    let announce0 = AnnounceAccount {
        account_id: "near0".parse().unwrap(),
        peer_id: peer_id0.clone(),
        epoch_id: epoch_id0,
        signature: Signature::default(),
    };

    // Announcement is added to first cache and to disk
    announcements_cache.add_accounts(vec![announce0.clone()]);
    assert_eq!(announcements_cache.get_announcements().len(), 1);
    // Second cache is empty
    assert_eq!(announcements_cache1.get_announcements().len(), 0);
    // Try to find this peer and load it from disk
    assert_eq!(announcements_cache1.get_account_owner(&announce0.account_id).unwrap(), peer_id0);
    // Second cache should contain account loaded from disk
    assert_eq!(announcements_cache1.get_announcements().len(), 1);
}
