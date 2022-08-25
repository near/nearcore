use crate::routing::routing_table_view::RoutingTableView;
use crate::store;
use crate::test_utils::{random_epoch_id, random_peer_id};
use near_crypto::Signature;
use near_primitives::network::AnnounceAccount;
use near_store::test_utils::create_test_store;

#[test]
fn announcement_same_epoch() {
    let store = store::Store::from(create_test_store());

    let peer_id0 = random_peer_id();
    let peer_id1 = random_peer_id();
    let epoch_id0 = random_epoch_id();

    let routing_table = RoutingTableView::new(store, random_peer_id());

    let announce0 = AnnounceAccount {
        account_id: "near0".parse().unwrap(),
        peer_id: peer_id0.clone(),
        epoch_id: epoch_id0.clone(),
        signature: Signature::default(),
    };

    // Same as announce1 but with different peer id
    let announce1 = AnnounceAccount {
        account_id: "near0".parse().unwrap(),
        peer_id: peer_id1.clone(),
        epoch_id: epoch_id0,
        signature: Signature::default(),
    };

    // Adding multiple announcements for the same account_id and epoch_id.
    // The first one should win.
    assert_eq!(routing_table.add_accounts(vec![announce0.clone(),announce1.clone()]),vec![announce0.clone()]);
    assert_eq!(routing_table.get_announce_accounts(), vec![announce0.clone()]);
    assert_eq!(routing_table.account_owner(&announce0.account_id).unwrap(), peer_id0);

    // Adding a conflicting announcement later. Should be a noop.
    assert_eq!(routing_table.add_accounts(vec![announce1.clone()]),vec![]);
    assert_eq!(routing_table.get_announce_accounts(), vec![announce0.clone()]);
    assert_eq!(routing_table.account_owner(&announce0.account_id).unwrap(), peer_id0);
}

#[test]
fn dont_load_on_build() {
    let store = store::Store::from(create_test_store());

    let peer_id0 = random_peer_id();
    let peer_id1 = random_peer_id();
    let epoch_id0 = random_epoch_id();
    let epoch_id1 = random_epoch_id();

    let routing_table = RoutingTableView::new(store.clone(), random_peer_id());

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

    routing_table.add_accounts(vec![announce0.clone()]);
    routing_table.add_accounts(vec![announce1.clone()]);
    let accounts: Vec<AnnounceAccount> = routing_table.get_announce_accounts();
    assert!(vec![announce0, announce1].iter().all(|announce| { accounts.contains(&announce) }));
    assert_eq!(accounts.len(), 2);

    let routing_table1 = RoutingTableView::new(store, random_peer_id());
    assert_eq!(routing_table1.get_announce_accounts().len(), 0);
}

#[test]
fn load_from_disk() {
    let store = store::Store::from(create_test_store());

    let peer_id0 = random_peer_id();
    let epoch_id0 = random_epoch_id();

    let routing_table = RoutingTableView::new(store.clone(), random_peer_id());
    let routing_table1 = RoutingTableView::new(store, random_peer_id());

    let announce0 = AnnounceAccount {
        account_id: "near0".parse().unwrap(),
        peer_id: peer_id0.clone(),
        epoch_id: epoch_id0,
        signature: Signature::default(),
    };

    // Announcement is added to cache of the first routing table and to disk
    routing_table.add_accounts(vec![announce0.clone()]);
    assert_eq!(routing_table.get_announce_accounts().len(), 1);
    // Cache of second routing table is empty
    assert_eq!(routing_table1.get_announce_accounts().len(), 0);
    // Try to find this peer and load it from disk
    assert_eq!(routing_table1.account_owner(&announce0.account_id).unwrap(), peer_id0);
    // Cache of second routing table should contain account loaded from disk
    assert_eq!(routing_table1.get_announce_accounts().len(), 1);
}
