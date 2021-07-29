use near_crypto::Signature;
use near_network::routing::RoutingTable;
use near_network::test_utils::{random_epoch_id, random_peer_id};
use near_primitives::network::AnnounceAccount;
use near_store::test_utils::create_test_store;

#[test]
fn announcement_same_epoch() {
    let store = create_test_store();

    let peer_id0 = random_peer_id();
    let peer_id1 = random_peer_id();
    let epoch_id0 = random_epoch_id();

    let mut routing_table = RoutingTable::new(peer_id0.clone(), store);

    let announce0 = AnnounceAccount {
        account_id: "near0".to_string(),
        peer_id: peer_id0.clone(),
        epoch_id: epoch_id0.clone(),
        signature: Signature::default(),
    };

    // Same as announce1 but with different peer id
    let announce1 = AnnounceAccount {
        account_id: "near0".to_string(),
        peer_id: peer_id1.clone(),
        epoch_id: epoch_id0,
        signature: Signature::default(),
    };

    routing_table.add_account(announce0.clone());
    assert!(routing_table.contains_account(&announce0));
    assert!(routing_table.contains_account(&announce1));
    assert_eq!(routing_table.get_announce_accounts().len(), 1);
    assert_eq!(routing_table.account_owner(&announce0.account_id).unwrap(), peer_id0);
    routing_table.add_account(announce1.clone());
    assert_eq!(routing_table.get_announce_accounts().len(), 1);
    assert_eq!(routing_table.account_owner(&announce1.account_id).unwrap(), peer_id1);
}

#[test]
fn dont_load_on_build() {
    let store = create_test_store();

    let peer_id0 = random_peer_id();
    let peer_id1 = random_peer_id();
    let epoch_id0 = random_epoch_id();
    let epoch_id1 = random_epoch_id();

    let mut routing_table = RoutingTable::new(peer_id0.clone(), store.clone());

    let announce0 = AnnounceAccount {
        account_id: "near0".to_string(),
        peer_id: peer_id0.clone(),
        epoch_id: epoch_id0.clone(),
        signature: Signature::default(),
    };

    // Same as announce1 but with different peer id
    let announce1 = AnnounceAccount {
        account_id: "near1".to_string(),
        peer_id: peer_id1,
        epoch_id: epoch_id1,
        signature: Signature::default(),
    };

    routing_table.add_account(announce0.clone());
    routing_table.add_account(announce1.clone());
    let accounts = routing_table.get_announce_accounts();
    assert!(vec![announce0.clone(), announce1.clone()]
        .iter()
        .all(|announce| { accounts.contains(announce) }));
    assert_eq!(routing_table.get_announce_accounts().len(), 2);

    let mut routing_table1 = RoutingTable::new(peer_id0, store);
    assert!(routing_table1.get_announce_accounts().is_empty());
}

#[test]
fn load_from_disk() {
    let store = create_test_store();

    let peer_id0 = random_peer_id();
    let epoch_id0 = random_epoch_id();

    let mut routing_table = RoutingTable::new(peer_id0.clone(), store.clone());
    let mut routing_table1 = RoutingTable::new(peer_id0.clone(), store.clone());

    let announce0 = AnnounceAccount {
        account_id: "near0".to_string(),
        peer_id: peer_id0.clone(),
        epoch_id: epoch_id0.clone(),
        signature: Signature::default(),
    };

    // Announcement is added to cache of the first routing table and to disk
    routing_table.add_account(announce0.clone());
    assert_eq!(routing_table.get_announce_accounts().len(), 1);
    // Cache of second routing table is empty
    assert_eq!(routing_table1.get_announce_accounts().len(), 0);
    // Try to find this peer and load it from disk
    assert_eq!(routing_table1.account_owner(&announce0.account_id).unwrap(), peer_id0);
    // Cache of second routing table should contain account loaded from disk
    assert_eq!(routing_table1.get_announce_accounts().len(), 1);
}
