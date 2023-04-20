use crate::network_protocol::testonly as data;
use crate::network_protocol::PeerIdOrHash;
use crate::routing;
use crate::routing::routing_table_view::*;
use crate::test_utils::{random_epoch_id, random_peer_id};
use crate::testonly::make_rng;
use near_async::time;
use near_crypto::Signature;
use near_primitives::network::AnnounceAccount;
use rand::seq::SliceRandom;
use std::sync::Arc;

#[test]
fn find_route() {
    let mut rng = make_rng(385305732);
    let clock = time::FakeClock::default();
    let rng = &mut rng;
    let store = crate::store::Store::from(near_store::db::TestDB::new());

    // Create a sample NextHopTable.
    let peers: Vec<_> = (0..10).map(|_| data::make_peer_id(rng)).collect();
    let mut next_hops = routing::NextHopTable::new();
    for p in &peers {
        next_hops.insert(p.clone(), (0..3).map(|_| peers.choose(rng).cloned().unwrap()).collect());
    }
    let next_hops = Arc::new(next_hops);

    // Check that RoutingTableView always selects a valid next hop.
    let rtv = RoutingTableView::new(store);
    rtv.update(next_hops.clone());
    for _ in 0..1000 {
        let p = peers.choose(rng).unwrap();
        let got = rtv.find_route(&clock.clock(), &PeerIdOrHash::PeerId(p.clone())).unwrap();
        assert!(next_hops.get(p).unwrap().contains(&got));
    }
}

#[test]
fn announcement_same_epoch() {
    let store = crate::store::Store::from(near_store::db::TestDB::new());

    let peer_id0 = random_peer_id();
    let peer_id1 = random_peer_id();
    let epoch_id0 = random_epoch_id();

    let routing_table = RoutingTableView::new(store);

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
        routing_table.add_accounts(vec![announce0.clone(), announce1.clone()]),
        vec![announce0.clone()]
    );
    assert_eq!(routing_table.get_announce_accounts(), vec![announce0.clone()]);
    assert_eq!(routing_table.account_owner(&announce0.account_id).unwrap(), peer_id0);

    // Adding a conflicting announcement later. Should be a noop.
    assert_eq!(routing_table.add_accounts(vec![announce1]), vec![]);
    assert_eq!(routing_table.get_announce_accounts(), vec![announce0.clone()]);
    assert_eq!(routing_table.account_owner(&announce0.account_id).unwrap(), peer_id0);
}

#[test]
fn dont_load_on_build() {
    let store = crate::store::Store::from(near_store::db::TestDB::new());

    let peer_id0 = random_peer_id();
    let peer_id1 = random_peer_id();
    let epoch_id0 = random_epoch_id();
    let epoch_id1 = random_epoch_id();

    let routing_table = RoutingTableView::new(store.clone());

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

    let routing_table1 = RoutingTableView::new(store);
    assert_eq!(routing_table1.get_announce_accounts().len(), 0);
}

#[test]
fn load_from_disk() {
    let store = crate::store::Store::from(near_store::db::TestDB::new());

    let peer_id0 = random_peer_id();
    let epoch_id0 = random_epoch_id();

    let routing_table = RoutingTableView::new(store.clone());
    let routing_table1 = RoutingTableView::new(store);

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
