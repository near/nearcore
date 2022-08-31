use near_crypto::{KeyType, SecretKey};
use near_network_primitives::types::{Blacklist, BlacklistEntry};
use near_store::{NodeStorage, StoreOpener};
use std::collections::HashSet;
use std::net::{Ipv4Addr, SocketAddrV4};

use crate::test_utils::create_test_peer_store;

use super::*;

fn get_peer_id(seed: String) -> PeerId {
    PeerId::new(SecretKey::from_seed(KeyType::ED25519, seed.as_str()).public_key())
}

fn get_addr(port: u16) -> SocketAddr {
    SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), port).into()
}

fn get_peer_info(peer_id: PeerId, addr: Option<SocketAddr>) -> PeerInfo {
    PeerInfo { id: peer_id, addr, account_id: None }
}

fn gen_peer_info(port: u16) -> PeerInfo {
    PeerInfo {
        id: PeerId::new(SecretKey::from_random(KeyType::ED25519).public_key()),
        addr: Some(get_addr(port)),
        account_id: None,
    }
}

#[test]
fn ban_store() {
    let clock = time::FakeClock::default();
    let (_tmp_dir, opener) = NodeStorage::test_opener();
    let peer_info_a = gen_peer_info(0);
    let peer_info_to_ban = gen_peer_info(1);
    let boot_nodes = vec![peer_info_a, peer_info_to_ban.clone()];
    {
        let store = store::Store::from(opener.open().unwrap());
        let mut peer_store =
            PeerStore::new(&clock.clock(), store, &boot_nodes, Default::default(), false).unwrap();
        assert_eq!(peer_store.healthy_peers(3).len(), 2);
        peer_store.peer_ban(&clock.clock(), &peer_info_to_ban.id, ReasonForBan::Abusive).unwrap();
        assert_eq!(peer_store.healthy_peers(3).len(), 1);
    }
    {
        let store_new = store::Store::from(opener.open().unwrap());
        let peer_store_new =
            PeerStore::new(&clock.clock(), store_new, &boot_nodes, Default::default(), false)
                .unwrap();
        assert_eq!(peer_store_new.healthy_peers(3).len(), 1);
    }
}

#[test]
fn test_unconnected_peer() {
    let clock = time::FakeClock::default();
    let (_tmp_dir, opener) = NodeStorage::test_opener();
    let peer_info_a = gen_peer_info(0);
    let peer_info_to_ban = gen_peer_info(1);
    let boot_nodes = vec![peer_info_a, peer_info_to_ban];
    {
        let store = store::Store::from(opener.open().unwrap());
        let peer_store =
            PeerStore::new(&clock.clock(), store, &boot_nodes, Default::default(), false).unwrap();
        assert!(peer_store.unconnected_peer(|_| false).is_some());
        assert!(peer_store.unconnected_peer(|_| true).is_none());
    }
}

#[test]
fn test_unconnected_peer_only_boot_nodes() {
    let clock = time::FakeClock::default();
    let peer_info_a = gen_peer_info(0);
    let peer_in_store = gen_peer_info(3);
    let boot_nodes = vec![peer_info_a.clone()];

    // 1 boot node (peer_info_a) that we're already connected to.
    // 1 non-boot (peer_in_store) node peer that is in the store.
    // we should connect to peer_in_store
    {
        let (_tmp_dir, opener) = NodeStorage::test_opener();
        let store = store::Store::from(opener.open().unwrap());
        let mut peer_store =
            PeerStore::new(&clock.clock(), store, &boot_nodes, Default::default(), false).unwrap();
        peer_store.add_peer(&clock.clock(), peer_in_store.clone(), TrustLevel::Direct).unwrap();
        peer_store.peer_connected(&clock.clock(), &peer_info_a).unwrap();
        assert_eq!(peer_store.unconnected_peer(|_| false), Some(peer_in_store.clone()));
    }

    // 1 boot node (peer_info_a) that we're already connected to.
    // 1 non-boot (peer_in_store) node peer that is in the store.
    // connect to only boot nodes is enabled - we should not find any peer to connect to.
    {
        let (_tmp_dir, opener) = NodeStorage::test_opener();
        let store = store::Store::from(opener.open().unwrap());
        let mut peer_store =
            PeerStore::new(&clock.clock(), store, &boot_nodes, Default::default(), true).unwrap();
        peer_store.add_peer(&clock.clock(), peer_in_store.clone(), TrustLevel::Direct).unwrap();
        peer_store.peer_connected(&clock.clock(), &peer_info_a).unwrap();
        assert_eq!(peer_store.unconnected_peer(|_| false), None);
    }

    // 1 boot node (peer_info_a) is in the store.
    // we should connect to it - no matter what the setting is.
    for connect_to_boot_nodes in [true, false] {
        let (_tmp_dir, opener) = NodeStorage::test_opener();
        let store = store::Store::from(opener.open().unwrap());
        let mut peer_store = PeerStore::new(
            &clock.clock(),
            store,
            &boot_nodes,
            Default::default(),
            connect_to_boot_nodes,
        )
        .unwrap();
        peer_store.add_peer(&clock.clock(), peer_info_a.clone(), TrustLevel::Direct).unwrap();
        assert_eq!(peer_store.unconnected_peer(|_| false), Some(peer_info_a.clone()));
    }
}

fn check_exist(
    peer_store: &PeerStore,
    peer_id: &PeerId,
    addr_level: Option<(SocketAddr, TrustLevel)>,
) -> bool {
    if let Some(peer_info) = peer_store.peer_states.get(peer_id) {
        let peer_info = &peer_info.peer_info;
        if let Some((addr, level)) = addr_level {
            peer_info.addr.map_or(false, |cur_addr| cur_addr == addr)
                && peer_store
                    .addr_peers
                    .get(&addr)
                    .map_or(false, |verified| verified.trust_level == level)
        } else {
            peer_info.addr.is_none()
        }
    } else {
        false
    }
}

fn check_integrity(peer_store: &PeerStore) -> bool {
    peer_store.peer_states.clone().iter().all(|(k, v)| {
        if let Some(addr) = v.peer_info.addr {
            if peer_store.addr_peers.get(&addr).map_or(true, |value| value.peer_id != *k) {
                return false;
            }
        }
        true
    }) && peer_store.addr_peers.clone().iter().all(|(k, v)| {
        !peer_store
            .peer_states
            .get(&v.peer_id)
            .map_or(true, |value| value.peer_info.addr.map_or(true, |addr| addr != *k))
    })
}

/// If we know there is a peer_id A at address #A, and after some time
/// we learn that there is a new peer B at address #A, we discard address of A
#[test]
fn handle_peer_id_change() {
    let clock = time::FakeClock::default();
    let store = create_test_peer_store();
    let mut peer_store =
        PeerStore::new(&clock.clock(), store, &[], Default::default(), false).unwrap();

    let peers_id = (0..2).map(|ix| get_peer_id(format!("node{}", ix))).collect::<Vec<_>>();
    let addr = get_addr(0);

    let peer_aa = get_peer_info(peers_id[0].clone(), Some(addr));
    peer_store.peer_connected(&clock.clock(), &peer_aa).unwrap();
    assert!(check_exist(&peer_store, &peers_id[0], Some((addr, TrustLevel::Signed))));

    let peer_ba = get_peer_info(peers_id[1].clone(), Some(addr));
    peer_store.add_peer(&clock.clock(), peer_ba, TrustLevel::Direct).unwrap();

    assert!(check_exist(&peer_store, &peers_id[0], None));
    assert!(check_exist(&peer_store, &peers_id[1], Some((addr, TrustLevel::Direct))));
    assert!(check_integrity(&peer_store));
}

/// If we know there is a peer_id A at address #A, and then we learn about
/// the same peer_id A at address #B, if that connection wasn't signed it is not updated,
/// to avoid malicious actor making us forget about known peers.
#[test]
fn dont_handle_address_change() {
    let clock = time::FakeClock::default();
    let store = create_test_peer_store();
    let mut peer_store =
        PeerStore::new(&clock.clock(), store, &[], Default::default(), false).unwrap();

    let peers_id = (0..1).map(|ix| get_peer_id(format!("node{}", ix))).collect::<Vec<_>>();
    let addrs = (0..2).map(get_addr).collect::<Vec<_>>();

    let peer_aa = get_peer_info(peers_id[0].clone(), Some(addrs[0]));
    peer_store.peer_connected(&clock.clock(), &peer_aa).unwrap();
    assert!(check_exist(&peer_store, &peers_id[0], Some((addrs[0], TrustLevel::Signed))));

    let peer_ba = get_peer_info(peers_id[0].clone(), Some(addrs[1]));
    peer_store.add_peer(&clock.clock(), peer_ba, TrustLevel::Direct).unwrap();
    assert!(check_exist(&peer_store, &peers_id[0], Some((addrs[0], TrustLevel::Signed))));
    assert!(check_integrity(&peer_store));
}

#[test]
fn check_add_peers_overriding() {
    let clock = time::FakeClock::default();
    let store = create_test_peer_store();
    let mut peer_store =
        PeerStore::new(&clock.clock(), store.clone(), &[], Default::default(), false).unwrap();

    // Five peers: A, B, C, D, X, T
    let peers_id = (0..6).map(|ix| get_peer_id(format!("node{}", ix))).collect::<Vec<_>>();
    // Five addresses: #A, #B, #C, #D, #X, #T
    let addrs = (0..6).map(get_addr).collect::<Vec<_>>();

    // Create signed connection A - #A
    let peer_00 = get_peer_info(peers_id[0].clone(), Some(addrs[0]));
    peer_store.peer_connected(&clock.clock(), &peer_00).unwrap();
    assert!(check_exist(&peer_store, &peers_id[0], Some((addrs[0], TrustLevel::Signed))));
    assert!(check_integrity(&peer_store));

    // Create direct connection B - #B
    let peer_11 = get_peer_info(peers_id[1].clone(), Some(addrs[1]));
    peer_store.add_peer(&clock.clock(), peer_11.clone(), TrustLevel::Direct).unwrap();
    assert!(check_exist(&peer_store, &peers_id[1], Some((addrs[1], TrustLevel::Direct))));
    assert!(check_integrity(&peer_store));

    // Create signed connection B - #B
    peer_store.peer_connected(&clock.clock(), &peer_11).unwrap();
    assert!(check_exist(&peer_store, &peers_id[1], Some((addrs[1], TrustLevel::Signed))));
    assert!(check_integrity(&peer_store));

    // Create indirect connection C - #C
    let peer_22 = get_peer_info(peers_id[2].clone(), Some(addrs[2]));
    peer_store.add_peer(&clock.clock(), peer_22.clone(), TrustLevel::Indirect).unwrap();
    assert!(check_exist(&peer_store, &peers_id[2], Some((addrs[2], TrustLevel::Indirect))));
    assert!(check_integrity(&peer_store));

    // Create signed connection C - #C
    peer_store.peer_connected(&clock.clock(), &peer_22).unwrap();
    assert!(check_exist(&peer_store, &peers_id[2], Some((addrs[2], TrustLevel::Signed))));
    assert!(check_integrity(&peer_store));

    // Create signed connection C - #B
    // This overrides C - #C and B - #B
    let peer_21 = get_peer_info(peers_id[2].clone(), Some(addrs[1]));
    peer_store.peer_connected(&clock.clock(), &peer_21).unwrap();
    assert!(check_exist(&peer_store, &peers_id[1], None));
    assert!(check_exist(&peer_store, &peers_id[2], Some((addrs[1], TrustLevel::Signed))));
    assert!(check_integrity(&peer_store));

    // Create indirect connection D - #D
    let peer_33 = get_peer_info(peers_id[3].clone(), Some(addrs[3]));
    peer_store.add_peer(&clock.clock(), peer_33, TrustLevel::Indirect).unwrap();
    assert!(check_exist(&peer_store, &peers_id[3], Some((addrs[3], TrustLevel::Indirect))));
    assert!(check_integrity(&peer_store));

    // Try to create indirect connection A - #X but fails since A - #A exists
    let peer_04 = get_peer_info(peers_id[0].clone(), Some(addrs[4]));
    peer_store.add_peer(&clock.clock(), peer_04, TrustLevel::Indirect).unwrap();
    assert!(check_exist(&peer_store, &peers_id[0], Some((addrs[0], TrustLevel::Signed))));
    assert!(check_integrity(&peer_store));

    // Try to create indirect connection X - #D but fails since D - #D exists
    let peer_43 = get_peer_info(peers_id[4].clone(), Some(addrs[3]));
    peer_store.add_peer(&clock.clock(), peer_43.clone(), TrustLevel::Indirect).unwrap();
    assert!(check_exist(&peer_store, &peers_id[3], Some((addrs[3], TrustLevel::Indirect))));
    assert!(check_integrity(&peer_store));

    // Create Direct connection X - #D and succeed removing connection D - #D
    peer_store.add_peer(&clock.clock(), peer_43, TrustLevel::Direct).unwrap();
    assert!(check_exist(&peer_store, &peers_id[4], Some((addrs[3], TrustLevel::Direct))));
    // D should still exist, but without any addr
    assert!(check_exist(&peer_store, &peers_id[3], None));
    assert!(check_integrity(&peer_store));

    // Try to create indirect connection A - #T but fails since A - #A (signed) exists
    let peer_05 = get_peer_info(peers_id[0].clone(), Some(addrs[5]));
    peer_store.add_peer(&clock.clock(), peer_05, TrustLevel::Direct).unwrap();
    assert!(check_exist(&peer_store, &peers_id[0], Some((addrs[0], TrustLevel::Signed))));
    assert!(check_integrity(&peer_store));

    // Check we are able to recover from store previous signed connection
    let peer_store_2 =
        PeerStore::new(&clock.clock(), store, &[], Default::default(), false).unwrap();
    assert!(check_exist(&peer_store_2, &peers_id[0], Some((addrs[0], TrustLevel::Indirect))));
    assert!(check_integrity(&peer_store_2));
}

#[test]
fn check_ignore_blacklisted_peers() {
    let clock = time::FakeClock::default();

    #[track_caller]
    fn assert_peers(peer_store: &PeerStore, expected: &[&PeerId]) {
        let expected: HashSet<&PeerId> = HashSet::from_iter(expected.iter().cloned());
        let got = HashSet::from_iter(peer_store.peer_states.keys());
        assert_eq!(expected, got);
    }

    let ids = (0..6).map(|ix| get_peer_id(format!("node{}", ix))).collect::<Vec<_>>();
    let store = create_test_peer_store();

    // Populate store with three peers.
    {
        let mut peer_store =
            PeerStore::new(&clock.clock(), store.clone(), &[], Default::default(), false).unwrap();
        peer_store
            .add_indirect_peers(
                &clock.clock(),
                [
                    get_peer_info(ids[0].clone(), None),
                    get_peer_info(ids[1].clone(), Some(get_addr(1))),
                    get_peer_info(ids[2].clone(), Some(get_addr(2))),
                ]
                .into_iter(),
            )
            .unwrap();
        assert_peers(&peer_store, &[&ids[0], &ids[1], &ids[2]]);
    }

    // Peers without address aren’t saved but make sure the rest are read
    // correctly.
    {
        let peer_store =
            PeerStore::new(&clock.clock(), store.clone(), &[], Default::default(), false).unwrap();
        assert_peers(&peer_store, &[&ids[1], &ids[2]]);
    }

    // Blacklist one of the existing peers and one new peer.
    {
        let blacklist: Blacklist =
            ["127.0.0.1:2", "127.0.0.1:5"].iter().map(|e| e.parse().unwrap()).collect();
        let mut peer_store = PeerStore::new(&clock.clock(), store, &[], blacklist, false).unwrap();
        // Peer 127.0.0.1:2 is removed since it's blacklisted.
        assert_peers(&peer_store, &[&ids[1]]);

        peer_store
            .add_indirect_peers(
                &clock.clock(),
                [
                    get_peer_info(ids[3].clone(), None),
                    get_peer_info(ids[4].clone(), Some(get_addr(4))),
                    get_peer_info(ids[5].clone(), Some(get_addr(5))),
                ]
                .into_iter(),
            )
            .unwrap();
        // Peer 127.0.0.1:5 is ignored and never added.
        assert_peers(&peer_store, &[&ids[1], &ids[3], &ids[4]]);
    }
}

#[test]
fn remove_blacklisted_peers_from_store() {
    let clock = time::FakeClock::default();
    let (_tmp_dir, opener) = NodeStorage::test_opener();
    let (peer_ids, peer_infos): (Vec<_>, Vec<_>) = (0..3)
        .map(|i| {
            let id = get_peer_id(format!("node{}", i));
            let info = get_peer_info(id.clone(), Some(get_addr(i)));
            (id, info)
        })
        .unzip();

    // Add three peers.
    {
        let store = store::Store::from(opener.open().unwrap());
        let mut peer_store =
            PeerStore::new(&clock.clock(), store, &[], Default::default(), false).unwrap();
        peer_store.add_indirect_peers(&clock.clock(), peer_infos.clone().into_iter()).unwrap();
    }
    assert_peers_in_store(&opener, &peer_ids);

    // Blacklisted peers are removed from the store.
    {
        let store = store::Store::from(opener.open().unwrap());
        let blacklist: Blacklist =
            [BlacklistEntry::from_addr(peer_infos[2].addr.unwrap())].into_iter().collect();
        let _peer_store = PeerStore::new(&clock.clock(), store, &[], blacklist, false).unwrap();
    }
    assert_peers_in_store(&opener, &peer_ids[0..2]);
}

#[track_caller]
fn assert_peers_in_store(opener: &StoreOpener, want: &[PeerId]) {
    let store = store::Store::from(opener.open().unwrap());
    let got: HashSet<PeerId> = store.list_peer_states().unwrap().into_iter().map(|x| x.0).collect();
    let want: HashSet<PeerId> = want.iter().cloned().collect();
    assert_eq!(got, want);
}

#[track_caller]
fn assert_peers_in_cache(
    peer_store: &PeerStore,
    expected_peers: &[PeerId],
    expected_addresses: &[SocketAddr],
) {
    let expected_peers: HashSet<&PeerId> = HashSet::from_iter(expected_peers);
    let cached_peers = HashSet::from_iter(peer_store.peer_states.keys());
    assert_eq!(expected_peers, cached_peers);

    let expected_addresses: HashSet<&SocketAddr> = HashSet::from_iter(expected_addresses);
    let cached_addresses = HashSet::from_iter(peer_store.addr_peers.keys());
    assert_eq!(expected_addresses, cached_addresses);
}

#[test]
fn test_delete_peers() {
    let clock = time::FakeClock::default();
    let (_tmp_dir, opener) = NodeStorage::test_opener();
    let (peer_ids, peer_infos): (Vec<_>, Vec<_>) = (0..3)
        .map(|i| {
            let id = get_peer_id(format!("node{}", i));
            let info = get_peer_info(id.clone(), Some(get_addr(i)));
            (id, info)
        })
        .unzip();
    let peer_addresses =
        peer_infos.iter().map(|info| info.addr.unwrap().clone()).collect::<Vec<_>>();

    {
        let store = store::Store::from(opener.open().unwrap());
        let mut peer_store =
            PeerStore::new(&clock.clock(), store, &[], Default::default(), false).unwrap();
        peer_store.add_indirect_peers(&clock.clock(), peer_infos.into_iter()).unwrap();
    }
    assert_peers_in_store(&opener, &peer_ids);

    {
        let store = store::Store::from(opener.open().unwrap());
        let mut peer_store =
            PeerStore::new(&clock.clock(), store, &[], Default::default(), false).unwrap();
        assert_peers_in_cache(&peer_store, &peer_ids, &peer_addresses);
        peer_store.delete_peers(&peer_ids).unwrap();
        assert_peers_in_cache(&peer_store, &[], &[]);
    }
    assert_peers_in_store(&opener, &[]);
}
