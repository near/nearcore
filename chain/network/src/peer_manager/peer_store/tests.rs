use super::*;
use crate::blacklist::Blacklist;
use crate::time;
use near_crypto::{KeyType, SecretKey};
use near_store::{Mode, NodeStorage, StoreOpener};
use std::collections::HashSet;
use std::net::{Ipv4Addr, SocketAddrV4};

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

fn make_config(
    boot_nodes: &[PeerInfo],
    blacklist: blacklist::Blacklist,
    connect_only_to_boot_nodes: bool,
) -> Config {
    Config {
        boot_nodes: boot_nodes.iter().cloned().collect(),
        blacklist,
        connect_only_to_boot_nodes,
        ban_window: time::Duration::seconds(1),
        peer_expiration_duration: time::Duration::days(1000),
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
        let peer_store = PeerStore::new(
            &clock.clock(),
            make_config(&boot_nodes, Blacklist::default(), false),
            store,
        )
        .unwrap();
        assert_eq!(peer_store.healthy_peers(3).len(), 2);
        peer_store.peer_ban(&clock.clock(), &peer_info_to_ban.id, ReasonForBan::Abusive).unwrap();
        assert_eq!(peer_store.healthy_peers(3).len(), 1);
    }
    {
        let store_new = store::Store::from(opener.open().unwrap());
        let peer_store_new = PeerStore::new(
            &clock.clock(),
            make_config(&boot_nodes, Blacklist::default(), false),
            store_new,
        )
        .unwrap();
        assert_eq!(peer_store_new.healthy_peers(3).len(), 1);
    }
}

#[test]
fn test_unconnected_peer() {
    let clock = time::FakeClock::default();
    let peer_info_a = gen_peer_info(0);
    let peer_info_to_ban = gen_peer_info(1);
    let boot_nodes = vec![peer_info_a, peer_info_to_ban];
    {
        let store = store::Store::from(near_store::db::TestDB::new());
        let peer_store = PeerStore::new(
            &clock.clock(),
            make_config(&boot_nodes, Blacklist::default(), false),
            store,
        )
        .unwrap();
        assert!(peer_store.unconnected_peer(|_| false, false).is_some());
        assert!(peer_store.unconnected_peer(|_| true, false).is_none());
    }
}

#[test]
fn test_unknown_vs_not_connected() {
    use KnownPeerStatus::{Connected, NotConnected, Unknown};
    let clock = time::FakeClock::default();
    let peer_info_a = gen_peer_info(0);
    let peer_info_b = gen_peer_info(1);
    let (_tmp_dir, opener) = NodeStorage::test_opener();
    let peer_info_boot_node = gen_peer_info(2);
    let boot_nodes = vec![peer_info_boot_node.clone()];

    let nodes = [&peer_info_a, &peer_info_b, &peer_info_boot_node];

    let get_in_memory_status = |peer_store: &PeerStore| {
        nodes.map(|peer| peer_store.get_peer_state(&peer.id).map(|known_state| known_state.status))
    };

    let get_database_status = || {
        let store = crate::store::Store::from(opener.open_in_mode(Mode::ReadOnly).unwrap());
        let peers_state: HashMap<PeerId, KnownPeerState> =
            store.list_peer_states().unwrap().into_iter().map(|x| (x.0, x.1)).collect();
        nodes.map(|peer| peers_state.get(&peer.id).map(|known_state| known_state.status.clone()))
    };

    {
        let store = store::Store::from(opener.open().unwrap());
        let peer_store = PeerStore::new(
            &clock.clock(),
            make_config(&boot_nodes, Blacklist::default(), false),
            store,
        )
        .unwrap();

        // Check the status of the in-memory store.
        // Boot node should be marked as not-connected, as we've verified it.
        // TODO(mm-near) - the boot node should have been added as 'NotConnected' and not Unknown.
        assert_eq!(get_in_memory_status(&peer_store), [None, None, Some(Unknown)]);

        // Add the remaining peers.
        peer_store.add_direct_peer(&clock.clock(), peer_info_a.clone()).unwrap();
        peer_store.add_direct_peer(&clock.clock(), peer_info_b.clone()).unwrap();

        // Check the state in a database.
        // Seems that boot node is not added to the database when 'new' is called.
        assert_eq!(get_database_status(), [Some(Unknown), Some(Unknown), None]);

        assert_eq!(
            get_in_memory_status(&peer_store),
            [Some(Unknown), Some(Unknown), Some(Unknown)]
        );

        // Connect to both nodes
        for peer_info in [peer_info_a.clone(), peer_info_b.clone()] {
            peer_store.peer_connected(&clock.clock(), &peer_info).unwrap();
        }
        assert_eq!(
            get_in_memory_status(&peer_store),
            [Some(Connected), Some(Connected), Some(Unknown)]
        );
        assert_eq!(get_database_status(), [Some(Connected), Some(Connected), None]);

        // Disconnect from 'b'
        peer_store.peer_disconnected(&clock.clock(), &peer_info_b.id).unwrap();

        assert_eq!(
            get_in_memory_status(&peer_store),
            [Some(Connected), Some(NotConnected), Some(Unknown)]
        );
        assert_eq!(get_database_status(), [Some(Connected), Some(NotConnected), None]);

        // if we prefer 'previously connected' peers - we should keep picking 'b'.
        assert_eq!(
            (0..10)
                .map(|_| peer_store.unconnected_peer(|_| false, true).unwrap().id)
                .collect::<HashSet<PeerId>>(),
            [peer_info_b.id.clone()].into_iter().collect()
        );

        // if we don't care, we should pick either 'b' or 'boot'.
        assert_eq!(
            (0..100)
                .map(|_| peer_store.unconnected_peer(|_| false, false).unwrap().id)
                .collect::<HashSet<PeerId>>(),
            [peer_info_b.id.clone(), peer_info_boot_node.id.clone()].into_iter().collect()
        );

        // And fail when trying to reconnect to b.
        peer_store
            .peer_connection_attempt(
                &clock.clock(),
                &peer_info_b.id,
                Err(anyhow::anyhow!("b failed to connect error")),
            )
            .unwrap();

        // It should move 'back' into Unknown state.
        assert_eq!(
            get_in_memory_status(&peer_store),
            [Some(Connected), Some(Unknown), Some(Unknown)]
        );
        assert_eq!(get_database_status(), [Some(Connected), Some(Unknown), None]);
    }

    {
        // Let's reset the store.
        let store = store::Store::from(opener.open().unwrap());
        let peer_store = PeerStore::new(
            &clock.clock(),
            make_config(&boot_nodes, Blacklist::default(), false),
            store,
        )
        .unwrap();
        assert_eq!(
            get_in_memory_status(&peer_store),
            [Some(NotConnected), Some(Unknown), Some(Unknown)]
        );
        assert_eq!(get_database_status(), [Some(Connected), Some(Unknown), None]);
        // After restart - we should try to connect to 'a' (if we prefer previously connected nodes).
        assert_eq!(
            (0..10)
                .map(|_| peer_store.unconnected_peer(|_| false, true).unwrap().id)
                .collect::<HashSet<PeerId>>(),
            [peer_info_a.id.clone()].into_iter().collect()
        );
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
        let store = store::Store::from(near_store::db::TestDB::new());
        let peer_store = PeerStore::new(
            &clock.clock(),
            make_config(&boot_nodes, Blacklist::default(), false),
            store,
        )
        .unwrap();
        peer_store.add_direct_peer(&clock.clock(), peer_in_store.clone()).unwrap();
        peer_store.peer_connected(&clock.clock(), &peer_info_a).unwrap();
        assert_eq!(peer_store.unconnected_peer(|_| false, false), Some(peer_in_store.clone()));
    }

    // 1 boot node (peer_info_a) that we're already connected to.
    // 1 non-boot (peer_in_store) node peer that is in the store.
    // connect to only boot nodes is enabled - we should not find any peer to connect to.
    {
        let store = store::Store::from(near_store::db::TestDB::new());
        let peer_store = PeerStore::new(
            &clock.clock(),
            make_config(&boot_nodes, Default::default(), true),
            store,
        )
        .unwrap();
        peer_store.add_direct_peer(&clock.clock(), peer_in_store).unwrap();
        peer_store.peer_connected(&clock.clock(), &peer_info_a).unwrap();
        assert_eq!(peer_store.unconnected_peer(|_| false, false), None);
    }

    // 1 boot node (peer_info_a) is in the store.
    // we should connect to it - no matter what the setting is.
    for connect_to_boot_nodes in [true, false] {
        let store = store::Store::from(near_store::db::TestDB::new());
        let peer_store = PeerStore::new(
            &clock.clock(),
            make_config(&boot_nodes, Default::default(), connect_to_boot_nodes),
            store,
        )
        .unwrap();
        peer_store.add_direct_peer(&clock.clock(), peer_info_a.clone()).unwrap();
        assert_eq!(peer_store.unconnected_peer(|_| false, false), Some(peer_info_a.clone()));
    }
}

fn check_exist(
    peer_store: &PeerStore,
    peer_id: &PeerId,
    addr_level: Option<(SocketAddr, TrustLevel)>,
) -> bool {
    let inner = peer_store.0.lock();
    if let Some(peer_info) = inner.peer_states.get(peer_id) {
        let peer_info = &peer_info.peer_info;
        if let Some((addr, level)) = addr_level {
            peer_info.addr.map_or(false, |cur_addr| cur_addr == addr)
                && inner
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
    let inner = peer_store.0.lock();
    inner.peer_states.clone().iter().all(|(k, v)| {
        if let Some(addr) = v.peer_info.addr {
            if inner.addr_peers.get(&addr).map_or(true, |value| value.peer_id != *k) {
                return false;
            }
        }
        true
    }) && inner.addr_peers.clone().iter().all(|(k, v)| {
        !inner
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
    let store = store::Store::from(near_store::db::TestDB::new());
    let peer_store =
        PeerStore::new(&clock.clock(), make_config(&[], Default::default(), false), store).unwrap();

    let peers_id = (0..2).map(|ix| get_peer_id(format!("node{}", ix))).collect::<Vec<_>>();
    let addr = get_addr(0);

    let peer_aa = get_peer_info(peers_id[0].clone(), Some(addr));
    peer_store.peer_connected(&clock.clock(), &peer_aa).unwrap();
    assert!(check_exist(&peer_store, &peers_id[0], Some((addr, TrustLevel::Signed))));

    let peer_ba = get_peer_info(peers_id[1].clone(), Some(addr));
    peer_store.add_direct_peer(&clock.clock(), peer_ba).unwrap();

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
    let store = store::Store::from(near_store::db::TestDB::new());
    let peer_store =
        PeerStore::new(&clock.clock(), make_config(&[], Default::default(), false), store).unwrap();

    let peers_id = (0..1).map(|ix| get_peer_id(format!("node{}", ix))).collect::<Vec<_>>();
    let addrs = (0..2).map(get_addr).collect::<Vec<_>>();

    let peer_aa = get_peer_info(peers_id[0].clone(), Some(addrs[0]));
    peer_store.peer_connected(&clock.clock(), &peer_aa).unwrap();
    assert!(check_exist(&peer_store, &peers_id[0], Some((addrs[0], TrustLevel::Signed))));

    let peer_ba = get_peer_info(peers_id[0].clone(), Some(addrs[1]));
    peer_store.add_direct_peer(&clock.clock(), peer_ba).unwrap();
    assert!(check_exist(&peer_store, &peers_id[0], Some((addrs[0], TrustLevel::Signed))));
    assert!(check_integrity(&peer_store));
}

#[test]
fn check_add_peers_overriding() {
    let clock = time::FakeClock::default();
    let store = store::Store::from(near_store::db::TestDB::new());
    let peer_store =
        PeerStore::new(&clock.clock(), make_config(&[], Default::default(), false), store.clone())
            .unwrap();

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
    peer_store.add_direct_peer(&clock.clock(), peer_11.clone()).unwrap();
    assert!(check_exist(&peer_store, &peers_id[1], Some((addrs[1], TrustLevel::Direct))));
    assert!(check_integrity(&peer_store));

    // Create signed connection B - #B
    peer_store.peer_connected(&clock.clock(), &peer_11).unwrap();
    assert!(check_exist(&peer_store, &peers_id[1], Some((addrs[1], TrustLevel::Signed))));
    assert!(check_integrity(&peer_store));

    // Create indirect connection C - #C
    let peer_22 = get_peer_info(peers_id[2].clone(), Some(addrs[2]));
    peer_store.add_indirect_peers(&clock.clock(), [peer_22.clone()].into_iter()).unwrap();
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
    peer_store.add_indirect_peers(&clock.clock(), [peer_33].into_iter()).unwrap();
    assert!(check_exist(&peer_store, &peers_id[3], Some((addrs[3], TrustLevel::Indirect))));
    assert!(check_integrity(&peer_store));

    // Try to create indirect connection A - #X but fails since A - #A exists
    let peer_04 = get_peer_info(peers_id[0].clone(), Some(addrs[4]));
    peer_store.add_indirect_peers(&clock.clock(), [peer_04].into_iter()).unwrap();
    assert!(check_exist(&peer_store, &peers_id[0], Some((addrs[0], TrustLevel::Signed))));
    assert!(check_integrity(&peer_store));

    // Try to create indirect connection X - #D but fails since D - #D exists
    let peer_43 = get_peer_info(peers_id[4].clone(), Some(addrs[3]));
    peer_store.add_indirect_peers(&clock.clock(), [peer_43.clone()].into_iter()).unwrap();
    assert!(check_exist(&peer_store, &peers_id[3], Some((addrs[3], TrustLevel::Indirect))));
    assert!(check_integrity(&peer_store));

    // Create Direct connection X - #D and succeed removing connection D - #D
    peer_store.add_direct_peer(&clock.clock(), peer_43).unwrap();
    assert!(check_exist(&peer_store, &peers_id[4], Some((addrs[3], TrustLevel::Direct))));
    // D should still exist, but without any addr
    assert!(check_exist(&peer_store, &peers_id[3], None));
    assert!(check_integrity(&peer_store));

    // Try to create indirect connection A - #T but fails since A - #A (signed) exists
    let peer_05 = get_peer_info(peers_id[0].clone(), Some(addrs[5]));
    peer_store.add_direct_peer(&clock.clock(), peer_05).unwrap();
    assert!(check_exist(&peer_store, &peers_id[0], Some((addrs[0], TrustLevel::Signed))));
    assert!(check_integrity(&peer_store));

    // Check we are able to recover from store previous signed connection
    let peer_store_2 =
        PeerStore::new(&clock.clock(), make_config(&[], Default::default(), false), store).unwrap();
    assert!(check_exist(&peer_store_2, &peers_id[0], Some((addrs[0], TrustLevel::Indirect))));
    assert!(check_integrity(&peer_store_2));
}

#[test]
fn check_ignore_blacklisted_peers() {
    let clock = time::FakeClock::default();

    #[track_caller]
    fn assert_peers(peer_store: &PeerStore, expected: &[&PeerId]) {
        let inner = peer_store.0.lock();
        let expected: HashSet<&PeerId> = HashSet::from_iter(expected.iter().cloned());
        let got = HashSet::from_iter(inner.peer_states.keys());
        assert_eq!(expected, got);
    }

    let ids = (0..6).map(|ix| get_peer_id(format!("node{}", ix))).collect::<Vec<_>>();
    let store = store::Store::from(near_store::db::TestDB::new());

    // Populate store with three peers.
    {
        let peer_store = PeerStore::new(
            &clock.clock(),
            make_config(&[], Default::default(), false),
            store.clone(),
        )
        .unwrap();
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
        let peer_store = PeerStore::new(
            &clock.clock(),
            make_config(&[], Default::default(), false),
            store.clone(),
        )
        .unwrap();
        assert_peers(&peer_store, &[&ids[1], &ids[2]]);
    }

    // Blacklist one of the existing peers and one new peer.
    {
        let blacklist: blacklist::Blacklist =
            ["127.0.0.1:2", "127.0.0.1:5"].iter().map(|e| e.parse().unwrap()).collect();
        let peer_store =
            PeerStore::new(&clock.clock(), make_config(&[], blacklist, false), store).unwrap();
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
        let peer_store =
            PeerStore::new(&clock.clock(), make_config(&[], Default::default(), false), store)
                .unwrap();
        peer_store.add_indirect_peers(&clock.clock(), peer_infos.clone().into_iter()).unwrap();
    }
    assert_peers_in_store(&opener, &peer_ids);

    // Blacklisted peers are removed from the store.
    {
        let store = store::Store::from(opener.open().unwrap());
        let blacklist: blacklist::Blacklist =
            [blacklist::Entry::from_addr(peer_infos[2].addr.unwrap())].into_iter().collect();
        let _peer_store =
            PeerStore::new(&clock.clock(), make_config(&[], blacklist, false), store).unwrap();
    }
    assert_peers_in_store(&opener, &peer_ids[0..2]);
}

#[track_caller]
fn assert_peers_in_store(opener: &StoreOpener, want: &[PeerId]) {
    let store = crate::store::Store::from(opener.open().unwrap());
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
    let inner = peer_store.0.lock();
    let expected_peers: HashSet<&PeerId> = HashSet::from_iter(expected_peers);
    let cached_peers = HashSet::from_iter(inner.peer_states.keys());
    assert_eq!(expected_peers, cached_peers);

    let expected_addresses: HashSet<&SocketAddr> = HashSet::from_iter(expected_addresses);
    let cached_addresses = HashSet::from_iter(inner.addr_peers.keys());
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
    let peer_addresses = peer_infos.iter().map(|info| info.addr.unwrap()).collect::<Vec<_>>();

    {
        let store = store::Store::from(opener.open().unwrap());
        let peer_store =
            PeerStore::new(&clock.clock(), make_config(&[], Default::default(), false), store)
                .unwrap();
        peer_store.add_indirect_peers(&clock.clock(), peer_infos.into_iter()).unwrap();
    }
    assert_peers_in_store(&opener, &peer_ids);

    {
        let store = store::Store::from(opener.open().unwrap());
        let peer_store =
            PeerStore::new(&clock.clock(), make_config(&[], Default::default(), false), store)
                .unwrap();
        assert_peers_in_cache(&peer_store, &peer_ids, &peer_addresses);
        peer_store.0.lock().delete_peers(&peer_ids).unwrap();
        assert_peers_in_cache(&peer_store, &[], &[]);
    }
    assert_peers_in_store(&opener, &[]);
}
