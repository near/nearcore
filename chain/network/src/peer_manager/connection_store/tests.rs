use crate::peer_manager::connection_store::ConnectionStore;
use crate::peer_manager::connection_store::OUTBOUND_CONNECTIONS_CACHE_SIZE;
use crate::store;
use crate::time;
use crate::types::{ConnectionInfo, PeerInfo};
use near_crypto::{KeyType, SecretKey};
use near_primitives::network::PeerId;
use near_store::NodeStorage;
use rand::Rng;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

fn get_addr(port: u16) -> SocketAddr {
    SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), port).into()
}

fn gen_peer_info(port: u16) -> PeerInfo {
    PeerInfo {
        id: PeerId::new(SecretKey::from_random(KeyType::ED25519).public_key()),
        addr: Some(get_addr(port)),
        account_id: None,
    }
}

fn check_outbound_connections(store: &ConnectionStore, wanted: &Vec<ConnectionInfo>) {
    let got = store.get_recent_outbound_connections();
    for c in &got {
        assert!(wanted.contains(c));
    }
    for c in wanted {
        assert!(got.contains(&*c));
    }
}

#[test]
fn test_store_connection() {
    let clock = time::FakeClock::default();
    let (_tmp_dir, opener) = NodeStorage::test_opener();

    let now_utc = clock.now_utc();

    let conn_info_a = ConnectionInfo {
        peer_info: gen_peer_info(0),
        time_established: now_utc - time::Duration::hours(1),
        time_connected_until: now_utc,
    };

    {
        let store = store::Store::from(opener.open().unwrap());
        let connection_store = ConnectionStore::new(store).unwrap();
        connection_store.insert_outbound_connections(vec![conn_info_a.clone()]);
    }
    {
        let store = store::Store::from(opener.open().unwrap());
        let connection_store = ConnectionStore::new(store).unwrap();
        assert!(connection_store.contains_outbound_connection(&conn_info_a.peer_info.id));
    }
}

#[test]
fn test_multiple_connections_to_same_peer() {
    let clock = time::FakeClock::default();
    let (_tmp_dir, opener) = NodeStorage::test_opener();
    let store = store::Store::from(opener.open().unwrap());
    let connection_store = ConnectionStore::new(store).unwrap();

    let now_utc = clock.now_utc();

    // create and store a connection
    let conn_info_a = ConnectionInfo {
        peer_info: gen_peer_info(0),
        time_established: now_utc - time::Duration::hours(1),
        time_connected_until: now_utc,
    };
    connection_store.insert_outbound_connections(vec![conn_info_a.clone()]);

    // push a second connection with the same peer but different data
    let conn_info_b = ConnectionInfo {
        peer_info: conn_info_a.peer_info,
        time_established: now_utc - time::Duration::hours(2),
        time_connected_until: now_utc + time::Duration::hours(1),
    };
    connection_store.insert_outbound_connections(vec![conn_info_b.clone()]);

    // check that precisely the second connection is present (and not the first)
    assert!(connection_store.get_recent_outbound_connections() == vec![conn_info_b.clone()]);
}

#[test]
fn test_evict_longest_disconnected() {
    let clock = time::FakeClock::default();
    let (_tmp_dir, opener) = NodeStorage::test_opener();
    let store = store::Store::from(opener.open().unwrap());
    let connection_store = ConnectionStore::new(store).unwrap();

    let now_utc = clock.now_utc();

    // create and store live connections up to the ConnectionStore limit
    let mut conn_infos = vec![];
    for i in 0..OUTBOUND_CONNECTIONS_CACHE_SIZE {
        conn_infos.push(ConnectionInfo {
            peer_info: gen_peer_info(i.try_into().unwrap()),
            time_established: now_utc - time::Duration::hours(1),
            time_connected_until: now_utc,
        });
    }
    connection_store.insert_outbound_connections(conn_infos.clone());

    // remove one of the connections, advance the clock and update the connection_store
    conn_infos.remove(rand::thread_rng().gen_range(0..OUTBOUND_CONNECTIONS_CACHE_SIZE));
    clock.advance(time::Duration::hours(1));
    let now_utc = clock.now_utc();
    for c in conn_infos.iter_mut() {
        c.time_connected_until = now_utc;
    }
    connection_store.insert_outbound_connections(conn_infos.clone());

    // insert a new connection
    let conn = ConnectionInfo {
        peer_info: gen_peer_info(OUTBOUND_CONNECTIONS_CACHE_SIZE.try_into().unwrap()),
        time_established: now_utc - time::Duration::hours(1),
        time_connected_until: now_utc,
    };
    connection_store.insert_outbound_connections(vec![conn.clone()]);
    conn_infos.push(conn);

    // check that the store contains exactly the expected connections
    check_outbound_connections(&connection_store, &conn_infos);
}
