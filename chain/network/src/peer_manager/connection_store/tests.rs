use crate::peer_manager::connection_store::ConnectionStore;
use crate::store;
use crate::time;
use crate::types::{ConnectionInfo, PeerInfo};
use near_crypto::{KeyType, SecretKey};
use near_primitives::network::PeerId;
use near_store::NodeStorage;
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
