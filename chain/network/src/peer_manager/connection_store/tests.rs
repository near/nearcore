use crate::network_protocol::testonly::make_peer_info;
use crate::peer_manager::connection_store::ConnectionStore;
use crate::peer_manager::connection_store::OUTBOUND_CONNECTIONS_CACHE_SIZE;
use crate::store;
use crate::testonly::make_rng;
use crate::testonly::AsSet;
use crate::types::ConnectionInfo;
use near_async::time;
use rand::Rng;

/// Returns a ConnectionInfo with the given value for time_connected_until,
/// and with randomly generated peer_info and time_established
fn make_connection_info<R: Rng>(rng: &mut R, time_connected_until: time::Utc) -> ConnectionInfo {
    ConnectionInfo {
        peer_info: make_peer_info(rng),
        time_established: time_connected_until - time::Duration::hours(rng.gen_range(1..1000)),
        time_connected_until,
    }
}

/// Returns num_connections ConnectionInfos with the given value for time_connected_until,
/// and with randomly generated peer_infos and time_established values
fn make_connection_infos<R: Rng>(
    rng: &mut R,
    time_connected_until: time::Utc,
    num_connections: usize,
) -> Vec<ConnectionInfo> {
    (0..num_connections).map(|_| make_connection_info(rng, time_connected_until)).collect()
}

#[test]
fn test_reload_from_storage() {
    let mut rng = make_rng(921853233);
    let rng = &mut rng;
    let clock = time::FakeClock::default();
    let store = store::Store::from(near_store::db::TestDB::new());

    let conn_info = make_connection_info(rng, clock.now_utc());
    {
        tracing::debug!(target:"test", "write connection info to storage");
        let connection_store = ConnectionStore::new(store.clone()).unwrap();
        connection_store.insert_outbound_connections(vec![conn_info.clone()]);
    }
    {
        tracing::debug!(target:"test", "read connection info from storage");
        let connection_store = ConnectionStore::new(store).unwrap();
        assert_eq!(connection_store.get_recent_outbound_connections(), vec![conn_info]);
    }
}

#[test]
fn test_overwrite_stored_connection() {
    let mut rng = make_rng(921853233);
    let rng = &mut rng;
    let clock = time::FakeClock::default();
    let store = store::Store::from(near_store::db::TestDB::new());
    let connection_store = ConnectionStore::new(store).unwrap();

    tracing::debug!(target:"test", "create and store a connection");
    let now_utc = clock.now_utc();
    let conn_info_a = make_connection_info(rng, now_utc);
    connection_store.insert_outbound_connections(vec![conn_info_a.clone()]);

    tracing::debug!(target:"test", "insert a second connection with the same PeerId but different data");
    clock.advance(time::Duration::seconds(123));
    let now_utc = clock.now_utc();
    let mut conn_info_b = make_connection_info(rng, now_utc);
    conn_info_b.peer_info.id = conn_info_a.peer_info.id;
    connection_store.insert_outbound_connections(vec![conn_info_b.clone()]);

    tracing::debug!(target:"test", "check that precisely the second connection is present (and not the first)");
    assert_eq!(connection_store.get_recent_outbound_connections(), vec![conn_info_b]);
}

#[test]
fn test_evict_longest_disconnected() {
    let mut rng = make_rng(921853233);
    let rng = &mut rng;
    let clock = time::FakeClock::default();
    let store = store::Store::from(near_store::db::TestDB::new());
    let connection_store = ConnectionStore::new(store).unwrap();

    tracing::debug!(target:"test", "create and store live connections up to the ConnectionStore limit");
    let now_utc = clock.now_utc();
    let mut conn_infos = make_connection_infos(rng, now_utc, OUTBOUND_CONNECTIONS_CACHE_SIZE);
    connection_store.insert_outbound_connections(conn_infos.clone());

    tracing::debug!(target:"test", "remove one of the connections, advance the clock and update the ConnectionStore");
    conn_infos.remove(rand::thread_rng().gen_range(0..OUTBOUND_CONNECTIONS_CACHE_SIZE));
    clock.advance(time::Duration::hours(1));
    let now_utc = clock.now_utc();
    for c in conn_infos.iter_mut() {
        c.time_connected_until = now_utc;
    }
    connection_store.insert_outbound_connections(conn_infos.clone());

    tracing::debug!(target:"test", "insert a new connection");
    let conn = make_connection_info(rng, now_utc);
    connection_store.insert_outbound_connections(vec![conn.clone()]);
    conn_infos.push(conn);

    tracing::debug!(target:"test", "check that the store contains exactly the expected connections");
    assert_eq!(connection_store.get_recent_outbound_connections().as_set(), conn_infos.as_set());
}

#[test]
fn test_recovery_from_clock_rewind() {
    let mut rng = make_rng(921853233);
    let rng = &mut rng;
    let clock = time::FakeClock::default();
    let store = store::Store::from(near_store::db::TestDB::new());
    let connection_store = ConnectionStore::new(store).unwrap();

    tracing::debug!(target:"test", "create and store live connections up to the ConnectionStore limit");
    let now_utc = clock.now_utc();
    connection_store.insert_outbound_connections(make_connection_infos(
        rng,
        now_utc,
        OUTBOUND_CONNECTIONS_CACHE_SIZE,
    ));

    tracing::debug!(target:"test", "turn back the clock 1 year");
    clock.set_utc(now_utc - time::Duration::days(365));
    assert!(clock.now_utc() < now_utc);

    tracing::debug!(target:"test", "insert a new connection and check that it's at the front of the storage");
    let now_utc = clock.now_utc();
    let conn = make_connection_info(rng, now_utc);
    connection_store.insert_outbound_connections(vec![conn.clone()]);
    assert_eq!(connection_store.get_recent_outbound_connections()[0], conn);
}
