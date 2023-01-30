use crate::time;
use near_o11y::testonly::init_test_logger;
use std::sync::Arc;

struct TestAuthHandler;

impl turn::auth::AuthHandler for TestAuthHandler {
    fn auth_handle(
        &self,
        _username: &str,
        _realm: &str,
        _src_addr: std::net::SocketAddr,
    ) -> Result<Vec<u8>, turn::Error> {
        unimplemented!();
    }
}

#[tokio::test]
async fn test_query() {
    init_test_logger();

    let server_conn = Arc::new(tokio::net::UdpSocket::bind("[::1]:0").await.unwrap());
    let server_addr = server_conn.local_addr().unwrap();

    let server = turn::server::Server::new(turn::server::config::ServerConfig {
        conn_configs: vec![turn::server::config::ConnConfig {
            conn: server_conn,
            relay_addr_generator: Box::new(turn::relay::relay_none::RelayAddressGeneratorNone {
                address: "[::]".to_owned(),
                net: Arc::new(webrtc_util::vnet::net::Net::new(None)),
            }),
        }],
        realm: "webrtc.rs".to_owned(),
        auth_handler: Arc::new(TestAuthHandler),
        channel_bind_timeout: std::time::Duration::from_secs(0),
    })
    .await
    .unwrap();

    let clock = time::FakeClock::default();
    let ip = super::query(&clock.clock(), &server_addr.to_string()).await.unwrap();
    assert_eq!(std::net::Ipv6Addr::LOCALHOST, ip);
    server.close().await.unwrap();
}
