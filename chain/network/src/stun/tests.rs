use crate::stun;
use near_o11y::testonly::init_test_logger;
use near_primitives::time;

#[tokio::test]
async fn test_query() {
    init_test_logger();
    let clock = time::FakeClock::default();
    let server = stun::testonly::Server::new().await;
    let ip = stun::query(&clock.clock(), &server.addr()).await.unwrap();
    assert_eq!(std::net::Ipv6Addr::LOCALHOST, ip);
    server.close().await;
}
