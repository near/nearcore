use crate::config_json::default_trusted_stun_servers;
use crate::stun;
use near_async::time;
use near_o11y::testonly::init_test_logger;

#[tokio::test]
async fn test_query() {
    init_test_logger();
    let clock = time::FakeClock::default();
    let server = stun::testonly::Server::new().await;
    let ip = stun::query(&clock.clock(), &server.addr()).await.unwrap();
    assert_eq!(std::net::Ipv6Addr::LOCALHOST, ip);
    server.close().await;
}

#[tokio::test]
async fn test_lookup_host() {
    init_test_logger();

    for addr in default_trusted_stun_servers() {
        tracing::debug!("Querying STUN server at {}", addr);

        // Allow lookup to return nothing; the server may be unreachable.
        // What we want to check here is that if an address is returned,
        // it has the expected type (IPv4 vs IPv6).
        if let Some(ipv4) = stun::lookup_host(&addr, true).await {
            assert!(ipv4.is_ipv4());
            tracing::debug!("My IPv4 addr is {}", ipv4);
        }

        if let Some(ipv6) = stun::lookup_host(&addr, false).await {
            assert!(ipv6.is_ipv6());
            tracing::debug!("My IPv6 addr is {}", ipv6);
        }
    }
}
