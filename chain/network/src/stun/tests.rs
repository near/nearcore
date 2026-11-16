use crate::config_json::default_trusted_stun_servers;
use crate::stun;
use near_async::time;
use near_o11y::testonly::init_test_logger;
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::runtime::Handle;

#[tokio::test]
async fn test_query() {
    init_test_logger();
    let clock = time::FakeClock::default();
    let server = stun::testonly::Server::new().await;
    let ip = stun::query(&clock.clock(), &server.addr()).await.unwrap();
    assert_eq!(std::net::Ipv6Addr::LOCALHOST, ip);
    server.close().await;
}

/// Checks that a query which times out releases the stun client's background tasks.
#[tokio::test]
async fn query_timeout_closes_client() {
    init_test_logger();
    let clock = time::FakeClock::default();
    // A UDP socket that never answers.
    let silent = UdpSocket::bind("[::1]:0").await.unwrap();
    let addr = silent.local_addr().unwrap();
    let metrics = Handle::current().metrics();
    let tasks_before = metrics.num_alive_tasks();
    for _ in 0..3 {
        let query_clock = clock.clock();
        let mut query = tokio::spawn(async move { stun::query(&query_clock, &addr).await });
        let res = loop {
            tokio::select! {
                res = &mut query => break res.unwrap(),
                _ = tokio::time::sleep(Duration::from_millis(5)) => {
                    clock.advance(time::Duration::seconds(1));
                }
            }
        };
        assert!(matches!(res, Err(stun::Error::ErrTransactionTimeOut)), "{res:?}");
    }
    // The client's background tasks exit asynchronously after close().
    for _ in 0..200 {
        if metrics.num_alive_tasks() <= tasks_before {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(metrics.num_alive_tasks(), tasks_before);
}

#[tokio::test]
async fn test_lookup_host() {
    init_test_logger();

    for addr in default_trusted_stun_servers() {
        tracing::debug!(%addr, "querying stun server");

        // Allow lookup to return nothing; the server may be unreachable.
        // What we want to check here is that if an address is returned,
        // it has the expected type (IPv4 vs IPv6).
        if let Some(ipv4) = stun::lookup_host(&addr, true).await {
            assert!(ipv4.is_ipv4());
            tracing::debug!(%ipv4, "my IPv4 addr");
        }

        if let Some(ipv6) = stun::lookup_host(&addr, false).await {
            assert!(ipv6.is_ipv6());
            tracing::debug!(%ipv6, "my IPv6 addr");
        }
    }
}
