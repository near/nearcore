use crate::time;
use near_o11y::testonly::init_test_logger;

#[tokio::test]
async fn test_query() {
    init_test_logger();
    let clock = time::FakeClock::default();
    let ip = super::query(&clock.clock(),"stun.l.google.com:19302".to_string()).await.unwrap();
    tracing::info!(target: "test", "ip = {ip}");
}
