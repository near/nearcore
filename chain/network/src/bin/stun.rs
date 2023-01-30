use near_network::time;
use near_o11y::testonly::init_test_logger;

#[tokio::main]
async fn main() {
    init_test_logger();
    let clock = time::FakeClock::default();
    let ip = near_network::stun::query(&clock.clock(),"stunserver.org:3478".to_string()).await.unwrap();
    tracing::info!(target: "test", "ip = {ip}");
}
