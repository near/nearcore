use crate::peer_manager::peer_manager_actor::Event as PME;
use crate::peer_manager::testonly::Event;
use crate::{network_protocol, peer_manager, testonly::make_rng};
use near_async::time::Duration;
use near_async::time::FakeClock;
use std::future::Future;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;

fn in_tokio(f: impl Future<Output = ()>) {
    tokio::runtime::Runtime::new().unwrap().block_on(f)
}

async fn random_handshake_connect(input: &[u8]) {
    let mut rng = make_rng(1234);
    let mut clock = FakeClock::default();
    let chain = Arc::new(network_protocol::testonly::Chain::make(&mut clock, &mut rng, 0));
    let clock = clock.clock();
    let mut config = chain.make_config(&mut rng);
    // Do not wait too long in this test, we’ll push the handshake right away anyway
    config.handshake_timeout = Duration::milliseconds(10);
    let mut pm = peer_manager::testonly::start(
        clock.clone(),
        near_store::db::TestDB::new(),
        config,
        chain.clone(),
    )
    .await;
    let fuzzer_peer_config = chain.make_config(&mut rng);
    let mut fuzzer_peer = pm.start_inbound(chain.clone(), fuzzer_peer_config.clone()).await;
    let _ = fuzzer_peer.stream.stream.write_all(input).await; // ignore failures, eg. connection closed
    pm.events
        .recv_until(|ev| match ev {
            Event::PeerManager(PME::HandshakeCompleted(_)) => {
                // TODO: should remove this panic, but for now let’s keep it until the fuzzer actually
                // hits it: it will prove that the fuzzer is actually able to generate interesting inputs.
                panic!("Fuzzer did find a valid handshake");
            }
            Event::PeerManager(PME::ConnectionClosed(_)) => Some(()),
            _ => None,
        })
        .await;
    pm.check_consistency().await;
    crate::tcp::RESERVED_LISTENER_ADDRS.lock().unwrap().clear();
}

#[test]
fn random_handshake_connect_fuzzer() {
    // init_test_logger(); // Enable only when reproducing a failure, to avoid slowing down the fuzzers too much
    bolero::check!().for_each(|input| in_tokio(random_handshake_connect(input)))
}
