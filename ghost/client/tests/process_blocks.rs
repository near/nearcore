use actix::actors::mocker::Mocker;
use actix::{Actor, System};
use near_chain::{BlockHeader, RuntimeAdapter};
use near_client::ClientActor;
use near_network::PeerManagerActor;
use near_store::test_utils::create_test_store;
use primitives::test_utils::init_test_logger;
use std::sync::Arc;

pub struct NoopAdapter {}

impl RuntimeAdapter for NoopAdapter {}

type NetworkMock = Mocker<PeerManagerActor>;

#[test]
fn accept_correct_blocks() {
    init_test_logger();
    System::run(|| {
        let pm = NetworkMock::mock(Box::new(|msg, ctx| Box::new(()))).start();
        let client = ClientActor::new(
            create_test_store(),
            Arc::new(NoopAdapter {}),
            BlockHeader::default(),
            pm.recipient(),
        )
        .unwrap()
        .start();
        client.do_send();
        System::current().stop();
    })
    .unwrap();
}
