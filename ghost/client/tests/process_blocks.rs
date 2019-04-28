use actix::actors::mocker::Mocker;
use actix::{Actor, System};
use near_chain::{BlockHeader, RuntimeAdapter, Block};
use near_client::{ClientActor, NetworkMessages};
use near_network::{PeerManagerActor, PeerInfo};
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
        // let block = Block { header: BlockHeader::default(), transactions: vec![] };
        // client.do_send(NetworkMessages::Block(block, PeerInfo { id:  }));
        System::current().stop();
    })
    .unwrap();
}
