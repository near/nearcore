use crate::auto_stop::AutoStopActor;
use crate::concurrency::outgoing_queue_limiter::OutgoingPermit;
use crate::config::DEFAULT_MAX_WRITE_BUFFER_CAPACITY_BYTES;
use crate::network_protocol::testonly as data;
use crate::network_protocol::{Encoding, PeerMessage, PeerMessageFrames, PeersRequest};
use crate::peer::stream::{self, IncomingFrame};
use crate::peer_manager::network_state::INCOMING_SEMAPHORE_PERMITS;
use crate::tcp;
use crate::testonly::make_rng;
use near_async::messaging::{CanSendAsync, IntoAsyncSender, IntoSender};
use near_async::tokio::TokioRuntimeHandle;
use near_async::{ActorSystem, messaging, time};
use rand::Rng as _;
use std::sync::Arc;
use tokio::sync::{Semaphore, mpsc};

struct Actor {
    handle: TokioRuntimeHandle<Self>,
    stream: stream::FramedStream,
    queue_send: mpsc::UnboundedSender<IncomingFrame>,
}

impl messaging::Actor for Actor {}

#[derive(Debug)]
struct SendFrame(PeerMessageFrames);

impl messaging::Handler<SendFrame> for Actor {
    fn handle(&mut self, SendFrame(message): SendFrame) {
        self.stream.send(stream::Frame::with_permit(message, OutgoingPermit::fake_for_test()));
    }
}

impl messaging::Handler<IncomingFrame> for Actor {
    fn handle(&mut self, frame: IncomingFrame) {
        self.queue_send.send(frame).ok().unwrap();
    }
}

impl messaging::Handler<stream::Error> for Actor {
    fn handle(&mut self, _err: stream::Error) {
        self.handle.stop();
    }
}

struct Handler {
    queue_recv: mpsc::UnboundedReceiver<IncomingFrame>,
    system: AutoStopActor<Actor>,
}

impl Actor {
    fn spawn(actor_system: ActorSystem, s: tcp::Stream) -> Handler {
        let (queue_send, queue_recv) = mpsc::unbounded_channel();
        let builder = actor_system.new_tokio_builder();
        let handle = builder.handle();
        let framed_stream = stream::FramedStream::spawn(
            handle.clone().into_sender(),
            handle.clone().into_async_sender(),
            &*handle.future_spawner(),
            s,
            Arc::default(),
            Arc::new(Semaphore::new(INCOMING_SEMAPHORE_PERMITS)),
            &[Encoding::Borsh, Encoding::Proto],
            DEFAULT_MAX_WRITE_BUFFER_CAPACITY_BYTES,
        );
        let actor = Actor { handle: handle.clone(), stream: framed_stream, queue_send };
        builder.spawn_tokio_actor(actor);
        Handler { queue_recv, system: AutoStopActor(handle) }
    }
}

#[tokio::test]
async fn send_recv() {
    let mut rng = make_rng(98324532);
    let (s1, s2) = tcp::Stream::loopback(data::make_peer_id(&mut rng), tcp::Tier::T2).await;
    let actor_system = ActorSystem::new();
    let a1 = Actor::spawn(actor_system.clone(), s1);
    let mut a2 = Actor::spawn(actor_system, s2);

    for _ in 0..5 {
        let n = rng.gen_range(1..10);
        let msgs: Vec<PeerMessageFrames> = (0..n)
            .map(|_| {
                let size = rng.gen_range(1..10000);
                let mut msg = vec![0; size];
                rng.fill(&mut msg[..]);
                // The first byte is never a borsh header, so that the first frame is
                // unambiguously protobuf.
                msg[0] = 0x20;
                PeerMessageFrames::proto(msg)
            })
            .collect();
        for msg in &msgs {
            a1.system.send_async(SendFrame(msg.clone())).await.unwrap();
        }
        for want in &msgs {
            let got = a2.queue_recv.recv().await.unwrap();
            assert_eq!(&got.message, want);
        }
    }
}

/// A node which supports both encodings sends its first handshake in borsh and then in
/// protobuf. The receiver of the borsh handshake ignores the protobuf copy.
#[tokio::test]
async fn skip_protobuf_copy_of_borsh_handshake() {
    let mut rng = make_rng(89028037453);
    let mut clock = time::FakeClock::default();
    let chain = data::Chain::make(&mut clock, &mut rng, 2);
    let handshake = PeerMessage::Tier2Handshake(data::make_handshake(&mut rng, &chain));
    let borsh_handshake = handshake.serialize_with_encoding(Encoding::Borsh);
    let proto_handshake = handshake.serialize_with_encoding(Encoding::Proto);
    let msg = PeerMessage::PeersRequest(PeersRequest { max_peers: None, max_direct_peers: None });
    let msg = msg.serialize_with_encoding(Encoding::Borsh);

    let (s1, s2) = tcp::Stream::loopback(data::make_peer_id(&mut rng), tcp::Tier::T2).await;
    let actor_system = ActorSystem::new();
    let a1 = Actor::spawn(actor_system.clone(), s1);
    let mut a2 = Actor::spawn(actor_system, s2);

    a1.system.send_async(SendFrame(borsh_handshake.clone())).await.unwrap();
    a1.system.send_async(SendFrame(proto_handshake)).await.unwrap();
    a1.system.send_async(SendFrame(msg.clone())).await.unwrap();

    let got = a2.queue_recv.recv().await.unwrap();
    assert_eq!(got.message, borsh_handshake);
    // The protobuf copy of the handshake is skipped.
    let got = a2.queue_recv.recv().await.unwrap();
    assert_eq!(got.message, msg);
}

/// A frame which does not parse as a borsh header.
fn non_borsh_frame() -> PeerMessageFrames {
    PeerMessageFrames { encoding: Encoding::Borsh, header: None, body: vec![0] }
}

/// A frame which does not parse as a borsh header is treated as a protobuf-encoded message.
#[tokio::test]
async fn non_borsh_first_frame_is_treated_as_protobuf() {
    let mut rng = make_rng(89028037453);
    let (s1, s2) = tcp::Stream::loopback(data::make_peer_id(&mut rng), tcp::Tier::T2).await;
    let actor_system = ActorSystem::new();
    let a1 = Actor::spawn(actor_system.clone(), s1);
    let mut a2 = Actor::spawn(actor_system, s2);

    a1.system.send_async(SendFrame(non_borsh_frame())).await.unwrap();
    let got = a2.queue_recv.recv().await.unwrap();
    assert_eq!(got.message, PeerMessageFrames::proto(vec![0]));
}

/// The peer may send a message before its handshake, e.g. `LastEdge` or `HandshakeFailure` in
/// response to a handshake it doesn't accept. The receiver must accept a header as the first
/// frame of a connection.
#[tokio::test]
async fn accept_message_before_handshake() {
    let mut rng = make_rng(89028037453);
    let msg = PeerMessage::PeersRequest(PeersRequest { max_peers: None, max_direct_peers: None });
    let msg = msg.serialize_with_encoding(Encoding::Borsh);

    let (s1, s2) = tcp::Stream::loopback(data::make_peer_id(&mut rng), tcp::Tier::T2).await;
    let actor_system = ActorSystem::new();
    let a1 = Actor::spawn(actor_system.clone(), s1);
    let mut a2 = Actor::spawn(actor_system, s2);

    a1.system.send_async(SendFrame(msg.clone())).await.unwrap();
    let got = a2.queue_recv.recv().await.unwrap();
    assert_eq!(got.message, msg);
}
