use crate::auto_stop::AutoStopActor;
use crate::network_protocol::testonly as data;
use crate::peer::stream::{self, IncomingFrame};
use crate::peer_manager::network_state::INCOMING_SEMAPHORE_PERMITS;
use crate::tcp;
use crate::testonly::make_rng;
use near_async::messaging::{CanSendAsync, IntoAsyncSender, IntoSender};
use near_async::tokio::TokioRuntimeHandle;
use near_async::{ActorSystem, messaging};
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
struct SendFrame(stream::Frame);

impl messaging::Handler<SendFrame> for Actor {
    fn handle(&mut self, SendFrame(frame): SendFrame) {
        self.stream.send(frame);
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
        let msgs: Vec<_> = (0..n)
            .map(|_| {
                let size = rng.gen_range(0..10000);
                let mut msg = vec![0; size];
                rng.fill(&mut msg[..]);
                stream::Frame(msg)
            })
            .collect();
        for msg in &msgs {
            a1.system.send_async(SendFrame(msg.clone())).await.unwrap();
        }
        for want in &msgs {
            let got = a2.queue_recv.recv().await.unwrap();
            assert_eq!(got.data, want.0);
        }
    }
}
