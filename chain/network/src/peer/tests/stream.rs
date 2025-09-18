use crate::actix::AutoStopActor;
use crate::network_protocol::testonly as data;
use crate::peer::stream;
use crate::tcp;
use crate::testonly::make_rng;
use near_async::messaging::{CanSendAsync, IntoSender};
use near_async::tokio::TokioRuntimeHandle;
use near_async::{ActorSystem, messaging};
use rand::Rng as _;
use std::sync::Arc;
use tokio::sync::mpsc;

struct Actor {
    handle: TokioRuntimeHandle<Self>,
    stream: stream::FramedStream,
    queue_send: mpsc::UnboundedSender<stream::Frame>,
}

impl messaging::Actor for Actor {}

#[derive(actix::Message, Debug)]
#[rtype("()")]
struct SendFrame(stream::Frame);

impl messaging::Handler<SendFrame> for Actor {
    fn handle(&mut self, SendFrame(frame): SendFrame) {
        self.stream.send(frame);
    }
}

impl messaging::Handler<stream::Frame> for Actor {
    fn handle(&mut self, frame: stream::Frame) {
        self.queue_send.send(frame).ok().unwrap();
    }
}

impl messaging::Handler<stream::Error> for Actor {
    fn handle(&mut self, _err: stream::Error) {
        self.handle.stop();
    }
}

struct Handler {
    queue_recv: mpsc::UnboundedReceiver<stream::Frame>,
    system: AutoStopActor<Actor>,
}

impl Actor {
    fn spawn(actor_system: ActorSystem, s: tcp::Stream) -> Handler {
        let (queue_send, queue_recv) = mpsc::unbounded_channel();
        let builder = actor_system.new_tokio_builder();
        let handle = builder.handle();
        let framed_stream = stream::FramedStream::spawn(
            handle.clone().into_sender(),
            handle.clone().into_sender(),
            &*handle.future_spawner(),
            s,
            Arc::default(),
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
            assert_eq!(&got, want);
        }
    }
}
