use crate::network_protocol::testonly as data;
use crate::peer::stream;
use crate::tcp;
use crate::testonly::make_rng;
use near_async::executor::{ExecutorHandle, ExecutorRuntime};
use near_async::messaging::{self, SendAsync};
use rand::Rng as _;
use std::sync::Arc;
use tokio::sync::mpsc;

struct Actor {
    stream: stream::FramedStream<Actor>,
    queue_send: mpsc::UnboundedSender<stream::Frame>,
    handle: ExecutorHandle<Self>,
}

impl messaging::Actor for Actor {}

#[derive(actix::Message)]
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
        self.handle.cancel();
    }
}

struct Handler {
    queue_recv: mpsc::UnboundedReceiver<stream::Frame>,
    system: ExecutorHandle<Actor>,
}

impl Actor {
    fn spawn(s: tcp::Stream) -> Handler {
        let (queue_send, queue_recv) = mpsc::unbounded_channel();
        Handler {
            queue_recv,
            system: ExecutorRuntime::new().construct_actor_in_runtime(|handle| {
                let stream = stream::FramedStream::spawn(handle.clone(), s, Arc::default());
                Self { stream, queue_send, handle }
            }),
        }
    }
}

#[tokio::test]
async fn send_recv() {
    let mut rng = make_rng(98324532);
    let (s1, s2) = tcp::Stream::loopback(data::make_peer_id(&mut rng), tcp::Tier::T2).await;
    let a1 = Actor::spawn(s1);
    let mut a2 = Actor::spawn(s2);

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
