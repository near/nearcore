use crate::actix::ActixSystem;
use crate::network_protocol::testonly as data;
use crate::peer::stream;
use crate::tcp;
use crate::testonly::make_rng;
use actix::Actor as _;
use actix::ActorContext as _;
use rand::Rng as _;
use std::sync::Arc;
use tokio::sync::mpsc;

struct Actor {
    stream: stream::FramedStream<Actor>,
    queue_send: mpsc::UnboundedSender<stream::Frame>,
}

impl actix::Actor for Actor {
    type Context = actix::Context<Actor>;
}

#[derive(actix::Message)]
#[rtype("()")]
struct SendFrame(stream::Frame);

impl actix::Handler<SendFrame> for Actor {
    type Result = ();
    fn handle(&mut self, SendFrame(frame): SendFrame, _ctx: &mut Self::Context) {
        self.stream.send(frame);
    }
}

impl actix::Handler<stream::Frame> for Actor {
    type Result = ();
    fn handle(&mut self, frame: stream::Frame, _ctx: &mut Self::Context) {
        self.queue_send.send(frame).ok().unwrap();
    }
}

impl actix::Handler<stream::Error> for Actor {
    type Result = ();
    fn handle(&mut self, _err: stream::Error, ctx: &mut Self::Context) {
        ctx.stop();
    }
}

struct Handler {
    queue_recv: mpsc::UnboundedReceiver<stream::Frame>,
    system: ActixSystem<Actor>,
}

impl Actor {
    async fn spawn(s: tcp::Stream) -> Handler {
        let (queue_send, queue_recv) = mpsc::unbounded_channel();
        Handler {
            queue_recv,
            system: ActixSystem::spawn(|| {
                Actor::create(|ctx| {
                    let stream = stream::FramedStream::spawn(ctx, s, Arc::default());
                    Self { stream, queue_send }
                })
            })
            .await,
        }
    }
}

#[tokio::test]
async fn send_recv() {
    let mut rng = make_rng(98324532);
    let (s1, s2) = tcp::Stream::loopback(data::make_peer_id(&mut rng), tcp::Tier::T2).await;
    let a1 = Actor::spawn(s1).await;
    let mut a2 = Actor::spawn(s2).await;

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
            a1.system.addr.send(SendFrame(msg.clone())).await.unwrap();
        }
        for want in &msgs {
            let got = a2.queue_recv.recv().await.unwrap();
            assert_eq!(&got, want);
        }
    }
}
