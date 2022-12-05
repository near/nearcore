use crate::actix::ActixSystem;
use crate::concurrency::rate;
use crate::network_protocol::testonly as data;
use crate::peer::stream;
use crate::peer::stream::Scope;
use crate::tcp;
use crate::testonly::make_rng;
use crate::time;
use actix::Actor as _;
use actix::ActorContext as _;
use actix::AsyncContext as _;
use near_o11y::testonly::init_test_logger;
use rand::Rng as _;
use std::sync::Arc;
use tokio::sync::mpsc;

struct Actor {
    writer: stream::FramedWriter<Actor>,
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
        self.writer.send(frame);
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
    async fn spawn(clock: &time::Clock, s: tcp::Stream) -> Handler {
        let (queue_send, queue_recv) = mpsc::unbounded_channel();
        let clock = clock.clone();
        Handler {
            queue_recv,
            system: ActixSystem::spawn(|| {
                Actor::create(|ctx| {
                    let scope = Scope { arbiter: actix::Arbiter::current(), addr: ctx.address() };
                    let (writer, mut reader) =
                        stream::FramedWriter::spawn(&scope, s, Arc::default());
                    let addr = ctx.address();
                    scope.arbiter.spawn(async move {
                        let limiter = rate::Limiter::new(
                            &clock,
                            rate::Limit { qps: 10000., burst: 10000000 },
                        );
                        loop {
                            match reader.recv(&clock, &limiter).await {
                                Ok(frame) => queue_send.send(frame).ok().unwrap(),
                                Err(err) => addr.do_send(stream::Error::Recv(err)),
                            }
                        }
                    });
                    Self { writer }
                })
            })
            .await,
        }
    }
}

#[tokio::test]
async fn send_recv() {
    init_test_logger();
    let clock = time::FakeClock::default();
    let mut rng = make_rng(98324532);
    let (s1, s2) = tcp::Stream::loopback(data::make_peer_id(&mut rng), tcp::Tier::T2).await;
    let a1 = Actor::spawn(&clock.clock(), s1).await;
    let mut a2 = Actor::spawn(&clock.clock(), s2).await;

    for i in 0..5 {
        tracing::info!(target:"test","iteration {i}");
        let n = rng.gen_range(1..10);
        let msgs: Vec<_> = (0..n)
            .map(|_| {
                let size = rng.gen_range(0..10000);
                let mut msg = vec![0; size];
                rng.fill(&mut msg[..]);
                stream::Frame(msg)
            })
            .collect();
        tracing::info!(target:"test", "send");
        for msg in &msgs {
            a1.system.addr.send(SendFrame(msg.clone())).await.unwrap();
        }
        tracing::info!(target:"test", "recv");
        for want in &msgs {
            let got = a2.queue_recv.recv().await.unwrap();
            assert_eq!(&got, want);
        }
    }
}
