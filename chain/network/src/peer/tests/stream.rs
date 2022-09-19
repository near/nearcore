use crate::actix::ActixSystem;
use crate::concurrency::rate;
use crate::peer::stream;
use crate::peer::stream::Scope;
use crate::testonly::make_rng;
use crate::time;
use actix::Actor as _;
use actix::ActorContext as _;
use actix::AsyncContext as _;
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
    async fn spawn(clock: &time::Clock, s: tokio::net::TcpStream) -> Handler {
        let (queue_send, queue_recv) = mpsc::unbounded_channel();
        let clock = clock.clone();
        Handler {
            queue_recv,
            system: ActixSystem::spawn(|| {
                Actor::create(|ctx| {
                    let scope = Scope { arbiter: actix::Arbiter::current(), addr: ctx.address() };
                    let (writer, mut reader) = stream::FramedWriter::spawn(
                        &scope,
                        s.peer_addr().unwrap(),
                        s,
                        Arc::default(),
                    );
                    let addr = ctx.address();
                    scope.arbiter.spawn(async move {
                        let limiter =
                            rate::Limiter::new(&clock, rate::Limit { qps: 10000., burst: 10000 });
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
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (s1, s2) = tokio::join!(
        tokio::net::TcpStream::connect(listener.local_addr().unwrap()),
        listener.accept(),
    );
    let clock = time::FakeClock::default();
    let a1 = Actor::spawn(&clock.clock(), s1.unwrap()).await;
    let mut a2 = Actor::spawn(&clock.clock(), s2.unwrap().0).await;

    let mut rng = make_rng(98324532);
    for _ in 0..5 {
        let n = rng.gen_range(1, 10);
        let msgs: Vec<_> = (0..n)
            .map(|_| {
                let size = rng.gen_range(0, 10000);
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
