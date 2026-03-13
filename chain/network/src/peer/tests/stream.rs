use crate::auto_stop::AutoStopActor;
use crate::network_protocol::testonly as data;
use crate::peer::stream;
use crate::stats::metrics;
use crate::tcp;
use crate::testonly::make_rng;
use near_async::messaging::{CanSendAsync, IntoAsyncSender, IntoSender};
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

#[derive(Debug)]
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
            handle.clone().into_async_sender(),
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

/// Simulates a half-open TCP connection by holding the receiver socket open without reading.
/// The sender's TCP buffer fills up, writes block, and the write timeout fires.
/// Uses run_send_loop directly with a short timeout (1s) to keep the test fast.
#[tokio::test]
async fn write_timeout_on_half_open_connection() {
    near_o11y::testonly::init_test_logger();
    let listener = tokio::net::TcpListener::bind("[::1]:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (client, server) = tokio::join!(tokio::net::TcpStream::connect(addr), listener.accept(),);
    let client = client.unwrap();
    // Hold the server socket open without reading — simulates a process that crashed
    // without sending FIN (e.g. killed with SIGKILL, or network partition).
    let _server = server.unwrap().0;

    let (_, tcp_send) = tokio::io::split(client);
    let (queue_send, queue_recv) = tokio::sync::mpsc::unbounded_channel();
    let stats: Arc<crate::peer_manager::connection::Stats> = Arc::default();
    let buf_size_metric = Arc::new(metrics::MetricGuard::new(
        &*metrics::PEER_DATA_WRITE_BUFFER_SIZE,
        vec!["test_half_open".to_string()],
    ));

    // Enqueue enough large messages to fill the TCP send buffer.
    for _ in 0..64 {
        let _ = queue_send.send(stream::Frame(vec![0u8; 1024 * 1024]));
    }
    // Close the sender so run_send_loop will drain the queue and exit.
    drop(queue_send);

    // Use a short write timeout (1s) to keep the test fast.
    let write_timeout = std::time::Duration::from_secs(5);
    let result = stream::FramedStream::run_send_loop(
        tcp_send,
        queue_recv,
        stats,
        buf_size_metric,
        write_timeout,
    )
    .await;

    let err = result.unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::TimedOut);
}
