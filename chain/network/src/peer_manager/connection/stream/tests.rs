use crate::concurrency::scope;
use crate::network_protocol::testonly as data;
use crate::peer_manager::connection::stream;
use crate::tcp;
use crate::testonly::abort_on_panic;
use crate::testonly::make_rng;
use rand::Rng as _;
use std::sync::Arc;

#[tokio::test]
async fn send_recv() {
    const OK: Result<(), ()> = Ok(());
    abort_on_panic();
    let mut rng = make_rng(98324532);
    scope::run!(|s| async {
        let (s1, s2) = tcp::Stream::loopback(data::make_peer_id(&mut rng), tcp::Tier::T2).await;
        let s1 = Arc::new(stream::FrameStream::new(s.new_service(), s1).unwrap());
        let s2 = Arc::new(stream::FrameStream::new(s.new_service(), s2).unwrap());
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
            scope::run!(|s| async {
                s.spawn(async {
                    for msg in &msgs {
                        s1.send(msg.clone()).await.unwrap();
                    }
                    OK
                });
                s.spawn(async {
                    for msg in &msgs {
                        assert_eq!(msg, &s2.recv().await.unwrap());
                    }
                    OK
                });
                OK
            })
            .unwrap();
        }
        OK
    })
    .unwrap();
}
