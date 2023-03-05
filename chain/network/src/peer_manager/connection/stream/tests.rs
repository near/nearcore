use crate::concurrency::scope;
use crate::network_protocol::testonly as data;
use crate::peer_manager::connection::stream;
use crate::tcp;
use crate::testonly::abort_on_panic;
use crate::testonly::make_rng;
use rand::Rng as _;

#[tokio::test]
async fn send_recv() {
    const OK: Result<(), ()> = Ok(());
    abort_on_panic();
    let mut rng = make_rng(98324532);
    let (s1, s2) = tcp::Stream::loopback(data::make_peer_id(&mut rng), tcp::Tier::T2).await;
    let (mut s1send, _) = stream::split(s1);
    let (_, mut s2recv) = stream::split(s2);
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
                    s1send.send(&msg).await.unwrap();
                }
                OK
            });
            s.spawn(async {
                for msg in &msgs {
                    assert_eq!(msg, &s2recv.recv().await.unwrap());
                }
                OK
            });
            OK
        })
        .unwrap();
    }
}
