use crate::concurrency::demux;

#[tokio::test]
async fn test_demux() {
    let demux = demux::Demux::new(demux::RateLimit { qps: 50., burst: 1 });
    for _ in 0..5 {
        let mut handles = vec![];
        for i in 0..1000 {
            let demux = demux.clone();
            handles.push(tokio::spawn(async move {
                let j = demux
                    .call(i, |is: Vec<u64>| async { is.into_iter().map(|i| i + 1).collect() })
                    .await
                    .unwrap();
                assert_eq!(i + 1, j);
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
    }
}
