use crate::concurrency::scope::WaitGroup;

#[tokio::test]
async fn test_wait_group() {
    let wg = WaitGroup::new(0);
    // Awaiting an empty waitgroup should return immediately.
    wg.wait().await;

    wg.inc();
    // Create a wait() future on a non-empty wg.
    let f = wg.wait();
    // Make the wg empty again.
    wg.dec();
    // The future should return immediately, even though f.await was not active,
    // when wg became empty.
    f.await;

    // wg is empty again, so this should return immediately.
    wg.wait().await;
}
