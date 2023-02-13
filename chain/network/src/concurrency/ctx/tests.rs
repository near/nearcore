use crate::concurrency::ctx::Ctx;

use futures::task;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
struct DummyWake;

impl task::ArcWake for DummyWake {
    fn wake_by_ref(_arc_self: &Arc<Self>) {}
}

fn try_await<F, T>(f: Pin<&mut F>) -> Option<T>
where
    F: Future<Output = T>,
{
    match f.poll(&mut task::Context::from_waker(&task::waker(Arc::new(DummyWake {})))) {
        task::Poll::Ready(v) => Some(v),
        task::Poll::Pending => None,
    }
}

#[tokio::test]
async fn test_cancel_propagation() {
    let ctx1 = Ctx::inf().with_cancel();
    let ctx2 = ctx1.with_cancel();
    let ctx3 = ctx1.with_cancel();
    let mut h1 = Box::pin(ctx1.canceled());
    let mut h2 = Box::pin(ctx2.canceled());
    let h3 = Box::pin(ctx3.canceled());
    assert!(!ctx1.is_canceled());
    assert!(!ctx2.is_canceled());
    assert!(!ctx3.is_canceled());

    ctx3.cancel();
    assert!(!ctx1.is_canceled());
    assert!(!ctx2.is_canceled());
    assert_eq!(None, try_await(h1.as_mut()));
    assert_eq!(None, try_await(h2.as_mut()));
    h3.await;

    ctx1.cancel();
    h1.await;
    h2.await;
}
