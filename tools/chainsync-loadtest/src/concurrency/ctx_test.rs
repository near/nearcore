use crate::concurrency::{ctx, Ctx};

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
    let ctx1 = Ctx::background().with_cancel();
    let ctx2 = ctx1.with_cancel();
    let ctx3 = ctx1.with_cancel();
    let mut h1 = Box::pin(ctx1.done());
    let mut h2 = Box::pin(ctx2.done());
    let h3 = Box::pin(ctx3.done());
    assert!(ctx1.err().is_none());
    assert!(ctx2.err().is_none());
    assert!(ctx3.err().is_none());

    ctx3.cancel();
    assert!(ctx1.err().is_none());
    assert!(ctx2.err().is_none());
    assert_eq!(None, try_await(h1.as_mut()));
    assert_eq!(None, try_await(h2.as_mut()));
    assert_eq!(ctx::Error::Cancelled, h3.await);

    ctx1.cancel();
    assert_eq!(ctx::Error::Cancelled, h1.await);
    assert_eq!(ctx::Error::Cancelled, h2.await);
}
