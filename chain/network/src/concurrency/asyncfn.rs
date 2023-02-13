use futures::future::{BoxFuture, Future, FutureExt};

/// Boxed asynchronous function. In rust asynchronous functions
/// are just regular functions which return a Future.
/// This is a convenience alias to express a
pub type BoxAsyncFn<'a, Arg, Res> = Box<dyn 'a + Send + FnOnce(Arg) -> BoxFuture<'a, Res>>;

/// AsyncFn trait represents asynchronous functions which can be boxed.
/// As a simplification (which makes the error messages more readable)
/// we require the argument and result types to be 'static + Send, which is
/// usually required anyway in practice due to Rust limitations.
pub trait AsyncFn<'a, Arg: 'a + Send, Res: 'a + Send>: 'a + Send {
    fn wrap(self) -> BoxAsyncFn<'a, Arg, Res>;
}

impl<'a, F, Arg, Res, Fut> AsyncFn<'a, Arg, Res> for F
where
    F: 'a + Send + FnOnce(Arg) -> Fut,
    Fut: 'a + Send + Future<Output = Res>,
    Arg: 'a + Send,
    Res: 'a + Send,
{
    fn wrap(self) -> BoxAsyncFn<'a, Arg, Res> {
        Box::new(move |a: Arg| self(a).boxed())
    }
}
