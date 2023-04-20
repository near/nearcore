//! A rate-limited demultiplexer.
//! It can be useful for example, if you want to aggregate a bunch of
//! network requests produced by unrelated routines into a single
//! bulk message to rate limit the QPS.
//!
//! Example usage:
//! `
//! let d = Demux::new(rate::Limit(10.,1));
//! ...
//! let res = d.call(arg,|inout| async {
//!   // Process all inout[i].arg together.
//!   // Send the results to inout[i].out.
//! }).await;
//! `
//! If d.call is called simultaneously multiple times,
//! the arguments `arg` will be collected and just one
//! of the provided handlers will be executed asynchronously
//! (other handlers will be dropped).
//!
use crate::concurrency::rate;
use futures::future::BoxFuture;
use futures::FutureExt;
use near_async::time;
use std::future::Future;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

/// Boxed asynchronous function. In rust asynchronous functions
/// are just regular functions which return a Future.
/// This is a convenience alias to express a
pub type BoxAsyncFn<Arg, Res> = Box<dyn 'static + Send + FnOnce(Arg) -> BoxFuture<'static, Res>>;

/// AsyncFn trait represents asynchronous functions which can be boxed.
/// As a simplification (which makes the error messages more readable)
/// we require the argument and result types to be 'static + Send, which is
/// usually required anyway in practice due to Rust limitations.
pub trait AsyncFn<Arg: 'static + Send, Res: 'static + Send> {
    fn wrap(self) -> BoxAsyncFn<Arg, Res>;
}

impl<F, Arg, Res, Fut> AsyncFn<Arg, Res> for F
where
    F: 'static + Send + FnOnce(Arg) -> Fut,
    Fut: 'static + Send + Future<Output = Res>,
    Arg: 'static + Send,
    Res: 'static + Send,
{
    fn wrap(self) -> BoxAsyncFn<Arg, Res> {
        Box::new(move |a: Arg| self(a).boxed())
    }
}

/// A demux handler should be in practice of type `[Arg; n]` → `[Res; n]` for
/// arbitrary `n`.  We approximate that by a function `Vec<Arg>` → `Vec<Res>`.
/// If the sizes do not match, demux will panic.
type Handler<Arg, Res> = BoxAsyncFn<Vec<Arg>, Vec<Res>>;

struct Call<Arg, Res> {
    arg: Arg,
    out: oneshot::Sender<Res>,
    handler: Handler<Arg, Res>,
}
type Stream<Arg, Res> = mpsc::UnboundedSender<Call<Arg, Res>>;

/// Rate limited demultiplexer.
/// The current implementation spawns a dedicated future with an infinite loop to
/// aggregate the requests, and every bulk of requests is handled also in a separate spawned
/// future. The drawback of this approach is that every `d.call()` call requires to specify a
/// handler (and with multiple call sites these handlers might be inconsistent).
/// Instead we could technically make the handler an argument of the new() call. Then however
/// we risk that the handler will (indirectly) store the reference to the demux, therefore creating
/// a reference loop. In such case we get a memory leak: the spawned demux-handling future will never be cleaned
/// up, because the channel will never be closed.
///
/// Alternatives:
/// - use a separate closing signal (a golang-like structured concurrency).
/// - get rid of the dedicated futures whatsoever and make one of the callers do the work:
///   callers may synchronize and select a leader to execute the handler. This will however make
///   the demux implementation way more complicated.
#[derive(Clone)]
pub struct Demux<Arg, Res>(Stream<Arg, Res>);

#[derive(thiserror::Error, Debug, PartialEq)]
#[error("tokio::Runtime running the demux service has been stopped")]
pub struct ServiceStoppedError;

impl<Arg: 'static + Send, Res: 'static + Send> Demux<Arg, Res> {
    pub fn call(
        &self,
        arg: Arg,
        f: impl AsyncFn<Vec<Arg>, Vec<Res>>,
    ) -> impl std::future::Future<Output = Result<Res, ServiceStoppedError>> {
        let stream = self.0.clone();
        async move {
            let (send, recv) = oneshot::channel();
            // ok().unwrap(), because DemuxCall doesn't implement Debug.
            stream
                .send(Call { arg, out: send, handler: f.wrap() })
                .map_err(|_| ServiceStoppedError)?;
            recv.await.map_err(|_| ServiceStoppedError)
        }
    }

    // Spawns a subroutine performing the demultiplexing.
    // Panics if rl is not valid.
    pub fn new(rl: rate::Limit) -> Demux<Arg, Res> {
        rl.validate().unwrap();
        let (send, mut recv): (Stream<Arg, Res>, _) = mpsc::unbounded_channel();
        // TODO(gprusak): this task should be running as long as Demux object exists.
        // "Current" runtime can have a totally different lifespan, so we shouldn't spawn on it.
        // Find a way to express "runtime lifetime > Demux lifetime".
        tokio::spawn(async move {
            let mut calls = vec![];
            let mut closed = false;
            let mut tokens = rl.burst;
            let mut next_token = None;
            let interval = (time::Duration::SECOND / rl.qps).try_into().unwrap();
            while !(calls.is_empty() && closed) {
                // Restarting the timer every time a new request comes could
                // cause a starvation, so we compute the next token arrival time
                // just once for each token.
                if tokens < rl.burst && next_token.is_none() {
                    next_token = Some(tokio::time::Instant::now() + interval);
                }

                tokio::select! {
                    // TODO(gprusak): implement sleep future support for FakeClock,
                    // so that we don't use tokio directly here.
                    _ = async {
                        // without async {} wrapper, next_token.unwrap() would be evaluated
                        // unconditionally.
                        tokio::time::sleep_until(next_token.unwrap()).await
                    }, if next_token.is_some() => {
                        tokens += 1;
                        next_token = None;
                    }
                    call = recv.recv(), if !closed => match call {
                        Some(call) => calls.push(call),
                        None => closed = true,
                    },
                }
                if !calls.is_empty() && tokens > 0 {
                    // First pop all the elements already accumulated on the queue.
                    // TODO(gprusak): technically calling try_recv() in a loop may cause a starvation,
                    // in case elements are added to the queue faster than we can take them out,
                    // so ideally we should rather atomically dump the content of the queue:
                    // we can achieve that by maintaining an atomic counter with number of elements in
                    // the queue.
                    while let Ok(call) = recv.try_recv() {
                        calls.push(call);
                    }

                    tokens -= 1;
                    // TODO(gprusak): as of now Demux (as a concurrency primitive) doesn't support
                    // cancellation. Once we add cancellation support, this task could accept a context sum:
                    // the sum is valid iff any context is valid.
                    let calls = std::mem::take(&mut calls);
                    let mut args = vec![];
                    let mut outs = vec![];
                    let mut handlers = vec![];
                    // TODO(gprusak): due to inlining the call at most 1 call is executed at any
                    // given time. Ideally we should have a separate limit for the number of
                    // in-flight calls. It would be dangerous to have it unbounded, especially in a
                    // case when the concurrent calls would cause a contention.
                    // TODO(gprusak): add metrics for:
                    // - demuxed call latency (the one inlined here)
                    // - outer call latency (i.e. latency of the whole Demux.call, from the PoV of
                    // the caller).
                    for call in calls {
                        args.push(call.arg);
                        outs.push(call.out);
                        handlers.push(call.handler);
                    }
                    let res = handlers.swap_remove(0)(args).await;
                    assert_eq!(
                        res.len(),
                        outs.len(),
                        "demux handler returned {} results, expected {}",
                        res.len(),
                        outs.len(),
                    );
                    for (res, out) in std::iter::zip(res, outs) {
                        // If the caller is no longer interested in the result,
                        // the channel will be closed. Ignore that.
                        let _ = out.send(res);
                    }
                }
            }
        });
        Demux(send)
    }
}
