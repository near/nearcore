use crate::concurrency::signal;
use std::future::Future;
use std::sync::Arc;

pub mod time;

#[cfg(test)]
mod tests;

thread_local! {
    static CTX: std::cell::UnsafeCell<Ctx> = std::cell::UnsafeCell::new(Ctx::new(
        near_async::time::Clock::real()
    ));
}

/// Ensures that the local ctx is rolled back even when stack unwinding happens.
struct SetLocalCtx<'a>(&'a mut Ctx);

impl<'a> SetLocalCtx<'a> {
    fn new(ctx: &'a mut Ctx) -> Self {
        CTX.with(|x| std::mem::swap(unsafe { &mut *x.get() }, ctx));
        Self(ctx)
    }
}

impl<'a> Drop for SetLocalCtx<'a> {
    fn drop(&mut self) {
        CTX.with(|x| std::mem::swap(unsafe { &mut *x.get() }, self.0));
    }
}

/// Inner representation of a context.
struct Inner {
    canceled: signal::Once,
    deadline: near_async::time::Deadline,
    clock: near_async::time::Clock,
    /// When Inner gets dropped, the context gets cancelled, so that
    /// the tokio task which propagates the cancellation from
    /// parent to child it terminated immediately and therefore doesn't
    /// leak memory (see `Ctx::with_deadline`). However we don't want
    /// the context to get cancelled only because the references to parent
    /// are dropped. Therefore we keep a reference to parent here, so
    /// that the parent is not dropped until all its children are dropped.
    _parent: Option<Arc<Inner>>,
}

impl Drop for Inner {
    fn drop(&mut self) {
        // This will wake the task propagating cancellation to children (see `Ctx::with_deadline`).
        // Note that since children keep a reference to the parent, at this point nobody awaits
        // the cancelation of any child.
        self.canceled.send();
    }
}

/// See `Ctx::wait`.
#[derive(thiserror::Error, Debug)]
#[error("task has been canceled")]
pub struct ErrCanceled;

/// See `Ctx::wait`.
pub type OrCanceled<T> = Result<T, ErrCanceled>;

/// Ctx is an implementation of https://pkg.go.dev/context.
/// Ctxs are a mechanism of broadcasting cancelation to concurrent routines (aka tokio tasks).
/// The routines are expected to react to context cancelation on their own (they are not preempted)
/// and perform a graceful shutdown. A tokio task is expected to return immediately once the context
/// is cancelled (i.e. a task with canceled context, when polled in a loop should complete
/// eventually without requiring any wake()).
///
/// Ctx is similar to a rust lifetime but for runtime:
/// Ctx is expected to be passed down the call stack and to the spawned tokio subtasks.
/// At any level of the call stack the Ctx can be narrowed down via `Ctx::with_cancel` (which
/// allows the routine to cancel context of subroutines it spawns) or via
/// `Ctx::with_timeout`/`Ctx::with_deadline`, which cancels the scope automatically after a given time.
/// If is NOT possible to extend the context provided by the caller - a subtask is expected
/// to adhere to the lifetime of its context and finish as soon as it gets canceled (or earlier).
#[derive(Clone)]
pub(super) struct Ctx(Arc<Inner>);

impl Ctx {
    /// Constructs a new context:
    /// * without a parent
    /// * with real clock
    /// * with infinite deadline
    ///
    /// It should be called directly from main.rs.
    pub(crate) fn new(clock: near_async::time::Clock) -> Ctx {
        return Ctx(Arc::new(Inner {
            canceled: signal::Once::new(),
            deadline: near_async::time::Deadline::Infinite,
            clock,
            _parent: None,
        }));
    }

    pub fn cancel(&self) {
        self.0.canceled.send();
    }

    pub fn sub(&self, deadline: near_async::time::Deadline) -> Ctx {
        let child = Ctx(Arc::new(Inner {
            canceled: signal::Once::new(),
            clock: self.0.clock.clone(),
            deadline: std::cmp::min(self.0.deadline, deadline),
            _parent: Some(self.0.clone()),
        }));
        tokio::spawn({
            let clock = self.0.clock.clone();
            let deadline = child.0.deadline;
            let parent = self.0.canceled.clone();
            let child = child.0.canceled.clone();
            async move {
                tokio::select! {
                    _ = clock.sleep_until_deadline(deadline) => child.send(),
                    _ = parent.recv() => child.send(),
                    _ = child.recv() => {}
                }
            }
        });
        child
    }
}

/// Awaits for the current task to get canceled.
pub async fn canceled() {
    local().0.canceled.recv().await
}

/// Check if the context has been canceled.
pub fn is_canceled() -> bool {
    local().0.canceled.try_recv()
}

/// The time at which the local context will be canceled.
/// The current task should use it to schedule its work accordingly.
/// Remember that this is just a hint, because the local context
/// may get canceled earlier.
pub fn get_deadline() -> near_async::time::Deadline {
    local().0.deadline
}

pub(super) fn local() -> Ctx {
    CTX.with(|ctx| unsafe { &*ctx.get() }.clone())
}

impl Ctx {
    /// Awaits until f completes, or the context gets canceled.
    /// f is required to be cancellable.
    async fn wait<F: Future>(&self, f: F) -> OrCanceled<F::Output> {
        tokio::select! {
            v = f => Ok(v),
            _ = self.0.canceled.recv() => Err(ErrCanceled),
        }
    }
}

pub async fn wait<F: Future>(f: F) -> OrCanceled<F::Output> {
    local().wait(f).await
}

// TODO(gprusak): run_with_timeout, run_with_deadline, run_canceled, run_test all are expected
// to be awaited at the construction site, so perhaps they should be macros, similarly to
// scope::run!.

/// Equivalent to `with_deadline(now()+d,f)`.
pub fn run_with_timeout<F: Future>(
    d: near_async::time::Duration,
    f: F,
) -> impl Future<Output = F::Output> {
    let ctx = local();
    let ctx = ctx.sub((ctx.0.clock.now() + d).into());
    CtxFuture { ctx, inner: f }
}

/// Runs a future with a context restricted to be canceled at time `t`.
/// It could be emulated via `scope::run!` with a background subtask
/// returning an error after `sleep_until(t)`, but that would be more
/// expensive and other tasks won't see the deadline via `ctx::get_deadline()`.
pub fn run_with_deadline<F: Future>(
    t: near_async::time::Instant,
    f: F,
) -> impl Future<Output = F::Output> {
    let ctx = local().sub(t.into());
    CtxFuture { ctx, inner: f }
}

/// Executes the future in a context that has been already canceled.
/// Useful for tests (also outside of this crate).
pub fn run_canceled<F: Future>(f: F) -> impl Future<Output = F::Output> {
    let ctx = local().sub(near_async::time::Deadline::Infinite);
    ctx.cancel();
    CtxFuture { ctx, inner: f }
}

/// Executes the future with a given clock, which can be set to fake clock.
/// Useful for tests.
pub fn run_test<F: Future>(
    clock: near_async::time::Clock,
    f: F,
) -> impl Future<Output = F::Output> {
    CtxFuture { ctx: Ctx::new(clock), inner: f }
}

#[pin_project::pin_project]
pub(super) struct CtxFuture<F: Future> {
    #[pin]
    pub(super) inner: F,
    pub(super) ctx: Ctx,
}

impl<F: Future> Future for CtxFuture<F> {
    type Output = F::Output;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let this = self.project();
        let _guard = SetLocalCtx::new(this.ctx);
        this.inner.poll(cx)
    }
}
