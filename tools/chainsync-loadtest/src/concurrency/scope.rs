use crate::concurrency::{ctx, Ctx, CtxWithCancel};
use std::future::Future;
use std::sync::{Arc, Mutex};

// WaitGroup is an atomic counter which can be awaited to become 0.
pub(super) struct WaitGroup {
    count: Mutex<u64>,
    empty: tokio::sync::Notify,
}

impl WaitGroup {
    pub fn new(init: u64) -> WaitGroup {
        return WaitGroup { count: Mutex::new(init), empty: tokio::sync::Notify::new() };
    }

    // wait() awaits for the WaitGroup to become empty.
    // With the current implementation, it will complete
    // even if WaitGroup gets empty BEFORE await but AFTER wait()
    // (future construction).
    // TODO: change if it becomes a problem, but I (gprusak) think it
    // is desirable.
    pub fn wait(&self) -> impl Future<Output = ()> + Send + '_ {
        let count = self.count.lock().unwrap();
        let n = self.empty.notified();
        let is_empty = *count == 0;
        async move {
            if is_empty {
                return;
            } else {
                n.await;
            }
        }
    }
    pub fn inc(&self) {
        *self.count.lock().unwrap() += 1;
    }
    pub fn dec(&self) {
        let mut count = self.count.lock().unwrap();
        *count -= 1;
        if *count != 0 {
            return;
        }
        self.empty.notify_waiters();
    }
}

struct ScopeState {
    err: Option<anyhow::Error>,
    cancelled: bool,
}

// Scope represents a collection of spawned futures with a common bounded lifetime.
// See Scope::run for details.
pub struct Scope {
    ctx: CtxWithCancel,
    // Main futures have to exit before the scope is cancelled.
    main_futures: WaitGroup,
    // Additional futures running in the background,
    // which will be cancelled as soon as main_futures exit.
    weak_futures: WaitGroup,
    state: Mutex<ScopeState>,
}

impl Scope {
    fn complete(&self, v: anyhow::Result<()>) {
        let e = if let Err(e) = v {
            e
        } else {
            return;
        };
        let mut s = self.state.lock().unwrap();
        if s.err.is_some() {
            return;
        }
        // Ignore Cancelled errors after the whole scope has been cancelled.
        if s.cancelled
            && e.downcast_ref::<ctx::Error>().map(|e| e == &ctx::Error::Cancelled).unwrap_or(false)
        {
            return;
        }
        s.err = Some(e);
        s.cancelled = true;
        self.ctx.cancel();
    }

    // Spawn a "main" future in the scope.
    pub fn spawn<F>(self: &Arc<Self>, f: impl Send + FnOnce(Ctx, Arc<Self>) -> F)
    where
        F: Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        let fut = f((*self.ctx).clone(), self.clone());
        let s = self.clone();
        s.main_futures.inc();
        tokio::spawn(async move {
            s.complete(fut.await);
            s.main_futures.dec();
        });
    }

    // Spawn a "weak" future in the scope.
    pub fn spawn_weak<F>(self: &Arc<Self>, f: impl Send + FnOnce(Ctx) -> F)
    where
        F: Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        let fut = f((*self.ctx).clone());
        let s = self.clone();
        s.weak_futures.inc();
        tokio::spawn(async move {
            s.complete(fut.await);
            s.weak_futures.dec();
        });
    }

    // Scope represents a collection of futures executed concurrently on a tokio runtime, which share a context.
    // Scope allows for cooperative execution, therefore all coroutines are allowed to run as long
    // as they want. If you cancel the context, the futures will get notified (see documentation of
    // Ctx) but won't get preempted: they will be able to shutdown gracefully.
    //
    // run() is a lambda-abstraction of the Scope:
    // 1. it creates a new scope, spawns f(ctx,scope) and adds it to the scope.
    // 2. This first future of the scope is allowed to spawn additional futures in the scope.
    // 3. run() is blocking until all members of the scope complete.
    //
    // Since from the point of view of the caller of Scope::run, it is just a blocking call, which
    // might spawn coroutines internally, but all of them will complete before Scope::run returns,
    // therefore Scope::run allows for expressing STRUCTURED CONCURRENCY.
    //
    // Additional details:
    // - Scope doesn't have a public constructor - it can only be constructed via Scope::run, for
    // encapsulation.
    // - All the futures of the scope are expected to complete successfully. If any of the futures
    // returns an error, the scope context will get cancelled, all still-ongoing scope futures will
    // be notified and are expected to shutdown gracefully. The first error encountered will be returned.
    // - Scope supports 2 methods to spawn futures: spawn() which spawns "main" futures and
    // spawn_weak(), which spawns "weak" futures. "main" futures are allowed to spawn additional
    // futures, "weak" futures are not. Scope will wait for all "main" to complete, then it will
    // cancel the context and wait for the "weak" futures to complete.
    // TODO: consider renaming "main and weak" to "foreground and background".
    //
    // Missing features:
    // - Even though the spawned futures are guaranteed to have a bounded lifetime, I failed so
    // far to express that reliably in rust lifetimes, therefore for now all futures are required
    // to be 'static.
    // - As a consequence the Scope object lifetime is also 'static, so there is no compile-time
    // protection from having scope outlive the run() call.
    // - There is no compile-time protection from having "weak" futures spawn stuff. Any
    // suggestions how to achieve that are welcome.
    pub async fn run<F, T>(ctx: &Ctx, f: impl FnOnce(Ctx, Arc<Scope>) -> F) -> anyhow::Result<T>
    where
        F: Future<Output = anyhow::Result<T>> + Send + 'static,
    {
        let ctx = ctx.with_cancel();
        let s = Arc::new(Scope {
            ctx: ctx.clone(),
            main_futures: WaitGroup::new(0),
            weak_futures: WaitGroup::new(0),
            state: Mutex::new(ScopeState { err: None, cancelled: false }),
        });
        let res = match f((*ctx).clone(), s.clone()).await {
            Err(e) => {
                s.complete(Err(e));
                None
            }
            Ok(v) => Some(v),
        };
        // Wait for the main_futures.
        s.main_futures.wait().await;
        // Cancel the scope.
        {
            let mut state = s.state.lock().unwrap();
            s.ctx.cancel();
            state.cancelled = true;
        }
        // Wait for the weak_futures.
        s.weak_futures.wait().await;
        // TODO: add runtime verification that weak futures do not spawn anything,
        // or at least that no strong futures are spawned after cancellation.
        let mut state = s.state.lock().unwrap();
        match state.err.take() {
            Some(e) => Err(e),
            None => Ok(res.unwrap()),
        }
    }
}
