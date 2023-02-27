#![cfg(not(doctest))]
//! Asynchronous scope.
//!
//! You can think of the scope as a lifetime 'env within a rust future, such that:
//! * within 'env you can spawn subtasks, which may return an error E.
//! * subtasks can spawn more subtasks.
//! * 'env has special semantics: at the end of 'env all spawned substasks are awaited for
//!    completion.
//! * if ANY of the subtasks returns an error, all the other subtasks are GRACEFULLY cancelled.
//!   It means that they are not just dropped, but rather they have a handle (aka Ctx) to be able
//!   to check at any time whether they were cancelled.
//!
//!     let (send,recv) = channel();
//!     ...
//!     'env: {
//!         spawn<'env>(async {
//!             recv.await
//!             Err(e)
//!         });
//!
//!         while !is_cancelled() {
//!             // do some useful async unit of work
//!         }
//!         // do some graceful cleanup.
//!         Ok(())
//!     }
//!
//! Since we cannot directly address lifetimes like that we simulate it via Scope and Ctx structs.
//! Ctx is hidden in the thread_local storage and is configured by Scope.
//! We cannot directly implement a function
//!   run : (Scope<'env> -> (impl 'env + Future)) -> (impl 'env + Future)
//! Because the compiler is not smart enough to deduce 'env for us.
//! Instead we first construct Scope<'env> explicitly, therefore fixing its lifetime,
//! and only then we pass a reference to it to another function.
//!
//!     let (send,recv) = channel();
//!     ...
//!     {
//!         let s = Scope<'env>::new();
//!         s.run(|s| async {
//!             s.spawn(async {
//!                 recv.await
//!                 Err(e)
//!             })
//!
//!             for !ctx::is_cancelled() {
//!                 // do some useful async unit of work
//!             }
//!             // do some graceful cleanup.
//!             Ok(())
//!         }).await
//!     }
//!
//! We wrap these 2 steps into a macro "run!" to hide this hack and avoid incorrect use.
use crate::concurrency::ctx;
use crate::concurrency::signal;
use futures::future::{BoxFuture, Future, FutureExt};
use near_primitives::time;
use std::borrow::Borrow;
use std::sync::{Arc, Weak, Mutex};

#[cfg(test)]
mod tests;

/// Passive representation of the scope.
/// This object may outlive the lifetime of the scope.
/// New tasks can be spawned in the scope, until the scope is terminated:
/// To spawn a task on a scope, you need a
struct Inner<E> {
    /// Context of this scope.
    /// All tasks spawned in this scope are provided with this context.
    ctx: ctx::Ctx,
    /// First error returned by any task in the scope.
    err: Mutex<Option<E>>,
    /// Signal sent once the scope is terminated.
    terminated: signal::Once,
}

impl<E> Inner<E> { 
    fn err_take(&self) -> Option<E> {
        std::mem::take(&mut *self.err.lock().unwrap())
    }
}

impl<E:Clone> Inner<E> {
    fn err_clone(&self) -> Option<E> {
        self.err.lock().unwrap().clone()
    }
}

/// Internal representation of a scope.
struct TerminateGuard<E: 'static>(Arc<Inner<E>>);

impl<E: 'static> Drop for TerminateGuard<E> {
    fn drop(&mut self) { self.0.terminated.send(); }
}

impl<E: 'static + Send> TerminateGuard<E> {
    pub fn new(ctx: &ctx::Ctx) -> Self {
        Self(Arc::new(Inner {
            ctx: ctx.sub(time::Deadline::Infinite), 
            err: Mutex::new(None),
            terminated: signal::Once::new(),
        }))
    }

    fn register(&self, err: E) {
        let mut m = self.0.err.lock().unwrap();
        if m.is_some() { return; }
        *m = Some(err);
        self.0.ctx.cancel();
    }

    /// Spawns a task in the scope, which owns a reference of to the scope,
    /// so that scope doesn't terminate before all tasks are completed.
    ///
    /// The reference to the scope can be either `Arc<TerminateGuard>` or `Arc<CancelGuard>`,
    /// so that the scope may get terminated/canceled when the guard is dropped.
    ///
    /// Returns a handle to the task, which awaits either for the task to return succesfully,
    /// or for the WHOLE scope to terminate.
    fn spawn<M: 'static + Send + Sync + Borrow<Self>, T: 'static + Send>(
        m: Arc<M>,
        f: impl 'static + Send + Future<Output = Result<T, E>>,
    ) -> tokio::task::JoinHandle<Result<T, Arc<Inner<E>>>> {
        tokio::spawn(must_complete(async move {
            match (ctx::CtxFuture { ctx: m.as_ref().borrow().0.ctx.clone(), inner: f }).await {
                Ok(v) => Ok(v),
                Err(err) => {
                    let m = m.as_ref().borrow();
                    m.register(err);
                    let inner = m.0.clone();
                    drop(m);
                    inner.terminated.recv().await;
                    Err(inner)
                }
            }
        }))
    }

    /// Spawns a new service in the scope.
    ///
    /// A service is a scope, which gets canceled when
    /// its handler (`Service`) is dropped. Service belongs to a scope, in a sense that
    /// a dedicated task is spawned on the scope which awaits for service to terminate and
    /// returns the service's result.
    pub fn new_service<E2:'static+Send>(self: Arc<Self>) -> Service<E2> {
        let sub = Arc::new(TerminateGuard::new(&self.0.ctx));
        let service = Service(Arc::downgrade(&sub), sub.0.clone());
        // Spawn a guard task in `self` scope, so that it is not terminated
        // before `sub` scope is.
        TerminateGuard::spawn(self, async move {
            let sub_inner = sub.0.clone();
            // Spawn a guard task in `sub` scope, so that it is not terminated
            // until its context is not canceled. See `Service` for a list
            // of events canceling the Service.
            TerminateGuard::spawn(sub, async move { Ok(ctx::canceled().await) });
            sub_inner.terminated.recv().await;
            Ok(())
        });
        service
    }
}

/// Error returned `Service::spawn()` and `Service::new_service()`
/// when spawning new things on the service scope is not allowed, because the
/// service has been already terminated. Returned by `JoinHandle::join` when
/// the task has returned an error and therefore the service has been terminated.
#[derive(thiserror::Error, Debug)]
#[error("service has been terminated")]
pub struct ErrTerminated;

/// A service is a subscope which doesn't keep the scope
/// alive, i.e. if all tasks spawned via `Scope::spawn` complete, the scope will
/// be cancelled (even though tasks in a service may be still running).
///
/// Note however that the scope won't be terminated until the tasks of the service complete.
/// Service is cancelled when the handle is dropped, so make sure to store it somewhere.
/// Service is cancelled when ANY of the tasks/services in the service returns an error.
/// Service is cancelled when the parent scope/service is cancelled.
/// Service is NOT cancelled just when all tasks within the service complete - in particular
/// a newly started service has no tasks.
/// Service is terminated when it is cancelled AND all tasks within the service complete.
pub struct Service<E: 'static>(Weak<TerminateGuard<E>>,Arc<Inner<E>>);

impl<E: 'static> Drop for Service<E> {
    fn drop(&mut self) { self.1.ctx.cancel(); }
}

impl<E: 'static + Send> Service<E> {
    /// Checks if the referred scope has been terminated.
    pub fn is_terminated(&self) -> bool {
        self.1.terminated.try_recv()
    }

    pub async fn terminate(&self) -> ctx::OrCanceled<()> {
        self.1.ctx.cancel();
        self.terminated().await
    }

    pub async fn terminated(&self) -> ctx::OrCanceled<()> {
        ctx::wait(self.1.terminated.recv()).await
    }

    /// Spawns a task in this scope.
    ///
    /// Returns ErrTerminated if the scope has already terminated.
    pub fn spawn<T: 'static + Send>(
        &self,
        f: impl 'static + Send + Future<Output = Result<T, E>>,
    ) -> Result<JoinHandle<'static, T, E>, ErrTerminated> {
        match self.0.upgrade().map(|m| TerminateGuard::spawn(m, f)) {
            Some(h) => Ok(JoinHandle(h, std::marker::PhantomData)),
            None => Err(ErrTerminated),
        }
    }

    /// Spawns a service in this scope.
    ///
    /// Returns ErrTerminated if the scope has already terminated.
    pub fn new_service<E2:'static+Send>(&self) -> Result<Service<E2>,ErrTerminated> {
        self.0.upgrade().map(|m| m.new_service()).ok_or(ErrTerminated)
    }
}

impl<E:'static+Send+Clone> Service<E> {
    pub async fn terminate_err(&self) -> ctx::OrCanceled<Result<(),E>> {
        self.1.ctx.cancel();
        self.terminated_err().await
    }

    pub async fn terminated_err(&self) -> ctx::OrCanceled<Result<(),E>> {
        ctx::wait(async {
            self.1.terminated.recv().await;
            match self.1.err_clone() {
                None => Ok(()),
                Some(err) => Err(err),
            }
        }).await
    }
}

/// Wrapper of a scope reference which cancels the scope when dropped.
///
/// Used by Scope to cancel the scope as soon as all tasks spawned via
/// `Scope::spawn` complete.
struct CancelGuard<E: 'static>(Arc<TerminateGuard<E>>);

impl<E: 'static> Borrow<TerminateGuard<E>> for CancelGuard<E> {
    fn borrow(&self) -> &TerminateGuard<E> { &*self.0 }
}

impl<E: 'static> Drop for CancelGuard<E> {
    fn drop(&mut self) { self.0.0.ctx.cancel(); }
}

/// Represents a task that can be joined by another task within Scope<'env>.
/// We do not support awaiting for tasks outside of the scope, to simplify
/// the concurrency model (you can still implement a workaround by using a channel,
/// if you really want to, but that might mean that Scope is not what you want
/// in the first place).
pub struct JoinHandle<'env, T, E>(
    tokio::task::JoinHandle<Result<T, Arc<Inner<E>>>>,
    std::marker::PhantomData<fn(&'env ()) -> &'env ()>,
);

impl<'env, T, E> JoinHandle<'env, T, E> {
    /// Cancel-safe.
    async fn join_raw(self) -> Result<T, ErrTerminated> {
        self.0.await.unwrap().map_err(|_|ErrTerminated)
    }

    /// Awaits for a task to be completed and returns the result.
    /// Returns Err(ErrCanceled) if the awaiting (current task) has been canceled.
    /// Returns Ok(Err(ErrTerminated)) if the awaited (joined task) has been canceled.
    pub async fn join(self) -> ctx::OrCanceled<Result<T, ErrTerminated>> {
        ctx::wait(self.join_raw()).await
    }
}

impl<'env, T, E:Clone> JoinHandle<'env, T, E> {
    /// Cancel-safe.
    async fn join_err_raw(self) -> Result<T, E> {
        self.0.await.unwrap().map_err(|inner|inner.err_clone().unwrap())
    }

    pub async fn join_err(self) -> ctx::OrCanceled<Result<T, E>> {
        ctx::wait(self.join_err_raw()).await
    }
}

/// Scope represents a concurrent computation bounded by lifetime 'env.
///
/// It should be created only via `run!` macro.
/// Scope is cancelled when the provided context is cancelled.
/// Scope is cancelled when any of the tasks in the scope returns an error.
/// Scope is cancelled when all the tasks in the scope complete.
/// Scope is terminated when it is cancelled AND all tasks in the scope complete.
pub struct Scope<'env, E: 'static>(
    /// Scope is equivalent to a strong service, but bounds
    Weak<CancelGuard<E>>,
    Weak<TerminateGuard<E>>,
    /// Makes Scope<'env,E> invariant in 'env.
    std::marker::PhantomData<fn(&'env ()) -> &'env ()>,
);

unsafe fn to_static<'env, T>(f: BoxFuture<'env, T>) -> BoxFuture<'static, T> {
    std::mem::transmute::<BoxFuture<'env, _>, BoxFuture<'static, _>>(f)
}

impl<'env, E: 'static + Send> Scope<'env, E> {
    /// Spawns a "main" task in the scope.
    /// Scope gets canceled as soon as all the "main" tasks complete.
    pub fn spawn<T: 'static + Send>(
        &self,
        f: impl 'env + Send + Future<Output = Result<T, E>>,
    ) -> JoinHandle<'env, T, E> {
        match self.0.upgrade() {
            Some(inner) => JoinHandle(
                TerminateGuard::spawn(inner, unsafe { to_static(f.boxed()) }),
                std::marker::PhantomData,
            ),
            // Upgrade may fail only if all the "main" tasks have already completed
            // so the caller is a "background" task. In that case we fall back
            // to spawning a "background" task instead. It is ok, since the distinction
            // between main task and background task disappears, once the scope is canceled.
            None => self.spawn_bg(f),
        }
    }

    /// Spawns a "background" task in the scope.
    /// It behaves just like a single-task Service, but
    /// has the same lifetime as the Scope, so it can spawn
    /// more tasks in the scope. It is not a "main" task, so
    /// it doesn't prevent scope cancelation.
    pub fn spawn_bg<T: 'static + Send>(
        &self,
        f: impl 'env + Send + Future<Output = Result<T, E>>,
    ) -> JoinHandle<'env, T, E> {
        JoinHandle(
            TerminateGuard::spawn(self.1.upgrade().unwrap(), unsafe { to_static(f.boxed()) }),
            std::marker::PhantomData,
        )
    }

    /// Spawns a service.
    ///
    /// Returns a handle to the service, which allows spawning new tasks within the service.
    pub fn new_service(&self) -> Service<E> {
        self.1.upgrade().unwrap().new_service()
    }
}

/// must_complete wraps a future, so that it aborts if it is dropped before completion.
///
/// Possibility that a future can be dropped/aborted at every await makes the control flow unnecessarily complicated.
/// In fact, only few basic futures (like io primitives) actually need to be abortable, so
/// that they can be put together into a tokio::select block. All the higher level logic
/// would greatly benefit (in terms of readability and bug-resistance) from being non-abortable.
/// Rust doesn't support linear types as of now, so best we can do is a runtime check.
fn must_complete<Fut: Future>(fut: Fut) -> impl Future<Output = Fut::Output> {
    let guard = MustCompleteGuard;
    async move {
        let res = fut.await;
        let _ = std::mem::ManuallyDrop::new(guard);
        res
    }
}

struct MustCompleteGuard;

impl Drop for MustCompleteGuard {
    fn drop(&mut self) {
        // We always abort here, no matter if compiled with panic=abort or panic=unwind.
        eprintln!("dropped a non-abortable future before completion");
        eprintln!("backtrace:\n{}", std::backtrace::Backtrace::force_capture());
        std::process::abort();
    }
}

/// Should be used only via run! macro.
#[doc(hidden)]
pub mod internal {
    use super::*;

    pub fn new_scope<'env, E: 'static>() -> Scope<'env, E> {
        Scope(Weak::new(), Weak::new(), std::marker::PhantomData)
    }

    pub async fn run<'env, E, T, F, Fut>(scope: &'env mut Scope<'env, E>, f: F) -> Result<T, E>
    where
        E: 'static + Send,
        T: 'static + Send,
        F: 'env + FnOnce(&'env Scope<'env, E>) -> Fut,
        Fut: 'env + Send + Future<Output = Result<T, E>>,
    {
        must_complete(async move {
            let guard = Arc::new(CancelGuard(Arc::new(TerminateGuard::new(&ctx::local()))));
            scope.0 = Arc::downgrade(&guard);
            scope.1 = Arc::downgrade(&guard.0);
            let task = scope.spawn(f(scope));
            let inner = guard.0.0.clone();
            // each task spawned on `scope` keeps its own reference to `guard` or `guard.0`.
            // As soon as all references to `guard` are dropped, scope will be cancelled.
            drop(guard);
            inner.terminated.recv().await;
            match inner.err_take() {
                Some(err) => Err(err),
                None => Ok(task.join_raw().await.unwrap()),
            }
        })
        .await
    }
}

/// A future running a task within a scope (see `Scope`).
///
/// `await` is called within the macro instantiation, so `run!` can be called only in an async context.
/// Dropping this future while incomplete will ABORT (not panic, since the future is not
/// UnwindSafe).
/// Note that immediate-await doesn't prevent dropping the future, as the outer future still can be dropped.
#[macro_export]
macro_rules! run {
    ($f:expr) => {{
        $crate::concurrency::scope::internal::run(
            // We pass a created scope via argument (rather than construct it within `run()`
            // So that rust compiler fixes the lifespan of the Scope, rather than trying to
            // reason about it - which is not smart enough to do.
            &mut $crate::concurrency::scope::internal::new_scope(),
            $f,
        )
        .await
    }};
}

pub use run;
