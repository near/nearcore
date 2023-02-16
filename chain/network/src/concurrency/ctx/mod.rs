use crate::concurrency::signal;
use crate::time;
use std::future::Future;
use std::sync::Arc;

#[cfg(test)]
mod tests;

/// Inner representation of a context.
struct Inner {
    canceled: signal::Once,
    deadline: time::Deadline,
    clock: time::Clock,
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
pub struct Ctx(Arc<Inner>);

impl Ctx {
    /// Constructs a new context:
    /// * without a parent
    /// * with real clock
    /// * with infinite deadline
    ///
    /// It should be called directly from main.rs.
    pub fn inf() -> Ctx {
        return Ctx(Arc::new(Inner {
            canceled: signal::Once::new(),
            deadline: time::Deadline::Infinite,
            clock: time::Clock::real(),
            _parent: None,
        }));
    }

    pub fn clock(&self) -> &time::Clock {
        &self.0.clock
    }

    fn cancel(&self) {
        self.0.canceled.send();
    }

    /// Check if the context has been canceled.
    pub fn is_canceled(&self) -> bool {
        self.0.canceled.try_recv()
    }

    /// Awaits for the context to get canceled.
    ///
    /// Cancellable (in the rust sense).
    pub fn canceled(&self) -> impl Future<Output = ()> + '_ {
        self.0.canceled.recv()
    }

    /// Awaits until f completes, or the context gets canceled.
    /// f is required to be cancellable.
    ///
    /// Cancellable.
    pub async fn wait<F, T>(&self, f: F) -> OrCanceled<T>
    where
        F: std::future::Future<Output = T>,
    {
        tokio::select! {
            v = f => Ok(v),
            _ = self.0.canceled.recv() => Err(ErrCanceled),
        }
    }

    pub fn with_deadline(&self, deadline: time::Deadline) -> Ctx {
        let child = Ctx(Arc::new(Inner {
            canceled: signal::Once::new(),
            clock: self.clock().clone(),
            deadline: std::cmp::min(self.0.deadline, deadline),
            _parent: Some(self.0.clone()),
        }));
        tokio::spawn({
            let clock = self.clock().clone();
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

    /// Creates a child context which can be manually canceled.
    ///
    /// If the parent context gets canceled, the child will also be canceled,
    /// but not vice versa (you cannot affect the parent context).
    pub fn with_cancel(&self) -> CtxWithCancel {
        return CtxWithCancel(self.with_deadline(time::Deadline::Infinite));
    }

    /// Same as `with_deadline()` but you provide a duration,
    /// rather than an instant.
    pub fn with_timeout(&self, timeout: time::Duration) -> Ctx {
        return self.with_deadline((self.0.clock.now() + timeout).into());
    }
}

#[derive(Clone)]
pub struct CtxWithCancel(Ctx);

impl std::ops::Deref for CtxWithCancel {
    type Target = Ctx;
    fn deref(&self) -> &Ctx {
        return &self.0;
    }
}

impl CtxWithCancel {
    pub fn cancel(&self) {
        self.0.cancel();
    }
}
