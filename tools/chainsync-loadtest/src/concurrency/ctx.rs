use crate::concurrency::Once;
use std::fmt;
use std::ops;
use std::sync::Arc;
use tokio::time;

// Error represents an error that Context can return.
#[derive(Debug, Clone, PartialEq)]
pub enum Error {
    /// Context (or one of the ancestors) has timed out.
    Timeout,
    /// Context (or one of the ancestors) has been manually cancelled.
    Cancelled,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(match self {
            Self::Timeout => "ctx::Error::Timeout: context or one of the ancestors has timed out",
            Self::Cancelled => {
                "ctx::Error::Cancelled: context or one of the ancestors has been cancelled"
            }
        })
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        return None;
    }
}

struct Ctx_ {
    label: Option<String>,
    done: Arc<Once<Error>>,
    deadline: Option<time::Instant>,
    parent: Option<Arc<Ctx_>>,
}

impl Drop for Ctx_ {
    fn drop(&mut self) {
        let _ = self.done.set(Error::Cancelled);
    }
}

// Ctx is an implementation of https://pkg.go.dev/context.
// Ctxs are a mechanism of broadcasting cancellation to concurrent routines (aka futures).
// The routines are expected to react to context cancellation on their own (they are not preempted)
// and perform a graceful shutdown. In general the graceful shutdown is expected to
// be non-blocking, but it still can be asynchronous (if it is known that an async call will
// be non-blocking as well). Ctx is similar to a rust lifetime but for runtime:
// Ctx is expected to be passed down the call stack and to the spawned subroutines.
// At any level of the call stack the Ctx can be narrowed down via Ctx::with_cancel (which
// allows the routine to cancel context of subroutines it spawns) or via
// Ctx::with_timeout/Ctx::with_deadline, which cancels the scope automatically after a given time.
// If is NOT possible to extend the context given by the parent - a subroutine is expected
// to adhere to the lifetime it represents and finish as soon as it gets cancelled (or earlier).
#[derive(Clone)]
pub struct Ctx(Arc<Ctx_>);

impl std::fmt::Debug for Ctx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.parent.as_ref().map(|p| Ctx(p.clone()).fmt(f));
        self.0.label.as_ref().map(|l| f.write_fmt(format_args!("::{}", &l)));
        Ok(())
    }
}

impl Ctx {
    // background() returns a top level context equivalent to 'static lifetime.
    // It should be called directly from main.rs.
    pub fn background() -> Ctx {
        return Ctx(Arc::new(Ctx_ {
            label: None,
            parent: None,
            deadline: None,
            done: Arc::new(Once::new()),
        }));
    }

    fn cancel(&self) {
        let _ = self.0.done.set(Error::Cancelled);
    }

    // err() returns the Error if the Context was already cancelled,
    // None otherwise.
    pub fn err(&self) -> Option<Error> {
        self.0.done.get()
    }

    // done() waits until the context gets cancelled and returns the Error.
    pub async fn done(&self) -> Error {
        self.0.done.wait().await
    }

    // wrap() executes the future f to completion, or until the context gets cancelled.
    // This function is expected to be called only by the low level code, which
    // wraps context-unaware code (context-aware code is expected to take ctx as an argument
    // instead).
    pub async fn wrap<F, T>(&self, f: F) -> Result<T, Error>
    where
        F: std::future::Future<Output = T>,
    {
        tokio::select! {
            v = f => Ok(v),
            e = self.0.done.wait() => Err(e),
        }
    }

    // wait_until() wraps tokio::time::sleep_until. Returns Ok(()) if deadline
    // has passed, or Error if the context got cancelled.
    pub async fn wait_until(&self, deadline: time::Instant) -> Result<(), Error> {
        self.wrap(time::sleep_until(deadline)).await
    }

    // wait() wraps tokio::time::sleep (see also wait_until()).
    pub async fn wait(&self, duration: time::Duration) -> Result<(), Error> {
        self.wrap(time::sleep(duration)).await
    }

    fn new_child(&self, label: Option<String>, deadline: Option<time::Instant>) -> Ctx {
        let deadline = match (deadline, self.0.deadline) {
            (Some(cd), Some(pd)) => Some(std::cmp::min(cd, pd)),
            (None, pd) => pd,
            (cd, None) => cd,
        };
        let done = Arc::new(Once::<Error>::new());
        tokio::spawn({
            let parent_done = self.0.done.clone();
            let child_done = done.clone();
            async move {
                let deadline_wait = async {
                    if let Some(d) = deadline {
                        time::sleep_until(d).await
                    } else {
                        std::future::pending().await
                    }
                };
                tokio::select! {
                    _ = deadline_wait => { let _ = child_done.set(Error::Timeout); }
                    e = parent_done.wait() => { let _ = child_done.set(e); }
                    _ = child_done.wait() => {}
                }
            }
        });
        return Ctx(Arc::new(Ctx_ { label, parent: Some(self.0.clone()), done, deadline }));
    }

    // with_cancel creates a child context which can be manually cancelled.
    // If the parent context gets cancelled, the child will also be cancelled,
    // but not vice versa (you cannot affect the parent context).
    pub fn with_cancel(&self) -> CtxWithCancel {
        return CtxWithCancel(self.new_child(None, None));
    }

    // with_deadline() creates a child context which will be automatically cancelled,
    // once deadline passes (or parent context gets cancelled).
    pub fn with_deadline(&self, deadline: tokio::time::Instant) -> Ctx {
        return self.new_child(None, Some(deadline));
    }

    // with_label() is a debugging utility which allows to create child context
    // with a custom label. The path of labels of the ancestors is returned
    // in the Debug implementation for Ctx.
    pub fn with_label(&self, label: &str) -> Ctx {
        return self.new_child(Some(label.to_string()), None);
    }

    // with_timeout() is the same as with_deadline() but you provide a duration,
    // rather than an instant.
    pub fn with_timeout(&self, timeout: time::Duration) -> Ctx {
        return self.with_deadline(time::Instant::now() + timeout);
    }
}

#[derive(Clone)]
pub struct CtxWithCancel(Ctx);

impl ops::Deref for CtxWithCancel {
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
