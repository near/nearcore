//! Deadline watchdog for blocking compiler-daemon IPC.

use parking_lot::{Condvar, Mutex};
use std::io;
use std::process::{Child, ExitStatus};
use std::sync::Arc;
use std::thread::{Builder, JoinHandle, sleep};
use std::time::{Duration, Instant};

/// Why the parent asked a worker to terminate.
///
/// This is stored alongside the child handle so observing a natural exit and
/// deciding to kill the child are one serialized transition. The first parent
/// termination request wins.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum TerminationReason {
    WatchdogTimeout { phase: &'static str, timeout: Duration },
    ProtocolCleanup,
    ProcessDrop,
}

#[derive(Debug, Eq, PartialEq)]
pub(super) enum WatchdogError {
    Timeout { phase: &'static str, timeout: Duration },
    Operation(String),
}

struct ProcessState {
    child: Child,
    status: Option<ExitStatus>,
    termination_reason: Option<TerminationReason>,
}

/// Shared control plane for a worker process.
///
/// Pipe I/O is deliberately not kept here: a watchdog may request termination
/// without contending with a lease blocked on an IPC operation.
pub(super) struct ProcessControl {
    state: Mutex<ProcessState>,
}

impl ProcessControl {
    pub(super) fn new(child: Child) -> Self {
        Self { state: Mutex::new(ProcessState { child, status: None, termination_reason: None }) }
    }

    pub(super) fn id(&self) -> u32 {
        self.state.lock().child.id()
    }

    /// Observe and reap a natural exit if one is already available.
    pub(super) fn try_status(&self) -> io::Result<Option<ExitStatus>> {
        let mut state = self.state.lock();
        if state.status.is_none() {
            state.status = state.child.try_wait()?;
        }
        Ok(state.status)
    }

    /// Mark a parent termination before issuing the kill.
    ///
    /// A natural exit already observable at the serialized transition takes
    /// precedence. Otherwise the first parent reason wins, even if the child
    /// exits naturally immediately before the kill reaches the kernel.
    pub(super) fn terminate(&self, reason: TerminationReason) -> io::Result<Option<ExitStatus>> {
        let mut state = self.state.lock();
        if state.status.is_none() {
            state.status = state.child.try_wait()?;
        }
        if state.status.is_some() {
            return Ok(state.status);
        }
        if state.termination_reason.is_none() {
            state.termination_reason = Some(reason);
        }
        match state.child.kill() {
            Ok(()) => Ok(None),
            Err(err) => {
                // The process may have exited between try_wait and kill. Keep
                // its real status when that race is observable.
                state.status = state.child.try_wait()?;
                if state.status.is_some() { Ok(state.status) } else { Err(err) }
            }
        }
    }

    pub(super) fn termination_reason(&self) -> Option<TerminationReason> {
        self.state.lock().termination_reason.clone()
    }

    /// Poll for a bounded amount of time so teardown cannot indefinitely block
    /// a compiler caller on a misbehaving child.
    pub(super) fn wait_for_exit(&self, timeout: Duration) -> io::Result<Option<ExitStatus>> {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(status) = self.try_status()? {
                return Ok(Some(status));
            }
            if Instant::now() >= deadline {
                return Ok(None);
            }
            sleep(Duration::from_millis(10));
        }
    }

    /// Used only by a detached last-resort reaper after bounded supervision has
    /// expired. No compiler caller waits for this operation.
    pub(super) fn reap(&self) -> io::Result<ExitStatus> {
        let mut state = self.state.lock();
        if let Some(status) = state.status {
            return Ok(status);
        }
        let status = state.child.wait()?;
        state.status = Some(status);
        Ok(status)
    }
}

#[derive(Default)]
struct WatchdogState {
    deadline: Option<(Instant, &'static str, Duration)>,
    timed_out: Option<(&'static str, Duration)>,
    shutdown: bool,
}

#[derive(Default)]
struct SharedState {
    state: Mutex<WatchdogState>,
    changed: Condvar,
}

pub(super) struct ProcessWatchdog {
    shared: Arc<SharedState>,
    thread: Option<JoinHandle<()>>,
}

impl ProcessWatchdog {
    pub(super) fn spawn(control: Arc<ProcessControl>) -> io::Result<Self> {
        let shared = Arc::new(SharedState::default());
        let watchdog_shared = Arc::clone(&shared);
        let thread =
            Builder::new().name("compiler-daemon-watchdog".to_owned()).spawn(move || {
                watchdog_loop(control, watchdog_shared);
            })?;
        Ok(Self { shared, thread: Some(thread) })
    }

    pub(super) fn arm(&self, timeout: Duration, phase: &'static str) {
        let deadline = Instant::now() + timeout;
        let mut state = self.shared.state.lock();
        state.deadline = Some((deadline, phase, timeout));
        state.timed_out = None;
        self.shared.changed.notify_one();
    }

    /// Disarm synchronously before returning the worker to the pool, so a
    /// timeout for its previous request cannot race with its next user.
    pub(super) fn finish<T>(&self, result: Result<T, String>) -> Result<T, WatchdogError> {
        let mut state = self.shared.state.lock();
        state.deadline = None;
        self.shared.changed.notify_one();
        if let Some((phase, timeout)) = state.timed_out.take() {
            return Err(WatchdogError::Timeout { phase, timeout });
        }
        result.map_err(WatchdogError::Operation)
    }

    pub(super) fn shutdown(&mut self) {
        let mut guard = self.shared.state.lock();
        guard.shutdown = true;
        self.shared.changed.notify_one();
        drop(guard);

        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

impl Drop for ProcessWatchdog {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn watchdog_loop(control: Arc<ProcessControl>, shared: Arc<SharedState>) {
    let mut state = shared.state.lock();
    while !state.shutdown {
        match state.deadline {
            Some((deadline, phase, timeout)) if Instant::now() >= deadline => {
                state.deadline = None;
                // Keep the watchdog state locked through the terminal transition
                // so finish and a subsequent arm cannot race with an old timeout.
                // A natural exit already observable by ProcessControl wins.
                let result =
                    control.terminate(TerminationReason::WatchdogTimeout { phase, timeout });
                if !matches!(result, Ok(Some(_))) {
                    state.timed_out = Some((phase, timeout));
                }
            }
            Some((deadline, _, _)) => {
                shared.changed.wait_until(&mut state, deadline);
            }
            None => shared.changed.wait(&mut state),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ProcessWatchdog, SharedState};
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    #[test]
    fn finish_then_rearm() {
        // Leave the background thread out so we can inspect the state it would
        // observe without racing the scheduler or relying on sleeps. In
        // particular, finish must disarm synchronously, not ask the thread to do it.
        let watchdog = ProcessWatchdog { shared: Arc::new(SharedState::default()), thread: None };
        let first_timeout = Duration::ZERO;
        watchdog.arm(first_timeout, "first request");
        let first_deadline = watchdog.shared.state.lock().deadline.unwrap().0;
        assert!(first_deadline <= Instant::now());
        assert_eq!(watchdog.finish(Ok(1)), Ok(1));
        {
            let state = watchdog.shared.state.lock();
            assert_eq!(state.deadline, None);
            assert_eq!(state.timed_out, None);
        }

        // Reuse the same watchdog after the first deadline has passed. Only the
        // new deadline may be visible to the thread, and finish must not report
        // a timeout inherited from the previous request.
        let second_timeout = Duration::from_secs(60);
        let before_rearm = Instant::now();
        watchdog.arm(second_timeout, "second request");
        {
            let state = watchdog.shared.state.lock();
            assert!(state.deadline.unwrap().0 >= before_rearm + second_timeout);
            assert_eq!(state.timed_out, None);
        }
        assert_eq!(watchdog.finish(Ok(2)), Ok(2));
        assert_eq!(watchdog.shared.state.lock().deadline, None);
    }
}
