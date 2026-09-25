//! Deadline watchdog for blocking compiler-daemon IPC.

use parking_lot::{Condvar, Mutex};
use std::io;
use std::process::Child;
use std::sync::Arc;
use std::thread::{Builder, JoinHandle};
use std::time::{Duration, Instant};

#[derive(Default)]
struct WatchdogState {
    deadline: Option<Instant>,
    timed_out: bool,
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
    pub(super) fn spawn(child: Arc<Mutex<Child>>) -> io::Result<Self> {
        let shared = Arc::new(SharedState::default());
        let watchdog_shared = Arc::clone(&shared);
        let thread =
            Builder::new().name("compiler-daemon-watchdog".to_owned()).spawn(move || {
                watchdog_loop(child, watchdog_shared);
            })?;
        Ok(Self { shared, thread: Some(thread) })
    }

    pub(super) fn arm(&self, timeout: Duration) {
        let deadline = Instant::now() + timeout;
        let mut state = self.shared.state.lock();
        state.deadline = Some(deadline);
        state.timed_out = false;
        self.shared.changed.notify_one();
    }

    /// Disarm synchronously before returning the worker to the pool, so a
    /// timeout for its previous request cannot race with its next user.
    pub(super) fn finish<T>(
        &self,
        timeout: Duration,
        phase: &str,
        result: Result<T, String>,
    ) -> Result<T, String> {
        let mut state = self.shared.state.lock();
        state.deadline = None;
        self.shared.changed.notify_one();
        if state.timed_out {
            return Err(format!(
                "compiler daemon timed out during {phase} after {} seconds",
                timeout.as_secs()
            ));
        }
        result
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

fn watchdog_loop(child: Arc<Mutex<Child>>, shared: Arc<SharedState>) {
    let mut state = shared.state.lock();
    while !state.shutdown {
        match state.deadline {
            Some(deadline) if Instant::now() >= deadline => {
                state.timed_out = true;
                state.deadline = None;
                // Killing the child is the only portable way to interrupt blocking
                // pipe operations. Keep the state locked through the kill so finish
                // and a subsequent arm cannot race with an old timeout.
                let _ = child.lock().kill();
            }
            Some(deadline) => {
                shared.changed.wait_until(&mut state, deadline);
            }
            None => shared.changed.wait(&mut state),
        }
    }
}
