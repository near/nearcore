//! Deadline watchdog for blocking compiler-daemon IPC.

use parking_lot::Mutex;
use std::process::Child;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::thread::{Builder, JoinHandle};
use std::time::{Duration, Instant};

/// Killing the child is the only portable way to interrupt blocking
/// `ChildStdin` and `ChildStdout` operations.
enum WatchdogCommand {
    // A generation identifies one startup or compilation operation. It keeps
    // late timeout/disarm handling for an old operation from affecting a newer
    // operation on the same worker.
    Arm { generation: u64, timeout: Duration, acknowledged: Sender<()> },
    Disarm { generation: u64, acknowledged: Sender<()> },
    Shutdown,
}

pub(super) struct ProcessWatchdog {
    commands: Sender<WatchdogCommand>,
    timed_out_generation: Arc<AtomicU64>,
    thread: Option<JoinHandle<()>>,
    next_generation: u64,
}

impl ProcessWatchdog {
    pub(super) fn spawn(child: Arc<Mutex<Child>>) -> std::io::Result<Self> {
        let (commands, receiver) = mpsc::channel();
        let timed_out_generation = Arc::new(AtomicU64::new(0));
        let watchdog_timed_out_generation = Arc::clone(&timed_out_generation);
        let thread =
            Builder::new().name("compiler-daemon-watchdog".to_owned()).spawn(move || {
                watchdog_loop(child, receiver, watchdog_timed_out_generation);
            })?;
        Ok(Self { commands, timed_out_generation, thread: Some(thread), next_generation: 1 })
    }

    pub(super) fn arm(&mut self, timeout: Duration) -> Result<u64, String> {
        let generation = self.next_generation;
        self.next_generation = self.next_generation.wrapping_add(1).max(1);
        let (acknowledged, acknowledgement) = mpsc::channel();
        self.commands
            .send(WatchdogCommand::Arm { generation, timeout, acknowledged })
            .map_err(|_| "compiler daemon watchdog stopped unexpectedly".to_owned())?;
        acknowledgement
            .recv()
            .map_err(|_| "compiler daemon watchdog stopped unexpectedly".to_owned())?;
        Ok(generation)
    }

    /// Disarm synchronously before returning the worker to the pool, so a
    /// timeout for its previous request cannot race with its next user.
    pub(super) fn finish<T>(
        &self,
        generation: u64,
        timeout: Duration,
        phase: &str,
        result: Result<T, String>,
    ) -> Result<T, String> {
        let (acknowledged, acknowledgement) = mpsc::channel();
        self.commands
            .send(WatchdogCommand::Disarm { generation, acknowledged })
            .map_err(|_| "compiler daemon watchdog stopped unexpectedly".to_owned())?;
        acknowledgement
            .recv()
            .map_err(|_| "compiler daemon watchdog stopped unexpectedly".to_owned())?;
        if self.timed_out_generation.load(Ordering::SeqCst) == generation {
            return Err(format!(
                "compiler daemon timed out during {phase} after {} seconds",
                timeout.as_secs()
            ));
        }
        result
    }

    pub(super) fn shutdown(&mut self) {
        let _ = self.commands.send(WatchdogCommand::Shutdown);
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

fn watchdog_loop(
    child: Arc<Mutex<Child>>,
    receiver: Receiver<WatchdogCommand>,
    timed_out_generation: Arc<AtomicU64>,
) {
    let mut armed: Option<(u64, Instant)> = None;
    loop {
        let command = match armed {
            Some((generation, deadline)) => {
                match receiver.recv_timeout(deadline.saturating_duration_since(Instant::now())) {
                    Ok(command) => command,
                    Err(RecvTimeoutError::Timeout) => {
                        timed_out_generation.store(generation, Ordering::SeqCst);
                        let _ = child.lock().kill();
                        armed = None;
                        continue;
                    }
                    Err(RecvTimeoutError::Disconnected) => return,
                }
            }
            None => match receiver.recv() {
                Ok(command) => command,
                Err(_) => return,
            },
        };
        match command {
            WatchdogCommand::Arm { generation, timeout, acknowledged } => {
                armed = Some((generation, Instant::now() + timeout));
                let _ = acknowledged.send(());
            }
            WatchdogCommand::Disarm { generation, acknowledged } => {
                if matches!(armed, Some((armed_generation, _)) if armed_generation == generation) {
                    armed = None;
                }
                let _ = acknowledged.send(());
            }
            WatchdogCommand::Shutdown => return,
        }
    }
}
