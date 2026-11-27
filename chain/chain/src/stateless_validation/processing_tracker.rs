use std::sync::{Arc, OnceLock};

/// `ProcessingDoneTracker` can be used in conjunction with a `ProcessingDoneWaiter`
/// to wait until some processing is finished and learn whether it succeeded.
/// `ProcessingDoneTracker` should be kept alive as long as the processing is ongoing.
/// The processing reports its result with `finish`. Once the tracker is finished or
/// dropped, the paired `ProcessingDoneWaiter` is notified with the result.
/// A tracker dropped without `finish` (e.g. the processing panicked, stopped early, or
/// was deferred, like an orphan state witness whose validation runs later without the
/// tracker) reports a failure, so an unfinished processing never looks like a success.
/// Does NOT implement `Clone`, if you want to clone it, wrap it in an `Arc`.
#[derive(Debug)]
pub struct ProcessingDoneTracker(Arc<OnceLock<bool>>);

impl ProcessingDoneTracker {
    pub fn new() -> ProcessingDoneTracker {
        ProcessingDoneTracker(Arc::new(OnceLock::new()))
    }

    /// Create a `ProcessingDoneWaiter` paired to this `ProcessingDoneTracker`.
    /// When this `ProcessingDoneTracker` is finished or dropped, the paired waiter will be notified.
    pub fn make_waiter(&self) -> ProcessingDoneWaiter {
        ProcessingDoneWaiter(self.0.clone())
    }

    /// Record whether the processing succeeded and notify the paired waiters.
    pub fn finish(self, success: bool) {
        // The cell can only be set here or in `drop`, and `finish` consumes the tracker,
        // so the cell is always empty at this point.
        let _ = self.0.set(success);
    }
}

impl Drop for ProcessingDoneTracker {
    fn drop(&mut self) {
        // Notify waiters that the processing has finished. If `finish` already recorded
        // a result, `set` returns an error and the recorded result is kept. Otherwise
        // the processing ended without reporting a result, which counts as a failure.
        let _ = self.0.set(false);
    }
}

/// `ProcessingDoneWaiter` is used to wait until the processing has finished.
/// The `wait()` method will block until the paired `ProcessingDoneTracker` is finished or dropped.
/// A new instance of `ProcessingDoneWaiter` can be created by calling `ProcessingDoneTracker::make_waiter`.
#[derive(Clone)]
pub struct ProcessingDoneWaiter(Arc<OnceLock<bool>>);

impl ProcessingDoneWaiter {
    /// Wait until the processing is finished. Returns `true` if the processing reported
    /// success, and `false` if it reported a failure or ended without reporting a result.
    /// `false` therefore means "not validated successfully", not necessarily "invalid".
    pub fn wait(self) -> bool {
        *self.0.wait()
    }
}

#[cfg(test)]
mod tests {
    use super::ProcessingDoneTracker;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, mpsc};
    use std::time::Duration;
    use std::{mem, thread};

    /// Basic test for `ProcessingDoneTracker` and `ProcessingDoneWaiter`.
    /// Spawns a task on a separate thread and waits for it to finish
    /// using a tracker and a waiter.
    #[test]
    fn test_processing_done() {
        let shared_value: Arc<AtomicBool> = Arc::new(AtomicBool::new(false));

        let tracker = ProcessingDoneTracker::new();
        let waiter = tracker.make_waiter();

        let captured_shared_value = shared_value.clone();
        thread::spawn(move || {
            let _captured_tracker = tracker;
            thread::sleep(Duration::from_millis(32));
            captured_shared_value.store(true, Ordering::Relaxed);
        });

        waiter.wait();
        assert_eq!(shared_value.load(Ordering::Relaxed), true);
    }

    /// Test that there can be multiple instances of `ProcessingDoneWaiter`.
    #[test]
    fn test_multiple_waiters() {
        let shared_value: Arc<AtomicBool> = Arc::new(AtomicBool::new(false));

        let tracker = ProcessingDoneTracker::new();
        let waiter1 = tracker.make_waiter();
        let waiter2 = waiter1.clone();
        let waiter3 = tracker.make_waiter();

        let (results_sender, results_receiver) = mpsc::channel();

        // Sawn waiter tasks
        for waiter in [waiter1, waiter2, waiter3] {
            let cur_sender = results_sender.clone();
            let cur_shared_value = shared_value.clone();
            thread::spawn(move || {
                waiter.wait();
                let read_value = cur_shared_value.load(Ordering::Relaxed);
                cur_sender.send(read_value).unwrap();
            });
        }

        // Wait 32ms then set the shared_value to true
        thread::sleep(Duration::from_millis(32));
        shared_value.store(true, Ordering::Relaxed);
        mem::drop(tracker);

        // Check values that waiters read
        for _ in 0..3 {
            let waiter_value = results_receiver.recv().unwrap();
            assert_eq!(waiter_value, true);
        }
    }

    /// A tracker finished with success reports success to every waiter.
    #[test]
    fn test_finish_success() {
        let tracker = ProcessingDoneTracker::new();
        let waiter1 = tracker.make_waiter();
        let waiter2 = waiter1.clone();

        let handle = thread::spawn(move || {
            thread::sleep(Duration::from_millis(32));
            tracker.finish(true);
        });

        assert!(waiter1.wait());
        assert!(waiter2.wait());
        handle.join().unwrap();
    }

    /// A tracker finished with failure reports failure.
    #[test]
    fn test_finish_failure() {
        let tracker = ProcessingDoneTracker::new();
        let waiter = tracker.make_waiter();
        thread::spawn(move || tracker.finish(false));
        assert!(!waiter.wait());
    }

    /// A tracker dropped without `finish` reports failure.
    #[test]
    fn test_drop_without_finish_reports_failure() {
        let tracker = ProcessingDoneTracker::new();
        let waiter = tracker.make_waiter();
        thread::spawn(move || drop(tracker));
        assert!(!waiter.wait());
    }

    /// A tracker owned by a thread that panics before `finish` reports failure, and
    /// the waiter doesn't hang.
    #[test]
    fn test_panic_before_finish_reports_failure() {
        let tracker = ProcessingDoneTracker::new();
        let waiter = tracker.make_waiter();
        let handle = thread::spawn(move || {
            let _tracker = tracker;
            panic!("processing failed");
        });
        assert!(!waiter.wait());
        assert!(handle.join().is_err());
    }
}
