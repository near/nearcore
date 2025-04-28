use std::sync::{Arc, OnceLock};

/// `ProcessingDoneTracker` can be used in conjunction with a `ProcessingDoneWaiter`
/// to wait until some processing is finished. `ProcessingDoneTracker` should be
/// kept alive as long as the processing is ongoing, then once it's dropped,
/// the paired `ProcessingDoneWaiter` will be notified that the processing has finished.
/// Does NOT implement `Clone`, if you want to clone it, wrap it in an `Arc`.
pub struct ProcessingDoneTracker(Arc<OnceLock<()>>);

impl ProcessingDoneTracker {
    /// Create a new `ProcessingDoneTracker`
    pub fn new() -> ProcessingDoneTracker {
        ProcessingDoneTracker(Arc::new(OnceLock::new()))
    }

    /// Create a `ProcessingDoneWaiter` paired to this `ProcessingDoneTracker`.
    /// When this `ProcessingDoneTracker` is dropped, the paired waiter will be notified.
    pub fn make_waiter(&self) -> ProcessingDoneWaiter {
        ProcessingDoneWaiter(self.0.clone())
    }
}

impl Drop for ProcessingDoneTracker {
    fn drop(&mut self) {
        // Set a value to notify waiters that the processing has finished.
        //
        // OnceLock::set() returns an Err when the cell already has a value.
        // This should be impossible, as there's only one ProcessingDoneTracker
        // that will set this value when dropped, so it's safe to unwrap() it.
        self.0.set(()).unwrap();
    }
}

/// `ProcessingDoneWaiter` is used to wait until the processing has finished.
/// The `wait()` method will block until the paired `ProcessingDoneTracker` is dropped.
/// A new instance of `ProcessingDoneWaiter` can be created by calling `ProcessingDoneTracker::make_waiter`.
#[derive(Clone)]
pub struct ProcessingDoneWaiter(Arc<OnceLock<()>>);

impl ProcessingDoneWaiter {
    /// Wait until the processing is finished.
    pub fn wait(self) {
        self.0.wait();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    use super::ProcessingDoneTracker;

    /// Basic test for `ProcessingDoneTracker` and `ProcessingDoneWaiter`.
    /// Spawns a task on a separate thread and waits for it to finish
    /// using a tracker and a waiter.
    #[test]
    fn test_processing_done() {
        let shared_value: Arc<AtomicBool> = Arc::new(AtomicBool::new(false));

        let tracker = ProcessingDoneTracker::new();
        let waiter = tracker.make_waiter();

        let captured_shared_value = shared_value.clone();
        std::thread::spawn(move || {
            let _captured_tracker = tracker;
            std::thread::sleep(Duration::from_millis(32));
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

        let (results_sender, results_receiver) = std::sync::mpsc::channel();

        // Sawn waiter tasks
        for waiter in [waiter1, waiter2, waiter3] {
            let cur_sender = results_sender.clone();
            let cur_shared_value = shared_value.clone();
            std::thread::spawn(move || {
                waiter.wait();
                let read_value = cur_shared_value.load(Ordering::Relaxed);
                cur_sender.send(read_value).unwrap();
            });
        }

        // Wait 32ms then set the shared_value to true
        std::thread::sleep(Duration::from_millis(32));
        shared_value.store(true, Ordering::Relaxed);
        std::mem::drop(tracker);

        // Check values that waiters read
        for _ in 0..3 {
            let waiter_value = results_receiver.recv().unwrap();
            assert_eq!(waiter_value, true);
        }
    }
}
