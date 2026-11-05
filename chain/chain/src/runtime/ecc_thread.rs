//! The thread that runs ECCs for the chunk producer, one at a time.
//!
//! A contract's run stops only when it ends or runs out of gas, so the producer waits for a run
//! only until its deadline. A run that is still going then finishes in the background, and until
//! it does the thread takes no other run.

#[cfg(feature = "test_features")]
use near_primitives::hash::CryptoHash;
#[cfg(feature = "test_features")]
use parking_lot::Mutex;
#[cfg(feature = "test_features")]
use std::collections::HashMap;
use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, RecvTimeoutError, Sender};
use std::sync::{Arc, OnceLock};
use std::thread;
use std::time::Instant;

type Job = Box<dyn FnOnce() + Send>;

/// The stack of the thread, the same as that of the threads that run contracts in apply (see
/// `neard`'s main). The default one is smaller, and a contract that recurses deeply could overflow
/// it, which aborts the node.
const STACK_SIZE_BYTES: usize = 8 * 1024 * 1024;

/// What came of a job given to the thread.
#[derive(Debug, PartialEq, Eq)]
pub enum EccRun<T> {
    /// The job finished before the deadline.
    Done(T),
    /// The job did not finish before the deadline. It goes on in the background, and its result
    /// is thrown away.
    Late,
    /// The thread is still busy with an earlier job, so this one did not start.
    Busy,
}

/// A thread that runs one job at a time. It starts with the first job.
pub struct EccThread {
    sender: OnceLock<Sender<Job>>,
    /// Whether a job is running. Cleared before the job's result is handed over, so the caller
    /// that got a result finds the thread free.
    busy: Arc<AtomicBool>,
}

impl EccThread {
    pub fn new() -> Self {
        Self { sender: OnceLock::new(), busy: Arc::new(AtomicBool::new(false)) }
    }

    /// Whether the thread is running a job, such as one that missed its deadline.
    pub fn is_busy(&self) -> bool {
        self.busy.load(Ordering::Acquire)
    }

    fn sender(&self) -> &Sender<Job> {
        self.sender.get_or_init(|| {
            let (sender, receiver) = mpsc::channel::<Job>();
            thread::Builder::new()
                .name("ecc-runner".to_string())
                .stack_size(STACK_SIZE_BYTES)
                .spawn(move || receiver.iter().for_each(|job| job()))
                .expect("failed to spawn the thread that runs ECCs");
            sender
        })
    }

    /// Runs `job` on the thread and waits for it until `deadline`. A panic of the job is passed
    /// on to the caller, and the thread stays usable.
    pub fn run<T: Send + 'static>(
        &self,
        job: impl FnOnce() -> T + Send + 'static,
        deadline: Instant,
    ) -> EccRun<T> {
        if self.busy.compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire).is_err() {
            return EccRun::Busy;
        }
        let (result_sender, result_receiver) = mpsc::channel();
        let busy = Arc::clone(&self.busy);
        let work: Job = Box::new(move || {
            let result = catch_unwind(AssertUnwindSafe(job));
            busy.store(false, Ordering::Release);
            // The caller is gone if the job was late.
            let _ = result_sender.send(result);
        });
        self.sender().send(work).expect("the thread that runs ECCs stopped");
        match result_receiver.recv_timeout(deadline.saturating_duration_since(Instant::now())) {
            Ok(Ok(value)) => EccRun::Done(value),
            Ok(Err(panic)) => resume_unwind(panic),
            Err(RecvTimeoutError::Timeout) => EccRun::Late,
            Err(RecvTimeoutError::Disconnected) => panic!("the thread that runs ECCs stopped"),
        }
    }
}

/// Holds up the runs of the ECCs that tests choose until they are released, so that a test can make
/// a run late without depending on the speed of the machine. The run of a held ECC waits on its
/// gate before it starts.
#[cfg(feature = "test_features")]
#[derive(Default)]
pub struct TestHeldEccs {
    gates: Mutex<HashMap<CryptoHash, Arc<OnceLock<()>>>>,
}

#[cfg(feature = "test_features")]
impl TestHeldEccs {
    /// Returns the gate of an ECC, if it is held. The job that runs the ECC waits on it.
    pub fn get_gate(&self, ecc_hash: &CryptoHash) -> Option<Arc<OnceLock<()>>> {
        self.gates.lock().get(ecc_hash).cloned()
    }

    pub fn hold(&self, ecc_hash: &CryptoHash) {
        let prev = self.gates.lock().insert(*ecc_hash, Arc::new(OnceLock::new()));
        assert!(prev.is_none(), "ECC {ecc_hash} is already held");
    }

    pub fn release(&self, ecc_hash: &CryptoHash) {
        let gate = self.gates.lock().remove(ecc_hash).expect("ECC was not held");
        let _ = gate.set(());
    }
}

#[cfg(test)]
mod tests {
    use super::{EccRun, EccThread};
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::sync::mpsc;
    use std::thread;
    use std::time::{Duration, Instant};

    fn deadline_in(duration: Duration) -> Instant {
        Instant::now() + duration
    }

    /// Waits until the thread finishes the job it is running.
    fn wait_until_free(thread: &EccThread) {
        while thread.is_busy() {
            thread::yield_now();
        }
    }

    #[test]
    fn job_that_finishes_in_time_returns_its_result() {
        let thread = EccThread::new();
        assert!(!thread.is_busy());

        assert_eq!(thread.run(|| 7, deadline_in(Duration::from_secs(60))), EccRun::Done(7));

        // The thread is free as soon as the result is in hand.
        assert!(!thread.is_busy());
        assert_eq!(thread.run(|| 8, deadline_in(Duration::from_secs(60))), EccRun::Done(8));
    }

    /// A job that is not done by the deadline is late, and it keeps the thread busy until it
    /// ends. Another job does not start meanwhile.
    #[test]
    fn late_job_keeps_the_thread_busy() {
        let thread = EccThread::new();
        let (release, gate) = mpsc::channel::<()>();
        let (started_sender, started) = mpsc::channel::<()>();

        let run = thread.run(
            move || {
                started_sender.send(()).unwrap();
                gate.recv().unwrap();
            },
            deadline_in(Duration::from_millis(10)),
        );
        assert_eq!(run, EccRun::Late);
        started.recv().unwrap();
        assert!(thread.is_busy());

        assert_eq!(thread.run(|| 1, deadline_in(Duration::from_secs(60))), EccRun::Busy);
        assert!(thread.is_busy());

        release.send(()).unwrap();
        wait_until_free(&thread);
        assert_eq!(thread.run(|| 2, deadline_in(Duration::from_secs(60))), EccRun::Done(2));
    }

    /// A panic of the job goes to the caller, and the thread stays usable.
    #[test]
    fn job_panic_goes_to_the_caller_and_the_thread_goes_on() {
        let thread = EccThread::new();

        let panicked = catch_unwind(AssertUnwindSafe(|| {
            thread.run(|| panic!("the contract run panicked"), deadline_in(Duration::from_secs(60)))
        }));

        assert!(panicked.is_err());
        assert!(!thread.is_busy());
        assert_eq!(thread.run(|| 3, deadline_in(Duration::from_secs(60))), EccRun::Done(3));
    }
}
