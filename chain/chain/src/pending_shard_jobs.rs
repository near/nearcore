use near_async::futures::{AsyncComputationSpawner, AsyncComputationSpawnerExt};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Produce a fallback value when a spawned task panics.
pub trait FromPanic {
    fn from_panic(message: String) -> Self;
}

impl<T> FromPanic for Result<T, crate::Error> {
    fn from_panic(message: String) -> Self {
        Err(crate::Error::Other(message))
    }
}

fn panic_payload_to_string(payload: Box<dyn std::any::Any + Send>) -> String {
    payload
        .downcast::<String>()
        .map(|s| *s)
        .or_else(|p| p.downcast::<&str>().map(|s| s.to_string()))
        .unwrap_or_else(|_| "unknown panic".to_string())
}

/// Spawns `count` tasks on spawner and collects their results in spawn order.
/// When the last task completes, `on_done` is called with all results. If
/// `count` is zero, `on_done` is invoked asynchronously via the spawner.
pub struct PendingShardJobs<K: Send + 'static, R: FromPanic + Send + 'static> {
    name: &'static str,
    spawner: Arc<dyn AsyncComputationSpawner>,
    next_index: AtomicUsize,
    remaining: AtomicUsize,
    results: parking_lot::Mutex<Vec<Option<(K, R)>>>,
    on_done: parking_lot::Mutex<Option<Box<dyn FnOnce(Vec<(K, R)>) + Send>>>,
}

impl<K: Send + 'static, R: FromPanic + Send + 'static> PendingShardJobs<K, R> {
    pub fn new(
        name: &'static str,
        spawner: Arc<dyn AsyncComputationSpawner>,
        count: usize,
        on_done: impl FnOnce(Vec<(K, R)>) + Send + 'static,
    ) -> Arc<Self> {
        let results: Vec<Option<(K, R)>> = (0..count).map(|_| None).collect();
        let pending = Arc::new(Self {
            name,
            spawner,
            next_index: AtomicUsize::new(0),
            remaining: AtomicUsize::new(count),
            results: parking_lot::Mutex::new(results),
            on_done: parking_lot::Mutex::new(Some(Box::new(on_done))),
        });
        if count == 0 {
            // Invoke on_done via the spawner to keep result delivery asynchronous.
            // Some test-loops depend on receiving results as a separate event.
            let pending_clone = pending.clone();
            pending.spawner.spawn(pending.name, move || {
                pending_clone.invoke_on_done();
            });
        }
        pending
    }

    /// Spawn a task. Results are delivered to `on_done` in the order `spawn`
    /// is called, regardless of which task finishes first.
    ///
    /// If `task` panics, `R::from_panic` produces a fallback result, keeping
    /// `on_done` delivery intact.
    pub fn spawn(self: &Arc<Self>, key: K, task: impl FnOnce() -> R + Send + 'static) {
        let index = self.next_index.fetch_add(1, Ordering::AcqRel);
        assert!(
            index < self.results.lock().len(),
            "{}: spawn called more times than the configured count",
            self.name
        );
        let pending = self.clone();
        self.spawner.spawn(self.name, move || {
            let result = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(task)) {
                Ok(value) => value,
                Err(payload) => {
                    let message = panic_payload_to_string(payload);
                    tracing::error!("{}: task panicked: {}", pending.name, message);
                    R::from_panic(message)
                }
            };
            pending.set_result(index, (key, result));
        });
    }

    fn set_result(&self, index: usize, result: (K, R)) {
        self.results.lock()[index] = Some(result);
        if self.remaining.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.invoke_on_done();
        }
    }

    fn invoke_on_done(&self) {
        let results =
            std::mem::take(&mut *self.results.lock()).into_iter().map(|r| r.unwrap()).collect();
        let on_done = self.on_done.lock().take().unwrap();
        on_done(results);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Error;
    use near_async::futures::StdThreadAsyncComputationSpawner;
    use std::sync::mpsc;
    use std::time::Duration;

    type TestResult = Result<String, Error>;

    #[test]
    fn results_in_spawn_order() {
        let (tx, rx) = mpsc::channel();
        let pending = PendingShardJobs::<u32, TestResult>::new(
            "test",
            Arc::new(StdThreadAsyncComputationSpawner),
            3,
            move |results| tx.send(results).unwrap(),
        );
        // Task 0 sleeps longest, task 2 returns immediately. Results must
        // still arrive in spawn order (0, 1, 2).
        pending.spawn(0, || {
            std::thread::sleep(Duration::from_millis(20));
            Ok("task 0".to_string())
        });
        pending.spawn(1, || {
            std::thread::sleep(Duration::from_millis(10));
            Ok("task 1".to_string())
        });
        pending.spawn(2, || Ok("task 2".to_string()));

        let results = rx.recv_timeout(Duration::from_secs(5)).unwrap();
        assert_eq!(results.len(), 3);
        for (i, (key, result)) in results.into_iter().enumerate() {
            assert_eq!(key, i as u32);
            assert_eq!(result.unwrap(), format!("task {i}"));
        }
    }

    #[test]
    fn panic_produces_error() {
        let (tx, rx) = mpsc::channel();
        let pending = PendingShardJobs::<u32, TestResult>::new(
            "test",
            Arc::new(StdThreadAsyncComputationSpawner),
            2,
            move |results| tx.send(results).unwrap(),
        );
        pending.spawn(0, || Ok("ok".to_string()));
        pending.spawn(1, || panic!("deliberate panic"));

        let results = rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let [(key_0, result_0), (key_1, result_1)] = results.try_into().unwrap();

        assert_eq!(key_0, 0);
        assert_eq!(result_0.unwrap(), "ok");

        assert_eq!(key_1, 1);
        let err = result_1.unwrap_err();
        assert!(err.to_string().contains("deliberate panic"), "{err}");
    }

    #[test]
    fn zero_count_invokes_on_done() {
        let (tx, rx) = mpsc::channel();
        PendingShardJobs::<u32, TestResult>::new(
            "test",
            Arc::new(StdThreadAsyncComputationSpawner),
            0,
            move |results| tx.send(results).unwrap(),
        );
        let results = rx.recv_timeout(Duration::from_secs(5)).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    #[should_panic(expected = "spawn called more times than the configured count")]
    fn spawn_over_count_panics() {
        let pending = PendingShardJobs::<u32, TestResult>::new(
            "test",
            Arc::new(StdThreadAsyncComputationSpawner),
            1,
            move |_| {},
        );
        pending.spawn(0, || Ok("first".to_string()));
        pending.spawn(1, || Ok("second".to_string()));
    }
}
