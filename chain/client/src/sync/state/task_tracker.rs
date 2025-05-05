use parking_lot::Mutex;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Performs two functions:
///  - Limits the parallelism of tasks that call `get_handle`. Only up to `limit` handles can
///    be obtained at the same time. Dropping a TaskHandle releases the slot.
///  - Keeps track of the status (a string) for each task handle that is active, for status
///    reporting.
#[derive(Clone)]
pub(super) struct TaskTracker {
    semaphore: Arc<Semaphore>,
    statuses: Arc<Mutex<BTreeMap<usize, String>>>,
    id_counter: Arc<std::sync::atomic::AtomicUsize>,
}

impl TaskTracker {
    /// Creates a new TaskTracker with a specified concurrency limit.
    pub fn new(limit: usize) -> Self {
        TaskTracker {
            semaphore: Arc::new(Semaphore::new(limit)),
            statuses: Arc::new(Mutex::new(BTreeMap::new())),
            id_counter: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        }
    }

    /// Asynchronously obtains a handle, waiting if necessary until a slot is available.
    /// "Asynchronously" means that when a handle is not available, the function does NOT block.
    /// The description will become part of the status string.
    #[tracing::instrument(skip(self))]
    pub async fn get_handle(&self, description: &str) -> Arc<TaskHandle> {
        // Acquire a permit from the semaphore.
        let permit = self.semaphore.clone().acquire_owned().await.unwrap();
        let description = description.to_string();
        // Generate a unique ID for the handle.
        let id = self.id_counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        {
            // Initialize the status for this handle.
            let mut statuses = self.statuses.lock();
            statuses.insert(id, description.clone());
        }
        TaskHandle {
            id,
            task_description: description,
            statuses: self.statuses.clone(),
            _permit: permit, // Holds the permit to keep the slot occupied.
        }
        .into()
    }

    /// Returns the statuses of all active tasks.
    pub fn statuses(&self) -> Vec<String> {
        self.statuses.lock().values().cloned().collect()
    }
}

/// A task handle. Tasks that are intended to be limited in parallelism should be holding
/// this handle while doing heavy work.
pub(super) struct TaskHandle {
    id: usize,
    task_description: String,
    statuses: Arc<Mutex<BTreeMap<usize, String>>>,
    _permit: OwnedSemaphorePermit, // Keeps the slot occupied in the semaphore.
}

impl TaskHandle {
    /// Sets the status string for this handle.
    pub fn set_status(&self, status: &str) {
        tracing::debug!(%status, "State sync task status changed");
        let mut statuses = self.statuses.lock();
        statuses.insert(self.id, format!("{}: {}", self.task_description, status));
    }
}

impl Drop for TaskHandle {
    /// Automatically called when the handle is dropped, freeing the slot and removing the status.
    fn drop(&mut self) {
        let mut statuses = self.statuses.lock();
        statuses.remove(&self.id);
    }
}
