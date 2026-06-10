use parking_lot::Mutex;
use std::collections::BTreeMap;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{RecvTimeoutError, channel};
use std::thread;
use std::time::Duration;

/// Registry of all currently live spawned futures, for diagnosing shutdown
/// hangs: a future that has neither completed nor been dropped keeps its
/// captures (e.g. `Arc` references) alive, which can pin resources such as
/// store handles past actor system shutdown.
static LIVE_FUTURES: Mutex<BTreeMap<u64, LiveFuture>> = Mutex::new(BTreeMap::new());
static NEXT_FUTURE_ID: AtomicU64 = AtomicU64::new(0);

struct LiveFuture {
    owner: String,
    description: &'static str,
}

struct LiveFutureGuard {
    id: u64,
}

impl Drop for LiveFutureGuard {
    fn drop(&mut self) {
        LIVE_FUTURES.lock().remove(&self.id);
    }
}

/// Wraps a future so it is listed in [`live_futures`] until it completes or
/// is dropped. `owner` names the runtime or spawner the future runs on.
pub(crate) fn track_future<F>(
    owner: &str,
    description: &'static str,
    f: F,
) -> impl Future<Output = F::Output> + Send + 'static
where
    F: Future + Send + 'static,
    F::Output: Send,
{
    let id = NEXT_FUTURE_ID.fetch_add(1, Ordering::Relaxed);
    let live_future = LiveFuture { owner: owner.to_string(), description };
    LIVE_FUTURES.lock().insert(id, live_future);
    let guard = LiveFutureGuard { id };
    async move {
        let _guard = guard;
        f.await
    }
}

/// Lists live spawned futures as "owner/description" strings.
pub fn live_futures() -> Vec<String> {
    LIVE_FUTURES.lock().values().map(|f| format!("{}/{}", f.owner, f.description)).collect()
}

/// Runs `f` (typically a blocking shutdown wait), logging the live spawned
/// futures every 30 seconds until it returns. A future that keeps appearing
/// in these dumps after actor system shutdown is leaked and pins everything
/// it captures.
pub fn with_stall_diagnostics<R>(waiting_for: &str, f: impl FnOnce() -> R) -> R {
    let (done_sender, done_receiver) = channel::<()>();
    let waiting_for = waiting_for.to_string();
    let watchdog = thread::spawn(move || {
        while let Err(RecvTimeoutError::Timeout) =
            done_receiver.recv_timeout(Duration::from_secs(30))
        {
            tracing::warn!(
                target: "future_registry",
                waiting_for,
                live_futures = ?live_futures(),
                "still waiting; dumping live spawned futures"
            );
        }
    });
    let result = f();
    drop(done_sender);
    watchdog.join().ok();
    result
}

#[cfg(test)]
mod tests {
    use super::{live_futures, track_future};

    #[tokio::test]
    async fn test_track_future_lifecycle() {
        let (sender, receiver) = tokio::sync::oneshot::channel::<()>();
        let task = tokio::spawn(track_future("test owner", "blocked future", async move {
            receiver.await.unwrap();
        }));
        // Wait for the spawned future to start and block on the channel.
        while !live_futures().contains(&"test owner/blocked future".to_string()) {
            tokio::task::yield_now().await;
        }
        sender.send(()).unwrap();
        task.await.unwrap();
        assert!(!live_futures().contains(&"test owner/blocked future".to_string()));

        let (_sender, receiver) = tokio::sync::oneshot::channel::<()>();
        let dropped = track_future("test owner", "dropped future", async move {
            receiver.await.unwrap();
        });
        assert!(live_futures().contains(&"test owner/dropped future".to_string()));
        drop(dropped);
        assert!(!live_futures().contains(&"test owner/dropped future".to_string()));
    }
}
