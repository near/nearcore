use rayon::iter::ParallelIterator;
use std::sync::atomic::{AtomicBool, Ordering};

/// spawns a closure on a global rayon threadpool and awaits its completion.
/// Returns the closure result.
/// WARNING: panicking within a rayon task seems to be causing a double panic,
/// and hence the panic message is not visible when running "cargo test".
pub async fn run<T: 'static + Send>(f: impl 'static + Send + FnOnce() -> T) -> T {
    let (send, recv) = tokio::sync::oneshot::channel();
    rayon::spawn(move || {
        if send.send(f()).is_err() {
            tracing::warn!("rayon::run has been aborted");
        }
    });
    recv.await.unwrap()
}

pub fn run_blocking<T: 'static + Send>(f: impl 'static + Send + FnOnce() -> T) -> T {
    let (send, recv) = tokio::sync::oneshot::channel();
    rayon::spawn(move || {
        if send.send(f()).is_err() {
            tracing::warn!("rayon::run has been aborted");
        }
    });
    recv.blocking_recv().unwrap()
}

/// Applies f to the iterated elements and collects the results, until the first None is returned.
/// Returns the results collected so far and a bool (false iff any None was returned).
pub fn try_map<I: ParallelIterator, T: Send>(
    iter: I,
    f: impl Sync + Send + Fn(I::Item) -> Option<T>,
) -> (Vec<T>, bool) {
    let ok = AtomicBool::new(true);
    let res = iter
        .filter_map(|v| {
            if !ok.load(Ordering::Acquire) {
                return None;
            }
            let res = f(v);
            if res.is_none() {
                ok.store(false, Ordering::Release);
            }
            res
        })
        .collect();
    (res, ok.load(Ordering::Acquire))
}
