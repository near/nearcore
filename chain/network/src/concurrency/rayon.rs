use rayon::iter::{Either, ParallelIterator};
use std::error::Error;
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

/// Applies `func` to the iterated elements and collects the outputs. On the first `Error` the execution is stopped.
/// Returns the outputs collected so far and a [`Result`] ([`Result::Err`] iff any `Error` was returned).
/// Same as [`try_map`], but it operates on [`Result`] instead of [`Option`].
pub fn try_map_result<I: ParallelIterator, T: Send, E: Error + Send>(
    iter: I,
    func: impl Sync + Send + Fn(I::Item) -> Result<T, E>,
) -> (Vec<T>, Result<(), E>) {
    // Call the function on every input value and emit some items for every result.
    // On a successful call this iterator emits one item: `Some(Ok(output_value))`
    // When an error occurs, the iterator emits two items: `Some(Err(the_error))` and `None``
    // The `None` will later be used to tell rayon to stop the execution.
    let optional_result_iter /* impl Iterator<Item = Option<Result<T, E>>> */ = iter
        .map(|v| match func(v) {
            Ok(val) => Either::Left(std::iter::once(Some(Ok(val)))),
            Err(err) => Either::Right([Some(Err(err)), None].into_iter()),
        })
        .flatten_iter();

    // `while_some()` monitors a stream of `Option` values and stops the execution when it spots a `None` value.
    // It's used to implement the short-circuit logic - on the first error rayon will stop processing subsequent items.
    let results_iter = optional_result_iter.while_some();

    // Split the results into two groups - the left group contains the outputs resulting from a successful execution,
    // while the right group contains errors. Collect them into twp separate Vecs.
    let (outputs, errors): (Vec<T>, Vec<E>) = results_iter
        .map(|res| match res {
            Ok(value) => Either::Left(value),
            Err(error) => Either::Right(error),
        })
        .collect();

    // Return the output and the first error (if there was any)
    match errors.into_iter().next() {
        Some(first_error) => (outputs, Err(first_error)),
        None => (outputs, Ok(())),
    }
}
