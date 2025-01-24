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
    // while the right group contains errors. Collect them into two separate Vecs.
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

#[cfg(test)]
mod tests {
    use super::try_map_result;
    use rayon::iter::ParallelBridge;

    #[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
    #[error("error for testing")]
    struct TestError;

    /// On empty input try_map_result returns empty output and Ok(())
    #[test]
    fn try_map_result_empty_iter() {
        let empty_array: [i32; 0] = [];

        let panicking_function = |_input: i32| -> Result<i64, TestError> {
            panic!();
        };

        let (outputs, result): (Vec<i64>, Result<(), TestError>) =
            try_map_result(empty_array.into_iter().par_bridge(), panicking_function);
        assert_eq!(outputs, Vec::<i64>::new());
        assert_eq!(result, Ok(()));
    }

    /// Happy path of try_map_result - all items are successfully processed, should return the outputs and Ok(())
    #[test]
    fn try_map_result_success() {
        let inputs = [1, 2, 3, 4, 5, 6].into_iter();
        let func = |input: i32| -> Result<i64, TestError> { Ok(input as i64 + 1) };

        let (mut outputs, result): (Vec<i64>, Result<(), TestError>) =
            try_map_result(inputs.into_iter().par_bridge(), func);
        outputs.sort();
        assert_eq!(outputs, vec![2i64, 3, 4, 5, 6, 7]);
        assert_eq!(result, Ok(()));
    }

    /// Run `try_map_result` with an infinite stream of tasks, but the 100th task returns an Error.
    /// `try_map_result` should stop the execution and return some successful outputs along with the Error.
    #[test]
    fn try_map_result_stops_on_error() {
        // Infinite iterator of inputs: 1, 2, 3, 4, 5, ...
        let infinite_iter = (1..).into_iter();

        let func = |input: i32| -> Result<i64, TestError> {
            if input == 100 {
                return Err(TestError);
            }

            Ok(2 * input as i64)
        };

        let (mut outputs, result): (Vec<i64>, Result<(), TestError>) =
            try_map_result(infinite_iter.par_bridge(), func);
        outputs.sort();

        // The error will happen on 100th input, but other threads might produce subsequent outputs in parallel,
        // so there might be over 100 outputs. We can't really assume anything about the outputs due to the
        // nature of multithreading, but we can check that some basic conditions hold.

        // All outputs should be distinct
        for two_outputs in outputs.windows(2) {
            assert_ne!(two_outputs[0], two_outputs[1]);

            // All outputs should be even, func multiplies the inputs by 2.
            assert_eq!(two_outputs[0] % 2, 0);
            assert_eq!(two_outputs[1] % 2, 0);
        }

        assert_eq!(result, Err(TestError));
    }

    /// When using try_map_result, a panic in the function will be propagated to the caller
    #[test]
    #[should_panic]
    fn try_map_result_panic() {
        let inputs = (1..1000).into_iter();

        let panicking_function = |input: i32| -> Result<i64, TestError> {
            if input == 100 {
                panic!("Oh no the input is equal to 100");
            }
            Ok(2 * input as i64)
        };

        let (_outputs, _result) = try_map_result(inputs.par_bridge(), panicking_function);
        // Should panic
    }
}
