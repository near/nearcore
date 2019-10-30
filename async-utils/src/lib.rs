#![feature(await_macro, async_await)]

use std::time::{Duration, Instant};

use futures03::{compat::Future01CompatExt as _, FutureExt as _};

/// This macro is extracted from
/// https://github.com/rust-lang-nursery/futures-rs/blob/c30adf513b9eea35ab385c0797210c77986fc82f/futures/src/lib.rs#L503-L510
///
/// It is useful when the `futures-preview` is imported as `futures03`.
macro_rules! select { // replace `::futures_util` with `::futures03` as the crate path
    ($($tokens:tt)*) => {
        futures03::inner_select::select! {
            futures_crate_path ( ::futures03 )
            $( $tokens )*
        }
    }
}

/// An async/await helper to delay the execution:
///
/// ```ignore
/// let _ = delay(std::time::Duration::from_secs(1)).await;
/// ```
pub async fn delay(duration: Duration) -> Result<(), tokio::timer::Error> {
    tokio::timer::Delay::new(Instant::now() + duration).compat().await
}

pub struct TimeoutError;

/// An async/await helper to timeout a given async context:
///
/// ```ignore
/// timeout(
///     std::time::Duration::from_secs(1),
///     async {
///         let _ = delay(std::time::Duration::from_secs(2)).await;
///     }
/// ).await
/// ```
pub async fn timeout<T, Fut>(timeout: Duration, f: Fut) -> Result<T, TimeoutError>
where
    Fut: futures03::Future<Output = T> + Send,
{
    select! {
        result = f.boxed().fuse() => Ok(result),
        _ = delay(timeout).boxed().fuse() => Err(TimeoutError {})
    }
}
