use near_async::futures::{FutureSpawner, FutureSpawnerExt};
use std::future::Future;
use tokio::sync::{mpsc, oneshot};
use tokio::time::Duration;

/// Configuration for rate limiting
#[derive(Copy, Clone, Debug)]
pub struct RateLimit {
    pub burst: u64,
    pub qps: f64,
}

impl RateLimit {
    pub fn validate(&self) -> anyhow::Result<()> {
        if self.qps <= 0.0 {
            anyhow::bail!("qps must be > 0");
        }
        if self.burst == 0 {
            anyhow::bail!("burst must be > 0");
        }
        Ok(())
    }
}

type HandlerFn<Arg, Res> = Box<dyn FnOnce(Vec<Arg>) -> BoxFuture<Vec<Res>> + Send + 'static>;
type BoxFuture<T> = std::pin::Pin<Box<dyn Future<Output = T> + Send + 'static>>;

struct BatchRequest<Arg, Res> {
    arg: Arg,
    response: oneshot::Sender<Res>,
    handler: HandlerFn<Arg, Res>,
}

/// Rate-limited batch processor that replaces the demux system.
/// Collects multiple requests and processes them in batches with rate limiting.
/// Maintains the same API as the original demux system.
pub struct BatchProcessor<Arg, Res> {
    sender: mpsc::UnboundedSender<BatchRequest<Arg, Res>>,
}

impl<Arg, Res> Clone for BatchProcessor<Arg, Res> {
    fn clone(&self) -> Self {
        Self { sender: self.sender.clone() }
    }
}

#[derive(thiserror::Error, Debug, PartialEq)]
#[error("batch processor has been stopped")]
pub struct ProcessorStoppedError;

impl<Arg: 'static + Send, Res: 'static + Send> BatchProcessor<Arg, Res> {
    /// Create a new batch processor with the given rate limit.
    pub fn new(rate_limit: RateLimit, future_spawner: &dyn FutureSpawner) -> Self {
        rate_limit.validate().expect("Invalid rate limit configuration");

        let (sender, mut receiver) = mpsc::unbounded_channel::<BatchRequest<Arg, Res>>();

        future_spawner.spawn("batch_processor", async move {
            let mut tokens = rate_limit.burst;
            let mut next_token = None;
            let interval_duration = Duration::from_secs_f64(1.0 / rate_limit.qps);
            let mut pending_requests = Vec::new();
            let mut closed = false;

            loop {
                // Calculate next token arrival time if needed
                if tokens < rate_limit.burst && next_token.is_none() {
                    next_token = Some(tokio::time::Instant::now() + interval_duration);
                }

                tokio::select! {
                    // Token arrival
                    _ = async {
                        tokio::time::sleep_until(next_token.unwrap()).await
                    }, if next_token.is_some() => {
                        tokens += 1;
                        next_token = None;
                    }

                    // New request arrival
                    request = receiver.recv(), if !closed => {
                        match request {
                            Some(req) => pending_requests.push(req),
                            None => closed = true,
                        }
                    }
                }

                // Process batch if we have tokens and pending requests
                if !pending_requests.is_empty() && tokens > 0 {
                    // Collect all pending requests
                    while let Ok(req) = receiver.try_recv() {
                        pending_requests.push(req);
                    }

                    tokens -= 1;
                    let batch = std::mem::take(&mut pending_requests);
                    tokio::spawn(Self::process_batch(batch));
                }

                // Exit if channel closed and no pending requests
                if closed && pending_requests.is_empty() {
                    break;
                }
            }
        });

        Self { sender }
    }

    /// Submit a request for batch processing. When the batch is ready, it will be processed
    /// together with the preset handler, and each call will be completed with its corresponding
    /// response.
    pub async fn call<F, Fut>(&self, arg: Arg, handler: F) -> Result<Res, ProcessorStoppedError>
    where
        F: FnOnce(Vec<Arg>) -> Fut + Send + 'static,
        Fut: Future<Output = Vec<Res>> + Send + 'static,
    {
        let (response_tx, response_rx) = oneshot::channel();

        let request = BatchRequest {
            arg,
            response: response_tx,
            handler: Box::new(move |args| Box::pin(handler(args))),
        };

        self.sender.send(request).map_err(|_| ProcessorStoppedError)?;
        response_rx.await.map_err(|_| ProcessorStoppedError)
    }

    async fn process_batch(batch: Vec<BatchRequest<Arg, Res>>) {
        if batch.is_empty() {
            return;
        }

        // Extract arguments, response channels, and handlers
        let mut args = Vec::with_capacity(batch.len());
        let mut response_channels = Vec::with_capacity(batch.len());
        let mut handlers = Vec::with_capacity(batch.len());

        for request in batch {
            args.push(request.arg);
            response_channels.push(request.response);
            handlers.push(request.handler);
        }

        // Use the first handler - we assume all requests use the same handler.
        if let Some(handler) = handlers.into_iter().next() {
            let results = handler(args).await;

            for (result, response_tx) in results.into_iter().zip(response_channels) {
                let _ = response_tx.send(result); // It's OK if receiver dropped
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use near_async::futures::DirectTokioFutureSpawnerForTest;
    use tokio::time::{Duration, Instant};

    #[tokio::test]
    async fn test_batch_processor_basic() {
        let processor = BatchProcessor::new(
            RateLimit { qps: 10.0, burst: 5 },
            &DirectTokioFutureSpawnerForTest,
        );

        let result = processor.call(42u32, |args: Vec<u32>| async move { args }).await.unwrap();
        assert_eq!(result, 42);
    }

    #[tokio::test]
    async fn test_rate_limiting() {
        let processor =
            BatchProcessor::new(RateLimit { qps: 2.0, burst: 1 }, &DirectTokioFutureSpawnerForTest);

        let start = Instant::now();

        // First call should go through immediately
        let _result1 = processor.call(1u32, |args: Vec<u32>| async move { args }).await.unwrap();

        // Second call should be rate limited
        let _result2 = processor.call(2u32, |args: Vec<u32>| async move { args }).await.unwrap();

        let elapsed = start.elapsed();
        // Should take at least 500ms due to rate limiting (1/2 QPS = 500ms interval)
        assert!(elapsed >= Duration::from_millis(400)); // Some tolerance for timing
    }

    #[tokio::test]
    async fn test_batching() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let processor = BatchProcessor::new(
            RateLimit { qps: 1.0, burst: 1000 },
            &DirectTokioFutureSpawnerForTest,
        );
        let call_count = Arc::new(AtomicUsize::new(0));

        // Submit multiple requests concurrently
        let handles: Vec<_> = (0..5)
            .map(|i| {
                let processor = processor.clone();
                let call_count = call_count.clone();
                tokio::spawn(async move {
                    processor
                        .call(i, {
                            let call_count = call_count.clone();
                            move |args: Vec<i32>| async move {
                                call_count.fetch_add(1, Ordering::SeqCst);
                                args // Return the same args
                            }
                        })
                        .await
                        .unwrap()
                })
            })
            .collect();

        // Wait for all to complete
        let results: Vec<_> = futures::future::try_join_all(handles).await.unwrap();

        // All should get their original value back
        for (i, result) in results.iter().enumerate() {
            assert_eq!(*result, i as i32);
        }

        // The handler should have been called only once (batching)
        assert_eq!(call_count.load(Ordering::SeqCst), 1);
    }
}
