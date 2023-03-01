use crate::concurrency::ctx;
use crate::stats::metrics;
use bytesize::{GIB, MIB};
use near_primitives::time;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// Maximum size of network message in encoded format.
/// We encode length as `u32`, and therefore maximum size can't be larger than `u32::MAX`.
const MESSAGE_MAX_BYTES: usize = 512 * MIB as usize;
/// Maximum capacity of write buffer in bytes.
const SEND_QUEUE_MAX_BYTES: usize = GIB as usize;

#[derive(thiserror::Error, Debug, Clone)]
pub(crate) enum Error {
    #[error("send queue is full")]
    SendQueueFull,
    #[error("message too large: got {got_bytes}B, want <={want_max_bytes}B")]
    MessageTooLarge { got_bytes: usize, want_max_bytes: usize },
}

pub(crate) struct WindowMetric {
    buckets: Vec<u64>,
    current_bucket: usize,
    current_time: time::Instant,
    window_size: time::Duration,
    bucket_size: time::Duration,
    total: u64,
    window: u64,
}

impl WindowMetric {
    pub fn new(buckets: usize, window_size: time::Duration) -> Self {
        Self {
            current_time: ctx::time::now(),
            current_bucket: 0,
            buckets: vec![0; buckets + 1],
            window_size,
            bucket_size: window_size / (buckets as f64),
            total: 0,
            window: 0,
        }
    }

    pub fn observe(&mut self, sample: u64) {
        self.roll();
        self.total += sample;
        self.buckets[self.current_bucket] += sample;
    }

    pub fn rate_per_sec(&mut self) -> f64 {
        self.roll();
        (self.total as f64) / self.window_size.as_seconds_f64()
    }

    pub fn total(&self) -> u64 {
        self.total
    }

    fn roll(&mut self) {
        let now = ctx::time::now();
        if self.current_time + self.window_size < now {
            self.current_time = now;
            for b in &mut self.buckets {
                *b = 0;
            }
            self.window = 0;
        }
        while self.current_time + self.bucket_size < now {
            self.window += self.buckets[self.current_bucket];
            self.current_bucket = (self.current_bucket + 1) % self.buckets.len();
            self.window -= self.buckets[self.current_bucket];
            self.buckets[self.current_bucket] = 0;
            self.current_time += self.bucket_size;
        }
    }
}

pub(crate) struct Stats {
    /// Number of messages received since the last reset of the counter.
    pub received_messages: Arc<Mutex<WindowMetric>>,
    /// Avg received bytes/s, based on the last few minutes of traffic.
    pub received_bytes: Arc<Mutex<WindowMetric>>,
    /// Avg sent bytes/s, based on the last few minutes of traffic.
    pub sent_bytes: Arc<Mutex<WindowMetric>>,

    /// Number of messages in the buffer to send.
    pub messages_to_send: AtomicU64,
    /// Number of bytes (sum of message sizes) in the buffer to send.
    pub bytes_to_send: AtomicU64,

    /// metrics
    send_buf_size_metric: metrics::IntGaugeGuard,
    recv_buf_size_metric: metrics::IntGaugeGuard,
    recv_msg_size_metric: metrics::HistogramGuard,
}

impl Stats {
    pub fn new(peer_addr: &std::net::SocketAddr) -> Self {
        let labels = vec![peer_addr.to_string()];
        Self {
            received_messages: Arc::new(Mutex::new(WindowMetric::new(
                10,
                time::Duration::minutes(1),
            ))),
            sent_bytes: Arc::new(Mutex::new(WindowMetric::new(10, time::Duration::minutes(1)))),
            received_bytes: Arc::new(Mutex::new(WindowMetric::new(10, time::Duration::minutes(1)))),

            messages_to_send: AtomicU64::new(0),
            bytes_to_send: AtomicU64::new(0),

            send_buf_size_metric: metrics::MetricGuard::new(
                &*metrics::PEER_DATA_WRITE_BUFFER_SIZE,
                labels.clone(),
            ),
            recv_buf_size_metric: metrics::MetricGuard::new(
                &metrics::PEER_DATA_READ_BUFFER_SIZE,
                labels.clone(),
            ),
            recv_msg_size_metric: metrics::MetricGuard::new(&metrics::PEER_MSG_SIZE_BYTES, labels),
        }
    }
}

pub(crate) struct SendGuard {
    stats: Arc<Stats>,
    frame_size: usize,
}

impl SendGuard {
    pub fn new(stats: Arc<Stats>, frame_size: usize) -> Result<SendGuard, Error> {
        if frame_size > MESSAGE_MAX_BYTES {
            metrics::MessageDropped::InputTooLong.inc_unknown_msg();
            return Err(Error::MessageTooLarge {
                got_bytes: frame_size,
                want_max_bytes: MESSAGE_MAX_BYTES,
            });
        }
        let buf_size = stats.bytes_to_send.fetch_add(frame_size as u64, Ordering::Relaxed) as usize
            + frame_size;
        if buf_size > SEND_QUEUE_MAX_BYTES {
            metrics::MessageDropped::MaxCapacityExceeded.inc_unknown_msg();
            stats.bytes_to_send.fetch_sub(frame_size as u64, Ordering::Relaxed);
            return Err(Error::SendQueueFull);
        }
        stats.messages_to_send.fetch_add(1, Ordering::Relaxed);
        stats.send_buf_size_metric.add(frame_size as i64);
        Ok(Self { stats, frame_size })
    }
}

impl Drop for SendGuard {
    fn drop(&mut self) {
        self.stats.send_buf_size_metric.sub(self.frame_size as i64);
        self.stats.messages_to_send.fetch_sub(1, Ordering::Relaxed);
        self.stats.bytes_to_send.fetch_sub(self.frame_size as u64, Ordering::Relaxed);
        self.stats.sent_bytes.lock().unwrap().observe(self.frame_size as u64);
    }
}

pub(super) struct RecvGuard {
    stats: Arc<Stats>,
    start_time: time::Instant,
    frame_size: usize,
}

impl RecvGuard {
    pub fn new(stats: Arc<Stats>, frame_size: usize) -> Result<RecvGuard, Error> {
        if frame_size > MESSAGE_MAX_BYTES {
            return Err(Error::MessageTooLarge {
                got_bytes: frame_size,
                want_max_bytes: MESSAGE_MAX_BYTES,
            });
        }
        stats.recv_buf_size_metric.set(frame_size as i64);
        Ok(Self { stats, start_time: ctx::time::now(), frame_size })
    }
}

impl Drop for RecvGuard {
    fn drop(&mut self) { 
        self.stats.recv_buf_size_metric.set(0);
        self.stats.recv_msg_size_metric.observe(self.frame_size as f64);
        self.stats.received_messages.lock().unwrap().observe(1);
        self.stats.received_bytes.lock().unwrap().observe(self.frame_size as u64);
        metrics::PEER_DATA_RECEIVED_BYTES.inc_by(self.frame_size as u64);
        metrics::PEER_MESSAGE_RECEIVED_TOTAL.inc();
        metrics::PEER_MSG_READ_LATENCY
            .observe((ctx::time::now() - self.start_time).as_seconds_f64());
    }
}
