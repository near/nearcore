#![doc = include_str!("../README.md")]

use std::sync::atomic::{AtomicU64, Ordering};

// NOTE: AtomicU64 is the same unit as BlockHeight, and use to store the expected blockheight to
// shutdown
pub static EXPECTED_SHUTDOWN_AT: AtomicU64 = AtomicU64::new(0);

pub fn reload(expected_shutdown: Option<u64>) {
    if let Some(expected_shutdown) = expected_shutdown {
        EXPECTED_SHUTDOWN_AT.store(expected_shutdown, Ordering::Relaxed);
    } else {
        EXPECTED_SHUTDOWN_AT.store(0, Ordering::Relaxed);
    }
}
