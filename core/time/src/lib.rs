//! Time module provides a non-global clock, which should be passed
//! as an argument to functions which need to read the current time.
//! In particular try to avoid storing the clock instances in the objects.
//! Functions which use system clock directly are non-hermetic, which
//! makes them effectively non-deterministic and hard to test.
//!
//! Clock provides 2 types of time reads:
//! 1. now() (aka POSIX CLOCK_MONOTONIC, aka time::Instant)
//!    time as perceived by the machine making the measurement.
//!    The subsequent calls to now() are guaranteed to return monotonic
//!    results. It should be used for measuring the latency of operations
//!    as observed by the machine. The time::Instant itself doesn't
//!    translate to any specific timestamp, so it is not meaningful for
//!    anyone other than the machine doing the measurement.
//! 2. now_utc() (aka POSIX CLOCK_REALTIME, aka time::Utc)
//!    expected to approximate the (global) UTC time.
//!    There is NO guarantee that the subsequent reads will be monotonic,
//!    as CLOCK_REALTIME it configurable in the OS settings, or can be updated
//!    during NTP sync. Should be used whenever you need to communicate a timestamp
//!    over the network, or store it for later use. Remember that clocks
//!    of different machines are not perfectly synchronized, and in extreme
//!    cases can be totally skewed.

#[cfg(feature = "clock")]
pub mod clock;

#[cfg(feature = "clock")]
pub use clock::*;

#[cfg(feature = "serde")]
pub mod serde;

#[cfg(feature = "serde")]
pub use serde::*;

pub use time::error;

// TODO: consider wrapping these types to prevent interactions
// with other time libraries, especially to prevent the direct access
// to the realtime (i.e. not through the Clock).
pub type Instant = std::time::Instant;
// TODO: OffsetDateTime stores the timestamp in a decomposed form of
// (year,month,day,hour,...). If we find it inefficient, we should
// probably migrate to a pure UNIX timestamp and convert is to datetime
// only when needed.
pub type Utc = time::OffsetDateTime;
pub type Duration = time::Duration;

// By the definition of derive(PartialEq), Finite(...) < Infinite.
#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
pub enum Deadline {
    Finite(Instant),
    Infinite,
}

impl From<Instant> for Deadline {
    fn from(t: Instant) -> Deadline {
        Deadline::Finite(t)
    }
}
