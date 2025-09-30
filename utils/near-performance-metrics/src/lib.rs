pub mod process;
pub mod stats_disabled;
#[cfg(feature = "performance_stats")]
pub mod stats_enabled;

#[cfg(not(feature = "performance_stats"))]
pub use stats_disabled as stats;
#[cfg(feature = "performance_stats")]
pub use stats_enabled as stats;
