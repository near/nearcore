mod cli;
mod log_config_watcher;

use self::cli::NeardCmd;
use crate::cli::RunError;
use near_primitives::version::{Version, PROTOCOL_VERSION};
use near_store::version::DB_VERSION;
use nearcore::get_default_home;
use once_cell::sync::Lazy;
use std::env;
use std::path::PathBuf;
use std::time::Duration;

static NEARD_VERSION: &'static str = env!("NEARD_VERSION");
static NEARD_BUILD: &'static str = env!("NEARD_BUILD");
static RUSTC_VERSION: &'static str = env!("NEARD_RUSTC_VERSION");

static NEARD_VERSION_STRING: Lazy<String> = Lazy::new(|| {
    format!(
        "(release {}) (build {}) (rustc {}) (protocol {}) (db {})",
        NEARD_VERSION, NEARD_BUILD, RUSTC_VERSION, PROTOCOL_VERSION, DB_VERSION
    )
});

fn neard_version() -> Version {
    Version {
        version: NEARD_VERSION.to_string(),
        build: NEARD_BUILD.to_string(),
        rustc_version: RUSTC_VERSION.to_string(),
    }
}

static DEFAULT_HOME: Lazy<PathBuf> = Lazy::new(get_default_home);

#[cfg(feature = "memory_stats")]
#[global_allocator]
static ALLOC: near_rust_allocator_proxy::ProxyAllocator<tikv_jemallocator::Jemalloc> =
    near_rust_allocator_proxy::ProxyAllocator::new(tikv_jemallocator::Jemalloc);

#[cfg(all(not(feature = "memory_stats"), feature = "jemalloc"))]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn main() -> Result<(), RunError> {
    if env::var("RUST_BACKTRACE").is_err() {
        // Enable backtraces on panics by default.
        env::set_var("RUST_BACKTRACE", "1");
    }

    rayon::ThreadPoolBuilder::new()
        .stack_size(8 * 1024 * 1024)
        .build_global()
        .map_err(RunError::RayonInstall)?;

    #[cfg(feature = "memory_stats")]
    ALLOC.set_report_usage_interval(512 << 20).enable_stack_trace(true);
    // We use it to automatically search the for root certificates to perform HTTPS calls
    // (sending telemetry and downloading genesis)
    openssl_probe::init_ssl_cert_env_vars();
    near_performance_metrics::process::schedule_printing_performance_stats(Duration::from_secs(60));

    NeardCmd::parse_and_run()
}
