mod cli;
mod watchers;

use self::cli::NeardCmd;
use anyhow::Context;
use near_primitives::version::{Version, PROTOCOL_VERSION};
use near_store::metadata::DB_VERSION;
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

#[cfg(not(feature = "memory_stats"))]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn main() -> anyhow::Result<()> {
    if env::var("RUST_BACKTRACE").is_err() {
        // Enable backtraces on panics by default.
        env::set_var("RUST_BACKTRACE", "1");
    }

    rayon::ThreadPoolBuilder::new()
        .stack_size(8 * 1024 * 1024)
        .build_global()
        .context("failed to create the threadpool")?;

    #[cfg(feature = "memory_stats")]
    ALLOC.set_report_usage_interval(512 << 20).enable_stack_trace(true);
    // We use it to automatically search the for root certificates to perform HTTPS calls
    // (sending telemetry and downloading genesis)
    openssl_probe::init_ssl_cert_env_vars();
    near_performance_metrics::process::schedule_printing_performance_stats(Duration::from_secs(60));

    // The default FD soft limit in linux is 1024.
    // We use more than that, for example we support up to 1000 TCP
    // connections, using 5 FDs per each connection.
    // We consider 2^16 to be a reasonable limit for this binary,
    // and we enforce it here. We also set the hard limit to the same value
    // to prevent the inner logic from trying to bump it further:
    // FD limit is a global variable, so it shouldn't be modified in an
    // uncoordinated way.
    const FD_LIMIT: u64 = 1<<15;
    rlimit::Resource::NOFILE
        .set(FD_LIMIT, FD_LIMIT)
        .context(format!("couldn't set the file descriptor limit to {FD_LIMIT}"))?;

    NeardCmd::parse_and_run()
}
