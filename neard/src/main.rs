mod cli;

use self::cli::NeardCmd;
use anyhow::Context;
use near_primitives::version::{Version, PROTOCOL_VERSION};
use near_store::metadata::DB_VERSION;
use nearcore::get_default_home;
use once_cell::sync::Lazy;
use std::env;
use std::path::PathBuf;
use std::time::Duration;

static NEARD_VERSION: &str = env!("NEARD_VERSION");
static NEARD_BUILD: &str = env!("NEARD_BUILD");
static RUSTC_VERSION: &str = env!("NEARD_RUSTC_VERSION");

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

    // We use it to automatically search the for root certificates to perform HTTPS calls
    // (sending telemetry and downloading genesis)
    openssl_probe::init_ssl_cert_env_vars();
    near_performance_metrics::process::schedule_printing_performance_stats(Duration::from_secs(60));

    // Retrieve FD_LIMIT from an environment variable with a default value of 65535.
    let fd_limit_default = 65535; // Default value
    let fd_limit_str = env::var("FD_LIMIT").unwrap_or_else(|_| fd_limit_default.to_string());
    let fd_limit = fd_limit_str.parse::<u64>().context("Failed to parse FD_LIMIT from environment variable")?;

    // Retrieve the current hard limit
    let (_, hard) = rlimit::Resource::NOFILE.get().context("rlimit::Resource::NOFILE::get()")?;
    // Attempt to set both the soft and hard limits to fd_limit
    rlimit::Resource::NOFILE.set(fd_limit, fd_limit).context(format!(
        "couldn't set the file descriptor limit to {}, hard limit = {}",
        fd_limit, hard
    ))?;

    NeardCmd::parse_and_run()
}
