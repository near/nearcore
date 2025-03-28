mod cli;

use self::cli::NeardCmd;
use anyhow::Context;
use near_primitives::version::{MIN_SUPPORTED_PROTOCOL_VERSION, PROTOCOL_VERSION, Version};
use near_store::db::metadata::DB_VERSION;
use nearcore::get_default_home;
use std::env;
use std::path::PathBuf;
use std::sync::LazyLock;
use std::time::Duration;

static NEARD_VERSION: &str = env!("NEARD_VERSION");
static NEARD_BUILD: &str = env!("NEARD_BUILD");
static NEARD_COMMIT: &str = env!("NEARD_COMMIT");
static RUSTC_VERSION: &str = env!("NEARD_RUSTC_VERSION");
static NEARD_FEATURES: &str = env!("NEARD_FEATURES");

static NEARD_VERSION_STRING: LazyLock<String> = LazyLock::new(|| {
    format!(
        "(release {}) (build {}) (commit {}) (rustc {}) (min_protocol {}) (protocol {}) (db {})\nfeatures: [{}]",
        NEARD_VERSION,
        NEARD_BUILD,
        NEARD_COMMIT,
        RUSTC_VERSION,
        MIN_SUPPORTED_PROTOCOL_VERSION,
        PROTOCOL_VERSION,
        DB_VERSION,
        NEARD_FEATURES
    )
});

fn neard_version() -> Version {
    Version {
        version: NEARD_VERSION.to_string(),
        build: NEARD_BUILD.to_string(),
        commit: NEARD_COMMIT.to_string(),
        rustc_version: RUSTC_VERSION.to_string(),
    }
}

static DEFAULT_HOME: LazyLock<PathBuf> = LazyLock::new(get_default_home);

// cspell:words tikv jemallocator Jemalloc
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn main() -> anyhow::Result<()> {
    if env::var("RUST_BACKTRACE").is_err() {
        // Enable backtraces on panics by default.
        unsafe { env::set_var("RUST_BACKTRACE", "1") };
    }

    rayon::ThreadPoolBuilder::new()
        .stack_size(8 * 1024 * 1024)
        .build_global()
        .context("failed to create the threadpool")?;

    // We use it to automatically search the for root certificates to perform HTTPS calls
    // (sending telemetry and downloading genesis)
    openssl_probe::init_ssl_cert_env_vars();
    near_performance_metrics::process::schedule_printing_performance_stats(Duration::from_secs(60));

    // The default FD soft limit in linux is 1024.
    // We use more than that, for example we support up to 1000 TCP
    // connections, using 5 FDs per each connection.
    // We consider 65535 to be a reasonable limit for this binary,
    // and we enforce it here. We also set the hard limit to the same value
    // to prevent the inner logic from trying to bump it further:
    // FD limit is a global variable, so it shouldn't be modified in an
    // uncoordinated way.
    const REQUIRED_NOFILE: u64 = 65535;
    let (soft, hard) = rlimit::Resource::NOFILE.get().context("rlimit::Resource::NOFILE::get()")?;
    if soft < REQUIRED_NOFILE || hard < REQUIRED_NOFILE {
        let new_soft = soft.max(REQUIRED_NOFILE);
        let new_hard = hard.max(REQUIRED_NOFILE);
        rlimit::Resource::NOFILE.set(new_soft, new_hard).context(format!(
            "couldn't set the file descriptor limit to ({new_soft}, {new_hard}), \
             current limit = ({soft}, {hard})"
        ))?;
    }

    NeardCmd::parse_and_run()
}
