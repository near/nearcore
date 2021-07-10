mod cli;

use std::env;

use self::cli::NeardCmd;
use clap::crate_version;
use git_version::git_version;
use lazy_static::lazy_static;
use near_performance_metrics;
use near_primitives::version::{Version, DB_VERSION, PROTOCOL_VERSION};
#[cfg(feature = "memory_stats")]
use near_rust_allocator_proxy::allocator::MyAllocator;
use nearcore::get_default_home;

lazy_static! {
    static ref NEARD_VERSION: Version = Version {
        version: crate_version!().to_string(),
        build: git_version!(fallback = "unknown").to_string(),
    };
    static ref NEARD_VERSION_STRING: String = {
        format!(
            "{} (build {}) (protocol {}) (db {})",
            NEARD_VERSION.version, NEARD_VERSION.build, PROTOCOL_VERSION, DB_VERSION
        )
    };
    static ref DEFAULT_HOME: String = get_default_home();
}

#[cfg(feature = "memory_stats")]
#[global_allocator]
static ALLOC: MyAllocator = MyAllocator;

#[cfg(all(not(feature = "memory_stats"), jemallocator))]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

fn main() {
    // We use it to automatically search the for root certificates to perform HTTPS calls
    // (sending telemetry and downloading genesis)
    openssl_probe::init_ssl_cert_env_vars();
    near_performance_metrics::process::schedule_printing_performance_stats(60);

    NeardCmd::parse_and_run()
}
