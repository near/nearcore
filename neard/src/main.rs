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
use std::path::PathBuf;

pub fn get_version() -> String {
    match crate_version!() {
        "0.0.0" => "trunk".to_string(),
        _ => crate_version!().to_string(),
    }
}

lazy_static! {
    static ref NEARD_VERSION: Version =
        Version { version: get_version(), build: git_version!(fallback = "unknown").to_string() };
    static ref NEARD_VERSION_STRING: String = {
        format!(
            "(release {}) (build {}) (protocol {}) (db {})",
            NEARD_VERSION.version, NEARD_VERSION.build, PROTOCOL_VERSION, DB_VERSION
        )
    };
    static ref DEFAULT_HOME: PathBuf = get_default_home();
}

#[cfg(feature = "memory_stats")]
#[global_allocator]
static ALLOC: MyAllocator<tikv_jemallocator::Jemalloc> =
    MyAllocator::new(tikv_jemallocator::Jemalloc);

#[cfg(all(not(feature = "memory_stats"), feature = "jemalloc"))]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn main() {
    // We use it to automatically search the for root certificates to perform HTTPS calls
    // (sending telemetry and downloading genesis)
    openssl_probe::init_ssl_cert_env_vars();
    near_performance_metrics::process::schedule_printing_performance_stats(60);

    NeardCmd::parse_and_run()
}
