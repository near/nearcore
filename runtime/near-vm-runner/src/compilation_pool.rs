use near_async::thread_pool::{Priority, ThreadPool};
use std::sync::OnceLock;
use std::time::Duration;

/// Shared OS-thread pool for running contract compilation (the top-level
/// `compile_and_cache` flow that holds the per-cache-key compilation mutex).
pub fn contract_compilation_pool() -> &'static ThreadPool {
    static POOL: OnceLock<ThreadPool> = OnceLock::new();
    POOL.get_or_init(|| {
        let thread_limit = std::cmp::max(2, num_cpus::get() / 2);
        ThreadPool::new(
            "contract_compilation",
            Duration::from_secs(30),
            thread_limit,
            Priority::Default,
        )
    })
}
