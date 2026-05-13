use crate::contract_code::RuntimeContractIdentifier;
use crate::ext::RuntimeContractExt;
use crate::metrics::{
    CACHE_WARMING_COMPILES_TOTAL, CACHE_WARMING_DROPPED_TOTAL, CACHE_WARMING_FAILURES,
};
use near_async::thread_pool::{ThreadPool, contract_warming_pool};
use near_parameters::vm::Config;
use near_store::contract::ContractStorage;
use near_vm_runner::logic::errors::{CacheError, CompilationError};
use near_vm_runner::{
    Contract as _, ContractCode, ContractPrecompilatonResult, ContractRuntimeCache,
    precompile_contract, try_precompile_contract,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Initialization parameters for the cache-warming subsystem. Plumbed
/// through to [`init_warming`] exactly once during runtime construction.
#[derive(Debug, Clone, Copy)]
pub struct WarmingConfig {
    /// Worker threads in the warming pool. `0` disables warming entirely
    /// (no pool created, every submission short-circuits silently).
    pub thread_count: usize,
    /// Cap on submissions queued in the warming pool. `0` disables warming.
    pub max_item_count: usize,
}

/// Configure the cache-warming subsystem. Call once at runtime startup.
/// The pool's thread count is fixed at the first call (OnceLock); the
/// queue cap is overwritten on subsequent calls.
pub fn init_warming(config: WarmingConfig) {
    near_async::thread_pool::init_contract_warming_pool(config.thread_count);
    WARMING_PENDING_SUBMISSIONS_CAP.store(config.max_item_count, Ordering::Relaxed);
}

/// Process-global queue cap. Set by [`init_warming`]; default `0` keeps
/// warming off until configured.
static WARMING_PENDING_SUBMISSIONS: AtomicUsize = AtomicUsize::new(0);
static WARMING_PENDING_SUBMISSIONS_CAP: AtomicUsize = AtomicUsize::new(0);

/// Try to claim a slot for a new warming submission. Returns `true` on
/// success; caller must invoke [`release_pending_slot`] once the worker has
/// dequeued the closure (so a freshly running worker frees the slot for the
/// next submission).
fn try_reserve_pending_slot() -> bool {
    let cap = WARMING_PENDING_SUBMISSIONS_CAP.load(Ordering::Relaxed);
    let mut current = WARMING_PENDING_SUBMISSIONS.load(Ordering::Relaxed);
    loop {
        if current >= cap {
            return false;
        }
        match WARMING_PENDING_SUBMISSIONS.compare_exchange_weak(
            current,
            current + 1,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            Ok(_) => return true,
            Err(observed) => current = observed,
        }
    }
}

fn release_pending_slot() {
    WARMING_PENDING_SUBMISSIONS.fetch_sub(1, Ordering::Relaxed);
}

/// Returns `true` if compiling a contract against `a` would land under a
/// different on-disk cache key than compiling against `b`. Mirrors the
/// non-`code_hash` inputs to [`near_vm_runner::cache::get_contract_cache_key`]:
/// `vm_kind` plus the config's non-crypto hash.
///
/// The `vm_hash` component of the cache key is intentionally not part of
/// this comparison. Within a single binary execution `vm_hash` is a function
/// of (i) `vm_kind`, (ii) the parts of `Config` that flow into VM-engine
/// setup (e.g. `WasmFeatures` reads `reftypes_bulk_memory`; the Wasmtime
/// engine reads `LimitConfig::{max_memory_pages, max_tables_per_contract,
/// max_elements_per_contract_table}`), and (iii) compile-time constants
/// (wasmtime crate version, build target, the `version` bump inside
/// `WasmtimeVM::vm_hash`). All Config-derived inputs are captured in
/// `non_crypto_hash` via `Config`'s `Hash` derive, and the compile-time
/// constants are identical for `a` and `b` in the same process — so
/// matching `(vm_kind, non_crypto_hash)` implies matching `vm_hash`.
/// If a future change introduces a `vm_hash` input that isn't reachable
/// from `Config`, this function must learn about it (or the next-PV cache
/// key may collide with the current-PV one).
pub fn cache_keys_differ(a: &Config, b: &Config) -> bool {
    a.vm_kind != b.vm_kind || a.non_crypto_hash() != b.non_crypto_hash()
}

/// Precompile `code` against `current_config` synchronously and
/// — when `next_config` is `Some` and its cache-key signature differs from
/// `current_config` — additionally enqueue a fire-and-forget warming
/// compilation against `next_config` on the [`contract_warming_pool`].
///
/// Errors from either compile are dropped.
pub(crate) fn precompile_contract_with_warming(
    code: &ContractCode,
    current_config: Arc<Config>,
    next_config: Option<Arc<Config>>,
    cache: Option<&dyn ContractRuntimeCache>,
) {
    if let (Some(next_config), Some(cache)) = (next_config, cache) {
        if cache_keys_differ(&current_config, &next_config) {
            spawn_cache_warming(code.clone(), next_config, cache.handle());
        }
    }
    let _ = precompile_contract(code, Arc::clone(&current_config), cache);
}

/// Eager warming spawn used by [`precompile_contract_with_warming`] on the
/// deploy hot path: the caller already holds the code bytes, so the inner
/// `get_code` closure trivially returns them.
fn spawn_cache_warming(
    code: ContractCode,
    config: Arc<Config>,
    cache_handle: Box<dyn ContractRuntimeCache>,
) {
    let code = Arc::new(code);
    spawn_warming(Box::new(move || Some(code)), config, cache_handle);
}

/// Lazy warming spawn used by the pipelining path: the worker fetches the
/// contract bytes from `storage` via [`RuntimeContractExt`] once it picks
/// up the closure, so the caller pays no extra I/O at submit time.
pub(crate) fn spawn_lazy_cache_warming(
    storage: ContractStorage,
    identifier: RuntimeContractIdentifier,
    config: Arc<Config>,
    cache_handle: Box<dyn ContractRuntimeCache>,
) {
    spawn_warming(
        Box::new(move || RuntimeContractExt { storage, identifier }.get_code()),
        config,
        cache_handle,
    );
}

/// Shared spawn impl. Eager vs lazy is just whether `get_code` returns
/// immediately or fetches from storage on the worker. Reserves a
/// pending-submission slot before enqueue; releases it on dequeue; runs
/// [`try_precompile_contract`] and records the outcome. Returns silently
/// when warming is disabled.
fn spawn_warming(
    get_code: Box<dyn FnOnce() -> Option<Arc<ContractCode>> + Send>,
    config: Arc<Config>,
    cache_handle: Box<dyn ContractRuntimeCache>,
) {
    let Some(pool) = try_get_warming_pool() else {
        return;
    };
    if !try_reserve_pending_slot() {
        CACHE_WARMING_DROPPED_TOTAL.inc();
        return;
    }
    pool.spawn_boxed(Box::new(move || {
        release_pending_slot();
        let Some(code) = get_code() else {
            return;
        };
        let result = try_precompile_contract(code.as_ref(), config, Some(&*cache_handle));
        record_warming_result(result);
    }));
}

/// Returns the warming pool only when warming is fully enabled — i.e. both
/// the pool was built with a non-zero thread count and the queue-depth cap
/// is non-zero. Folds the two disable knobs into a single guard so the
/// spawn paths don't have to consult them independently.
fn try_get_warming_pool() -> Option<&'static Arc<ThreadPool>> {
    if WARMING_PENDING_SUBMISSIONS_CAP.load(Ordering::Relaxed) == 0 {
        return None;
    }
    contract_warming_pool()
}

/// Increment the compiles counter only on a fresh compile;
/// `ContractAlreadyInCache` is intentionally not counted.
fn record_warming_result(
    result: Result<Result<ContractPrecompilatonResult, CompilationError>, CacheError>,
) {
    match result {
        Ok(Ok(ContractPrecompilatonResult::ContractCompiled)) => {
            CACHE_WARMING_COMPILES_TOTAL.inc();
        }
        Ok(Ok(ContractPrecompilatonResult::ContractAlreadyInCache)) => {}
        Ok(Ok(ContractPrecompilatonResult::CacheNotAvailable)) => {
            // Reachable only if a caller wires `None` as the cache.
            debug_assert!(false, "warming submission with no cache handle");
        }
        Ok(Err(_)) | Err(_) => {
            CACHE_WARMING_FAILURES.inc();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use near_parameters::RuntimeConfig;
    use near_parameters::vm::VMKind;

    fn vm_config_with(vm_kind: VMKind) -> Arc<Config> {
        let mut config = Config::clone(RuntimeConfig::test().wasm_config.as_ref());
        config.vm_kind = vm_kind;
        Arc::new(config)
    }

    #[test]
    fn signatures_equal_for_identical_configs() {
        let a = vm_config_with(VMKind::Wasmtime);
        let b = vm_config_with(VMKind::Wasmtime);
        assert!(!cache_keys_differ(&a, &b));
    }

    #[test]
    fn signatures_differ_for_distinct_vm_kinds() {
        let a = vm_config_with(VMKind::Wasmtime);
        let b = vm_config_with(VMKind::NearVm);
        assert!(cache_keys_differ(&a, &b));
    }

    #[test]
    fn signatures_differ_for_distinct_non_crypto_hash() {
        let a = vm_config_with(VMKind::Wasmtime);
        let mut b_inner = Config::clone(a.as_ref());
        // Tweak a limit that participates in `non_crypto_hash`.
        b_inner.limit_config.max_total_log_length =
            b_inner.limit_config.max_total_log_length.wrapping_add(1);
        let b = Arc::new(b_inner);
        // Sanity: vm_kind unchanged, but the hash should differ now.
        assert_eq!(a.vm_kind, b.vm_kind);
        assert!(cache_keys_differ(&a, &b));
    }

    /// Tests touch the process-global pending cap; serialize to keep their
    /// observations independent of other cap tests running concurrently.
    static CAP_TEST_LOCK: parking_lot::Mutex<()> = parking_lot::Mutex::new(());

    fn with_pending_cap<R>(cap: usize, run: impl FnOnce() -> R) -> R {
        let _guard = CAP_TEST_LOCK.lock();
        let prev_cap = WARMING_PENDING_SUBMISSIONS_CAP.load(Ordering::Relaxed);
        WARMING_PENDING_SUBMISSIONS.store(0, Ordering::Relaxed);
        WARMING_PENDING_SUBMISSIONS_CAP.store(cap, Ordering::Relaxed);
        let result = run();
        WARMING_PENDING_SUBMISSIONS.store(0, Ordering::Relaxed);
        WARMING_PENDING_SUBMISSIONS_CAP.store(prev_cap, Ordering::Relaxed);
        result
    }

    #[test]
    fn pending_cap_admits_up_to_capacity() {
        with_pending_cap(5, || {
            for _ in 0..5 {
                assert!(try_reserve_pending_slot());
            }
            assert!(!try_reserve_pending_slot());
            release_pending_slot();
            assert!(try_reserve_pending_slot());
        });
    }

    #[test]
    fn pending_cap_zero_rejects_everything() {
        with_pending_cap(0, || {
            assert!(!try_reserve_pending_slot());
        });
    }
}
