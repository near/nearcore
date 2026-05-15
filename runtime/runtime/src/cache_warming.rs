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
    config_cache_key_signature, precompile_contract, try_precompile_contract,
};
use parking_lot::Mutex;
use std::sync::{Arc, OnceLock};

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

/// Configure the cache-warming subsystem. Call once at runtime startup;
/// all settings are fixed at the first call (subsequent calls are no-ops).
pub fn init_warming(config: WarmingConfig) {
    near_async::thread_pool::init_contract_warming_pool(config.thread_count);
    let _ = WARMING_PENDING_SUBMISSIONS_CAP.set(config.max_item_count);
}

/// Process-global queue cap. Set by [`init_warming`]; absence (`None`)
/// means warming has not been configured yet — every submission rejects.
static WARMING_PENDING_SUBMISSIONS_CAP: OnceLock<usize> = OnceLock::new();
static WARMING_PENDING_SUBMISSIONS: Mutex<usize> = Mutex::new(0);

/// Try to claim a slot for a new warming submission. Returns `true` on
/// success; caller must invoke [`release_pending_slot`] once the worker has
/// dequeued the closure (so a freshly running worker frees the slot for the
/// next submission).
fn try_reserve_pending_slot() -> bool {
    let Some(&cap) = WARMING_PENDING_SUBMISSIONS_CAP.get() else {
        return false;
    };
    let mut count = WARMING_PENDING_SUBMISSIONS.lock();
    if *count >= cap {
        return false;
    }
    *count += 1;
    true
}

fn release_pending_slot() {
    *WARMING_PENDING_SUBMISSIONS.lock() -= 1;
}

/// Test-only variant of [`try_reserve_pending_slot`] that operates on a
/// caller-supplied `count` mutex and `cap`. Lets unit tests exercise the
/// admission logic without touching process-global state.
#[cfg(test)]
fn try_reserve_pending_slot_with_custom_cap(count: &Mutex<usize>, cap: usize) -> bool {
    let mut count = count.lock();
    if *count >= cap {
        return false;
    }
    *count += 1;
    true
}

/// Returns `true` if compiling a contract against `a` would land under a
/// different on-disk cache key than compiling against `b`.
pub fn cache_keys_differ(a: Arc<Config>, b: Arc<Config>) -> bool {
    config_cache_key_signature(a) != config_cache_key_signature(b)
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
        if cache_keys_differ(Arc::clone(&current_config), Arc::clone(&next_config)) {
            spawn_cache_warming(code.clone(), next_config, cache.handle());
        }
    }
    let _ = precompile_contract(code, Arc::clone(&current_config), cache);
}

/// Eager warming spawn used by [`precompile_contract_with_warming`] on the
/// deploy hot path
fn spawn_cache_warming(
    code: ContractCode,
    config: Arc<Config>,
    cache_handle: Box<dyn ContractRuntimeCache>,
) {
    let code = Arc::new(code);
    spawn_warming(Box::new(move || Some(code)), config, cache_handle);
}

/// Lazy warming spawn used by the pipelining path
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

/// Returns the warming pool only when contract cache pre-warming is enabled
fn try_get_warming_pool() -> Option<&'static Arc<ThreadPool>> {
    match WARMING_PENDING_SUBMISSIONS_CAP.get() {
        None | Some(0) => None,
        Some(_) => contract_warming_pool(),
    }
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
        assert!(!cache_keys_differ(a, b));
    }

    #[test]
    fn signatures_differ_for_distinct_vm_kinds() {
        let a = vm_config_with(VMKind::Wasmtime);
        let b = vm_config_with(VMKind::NearVm);
        assert!(cache_keys_differ(a, b));
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
        assert!(cache_keys_differ(a, b));
    }

    #[test]
    fn pending_cap_admits_up_to_capacity() {
        let count = Mutex::new(0);
        let cap = 5;
        for _ in 0..cap {
            assert!(try_reserve_pending_slot_with_custom_cap(&count, cap));
        }
        assert!(!try_reserve_pending_slot_with_custom_cap(&count, cap));
        *count.lock() -= 1;
        assert!(try_reserve_pending_slot_with_custom_cap(&count, cap));
    }

    #[test]
    fn pending_cap_zero_rejects_everything() {
        let count = Mutex::new(0);
        assert!(!try_reserve_pending_slot_with_custom_cap(&count, 0));
    }
}
