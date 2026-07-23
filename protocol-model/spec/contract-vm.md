# Smart-contract VM

> Protocol version: 86 (stable) · Release: 2.13.0 · Derived from commit: 499283a · Generated: 2026-07-09
> Primary crates/files: `runtime/near-vm-runner/src/runner.rs`, `runtime/near-vm-runner/src/logic/logic.rs`, `runtime/near-vm-runner/src/logic/gas_counter.rs`, `runtime/near-vm-runner/src/logic/context.rs`, `runtime/near-vm-runner/src/logic/recorded_storage_counter.rs`, `runtime/near-vm-runner/src/logic/errors.rs`, `runtime/near-vm-runner/src/cache.rs`, `runtime/near-vm-runner/src/prepare.rs`, `runtime/near-vm-runner/src/wasmtime_runner/mod.rs`, `core/parameters/src/vm.rs`, `core/parameters/src/cost.rs`

## Role

This component executes WASM smart-contract code for the `FunctionCall` action. It is invoked by the runtime ([runtime-execution](runtime-execution.md)) once per executed function call: the runtime hands it a `VMContext` (inputs, balances, prepaid gas), a `Config`/`RuntimeFeesConfig`, and an `External` trait object that proxies trie reads/writes and receipt creation. The VM validates and instruments the WASM (`prepare`), links it against the host-function (bindings) ABI exposed by `VMLogic`, meters every WASM op and host call against a gas counter, and runs the requested entry point. Host calls let the contract read/write contract storage (delegated to [state-storage](state-storage.md)), read context/economics, compute hashes/signatures, and **create promises** that the runtime turns into receipts handled by [cross-shard-congestion](cross-shard-congestion.md). The result is a `VMOutcome` (burnt/used gas, compute usage, logs, return data, new receipts, and an optional `aborted` guest error). Execution is a deterministic pure function of its inputs: gas and outcome must be byte-identical on every validator (`runtime/near-vm-runner/src/runner.rs:16` — `VMResult` doc). Out of scope here: how the runtime drives the `FunctionCall` action loop and distributes gas weights into receipts → [runtime-execution](runtime-execution.md); the trie itself → [state-storage](state-storage.md).

## Key data structures

- **`VMContext`** — `runtime/near-vm-runner/src/logic/context.rs:11` — immutable per-call context extracted from the receipt/account: `current_account_id`, `signer_account_id`/`signer_account_pk`, `predecessor_account_id`, `refund_to_account_id`, `input` (`Rc<[u8]>`), `promise_results`, `block_height`/`block_timestamp`/`epoch_height`, `account_balance`/`account_locked_balance`/`storage_usage`/`account_contract`, `attached_deposit`, `prepaid_gas`, `random_seed`, `view_config`, `output_data_receivers`. `is_view()` (`:68`) returns `view_config.is_some()` and gates all state-mutating/private host functions.
- **`VMLogic`** — `runtime/near-vm-runner/src/logic/logic.rs:158` — the host side of the contract↔host boundary. Holds `ext: &mut dyn External`, `context`, `memory`, `config: Arc<Config>`, `fees_config`, `registers`, `promises: Vec<Promise>` (the promise DAG), `remaining_stack`, `recorded_storage_counter`, and `result_state`. Most `pub fn` on it are host functions.
- **`ExecutionResultState`** — `runtime/near-vm-runner/src/logic/logic.rs:38` — the subset of `VMLogic` needed to build a `VMOutcome`: `config`, `gas_counter`, `logs`, `total_log_length`, `return_data`, `current_account_balance`, `subsidized_amount`, `current_storage_usage`. `compute_outcome` (`:131`) finalizes it into a `VMOutcome`.
- **`Promise`** — `runtime/near-vm-runner/src/logic/logic.rs:197` — a node of the promise DAG. `Receipt(ReceiptIndex)` is a real receipt; `NotReceipt(Vec<ReceiptIndex>)` is the ephemeral join created by `promise_and` (holds its receipt set, has no receipt of its own).
- **`VMOutcome`** — `runtime/near-vm-runner/src/logic/logic.rs:4481` — result of a run: `balance`, `storage_usage`, `return_data`, `burnt_gas`, `used_gas`, `compute_usage`, `logs`, `profile` (`ProfileDataV3`), `aborted: Option<FunctionCallError>` (guest error — a graceful outcome committed on-chain), `subsidized_amount`. Constructed via `ok`/`abort`/`nop_outcome`/`abort_but_nop_outcome_in_old_protocol` (`:4497`–`:4543`).
- **`GasCounter`** — `runtime/near-vm-runner/src/logic/gas_counter.rs:65` — tracks burnt vs. used gas. Wraps a `FastGasCounter` (`:29`, `#[repr(C)]`, exposed to compiled code for inline metering: `burnt_gas`/`gas_limit`/`opcode_cost`) plus `promises_gas`, `max_gas_burnt`, `prepaid_gas`, `is_view`, `ext_costs_config`, `profile`, `send_action_compute_usage`.
- **`RecordedStorageCounter`** — `runtime/near-vm-runner/src/logic/recorded_storage_counter.rs:6` — tracks the recorded trie-storage-proof size and errors with `RecordedStorageExceeded` when it exceeds `per_receipt_storage_proof_size_limit`.
- **`Config`** — `core/parameters/src/vm.rs:181` — the VM config: `ext_costs: ExtCostsConfig`, `grow_mem_cost`, `regular_op_cost`, `linear_op_base_cost`/`linear_op_unit_cost`, `vm_kind: VMKind`, `storage_get_mode`, and boolean feature gates (`fix_contract_loading_cost`, `eth_implicit_accounts`, `eth_implicit_global_contract`, `discard_custom_sections`, `global_contract_host_fns`, `reftypes_bulk_memory`, `gas_key_host_fns`, `one_yocto_on_promise`, `p256_verify_host_fn`, `yield_with_id_host_fns`, `chain_id_host_fn`, `bls12381_not_in_group_fix`), and `limit_config: LimitConfig`. `non_crypto_hash` (`:254`) feeds the cache key. (Note: there is **no** `fix_contract_loading_error` field on 2.13.0.)
- **`LimitConfig`** — `core/parameters/src/vm.rs:59` — VM/runtime limits: `max_gas_burnt`, `max_stack_height`, `initial_memory_pages`/`max_memory_pages`, register limits, log limits, `per_receipt_storage_proof_size_limit` (`:170`), plus the optional wasm-shape caps (`max_functions_number_per_contract`, `max_locals_per_contract`, `max_operand_stack_bytes_per_function`, `max_blocks_per_function`/`_contract`, `max_instrumented_code_size`, etc.).
- **`VMKind`** — `core/parameters/src/vm.rs:30` — `Wasmer0`, `Wasmtime`, `Wasmer2`, `NearVm`. `Wasmer0`/`Wasmer2` are removed: `is_available` returns false for them (`runtime/near-vm-runner/src/runner.rs:205`).
- **`ContractCacheKey` / `CompiledContractInfo`** — `runtime/near-vm-runner/src/cache.rs:41` / `:108` — on-disk cache key (`Version5 { code_hash, vm_config_non_crypto_hash, vm_kind, vm_hash }`) and stored value (`wasm_bytes: u64` + `CompiledContract::Code(Vec<u8>) | CompileModuleError`).

## Behavior

### 1. VM backend selection
`VMKind` comes from `Config::vm_kind`, populated from the versioned parameter table (`core/parameters/src/parameter_table.rs:456+`). On 2.13.0 mainnet the active kind is **`Wasmtime`**, flipped from `NearVm` at PV 84 (`core/parameters/res/runtime_configs/84.yaml`: `vm_kind: { old: "NearVm", new: "Wasmtime" }`). `VMKind::runtime` (`runtime/near-vm-runner/src/runner.rs:214`) instantiates the backend, returning `None` for the removed `Wasmer0`/`Wasmer2` kinds (`is_available` returns false for them at `:208`/`:209`); the public wrappers then panic via `unwrap_or_else` when the runtime is absent (`prepare` at `:62`). `replace_with_wasmtime_if_unsupported` (`core/parameters/src/vm.rs:42`) forces Wasmtime on non-x86_64 (NearVm is x86_64-only). The public entry points `prepare`/`run` (`runner.rs:54`, `:89`) dispatch through this `VM` trait.

### 2. Contract preparation (`prepare`)
`prepare_contract` (`runtime/near-vm-runner/src/prepare.rs:22`) validates and instruments the WASM. It selects the preparation pipeline by config: `prepare_v3` when `reftypes_bulk_memory` is set **or** `vm_kind == Wasmtime`, else `prepare_v2` (`:28`). On 2.13.0 both conditions hold (Wasmtime + `reftypes_bulk_memory` true at PV 84 per `84.yaml`), so **prepare_v3** is used. Checks performed (per the doc block at `:11` and enforced in the v2/v3 passes): no internal memory instance; imported memory within `max_memory_pages`; imports only from the `env` module; function/local/table/type/param counts and body/block/instrumented-code sizes within `LimitConfig`; operand-stack bytes per function bounded. Failures surface as `PrepareError` variants (`runtime/near-vm-runner/src/logic/errors.rs:166`), wrapped in `CompilationError::PrepareError` (`:131`). The pass injects gas-metering and stack-height instrumentation (finite-wasm), so instrumented code is larger than the source.

### 3. Compile + load + loading fee (`with_compiled_and_loaded`)
`WasmtimeVM::with_compiled_and_loaded` (`runtime/near-vm-runner/src/wasmtime_runner/mod.rs:683`) resolves the compiled artifact through a two-level cache (see §7), then charges the loading fee. Ordering (load-bearing):
1. Look up `ContractCacheKey` in the in-memory `AnyCache`, then the on-disk cache; on a miss, fetch code via `Contract::get_code` and `compile_and_cache` (`:606`); a compile failure is stored/returned as `CompiledContract::CompileModuleError` (`mod.rs:723`).
2. `deserialize` the module, resolve the `memory` export (missing ⇒ `LinkError`), build a `Linker`, `link` host functions (`mod.rs:1127`), and `instantiate_pre`.
3. `before_loading_executable` (`gas_counter.rs:236`): reject empty `method_name` (`MethodResolveError::MethodEmptyName`); if `fix_contract_loading_cost` is set, pre-charge `add_contract_loading_fee` (`contract_loading_base` + `contract_loading_bytes * code_len`, `gas_counter.rs:225`) — on OOG return `HostError::GasExceeded` as an abort.
4. `after_loading_executable` (`gas_counter.rs:260`): if `fix_contract_loading_cost` is **not** set, charge the loading fee *after* loading instead (legacy ordering). On 2.13.0 mainnet `fix_contract_loading_cost` is `false` (the fix is nightly-only, PV 129), so the loading fee is charged post-load.

### 4. Run (`PreparedContract::run`)
`run` (`runtime/near-vm-runner/src/wasmtime_runner/mod.rs:983`) turns the `PreparedContract` into a `VMOutcome`:
1. Build `ExecutionResultState::new` (`logic.rs:66`) — panics if `account_balance + attached_deposit` overflows `u128`.
2. If preparation produced `OutcomeAbort`/`OutcomeAbortButNopInOldProtocol`, return immediately (`mod.rs:994`–`999`) via `abort`/`abort_but_nop_outcome_in_old_protocol`.
3. Create the `Ctx`/`Store`, acquire a concurrency permit (failure ⇒ `LinkError "failed to acquire execution slot"`), and `instantiate`.
4. Install a `call_hook` synchronizing the compiled `remaining_gas` global with the `GasCounter` on every host/wasm boundary crossing (`mod.rs:1047`): burns the delta on entering the host and re-publishes remaining gas on returning to wasm. This is how inline-metered wasm gas reaches the counter.
5. Run the optional `start` function, then the resolved method (`call`, `mod.rs:963`). A `MethodNotFound` at call time yields `AbortNop`.
6. Map the run result: `Ok ⇒ VMOutcome::ok`, `AbortNop ⇒ abort_but_nop_outcome_in_old_protocol`, `Abort ⇒ abort` (`mod.rs:1086`).

The NearVm backend follows the identical loading-fee/outcome ordering (`runtime/near-vm-runner/src/near_vm_runner/runner.rs:350`, `:728`).

### 5. Gas metering
Two categories of gas are metered, both through `GasCounter`:
- **WASM instruction cost.** finite-wasm instrumentation calls `finite_wasm_gas`/`gas_opcodes` (`logic.rs:319`, `:1960`) charging `opcodes * regular_op_cost`; bulk memory/table ops use `linear_gas` (`:323`). Stack accounting is done by `finite_wasm_stack`/`_unstack` (`:384`) against `remaining_stack`.
- **Host-function `ExtCosts`.** Each host fn charges via `pay_base`/`pay_per` (`gas_counter.rs:306`, `:291`) — e.g. every host fn starts with `pay_base(base)`. Action-shaped costs (creating receipts) go through `pay_action_accumulated` (`gas_counter.rs:323`), which also accumulates `send_action_compute_usage`.

Burnt vs. used: `burn_gas` (`gas_counter.rs:148`) adds to `fast_counter.burnt_gas`, capped by `gas_limit = min(max_gas_burnt, prepaid_gas)`. `deduct_gas` (`:119`) splits a `(burnt, used)` pair, moving the difference into `promises_gas` (gas reserved for prepaid promise execution) and shrinking `gas_limit` accordingly (`:132`). `used_gas = burnt_gas + promises_gas` (`:385`). On limit crossing, `process_gas_limit` (`:169`) clamps burnt to `min(prepaid_gas, max_gas_burnt)` and returns `GasLimitExceeded` (max-burnt crossed) or `GasExceeded` (prepaid crossed), preferring `GasLimitExceeded`. In view mode `prepaid_gas` is forced to `Gas::MAX` (`:94`).

`compute_outcome` (`logic.rs:131`) computes `compute_usage` from the profile via `total_compute_usage` (NEP-455 compute costs), decoupled from gas.

### 6. Host-function (bindings) API by category
Every host fn is a `pub fn` on `VMLogic`. Categories:
- **Registers** — `read_register`/`register_len`/`write_register` (`logic.rs:449`/`:466`/`:484`). Registers are host-side scratch blobs bounded by `registers_memory_limit`/`max_number_registers`/`max_register_size`.
- **Context** — `current_account_id`, `signer_account_id`, `signer_account_pk`, `predecessor_account_id`, `refund_to_account_id`, `input`, `block_index`, `block_timestamp`, `epoch_height`, `storage_usage`, `chain_id` (`logic.rs:671`–`:920`). `signer_*`/`predecessor_*`/`refund_to_*` are `ProhibitedInView`.
- **Economics** — `account_balance`, `account_locked_balance`, `attached_deposit`, `prepaid_gas`, `used_gas` (`logic.rs:932`–`:1009`); the last three are `ProhibitedInView`.
- **Math/crypto** — the `alt_bn128_g1_multiexp`/`_g1_sum`/`_pairing_check` family (`logic.rs:1044`/`:1094`/`:1143`) and the BLS12-381 family generated by the `bls12381_impl!` macro (`:1157`+): `bls12381_p1_sum`, `p2_sum`, `g1_multiexp`, `g2_multiexp`, plus map/decompress/pairing; then `random_seed` (`:1569`), `sha256` (`:1589`), `keccak256`/`keccak512` (`:1615`/`:1641`), `ripemd160` (`:1669`), `ecrecover` (`:1713`), `ed25519_verify` (`:1815`), `p256_verify` (`:1900`, gated by `p256_verify_host_fn`).
- **Validator** — `validator_stake`, `validator_total_stake` (`logic.rs:876`, `:896`).
- **Storage** — `storage_write`, `storage_read`, `storage_remove`, `storage_has_key` (`logic.rs:4093`, `:4184`, `:4247`, `:4303`); iterators (`storage_iter_*`, `:4355`+) are disabled and always error. Writes/reads delegate to `ext.storage_set`/`storage_get` ([state-storage](state-storage.md)) and update `current_storage_usage`; each op calls `recorded_storage_counter.observe_size` (`logic.rs:4128`, `:4213`).
- **Promises** — see §6.1.
- **Miscellaneous** — `value_return` (`logic.rs:3825`), `panic`/`panic_utf8` (`:3874`/`:3890`, both raise `GuestPanic`), `log_utf8`/`log_utf16` (`:3910`/`:3934`), `abort` (`:3959`), `gas` (`:1956`).

#### 6.1 Promises → receipts
`promise_batch_create` (`logic.rs:2213`) reads/parses the target account, charges `pay_gas_for_new_receipt` (`:2004`), and calls `ext.create_action_receipt`, pushing a `Promise::Receipt`. `promise_batch_then` (`:2254`) additionally wires the new receipt's data dependencies to the receipts of `promise_idx`. `promise_create`/`promise_then` (`:2045`/`:2088`) are thin wrappers over the batch pair plus a function-call action (charging `base` twice, per their doc). `promise_and` (`:2139`) builds a `Promise::NotReceipt` joining several promises' receipt sets. `promise_batch_action_*` (`:2365`–`:3450`) append actions (CreateAccount, DeployContract/GlobalContract, UseGlobalContract, StateInit, FunctionCall(_weight), Transfer, AddKey, Stake, DeleteKey, DeleteAccount, gas-key variants) to a batch's receipt via `ext`. `promise_batch_action_function_call` (`:2794`) forwards to `_function_call_weight` (`:2852`) with weight 0; a nonzero weight defers gas assignment to the runtime's unused-gas distribution. Yield/resume: `promise_yield_create`/`_with_id` (`:3450`/`:3517`) and `promise_yield_resume`/`_with_yield_id` (`:3631`/`:3670`; the `_with_id`/`_with_yield_id` pair gated by `yield_with_id_host_fns`). `promise_return` (`:3789`) sets `ReturnData::ReceiptIndex` (joint promise ⇒ `CannotReturnJointPromise`). `checked_push_promise` (`:630`) enforces `max_promises_per_function_call_action`. All promise host fns are `ProhibitedInView`. The actual receipt objects and cross-shard delivery are owned by [runtime-execution](runtime-execution.md)/[cross-shard-congestion](cross-shard-congestion.md).

### 7. Compiled-contract caching
The cache key `ContractCacheKey::Version5` (`cache.rs:46`) hashes `code_hash`, `Config::non_crypto_hash()`, `vm_kind`, and the backend `vm_hash` (`get_contract_cache_key`, `:55`) — so any config/VM change invalidates all entries. Two levels: a per-VM in-memory `AnyCache` of loaded artifacts (`cache.rs:1058`, weight-bounded LRU) and an on-disk `FilesystemContractRuntimeCache` (`:302`) storing serialized executables or a `CompileModuleError`, with a size-bounded LRU eviction over the directory. Caching exists because compilation is expensive; the stored value carries `wasm_bytes` so the loading fee can be charged without re-reading source. `precompile_contract` (`cache.rs:1183`) warms the cache; `on_protocol_version_update` (`:862`) sweeps stale files at the `Wasmtime` cutover (PV 84).

## Interactions

- **Invoked by** the runtime for the `FunctionCall` action; the runtime supplies `VMContext`, `Config`, `RuntimeFeesConfig`, and the `External` impl, and consumes the `VMOutcome` → [runtime-execution](runtime-execution.md).
- **Emits promises** that the runtime materializes as action/data receipts, then routed and throttled by [cross-shard-congestion](cross-shard-congestion.md).
- **Storage host calls** delegate to `ext` (trie/flat-storage) → [state-storage](state-storage.md); `storage_get_mode` selects flat-storage vs. trie reads.
- **Recorded storage proof** feeds stateless validation; the per-receipt proof-size bound is seeded by the runtime and enforced here → [stateless-validation](stateless-validation.md).
- **Costs** (`ExtCosts`, `ActionCosts`, wasm op costs) come from the versioned parameter table → [economics](economics.md), [genesis-configuration](genesis-configuration.md).

## Protocol-version-gated behavior

All activation versions verified against `core/primitives-core/src/version.rs` in this tree.

- **`Wasmtime`** — activates **PV 84** (`version.rs:558`). Switches `vm_kind` from `NearVm` to `Wasmtime` and (with `reftypes_bulk_memory`, also PV 84) moves preparation to `prepare_v3`. On 2.13.0 (PV 86) the active backend is Wasmtime. `on_protocol_version_update` sweeps the on-disk cache once at this cutover (`cache.rs:864`).
- **`EnforcePerReceiptStorageProofLimit`** — activates **PV 86** (`version.rs:576`); the PV-86 feature of this release. The runtime seeds `storage_proof_size_before_receipt` only when this is enabled (`runtime/runtime/src/lib.rs:839`); the VM's `RecordedStorageCounter` then enforces `per_receipt_storage_proof_size_limit`, raising `HostError::RecordedStorageExceeded` (`recorded_storage_counter.rs:27`) when a receipt's proof grows past the limit. Before PV 86 the bound is effectively unset.
- **`YieldWithId`** — activates **PV 85** (`version.rs:570`). Gates `yield_with_id_host_fns`, enabling `promise_yield_create_with_id`/`promise_yield_resume_with_yield_id`.
- **`EthImplicitGlobalContract`** — activates **PV 83** (`version.rs:556`). Gates `eth_implicit_global_contract`, using a global contract for ETH-implicit accounts instead of embedded WASM.
- **`FixContractLoadingCost`** — **nightly only, PV 129** (`version.rs:579`); **not active on 2.13.0**. When enabled, `fix_contract_loading_cost` pre-charges the loading fee in `before_loading_executable` and makes loading-phase failures `abort` (committed) rather than `nop_outcome`; on stable it stays `false`, so the fee is charged post-load and loading-phase resolve errors return NOP outcomes (`gas_counter.rs:248`/`:265`, `logic.rs:4533` `abort_but_nop_outcome_in_old_protocol`).
- **Compute costs (NEP-455)** — long stabilized (`_DeprecatedComputeCosts`, PV 61, `version.rs:506`). `compute_usage` is computed independently of gas in `compute_outcome` (`logic.rs:137`) and carried on `VMOutcome`.

Older VM-relevant transitions are historical/deprecated on this release (e.g. `_DeprecatedPreparationV2`/`_DeprecatedNearVmRuntime` PV 62, `_DeprecatedAltBn128` PV 55, `_DeprecatedBLS12381` PV 70). `MIN_SUPPORTED_PROTOCOL_VERSION` is 83 (`version.rs:600`), so Wasmer0/Wasmer2 backends are removed.

## Invariants & failure modes

- **Determinism.** Gas values and the outcome (including a guest error) must be identical across validators; `VMRunnerError` (not `FunctionCallError`) signals node-local corruption and should crash/challenge rather than diverge (`runner.rs:16`, `errors.rs:12`).
- **Graceful guest errors vs. runner errors.** A `FunctionCallError` (`errors.rs:42`) — `CompilationError`, `LinkError`, `MethodResolveError`, `WasmTrap`, `HostError`, `LoadingError`, `WasmUnknownError` — becomes `VMOutcome.aborted` and is committed on-chain. A `VMRunnerError` propagates as `Err` from `run`.
- **Gas limits.** Never burn more than `min(prepaid_gas, max_gas_burnt)`; enforced in `burn_gas`/`process_gas_limit` (`gas_counter.rs:152`/`:169`). Out-of-gas ⇒ `GasLimitExceeded`/`GasExceeded`.
- **View-mode restrictions.** State-mutating and private host fns return `HostError::ProhibitedInView` in view calls (e.g. `storage_write` `logic.rs:4102`, all promise fns).
- **Limit enforcement.** `NumberPromisesExceeded` (`logic.rs:636`), `KeyLengthExceeded`/`ValueLengthExceeded` (`:4110`/`:4118`), `TotalLogLengthExceeded`/`NumberOfLogsExceeded` (`:118`/`:97`), `ReturnedValueLengthExceeded` (`:3831`), `RecordedStorageExceeded` (`recorded_storage_counter.rs:27`).
- **Balance.** `deduct_balance` errors with `BalanceExceeded` (`logic.rs:89`); `ExecutionResultState::new` panics if starting balance overflows `u128` (`:70`) — a should-never-happen invariant guaranteed by the runtime.
- **Preparation defense-in-depth.** `CompilationError::WasmtimeCompileError` (`errors.rs:141`) is emitted if the backend rejects a module our own preparation pass should have caught.
- **Loading-phase failures.** On stable (no `fix_contract_loading_cost`), an empty method name or method-resolve failure yields a NOP outcome via `abort_but_nop_outcome_in_old_protocol` (`logic.rs:4533`).

## Code anchors

| Location | Symbol | What happens here |
| --- | --- | --- |
| `runtime/near-vm-runner/src/runner.rs:54` | `prepare` | Public entry: dispatch preparation to the selected `VMKind`. |
| `runtime/near-vm-runner/src/runner.rs:89` | `run` | Public entry: run a `PreparedContract`, record metrics. |
| `runtime/near-vm-runner/src/runner.rs:205` | `VMKindExt::is_available` | Wasmer0/Wasmer2 removed (false); Wasmtime/NearVm gated by cargo features. |
| `runtime/near-vm-runner/src/runner.rs:214` | `VMKindExt::runtime` | Build the backend `VM`; return `None` for removed kinds (callers then panic). |
| `runtime/near-vm-runner/src/prepare.rs:22` | `prepare_contract` | Validate + instrument; choose prepare_v3 (Wasmtime/reftypes) vs prepare_v2. |
| `runtime/near-vm-runner/src/wasmtime_runner/mod.rs:683` | `with_compiled_and_loaded` | Cache lookup, compile, deserialize, link, instantiate_pre, loading fee. |
| `runtime/near-vm-runner/src/wasmtime_runner/mod.rs:797` | `before_loading_executable` call | Pre-load fee/method-name check ordering. |
| `runtime/near-vm-runner/src/wasmtime_runner/mod.rs:983` | `PreparedContract::run` | Build outcome; instantiate; gas call_hook; run method. |
| `runtime/near-vm-runner/src/wasmtime_runner/mod.rs:1127` | `link` | Link host functions into the wasmtime `Linker`. |
| `runtime/near-vm-runner/src/logic/gas_counter.rs:119` | `GasCounter::deduct_gas` | Split burnt/used, reserve promises_gas, shrink gas_limit. |
| `runtime/near-vm-runner/src/logic/gas_counter.rs:169` | `process_gas_limit` | Clamp on OOG; choose GasLimitExceeded vs GasExceeded. |
| `runtime/near-vm-runner/src/logic/gas_counter.rs:225` | `add_contract_loading_fee` | contract_loading_base + contract_loading_bytes*len. |
| `runtime/near-vm-runner/src/logic/gas_counter.rs:248` | `before_loading_executable` | Pre-charge loading fee iff fix_contract_loading_cost. |
| `runtime/near-vm-runner/src/logic/logic.rs:131` | `compute_outcome` | Finalize burnt/used gas, profile, compute_usage into VMOutcome. |
| `runtime/near-vm-runner/src/logic/logic.rs:2004` | `pay_gas_for_new_receipt` | Charge receipt dispatch/exec + data-receipt base costs. |
| `runtime/near-vm-runner/src/logic/logic.rs:2213` | `promise_batch_create` | Create action receipt; push Promise::Receipt. |
| `runtime/near-vm-runner/src/logic/logic.rs:2852` | `promise_batch_action_function_call_weight` | Append FunctionCall action; optional gas weight. |
| `runtime/near-vm-runner/src/logic/logic.rs:4093` | `storage_write` | Limit-check key/value; ext.storage_set; update storage_usage. |
| `runtime/near-vm-runner/src/logic/logic.rs:4533` | `abort_but_nop_outcome_in_old_protocol` | NOP outcome unless fix_contract_loading_cost. |
| `runtime/near-vm-runner/src/logic/recorded_storage_counter.rs:19` | `observe_size` | Enforce per-receipt storage proof size limit. |
| `runtime/near-vm-runner/src/cache.rs:55` | `get_contract_cache_key` | Version5 key over code_hash/config hash/vm_kind/vm_hash. |
| `runtime/near-vm-runner/src/cache.rs:862` | `on_protocol_version_update` | Sweep on-disk cache at the Wasmtime cutover. |
| `core/parameters/src/vm.rs:181` | `Config` | VM config + boolean feature gates (no fix_contract_loading_error field). |
| `core/parameters/src/vm.rs:59` | `LimitConfig` | Memory/stack/count/proof-size limits. |
| `core/primitives-core/src/version.rs:558` | `ProtocolFeature::Wasmtime` | PV 84 — Wasmtime backend. |
| `core/primitives-core/src/version.rs:576` | `EnforcePerReceiptStorageProofLimit` | PV 86 — per-receipt proof-size enforcement. |
| `runtime/runtime/src/lib.rs:839` | storage_proof_size_before_receipt | Runtime seeds the proof bound iff PV≥86. |

## Open questions

- The exact per-check ordering inside `prepare_v3`/`prepare_v2` (which limit is reported first when several are violated) is only spot-checked via the tests in `prepare.rs`; the module-internal `instrument_v3`/`prepare_v2`/`prepare_v3` files were not read line-by-line here.
- NearVm remains selectable (x86_64) and its `run` path mirrors Wasmtime, but this spec derives ordering primarily from the Wasmtime backend; any NearVm-specific instrumentation-skipping (e.g. block limits not enforced, per `prepare.rs` tests) is noted only indirectly.
