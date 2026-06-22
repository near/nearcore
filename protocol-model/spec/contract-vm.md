# Smart-contract VM

> Protocol version: 86 (stable) · Derived from commit: f0c7706f · Generated: 2026-06-22
> Primary crates/files: `runtime/near-vm-runner/src/runner.rs`, `runtime/near-vm-runner/src/logic/logic.rs`, `runtime/near-vm-runner/src/logic/gas_counter.rs`, `runtime/near-vm-runner/src/logic/context.rs`, `runtime/near-vm-runner/src/cache.rs`, `runtime/near-vm-runner/src/prepare.rs`, `runtime/near-vm-runner/src/wasmtime_runner/mod.rs`, `core/parameters/src/vm.rs`, `core/parameters/src/cost.rs`

## Role

This component executes WASM smart-contract code for the `FunctionCall` action. It is invoked by the runtime ([runtime-execution](runtime-execution.md)) once per executed function call: the runtime hands it a prepared `VMContext` (inputs, balances, prepaid gas), a `RuntimeConfig`, and an `External` trait object that proxies trie reads/writes and receipt creation. The VM validates and instruments the WASM, links it against the host-function (bindings) ABI exposed by `VMLogic`, meters every WASM op and host call against a gas counter, and runs the requested entry point. Host calls let the contract read/write contract storage (delegated to [state-storage](state-storage.md)), read context/economics, compute hashes/signatures, and **create promises** that the runtime turns into receipts handled by [cross-shard-congestion](cross-shard-congestion.md). The result is a `VMOutcome` (burnt/used gas, compute usage, logs, return data, new receipts, and an optional `aborted` guest error). It is a deterministic pure function of its inputs: gas and outcome must be byte-identical on every validator. Out of scope here: how the runtime drives this VM (the `FunctionCall` action loop, gas weight distribution into receipts) → [runtime-execution](runtime-execution.md).

## Key data structures

- **`VMContext`** — `runtime/near-vm-runner/src/logic/context.rs:11` — immutable per-call context extracted from the receipt/account: `current_account_id`, `signer_account_id`/`pk`, `predecessor_account_id`, `refund_to_account_id`, `input` (`Rc<[u8]>`), `promise_results`, `block_height`/`timestamp`/`epoch_height`, `account_balance`/`account_locked_balance`/`storage_usage`/`account_contract`, `attached_deposit`, `prepaid_gas`, `random_seed`, `view_config` (`Some` ⇒ read-only view call), `output_data_receivers`. `is_view` (`:68`) gates all state-mutating/private host functions.
- **`VMLogic`** — `runtime/near-vm-runner/src/logic/logic.rs:158` — the host side of the contract↔host boundary. Holds `ext: &mut dyn External`, `context`, `memory` (guest linear memory view), `config: Arc<Config>`, `fees_config`, `registers`, `promises: Vec<Promise>` (the promise DAG), `remaining_stack`, `recorded_storage_counter`, and `result_state`. Every `pub fn` on it is a host function.
- **`ExecutionResultState`** — `runtime/near-vm-runner/src/logic/logic.rs:38` — the subset of `VMLogic` needed to build a `VMOutcome`: `config`, `gas_counter`, `logs`, `total_log_length`, `return_data`, `current_account_balance`, `subsidized_amount`, `current_storage_usage`. `compute_outcome` (`:131`) finalizes it into a `VMOutcome`.
- **`Promise`** — `runtime/near-vm-runner/src/logic/logic.rs:197` — a node of the promise DAG. `Receipt(ReceiptIndex)` is a real receipt; `NotReceipt(Vec<ReceiptIndex>)` is the ephemeral join created by `promise_and` (its receipt set, no receipt of its own).
- **`VMOutcome`** — `runtime/near-vm-runner/src/logic/logic.rs:4427` — result of a run: `balance`, `storage_usage`, `return_data`, `burnt_gas`, `used_gas`, `compute_usage`, `logs`, `profile` (`ProfileDataV3`), `aborted: Option<FunctionCallError>` (guest error — a graceful outcome, committed on-chain), `subsidized_amount`. Constructed via `ok`/`abort`/`nop_outcome`/`abort_but_nop_outcome_in_old_protocol` (`:4446`–`:4488`).
- **`GasCounter`** — `runtime/near-vm-runner/src/logic/gas_counter.rs:65` — tracks burnt vs. used gas. Wraps a `FastGasCounter` (`:29`, `#[repr(C)]`, exposed to compiled code for inline metering) plus `promises_gas`, `max_gas_burnt`, `prepaid_gas`, `is_view`, `ext_costs_config`, `profile`, `send_action_compute_usage`.
- **`Config`** — `core/parameters/src/vm.rs:181` — the VM config: `ext_costs: ExtCostsConfig`, `regular_op_cost: u32`, `vm_kind: VMKind`, `fix_contract_loading_cost`/`fix_contract_loading_error` (bool gates), `eth_implicit_accounts`, `reftypes_bulk_memory`, `one_yocto_on_promise`, `limit_config: LimitConfig`. `non_crypto_hash` (`:257`) feeds the cache key.
- **`ContractCacheKey` / `CompiledContractInfo`** — `runtime/near-vm-runner/src/cache.rs:41` / `:108` — on-disk cache key (`Version5 { code_hash, vm_config_non_crypto_hash, vm_kind, vm_hash }`) and stored value (`wasm_bytes` + `CompiledContract::Code | CompileModuleError`).
- **`VMKind`** — `core/parameters/src/vm.rs:30` — `Wasmer0`/`Wasmer2` (both removed; `is_available` is false), `Wasmtime`, `NearVm`.

## Behavior

### VM backend selection

1. The backend is chosen by `config.vm_kind` (`core/parameters/src/vm.rs:198`); the runtime sets this per protocol version. `VMKind::runtime` (`runner.rs:214`) instantiates the matching `VM`. Only `Wasmtime` (when built with `wasmtime_vm`) and `NearVm` (only `x86_64` + `near_vm`) are available; `Wasmer0`/`Wasmer2` return `false` from `is_available` (`runner.rs:205`).
2. On non-`x86_64` targets, `replace_with_wasmtime_if_unsupported` (`core/parameters/src/vm.rs:42`) forces `Wasmtime` since NearVM is x86_64-only.
3. The `Wasmtime` `ProtocolFeature` (version 84, see below) is what flips the production `vm_kind` from `NearVm` to `Wasmtime`; both produce identical gas/outcome.

### Top-level run flow

`run` (`runner.rs:89`) is the entry point: the contract has already been `prepare`d into a `Box<dyn PreparedContract>` (`runner.rs:54`, `prepared.run(...)` at `:98`). Preparation and execution may run on different threads. For the Wasmtime backend:

1. **Resolve & compile/load** — `with_compiled_and_loaded` (`wasmtime_runner/mod.rs:683`) computes the cache key (`get_contract_cache_key`, `:699`), `touch`es it, then looks up the in-memory `AnyCache` → on-disk cache → compiles. Compilation result (a deserialized `Module`, or a cached `FunctionCallError`/`CompilationError`) is memoized.
2. **Charge loading fee (ordering is load-bearing)** — `before_loading_executable` (`gas_counter.rs:236`) rejects an empty method name (`MethodResolveError::MethodEmptyName`) and, **only if `config.fix_contract_loading_cost`**, pre-charges `contract_loading_base + contract_loading_bytes * wasm_len` *before* loading (`:248`). Otherwise the same fee is charged *after* loading by `after_loading_executable` (`:260`). `add_contract_loading_fee` (`:225`) is the shared charge.
3. **Resolve the method** (`prepare`, `wasmtime_runner/mod.rs:888`): the exported entry point must exist (`MethodResolveError::MethodNotFound`) and have signature `() -> ()` (`MethodInvalidSignature`); failures become a `PreparationResult::OutcomeAbortButNopInOldProtocol`.
4. **Run** (`PreparedContract::run`, `wasmtime_runner/mod.rs:997`): build `ExecutionResultState` (`:1004`), and depending on `PreparationResult` either invoke the WASM (`call`, `:977`) or return an abort outcome. `OutcomeAbortButNopInOldProtocol` → `abort_but_nop_outcome_in_old_protocol` (which is a no-op outcome unless `fix_contract_loading_cost`); `OutcomeAbort` → `abort` (gas-bearing); `Ready` → execute and finalize via `compute_outcome`.

### Contract preparation, validation & instrumentation

`prepare_contract` (`prepare.rs:22`) parses, validates, and instruments the WASM:
- Picks `prepare_v3` when `config.reftypes_bulk_memory` is set or `vm_kind == Wasmtime`, else `prepare_v2` (`prepare.rs:28`). Both validate against `WasmFeatures::new(config)`.
- Static checks (enforced largely during instrumentation; **NearVM skips the instrumentation pass** so several caps below are not enforced there — see the `if kind == VMKind::NearVm { return }` guards in `prepare.rs` tests): no internal memory definition; an imported memory must not exceed `limit_config.max_memory_pages` (`PrepareError::Memory`); only the `env` module may be imported (`Instantiate`); function-body size ≤ `max_function_body_size` (`FunctionBodyTooLarge`); ≤ `max_blocks_per_function`/`max_blocks_per_contract` (`TooManyBlocksPer*`); ≤ `max_types_per_contract` (`TooManyTypes`); ≤ `max_functions_number_per_contract` (`TooManyFunctions`); ≤ `max_locals`/params per function and per contract (`TooManyParamsPer*`); instrumented size ≤ `max_instrumented_code_size` (`InstrumentedCodeTooLarge`); peak operand-stack bytes ≤ `max_operand_stack_bytes_per_function` (`OperandStackTooLarge`).
- Instrumentation injects **finite-wasm gas metering** (block-boundary `finite_wasm_gas`, `logic.rs:302`) and **stack-height metering** (`finite_wasm_stack`/`finite_wasm_unstack`, `logic.rs:367`/`:377`, against `remaining_stack` seeded from `limit_config.max_stack_height`). Bulk-memory ops (`memory.copy/fill/init`, `table.copy/fill/init`) are charged via `linear_gas` (`logic.rs:306`).
- WASM-instruction gas: the legacy `gas` import is exposed to WASM as `gas_seen_from_wasm` (`logic.rs:1950`), which calls `gas_opcodes` (`:1935`) to charge `opcodes * regular_op_cost` (the raw `gas` host fn at `:1931` just burns a gas amount); under finite-wasm metering the per-instruction cost is reconstructed in the profile by `compute_wasm_instruction_cost(burnt_gas)` (`logic.rs:136`).

### Gas metering: burnt vs. used vs. prepaid

`GasCounter` (`gas_counter.rs`) distinguishes **burnt** gas (irreversibly consumed by this execution) from **used** gas (burnt + gas attached to promises = `promises_gas`):
1. **Construction** (`new`, `:85`): `gas_limit = min(max_gas_burnt, prepaid_gas)`; for view calls `prepaid_gas` is forced to `Gas::MAX` (`:94`).
2. **`burn_gas`** (`:148`, the hot path, used by `pay_base`/`pay_per`): adds to `burnt_gas`, errors if it exceeds `fast_counter.gas_limit`. `pay_base`/`pay_per` (`:306`/`:291`) look up an `ExtCosts` value via `cost.gas(ext_costs_config)`, charge it, and record it into the profile.
3. **`deduct_gas(burnt, used)`** (`:119`): for promise creation — asserts `burnt <= used`, adds the difference to `promises_gas`, and lowers `fast_counter.gas_limit` to leave room for prepaid promise gas. `prepay_gas` (`:376`) is `deduct_gas(0, gas)`.
4. **Limits** (`process_gas_limit`, `:169`): on overrun, `burnt_gas` is clamped to `min(prepaid_gas, max_gas_burnt)`; returns `GasLimitExceeded` if `max_gas_burnt` was crossed, else `GasExceeded`. The wrapping-vs-saturating handling here is consensus-critical and deliberately preserves historical behavior (the long comment at `:156`–`:206`).
5. **Compute costs (NEP-455)**: distinct from gas. `pay_action_accumulated` (`:323`) accumulates `send_action_compute_usage`; `total_compute_usage` (called from `compute_outcome`, `logic.rs:137`) sums ext-cost compute + send-action compute. On out-of-gas it falls back to charging compute == gas for the last step (`:340`) to preserve compatibility.
6. **Storage TTN fees**: `GasCounter` implements `StorageAccessTracker` (`:400`) so trie traversal charges `touching_trie_node`/`read_cached_trie_node` and evicted/removed value bytes as the host `storage_*` calls run.

### Host-function (bindings) API by category

Every host fn first charges `base` (the per-host-call fee) and, for state-mutating or private ones, returns `ProhibitedInView` when `context.is_view()`. Costs are charged eagerly before doing the work.

| Category | Host functions (`logic.rs`) |
|---|---|
| Registers | `read_register` (`:432`), `register_len` (`:449`), `write_register` (`:467`) — blob scratch space, capped by `max_number_registers`/`max_register_size`/`registers_memory_limit`. |
| Context | `current_account_id` (`:646`), `chain_id` (`:665`), `signer_account_id`/`_pk` (`:689`/`:718`), `predecessor_account_id` (`:747`), `refund_to_account_id` (`:777`), `input` (`:801`), `block_index` (`:820`), `block_timestamp` (`:830`), `epoch_height` (`:840`), `storage_usage` (`:892`), `current_contract_code` (`:3948`). |
| Economics | `account_balance` (`:907`), `account_locked_balance` (`:921`), `attached_deposit` (`:940`), `prepaid_gas` (`:959`), `used_gas` (`:978`). |
| Math/crypto | `sha256` (`:1564`), `keccak256`/`keccak512` (`:1590`/`:1616`), `ripemd160` (`:1644`), `ecrecover` (secp256k1, `:1688`), `ed25519_verify` (`:1790`), `p256_verify` (P-256/NEP-635, `:1875`), `random_seed` (`:1544`). |
| alt_bn128 | `alt_bn128_g1_multiexp` (`:1019`), `alt_bn128_g1_sum` (`:1069`), `alt_bn128_pairing_check` (`:1118`). |
| BLS12-381 | `bls12381_p1_sum`/`p2_sum`, `g1_multiexp`/`g2_multiexp`, `map_fp_to_g1`/`map_fp2_to_g2`, `pairing_check` (`:1428`), `p1_decompress`/`p2_decompress` — generated by `bls12381_impl!` (`:1132`+). |
| Validator | `validator_stake` (`:851`), `validator_total_stake` (`:871`). |
| Promises | see below. |
| Misc | `value_return` (`:3771`), `panic`/`panic_utf8` (`:3820`/`:3836`), `log_utf8`/`log_utf16`/`abort` (`:3856`/`:3880`/`:3905`), `gas`/`gas_seen_from_wasm` (`:1931`/`:1950`). |
| Storage | `storage_write` (`:4039`), `storage_read` (`:4130`), `storage_remove` (`:4193`), `storage_has_key` (`:4249`); iterator fns `storage_iter_*` are **deprecated** and always error `Deprecated` (`:4301`+). |

Storage host calls maintain `current_storage_usage` and delegate the actual trie access to `ext` ([state-storage](state-storage.md)); `storage_read` adds a large-value overhead surcharge above `INLINE_DISK_VALUE_THRESHOLD` (`:4148`). All math/crypto write results into a register and charge `*_base + *_byte/element * n`.

### Promise creation (turning calls into receipts)

Promises build a DAG via `ext` calls that the runtime later materializes into receipts:
1. `promise_batch_create(account_id)` (`logic.rs:2188`) charges `pay_gas_for_new_receipt` (`:1979`, the dispatch+exec fee of `new_action_receipt`) and calls `ext.create_action_receipt`, pushing `Promise::Receipt`.
2. `promise_batch_then(promise_idx, account_id)` (`:2229`) creates a receipt that depends on the receipts of `promise_idx`; charges per data dependency.
3. `promise_and(ids)` (`:2114`) builds an ephemeral `Promise::NotReceipt` joining the dependency set; enforces `max_number_input_data_dependencies`.
4. **Append actions** to a batch's receipt: `promise_batch_action_function_call[_weight]` (`:2769`/`:2827`), `_transfer` (`:2907`), `_stake` (`:3155`), `_deploy_contract` (`:2373`), `_create_account` (`:2340`), add/delete key (`:3196`+/`:3298`), `_delete_account` (`:3334`), global-contract deploy/use (`:2420`+), `_transfer_to_gas_key`/gas-key add (`:2965`/`:3029`), and `DeterministicStateInit` (`:2618`+). Each charges the action's send fee as burnt and (send+exec) as used via `pay_action_base`/`pay_action_per_byte` (`:4394`/`:4403`) or `pay_action_accumulated`. Joint (`NotReceipt`) promises cannot have actions appended (`CannotAppendActionToJointPromise`).
5. `promise_create`/`promise_then` (`:2020`/`:2063`) are convenience wrappers = `promise_batch_create`/`_then` + `_action_function_call` (so they charge `base` twice).
6. **Gas weight**: `promise_batch_action_function_call_weight` (`:2827`) records a `GasWeight`; the runtime later distributes leftover prepaid gas among weighted calls (distribution itself is in [runtime-execution](runtime-execution.md)). `prepay_gas` reserves the attached static gas immediately (`:2864`).
7. **Attached deposit**: deducted from `current_account_balance` (`deduct_balance`, `:89`). Exception: exactly 1 yoctoNEAR on a zero-balance account is **subsidized** (minted, tracked in `subsidized_amount`) when `config.one_yocto_on_promise` (`:2869`).
8. `value_return` (`:3771`) sets `ReturnData::Value` and pre-charges data-receipt byte fees per `output_data_receiver`; `promise_return` (`:3735`) sets `ReturnData::ReceiptIndex` so the promise's result becomes this call's result.

### Yield / resume

`promise_yield_create` (`:3396`) creates a self-addressed receipt with a single (unresolved) data dependency, charges `yield_create_base` + `yield_create_byte`, prepays the callback gas, appends a `FunctionCall` action, and writes a 32-byte resumption `data_id` into a register. `promise_yield_resume` (`:3577`) submits a payload for that `data_id` (bounded by `max_yield_payload_size`). `promise_yield_create_with_id`/`_resume_with_yield_id` (`:3463`/`:3616`) use a caller-chosen `YieldId`; create returns `u64::MAX` if a yield with that id is already pending. If resume never happens within `yield_timeout_length_in_blocks`, the runtime invokes the callback with `PromiseResult::Failed`.

### Outcome finalization

`compute_outcome` (`logic.rs:131`): reads `burnt_gas`/`used_gas` from the counter, reconstructs the per-instruction WASM cost into the profile, computes `compute_usage`, and packages the `VMOutcome`. A guest error becomes `VMOutcome::abort` (gas-bearing, `aborted = Some(err)`); a successful run is `VMOutcome::ok`. `VMRunnerError` (distinct from a guest error) means the node is buggy/corrupted and must crash or ban — it is **not** a deterministic on-chain outcome (`runner.rs:25`).

### Compiled-contract caching

- **Why**: WASM compilation is expensive; the compiled native module is cached so repeat calls skip it.
- **Key**: `get_contract_cache_key` (`cache.rs:55`) hashes `ContractCacheKey::Version5 { code_hash, config.non_crypto_hash(), vm_kind, vm_hash }` — so any config change, VM-kind change, or VM-version bump invalidates the cache automatically.
- **Two tiers**: a per-VM in-memory `AnyCache` of *loaded* artifacts (`cache.rs:1058`, weight-bounded LRU) and an on-disk `FilesystemContractRuntimeCache` (`:302`) of *serialized* compiled modules, size-bounded with an LRU/atime eviction index (`build_disk_index`, `:629`; eviction on `put`, `:717`). `precompile_contract`/`try_precompile_contract` (`:1183`/`:1207`) warm the cache off the hot path; `try_*` returns `ContractAlreadyInCache` instead of blocking on the per-key compile lock.
- **What is cached**: `CompiledContract::Code(bytes)` on success, or `CompileModuleError` so a known-bad contract is not recompiled. On a protocol-version bump to the `Wasmtime` cutover, `on_protocol_version_update` (`:862`) sweeps disk entries older than 24h.
- `contract_cached` (`runner.rs:27`) reports whether a contract is already compiled without compiling.

## Interactions

- **Invoked by**: the runtime `FunctionCall` action handler, which builds `VMContext`/`External` and calls `run`, then folds `VMOutcome` (gas, logs, new receipts, return data) into the receipt's `ActionResult` → [runtime-execution](runtime-execution.md). Gas-weight distribution into receipts also happens there.
- **Produces**: new receipts via `ext.create_action_receipt`/`create_promise_yield_receipt` and appended actions; these become outgoing receipts forwarded/buffered by [cross-shard-congestion](cross-shard-congestion.md).
- **Storage**: `storage_read`/`write`/`remove`/`has_key` and `touching_trie_node`/`read_cached_trie_node` TTN metering delegate to the `External` trie implementation → [state-storage](state-storage.md) (the host ABI is here; the trie internals are there). Recorded-storage size feeds stateless-validation proof limits ([stateless-validation](stateless-validation.md)).
- **Config**: `Config`/`ExtCostsConfig`/`RuntimeFeesConfig` come from the per-version `RuntimeConfig` ([genesis-configuration](genesis-configuration.md)); cost values and limits are defined in `core/parameters/src/cost.rs` and `core/parameters/src/vm.rs`.
- **Validators**: `validator_stake`/`validator_total_stake` read epoch validator data → [epoch-validators-staking](epoch-validators-staking.md).

## Protocol-version-gated behavior

Activation versions verified against `core/primitives-core/src/version.rs:protocol_version` (`:453`+).

| Feature | Version | Effect on this component |
|---|---|---|
| `Wasmtime` | 84 (`version.rs:553`) | Flips production `vm_kind` from `NearVm` to `Wasmtime`. Both VMs are required to produce identical gas/outcome; `on_protocol_version_update` sweeps the disk cache at this cutover (`cache.rs:864`). Active at v86. |
| `FixContractLoadingError` | 86 (`version.rs:572`) | Gates `config.fix_contract_loading_error`. When set, a module-deserialization failure becomes a cached, gas-bearing `FunctionCallError::LoadingError` that flows through the fee-charge points and finalizes as an abort, instead of a `VMRunnerError::LoadingError` that crashes/bans (`wasmtime_runner/mod.rs:750`). **Newly active at v86.** |
| `PostQuantumSignatures` | 85 (`version.rs:562`) | Adds ML-DSA-65 as a signature scheme. Affects transaction/key verification; the VM crypto host fns (`p256_verify`, `ed25519_verify`, `ecrecover`) are unchanged. Active at v86. |
| `GasKeys` / `DelegateV2` | 85 (`version.rs:557`,`:570`) | Enable the gas-key host functions (`promise_batch_action_add_gas_key_with_full_access`/`_function_call`, `_transfer_to_gas_key`, `logic.rs:3029`/`:3086`/`:2965`) and their fee charging (`pay_gas_key_add_key_fees`, `gas_counter.rs:359`). The v85 config bump flips `config.gas_key_host_fns` (`vm.rs:226`, `runtime_configs/85.yaml:3`). Active at v86. |
| `YieldWithId` | 85 (`version.rs:565`) | Flips `config.yield_with_id_host_fns` (`vm.rs:238`, `runtime_configs/85.yaml:5`), enabling the custom-yield-id host fns `promise_yield_create_with_id`/`promise_yield_resume_with_yield_id` (`logic.rs:3463`/`:3616`). Active at v86. |
| `p256_verify` / `chain_id` host fns (NEP-635 / NEP-638) | v85 config bump (`runtime_configs/85.yaml:2`,`:4`) | Flips `config.p256_verify_host_fn` (`vm.rs:234`) and `config.chain_id_host_fn` (`vm.rs:241`), enabling `p256_verify` (`logic.rs:1875`) and `chain_id` (`logic.rs:665`). No dedicated named `ProtocolFeature`; bundled into the v85 config version. Active at v86. |
| `one_yocto_on_promise` | v85 config bump (`runtime_configs/85.yaml:1`) | Flips `config.one_yocto_on_promise` (`vm.rs:230`), enabling the 1-yoctoNEAR subsidy on zero-balance promise function calls / yield-create (`logic.rs:2869`,`:3528`). No dedicated named `ProtocolFeature`. Active at v86. |
| Compute costs (NEP-455) | folded into base config at v86 (the standalone `_DeprecatedComputeCosts` feature was v61, `version.rs:501`) | `compute_usage` is computed separately from gas in `total_compute_usage`/`pay_action_accumulated` (`gas_counter.rs:323`). Always active at v86. |
| Global contracts | folded in (`_DeprecatedGlobalContracts` was v77, `version.rs:533`) | `promise_batch_action_deploy_global_contract[_by_account_id]` / `_use_global_contract[_by_account_id]` / `DeterministicStateInit` host fns (`logic.rs:2420`+,`:2618`+) are available at v86. |
| `FixContractLoadingCost` | 129 — **nightly only** (`version.rs:575`) | Would move the contract-loading fee to *before* loading (`gas_counter.rs:248`) and make `abort_but_nop_outcome_in_old_protocol` gas-bearing (`logic.rs:4483`). **NOT active at v86**; at v86 the loading fee is charged *after* loading (`gas_counter.rs:260`) and method-resolve aborts on missing/bad methods produce a no-op outcome. |

## Invariants & failure modes

- **Determinism**: gas/used-gas/outcome must be byte-identical across validators even on guest error; a non-deterministic path is a `VMRunnerError` (crash/ban), never an on-chain outcome (`runner.rs:25`).
- **`burnt_gas <= used_gas <= prepaid_gas`** and **`burnt_gas <= max_gas_burnt`**: enforced in `deduct_gas`/`burn_gas`/`process_gas_limit` (`gas_counter.rs:119`/`:148`/`:169`). `deduct_gas` asserts `burnt <= used` (`:120`).
- **Out of gas**: `GasLimitExceeded` (crossed `max_gas_burnt`) or `GasExceeded` (crossed `prepaid_gas`) — `process_gas_limit` (`:202`); both clamp `burnt_gas` to `min(prepaid_gas, max_gas_burnt)`.
- **Guest errors** (`HostError`, `logic.rs`): `GuestPanic` (`panic`/`panic_utf8`/`abort`), `ProhibitedInView` (state/private host fn in a view call), `BalanceExceeded` (over-deduct deposit, `:91`), `MemoryAccessViolation`, `BadUTF8`/`BadUTF16`, `InvalidPublicKey`, `InvalidPromiseIndex`, `CannotAppendActionToJointPromise`/`CannotReturnJointPromise`/`CannotSetRefundToOnJointPromise`, `Deprecated` (storage iterators). These become a gas-bearing `VMOutcome::abort`.
- **Limit violations**: `KeyLengthExceeded`/`ValueLengthExceeded` (`:4055`/`:4063`), `ContractSizeExceeded` (`:2389`), `NumberPromisesExceeded` (`:619`), `NumberInputDataDependenciesExceeded` (`:2159`), `NumberOfLogsExceeded`/`TotalLogLengthExceeded` (`:97`/`:118`), `ReturnedValueLengthExceeded` (`:3776`), `YieldPayloadLength` (`:3596`), `EmptyMethodName` (`:2851`).
- **Preparation failures** → `FunctionCallError::CompilationError(PrepareError::*)`; the loading/method-resolve aborts produce a no-op or gas-bearing outcome per `fix_contract_loading_cost`/`fix_contract_loading_error` gating (above).
- **Cache resilience**: a malformed/missing/truncated on-disk cache file is treated as a miss and recompiled, never a crash (`cache.rs:803`,`:820`,`:839`).
- **Stack exhaustion**: `finite_wasm_stack` underflow → `MemoryAccessViolation` (`logic.rs:371`); gas exhaustion in instrumentation burns remaining gas then errors `IntegerOverflow` (`:385`).

## Code anchors

| Location | Symbol | What happens here |
|---|---|---|
| `runtime/near-vm-runner/src/runner.rs:89` | `run` | VM entry point: runs the prepared contract, records gas. |
| `runtime/near-vm-runner/src/runner.rs:54` | `prepare` | Dispatches to the `vm_kind` runtime to prepare a contract. |
| `runtime/near-vm-runner/src/runner.rs:205` | `VMKindExt::is_available` / `runtime` | Backend availability & instantiation (Wasmtime / NearVm). |
| `runtime/near-vm-runner/src/logic/logic.rs:158` | `VMLogic` | Host side of the contract↔host boundary; all host fns. |
| `runtime/near-vm-runner/src/logic/logic.rs:131` | `ExecutionResultState::compute_outcome` | Finalizes burnt/used/compute into a `VMOutcome`. |
| `runtime/near-vm-runner/src/logic/logic.rs:1979` | `pay_gas_for_new_receipt` | Charges receipt dispatch+exec fees on promise creation. |
| `runtime/near-vm-runner/src/logic/logic.rs:2188` | `promise_batch_create` | Creates an action receipt → `Promise::Receipt`. |
| `runtime/near-vm-runner/src/logic/logic.rs:2114` | `promise_and` | Builds the ephemeral join `Promise::NotReceipt`. |
| `runtime/near-vm-runner/src/logic/logic.rs:2827` | `promise_batch_action_function_call_weight` | Appends a `FunctionCall` action with gas weight; subsidy logic. |
| `runtime/near-vm-runner/src/logic/logic.rs:3396` | `promise_yield_create` | Self-receipt with an unresolved data dependency. |
| `runtime/near-vm-runner/src/logic/logic.rs:4039` | `storage_write` | Storage host fn; updates `current_storage_usage`, delegates to `ext`. |
| `runtime/near-vm-runner/src/logic/gas_counter.rs:119` | `deduct_gas` | Burnt/used split, promise gas reservation, limit check. |
| `runtime/near-vm-runner/src/logic/gas_counter.rs:169` | `process_gas_limit` | Clamps burnt gas; picks `GasLimitExceeded`/`GasExceeded`. |
| `runtime/near-vm-runner/src/logic/gas_counter.rs:236` | `before_loading_executable` / `after_loading_executable` | Contract-loading fee, ordered by `fix_contract_loading_cost`. |
| `runtime/near-vm-runner/src/prepare.rs:22` | `prepare_contract` | Validation + gas/stack instrumentation; v2/v3 dispatch. |
| `runtime/near-vm-runner/src/cache.rs:55` | `get_contract_cache_key` | `Version5` cache key over code+config+vm. |
| `runtime/near-vm-runner/src/cache.rs:717` | `FilesystemContractRuntimeCache::put` | On-disk write + LRU eviction. |
| `runtime/near-vm-runner/src/wasmtime_runner/mod.rs:683` | `with_compiled_and_loaded` | Cache lookup / compile / load + loading-fee ordering. |
| `runtime/near-vm-runner/src/wasmtime_runner/mod.rs:997` | `PreparedContract::run` | Builds result state, runs WASM, finalizes outcome. |
| `core/parameters/src/vm.rs:181` | `Config` | VM config: vm_kind, costs, limits, feature gates. |
| `core/primitives-core/src/version.rs:453` | `protocol_version` | Activation versions for the gated features above. |

## Open questions

- The NearVM backend (`runner.rs:219`, `near_vm_runner/`) was read only at the trait/dispatch level per the plan's "overview only" scope; its run/loading ordering is assumed to mirror Wasmtime's `before_loading_executable`/`after_loading_executable` because both go through the shared `GasCounter` helpers, but the NearVM `run` body was not line-traced here.
- At v86 the production `vm_kind` is `Wasmtime` (feature active since v84), but the exact value is set by the runtime-config layer ([genesis-configuration](genesis-configuration.md)); this spec did not trace where `Config::vm_kind` is populated per protocol version.
</content>
</invoke>
