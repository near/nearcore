# Plan: Smart-contract VM

Generation brief for `spec/contract-vm.md`. Follow `../CONVENTIONS.md`.

## Scope
Execution of WASM smart contracts: contract preparation/validation, the supported VM
backends, the host-function (bindings) API exposed to contracts, gas
metering of WASM ops and host calls, registers/memory, promise creation from within a
contract, and compiled-contract caching.

## Out of scope
- The FunctionCall *action* and how the runtime invokes the VM →
  [runtime-execution](../spec/runtime-execution.md).
- Storage reads/writes the host functions delegate to →
  [state-storage](../spec/state-storage.md) (host API covered here; trie there).

## Code to read
- `runtime/near-vm-runner/src/runner.rs` — `run`, `prepare`, VM dispatch by `VMKind`.
- `runtime/near-vm-runner/src/wasmtime_runner/{mod,logic}.rs` — the wasmtime backend,
  the host state (`Ctx`) and every host function. NOTE: the `VMLogic` struct and the
  legacy host-function implementations were removed in 2.14.0; `logic/logic.rs` now
  holds only `VMContext`, `ExecutionResultState` and `VMOutcome`.
- `runtime/near-vm-runner/src/logic/{gas_counter,dependencies,context,errors}.rs`.
- `runtime/near-vm-runner/src/{cache,prepare}.rs` and
  `runtime/near-vm-runner/src/prepare/{prepare_v3,instrument_v3}.rs` — caching, wasm
  validation/instrumentation. NOTE: `prepare_v2` was removed in 2.14.0.
- `runtime/near-vm-runner/src/{features,imports}.rs` — the wasm feature allow-list and
  the version/config gating of host-function imports.
- NOTE: the `runtime/near-vm*` engine crates (the NearVM backend) were deleted in
  2.14.0. Wasmtime is the only backend with an implementation.
- `core/parameters/src/vm.rs`, `core/parameters/src/cost.rs` — VM limits, `ExtCosts`,
  wasm op costs.
- `docs/RuntimeSpec/{FunctionCall,Preparation}.md`,
  `docs/RuntimeSpec/Components/BindingsSpec/*` (cross-check).

## Questions the spec must answer
- What VM backends exist (Wasmtime, NearVM, Wasmer2) and how is one chosen?
- Contract preparation: what validation/instrumentation happens, what limits apply
  (memory pages, stack height, function/local counts), and how is it gas-costed?
- The host-function API by category (registers, storage, context/economics, promises,
  math/crypto, validator, alt_bn128/bls12381). What each does at a high level.
- Gas metering: how are wasm instruction costs and `ExtCosts` charged; what is the
  gas counter's relationship to burnt/used gas and prepaid limits?
- How are promises created/chained from a contract (`promise_create`, `promise_then`,
  `promise_and`, yield/resume) and turned into receipts?
- Compiled-contract caching: what is cached, keyed how, and why (compilation cost).
- Failure modes: out-of-gas, host errors, compilation errors, deserialization errors.

## Cross-component edges
- Invoked by runtime FunctionCall; emits promises that become receipts handled by
  cross-shard/congestion; storage host calls hit state-storage. Link to each.

## Relevant ProtocolFeatures
- `Wasmtime`, `FixContractLoadingCost`, post-quantum / crypto host-fn features, compute
  costs (NEP-455), global contracts. Verify against `version.rs`.
