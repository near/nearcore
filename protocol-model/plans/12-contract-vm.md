# Plan: Smart-contract VM

Generation brief for `spec/contract-vm.md`. Follow `../CONVENTIONS.md`.

## Scope
Execution of WASM smart contracts: contract preparation/validation, the supported VM
backends, the host-function (bindings) API exposed to contracts via `VMLogic`, gas
metering of WASM ops and host calls, registers/memory, promise creation from within a
contract, and compiled-contract caching.

## Out of scope
- The FunctionCall *action* and how the runtime invokes the VM →
  [runtime-execution](../spec/runtime-execution.md).
- Storage reads/writes the host functions delegate to →
  [state-storage](../spec/state-storage.md) (host API covered here; trie there).

## Code to read
- `runtime/near-vm-runner/src/runner.rs` — `run`, `prepare`, VM dispatch by `VMKind`.
- `runtime/near-vm-runner/src/logic/logic.rs` — `VMLogic`, all host functions,
  `VMContext`, `ExecutionResultState`, `VMOutcome`.
- `runtime/near-vm-runner/src/logic/{gas_counter,dependencies,context,errors}.rs`.
- `runtime/near-vm-runner/src/{cache,prepare}.rs` — caching, wasm validation/instrumentation.
- `runtime/near-vm/` — NearVM backend (overview only).
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
